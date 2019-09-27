// https://www.loc.gov/preservation/digital/formats/fdd/fdd000471.shtml
// http://www.stata.com/help.cgi?dta

package datareader

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// These are constants used in Dta files to represent different data types.
const (
	StataFloat64Type ColumnTypeT = 65526
	StataFloat32Type ColumnTypeT = 65527
	StataInt32Type   ColumnTypeT = 65528
	StataInt16Type   ColumnTypeT = 65529
	StataInt8Type    ColumnTypeT = 65530
	StataStrlType    ColumnTypeT = 32768
)

var (
	supportedDtaVersions = []int{114, 115, 117, 118}
	rowCountLength       = map[int]int{114: 4, 115: 4, 117: 4, 118: 8}
	nvarLength           = map[int]int{114: 2, 115: 2, 117: 2, 118: 2}
	datasetLabelLength   = map[int]int{117: 1, 118: 2}
	valueLabelLength     = map[int]int{117: 33, 118: 129}
	voLength             = map[int]int{117: 8, 118: 12}
)

func logerr(err error) {
	if err != nil {
		log.Printf("%+v", errors.Wrap(err, ""))
	}
}

// StataReader reads Stata dta data files.  Currently dta format
// versions 114, 115, 117, and 118 can be read.
//
// The Read method reads and returns the data.  Several fields of the
// StataReader struct may also be of interest.
//
// Technical information about the file format can be found here:
// http://www.stata.com/help.cgi?dta
type StataReader struct {

	// If true, the strl numerical codes are replaced with their
	// string values when available.
	InsertStrls bool

	// If true, the categorial numerical codes are replaced with
	// their string labels when available.
	InsertCategoryLabels bool

	// If true, dates are converted to Go date format.
	ConvertDates bool

	// A short text label for the data set.
	DatasetLabel string

	// The time stamp for the data set
	TimeStamp string

	// Number of variables
	Nvar int

	// Number of observations
	rowCount int

	// Variable types, see technical documentation for meaning
	varTypes []ColumnTypeT

	// A name for each variable
	columnNames []string

	// An additional text entry describing each variable
	ColumnNamesLong []string

	// String labels for categorical variables
	ValueLabels     map[string]map[int32]string
	ValueLabelNames []string

	// Format codes for each variable
	Formats []string

	// Maps from strl keys to values
	Strls      map[uint64]string
	StrlsBytes map[uint64][]byte

	// The format version of the dta file
	FormatVersion int

	// The endian-ness of the file
	ByteOrder binary.ByteOrder

	// The number of rows of data that have been read.
	rowsRead int

	// Map information
	seekVartypes        int64
	seekVarnames        int64
	seekSortlist        int64
	seekFormats         int64
	seekValueLabelNames int64
	seekVariableLabels  int64
	seekCharacteristics int64
	seekData            int64
	seekStrls           int64
	seekValueLabels     int64

	// Indicates the columns that contain dates
	isDate []bool

	// An io channel from which the data are read
	reader io.ReadSeeker
}

// NewStataReader returns a StataReader for reading from the given io.ReadSeeker.
func NewStataReader(r io.ReadSeeker) (*StataReader, error) {
	rdr := new(StataReader)
	rdr.reader = r

	// Defaults, can be changed before reading
	rdr.InsertStrls = true
	rdr.InsertCategoryLabels = true
	rdr.ConvertDates = true

	err := rdr.init()
	if err != nil {
		return nil, err
	}
	return rdr, nil
}

// RowCount returns the number of rows in the data set.
func (rdr *StataReader) RowCount() int {
	return rdr.rowCount
}

// ColumnNames returns the names of the columns in the data file.
func (rdr *StataReader) ColumnNames() []string {
	return rdr.columnNames
}

// ColumnTypes returns integer codes corresponding to the data types
// in the Stata file.  See the Stata dta doumentation for more
// information.
func (rdr *StataReader) ColumnTypes() []ColumnTypeT {
	return rdr.varTypes
}

func (rdr *StataReader) init() error {

	var err error

	// Determine if we have <117 or >=117 dta version.
	c := make([]byte, 1)
	_, err = rdr.reader.Read(c)
	if err != nil {
		logerr(err)
		return err
	}
	if _, err := rdr.reader.Seek(0, 0); err != nil {
		panic(err)
	}

	if string(c) == "<" {
		err = rdr.readNewHeader()
	} else {
		err = rdr.readOldHeader()
	}
	if err != nil {
		logerr(err)
		return err
	}

	if err := rdr.readVartypes(); err != nil {
		logerr(err)
		return err
	}

	if rdr.FormatVersion < 117 {
		if err := rdr.translateVartypes(); err != nil {
			logerr(err)
			return err
		}
	}

	if err := rdr.readVarnames(); err != nil {
		logerr(err)
		return err
	}

	// Skip over srtlist
	if rdr.FormatVersion < 117 {
		m := int64(2 * (rdr.Nvar + 1))
		if _, err := rdr.reader.Seek(m, 1); err != nil {
			logerr(err)
			return err
		}
	}

	if err := rdr.readFormats(); err != nil {
		logerr(err)
		return err
	}

	if err := rdr.readValueLabelNames(); err != nil {
		logerr(err)
		return err
	}

	if err := rdr.readVariableLabels(); err != nil {
		logerr(err)
		return err
	}

	if rdr.FormatVersion < 117 {
		if err := rdr.readExpansionFields(); err != nil {
			logerr(err)
			return err
		}
	}

	if rdr.FormatVersion >= 117 {
		if err := rdr.readStrls(); err != nil {
			logerr(err)
			return err
		}

		// Must be called manually for older format < 117.
		if err := rdr.readValueLabels(); err != nil {
			logerr(err)
			return err
		}
	}

	return nil
}

func (rdr *StataReader) readExpansionFields() error {
	var b byte
	var i int32

	for {
		err := binary.Read(rdr.reader, rdr.ByteOrder, &b)
		if err != nil {
			logerr(err)
			return err
		}
		err = binary.Read(rdr.reader, rdr.ByteOrder, &i)
		if err != nil {
			logerr(err)
			return err
		}

		if b == 0 && i == 0 {
			break
		}
		if _, err := rdr.reader.Seek(int64(i), 1); err != nil {
			logerr(err)
			return err
		}
	}

	return nil
}

// readInt reads a 1, 2, 4 or 8 byte signed integer.
func (rdr *StataReader) readInt(width int) (int, error) {

	switch width {
	default:
		return 0, fmt.Errorf("unsupported width %d in readInt", width)
	case 1:
		var x int8
		err := binary.Read(rdr.reader, rdr.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	case 2:
		var x int16
		err := binary.Read(rdr.reader, rdr.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	case 4:
		var x int32
		err := binary.Read(rdr.reader, rdr.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	case 8:
		var x int64
		err := binary.Read(rdr.reader, rdr.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	}
}

// readUint reads a 1, 2, 4 or 8 byte unsigned integer.
func (rdr *StataReader) readUint(width int) (int, error) {

	switch width {
	default:
		panic("unsupported width in readUint")
	case 1:
		var x uint8
		err := binary.Read(rdr.reader, rdr.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	case 2:
		var x uint16
		err := binary.Read(rdr.reader, rdr.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	case 4:
		var x uint32
		err := binary.Read(rdr.reader, rdr.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	case 8:
		var x uint64
		err := binary.Read(rdr.reader, rdr.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	}
}

// readOldHeader reads the pre version 117 header
func (rdr *StataReader) readOldHeader() error {

	if _, err := rdr.reader.Seek(0, 0); err != nil {
		logerr(err)
		return err
	}
	buf := make([]byte, 81)

	// Get the format
	var format uint8
	err := binary.Read(rdr.reader, binary.LittleEndian, &format)
	if err != nil {
		logerr(err)
		return err
	}
	rdr.FormatVersion = int(format)
	if !rdr.supportedVersion() {
		return fmt.Errorf("Invalid Stata dta format version: %v\n", rdr.FormatVersion)
	}

	// Get the byte order
	var bo uint8
	err = binary.Read(rdr.reader, binary.LittleEndian, &bo)
	if err != nil {
		logerr(err)
		return err
	}
	if bo == 1 {
		rdr.ByteOrder = binary.BigEndian
	} else {
		rdr.ByteOrder = binary.LittleEndian
	}

	// Skip two bytes
	if _, err := rdr.reader.Seek(2, 1); err != nil {
		logerr(err)
		return err
	}

	// Number of variables
	rdr.Nvar, err = rdr.readInt(nvarLength[rdr.FormatVersion])
	if err != nil {
		logerr(err)
		return err
	}

	// Number of observations
	rdr.rowCount, err = rdr.readInt(rowCountLength[rdr.FormatVersion])
	if err != nil {
		logerr(err)
		return err
	}

	// Data label
	n, err := rdr.reader.Read(buf[0:81])
	if err != nil {
		logerr(err)
		return err
	}
	if n != 81 {
		return fmt.Errorf("stata file appears to be truncated")
	}
	rdr.DatasetLabel = string(partition(buf[0:81]))

	// Time stamp
	n, err = rdr.reader.Read(buf[0:18])
	if err != nil {
		logerr(err)
		return err
	}
	if n != 18 {
		return fmt.Errorf("stata file appears to be truncated")
	}
	rdr.TimeStamp = string(partition(buf[0:18]))

	return nil
}

func (rdr *StataReader) supportedVersion() bool {

	for _, v := range supportedDtaVersions {
		if rdr.FormatVersion == v {
			return true
		}
	}
	return false
}

// readNewHeader reads a new-style xml header (versions 117+).
func (rdr *StataReader) readNewHeader() error {

	buf := make([]byte, 500)
	var n8 uint8

	// <stata_dta><header><release>
	n, err := rdr.reader.Read(buf[0:28])
	if err != nil {
		logerr(err)
		return err
	}
	if n != 28 {
		return fmt.Errorf("file appears to be truncated")
	}
	if string(buf[0:11]) != "<stata_dta>" {
		return fmt.Errorf("Invalid Stata file")
	}

	// Stata file version
	if _, err := rdr.reader.Read(buf[0:3]); err != nil {
		logerr(err)
		return err
	}
	x, err := strconv.ParseUint(string(buf[0:3]), 0, 64)
	if err != nil {
		logerr(err)
		return err
	}
	rdr.FormatVersion = int(x)
	if !rdr.supportedVersion() {
		return fmt.Errorf("Invalid Stata dta format version")
	}

	// </release><byteorder>
	if _, err := rdr.reader.Seek(21, 1); err != nil {
		logerr(err)
		return err
	}

	// Byte order
	if _, err := rdr.reader.Read(buf[0:3]); err != nil {
		logerr(err)
		return err
	}
	if string(buf[0:3]) == "MSF" {
		rdr.ByteOrder = binary.BigEndian
	} else {
		rdr.ByteOrder = binary.LittleEndian
	}

	// </byteorder><K>
	if _, err := rdr.reader.Seek(15, 1); err != nil {
		logerr(err)
		return err
	}

	// Number of variables
	rdr.Nvar, err = rdr.readInt(nvarLength[rdr.FormatVersion])
	if err != nil {
		logerr(err)
		return err
	}

	// </K><N>
	if _, err := rdr.reader.Seek(7, 1); err != nil {
		logerr(err)
		return err
	}

	// Number of observations
	rdr.rowCount, err = rdr.readInt(rowCountLength[rdr.FormatVersion])
	if err != nil {
		logerr(err)
		return err
	}

	// </N><label>
	if _, err := rdr.reader.Seek(11, 1); err != nil {
		logerr(err)
		return err
	}

	// Data set label
	w, err := rdr.readUint(datasetLabelLength[rdr.FormatVersion])
	if err != nil {
		logerr(err)
		return err
	}
	n, err = rdr.reader.Read(buf[0:w])
	if err != nil {
		logerr(err)
		return err
	}
	if n != w {
		return fmt.Errorf("stata file appears to be truncated")
	}
	rdr.DatasetLabel = string(buf[0:w])

	// </label><timestamp>
	if _, err := rdr.reader.Seek(19, 1); err != nil {
		logerr(err)
		return err
	}

	// Time stamp
	err = binary.Read(rdr.reader, rdr.ByteOrder, &n8)
	if err != nil {
		logerr(err)
		return err
	}
	n, err = rdr.reader.Read(buf[0:n8])
	if err != nil {
		logerr(err)
		return err
	}
	if n != int(n8) {
		logerr(fmt.Errorf("stata file appears to be truncated"))
		return err
	}
	rdr.TimeStamp = string(buf[0:n8])

	// </timestamp></header><map> + 16 bytes
	if _, err := rdr.reader.Seek(42, 1); err != nil {
		logerr(err)
		return err
	}

	// Map
	if err := binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seekVartypes); err != nil {
		return err
	}
	if err := binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seekVarnames); err != nil {
		return err
	}
	if err := binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seekSortlist); err != nil {
		return err
	}
	if err := binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seekFormats); err != nil {
		return err
	}
	if err := binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seekValueLabelNames); err != nil {
		return err
	}
	if err := binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seekVariableLabels); err != nil {
		return err
	}
	if err := binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seekCharacteristics); err != nil {
		return err
	}
	if err := binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seekData); err != nil {
		return err
	}
	if err := binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seekStrls); err != nil {
		return err
	}
	if err := binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seekValueLabels); err != nil {
		return err
	}

	return nil
}

func (rdr *StataReader) readVartypes() error {

	var err error

	switch {
	case rdr.FormatVersion == 118:
		err = rdr.readVartypes16()
	case rdr.FormatVersion == 117:
		err = rdr.readVartypes16()
	case rdr.FormatVersion == 115:
		err = rdr.readVartypes8()
	case rdr.FormatVersion == 114:
		err = rdr.readVartypes8()
	default:
		err = fmt.Errorf("unknown format version %v", rdr.FormatVersion)
	}

	return err
}

func (rdr *StataReader) readVartypes16() error {

	if _, err := rdr.reader.Seek(rdr.seekVartypes+16, 0); err != nil {
		logerr(err)
		return err
	}

	rdr.varTypes = make([]ColumnTypeT, rdr.Nvar)
	for k := range rdr.varTypes {
		v, err := rdr.readUint(2)
		if err != nil {
			logerr(err)
			return err
		}
		rdr.varTypes[k] = ColumnTypeT(v)
	}

	return nil
}

func (rdr *StataReader) readVartypes8() error {

	rdr.varTypes = make([]ColumnTypeT, rdr.Nvar)
	for k := range rdr.varTypes {
		v, err := rdr.readUint(1)
		if err != nil {
			logerr(err)
			return err
		}
		rdr.varTypes[k] = ColumnTypeT(v)
	}

	return nil
}

func (rdr *StataReader) translateVartypes() error {

	for k := 0; k < int(rdr.Nvar); k++ {
		switch {
		case rdr.varTypes[k] <= 244:
			// strf
			continue
		case rdr.varTypes[k] == 251:
			rdr.varTypes[k] = StataInt8Type
		case rdr.varTypes[k] == 252:
			rdr.varTypes[k] = StataInt16Type
		case rdr.varTypes[k] == 253:
			rdr.varTypes[k] = StataInt32Type
		case rdr.varTypes[k] == 254:
			rdr.varTypes[k] = StataFloat32Type
		case rdr.varTypes[k] == 255:
			rdr.varTypes[k] = StataFloat64Type
		default:
			return fmt.Errorf("unknown variable type")
		}
	}

	return nil
}

func (rdr *StataReader) readFormats() error {

	var err error

	switch {
	case rdr.FormatVersion == 118:
		err = rdr.doReadFormats(57, true)
	case rdr.FormatVersion == 117:
		err = rdr.doReadFormats(49, true)
	case rdr.FormatVersion == 115:
		err = rdr.doReadFormats(49, false)
	case rdr.FormatVersion == 114:
		err = rdr.doReadFormats(49, false)
	default:
		err = fmt.Errorf("unknown format version %v", rdr.FormatVersion)
	}

	if err != nil {
		logerr(err)
	}

	return err
}

func (rdr *StataReader) doReadFormats(bufsize int, seek bool) error {

	buf := make([]byte, bufsize)
	if seek {
		if _, err := rdr.reader.Seek(rdr.seekFormats+9, 0); err != nil {
			logerr(err)
			return err
		}
	}

	rdr.Formats = make([]string, rdr.Nvar)
	for k := range rdr.Formats {
		if _, err := rdr.reader.Read(buf); err != nil {
			logerr(err)
			return err
		}
		rdr.Formats[k] = string(partition(buf))
	}

	rdr.isDate = make([]bool, rdr.Nvar)
	for k := range rdr.isDate {
		if strings.Index(rdr.Formats[k], "%td") == 0 {
			rdr.isDate[k] = true
		} else if strings.Index(rdr.Formats[k], "%tc") == 0 {
			rdr.isDate[k] = true
		}
	}

	return nil
}

// Returns everything before the first null byte.
func partition(b []byte) []byte {
	for i, v := range b {
		if v == 0 {
			return b[0:i]
		}
	}
	return b
}

// readVarnames dispatches to the correct function for reading
// variable names for the dta file format.
func (rdr *StataReader) readVarnames() error {

	var err error
	switch rdr.FormatVersion {
	case 118:
		err = rdr.doReadVarnames(129, true)
	case 117:
		err = rdr.doReadVarnames(33, true)
	case 115:
		err = rdr.doReadVarnames(33, false)
	case 114:
		err = rdr.doReadVarnames(33, false)
	default:
		err = fmt.Errorf("unknown format version %d", rdr.FormatVersion)
	}

	if err != nil {
		logerr(err)
	}
	return err
}

func (rdr *StataReader) doReadVarnames(bufsize int, seek bool) error {

	buf := make([]byte, bufsize)
	if seek {
		_, err := rdr.reader.Seek(rdr.seekVarnames+10, 0)
		if err != nil {
			panic(err)
		}
	}

	rdr.columnNames = make([]string, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		n, err := rdr.reader.Read(buf)
		if err != nil {
			logerr(err)
			return err
		}
		if n != bufsize {
			return fmt.Errorf("stata file appears to be truncated")
		}
		rdr.columnNames[k] = string(partition(buf))
	}

	return nil
}

func (rdr *StataReader) readValueLabelNames() error {

	var err error
	switch rdr.FormatVersion {
	case 118:
		err = rdr.doReadValueLabelNames(129, true)
	case 117:
		err = rdr.doReadValueLabelNames(33, true)
	case 116:
		err = rdr.doReadValueLabelNames(33, false)
	case 115:
		err = rdr.doReadValueLabelNames(33, false)
	default:
		return fmt.Errorf("unknown format version %v", rdr.FormatVersion)
	}

	if err != nil {
		logerr(err)
	}
	return err
}

func (rdr *StataReader) doReadValueLabelNames(bufsize int, seek bool) error {

	buf := make([]byte, bufsize)
	if seek {
		if _, err := rdr.reader.Seek(rdr.seekValueLabelNames+19, 0); err != nil {
			logerr(err)
			return err
		}
	}

	rdr.ValueLabelNames = make([]string, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		if _, err := rdr.reader.Read(buf); err != nil {
			return err
		}
		rdr.ValueLabelNames[k] = string(partition(buf))
	}

	return nil
}

func (rdr *StataReader) readVariableLabels() error {

	var err error
	switch rdr.FormatVersion {
	case 118:
		err = rdr.doReadVariableLabels(321, true)
	case 117:
		err = rdr.doReadVariableLabels(81, true)
	case 115:
		err = rdr.doReadVariableLabels(81, false)
	case 114:
		err = rdr.doReadVariableLabels(81, false)
	default:
		err = fmt.Errorf("Unknown format version %d", rdr.FormatVersion)
	}

	if err != nil {
		logerr(err)
	}
	return err
}

func (rdr *StataReader) doReadVariableLabels(bufsize int, seek bool) error {

	buf := make([]byte, bufsize)
	if seek {
		if _, err := rdr.reader.Seek(rdr.seekVariableLabels+17, 0); err != nil {
			logerr(err)
			return err
		}
	}

	rdr.ColumnNamesLong = make([]string, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		if _, err := rdr.reader.Read(buf); err != nil {
			logerr(err)
			return err
		}
		rdr.ColumnNamesLong[k] = string(partition(buf))
	}

	return nil
}

func (rdr *StataReader) readValueLabels() error {

	vl := make(map[string]map[int32]string)
	buf := make([]byte, 321)

	if _, err := rdr.reader.Seek(rdr.seekValueLabels+14, 0); err != nil {
		return err
	}

	var n int32
	var textlen int32
	vlw := valueLabelLength[rdr.FormatVersion]

	for {
		if _, err := rdr.reader.Read(buf[0:5]); err != nil {
			return err
		}
		if string(buf[0:5]) != "<lbl>" {
			break
		}

		if _, err := rdr.reader.Seek(4, 1); err != nil {
			return err
		}
		if _, err := rdr.reader.Read(buf[0:vlw]); err != nil {
			return err
		}
		labname := string(partition(buf[0:vlw]))
		if _, err := rdr.reader.Seek(3, 1); err != nil {
			return err
		}

		if err := binary.Read(rdr.reader, rdr.ByteOrder, &n); err != nil {
			return err
		}
		if err := binary.Read(rdr.reader, rdr.ByteOrder, &textlen); err != nil {
			return err
		}

		off := make([]int32, n)
		val := make([]int32, n)

		for j := int32(0); j < n; j++ {
			if err := binary.Read(rdr.reader, rdr.ByteOrder, &off[j]); err != nil {
				return err
			}
		}

		for j := int32(0); j < n; j++ {
			if err := binary.Read(rdr.reader, rdr.ByteOrder, &val[j]); err != nil {
				return err
			}
		}

		if cap(buf) < int(textlen) {
			buf = make([]byte, 2*textlen)
		}

		if _, err := rdr.reader.Read(buf[0:textlen]); err != nil {
			return err
		}

		vk := make(map[int32]string)
		for j := int32(0); j < n; j++ {
			vk[val[j]] = string(partition(buf[off[j]:]))
		}
		vl[labname] = vk

		// </lbl>
		if _, err := rdr.reader.Seek(6, 1); err != nil {
			return err
		}
	}

	rdr.ValueLabels = vl

	return nil
}

func (rdr *StataReader) readStrls() error {

	if _, err := rdr.reader.Seek(rdr.seekStrls+7, 0); err != nil {
		return err
	}

	vo := make([]byte, voLength[rdr.FormatVersion])
	vo8 := make([]byte, 8)
	var t uint8
	var length uint32

	rdr.Strls = make(map[uint64]string)
	rdr.StrlsBytes = make(map[uint64][]byte)

	rdr.Strls[0] = ""

	buf := make([]byte, 100)
	buf3 := make([]byte, 3)

	for {
		n, err := rdr.reader.Read(buf3)
		if n <= 0 || err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if string(buf3) != "GSO" {
			break
		}

		if err := binary.Read(rdr.reader, rdr.ByteOrder, vo); err != nil {
			return err
		}
		if err := binary.Read(rdr.reader, rdr.ByteOrder, &t); err != nil {
			return err
		}
		if err := binary.Read(rdr.reader, rdr.ByteOrder, &length); err != nil {
			return err
		}

		if voLength[rdr.FormatVersion] == 12 {
			copy(vo8[0:2], vo[0:2])
			copy(vo8[2:8], vo[4:10])
		} else {
			copy(vo8, vo)
		}

		var ptr uint64
		if err := binary.Read(bytes.NewReader(vo8), rdr.ByteOrder, &ptr); err != nil {
			return err
		}

		if len(buf) < int(length) {
			buf = make([]byte, 2*length)
		}
		if _, err := rdr.reader.Read(buf[0:length]); err != nil {
			return err
		}

		switch t {
		case 130:
			buf = partition(buf[0:length])
			rdr.Strls[ptr] = string(buf)
		case 129:
			rdr.StrlsBytes[ptr] = make([]byte, length)
			copy(rdr.StrlsBytes[ptr], buf[0:length])
		default:
			return fmt.Errorf("unknown t value")
		}
	}

	return nil
}

func (rdr *StataReader) allocateCols(nval int) []interface{} {

	data := make([]interface{}, rdr.Nvar)
	for j, t := range rdr.varTypes {
		switch {
		case t <= 2045:
			data[j] = make([]string, nval)
		case t == StataStrlType:
			if rdr.InsertStrls {
				data[j] = make([]string, nval)
			} else {
				data[j] = make([]uint64, nval)
			}
		case t == StataFloat64Type:
			data[j] = make([]float64, nval)
		case t == StataFloat32Type:
			data[j] = make([]float32, nval)
		case t == StataInt32Type:
			data[j] = make([]int32, nval)
		case t == StataInt16Type:
			data[j] = make([]int16, nval)
		case t == StataInt8Type:
			data[j] = make([]int8, nval)
		default:
			panic(fmt.Sprintf("unknown variable type: %v", t))
		}
	}

	return data
}

func (rdr *StataReader) doInsertCategoryLabels(data []interface{}, missing [][]bool, nval int) {

	for j := 0; j < rdr.Nvar; j++ {
		labname := rdr.ValueLabelNames[j]
		mp, ok := rdr.ValueLabels[labname]
		if !ok {
			continue
		}

		idat, err := castToInt(data[j])
		if err != nil {
			panic(fmt.Sprintf("non-integer value label indices: %v", err))
		}

		newdata := make([]string, nval)
		for i := 0; i < nval; i++ {
			if !missing[j][i] {
				v, ok := mp[int32(idat[i])]
				if ok {
					newdata[i] = v
				} else {
					newdata[i] = fmt.Sprintf("%v", idat[i])
				}
			}
		}
		data[j] = newdata
	}
}

func (rdr *StataReader) readRow(i int, buf, buf8 []byte, data []interface{}, missing [][]bool) {

	for j := 0; j < rdr.Nvar; j++ {
		switch t := rdr.varTypes[j]; {
		case t <= 2045:
			// strf
			if _, err := rdr.reader.Read(buf[0:t]); err != nil {
				panic(err)
			}
			data[j].([]string)[i] = string(partition(buf[0:t]))
		case t == StataStrlType:
			if rdr.InsertStrls {
				// The STRL pointer is 2 byte integer followed by 6 byte integer
				// or 4 + 4 depending on the version
				if err := binary.Read(rdr.reader, rdr.ByteOrder, buf8); err != nil {
					panic(err)
				}
				var ptr uint64
				if err := binary.Read(bytes.NewReader(buf8), rdr.ByteOrder, &ptr); err != nil {
					panic(err)
				}
				data[j].([]string)[i] = rdr.Strls[ptr]
			} else {
				if err := binary.Read(rdr.reader, rdr.ByteOrder, &(data[j].([]uint64)[i])); err != nil {
					panic(err)
				}
			}
		case t == StataFloat64Type:
			var x float64
			if err := binary.Read(rdr.reader, rdr.ByteOrder, &x); err != nil {
				panic(err)
			}
			data[j].([]float64)[i] = x
			// Lower bound in dta spec is out of range.
			if x > 8.988e307 || x < -8.988e307 {
				missing[j][i] = true
			}
		case t == StataFloat32Type:
			var x float32
			if err := binary.Read(rdr.reader, rdr.ByteOrder, &x); err != nil {
				panic(err)
			}
			data[j].([]float32)[i] = x
			if x > 1.701e38 || x < -1.701e38 {
				missing[j][i] = true
			}
		case t == StataInt32Type:
			var x int32
			if err := binary.Read(rdr.reader, rdr.ByteOrder, &x); err != nil {
				panic(err)
			}
			data[j].([]int32)[i] = x
			if x > 2147483620 || x < -2147483647 {
				missing[j][i] = true
			}
		case t == StataInt16Type:
			var x int16
			if err := binary.Read(rdr.reader, rdr.ByteOrder, &x); err != nil {
				panic(err)
			}
			data[j].([]int16)[i] = x
			if x > 32740 || x < -32767 {
				missing[j][i] = true
			}
		case t == StataInt8Type:
			var x int8
			if err := binary.Read(rdr.reader, rdr.ByteOrder, &x); err != nil {
				panic(err)
			}
			if x < -127 || x > 100 {
				missing[j][i] = true
			}
			data[j].([]int8)[i] = x
		default:
			msg := fmt.Sprintf("Unknown variable type")
			panic(msg)
		}
	}
}

// Read returns the given number of rows of data from the Stata data
// file.  The data are returned as an array of Series objects.  If
// rows is negative, the remainder of the file is read.
func (rdr *StataReader) Read(rows int) ([]*Series, error) {

	// Compute number of values to read
	nval := int(rdr.rowCount) - rdr.rowsRead
	if rows >= 0 && rows < nval {
		nval = rows
	} else if nval <= 0 {
		return nil, nil
	}

	data := rdr.allocateCols(nval)
	missing := make([][]bool, rdr.Nvar)

	for j := 0; j < int(rdr.Nvar); j++ {
		missing[j] = make([]bool, nval)
	}

	if rdr.FormatVersion >= 117 && rdr.rowsRead == 0 {
		if _, err := rdr.reader.Seek(rdr.seekData+6, 0); err != nil {
			return nil, err
		}
	}

	buf := make([]byte, 2045)
	buf8 := make([]byte, 8)
	for i := 0; i < nval; i++ {

		rdr.rowsRead += 1
		if rdr.rowsRead > int(rdr.rowCount) {
			break
		}

		rdr.readRow(i, buf, buf8, data, missing)
	}

	if rdr.InsertCategoryLabels {
		rdr.doInsertCategoryLabels(data, missing, nval)
	}

	if rdr.ConvertDates {
		for j := range data {
			if rdr.isDate[j] {
				data[j] = rdr.doConvertDates(data[j], rdr.Formats[j])
			}
		}
	}

	// Now that we have the raw data, convert it to a series.
	rdata := make([]*Series, len(data))
	var err error
	for j, v := range data {
		rdata[j], err = NewSeries(rdr.columnNames[j], v, missing[j])
		if err != nil {
			return nil, err
		}
	}

	return rdata, nil
}

func (rdr *StataReader) doConvertDates(v interface{}, format string) interface{} {

	vec, err := upcastNumeric(v)
	if err != nil {
		panic(fmt.Sprintf("unable to handle type %T in date vector", v))
	}

	bt := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)

	rvec := make([]time.Time, len(vec))

	var tq time.Duration
	if strings.Index(format, "%td") == 0 {
		tq = time.Hour * 24
	} else if strings.Index(format, "%tc") == 0 {
		tq = time.Millisecond
	} else {
		panic("unable to handle format in date vector")
	}

	for j, v := range vec {
		d := time.Duration(v) * tq
		rvec[j] = bt.Add(d)
	}

	return rvec
}
