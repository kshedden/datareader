package datareader

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

var supported_dta_versions = []int{114, 115, 117, 118}
var row_count_length = map[int]int{114: 4, 115: 4, 117: 4, 118: 8}
var nvar_length = map[int]int{114: 2, 115: 2, 117: 2, 118: 2}
var dataset_label_length = map[int]int{117: 1, 118: 2}
var value_label_length = map[int]int{117: 33, 118: 129}
var vo_length = map[int]int{117: 8, 118: 12}

// A StataReader reads Stata dta data files.  Currently dta format
// versions 115, 117, and 118 can be read.
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
	row_count int

	// Variable types, see technical documentation for meaning
	var_types []int

	// A name for each variable
	column_names []string

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
	rows_read int

	// Map information
	seek_vartypes          int64
	seek_varnames          int64
	seek_sortlist          int64
	seek_formats           int64
	seek_value_label_names int64
	seek_variable_labels   int64
	seek_characteristics   int64
	seek_data              int64
	seek_strls             int64
	seek_value_labels      int64

	// Indicates the columns that contain dates
	is_date []bool

	// An io channel from which the data are read
	reader io.ReadSeeker
}

// NewStataReader returns a StataReader for reading from the given io channel.
func NewStataReader(r io.ReadSeeker) (*StataReader, error) {
	rdr := new(StataReader)
	rdr.reader = r

	// Defaults
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
	return rdr.row_count
}

// ColumnNames returns the names of the columns in the data file.
func (rdr *StataReader) ColumnNames() []string {
	return rdr.column_names
}

// ColumnTypes returns integer codes corresponding to the data types
// in the Stata file.  See the Stata dta doumentation for more
// information.
func (rdr *StataReader) ColumnTypes() []int {
	return rdr.var_types
}

func (rdr *StataReader) init() error {

	var err error

	// Determine if we have <117 or >=117 dta version.
	c := make([]byte, 1)
	_, err = rdr.reader.Read(c)
	if err != nil {
		return err
	}
	rdr.reader.Seek(0, 0)

	if string(c) == "<" {
		err = rdr.read_new_header()
	} else {
		err = rdr.read_old_header()
	}
	if err != nil {
		return err
	}

	err = rdr.read_vartypes()
	if err != nil {
		return err
	}

	if rdr.FormatVersion < 117 {
		rdr.translate_vartypes()
	}

	err = rdr.read_varnames()
	if err != nil {
		return err
	}

	// Skip over srtlist
	if rdr.FormatVersion < 117 {
		m := int64(2 * (rdr.Nvar + 1))
		rdr.reader.Seek(m, 1)
	}

	rdr.read_formats()
	rdr.read_value_label_names()
	rdr.read_variable_labels()

	if rdr.FormatVersion < 117 {
		err = rdr.read_expansion_fields()
		if err != nil {
			return err
		}
	}

	if rdr.FormatVersion >= 117 {
		rdr.read_strls()

		// Must be called manually for older format < 117.
		rdr.read_value_labels()
	}

	return nil
}

func (rdr *StataReader) read_expansion_fields() error {
	var b byte
	var i int32

	for {
		err := binary.Read(rdr.reader, rdr.ByteOrder, &b)
		if err != nil {
			return err
		}
		err = binary.Read(rdr.reader, rdr.ByteOrder, &i)
		if err != nil {
			return err
		}

		if (b == 0) && (i == 0) {
			break
		}
		rdr.reader.Seek(int64(i), 1)
	}

	return nil
}

func (rdr *StataReader) read_int(width int) (int, error) {

	switch width {
	default:
		return 0, errors.New(fmt.Sprintf("unsupported width %d in read_int", width))
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

func (rdr *StataReader) read_uint(width int) (int, error) {

	switch width {
	default:
		panic("unsupported width in read_int")
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

// read_old_header reads the pre version 117 header
func (rdr *StataReader) read_old_header() error {

	rdr.reader.Seek(0, 0)
	buf := make([]byte, 81)

	// Get the format
	var format uint8
	err := binary.Read(rdr.reader, binary.LittleEndian, &format)
	if err != nil {
		return err
	}
	rdr.FormatVersion = int(format)
	if !rdr.supported_version() {
		return errors.New(fmt.Sprintf("Invalid Stata dta format version: %v\n", rdr.FormatVersion))
	}

	// Get the byte order
	var bo uint8
	err = binary.Read(rdr.reader, binary.LittleEndian, &bo)
	if err != nil {
		return err
	}
	if bo == 1 {
		rdr.ByteOrder = binary.BigEndian
	} else {
		rdr.ByteOrder = binary.LittleEndian
	}

	// Skip two bytes
	rdr.reader.Seek(2, 1)

	// Number of variables
	rdr.Nvar, err = rdr.read_int(nvar_length[rdr.FormatVersion])
	if err != nil {
		return err
	}

	// Number of observations
	rdr.row_count, err = rdr.read_int(row_count_length[rdr.FormatVersion])
	if err != nil {
		return err
	}

	// Data label
	n, err := rdr.reader.Read(buf[0:81])
	if err != nil {
		return err
	}
	if n != 81 {
		return errors.New("stata file appears to be truncated")
	}
	rdr.DatasetLabel = string(partition(buf[0:81]))

	// Time stamp
	n, err = rdr.reader.Read(buf[0:18])
	if err != nil {
		return err
	}
	if n != 18 {
		return errors.New("stata file appears to be truncated")
	}
	rdr.TimeStamp = string(partition(buf[0:18]))

	return nil
}

func (rdr *StataReader) supported_version() bool {

	supported := false
	for _, v := range supported_dta_versions {
		if rdr.FormatVersion == v {
			supported = true
		}
	}
	return supported
}

// read_new_header reads a new-style xml header (versions 117+).
func (rdr *StataReader) read_new_header() error {

	buf := make([]byte, 500)
	var n8 uint8

	// <stata_dta><header><release>
	n, err := rdr.reader.Read(buf[0:28])
	if err != nil {
		return err
	}
	if n != 28 {
		return errors.New("file appears to be truncated")
	}
	if string(buf[0:11]) != "<stata_dta>" {
		return errors.New("Invalid Stata file")
	}

	// Stata file version
	rdr.reader.Read(buf[0:3])
	x, err := strconv.ParseUint(string(buf[0:3]), 0, 64)
	if err != nil {
		return err
	}
	rdr.FormatVersion = int(x)
	if !rdr.supported_version() {
		return errors.New("Invalid Stata dta format version")
	}

	// </release><byteorder>
	rdr.reader.Seek(21, 1)

	// Byte order
	rdr.reader.Read(buf[0:3])
	if string(buf[0:3]) == "MSF" {
		rdr.ByteOrder = binary.BigEndian
	} else {
		rdr.ByteOrder = binary.LittleEndian
	}

	// </byteorder><K>
	rdr.reader.Seek(15, 1)

	// Number of variables
	rdr.Nvar, err = rdr.read_int(nvar_length[rdr.FormatVersion])
	if err != nil {
		return err
	}

	// </K><N>
	rdr.reader.Seek(7, 1)

	// Number of observations
	rdr.row_count, err = rdr.read_int(row_count_length[rdr.FormatVersion])
	if err != nil {
		return err
	}

	// </N><label>
	rdr.reader.Seek(11, 1)

	// Data set label
	w, err := rdr.read_uint(dataset_label_length[rdr.FormatVersion])
	if err != nil {
		return err
	}
	n, err = rdr.reader.Read(buf[0:w])
	if err != nil {
		return err
	}
	if n != w {
		return errors.New("stata file appears to be truncated")
	}
	rdr.DatasetLabel = string(buf[0:w])

	// </label><timestamp>
	rdr.reader.Seek(19, 1)

	// Time stamp
	err = binary.Read(rdr.reader, rdr.ByteOrder, &n8)
	if err != nil {
		return err
	}
	n, err = rdr.reader.Read(buf[0:n8])
	if err != nil {
		return err
	}
	if n != int(n8) {
		return errors.New("stata file appears to be truncated")
	}
	rdr.TimeStamp = string(buf[0:n8])

	// </timestamp></header><map> + 16 bytes
	rdr.reader.Seek(42, 1)

	// Map
	binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seek_vartypes)
	binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seek_varnames)
	binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seek_sortlist)
	binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seek_formats)
	binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seek_value_label_names)
	binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seek_variable_labels)
	binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seek_characteristics)
	binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seek_data)
	binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seek_strls)
	binary.Read(rdr.reader, rdr.ByteOrder, &rdr.seek_value_labels)

	return nil
}

func (rdr *StataReader) read_vartypes() error {

	var err error

	switch {
	case rdr.FormatVersion == 118:
		err = rdr.read_vartypes_16()
	case rdr.FormatVersion == 117:
		err = rdr.read_vartypes_16()
	case rdr.FormatVersion == 115:
		err = rdr.read_vartypes_8()
	case rdr.FormatVersion == 114:
		err = rdr.read_vartypes_8()
	default:
		err = errors.New(fmt.Sprintf("unknown format version %v in read_vartypes", rdr.FormatVersion))
	}

	return err
}

func (rdr *StataReader) read_vartypes_16() error {
	var err error
	rdr.reader.Seek(rdr.seek_vartypes+16, 0)
	rdr.var_types = make([]int, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		rdr.var_types[k], err = rdr.read_uint(2)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rdr *StataReader) read_vartypes_8() error {
	var err error
	rdr.var_types = make([]int, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		rdr.var_types[k], err = rdr.read_uint(1)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rdr *StataReader) translate_vartypes() {
	for k := 0; k < int(rdr.Nvar); k++ {
		switch {
		// strf
		case rdr.var_types[k] <= 244:
			continue
		case rdr.var_types[k] == 251:
			rdr.var_types[k] = 65530
		case rdr.var_types[k] == 252:
			rdr.var_types[k] = 65529
		case rdr.var_types[k] == 253:
			rdr.var_types[k] = 65528
		case rdr.var_types[k] == 254:
			rdr.var_types[k] = 65527
		case rdr.var_types[k] == 255:
			rdr.var_types[k] = 65526
		default:
			panic("unknown variable type %v in translate_vartypes")
		}
	}
}

func (rdr *StataReader) read_formats() {
	switch {
	case rdr.FormatVersion == 118:
		rdr.do_read_formats(57, true)
	case rdr.FormatVersion == 117:
		rdr.do_read_formats(49, true)
	case rdr.FormatVersion == 115:
		rdr.do_read_formats(49, false)
	case rdr.FormatVersion == 114:
		rdr.do_read_formats(49, false)
	default:
		panic(fmt.Sprintf("unknown format version %v in read_varnames", rdr.FormatVersion))
	}
}

func (rdr *StataReader) do_read_formats(bufsize int, seek bool) {

	buf := make([]byte, bufsize)
	if seek {
		rdr.reader.Seek(rdr.seek_formats+9, 0)
	}
	rdr.Formats = make([]string, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		rdr.reader.Read(buf)
		rdr.Formats[k] = string(partition(buf))
	}

	rdr.is_date = make([]bool, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		if strings.Index(rdr.Formats[k], "%td") == 0 {
			rdr.is_date[k] = true
		} else if strings.Index(rdr.Formats[k], "%tc") == 0 {
			rdr.is_date[k] = true
		}
	}
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

// read_varnames dispatches to the correct function for reading
// variable names for the dta file format.
func (rdr *StataReader) read_varnames() error {
	var err error
	switch {
	case rdr.FormatVersion == 118:
		err = rdr.do_read_varnames(129, true)
	case rdr.FormatVersion == 117:
		err = rdr.do_read_varnames(33, true)
	case rdr.FormatVersion == 115:
		err = rdr.do_read_varnames(33, false)
	case rdr.FormatVersion == 114:
		err = rdr.do_read_varnames(33, false)
	default:
		return errors.New(fmt.Sprintf("unknown format version %v in read_varnames", rdr.FormatVersion))
	}
	return err
}

func (rdr *StataReader) do_read_varnames(bufsize int, seek bool) error {
	buf := make([]byte, bufsize)
	if seek {
		rdr.reader.Seek(rdr.seek_varnames+10, 0)
	}
	rdr.column_names = make([]string, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		n, err := rdr.reader.Read(buf)
		if err != nil {
			return err
		}
		if n != bufsize {
			return errors.New("stata file appears to be truncated")
		}
		rdr.column_names[k] = string(partition(buf))
	}

	return nil
}

func (rdr *StataReader) read_value_label_names() {
	switch {
	case rdr.FormatVersion == 118:
		rdr.do_read_value_label_names(129, true)
	case rdr.FormatVersion == 117:
		rdr.do_read_value_label_names(33, true)
	case rdr.FormatVersion == 116:
		rdr.do_read_value_label_names(33, false)
	case rdr.FormatVersion == 115:
		rdr.do_read_value_label_names(33, false)
	default:
		panic(fmt.Sprintf("unknown format version %v in read_value_label_names", rdr.FormatVersion))
	}
}

func (rdr *StataReader) do_read_value_label_names(bufsize int, seek bool) {
	buf := make([]byte, bufsize)
	if seek {
		rdr.reader.Seek(rdr.seek_value_label_names+19, 0)
	}
	rdr.ValueLabelNames = make([]string, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		rdr.reader.Read(buf)
		rdr.ValueLabelNames[k] = string(partition(buf))
	}
}

func (rdr *StataReader) read_variable_labels() {
	switch {
	case rdr.FormatVersion == 118:
		rdr.do_read_variable_labels(321, true)
	case rdr.FormatVersion == 117:
		rdr.do_read_variable_labels(321, true)
	case rdr.FormatVersion == 115:
		rdr.do_read_variable_labels(81, false)
	case rdr.FormatVersion == 114:
		rdr.do_read_variable_labels(81, false)
	}
}

func (rdr *StataReader) do_read_variable_labels(bufsize int, seek bool) {
	buf := make([]byte, bufsize)
	if seek {
		rdr.reader.Seek(rdr.seek_variable_labels+17, 0)
	}
	rdr.ColumnNamesLong = make([]string, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		rdr.reader.Read(buf)
		rdr.ColumnNamesLong[k] = string(partition(buf))
	}
}

func (rdr *StataReader) read_value_labels() {

	vl := make(map[string]map[int32]string)

	buf := make([]byte, 321)
	rdr.reader.Seek(rdr.seek_value_labels+14, 0)
	var n int32
	var textlen int32
	vlw := value_label_length[rdr.FormatVersion]

	for {
		rdr.reader.Read(buf[0:5])
		if string(buf[0:5]) != "<lbl>" {
			break
		}

		rdr.reader.Seek(4, 1)
		rdr.reader.Read(buf[0:vlw])
		labname := string(partition(buf[0:vlw]))
		rdr.reader.Seek(3, 1)

		binary.Read(rdr.reader, rdr.ByteOrder, &n)
		binary.Read(rdr.reader, rdr.ByteOrder, &textlen)

		off := make([]int32, n)
		val := make([]int32, n)

		for j := int32(0); j < n; j++ {
			binary.Read(rdr.reader, rdr.ByteOrder, &off[j])
		}

		for j := int32(0); j < n; j++ {
			binary.Read(rdr.reader, rdr.ByteOrder, &val[j])
		}

		if cap(buf) < int(textlen) {
			buf = make([]byte, 2*textlen)
		}
		rdr.reader.Read(buf[0:textlen])

		vk := make(map[int32]string)

		for j := int32(0); j < n; j++ {
			vk[val[j]] = string(partition(buf[off[j]:]))
		}
		vl[labname] = vk

		// </lbl>
		rdr.reader.Seek(6, 1)
	}
	rdr.ValueLabels = vl
}

func (rdr *StataReader) read_strls() error {

	rdr.reader.Seek(rdr.seek_strls+7, 0)

	vo := make([]byte, vo_length[rdr.FormatVersion])
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
		if (n <= 0) || (err == io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if string(buf3) != "GSO" {
			break
		}

		binary.Read(rdr.reader, rdr.ByteOrder, vo)
		binary.Read(rdr.reader, rdr.ByteOrder, &t)
		binary.Read(rdr.reader, rdr.ByteOrder, &length)

		if vo_length[rdr.FormatVersion] == 12 {
			copy(vo8[0:2], vo[0:2])
			copy(vo8[2:8], vo[4:10])
		} else {
			copy(vo8, vo)
		}

		var ptr uint64
		binary.Read(bytes.NewReader(vo8), rdr.ByteOrder, &ptr)

		if len(buf) < int(length) {
			buf = make([]byte, 2*length)
		}
		rdr.reader.Read(buf[0:length])

		if t == 130 {
			buf = partition(buf[0:length])
			rdr.Strls[ptr] = string(buf)
		} else if t == 129 {
			rdr.StrlsBytes[ptr] = make([]byte, length)
			copy(rdr.StrlsBytes[ptr], buf[0:length])
		} else {
			return errors.New("unknown t value")
		}
	}
	return nil
}

// Read returns the given number of rows of data from the Stata data
// file.  The data are returned as an array of Series objects.  If
// rows is negative, the remainder of the file is read.
func (rdr *StataReader) Read(rows int) ([]*Series, error) {

	data := make([]interface{}, rdr.Nvar)
	missing := make([][]bool, rdr.Nvar)

	nval := int(rdr.row_count) - rdr.rows_read
	if (rows >= 0) && (nval > rows) {
		nval = rows
	} else if nval <= 0 {
		return nil, nil
	}

	for j := 0; j < int(rdr.Nvar); j++ {
		missing[j] = make([]bool, nval)
	}

	for j, t := range rdr.var_types {
		switch {
		default:
			return nil, errors.New(fmt.Sprintf("unknown variable type: %v", t))
		case t <= 2045:
			data[j] = make([]string, nval)
		case t == 32768:
			if rdr.InsertStrls {
				data[j] = make([]string, nval)
			} else {
				data[j] = make([]uint64, nval)
			}
		case t == 65526:
			data[j] = make([]float64, nval)
		case t == 65527:
			data[j] = make([]float32, nval)
		case t == 65528:
			data[j] = make([]int32, nval)
		case t == 65529:
			data[j] = make([]int16, nval)
		case t == 65530:
			data[j] = make([]int8, nval)
		}
	}

	if rdr.FormatVersion >= 117 {
		rdr.reader.Seek(rdr.seek_data+6, 0)
	}

	buf := make([]byte, 2045)
	buf8 := make([]byte, 8)
	for i := 0; i < nval; i++ {

		rdr.rows_read += 1
		if rdr.rows_read > int(rdr.row_count) {
			break
		}

		for j := 0; j < rdr.Nvar; j++ {

			t := rdr.var_types[j]
			switch {
			case t <= 2045:
				// strf
				rdr.reader.Read(buf[0:t])
				data[j].([]string)[i] = string(partition(buf[0:t]))
			case t == 32768:
				if rdr.InsertStrls {
					// The STRL pointer is 2 byte integer followed by 6 byte integer
					// or 4 + 4 depending on the version
					binary.Read(rdr.reader, rdr.ByteOrder, buf8)
					var ptr uint64
					binary.Read(bytes.NewReader(buf8), rdr.ByteOrder, &ptr)
					data[j].([]string)[i] = rdr.Strls[ptr]
				} else {
					binary.Read(rdr.reader, rdr.ByteOrder, &(data[j].([]uint64)[i]))
				}
			case t == 65526:
				var x float64
				binary.Read(rdr.reader, rdr.ByteOrder, &x)
				data[j].([]float64)[i] = x
				// Lower bound in dta spec is out of range.
				if (x > 8.988e307) || (x < -8.988e307) {
					missing[j][i] = true
				}
			case t == 65527:
				var x float32
				binary.Read(rdr.reader, rdr.ByteOrder, &x)
				data[j].([]float32)[i] = x
				if (x > 1.701e38) || (x < -1.701e38) {
					missing[j][i] = true
				}
			case t == 65528:
				var x int32
				binary.Read(rdr.reader, rdr.ByteOrder, &x)
				data[j].([]int32)[i] = x
				if (x > 2147483620) || (x < -2147483647) {
					missing[j][i] = true
				}
			case t == 65529:
				var x int16
				binary.Read(rdr.reader, rdr.ByteOrder, &x)
				data[j].([]int16)[i] = x
				if (x > 32740) || (x < -32767) {
					missing[j][i] = true
				}
			case t == 65530:
				var x int8
				binary.Read(rdr.reader, rdr.ByteOrder, &x)
				if (x < -127) || (x > 100) {
					missing[j][i] = true
				}
				data[j].([]int8)[i] = x
			}
		}
	}

	if rdr.InsertCategoryLabels {
		for j := 0; j < rdr.Nvar; j++ {
			labname := rdr.ValueLabelNames[j]
			mp, ok := rdr.ValueLabels[labname]
			if !ok {
				continue
			}

			idat, err := cast_to_int(data[j])
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

	if rdr.ConvertDates {
		for j, _ := range data {
			if rdr.is_date[j] {
				data[j] = rdr.do_convert_dates(data[j], rdr.Formats[j])
			}
		}
	}

	// Now that we have the raw data, convert it to a series.
	rdata := make([]*Series, len(data))
	var err error
	for j, v := range data {
		rdata[j], err = NewSeries(rdr.column_names[j], v, missing[j])
		if err != nil {
			return nil, err
		}
	}

	return rdata, nil
}

func (rdr *StataReader) do_convert_dates(v interface{}, format string) interface{} {

	vec, err := upcast_numeric(v)
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
