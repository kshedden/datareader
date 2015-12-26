package datareader

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// A StataReader reads Stata dta data files.  Currently only certain
// versions of Stata dta files can be read.  Not all fields in the
// StataReader struct are applicable to all file formats.
//
// The Read method reads and returns the data.  Several fields of the
// StataReader struct may also be of interest.
//
// Tehnical information about the file format can be found here:
//   http://www.stata.com/help.cgi?dta
type StataReader struct {

	// If true, the strl numerical codes are replaced with their
	// string values when available.
	Insert_strls bool

	// If true, the categorial numerical codes are replaced with
	// their string labels when available.
	Insert_category_labels bool

	// If true, dates are converted to Go date format.
	Convert_dates bool

	// A short text label for the data set.
	Data_label string

	// The time stamp for the data set
	Time_stamp string

	// Number of variables
	Nvar uint16

	// Number of observations
	Nobs uint64

	// Variable types, see technical documentation for meaning
	Vartypes []uint16

	// A name for each variable
	Variable_names []string

	// An additional text entry describing each variable
	Variable_labels []string

	// String labels for categorical variables
	Value_labels      map[string]map[int32]string
	value_label_names []string

	// Format codes for each variable
	Formats []string

	// Maps from strl keys to values
	Strls       map[uint64]string
	Strls_bytes map[uint64][]byte

	// The format version of the dta file
	Format_version uint64

	// The endian-ness of the file
	Byte_order binary.ByteOrder

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
	rdr.Insert_strls = true
	rdr.Insert_category_labels = true
	rdr.Convert_dates = true

	err := rdr.init()
	if err != nil {
		return nil, err
	}
	return rdr, nil
}

func (rdr *StataReader) init() error {

	// Determine if we have <117 or >=117 dta version.
	c := make([]byte, 1)
	rdr.reader.Read(c)
	rdr.reader.Seek(0, 0)

	var err error
	if string(c) == "<" {
		err = rdr.read_new_header()
	} else {
		err = rdr.read_old_header()
	}
	if err != nil {
		return err
	}

	rdr.read_vartypes()

	if rdr.Format_version < 117 {
		rdr.translate_vartypes()
	}

	rdr.read_varnames()

	// Skip over srtlist
	if rdr.Format_version < 117 {
		m := int64(2 * (rdr.Nvar + 1))
		rdr.reader.Seek(m, 1)
	}

	rdr.read_formats()
	rdr.read_value_label_names()
	rdr.read_variable_labels()

	if rdr.Format_version < 117 {
		rdr.read_expansion_fields()
	}

	if rdr.Format_version >= 117 {
		rdr.read_strls()

		// Must be called manually for older format < 117.
		rdr.read_value_labels()
	}

	return nil
}

func (rdr *StataReader) read_expansion_fields() {
	var b byte
	var i int32

	for {
		binary.Read(rdr.reader, rdr.Byte_order, &b)
		binary.Read(rdr.reader, rdr.Byte_order, &i)

		if (b == 0) && (i == 0) {
			break
		}
		rdr.reader.Seek(int64(i), 1)
	}
}

// read_old_header reads the pre version 117 header
func (rdr *StataReader) read_old_header() error {

	buf := make([]byte, 81)

	// Get the format (byte order doesn't matter here)
	var format uint8
	binary.Read(rdr.reader, binary.LittleEndian, &format)

	if (format != uint8(114)) && (format != uint8(115)) {
		return errors.New(fmt.Sprintf("Format %d not implemented", format))
	}
	rdr.Format_version = uint64(format)

	// Get the byte order
	var bo uint8
	binary.Read(rdr.reader, binary.LittleEndian, &bo)
	if bo == 1 {
		rdr.Byte_order = binary.BigEndian
	} else {
		rdr.Byte_order = binary.LittleEndian
	}

	// Skip two bytes
	rdr.reader.Seek(2, 1)

	// Number of variables
	var nvar int16
	binary.Read(rdr.reader, rdr.Byte_order, &nvar)
	rdr.Nvar = uint16(nvar)

	// Number of observations
	var nobs int32
	binary.Read(rdr.reader, rdr.Byte_order, &nobs)
	rdr.Nobs = uint64(nobs)

	// Data label
	rdr.reader.Read(buf[0:81])
	rdr.Data_label = string(partition(buf[0:81]))

	// Time stamp
	rdr.reader.Read(buf[0:18])
	rdr.Time_stamp = string(partition(buf[0:18]))

	return nil
}

// read_new_header reads a new-style xml header (versions 117+).
func (rdr *StataReader) read_new_header() error {

	buf := make([]byte, 500)
	var n8 uint8
	var n16 uint16

	// <stata_dta><header><release>
	rdr.reader.Read(buf[0:28])
	if string(buf[0:11]) != "<stata_dta>" {
		return errors.New("Invalid Stata file")
	}

	// Stata file version
	rdr.reader.Read(buf[0:3])
	rdr.Format_version, _ = strconv.ParseUint(string(buf[0:3]), 0, 64)
	if rdr.Format_version != 118 {
		return errors.New("Invalid Stata dta format version (must be 118)")
	}

	// </release><byteorder>
	rdr.reader.Seek(21, 1)

	// Byte order
	rdr.reader.Read(buf[0:3])
	if string(buf[0:3]) == "MSF" {
		rdr.Byte_order = binary.BigEndian
	} else {
		rdr.Byte_order = binary.LittleEndian
	}

	// </byteorder><K>
	rdr.reader.Seek(15, 1)

	// Number of variables
	binary.Read(rdr.reader, rdr.Byte_order, &rdr.Nvar)

	// </K><N>
	rdr.reader.Seek(7, 1)

	// Number of observations
	binary.Read(rdr.reader, rdr.Byte_order, &rdr.Nobs)

	// </N><label>
	rdr.reader.Seek(11, 1)

	// Data set label
	binary.Read(rdr.reader, rdr.Byte_order, &n16)
	rdr.reader.Read(buf[0:n16])
	rdr.Data_label = string(buf[0:n16])

	// </label><timestamp>
	rdr.reader.Seek(19, 1)

	// Time stamp
	binary.Read(rdr.reader, rdr.Byte_order, &n8)
	rdr.reader.Read(buf[0:n8])
	rdr.Time_stamp = string(buf[0:n8])

	// </timestamp></header><map> + 16 bytes
	rdr.reader.Seek(42, 1)

	// Map
	binary.Read(rdr.reader, rdr.Byte_order, &rdr.seek_vartypes)
	binary.Read(rdr.reader, rdr.Byte_order, &rdr.seek_varnames)
	binary.Read(rdr.reader, rdr.Byte_order, &rdr.seek_sortlist)
	binary.Read(rdr.reader, rdr.Byte_order, &rdr.seek_formats)
	binary.Read(rdr.reader, rdr.Byte_order, &rdr.seek_value_label_names)
	binary.Read(rdr.reader, rdr.Byte_order, &rdr.seek_variable_labels)
	binary.Read(rdr.reader, rdr.Byte_order, &rdr.seek_characteristics)
	binary.Read(rdr.reader, rdr.Byte_order, &rdr.seek_data)
	binary.Read(rdr.reader, rdr.Byte_order, &rdr.seek_strls)
	binary.Read(rdr.reader, rdr.Byte_order, &rdr.seek_value_labels)

	return nil
}

func (rdr *StataReader) read_vartypes() {
	switch {
	case rdr.Format_version == 118:
		rdr.read_vartypes_16()
	case rdr.Format_version == 117:
		rdr.read_vartypes_16()
	case rdr.Format_version == 115:
		rdr.read_vartypes_8()
	case rdr.Format_version == 114:
		rdr.read_vartypes_8()
	default:
		panic(fmt.Sprintf("unknown format version %v in read_vartypes", rdr.Format_version))
	}
}

func (rdr *StataReader) read_vartypes_16() {
	rdr.reader.Seek(rdr.seek_vartypes+16, 0)
	rdr.Vartypes = make([]uint16, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		binary.Read(rdr.reader, rdr.Byte_order, &rdr.Vartypes[k])
	}
}

func (rdr *StataReader) read_vartypes_8() {
	rdr.Vartypes = make([]uint16, rdr.Nvar)
	b := make([]byte, 1)
	for k := 0; k < int(rdr.Nvar); k++ {
		binary.Read(rdr.reader, rdr.Byte_order, &b)
		rdr.Vartypes[k] = uint16(b[0])
	}
}

func (rdr *StataReader) translate_vartypes() {
	for k := 0; k < int(rdr.Nvar); k++ {
		switch {
		// strf
		case rdr.Vartypes[k] <= 244:
			continue
		case rdr.Vartypes[k] == 251:
			rdr.Vartypes[k] = 65530
		case rdr.Vartypes[k] == 252:
			rdr.Vartypes[k] = 65529
		case rdr.Vartypes[k] == 253:
			rdr.Vartypes[k] = 65528
		case rdr.Vartypes[k] == 254:
			rdr.Vartypes[k] = 65527
		case rdr.Vartypes[k] == 255:
			rdr.Vartypes[k] = 65526
		default:
			panic("unknown variable type %v in translate_vartypes")
		}
	}
}

func (rdr *StataReader) read_formats() {
	switch {
	case rdr.Format_version == 118:
		rdr.do_read_formats(57, true)
	case rdr.Format_version == 117:
		rdr.do_read_formats(57, true)
	case rdr.Format_version == 115:
		rdr.do_read_formats(49, false)
	case rdr.Format_version == 114:
		rdr.do_read_formats(49, false)
	default:
		panic(fmt.Sprintf("unknown format version %v in read_varnames", rdr.Format_version))
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
func (rdr *StataReader) read_varnames() {
	switch {
	case rdr.Format_version == 118:
		rdr.do_read_varnames(129, true)
	case rdr.Format_version == 117:
		rdr.do_read_varnames(129, true)
	case rdr.Format_version == 115:
		rdr.do_read_varnames(33, false)
	case rdr.Format_version == 114:
		rdr.do_read_varnames(33, false)
	default:
		panic(fmt.Sprintf("unknown format version %v in read_varnames", rdr.Format_version))
	}
}

func (rdr *StataReader) do_read_varnames(bufsize int, seek bool) {
	buf := make([]byte, bufsize)
	if seek {
		rdr.reader.Seek(rdr.seek_varnames+10, 0)
	}
	rdr.Variable_names = make([]string, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		rdr.reader.Read(buf)
		rdr.Variable_names[k] = string(partition(buf))
	}
}

func (rdr *StataReader) read_value_label_names() {
	switch {
	case rdr.Format_version == 118:
		rdr.do_read_value_label_names(129, true)
	case rdr.Format_version == 117:
		rdr.do_read_value_label_names(129, true)
	case rdr.Format_version == 116:
		rdr.do_read_value_label_names(33, false)
	case rdr.Format_version == 115:
		rdr.do_read_value_label_names(33, false)
	default:
		panic(fmt.Sprintf("unknown format version %v in read_value_label_names", rdr.Format_version))
	}
}

func (rdr *StataReader) do_read_value_label_names(bufsize int, seek bool) {
	buf := make([]byte, bufsize)
	if seek {
		rdr.reader.Seek(rdr.seek_value_label_names+19, 0)
	}
	rdr.value_label_names = make([]string, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		rdr.reader.Read(buf)
		rdr.value_label_names[k] = string(partition(buf))
	}
}

func (rdr *StataReader) read_variable_labels() {
	switch {
	case rdr.Format_version == 118:
		rdr.do_read_variable_labels(321, true)
	case rdr.Format_version == 117:
		rdr.do_read_variable_labels(321, true)
	case rdr.Format_version == 115:
		rdr.do_read_variable_labels(81, false)
	case rdr.Format_version == 114:
		rdr.do_read_variable_labels(81, false)
	}
}

func (rdr *StataReader) do_read_variable_labels(bufsize int, seek bool) {
	buf := make([]byte, bufsize)
	if seek {
		rdr.reader.Seek(rdr.seek_variable_labels+17, 0)
	}
	rdr.Variable_labels = make([]string, rdr.Nvar)
	for k := 0; k < int(rdr.Nvar); k++ {
		rdr.reader.Read(buf)
		rdr.Variable_labels[k] = string(partition(buf))
	}
}

func (rdr *StataReader) read_value_labels() {

	vl := make(map[string]map[int32]string)

	buf := make([]byte, 321)
	rdr.reader.Seek(rdr.seek_value_labels+14, 0)
	var n int32
	var textlen int32

	for {
		rdr.reader.Read(buf[0:5])
		if string(buf[0:5]) != "<lbl>" {
			break
		}

		rdr.reader.Seek(4, 1)
		rdr.reader.Read(buf[0:129])
		labname := string(partition(buf[0:129]))
		rdr.reader.Seek(3, 1)

		binary.Read(rdr.reader, rdr.Byte_order, &n)
		binary.Read(rdr.reader, rdr.Byte_order, &textlen)

		off := make([]int32, n)
		val := make([]int32, n)

		for j := int32(0); j < n; j++ {
			binary.Read(rdr.reader, rdr.Byte_order, &off[j])
		}

		for j := int32(0); j < n; j++ {
			binary.Read(rdr.reader, rdr.Byte_order, &val[j])
		}

		buf = make([]byte, textlen)
		rdr.reader.Read(buf)

		vk := make(map[int32]string)

		for j := int32(0); j < n; j++ {
			vk[val[j]] = string(partition(buf[off[j]:]))
		}
		vl[labname] = vk

		// </lbl>
		rdr.reader.Seek(6, 1)
	}
	rdr.Value_labels = vl
}

func (rdr *StataReader) read_strls() error {

	rdr.reader.Seek(rdr.seek_strls+7, 0)

	var v uint32
	var o uint64
	var t uint8
	var length uint32

	rdr.Strls = make(map[uint64]string)
	rdr.Strls_bytes = make(map[uint64][]byte)

	rdr.Strls[0] = ""

	buf3 := make([]byte, 3)

	for {
		rdr.reader.Read(buf3)
		if string(buf3) != "GSO" {
			break
		}

		binary.Read(rdr.reader, rdr.Byte_order, &v)
		binary.Read(rdr.reader, rdr.Byte_order, &o)
		binary.Read(rdr.reader, rdr.Byte_order, &t)
		binary.Read(rdr.reader, rdr.Byte_order, &length)

		// This is intended to create an 8-byte key that
		// matches the keys found in the actual data.  We then
		// insert the strls into the data set by key.
		var ky uint64
		ky = uint64(v) | (o << 16)

		buf := make([]byte, length)
		rdr.reader.Read(buf)

		if t == 130 {
			buf = partition(buf)
			rdr.Strls[ky] = string(buf)
		} else if t == 129 {
			rdr.Strls_bytes[ky] = buf
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

	nval := int(rdr.Nobs) - rdr.rows_read
	if (rows >= 0) && (nval > rows) {
		nval = rows
	}

	for j := 0; j < int(rdr.Nvar); j++ {
		missing[j] = make([]bool, nval)
	}

	for j, t := range rdr.Vartypes {
		switch {
		case t <= 2045:
			data[j] = make([]string, nval)
		case t == 32768:
			if rdr.Insert_strls {
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
			if rdr.Insert_category_labels {
				data[j] = make([]string, nval)
			} else {
				data[j] = make([]int8, nval)
			}
		}
	}

	if rdr.Format_version >= 117 {
		rdr.reader.Seek(rdr.seek_data+6, 0)
	}

	buf := make([]byte, 2045)
	for i := 0; i < nval; i++ {

		rdr.rows_read += 1
		if rdr.rows_read > int(rdr.Nobs) {
			break
		}

		for j := uint16(0); j < rdr.Nvar; j++ {

			t := rdr.Vartypes[j]
			switch {
			case t <= 2045:
				// strf
				rdr.reader.Read(buf[0:t])
				data[j].([]string)[i] = string(partition(buf[0:t]))
			case t == 32768:
				if rdr.Insert_strls {
					var ptr uint64
					binary.Read(rdr.reader, rdr.Byte_order, &ptr)
					data[j].([]string)[i] = rdr.Strls[ptr]
				} else {
					binary.Read(rdr.reader, rdr.Byte_order, &(data[j].([]uint64)[i]))
				}
			case t == 65526:
				var x float64
				binary.Read(rdr.reader, rdr.Byte_order, &x)
				data[j].([]float64)[i] = x
				// Lower bound in dta spec is out of range.
				if (x > 8.988e307) || (x < -8.988e307) {
					missing[j][i] = true
				}
			case t == 65527:
				var x float32
				binary.Read(rdr.reader, rdr.Byte_order, &x)
				data[j].([]float32)[i] = x
				if (x > 1.701e38) || (x < -1.701e38) {
					missing[j][i] = true
				}
			case t == 65528:
				var x int32
				binary.Read(rdr.reader, rdr.Byte_order, &x)
				data[j].([]int32)[i] = x
				if (x > 2147483620) || (x < -2147483647) {
					missing[j][i] = true
				}
			case t == 65529:
				var x int16
				binary.Read(rdr.reader, rdr.Byte_order, &x)
				data[j].([]int16)[i] = x
				if (x > 32740) || (x < -32767) {
					missing[j][i] = true
				}
			case t == 65530:
				var x int8
				binary.Read(rdr.reader, rdr.Byte_order, &x)
				if (x < -127) || (x > 100) {
					missing[j][i] = true
				}
				if !rdr.Insert_category_labels {
					data[j].([]int8)[i] = x
				} else {
					// bytes are converted to categorical.
					// We attempt to replace the value
					// with its appropriate category
					// label.  If this is not possible, we
					// just convert the byte value to a
					// string.

					// Check to see if we have label information
					labname := rdr.value_label_names[j]
					mp, ok := rdr.Value_labels[labname]
					if !ok {
						data[j].([]string)[i] = fmt.Sprintf("%v", x)
						continue
					}

					// Check to see if we have a label for this category
					v, ok := mp[int32(x)]
					if ok {
						data[j].([]string)[i] = v
					} else {
						data[j].([]string)[i] = fmt.Sprintf("%v", x)
					}
				}
			}
		}
	}

	if rdr.Convert_dates {
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
		rdata[j], err = NewSeries(rdr.Variable_names[j], v, missing[j])
		if err != nil {
			return nil, err
		}
	}

	return rdata, nil
}

func (rdr *StataReader) do_convert_dates(v interface{}, format string) interface{} {

	vec, ok := v.([]int32)
	if !ok {
		panic("unable to handle raw type in date vector")
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
