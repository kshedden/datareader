package datareader

// Read SAS7BDAT files with go.
//
// This code is based on the Python module:
// https://pypi.python.org/pypi/sas7bdat
//
// See also:
// https://cran.r-project.org/web/packages/sas7bdat/vignettes/sas7bdat.pdf
//
// Binary data compression:
// http://collaboration.cmc.ec.gc.ca/science/rpn/biblio/ddj/Website/articles/CUJ/1992/9210/ross/ross.htm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	xencoding "golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

// SAS7BDAT represents a SAS data file in SAS7BDAT format.
type SAS7BDAT struct {
	// Data types of the columns
	column_types []int

	// Formats for the columns
	ColumnFormats []string

	// If true, trim whitespace from right of each string variable
	// (SAS7BDAT strings are fixed width)
	TrimStrings bool

	// If true, converts some date formats to Go date values (does
	// not work for all SAS date formats)
	ConvertDates bool

	// The creation date of the file
	DateCreated time.Time

	// The modification date of the file
	DateModified time.Time

	// The number of rows in the file
	row_count int

	// The name of the data set
	Name string

	// The platform used to create the file
	Platform string

	// The SAS release used to create the file
	SASRelease string

	// The server type used to create the file
	ServerType string

	// The operating system type used to create the file
	OSType string

	// The operating system name used to create the file
	OSName string

	// The SAS file type
	FileType string

	// True if the file was created on a 64 bit architecture
	U64 bool

	// The byte order of the file
	ByteOrder binary.ByteOrder

	// The compression mode of the file
	Compression string

	encoding                             string
	textdecoder                          *xencoding.Decoder
	column_names                         []string
	path                                 string
	buf                                  []byte
	align_correction                     int
	file                                 io.ReadSeeker
	cached_page                          []byte
	current_page_type                    int
	current_page_block_count             int
	current_page_subheaders_count        int
	current_row_in_file_index            int
	current_row_on_page_index            int
	current_page_data_subheader_pointers []*subheaderPointer
	stringchunk                          [][]string
	bytechunk                            [][]byte
	current_row_in_chunk_index           int
	column_names_strings                 []string
	column_data_offsets                  []int
	column_data_lengths                  []int
	columns                              []*Column
	properties                           *sasProperties
}

// These values don't change after the header is read.
type sasProperties struct {
	int_length               int
	page_bit_offset          int
	subheader_pointer_length int
	header_length            int
	page_length              int
	page_count               int
	row_length               int
	col_count_p1             int
	col_count_p2             int
	mix_page_row_count       int
	lcs                      int
	lcp                      int
	creator                  string
	creator_proc             string
	column_count             int
}

type Column struct {
	col_id int
	name   string
	label  string
	format string
	ctype  int
	length int
}

type subheaderPointer struct {
	offset      int
	length      int
	compression int
	ptype       int
}

const (
	rowSizeIndex          = iota
	columnSizeIndex       = iota
	subheaderCountsIndex  = iota
	columnTextIndex       = iota
	columnNameIndex       = iota
	columnAttributesIndex = iota
	formatAndLabelIndex   = iota
	columnListIndex       = iota
	dataSubheaderIndex    = iota
)

const (
	number_column_type = iota
	string_column_type = iota
)

// Subheader signatures, 32 and 64 bit, little and big endian
var subheader_signature_to_index = map[string]int{
	"\xF7\xF7\xF7\xF7":                 rowSizeIndex,
	"\x00\x00\x00\x00\xF7\xF7\xF7\xF7": rowSizeIndex,
	"\xF7\xF7\xF7\xF7\x00\x00\x00\x00": rowSizeIndex,
	"\xF7\xF7\xF7\xF7\xFF\xFF\xFB\xFE": rowSizeIndex,
	"\xF6\xF6\xF6\xF6":                 columnSizeIndex,
	"\x00\x00\x00\x00\xF6\xF6\xF6\xF6": columnSizeIndex,
	"\xF6\xF6\xF6\xF6\x00\x00\x00\x00": columnSizeIndex,
	"\xF6\xF6\xF6\xF6\xFF\xFF\xFB\xFE": columnSizeIndex,
	"\x00\xFC\xFF\xFF":                 subheaderCountsIndex,
	"\xFF\xFF\xFC\x00":                 subheaderCountsIndex,
	"\x00\xFC\xFF\xFF\xFF\xFF\xFF\xFF": subheaderCountsIndex,
	"\xFF\xFF\xFF\xFF\xFF\xFF\xFC\x00": subheaderCountsIndex,
	"\xFD\xFF\xFF\xFF":                 columnTextIndex,
	"\xFF\xFF\xFF\xFD":                 columnTextIndex,
	"\xFD\xFF\xFF\xFF\xFF\xFF\xFF\xFF": columnTextIndex,
	"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFD": columnTextIndex,
	"\xFF\xFF\xFF\xFF":                 columnNameIndex,
	"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF": columnNameIndex,
	"\xFC\xFF\xFF\xFF":                 columnAttributesIndex,
	"\xFF\xFF\xFF\xFC":                 columnAttributesIndex,
	"\xFC\xFF\xFF\xFF\xFF\xFF\xFF\xFF": columnAttributesIndex,
	"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFC": columnAttributesIndex,
	"\xFE\xFB\xFF\xFF":                 formatAndLabelIndex,
	"\xFF\xFF\xFB\xFE":                 formatAndLabelIndex,
	"\xFE\xFB\xFF\xFF\xFF\xFF\xFF\xFF": formatAndLabelIndex,
	"\xFF\xFF\xFF\xFF\xFF\xFF\xFB\xFE": formatAndLabelIndex,
	"\xFE\xFF\xFF\xFF":                 columnListIndex,
	"\xFF\xFF\xFF\xFE":                 columnListIndex,
	"\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF": columnListIndex,
	"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE": columnListIndex,
}

const (
	magic = ("\x00\x00\x00\x00\x00\x00\x00\x00" +
		"\x00\x00\x00\x00\xc2\xea\x81\x60" +
		"\xb3\x14\x11\xcf\xbd\x92\x08\x00" +
		"\x09\xc7\x31\x8c\x18\x1f\x10\x11")
	align_1_checker_value                     = '3'
	align_1_offset                            = 32
	align_1_length                            = 1
	align_1_value                             = 4
	u64_byte_checker_value                    = '3'
	align_2_offset                            = 35
	align_2_length                            = 1
	align_2_value                             = 4
	endianness_offset                         = 37
	endianness_length                         = 1
	platform_offset                           = 39
	platform_length                           = 1
	dataset_offset                            = 92
	dataset_length                            = 64
	file_type_offset                          = 156
	file_type_length                          = 8
	date_created_offset                       = 164
	date_created_length                       = 8
	date_modified_offset                      = 172
	date_modified_length                      = 8
	header_size_offset                        = 196
	header_size_length                        = 4
	page_size_offset                          = 200
	page_size_length                          = 4
	page_count_offset                         = 204
	page_count_length                         = 4
	sas_release_offset                        = 216
	sas_release_length                        = 8
	sas_server_type_offset                    = 224
	sas_server_type_length                    = 16
	os_version_number_offset                  = 240
	os_version_number_length                  = 16
	os_maker_offset                           = 256
	os_maker_length                           = 16
	os_name_offset                            = 272
	os_name_length                            = 16
	page_bit_offset_x86                       = 16
	page_bit_offset_x64                       = 32
	subheader_pointer_length_x86              = 12
	subheader_pointer_length_x64              = 24
	page_type_offset                          = 0
	page_type_length                          = 2
	block_count_offset                        = 2
	block_count_length                        = 2
	subheader_count_offset                    = 4
	subheader_count_length                    = 2
	page_meta_type                            = 0
	page_data_type                            = 256
	page_amd_type                             = 1024
	page_metc_type                            = 16384
	page_comp_type                            = -28672
	subheader_pointers_offset                 = 8
	truncated_subheader_id                    = 1
	compressed_subheader_id                   = 4
	compressed_subheader_type                 = 1
	text_block_size_length                    = 2
	row_length_offset_multiplier              = 5
	row_count_offset_multiplier               = 6
	col_count_p1_multiplier                   = 9
	col_count_p2_multiplier                   = 10
	row_count_on_mix_page_offset_multiplier   = 15
	column_name_pointer_length                = 8
	column_name_text_subheader_offset         = 0
	column_name_text_subheader_length         = 2
	column_name_offset_offset                 = 2
	column_name_offset_length                 = 2
	column_name_length_offset                 = 4
	column_name_length_length                 = 2
	column_data_offset_offset                 = 8
	column_data_length_offset                 = 8
	column_data_length_length                 = 4
	column_type_offset                        = 14
	column_type_length                        = 1
	column_format_text_subheader_index_offset = 22
	column_format_text_subheader_index_length = 2
	column_format_offset_offset               = 24
	column_format_offset_length               = 2
	column_format_length_offset               = 26
	column_format_length_length               = 2
	column_label_text_subheader_index_offset  = 28
	column_label_text_subheader_index_length  = 2
	column_label_offset_offset                = 30
	column_label_offset_length                = 2
	column_label_length_offset                = 32
	column_label_length_length                = 2
	rle_compression                           = "SASYZCRL"
	rdc_compression                           = "SASYZCR2"
)

var compression_literals = []string{rle_compression, rdc_compression}

// ensure_buf_size enlarges the data buffer if needed to accommodate
// at least m bytes of data.
func (sas *SAS7BDAT) ensure_buf_size(m int) {
	if cap(sas.buf) < m {
		sas.buf = make([]byte, 2*m)
	}
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// rle_decompress decompresses data using the Run Length Encoding
// algorithm.  It is partially documented here:
//
// https://cran.r-project.org/web/packages/sas7bdat/vignettes/sas7bdat.pdf
func rle_decompress(result_length int, inbuff []byte) ([]byte, error) {

	result := make([]byte, 0, result_length)
	for len(inbuff) > 0 {
		control_byte := inbuff[0] & 0xF0
		end_of_first_byte := int(inbuff[0] & 0x0F)

		inbuff = inbuff[1:]
		if control_byte == 0x00 {
			if end_of_first_byte != 0 {
				os.Stderr.WriteString("Unexpected non-zero end_of_first_byte\n")
			}
			nbytes := int(inbuff[0]) + 64
			inbuff = inbuff[1:]
			result = append(result, inbuff[0:nbytes]...)
			inbuff = inbuff[nbytes:]
		} else if control_byte == 0x40 {
			// not documented
			nbytes := end_of_first_byte * 16
			nbytes += int(inbuff[0])
			inbuff = inbuff[1:]
			for k := 0; k < nbytes; k++ {
				result = append(result, inbuff[0])
			}
			inbuff = inbuff[1:]
		} else if control_byte == 0x60 {
			nbytes := end_of_first_byte*256 + int(inbuff[0]) + 17
			inbuff = inbuff[1:]
			for k := 0; k < nbytes; k++ {
				result = append(result, 0x20)
			}
		} else if control_byte == 0x70 {
			nbytes := end_of_first_byte*256 + int(inbuff[0]) + 17
			inbuff = inbuff[1:]
			for k := 0; k < nbytes; k++ {
				result = append(result, 0x00)
			}
		} else if control_byte == 0x80 {
			nbytes := end_of_first_byte + 1
			result = append(result, inbuff[0:nbytes]...)
			inbuff = inbuff[nbytes:]
		} else if control_byte == 0x90 {
			nbytes := end_of_first_byte + 17
			result = append(result, inbuff[0:nbytes]...)
			inbuff = inbuff[nbytes:]
		} else if control_byte == 0xA0 {
			nbytes := end_of_first_byte + 33
			result = append(result, inbuff[0:nbytes]...)
			inbuff = inbuff[nbytes:]
		} else if control_byte == 0xB0 {
			nbytes := end_of_first_byte + 49
			result = append(result, inbuff[0:nbytes]...)
			inbuff = inbuff[nbytes:]
		} else if control_byte == 0xC0 {
			nbytes := end_of_first_byte + 3
			x := inbuff[0]
			inbuff = inbuff[1:]
			for k := 0; k < nbytes; k++ {
				result = append(result, x)
			}
		} else if control_byte == 0xD0 {
			nbytes := end_of_first_byte + 2
			for k := 0; k < nbytes; k++ {
				result = append(result, 0x40)
			}
		} else if control_byte == 0xE0 {
			nbytes := end_of_first_byte + 2
			for k := 0; k < nbytes; k++ {
				result = append(result, 0x20)
			}
		} else if control_byte == 0xF0 {
			nbytes := end_of_first_byte + 2
			for k := 0; k < nbytes; k++ {
				result = append(result, 0x00)
			}
		} else {
			return nil, fmt.Errorf("unknown control byte: %v", control_byte)
		}
	}

	if len(result) != result_length {
		os.Stderr.WriteString(fmt.Sprintf("RLE: %v != %v\n", len(result), result_length))
	}

	return result, nil
}

// rdc_decompress decompresses data using the Ross Data Compression algorithm:
//
// http://collaboration.cmc.ec.gc.ca/science/rpn/biblio/ddj/Website/articles/CUJ/1992/9210/ross/ross.htm
func rdc_decompress(result_length int, inbuff []byte) ([]byte, error) {

	var ctrl_bits uint16
	var ctrl_mask uint16
	var cmd uint8
	var ofs uint16
	var cnt uint16
	var inbuff_pos int
	outbuff := make([]byte, 0, result_length)

	for inbuff_pos < len(inbuff) {
		ctrl_mask = ctrl_mask >> 1
		if ctrl_mask == 0 {
			ctrl_bits = uint16(inbuff[inbuff_pos])<<8 + uint16(inbuff[inbuff_pos+1])
			inbuff_pos += 2
			ctrl_mask = 0x8000
		}

		if (ctrl_bits & ctrl_mask) == 0 {
			outbuff = append(outbuff, inbuff[inbuff_pos])
			inbuff_pos++
			continue
		}

		cmd = (inbuff[inbuff_pos] >> 4) & 0x0F
		cnt = uint16(inbuff[inbuff_pos] & 0x0F)
		inbuff_pos++

		switch {
		case cmd == 0: /* short rle */
			cnt += 3
			for k := 0; k < int(cnt); k++ {
				outbuff = append(outbuff, inbuff[inbuff_pos])
			}
			inbuff_pos++
		case cmd == 1: /* long /rle */
			cnt += uint16(inbuff[inbuff_pos]) << 4
			cnt += 19
			inbuff_pos++
			for k := 0; k < int(cnt); k++ {
				outbuff = append(outbuff, inbuff[inbuff_pos])
			}
			inbuff_pos++
		case cmd == 2: /* long pattern */
			ofs := cnt + 3
			ofs += uint16(inbuff[inbuff_pos]) << 4
			inbuff_pos++
			cnt = uint16(inbuff[inbuff_pos])
			inbuff_pos++
			cnt += 16
			tmp := outbuff[len(outbuff)-int(ofs) : len(outbuff)-int(ofs)+int(cnt)]
			outbuff = append(outbuff, tmp...)
		case (cmd >= 3) && (cmd <= 15): /* short pattern */
			ofs = cnt + 3
			ofs += uint16(inbuff[inbuff_pos]) << 4
			inbuff_pos++
			tmp := outbuff[len(outbuff)-int(ofs) : len(outbuff)-int(ofs)+int(cmd)]
			outbuff = append(outbuff, tmp...)
		default:
			return nil, fmt.Errorf("unknown RDC command")
		}
	}

	if len(outbuff) != result_length {
		os.Stderr.WriteString(fmt.Sprintf("RDC: %v != %v\n", len(outbuff), result_length))
	}

	return outbuff, nil
}

func (sas *SAS7BDAT) get_decompressor() func(int, []byte) ([]byte, error) {
	switch sas.Compression {
	default:
		return nil
	case rle_compression:
		return rle_decompress
	case rdc_compression:
		return rdc_decompress
	}
}

// NewSAS7BDATReader returns a new reader object for SAS7BDAT files.
// Call the Read method to obtain the data.
func NewSAS7BDATReader(r io.ReadSeeker) (*SAS7BDAT, error) {

	sas := new(SAS7BDAT)
	sas.file = r
	err := sas.getProperties()
	if err != nil {
		return nil, err
	}
	sas.cached_page = make([]byte, sas.properties.page_length)
	err = sas.parse_metadata()
	if err != nil {
		return nil, err
	}

	sas.textdecoder = charmap.Windows1250.NewDecoder()

	return sas, nil
}

// read_bytes read length bytes from the given offset in the current
// page (or from the beginning of the file if no page has yet been
// read).
func (sas *SAS7BDAT) read_bytes(offset, length int) error {
	sas.ensure_buf_size(length)

	if sas.cached_page == nil {
		sas.file.Seek(int64(offset), 0)
		n, err := sas.file.Read(sas.buf[0:length])
		if err != nil {
			return err
		} else if n < length {
			return fmt.Errorf("Unable to read %d bytes from file position %d.", length, offset)
		}
	} else {
		if offset+length > len(sas.cached_page) {
			return fmt.Errorf("The cached page is too small.")
		}
		copy(sas.buf, sas.cached_page[offset:offset+length])
	}
	return nil
}

func (sas *SAS7BDAT) read_float(offset, width int) (float64, error) {
	r := bytes.NewReader(sas.buf[offset : offset+width])
	var x float64
	switch width {
	default:
		return 0, fmt.Errorf("unknown float width")
	case 8:
		err := binary.Read(r, sas.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
	}
	return x, nil
}

// Read an integer of 1, 2, 4 or 8 byte width from the supplied bytes.
func (sas *SAS7BDAT) read_int_from_buffer(buf []byte, width int) (int, error) {

	r := bytes.NewReader(buf[0:width])
	switch width {
	default:
		return 0, fmt.Errorf("invalid integer width")
	case 1:
		var x int8
		err := binary.Read(r, sas.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	case 2:
		var x int16
		err := binary.Read(r, sas.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	case 4:
		var x int32
		err := binary.Read(r, sas.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	case 8:
		var x int64
		err := binary.Read(r, sas.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
		return int(x), nil
	}
}

// Read an integer of 1, 2, 4 or 8 byte width from a given offset in
// the current page (or from the beginning of the file if no page has
// yet been read), then return it as an int.
func (sas *SAS7BDAT) read_int(offset, width int) (int, error) {

	err := sas.read_bytes(offset, width)
	if err != nil {
		return 0, err
	}

	x, err := sas.read_int_from_buffer(sas.buf[0:width], width)
	if err != nil {
		return 0, err
	}
	return x, nil
}

// Read returns up to num_rows rows of data from the SAS7BDAT file, as
// an array of Series objects.  The Series data types are either
// float64 or string.  If num_rows is negative, the remainder of the
// file is read.
//
// SAS strings variables have a fixed width.  By default, right
// whitespace is trimmed from each string, but this can be turned off
// by setting the TrimRight field in the SAS7BDAT struct.
func (sas *SAS7BDAT) Read(num_rows int) ([]*Series, error) {

	if num_rows < 0 {
		num_rows = sas.row_count - sas.current_row_in_file_index
	}

	if sas.current_row_in_file_index >= sas.row_count {
		return nil, nil
	}

	if sas.bytechunk == nil {
		sas.bytechunk = make([][]byte, sas.properties.column_count)
		sas.stringchunk = make([][]string, sas.properties.column_count)
		for j := 0; j < sas.properties.column_count; j++ {
			switch sas.column_types[j] {
			default:
				return nil, fmt.Errorf("unknown column type")
			case number_column_type:
				sas.bytechunk[j] = make([]byte, 8*num_rows)
			case string_column_type:
				sas.stringchunk[j] = make([]string, num_rows)
			}
		}
	}

	sas.current_row_in_chunk_index = 0
	for i := 0; i < num_rows; i++ {
		err, done := sas.readline()
		if err != nil {
			return nil, err
		} else if done {
			break
		}
	}

	rslt := sas.chunk_to_series()

	return rslt, nil
}

func (sas *SAS7BDAT) chunk_to_series() []*Series {

	rslt := make([]*Series, sas.properties.column_count)
	n := sas.current_row_in_chunk_index

	for j := 0; j < sas.properties.column_count; j++ {

		name := sas.column_names[j]
		miss := make([]bool, n)

		switch sas.column_types[j] {
		default:
			panic("Unknown column type")
		case number_column_type:
			vec := make([]float64, n)
			buf := bytes.NewReader(sas.bytechunk[j][0 : 8*n])
			binary.Read(buf, sas.ByteOrder, &vec)
			for i := 0; i < n; i++ {
				if math.IsNaN(vec[i]) {
					miss[i] = true
				}
			}
			if sas.ConvertDates && (sas.ColumnFormats[j] == "MMDDYY") {
				tvec := to_date(vec)
				rslt[j], _ = NewSeries(name, tvec, miss)
			} else {
				rslt[j], _ = NewSeries(name, vec, miss)
			}
		case string_column_type:
			if sas.TrimStrings {
				sas.trim_strings(n, j)
			}
			//sas.decode_strings(n, j)
			rslt[j], _ = NewSeries(name, sas.stringchunk[j][0:n], miss)
		}
	}

	return rslt
}

func to_date(x []float64) []time.Time {

	rslt := make([]time.Time, len(x))

	base := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)

	for j, v := range x {
		rslt[j] = base.Add(time.Hour * time.Duration(24*v))
	}

	return rslt
}

func (sas *SAS7BDAT) trim_strings(n, j int) {
	for i := 0; i < n; i++ {
		bs := []byte(sas.stringchunk[j][i])
		newbs := make([]byte, 0)
		for {
			r, size := utf8.DecodeRune(bs)
			if r == utf8.RuneError || r == '\u0000' || r == '\u0020' {
				break
			}
			newbs = append(newbs, bs[0:size]...)
			bs = bs[size:]
		}
		sas.stringchunk[j][i] = string(newbs)
	}
}

func (sas *SAS7BDAT) decode_strings(n, j int) {
	for i := 0; i < n; i++ {
		s, err := sas.textdecoder.String(sas.stringchunk[j][i])
		if err != nil {
			panic(err)
		}
		sas.stringchunk[j][i] = s
	}
}

func (sas *SAS7BDAT) readline() (error, bool) {

	bit_offset := sas.properties.page_bit_offset
	subheader_pointer_length := sas.properties.subheader_pointer_length

	// If there is no page, go to the end of the header and read a page.
	if sas.cached_page == nil {
		sas.file.Seek(int64(sas.properties.header_length), 0)
		err, done := sas.read_next_page()
		if err != nil {
			return err, false
		} else if done {
			return nil, true
		}
	}

	// Loop until a data row is read
	for {
		if sas.current_page_type == page_meta_type {
			if sas.current_row_on_page_index >= len(sas.current_page_data_subheader_pointers) {
				err, done := sas.read_next_page()
				if err != nil {
					return err, false
				} else if done {
					return nil, true
				}
				sas.current_row_on_page_index = 0
				continue
			}
			current_subheader_pointer :=
				sas.current_page_data_subheader_pointers[sas.current_row_on_page_index]
			err := sas.process_byte_array_with_data(current_subheader_pointer.offset, current_subheader_pointer.length)
			if err != nil {
				return err, false
			}
			return nil, false
		} else if sas.is_page_mix_type(sas.current_page_type) {
			align_correction := bit_offset + subheader_pointers_offset +
				sas.current_page_subheaders_count*subheader_pointer_length
			align_correction = align_correction % 8
			offset := bit_offset + align_correction
			offset += subheader_pointers_offset
			offset += sas.current_page_subheaders_count * subheader_pointer_length
			offset += sas.current_row_on_page_index * sas.properties.row_length
			err := sas.process_byte_array_with_data(offset, sas.properties.row_length)
			if err != nil {
				return err, false
			}
			if sas.current_row_on_page_index == min(
				sas.row_count,
				sas.properties.mix_page_row_count) {
				err, done := sas.read_next_page()
				if err != nil {
					return err, false
				} else if done {
					return nil, true
				}
				sas.current_row_on_page_index = 0
			}
			return nil, false
		} else if sas.current_page_type == page_data_type {
			err := sas.process_byte_array_with_data(
				bit_offset+subheader_pointers_offset+
					sas.current_row_on_page_index*
						sas.properties.row_length,
				sas.properties.row_length)
			if err != nil {
				return err, false
			}
			if sas.current_row_on_page_index == sas.current_page_block_count {
				err, done := sas.read_next_page()
				if err != nil {
					return err, false
				} else if done {
					return nil, true
				}
				sas.current_row_on_page_index = 0
			}
			return nil, false
		} else {
			return fmt.Errorf("unknown page type: %d", sas.current_page_type), false
		}
	}
	return nil, false
}

func (sas *SAS7BDAT) read_next_page() (error, bool) {
	sas.current_page_data_subheader_pointers = make([]*subheaderPointer, 0, 10)
	sas.cached_page = make([]byte, sas.properties.page_length)
	n, err := sas.file.Read(sas.cached_page)
	if n <= 0 {
		return nil, true
	}
	if err != nil {
		return err, false
	}

	if len(sas.cached_page) != sas.properties.page_length {
		return fmt.Errorf("failed to read complete page from file (read %d of %d bytes)",
			len(sas.cached_page), sas.properties.page_length), false
	}
	sas.read_page_header()
	if sas.current_page_type == page_meta_type {
		err = sas.process_page_metadata()
		if err != nil {
			return err, false
		}
	}
	if check_page_type(sas.current_page_type) {
		return sas.read_next_page()
	}
	return nil, false
}

func (sas *SAS7BDAT) getProperties() error {
	prop := new(sasProperties)
	sas.properties = prop

	// Check magic number
	err := sas.read_bytes(0, 288)
	if err != nil {
		return err
	}
	sas.cached_page = make([]byte, 288)
	copy(sas.cached_page, sas.buf[0:288])
	if check_magic_number(sas.cached_page) == false {
		return fmt.Errorf("magic number mismatch (not a SAS file?)")
	}

	// Get alignment information
	align1 := 0
	align2 := 0
	err = sas.read_bytes(align_1_offset, align_1_length)
	if err != nil {
		return err
	}
	prop.page_bit_offset = page_bit_offset_x86
	prop.subheader_pointer_length = subheader_pointer_length_x86
	prop.int_length = 4
	if string(sas.buf[0:align_1_length]) == string(u64_byte_checker_value) {
		align2 = align_2_value
		sas.U64 = true
		prop.int_length = 8
		prop.page_bit_offset = page_bit_offset_x64
		prop.subheader_pointer_length = subheader_pointer_length_x64
	}
	err = sas.read_bytes(align_2_offset, align_2_length)
	if err != nil {
		return err
	}
	if string(sas.buf[0:align_2_length]) == string(align_1_checker_value) {
		align1 = align_2_value
	}
	total_align := align1 + align2

	// Get endianness information
	err = sas.read_bytes(endianness_offset, endianness_length)
	if err != nil {
		return err
	}
	if sas.buf[0] == '\x01' {
		sas.ByteOrder = binary.LittleEndian
	} else {
		sas.ByteOrder = binary.BigEndian
	}

	// Get platform information
	err = sas.read_bytes(platform_offset, platform_length)
	if err != nil {
		return err
	}
	if sas.buf[0] == '1' {
		sas.Platform = "unix"
	} else if sas.buf[0] == '2' {
		sas.Platform = "windows"
	} else {
		sas.Platform = "unknown"
	}

	err = sas.read_bytes(dataset_offset, dataset_length)
	if err != nil {
		return err
	}
	sas.Name = string(sas.buf[0:dataset_length])

	err = sas.read_bytes(file_type_offset, file_type_length)
	if err != nil {
		return err
	}
	sas.FileType = string(sas.buf[0:file_type_length])

	// Timestamp is epoch 01/01/1960
	epoch := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	x, err := sas.read_float(date_created_offset+align1, date_created_length)
	if err != nil {
		return err
	}
	sas.DateCreated = epoch.Add(time.Duration(x) * time.Second)

	x, err = sas.read_float(date_modified_offset+align1, date_modified_length)
	if err != nil {
		return err
	}
	sas.DateModified = epoch.Add(time.Duration(x) * time.Second)

	prop.header_length, err = sas.read_int(header_size_offset+align1, header_size_length)
	if err != nil {
		return fmt.Errorf("Unable to read header size\n")
	}
	/*
		if sas.U64 && prop.header_length != 8192 {
			os.Stderr.WriteString(fmt.Sprintf("header length %d != 8192\n", prop.header_length))
		}
	*/

	// Read the rest of the header into cached_page.
	v := make([]byte, prop.header_length-288)
	sas.file.Read(v)
	sas.cached_page = append(sas.cached_page, v...)
	if len(sas.cached_page) != prop.header_length {
		return fmt.Errorf("The SAS7BDAT file appears to be truncated.")
	}

	prop.page_length, err = sas.read_int(page_size_offset+align1, page_size_length)
	if err != nil {
		return fmt.Errorf("Unable to read the page size value.")
	}
	prop.page_count, err = sas.read_int(page_count_offset+align1, page_count_length)
	if err != nil {
		return fmt.Errorf("Unable to read the page count value.")
	}

	err = sas.read_bytes(sas_release_offset+total_align, sas_release_length)
	if err != nil {
		return fmt.Errorf("Unable to read SAS relase value.")
	}
	sas.SASRelease = string(sas.buf[0:sas_release_length])

	err = sas.read_bytes(sas_server_type_offset+total_align, sas_server_type_length)
	if err != nil {
		return fmt.Errorf("Unable to read SAS server type value.")
	}
	sas.ServerType = string(bytes.TrimRight(sas.buf[0:sas_server_type_length], " \000"))

	err = sas.read_bytes(os_version_number_offset+total_align, os_version_number_length)
	if err != nil {
		return fmt.Errorf("Unable to read version number.")
	}
	sas.OSType = string(bytes.TrimRight(sas.buf[0:os_version_number_length], "\000"))

	err = sas.read_bytes(os_name_offset+total_align, os_name_length)
	if err != nil {
		return fmt.Errorf("Unable to read OS name.")
	}
	if sas.buf[0] != 0 {
		sas.OSName = string(bytes.TrimRight(sas.buf[0:os_name_length], " \000"))
	} else {
		err = sas.read_bytes(os_maker_offset+total_align, os_maker_length)
		if err != nil {
			return fmt.Errorf("Unable to read OS maker value.")
		}
		sas.OSName = string(bytes.TrimRight(sas.buf[0:os_maker_length], " \000"))
	}

	return nil
}

func (sas *SAS7BDAT) read_page_header() error {
	bit_offset := sas.properties.page_bit_offset
	var err error
	sas.current_page_type, err = sas.read_int(page_type_offset+bit_offset, page_type_length)
	if err != nil {
		return fmt.Errorf("Unable to read page type value.")
	}
	sas.current_page_block_count, err = sas.read_int(block_count_offset+bit_offset, block_count_length)
	if err != nil {
		return fmt.Errorf("Unable to read block count value.")
	}
	sas.current_page_subheaders_count, err = sas.read_int(subheader_count_offset+bit_offset, subheader_count_length)
	if err != nil {
		return fmt.Errorf("Unable to read subheader count value.")
	}

	return nil
}

func (sas *SAS7BDAT) process_page_metadata() error {
	bit_offset := sas.properties.page_bit_offset

	for i := 0; i < sas.current_page_subheaders_count; i++ {
		pointer, err := sas.process_subheader_pointers(
			subheader_pointers_offset+bit_offset, i)
		if err != nil {
			return err
		}
		if pointer.length == 0 {
			continue
		}
		if pointer.compression == truncated_subheader_id {
			continue
		}
		subheader_signature, err := sas.read_subheader_signature(pointer.offset)
		if err != nil {
			return err
		}
		subheader_index, err := sas.get_subheader_index(subheader_signature,
			pointer.compression, pointer.ptype)
		if err != nil {
			return fmt.Errorf("unknown subheader: %v\n", subheader_signature)
		}
		err = sas.process_subheader(subheader_index, pointer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sas *SAS7BDAT) process_subheader(subheader_index int, pointer *subheaderPointer) error {
	var processor func(int, int) error
	offset := pointer.offset
	length := pointer.length

	switch subheader_index {
	default:
		return fmt.Errorf("unknown index type")
	case rowSizeIndex:
		processor = sas.process_rowsize_subheader
	case columnSizeIndex:
		processor = sas.process_columnsize_subheader
	case columnTextIndex:
		processor = sas.process_columntext_subheader
	case columnNameIndex:
		processor = sas.process_columnname_subheader
	case columnAttributesIndex:
		processor = sas.process_columnattributes_subheader
	case formatAndLabelIndex:
		processor = sas.process_format_subheader
	case columnListIndex:
		processor = sas.process_columnlist_subheader
	case subheaderCountsIndex:
		processor = sas.process_subheader_counts
	case dataSubheaderIndex:
		sas.current_page_data_subheader_pointers =
			append(sas.current_page_data_subheader_pointers, pointer)
		return nil
	}

	err := processor(offset, length)
	if err != nil {
		return err
	}

	return nil
}

func (sas *SAS7BDAT) read_subheader_signature(offset int) ([]byte, error) {
	length := sas.properties.int_length
	err := sas.read_bytes(offset, length)
	if err != nil {
		return nil, err
	}
	subheader_signature := make([]byte, length)
	copy(subheader_signature, sas.buf[0:length])
	return subheader_signature, nil
}

func (sas *SAS7BDAT) process_subheader_counts(offset, length int) error {
	return nil
}

func (sas *SAS7BDAT) process_subheader_pointers(offset, subheader_pointer_index int) (*subheaderPointer, error) {

	length := sas.properties.int_length
	subheader_pointer_length := sas.properties.subheader_pointer_length
	total_offset := offset + subheader_pointer_length*subheader_pointer_index

	subheader_offset, err := sas.read_int(total_offset, length)
	if err != nil {
		return nil, fmt.Errorf("Unable to read subheader offset value.")
	}
	total_offset += length

	subheader_length, err := sas.read_int(total_offset, length)
	if err != nil {
		return nil, fmt.Errorf("Unable to read subheader length value.")
	}
	total_offset += length

	subheader_compression, err := sas.read_int(total_offset, 1)
	if err != nil {
		return nil, fmt.Errorf("Unable to read subheader compression value.")
	}
	total_offset++

	subheader_type, err := sas.read_int(total_offset, 1)
	if err != nil {
		return nil, fmt.Errorf("Unable to read subheader type value.")
	}

	return &subheaderPointer{subheader_offset, subheader_length,
		subheader_compression, subheader_type}, nil
}

func (sas *SAS7BDAT) get_subheader_index(signature []byte, compression, ptype int) (int, error) {

	index, ok := subheader_signature_to_index[string(signature)]
	if !ok {
		f := (compression == compressed_subheader_id) || (compression == 0)
		if (sas.Compression != "") && f && (ptype == compressed_subheader_type) {
			index = dataSubheaderIndex
		} else {
			return 0, fmt.Errorf("unknown subheader signature")
		}
	}
	return index, nil
}

func (sas *SAS7BDAT) process_byte_array_with_data(offset, length int) error {

	var source []byte
	if (sas.Compression != "") && (length < sas.properties.row_length) {
		decompressor := sas.get_decompressor()
		var err error
		source, err = decompressor(sas.properties.row_length, sas.cached_page[offset:offset+length])
		if err != nil {
			return err
		}
	} else {
		source = sas.cached_page[offset : offset+length]
	}

	for j := 0; j < sas.properties.column_count; j++ {
		length := sas.column_data_lengths[j]
		if length == 0 {
			break
		}
		start := sas.column_data_offsets[j]
		end := start + length
		temp := source[start:end]
		if sas.columns[j].ctype == number_column_type {
			s := 8 * sas.current_row_in_chunk_index
			if sas.ByteOrder == binary.LittleEndian {
				m := 8 - length
				copy(sas.bytechunk[j][s+m:s+8], temp)
			} else {
				copy(sas.bytechunk[j][s:s+length], temp)
			}
		} else {
			sas.stringchunk[j][sas.current_row_in_chunk_index] = string(temp)
		}
	}

	sas.current_row_on_page_index++
	sas.current_row_in_chunk_index++
	sas.current_row_in_file_index++
	return nil
}

func (sas *SAS7BDAT) pad_float(buf []byte) []byte {
	w := len(buf)
	newbuf := make([]byte, 8)
	if sas.ByteOrder == binary.LittleEndian {
		copy(newbuf[8-w:8], buf)
	} else {
		copy(newbuf[0:w], buf)
	}
	return newbuf
}

func check_magic_number(b []byte) bool {
	for i, v := range []byte(magic) {
		if v != b[i] {
			return false
		}
	}
	return true
}

func (sas *SAS7BDAT) process_rowsize_subheader(offset, length int) error {
	int_len := sas.properties.int_length
	lcs_offset := offset
	lcp_offset := offset
	if sas.U64 {
		lcs_offset += 682
		lcp_offset += 706
	} else {
		lcs_offset += 354
		lcp_offset += 378
	}

	var err error
	sas.properties.row_length, err = sas.read_int(offset+row_length_offset_multiplier*int_len, int_len)
	if err != nil {
		return err
	}
	sas.row_count, err = sas.read_int(offset+row_count_offset_multiplier*int_len, int_len)
	if err != nil {
		return err
	}
	sas.properties.col_count_p1, err = sas.read_int(offset+col_count_p1_multiplier*int_len, int_len)
	if err != nil {
		return err
	}
	sas.properties.col_count_p2, err = sas.read_int(offset+col_count_p2_multiplier*int_len, int_len)
	if err != nil {
		return err
	}
	sas.properties.mix_page_row_count, err = sas.read_int(offset+row_count_on_mix_page_offset_multiplier*int_len, int_len)
	if err != nil {
		return err
	}
	sas.properties.lcs, err = sas.read_int(lcs_offset, 2)
	if err != nil {
		return err
	}
	sas.properties.lcp, err = sas.read_int(lcp_offset, 2)
	if err != nil {
		return err
	}

	return nil
}

func (sas *SAS7BDAT) process_columnsize_subheader(offset, length int) error {
	int_len := sas.properties.int_length
	offset += int_len
	var err error
	sas.properties.column_count, err = sas.read_int(offset, int_len)
	if err != nil {
		return err
	}
	if sas.properties.col_count_p1+sas.properties.col_count_p2 !=
		sas.properties.column_count {
		os.Stderr.WriteString(fmt.Sprintf("Warning: column count mismatch (%d + %d != %d)\n",
			sas.properties.col_count_p1, sas.properties.col_count_p2,
			sas.properties.column_count))
	}
	return nil
}

func (sas *SAS7BDAT) process_columntext_subheader(offset, length int) error {

	offset += sas.properties.int_length

	text_block_size, err := sas.read_int(offset, text_block_size_length)
	if err != nil {
		return fmt.Errorf("Cannot read text block size for column names.")
	}

	err = sas.read_bytes(offset, text_block_size)
	if err != nil {
		return fmt.Errorf("Cannot read column names strings.")
	}
	sas.column_names_strings = append(sas.column_names_strings, string(sas.buf[0:text_block_size]))

	if len(sas.column_names_strings) == 1 {
		column_name := sas.column_names_strings[0]
		var compression_literal string
		for _, cl := range compression_literals {
			if strings.Contains(column_name, cl) {
				compression_literal = cl
				break
			}
		}
		sas.Compression = compression_literal
		offset -= sas.properties.int_length

		offset1 := offset + 16
		if sas.U64 {
			offset1 += 4
		}
		err := sas.read_bytes(offset1, sas.properties.lcp)
		if err != nil {
			return err
		}
		compression_literal = strings.Trim(string(sas.buf[0:8]), "\x00")
		if compression_literal == "" {
			sas.properties.lcs = 0
			offset1 = offset + 32
			if sas.U64 {
				offset1 += 4
			}
			err = sas.read_bytes(offset1, sas.properties.lcp)
			if err != nil {
				return err
			}
			sas.properties.creator_proc = string(sas.buf[0:sas.properties.lcp])
		} else if compression_literal == rle_compression {
			offset1 = offset + 40
			if sas.U64 {
				offset1 += 4
			}
			err = sas.read_bytes(offset1, sas.properties.lcp)
			if err != nil {
				return err
			}
			sas.properties.creator_proc = string(sas.buf[0:sas.properties.lcp])
		} else if sas.properties.lcs > 0 {
			sas.properties.lcp = 0
			offset1 = offset + 16
			if sas.U64 {
				offset1 += 4
			}
			err = sas.read_bytes(offset1, sas.properties.lcs)
			if err != nil {
				return err
			}
			sas.properties.creator_proc = string(sas.buf[0:sas.properties.lcp])
		}
	}
	return nil
}

func (sas *SAS7BDAT) process_columnname_subheader(offset, length int) error {

	int_len := sas.properties.int_length
	offset += int_len
	column_name_pointers_count := (length - 2*int_len - 12) / 8
	for i := 0; i < column_name_pointers_count; i++ {
		text_subheader := offset + column_name_pointer_length*(i+1) + column_name_text_subheader_offset
		col_name_offset := offset + column_name_pointer_length*(i+1) + column_name_offset_offset
		col_name_length := offset + column_name_pointer_length*(i+1) + column_name_length_offset

		idx, err := sas.read_int(text_subheader, column_name_text_subheader_length)
		if err != nil {
			return fmt.Errorf("Unable to read text subheader for column name.")
		}
		col_offset, err := sas.read_int(col_name_offset, column_name_offset_length)
		if err != nil {
			return fmt.Errorf("Unable to read column_name offset.")
		}
		col_len, err := sas.read_int(col_name_length, column_name_length_length)
		if err != nil {
			return fmt.Errorf("Unable to read column name length.")
		}

		name_str := sas.column_names_strings[idx]
		sas.column_names = append(sas.column_names, name_str[col_offset:col_offset+col_len])
	}

	return nil
}

func (sas *SAS7BDAT) process_columnlist_subheader(offset, length int) error {
	// unknown purpose
	return nil
}

func (sas *SAS7BDAT) process_columnattributes_subheader(offset, length int) error {

	int_len := sas.properties.int_length
	column_attributes_vectors_count := (length - 2*int_len - 12) / (int_len + 8)
	for i := 0; i < column_attributes_vectors_count; i++ {
		col_data_offset := (offset + int_len + column_data_offset_offset + i*(int_len+8))
		col_data_len := (offset + 2*int_len + column_data_length_offset + i*(int_len+8))
		col_types := (offset + 2*int_len + column_type_offset + i*(int_len+8))

		x, err := sas.read_int(col_data_offset, int_len)
		if err != nil {
			return err
		}
		sas.column_data_offsets = append(sas.column_data_offsets, x)

		x, err = sas.read_int(col_data_len, column_data_length_length)
		if err != nil {
			return err
		}
		sas.column_data_lengths = append(sas.column_data_lengths, x)

		x, err = sas.read_int(col_types, column_type_length)
		if err != nil {
			return err
		}
		if x == 1 {
			sas.column_types = append(sas.column_types, number_column_type)
		} else {
			sas.column_types = append(sas.column_types, string_column_type)
		}
	}

	return nil
}

func (sas *SAS7BDAT) process_format_subheader(offset, length int) error {

	int_len := sas.properties.int_length
	text_subheader_format := offset + column_format_text_subheader_index_offset + 3*int_len
	col_format_offset := offset + column_format_offset_offset + 3*int_len
	col_format_len := offset + column_format_length_offset + 3*int_len
	text_subheader_label := offset + column_label_text_subheader_index_offset + 3*int_len
	col_label_offset := offset + column_label_offset_offset + 3*int_len
	col_label_len := offset + column_label_offset_length + 3*int_len

	x, _ := sas.read_int(text_subheader_format, column_format_text_subheader_index_length)
	format_idx := min(x, len(sas.column_names_strings)-1)

	format_start, _ := sas.read_int(col_format_offset, column_format_offset_length)
	format_len, _ := sas.read_int(col_format_len, column_format_length_length)

	label_idx, _ := sas.read_int(text_subheader_label, column_label_text_subheader_index_length)
	label_idx = min(label_idx, len(sas.column_names_strings)-1)

	label_start, _ := sas.read_int(col_label_offset, column_label_offset_length)
	label_len, _ := sas.read_int(col_label_len, column_label_length_length)

	label_names := sas.column_names_strings[label_idx]
	column_label := label_names[label_start : label_start+label_len]
	format_names := sas.column_names_strings[format_idx]
	column_format := format_names[format_start : format_start+format_len]
	current_column_number := len(sas.columns)

	col := &Column{current_column_number,
		sas.column_names[current_column_number],
		column_label,
		column_format,
		sas.column_types[current_column_number],
		sas.column_data_lengths[current_column_number]}

	sas.ColumnFormats = append(sas.ColumnFormats, column_format)
	sas.columns = append(sas.columns, col)

	return nil
}

// RowCount returns the number of rows in the data set.
func (sas *SAS7BDAT) RowCount() int {
	return sas.row_count
}

// ColumnNames returns the names of the columns.
func (sas *SAS7BDAT) ColumnNames() []string {
	return sas.column_names
}

// ColumnTypes returns integer codes for the column data types.
func (sas *SAS7BDAT) ColumnTypes() []int {
	return sas.column_types
}

func (sas *SAS7BDAT) parse_metadata() error {
	done := false
	for !done {
		n, err := sas.file.Read(sas.cached_page)
		if n <= 0 {
			break
		}
		if err != nil {
			return err
		}
		if n != sas.properties.page_length {
			return fmt.Errorf("Failed to read a meta data page from the SAS file.")
		}
		done, err = sas.process_page_meta()
		if err != nil {
			return err
		}
	}
	return nil
}

func (sas *SAS7BDAT) process_page_meta() (bool, error) {
	sas.read_page_header()
	if is_page_meta_mix_amd(sas.current_page_type) {
		err := sas.process_page_metadata()
		if err != nil {
			return false, err
		}
	}
	return sas.is_page_mix_data_type(sas.current_page_type) ||
		(sas.current_page_data_subheader_pointers != nil), nil
}

func is_page_meta_mix_amd(pagetype int) bool {
	switch pagetype {
	case page_meta_type, 512, 640, page_amd_type:
		return true
	}
	return false
}

func (sas *SAS7BDAT) is_page_mix_type(val int) bool {
	switch val {
	case 512, 640:
		return true
	}
	return false
}

func (sas *SAS7BDAT) is_page_mix_data_type(val int) bool {
	switch val {
	case 512, 640, 256:
		return true
	}
	return false
}

func (sas *SAS7BDAT) is_page_mix_amd(val int) bool {
	switch val {
	case 0, 512, 640, 1024:
		return true
	}
	return false
}

func is_page_any(val int) bool {
	switch val {
	case 0, 512, 640, 1024, 256, 16384, -28672:
		return true
	}
	return false
}

func check_page_type(current_page int) bool {
	switch current_page {
	case page_meta_type, page_data_type, 512, 640:
		return false
	}
	return true
}

func tmp_sum(vec []byte) int {
	x := 0
	for _, v := range vec {
		x += int(v)
	}
	return x
}
