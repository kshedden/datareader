package datareader

// Read SAS7BDAT files with go.
//
// This code is heavily based on the Python module:
// https://pypi.python.org/pypi/sas7bdat
//
// See also:
// https://cran.r-project.org/web/packages/sas7bdat/vignettes/sas7bdat.pdf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
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

	column_names                         []string
	path                                 string
	buf                                  []byte
	align_correction                     int
	file                                 io.ReadSeeker
	cached_page                          []byte
	current_page_type                    int
	current_page_block_count             int
	current_page_subheaders_count        int
	current_file_position                int
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
	"\xF6\xF6\xF6\xF6":                 columnSizeIndex,
	"\x00\x00\x00\x00\xF6\xF6\xF6\xF6": columnSizeIndex,
	"\xF6\xF6\xF6\xF6\x00\x00\x00\x00": columnSizeIndex,
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
	column_format_length_length               = 26
	colun_format_length_length                = 2
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
// algorithm.
func rle_decompress(offset int, length int, result_length int, page []byte) []byte {
	current_result_array_index := 0
	result := make([]byte, 0, 1024)
	i := 0
	for j := 0; j < length; j++ {
		if i != j {
			continue
		}
		control_byte := page[offset+i] & 0xF0
		end_of_first_byte := page[offset+i] & 0x0F
		if control_byte == 0x00 {
			if i != (length - 1) {
				nbytes := int(page[offset+i+1] & 0xFF)
				nbytes += 64 + int(end_of_first_byte)*256
				start := offset + i + 2
				end := start + nbytes
				result = append(result, page[start:end]...)
				i += nbytes + 1
				current_result_array_index += nbytes
			}
		} else if control_byte == 0x40 {
			nbytes := int(end_of_first_byte * 16)
			nbytes += int(page[offset+i+1] & 0xFF)
			for k := 0; k < nbytes; k++ {
				result = append(result, page[offset+i+2])
				current_result_array_index += 1
			}
			i += 2
		} else if control_byte == 0x60 {
			nbytes := int(end_of_first_byte) * 256
			nbytes += int(page[offset+i+1]&0xFF) + 17
			for k := 0; k < nbytes; k++ {
				result = append(result, 0x20)
				current_result_array_index += 1
			}
			i += 1
		} else if control_byte == 0x70 {
			nbytes := int(page[offset+i+1]&0xFF) + 17
			for k := 0; k < nbytes; k++ {
				result = append(result, 0x00)
				current_result_array_index += 1
			}
			i += 1
		} else if control_byte == 0x80 {
			nbytes := min(int(end_of_first_byte+1), length-(i+1))
			start := offset + i + 1
			end := start + nbytes
			result = append(result, page[start:end]...)
			i += nbytes
			current_result_array_index += nbytes
		} else if control_byte == 0x90 {
			nbytes := min(int(end_of_first_byte+17),
				length-(i+1))
			start := offset + i + 1
			end := start + nbytes
			result = append(result, page[start:end]...)
			i += nbytes
			current_result_array_index += nbytes
		} else if control_byte == 0xA0 {
			nbytes := min(int(end_of_first_byte+33),
				length-(i+1))
			start := offset + i + 1
			end := start + nbytes
			result = append(result, page[start:end]...)
			i += nbytes
			current_result_array_index += nbytes
		} else if control_byte == 0xB0 {
			nbytes := min(int(end_of_first_byte+49),
				length-(i+1))
			start := offset + i + 1
			end := start + nbytes
			result = append(result, page[start:end]...)
			i += nbytes
			current_result_array_index += nbytes
		} else if control_byte == 0xC0 {
			nbytes := int(end_of_first_byte + 3)
			for k := 0; k < nbytes; k++ {
				result = append(result, page[offset+i+1])
				current_result_array_index += 1
			}
			i += 1
		} else if control_byte == 0xD0 {
			nbytes := int(end_of_first_byte + 2)
			for k := 0; k < nbytes; k++ {
				result = append(result, 0x40)
				current_result_array_index += 1
			}
		} else if control_byte == 0xE0 {
			nbytes := int(end_of_first_byte + 2)
			for k := 0; k < nbytes; k++ {
				result = append(result, 0x20)
				current_result_array_index += 1
			}
		} else if control_byte == 0xF0 {
			nbytes := int(end_of_first_byte + 2)
			for k := 0; k < nbytes; k++ {
				result = append(result, 0x00)
				current_result_array_index += 1
			}
		} else {
			// do something else here...
			panic(fmt.Sprintf("unknown control byte: %v", control_byte))
		}
		i += 1
	}
	return result
}

func bytes_to_bits(src []byte, offset int, length int) []byte {
	result := make([]byte, length*8)
	for i := 0; i < length; i++ {
		b := src[offset+i]
		for bit := 0; bit < 8; bit++ {
			if (b & (1 << uint(bit))) != 0 {
				result[8*i+(7-bit)] = 1
			}
		}
	}
	return result
}

func is_short_rle(b byte) bool {
	for _, v := range []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05} {
		if b == v {
			return true
		}
	}
	return false
}

func is_single_byte_marker(b byte) bool {
	for _, v := range []byte{0x02, 0x04, 0x06, 0x08, 0x0A} {
		if b == v {
			return true
		}
	}
	return false
}

func is_two_bytes_marker(b []byte) bool {
	if len(b) != 2 {
		return false
	}
	return ((b[0] >> 4) & 0xF) > 2
}

func is_three_bytes_marker(b []byte) bool {
	if len(b) != 3 {
		return false
	}
	flag := (b[0] >> 4) & 0xF
	return (flag == 1) || (flag == 2)
}

func get_length_of_rle_pattern(b byte) int {
	if b <= 0x05 {
		return int(b) + 3
	}
	return 0
}

func get_length_of_one_byte_pattern(b byte) int {
	if is_single_byte_marker(b) {
		return int(b) + 14
	}
	return 0
}

func get_length_of_two_bytes_pattern(b []byte) int {
	return int((b[0] >> 4) & 0xF)
}

func get_length_of_three_bytes_pattern(p_type int, b []byte) int {
	if p_type == 1 {
		return int(19 + (b[0] & 0xF) + (b[1] * 16))
	} else if p_type == 2 {
		return int(b[2] + 16)
	}
	return 0
}

func get_offset_for_one_byte_pattern(b byte) int {
	if b == 0x08 {
		return 24
	} else if b == 0x0A {
		return 40
	}
	return 0
}

func get_offset_for_two_bytes_pattern(b []byte) int {
	return int(3 + (b[0] & 0xF) + (b[1] * 16))
}

func get_offset_for_three_bytes_pattern(b []byte) int {
	return int(3 + (b[0] & 0xF) + (b[1] * 16))
}

func clone_byte(b byte, length int) []byte {
	out := make([]byte, length)
	for k := 0; k < length; k++ {
		out[k] = b
	}
	return out
}

// rdc_decompress decompresses data using the Ross Data Compression
// algorithm
func rdc_decompress(offset, length, result_length int, page []byte) []byte {
	src_row := page[offset : offset+length]
	out_row := make([]byte, 0, 1024)
	src_offset := 0
	for src_offset < len(src_row)-2 {
		prefix_bits := bytes_to_bits(src_row, src_offset, 2)
		src_offset += 2
		for bit_index := 0; bit_index < 16; bit_index++ {
			if src_offset >= len(src_row) {
				break
			}
			if prefix_bits[bit_index] == 0 {
				out_row = append(out_row, src_row[src_offset])
				src_offset++
				continue
			}
			marker_byte := src_row[src_offset]
			if src_offset+1 >= len(src_row) {
				break
			}
			next_byte := src_row[src_offset+1]
			if is_short_rle(marker_byte) {
				length = get_length_of_rle_pattern(marker_byte)
				for j := 0; j < length; j++ {
					out_row = append(out_row, next_byte)
				}
				src_offset += 2
				continue
			} else if is_single_byte_marker(marker_byte) &&
				((next_byte & 0xF0) != ((next_byte << 4) & 0xF0)) {
				length = get_length_of_one_byte_pattern(marker_byte)
				back_offset := get_offset_for_one_byte_pattern(marker_byte)
				start := len(out_row) - back_offset
				end := start + length
				out_row = append(out_row, out_row[start:end]...)
				src_offset++
				continue
			}
			two_bytes_marker := src_row[src_offset : src_offset+2]
			if is_two_bytes_marker(two_bytes_marker) {
				length := get_length_of_two_bytes_pattern(two_bytes_marker)
				back_offset := get_offset_for_two_bytes_pattern(two_bytes_marker)
				start := len(out_row) - back_offset
				end := start + length
				out_row = append(out_row, out_row[start:end]...)
				src_offset += 2
				continue
			}
			three_bytes_marker := src_row[src_offset : src_offset+3]
			if is_three_bytes_marker(three_bytes_marker) {
				p_type := int((three_bytes_marker[0] >> 4) & 0x0F)
				back_offset := 0
				if p_type == 2 {
					back_offset = get_offset_for_three_bytes_pattern(three_bytes_marker)
				}
				length := get_length_of_three_bytes_pattern(p_type, three_bytes_marker)
				if p_type == 1 {
					for j := 0; j < length; j++ {
						out_row = append(out_row, three_bytes_marker[2])
					}
				} else {
					start := len(out_row) - back_offset
					end := start + length
					out_row = append(out_row, out_row[start:end]...)
				}
				src_offset += 3
				continue
			} else {
				// do something else here
				panic(
					fmt.Sprintf("unknown marker %s at offset %s", src_row[src_offset], src_offset))
			}
		}
	}
	return out_row
}

func (sas *SAS7BDAT) get_decompressor() func(int, int, int, []byte) []byte {

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
	err := sas.setProperties()
	if err != nil {
		return nil, err
	}
	sas.cached_page = make([]byte, sas.properties.page_length)
	sas.parse_metadata()
	return sas, nil
}

// read_bytes read length bytes from the given offset in the current
// page (or from the beginning of the file if no page has yet been
// read).
func (sas *SAS7BDAT) read_bytes(offset int, length int) error {
	sas.ensure_buf_size(length)

	if sas.cached_page == nil {
		sas.file.Seek(int64(offset), 0)
		n, err := sas.file.Read(sas.buf[0:length])
		sas.current_file_position = offset + n
		if err != nil {
			return err
		} else if n < length {
			return errors.New(fmt.Sprintf("Unable to read %d bytes from file position %d.", length, offset))
		}
	} else {
		if offset+length > len(sas.cached_page) {
			return errors.New(fmt.Sprintf("The cached page is too small."))
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
		return 0, errors.New("unknown float width")
	case 8:
		err := binary.Read(r, sas.ByteOrder, &x)
		if err != nil {
			return 0, err
		}
	}
	return x, nil
}

// Read an integer of 1, 2, 4 or 8 byte width from the supplied buffer.
func (sas *SAS7BDAT) read_int_from_buffer(buf []byte, width int) (int, error) {

	r := bytes.NewReader(buf[0:width])
	switch width {
	default:
		return 0, errors.New("invalid integer width")
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
// SAS date/time values are returned as float64 values.  The meaning
// of these values depends on the formats, which are placed in
// SAS7BDAT.ColumnFormats.  For example, SAS date formats correspond to
// the number of days since January 1, 1960.
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
				return nil, errors.New("unknown column type")
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
			rslt[j], _ = NewSeries(name, vec, miss)
		case string_column_type:
			if sas.TrimStrings {
				for i := 0; i < n; i++ {
					sas.stringchunk[j][i] = strings.TrimRight(sas.stringchunk[j][i], " ")
				}
			}
			rslt[j], _ = NewSeries(name, sas.stringchunk[j][0:n], miss)
		}
	}

	return rslt
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
			align_correction := 0
			if sas.align_correction != 0 {
				align_correction = bit_offset
				align_correction += subheader_pointers_offset
				align_correction += sas.current_page_subheaders_count * subheader_pointer_length
				align_correction %= 8
			}
			offset := bit_offset
			offset += subheader_pointers_offset
			offset += align_correction
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
			return errors.New(fmt.Sprintf("unknown page type: %s", sas.current_page_type)), false
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
		return errors.New(fmt.Sprintf("failed to read complete page from file (read %d of %d bytes)",
			len(sas.cached_page), sas.properties.page_length)), false
	}
	sas.read_page_header()
	if sas.current_page_type == page_meta_type {
		err = sas.process_page_metadata()
		if err != nil {
			return err, false
		}
	}
	if check_page_type(sas.current_page_type, sas) {
		return sas.read_next_page()
	}
	return nil, false
}

func (sas *SAS7BDAT) setProperties() error {
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
		return errors.New("magic number mismatch (not a SAS file?)")
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

	prop.header_length, _ = sas.read_int(header_size_offset+align1, header_size_length)

	// Read the rest of the header into cached_page.
	v := make([]byte, prop.header_length-288)
	sas.file.Read(v)
	sas.cached_page = append(sas.cached_page, v...)
	if len(sas.cached_page) != prop.header_length {
		return errors.New("The header is too short (is this a sas7bdat file?)")
	}

	prop.page_length, err = sas.read_int(page_size_offset+align1, page_size_length)
	if err != nil {
		return errors.New("Unable to read the page size value.")
	}
	prop.page_count, err = sas.read_int(page_count_offset+align1, page_count_length)
	if err != nil {
		return errors.New("Unable to read the page count value.")
	}

	err = sas.read_bytes(sas_release_offset+total_align, sas_release_length)
	if err != nil {
		return errors.New("Unable to read SAS relase value.")
	}
	sas.SASRelease = string(sas.buf[0:sas_release_length])

	err = sas.read_bytes(sas_server_type_offset+total_align, sas_server_type_length)
	if err != nil {
		return errors.New("Unable to read SAS server type value.")
	}
	sas.ServerType = string(sas.buf[0:sas_server_type_length])

	err = sas.read_bytes(os_version_number_offset+total_align, os_version_number_length)
	if err != nil {
		return errors.New("Unable to read version number.")
	}
	sas.OSType = string(sas.buf[0:os_version_number_length])

	err = sas.read_bytes(os_name_offset+total_align, os_name_length)
	if err != nil {
		return errors.New("Unable to read OS name.")
	}
	if sas.buf[0] != 0 {
		sas.OSName = string(sas.buf[0:os_name_length])
	} else {
		err = sas.read_bytes(os_maker_offset+total_align, os_maker_length)
		if err != nil {
			return errors.New("Unable to read OS maker value.")
		}
		sas.OSName = string(sas.buf[0:os_maker_length])
	}

	return nil
}

func (sas *SAS7BDAT) read_page_header() error {
	bit_offset := sas.properties.page_bit_offset
	var err error
	sas.current_page_type, err = sas.read_int(page_type_offset+bit_offset, page_type_length)
	if err != nil {
		return errors.New("Unable to read page type value.")
	}
	sas.current_page_block_count, err = sas.read_int(block_count_offset+bit_offset, block_count_length)
	if err != nil {
		return errors.New("Unable to read block count value.")
	}
	sas.current_page_subheaders_count, err = sas.read_int(subheader_count_offset+bit_offset, subheader_count_length)
	if err != nil {
		return errors.New("Unable to read subheader count value.")
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
			return err
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
		return errors.New("unknown index type")
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
	length := 4
	if sas.U64 {
		length = 8
	}
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

	length := 8
	if !sas.U64 {
		length = 4
	}
	subheader_pointer_length := sas.properties.subheader_pointer_length
	total_offset := offset + subheader_pointer_length*subheader_pointer_index

	subheader_offset, err := sas.read_int(total_offset, length)
	if err != nil {
		return nil, errors.New("Unable to read subheader offset value.")
	}
	total_offset += length

	subheader_length, err := sas.read_int(total_offset, length)
	if err != nil {
		return nil, errors.New("Unable to read subheader length value.")
	}
	total_offset += length

	subheader_compression, err := sas.read_int(total_offset, 1)
	if err != nil {
		return nil, errors.New("Unable to read subheader compression value.")
	}
	total_offset++

	subheader_type, err := sas.read_int(total_offset, 1)
	if err != nil {
		return nil, errors.New("Unable to read subheader type value.")
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
			return 0, errors.New("Unknwn subheader signature")
		}
	}
	return index, nil
}

func (sas *SAS7BDAT) process_byte_array_with_data(offset, length int) error {

	var source []byte
	if (sas.Compression != "") && (length < sas.properties.row_length) {
		decompressor := sas.get_decompressor()
		source = decompressor(offset, length, sas.properties.row_length, sas.cached_page)
		offset = 0
	} else {
		source = sas.cached_page
	}

	for j := 0; j < sas.properties.column_count; j++ {
		length := sas.column_data_lengths[j]
		if length == 0 {
			break
		}
		start := offset + sas.column_data_offsets[j]
		end := start + length
		temp := source[start:end]
		if sas.columns[j].ctype == number_column_type {

			//fmat := sas.columns[j].format
			if false { //fmat in self.TIME_FORMAT_STRINGS {
				//row_elements.append(self._read_val(
				//'time', temp, length
				//))
			} else if false { //fmat in self.DATE_TIME_FORMAT_STRINGS {
				//row_elements.append(self._read_val(
				//'datetime', temp, length
				//))
			} else if false { //fmat in self.DATE_FORMAT_STRINGS {
				//	row_elements.append(self._read_val(
				//		'date', temp, length
				//))
			} else {
				s := 8 * sas.current_row_in_chunk_index
				if sas.ByteOrder == binary.LittleEndian {
					m := 8 - length
					copy(sas.bytechunk[j][s+m:s+8], temp)
				} else {
					copy(sas.bytechunk[j][s:s+length], temp)
				}
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
	sas.properties.column_count, _ = sas.read_int(offset, int_len)
	if sas.properties.col_count_p1+sas.properties.col_count_p2 !=
		sas.properties.column_count {
		return errors.New("column count mismatch")
	}
	return nil
}

func (sas *SAS7BDAT) process_columntext_subheader(offset, length int) error {

	offset += sas.properties.int_length

	text_block_size, err := sas.read_int(offset, text_block_size_length)
	if err != nil {
		return errors.New("Cannot read text block size for column names.")
	}

	err = sas.read_bytes(offset, text_block_size)
	if err != nil {
		return errors.New("Cannot read column names strings.")
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
			return errors.New("Unable to read text subheader for column name.")
		}
		col_offset, err := sas.read_int(col_name_offset, column_name_offset_length)
		if err != nil {
			return errors.New("Unable to read column_name offset.")
		}
		col_len, err := sas.read_int(col_name_length, column_name_length_length)
		if err != nil {
			return errors.New("Unable to read column name length.")
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
	col_format_len := offset + column_format_length_length + 3*int_len
	text_subheader_label := offset + column_label_text_subheader_index_offset + 3*int_len
	col_label_offset := offset + column_label_offset_offset + 3*int_len
	col_label_len := offset + column_label_length_offset + 3*int_len

	x, _ := sas.read_int(text_subheader_format, column_format_text_subheader_index_length)
	format_idx := min(x, len(sas.column_names_strings)-1)

	format_start, _ := sas.read_int(col_format_offset, column_format_offset_length)
	format_len, _ := sas.read_int(col_format_len, colun_format_length_length)

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
			return errors.New("Failed to read a meta data page from the SAS file.")
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

func check_page_type(current_page int, sas *SAS7BDAT) bool {
	switch current_page {
	case page_meta_type, page_data_type, 512, 640:
		return false
	}
	return true
}
