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

	xencoding "golang.org/x/text/encoding"
)

// SAS7BDAT represents a SAS data file in SAS7BDAT format.
type SAS7BDAT struct {

	// Formats for the columns
	ColumnFormats []string

	// If true, trim whitespace from right of each string variable
	// (SAS7BDAT strings are fixed width)
	TrimStrings bool

	// If true, converts some date formats to Go date values (does
	// not work for all SAS date formats)
	ConvertDates bool

	// If true, strings are represented as uint64 values.  Call
	// the StringFactorMap method to obtain the mapping from these
	// coded values to the actual strings that they represent.
	FactorizeStrings bool

	// If true, turns off alignment correction when reading mix-type pages.
	// In general this should be set to false.  However some files
	// are read incorrectly and need this flag set to true.  At present,
	// we do not know how to automatically detect the correct setting, so
	// we leave this as a configurable option.
	NoAlignCorrection bool

	// The creation date of the file
	DateCreated time.Time

	// The modification date of the file
	DateModified time.Time

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

	// The encoding name
	FileEncoding string

	// True if the file was created on a 64 bit architecture
	U64 bool

	// The byte order of the file
	ByteOrder binary.ByteOrder

	// The compression mode of the file
	Compression string

	// A decoder for decoding text to unicode
	TextDecoder *xencoding.Decoder

	// The number of rows in the file
	rowCount int

	// Data types of the columns
	columnTypes []ColumnTypeT

	// Labels for the columns
	columnLabels []string

	// Names of the columns
	columnNames []string

	buf                              []byte
	file                             io.ReadSeeker
	cachedPage                       []byte
	currentPageType                  int
	currentPageBlockCount            int
	currentPageSubheadersCount       int
	currentRowInFileIndex            int
	currentRowOnPageIndex            int
	currentPageDataSubheaderPointers []*subheaderPointer
	stringchunk                      [][]uint64
	bytechunk                        [][]byte
	currentRowInChunkIndex           int
	columnNamesStrings               []string
	columnDataOffsets                []int
	columnDataLengths                []int
	columns                          []*column
	properties                       *sasProperties
	stringPool                       map[uint64]string
	stringPoolR                      map[string]uint64
}

// These values don't change after the header is read.
type sasProperties struct {
	intLength              int
	pageBitOffset          int
	subheaderPointerLength int
	headerLength           int
	pageLength             int
	pageCount              int
	rowLength              int
	colCountP1             int
	colCountP2             int
	mixPageRowCount        int
	lcs                    int
	lcp                    int
	creatorProc            string
	columnCount            int
}

type column struct {
	colId  int
	name   string
	label  string
	format string
	ctype  ColumnTypeT
	length int
}

type subheaderPointer struct {
	offset      int
	length      int
	compression int
	ptype       int
}

const (
	rowSizeIndex = iota
	columnSizeIndex
	subheaderCountsIndex
	columnTextIndex
	columnNameIndex
	columnAttributesIndex
	formatAndLabelIndex
	columnListIndex
	dataSubheaderIndex
)

// ColumnTypeT is the type of a data column in a SAS or Stata file.
type ColumnTypeT uint16

const (
	SASNumericType ColumnTypeT = iota
	SASStringType
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
	magic = ("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc2\xea\x81\x60" +
		"\xb3\x14\x11\xcf\xbd\x92\x08\x00\x09\xc7\x31\x8c\x18\x1f\x10\x11")
	align_1_checker_value                     = '3'
	align_1_offset                            = 32
	align_1_length                            = 1
	u64_byte_checker_value                    = '3'
	align_2_offset                            = 35
	align_2_length                            = 1
	align_2_value                             = 4
	endianness_offset                         = 37
	endianness_length                         = 1
	platform_offset                           = 39
	platform_length                           = 1
	encoding_offset                           = 70
	encoding_length                           = 1
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

// StringFactorMap returns a map that associates integer codes
// with the string value that each code represents.  This is only
// relevant if FactorizeStrings is set to True.
func (sas *SAS7BDAT) StringFactorMap() map[uint64]string {
	return sas.stringPool
}

// Incomplete list of encodings
var encoding_names = map[int]string{29: "latin1", 20: "utf-8", 33: "cyrillic", 60: "wlatin2",
	61: "wcyrillic", 62: "wlatin1", 90: "ebcdic870"}

var compression_literals = []string{rle_compression, rdc_compression}

// ensureBufSize enlarges the data buffer if needed to accommodate
// at least m bytes of data.
func (sas *SAS7BDAT) ensureBufSize(m int) {
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

func (sas *SAS7BDAT) getDecompressor() func(int, []byte) ([]byte, error) {
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

	sas.cachedPage = make([]byte, sas.properties.pageLength)
	err = sas.parseMetadata()
	if err != nil {
		return nil, err
	}

	// Default text decoder
	// leave as nil for now (no decoding)
	//sas.TextDecoder = charmap.Windows1250.NewDecoder()

	return sas, nil
}

// readBytes read length bytes from the given offset in the current
// page (or from the beginning of the file if no page has yet been
// read).
func (sas *SAS7BDAT) readBytes(offset, length int) error {

	sas.ensureBufSize(length)

	if sas.cachedPage == nil {
		if _, err := sas.file.Seek(int64(offset), 0); err != nil {
			panic(err)
		}
		n, err := sas.file.Read(sas.buf[0:length])
		if err != nil {
			return err
		} else if n < length {
			return fmt.Errorf("Unable to read %d bytes from file position %d.", length, offset)
		}
	} else {
		if offset+length > len(sas.cachedPage) {
			return fmt.Errorf("The cached page is too small.")
		}
		copy(sas.buf, sas.cachedPage[offset:offset+length])
	}
	return nil
}

func (sas *SAS7BDAT) readFloat(offset, width int) (float64, error) {
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
func (sas *SAS7BDAT) readIntFromBuffer(buf []byte, width int) (int, error) {

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
func (sas *SAS7BDAT) readInt(offset, width int) (int, error) {

	err := sas.readBytes(offset, width)
	if err != nil {
		return 0, err
	}

	x, err := sas.readIntFromBuffer(sas.buf[0:width], width)
	if err != nil {
		return 0, err
	}
	return x, nil
}

// Read returns up to num_rows rows of data from the SAS7BDAT file, as
// an array of Series objects.  The Series data types are either
// float64 or string.  If num_rows is negative, the remainder of the
// file is read.  Returns (nil, io.EOF) when no rows remain.
//
// SAS strings variables have a fixed width and are right-padded with
// whitespace.  The TrimRight field of the SAS7BDAT struct can be set
// to true to automatically trim this whitespace.
func (sas *SAS7BDAT) Read(num_rows int) ([]*Series, error) {

	if num_rows < 0 {
		num_rows = sas.rowCount - sas.currentRowInFileIndex
	}

	if sas.currentRowInFileIndex >= sas.rowCount {
		return nil, io.EOF
	}

	sas.stringPool = make(map[uint64]string)
	sas.stringPoolR = make(map[string]uint64)

	// Reallocate each call so the results are backed by
	// completely independent memory with each call to read (to
	// support concurrent processing of results while continuing
	// reading).
	sas.bytechunk = make([][]byte, sas.properties.columnCount)
	sas.stringchunk = make([][]uint64, sas.properties.columnCount)
	for j := 0; j < sas.properties.columnCount; j++ {
		switch sas.columnTypes[j] {
		case SASNumericType:
			sas.bytechunk[j] = make([]byte, 8*num_rows)
		case SASStringType:
			sas.stringchunk[j] = make([]uint64, num_rows)
		default:
			return nil, fmt.Errorf("unknown column type")
		}
	}

	sas.currentRowInChunkIndex = 0
	for i := 0; i < num_rows; i++ {
		err, done := sas.readline()
		if err != nil {
			return nil, err
		} else if done {
			break
		}
	}

	rslt := sas.chunkToSeries()

	return rslt, nil
}

func (sas *SAS7BDAT) chunkToSeries() []*Series {

	rslt := make([]*Series, sas.properties.columnCount)
	n := sas.currentRowInChunkIndex

	for j := 0; j < sas.properties.columnCount; j++ {

		name := sas.columnNames[j]
		miss := make([]bool, n)

		switch sas.columnTypes[j] {
		case SASNumericType:
			vec := make([]float64, n)
			buf := bytes.NewReader(sas.bytechunk[j][0 : 8*n])
			if err := binary.Read(buf, sas.ByteOrder, &vec); err != nil {
				panic(err)
			}
			for i := 0; i < n; i++ {
				if math.IsNaN(vec[i]) {
					miss[i] = true
				}
			}
			if sas.ConvertDates && sas.ColumnFormats[j] == "MMDDYY" || sas.ColumnFormats[j] == "DATE" {
				tvec := toDate(vec)
				rslt[j], _ = NewSeries(name, tvec, miss)
			} else if sas.ConvertDates && sas.ColumnFormats[j] == "DATETIME" {
				tvec := toDateTime(vec)
				rslt[j], _ = NewSeries(name, tvec, miss)
			} else {
				rslt[j], _ = NewSeries(name, vec, miss)
			}
		case SASStringType:
			if sas.FactorizeStrings {
				rslt[j], _ = NewSeries(name, sas.stringchunk[j], miss)
			} else {
				s := make([]string, n)
				for i := 0; i < n; i++ {
					s[i] = sas.stringPool[sas.stringchunk[j][i]]
				}
				rslt[j], _ = NewSeries(name, s, miss)
			}
		default:
			panic("Unknown column type")
		}
	}

	return rslt
}

func toDate(x []float64) []time.Time {

	rslt := make([]time.Time, len(x))

	base := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)

	for j, v := range x {
		rslt[j] = base.Add(time.Hour * time.Duration(24*v))
	}

	return rslt
}

func date_time(x float64) time.Time {
	// Timestamp is epoch 01/01/1960
	base := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	return base.Add(time.Duration(x) * time.Second)
}

func toDateTime(x []float64) []time.Time {
	rslt := make([]time.Time, len(x))

	for j, v := range x {
		rslt[j] = date_time(v)
	}

	return rslt
}

func (sas *SAS7BDAT) readline() (error, bool) {

	bit_offset := sas.properties.pageBitOffset
	subheaderPointerLength := sas.properties.subheaderPointerLength

	// If there is no page, go to the end of the header and read a page.
	if sas.cachedPage == nil {
		if _, err := sas.file.Seek(int64(sas.properties.headerLength), 0); err != nil {
			return err, false
		}
		err, done := sas.readNextPage()
		if err != nil {
			return err, false
		} else if done {
			return nil, true
		}
	}

	// Loop until a data row is read
	for {
		if sas.currentPageType == page_meta_type {
			if sas.currentRowOnPageIndex >= len(sas.currentPageDataSubheaderPointers) {
				err, done := sas.readNextPage()
				if err != nil {
					return err, false
				} else if done {
					return nil, true
				}
				sas.currentRowOnPageIndex = 0
				continue
			}
			current_subheader_pointer := sas.currentPageDataSubheaderPointers[sas.currentRowOnPageIndex]
			err := sas.processByteArrayWithData(current_subheader_pointer.offset, current_subheader_pointer.length)
			if err != nil {
				return err, false
			}
			return nil, false
		} else if sas.isPageMixType(sas.currentPageType) {
			alignCorrection := (bit_offset + subheader_pointers_offset +
				sas.currentPageSubheadersCount*subheaderPointerLength) % 8
			if sas.NoAlignCorrection {
				alignCorrection = 0
			}
			offset := bit_offset + subheader_pointers_offset +
				sas.currentPageSubheadersCount*subheaderPointerLength +
				sas.currentRowOnPageIndex*sas.properties.rowLength +
				alignCorrection
			err := sas.processByteArrayWithData(offset, sas.properties.rowLength)
			if err != nil {
				return err, false
			}
			if sas.currentRowOnPageIndex == min(sas.rowCount, sas.properties.mixPageRowCount) {
				err, done := sas.readNextPage()
				if err != nil {
					return err, false
				} else if done {
					return nil, true
				}
				sas.currentRowOnPageIndex = 0
			}
			return nil, false
		} else if sas.currentPageType == page_data_type {
			err := sas.processByteArrayWithData(
				bit_offset+subheader_pointers_offset+sas.currentRowOnPageIndex*sas.properties.rowLength,
				sas.properties.rowLength)
			if err != nil {
				return err, false
			}
			if sas.currentRowOnPageIndex == sas.currentPageBlockCount {
				err, done := sas.readNextPage()
				if err != nil {
					return err, false
				} else if done {
					return nil, true
				}
				sas.currentRowOnPageIndex = 0
			}
			return nil, false
		} else {
			return fmt.Errorf("unknown page type: %d", sas.currentPageType), false
		}
	}
}

func (sas *SAS7BDAT) readNextPage() (error, bool) {

	sas.currentPageDataSubheaderPointers = make([]*subheaderPointer, 0, 10)
	sas.cachedPage = make([]byte, sas.properties.pageLength)
	n, err := sas.file.Read(sas.cachedPage)
	if n <= 0 {
		return nil, true
	}

	if err != nil && err != io.EOF {
		return err, false
	}

	if len(sas.cachedPage) != sas.properties.pageLength {
		return fmt.Errorf("failed to read complete page from file (read %d of %d bytes)",
			len(sas.cachedPage), sas.properties.pageLength), false
	}

	if err := sas.readPageHeader(); err != nil {
		return err, false
	}

	if sas.currentPageType == page_meta_type {
		err = sas.processPageMetadata()
		if err != nil {
			return err, false
		}
	}

	if checkPageType(sas.currentPageType) {
		return sas.readNextPage()
	}

	return nil, false
}

func (sas *SAS7BDAT) getProperties() error {

	prop := new(sasProperties)
	sas.properties = prop

	// Check magic number
	err := sas.readBytes(0, 288)
	if err != nil {
		return err
	}
	sas.cachedPage = make([]byte, 288)
	copy(sas.cachedPage, sas.buf[0:288])
	if !bytes.Equal(sas.cachedPage[0:len(magic)], []byte(magic)) {
		return fmt.Errorf("magic number mismatch (not a SAS file?)")
	}

	// Get alignment information
	var align1, align2 int
	err = sas.readBytes(align_1_offset, align_1_length)
	if err != nil {
		return err
	}
	prop.pageBitOffset = page_bit_offset_x86
	prop.subheaderPointerLength = subheader_pointer_length_x86
	prop.intLength = 4
	if sas.buf[0] == u64_byte_checker_value {
		align2 = align_2_value
		sas.U64 = true
		prop.intLength = 8
		prop.pageBitOffset = page_bit_offset_x64
		prop.subheaderPointerLength = subheader_pointer_length_x64
	}
	err = sas.readBytes(align_2_offset, align_2_length)
	if err != nil {
		return err
	}

	if string(sas.buf[0:align_2_length]) == string(align_1_checker_value) {
		align1 = align_2_value
	}
	total_align := align1 + align2

	// Get endianness information
	err = sas.readBytes(endianness_offset, endianness_length)
	if err != nil {
		return err
	}

	if sas.buf[0] == '\x01' {
		sas.ByteOrder = binary.LittleEndian
	} else {
		sas.ByteOrder = binary.BigEndian
	}

	// Get platform information
	err = sas.readBytes(platform_offset, platform_length)
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

	// Try to get encoding information.
	err = sas.readBytes(encoding_offset, encoding_length)
	if err != nil {
		return err
	}
	xb := int(sas.buf[0])
	encoding, ok := encoding_names[xb]
	if ok {
		sas.FileEncoding = encoding
	} else {
		sas.FileEncoding = fmt.Sprintf("encoding code=%d", xb)
	}

	err = sas.readBytes(dataset_offset, dataset_length)
	if err != nil {
		return err
	}
	sas.Name = string(sas.buf[0:dataset_length])

	err = sas.readBytes(file_type_offset, file_type_length)
	if err != nil {
		return err
	}
	sas.FileType = string(sas.buf[0:file_type_length])

	x, err := sas.readFloat(date_created_offset+align1, date_created_length)
	if err != nil {
		return err
	}
	sas.DateCreated = date_time(x)

	x, err = sas.readFloat(date_modified_offset+align1, date_modified_length)
	if err != nil {
		return err
	}
	sas.DateModified = date_time(x)

	prop.headerLength, err = sas.readInt(header_size_offset+align1, header_size_length)
	if err != nil {
		return fmt.Errorf("Unable to read header size\n")
	}
	if sas.U64 && prop.headerLength != 8192 {
		os.Stderr.WriteString(fmt.Sprintf("header length %d != 8192\n", prop.headerLength))
	}

	// Read the rest of the header into cachedPage.
	v := make([]byte, prop.headerLength-288)
	if _, err := sas.file.Read(v); err != nil {
		return err
	}
	sas.cachedPage = append(sas.cachedPage, v...)
	if len(sas.cachedPage) != prop.headerLength {
		return fmt.Errorf("The SAS7BDAT file appears to be truncated.")
	}

	prop.pageLength, err = sas.readInt(page_size_offset+align1, page_size_length)
	if err != nil {
		return fmt.Errorf("Unable to read the page size value.")
	}
	prop.pageCount, err = sas.readInt(page_count_offset+align1, page_count_length)
	if err != nil {
		return fmt.Errorf("Unable to read the page count value.")
	}

	err = sas.readBytes(sas_release_offset+total_align, sas_release_length)
	if err != nil {
		return fmt.Errorf("Unable to read SAS relase value.")
	}
	sas.SASRelease = string(sas.buf[0:sas_release_length])

	err = sas.readBytes(sas_server_type_offset+total_align, sas_server_type_length)
	if err != nil {
		return fmt.Errorf("Unable to read SAS server type value.")
	}
	sas.ServerType = string(bytes.TrimRight(sas.buf[0:sas_server_type_length], " \000"))

	err = sas.readBytes(os_version_number_offset+total_align, os_version_number_length)
	if err != nil {
		return fmt.Errorf("Unable to read version number.")
	}
	sas.OSType = string(bytes.TrimRight(sas.buf[0:os_version_number_length], "\000"))

	err = sas.readBytes(os_name_offset+total_align, os_name_length)
	if err != nil {
		return fmt.Errorf("Unable to read OS name.")
	}
	if sas.buf[0] != 0 {
		sas.OSName = string(bytes.TrimRight(sas.buf[0:os_name_length], " \000"))
	} else {
		err = sas.readBytes(os_maker_offset+total_align, os_maker_length)
		if err != nil {
			return fmt.Errorf("Unable to read OS maker value.")
		}
		sas.OSName = string(bytes.TrimRight(sas.buf[0:os_maker_length], " \000"))
	}

	return nil
}

func (sas *SAS7BDAT) readPageHeader() error {

	bitOffset := sas.properties.pageBitOffset
	var err error
	sas.currentPageType, err = sas.readInt(page_type_offset+bitOffset, page_type_length)
	if err != nil {
		return fmt.Errorf("Unable to read page type value.")
	}
	sas.currentPageBlockCount, err = sas.readInt(block_count_offset+bitOffset, block_count_length)
	if err != nil {
		return fmt.Errorf("Unable to read block count value.")
	}
	sas.currentPageSubheadersCount, err = sas.readInt(subheader_count_offset+bitOffset, subheader_count_length)
	if err != nil {
		return fmt.Errorf("Unable to read subheader count value.")
	}

	return nil
}

func (sas *SAS7BDAT) processPageMetadata() error {

	bit_offset := sas.properties.pageBitOffset

	for i := 0; i < sas.currentPageSubheadersCount; i++ {
		pointer, err := sas.processSubheaderPointers(subheader_pointers_offset+bit_offset, i)
		if err != nil {
			return err
		}
		if pointer.length == 0 || pointer.compression == truncated_subheader_id {
			continue
		}
		subheader_signature, err := sas.readSubheaderSignature(pointer.offset)
		if err != nil {
			return err
		}
		subheader_index, err := sas.getSubheaderIndex(subheader_signature,
			pointer.compression, pointer.ptype)
		if err != nil {
			return fmt.Errorf("unknown subheader: %v\n", subheader_signature)
		}
		err = sas.processSubheader(subheader_index, pointer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sas *SAS7BDAT) processSubheader(subheader_index int, pointer *subheaderPointer) error {

	var processor func(int, int) error
	offset := pointer.offset
	length := pointer.length

	switch subheader_index {
	default:
		return fmt.Errorf("unknown index type")
	case rowSizeIndex:
		processor = sas.processRowSizeSubheader
	case columnSizeIndex:
		processor = sas.processColumnsizeSubheader
	case columnTextIndex:
		processor = sas.processColumnTextSubheader
	case columnNameIndex:
		processor = sas.processColumnNameSubheader
	case columnAttributesIndex:
		processor = sas.processColumnAttributesSubheader
	case formatAndLabelIndex:
		processor = sas.processFormatSubheader
	case columnListIndex:
		processor = sas.processColumnListSubheader
	case subheaderCountsIndex:
		processor = sas.processSubheaderCounts
	case dataSubheaderIndex:
		sas.currentPageDataSubheaderPointers = append(sas.currentPageDataSubheaderPointers, pointer)
		return nil
	}

	err := processor(offset, length)
	if err != nil {
		return err
	}

	return nil
}

func (sas *SAS7BDAT) readSubheaderSignature(offset int) ([]byte, error) {

	length := sas.properties.intLength
	err := sas.readBytes(offset, length)
	if err != nil {
		return nil, err
	}
	subheader_signature := make([]byte, length)
	copy(subheader_signature, sas.buf[0:length])
	return subheader_signature, nil
}

func (sas *SAS7BDAT) processSubheaderCounts(offset, length int) error {
	return nil
}

func (sas *SAS7BDAT) processSubheaderPointers(offset, subheaderPointerIndex int) (*subheaderPointer, error) {

	length := sas.properties.intLength
	subheaderPointerLength := sas.properties.subheaderPointerLength
	totalOffset := offset + subheaderPointerLength*subheaderPointerIndex

	subheaderOffset, err := sas.readInt(totalOffset, length)
	if err != nil {
		return nil, fmt.Errorf("Unable to read subheader offset value.")
	}
	totalOffset += length

	subheaderLength, err := sas.readInt(totalOffset, length)
	if err != nil {
		return nil, fmt.Errorf("Unable to read subheader length value.")
	}
	totalOffset += length

	subheaderCompression, err := sas.readInt(totalOffset, 1)
	if err != nil {
		return nil, fmt.Errorf("Unable to read subheader compression value.")
	}
	totalOffset++

	subheaderType, err := sas.readInt(totalOffset, 1)
	if err != nil {
		return nil, fmt.Errorf("Unable to read subheader type value.")
	}

	return &subheaderPointer{subheaderOffset, subheaderLength, subheaderCompression, subheaderType}, nil
}

func (sas *SAS7BDAT) getSubheaderIndex(signature []byte, compression, ptype int) (int, error) {

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

func (sas *SAS7BDAT) processByteArrayWithData(offset, length int) error {

	var source []byte
	if sas.Compression != "" && length < sas.properties.rowLength {
		decompressor := sas.getDecompressor()
		var err error
		source, err = decompressor(sas.properties.rowLength, sas.cachedPage[offset:offset+length])
		if err != nil {
			return err
		}
	} else {
		if offset+length > len(sas.cachedPage) {
			oldPage := sas.cachedPage
			err, ok := sas.readNextPage()
			if err != nil || !ok {
				return fmt.Errorf("error reading next page - %w", err)
			}
			sas.cachedPage = append(oldPage, sas.cachedPage...)
		}
		source = sas.cachedPage[offset : offset+length]
	}

	for j := 0; j < sas.properties.columnCount; j++ {
		length := sas.columnDataLengths[j]
		if length == 0 {
			break
		}
		start := sas.columnDataOffsets[j]
		end := start + length
		temp := source[start:end]
		if sas.columns[j].ctype == SASNumericType {
			s := 8 * sas.currentRowInChunkIndex
			if sas.ByteOrder == binary.LittleEndian {
				m := 8 - length
				copy(sas.bytechunk[j][s+m:s+8], temp)
			} else {
				copy(sas.bytechunk[j][s:s+length], temp)
			}
		} else {
			if sas.TrimStrings {
				temp = bytes.TrimRight(temp, "\u0000\u0020")
			}
			if sas.TextDecoder != nil {
				var err error
				temp, err = sas.TextDecoder.Bytes(temp)
				if err != nil {
					panic(err)
				}
			}

			k, ok := sas.stringPoolR[string(temp)]
			if !ok {
				k = uint64(len(sas.stringPool))
				sas.stringPool[k] = string(temp)
				sas.stringPoolR[string(temp)] = k
			}
			sas.stringchunk[j][sas.currentRowInChunkIndex] = k
		}
	}

	sas.currentRowOnPageIndex++
	sas.currentRowInChunkIndex++
	sas.currentRowInFileIndex++
	return nil
}

func (sas *SAS7BDAT) processRowSizeSubheader(offset, length int) error {

	int_len := sas.properties.intLength
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
	sas.properties.rowLength, err = sas.readInt(offset+row_length_offset_multiplier*int_len, int_len)
	if err != nil {
		return err
	}
	sas.rowCount, err = sas.readInt(offset+row_count_offset_multiplier*int_len, int_len)
	if err != nil {
		return err
	}
	sas.properties.colCountP1, err = sas.readInt(offset+col_count_p1_multiplier*int_len, int_len)
	if err != nil {
		return err
	}
	sas.properties.colCountP2, err = sas.readInt(offset+col_count_p2_multiplier*int_len, int_len)
	if err != nil {
		return err
	}
	sas.properties.mixPageRowCount, err = sas.readInt(offset+row_count_on_mix_page_offset_multiplier*int_len, int_len)
	if err != nil {
		return err
	}
	sas.properties.lcs, err = sas.readInt(lcs_offset, 2)
	if err != nil {
		return err
	}
	sas.properties.lcp, err = sas.readInt(lcp_offset, 2)
	if err != nil {
		return err
	}

	return nil
}

func (sas *SAS7BDAT) processColumnsizeSubheader(offset, length int) error {

	intLen := sas.properties.intLength
	offset += intLen
	var err error
	sas.properties.columnCount, err = sas.readInt(offset, intLen)
	if err != nil {
		return err
	}
	if sas.properties.colCountP1+sas.properties.colCountP2 != sas.properties.columnCount {
		msg := fmt.Sprintf("Warning: column count mismatch (%d + %d != %d)\n",
			sas.properties.colCountP1, sas.properties.colCountP2, sas.properties.columnCount)
		os.Stderr.WriteString(msg)
	}

	return nil
}

func (sas *SAS7BDAT) processColumnTextSubheader(offset, length int) error {

	offset += sas.properties.intLength
	textBlockSize := length - sas.properties.intLength

	err := sas.readBytes(offset, textBlockSize)
	if err != nil {
		return fmt.Errorf("Cannot read column names strings.")
	}
	sas.columnNamesStrings = append(sas.columnNamesStrings, string(sas.buf[0:textBlockSize]))

	if len(sas.columnNamesStrings) == 1 {
		column_name := sas.columnNamesStrings[0]
		var compression_literal string
		for _, cl := range compression_literals {
			if strings.Contains(column_name, cl) {
				compression_literal = cl
				break
			}
		}
		sas.Compression = compression_literal
		offset -= sas.properties.intLength

		offset1 := offset + 16
		if sas.U64 {
			offset1 += 4
		}
		err := sas.readBytes(offset1, sas.properties.lcp)
		if err != nil {
			return err
		}
		compression_literal = strings.Trim(string(sas.buf[0:8]), "\x00")

		switch {
		case compression_literal == "":
			sas.properties.lcs = 0
			offset1 = offset + 32
			if sas.U64 {
				offset1 += 4
			}
			err = sas.readBytes(offset1, sas.properties.lcp)
			if err != nil {
				return err
			}
			sas.properties.creatorProc = string(sas.buf[0:sas.properties.lcp])
		case compression_literal == rle_compression:
			offset1 = offset + 40
			if sas.U64 {
				offset1 += 4
			}
			err = sas.readBytes(offset1, sas.properties.lcp)
			if err != nil {
				return err
			}
			sas.properties.creatorProc = string(sas.buf[0:sas.properties.lcp])
		case sas.properties.lcs > 0:
			sas.properties.lcp = 0
			offset1 = offset + 16
			if sas.U64 {
				offset1 += 4
			}
			err = sas.readBytes(offset1, sas.properties.lcs)
			if err != nil {
				return err
			}
			sas.properties.creatorProc = string(sas.buf[0:sas.properties.lcp])
		}
	}
	return nil
}

func (sas *SAS7BDAT) processColumnNameSubheader(offset, length int) error {

	intLen := sas.properties.intLength
	offset += intLen
	column_name_pointers_count := (length - 2*intLen - 12) / 8
	for i := 0; i < column_name_pointers_count; i++ {
		text_subheader := offset + column_name_pointer_length*(i+1) + column_name_text_subheader_offset
		col_name_offset := offset + column_name_pointer_length*(i+1) + column_name_offset_offset
		col_name_length := offset + column_name_pointer_length*(i+1) + column_name_length_offset

		idx, err := sas.readInt(text_subheader, column_name_text_subheader_length)
		if err != nil {
			return fmt.Errorf("Unable to read text subheader for column name.")
		}
		col_offset, err := sas.readInt(col_name_offset, column_name_offset_length)
		if err != nil {
			return fmt.Errorf("Unable to read column_name offset.")
		}
		col_len, err := sas.readInt(col_name_length, column_name_length_length)
		if err != nil {
			return fmt.Errorf("Unable to read column name length.")
		}

		name_str := sas.columnNamesStrings[idx]
		sas.columnNames = append(sas.columnNames, name_str[col_offset:col_offset+col_len])
	}

	return nil
}

func (sas *SAS7BDAT) processColumnListSubheader(offset, length int) error {
	// unknown purpose
	return nil
}

func (sas *SAS7BDAT) processColumnAttributesSubheader(offset, length int) error {

	intLen := sas.properties.intLength
	column_attributes_vectors_count := (length - 2*intLen - 12) / (intLen + 8)
	for i := 0; i < column_attributes_vectors_count; i++ {

		colDataOffset := offset + intLen + column_data_offset_offset + i*(intLen+8)
		colDataLen := offset + 2*intLen + column_data_length_offset + i*(intLen+8)
		colTypes := offset + 2*intLen + column_type_offset + i*(intLen+8)

		x, err := sas.readInt(colDataOffset, intLen)
		if err != nil {
			return err
		}
		sas.columnDataOffsets = append(sas.columnDataOffsets, x)

		x, err = sas.readInt(colDataLen, column_data_length_length)
		if err != nil {
			return err
		}
		sas.columnDataLengths = append(sas.columnDataLengths, x)

		x, err = sas.readInt(colTypes, column_type_length)
		if err != nil {
			return err
		}
		if x == 1 {
			sas.columnTypes = append(sas.columnTypes, SASNumericType)
		} else {
			sas.columnTypes = append(sas.columnTypes, SASStringType)
		}
	}

	return nil
}

func (sas *SAS7BDAT) processFormatSubheader(offset, length int) error {

	int_len := sas.properties.intLength
	text_subheader_format := offset + column_format_text_subheader_index_offset + 3*int_len
	col_format_offset := offset + column_format_offset_offset + 3*int_len
	col_format_len := offset + column_format_length_offset + 3*int_len
	text_subheader_label := offset + column_label_text_subheader_index_offset + 3*int_len
	col_label_offset := offset + column_label_offset_offset + 3*int_len
	col_label_len := offset + column_label_length_offset + 3*int_len

	format_idx, _ := sas.readInt(text_subheader_format, column_format_text_subheader_index_length)
	format_idx = min(format_idx, len(sas.columnNamesStrings)-1)

	format_start, _ := sas.readInt(col_format_offset, column_format_offset_length)
	format_len, _ := sas.readInt(col_format_len, column_format_length_length)

	label_idx, _ := sas.readInt(text_subheader_label, column_label_text_subheader_index_length)
	label_idx = min(label_idx, len(sas.columnNamesStrings)-1)

	label_start, _ := sas.readInt(col_label_offset, column_label_offset_length)
	label_len, _ := sas.readInt(col_label_len, column_label_length_length)

	label_names := sas.columnNamesStrings[label_idx]
	column_label := label_names[label_start : label_start+label_len]
	format_names := sas.columnNamesStrings[format_idx]
	column_format := format_names[format_start : format_start+format_len]
	current_column_number := len(sas.columns)

	col := &column{
		colId:  current_column_number,
		name:   sas.columnNames[current_column_number],
		label:  column_label,
		format: column_format,
		ctype:  sas.columnTypes[current_column_number],
		length: sas.columnDataLengths[current_column_number],
	}

	sas.columnLabels = append(sas.columnLabels, column_label)
	sas.ColumnFormats = append(sas.ColumnFormats, column_format)
	sas.columns = append(sas.columns, col)

	return nil
}

// RowCount returns the number of rows in the data set.
func (sas *SAS7BDAT) RowCount() int {
	return sas.rowCount
}

// ColumnNames returns the names of the columns.
func (sas *SAS7BDAT) ColumnNames() []string {
	return sas.columnNames
}

// ColumnLabels returns the column labels.
func (sas *SAS7BDAT) ColumnLabels() []string {
	return sas.columnLabels
}

// ColumnTypes returns integer codes for the column data types.
func (sas *SAS7BDAT) ColumnTypes() []ColumnTypeT {
	return sas.columnTypes
}

func (sas *SAS7BDAT) parseMetadata() error {

	for {
		n, err := sas.file.Read(sas.cachedPage)
		if n <= 0 {
			break
		}
		if err != nil {
			return err
		}
		if n != sas.properties.pageLength {
			return fmt.Errorf("Failed to read a meta data page from the SAS file.")
		}
		var done bool
		if done, err = sas.processPageMeta(); err != nil {
			return err
		}
		if done {
			break
		}
	}

	return nil
}

func (sas *SAS7BDAT) processPageMeta() (bool, error) {

	if err := sas.readPageHeader(); err != nil {
		return false, err
	}

	if isPageMetaMixAmd(sas.currentPageType) {
		if err := sas.processPageMetadata(); err != nil {
			return false, err
		}
	}

	return sas.isPageMixDataType(sas.currentPageType) || sas.currentPageDataSubheaderPointers != nil, nil
}

func isPageMetaMixAmd(pagetype int) bool {
	switch pagetype {
	case page_meta_type, 512, 640, page_amd_type:
		return true
	}
	return false
}

func (sas *SAS7BDAT) isPageMixType(val int) bool {
	switch val {
	case 512, 640:
		return true
	}
	return false
}

func (sas *SAS7BDAT) isPageMixDataType(val int) bool {
	switch val {
	case 512, 640, 256:
		return true
	}
	return false
}

func checkPageType(current_page int) bool {
	switch current_page {
	case page_meta_type, page_data_type, 512, 640:
		return false
	}
	return true
}
