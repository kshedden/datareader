package datareader

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// A CSVReader specifies how a data set in CSV format can be read from
// a text file.
type CSVReader struct {

	// Skip this number of rows before reading the header.
	SkipRows int

	// If true, there is a header to read, otherwise default column names are used
	HasHeader bool

	// The column names, in the order that they appear in the file.
	ColumnNames []string

	// User-specified data types (maps column name to type name).
	TypeHintsName map[string]string

	// User-specified data types (indexed by column number).
	TypeHintsPos []string

	// The data type for each column.
	data_types []string

	// Has the init method been run yet?
	init_run bool

	// The reader object provided by the caller.
	reader *io.ReadSeeker
}

// NewReader returns a dataframe.CSVReader that reads CSV data from r
// with type inference and chunking.
func NewCSVReader(r io.ReadSeeker) *CSVReader {
	rdr := new(CSVReader)
	rdr.HasHeader = true
	rdr.reader = &r
	return rdr
}

func (rdr *CSVReader) get_column_names() error {

	(*rdr.reader).Seek(0, 0)
	c := csv.NewReader((*rdr.reader).(io.Reader))

	// Skip rows as requested.
	for k := 0; k < rdr.SkipRows; k++ {
		_, err := c.Read()
		if err == io.EOF {
			return errors.New(fmt.Sprintf("SkipRows=%d is greater than the file length\n", rdr.SkipRows))
		} else if err != nil {
			return err
		}
	}

	// The next line determines the number of columns, even if it is not the header.
	line, err := c.Read()
	if err == io.EOF {
		return errors.New("Reached end of file before finding data\n")
	} else if err != nil {
		return err
	}

	if rdr.HasHeader {
		rdr.ColumnNames = line
		return nil
	}

	// Default names
	rdr.ColumnNames = make([]string, len(line))
	for k := 0; k < len(line); k++ {
		rdr.ColumnNames[k] = fmt.Sprintf("Column %d", k+1)
	}

	return nil
}

func (rdr *CSVReader) sniff_types() error {

	c, err := rdr.seek_data()
	if err != nil {
		return err
	}

	// Read up to 100 lines
	data := make([][]string, 0, 100)
	for {
		line, err := c.Read()
		if err != nil {
			break
		}
		data = append(data, line)
	}

	rdr.data_types = make([]string, len(rdr.ColumnNames))
	for j, col := range rdr.ColumnNames {

		// Check for a type hint
		t := "infer"
		tm, ok := rdr.TypeHintsName[col]
		if ok {
			t = tm
		} else if len(rdr.TypeHintsPos) >= j+1 {
			if rdr.TypeHintsPos[j] != "" {
				t = rdr.TypeHintsPos[j]
			}
		}

		if t != "infer" {
			rdr.data_types[j] = t
		} else {
			n_floats, n_obs := count_floats(data)

			if (n_floats[j] == n_obs[j]) && (n_obs[j] > 0) {
				rdr.data_types[j] = "float64"
			} else {
				rdr.data_types[j] = "string"
			}
		}
	}

	return nil
}

func (rdr *CSVReader) seek_data() (*csv.Reader, error) {

	(*rdr.reader).Seek(0, 0)
	c := csv.NewReader(*rdr.reader)

	// Skip rows as requested.
	for k := 0; k < rdr.SkipRows; k++ {
		_, err := c.Read()
		if err != nil {
			return nil, err
		}
	}

	if rdr.HasHeader {
		_, err := c.Read()
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

// init performs some initializations before reading data.
func (rdr *CSVReader) init() error {

	if rdr.ColumnNames == nil {
		err := rdr.get_column_names()
		if err != nil {
			return err
		}
	}

	if rdr.data_types == nil {
		err := rdr.sniff_types()
		if err != nil {
			return err
		}
	}

	return nil
}

// Read reads up lines rows of data and returns the results as an
// array of Series objects.  If lines is negative the whole file is
// read.  Data types of the Series objects are inferred from the file.
// Use type hints in the CSVReader struct to control the types
// directly.
func (rdr *CSVReader) Read(lines int) ([]*Series, error) {

	if !rdr.init_run {
		rdr.init()
		rdr.init_run = true
	}

	data_array := make([]interface{}, len(rdr.ColumnNames))
	miss := make([][]bool, len(rdr.ColumnNames))
	for j := range rdr.ColumnNames {
		switch rdr.data_types[j] {
		case "float64":
			data_array[j] = make([]float64, 0, 100)
		case "string":
			data_array[j] = make([]string, 0, 100)
		}
		miss[j] = make([]bool, 0, 100)
	}

	rdr.init()
	c, err := rdr.seek_data()
	if err != nil {
		return nil, err
	}

	dlines, err := c.ReadAll()
	if err != nil {
		return nil, err
	}

	num_read := 0
	for j := range rdr.ColumnNames {
		num_read = 0
		switch rdr.data_types[j] {
		case "float64":
			da := make([]float64, 0, 10)
			num_read = 0
			for i, line := range dlines {
				num_read++
				x, err := strconv.ParseFloat(line[j], 64)
				if err != nil {
					miss[j] = append(miss[j], true)
				} else {
					miss[j] = append(miss[j], false)
				}
				da = append(da, x)
				if (lines >= 0) && (i >= lines) {
					break
				}
			}
			data_array[j] = da
		case "string":
			da := make([]string, 0, 10)
			num_read = 0
			for i, line := range dlines {
				num_read++
				da = append(da, line[j])
				miss[j] = append(miss[j], false)
				if (lines >= 0) && (i >= lines) {
					break
				}
			}
			data_array[j] = da
		}
	}

	data_series := make([]*Series, len(data_array))
	for j := 0; j < len(data_array); j++ {
		name := fmt.Sprintf("Column %d", j+1)
		var err error
		data_series[j], err = NewSeries(name, data_array[j], miss[j])
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
	}
	return data_series, nil
}

// count_floats returns the number of elements of each column of array
// that can be converted to float64 type.
func count_floats(array [][]string) ([]int, []int) {

	num_floats := make([]int, len(array[0]))
	num_obs := make([]int, len(array[0]))

	for _, x := range array {
		for j, y := range x {
			y = strings.TrimSpace(y)
			if len(y) == 0 {
				continue
			}
			num_obs[j] += 1
			_, err := strconv.ParseFloat(y, 64)
			if err == nil {
				num_floats[j] += 1
			}
		}
	}

	return num_floats, num_obs
}
