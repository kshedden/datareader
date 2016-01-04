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

	// The column names, in the order that they appear in the
	// file.  Can be set by caller.
	ColumnNames []string

	// User-specified data types (maps column name to type name).
	TypeHintsName map[string]string

	// User-specified data types (indexed by column number).
	TypeHintsPos []string

	// The data type for each column.
	DataTypes []string

	// Has the init method been run yet?
	init_run bool

	// Cached lines
	lines [][]string

	// The reader object provided by the caller.
	reader *io.Reader

	// The underlying csv Reader object
	csvreader *csv.Reader

	// Workspace
	data_array []interface{}
	miss       [][]bool
	num_rows   int
}

// NewReader returns a dataframe.CSVReader that reads CSV data from r
// with type inference and chunking.
func NewCSVReader(r io.Reader) *CSVReader {
	rdr := new(CSVReader)
	rdr.HasHeader = true
	rdr.reader = &r

	rdr.csvreader = csv.NewReader(*rdr.reader)
	rdr.csvreader.FieldsPerRecord = -1

	return rdr
}

func (rdr *CSVReader) get_column_names() error {

	if rdr.HasHeader {
		rdr.ColumnNames = rdr.lines[0]
		rdr.lines = rdr.lines[1:]
		return nil
	}

	// Default names
	m := len(rdr.lines[0])
	rdr.ColumnNames = make([]string, m)
	for k := 0; k < m; k++ {
		rdr.ColumnNames[k] = fmt.Sprintf("Column %d", k+1)
	}

	return nil
}

func (rdr *CSVReader) sniff_types() {

	n_floats, n_obs := rdr.count_floats()

	rdr.DataTypes = make([]string, len(rdr.ColumnNames))
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
			rdr.DataTypes[j] = t
		} else {
			if (n_floats[j] == n_obs[j]) && (n_obs[j] > 0) {
				rdr.DataTypes[j] = "float64"
			} else {
				rdr.DataTypes[j] = "string"
			}
		}
	}
}

// init performs some initializations before reading data.
func (rdr *CSVReader) init() error {

	// Read up to 100 lines.
	rdr.lines = make([][]string, 0, 100)
	for k := 0; k < 100+rdr.SkipRows; k++ {
		v, err := rdr.csvreader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if k >= rdr.SkipRows {
			rdr.lines = append(rdr.lines, v)
		}
	}

	if len(rdr.lines) == 0 {
		return errors.New("file appears to be empty")
	}

	if rdr.ColumnNames == nil {
		err := rdr.get_column_names()
		if err != nil {
			return err
		}
	}

	if rdr.DataTypes == nil {
		rdr.sniff_types()
	}

	rdr.init_run = true

	return nil
}

func (rdr *CSVReader) ensure_width(w int) {

	if len(rdr.ColumnNames) >= w {
		return
	}

	for k := len(rdr.ColumnNames); k < w; k++ {
		rdr.ColumnNames = append(rdr.ColumnNames, fmt.Sprintf("Column %d", k+1))
		rdr.DataTypes = append(rdr.DataTypes, "string")
	}

	for j := 0; j < w; j++ {
		if len(rdr.data_array) <= j {
			switch rdr.DataTypes[j] {
			case "float64":
				rdr.data_array = append(rdr.data_array, make([]float64, rdr.num_rows))
			case "string":
				rdr.data_array = append(rdr.data_array, make([]string, rdr.num_rows))
			}
			miss := make([]bool, rdr.num_rows)
			for i := 0; i < rdr.num_rows; i++ {
				miss[i] = true
			}
			rdr.miss = append(rdr.miss, miss)
		}
	}
}

// Read reads up lines rows of data and returns the results as an
// array of Series objects.  If lines is negative the whole file is
// read.  Data types of the Series objects are inferred from the file.
// Use type hints in the CSVReader struct to control the types
// directly.
func (rdr *CSVReader) Read(lines int) ([]*Series, error) {

	if !rdr.init_run {
		err := rdr.init()
		if err != nil {
			return nil, err
		}
	}

	rdr.data_array = make([]interface{}, len(rdr.ColumnNames))
	rdr.miss = make([][]bool, len(rdr.ColumnNames))
	for j := range rdr.ColumnNames {
		switch rdr.DataTypes[j] {
		case "float64":
			rdr.data_array[j] = make([]float64, 0, 100)
		case "string":
			rdr.data_array[j] = make([]string, 0, 100)
		}
		rdr.miss[j] = make([]bool, 0, 100)
	}

	for {
		if (lines > 0) && (rdr.num_rows >= lines) {
			break
		}

		var line []string
		var err error
		if len(rdr.lines) > 0 {
			line = rdr.lines[0]
			rdr.lines = rdr.lines[1:]
		} else {
			line, err = rdr.csvreader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			rdr.ensure_width(len(line))
		}

		for j := range rdr.ColumnNames {
			switch rdr.DataTypes[j] {
			case "float64":
				x, err := strconv.ParseFloat(line[j], 64)
				if err != nil {
					rdr.miss[j] = append(rdr.miss[j], true)
				} else {
					rdr.miss[j] = append(rdr.miss[j], false)
				}
				rdr.data_array[j] = append(rdr.data_array[j].([]float64), x)
			case "string":
				rdr.miss[j] = append(rdr.miss[j], false)
				rdr.data_array[j] = append(rdr.data_array[j].([]string), line[j])
			}
		}

		rdr.num_rows++
	}

	data_series := make([]*Series, len(rdr.data_array))
	for j := 0; j < len(rdr.data_array); j++ {
		var name string
		if len(rdr.ColumnNames) >= j {
			name = rdr.ColumnNames[j]
		} else {
			name = fmt.Sprintf("Column %d", j+1)
		}
		var err error
		data_series[j], err = NewSeries(name, rdr.data_array[j], rdr.miss[j])
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
	}
	return data_series, nil
}

// count_floats returns the number of elements of each column of array
// that can be converted to float64 type.
func (rdr *CSVReader) count_floats() ([]int, []int) {

	// Find the longest record in the cache
	m := 0
	for _, v := range rdr.lines {
		if len(v) > m {
			m = len(v)
		}
	}

	num_floats := make([]int, m)
	num_obs := make([]int, m)

	for _, x := range rdr.lines {
		for j, y := range x {
			y = strings.TrimSpace(y)
			// Skip blanks
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
