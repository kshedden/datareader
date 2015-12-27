package datareader

//

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
)

// A Reader specifies how a data set can be read from a file.
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

	// The reader object provided by the caller.
	reader *io.ReadSeeker
}

// NewReader returns a dataframe.Reader that reads from r.
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
			n_floats := count_floats(data)
			n := len(data)

			if n_floats[j] == n {
				rdr.data_types[j] = "float64"
			} else {
				rdr.data_types[j] = "string"
			}
		}
	}

	return nil
}

// seek_data moves the io.reader to the beginning of the first row of
// data and returns a csv.Reader for reading the data.
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

// Read reads up to the given number of lines of data and returns the
// results.  If lines is negative the whole file is read.
func (rdr *CSVReader) Read(lines int) ([]*Series, error) {

	rdr.init()

	// Create a structure to hold the data.  For efficiency, start
	// with an array of arrays.
	data_array := make([]interface{}, len(rdr.ColumnNames))
	for j := range rdr.ColumnNames {
		switch rdr.data_types[j] {
		case "float64":
			data_array[j] = make([]float64, 0, 1000)
		case "string":
			data_array[j] = make([]string, 0, 1000)
		}
	}

	rdr.init()
	c, err := rdr.seek_data()
	if err != nil {
		return nil, err
	}

	dlines, _ := c.ReadAll()

	num_read := 0
	for _, line := range dlines {
		for j := range rdr.ColumnNames {
			switch rdr.data_types[j] {
			case "float64":
				x, err := strconv.ParseFloat(line[j], 64)
				if err != nil {
					x = math.NaN()
				}
				data_array[j] = append(data_array[j].([]float64), x)
			case "string":
				data_array[j] = append(data_array[j].([]string), line[j])
			}
		}

		num_read += 1
		if (lines >= 0) && (num_read >= lines) {
			break
		}
	}

	data_series := make([]*Series, len(data_array))
	for j := 0; j < len(data_array); j++ {
		name := fmt.Sprintf("col%d", j)
		var err error
		data_series[j], err = NewSeries(name, data_array[j], nil)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
	}
	return data_series, nil
}

// count_floats returns the number of elements of each column of array
// that can be converted to float64 type.
func count_floats(array [][]string) []int {

	num_floats := make([]int, len(array[0]))

	for _, x := range array {
		for j, y := range x {
			_, err := strconv.ParseFloat(y, 64)
			if err == nil {
				num_floats[j] += 1
			}
		}
	}

	return num_floats
}
