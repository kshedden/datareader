package datareader

import (
	"encoding/csv"
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
	initRun bool

	// Cached lines
	lines [][]string

	// The reader object provided by the caller.
	reader *io.Reader

	// The underlying csv Reader object
	csvreader *csv.Reader

	// Workspace
	dataArray []interface{}
	miss      [][]bool
	numRows   int
}

// NewCSVReader returns a CSVReader that reads CSV data from the given io.reader,
// with type inference and chunking.
func NewCSVReader(r io.Reader) *CSVReader {

	rdr := new(CSVReader)
	rdr.HasHeader = true
	rdr.reader = &r

	rdr.csvreader = csv.NewReader(*rdr.reader)
	rdr.csvreader.FieldsPerRecord = -1

	return rdr
}

func (rdr *CSVReader) getColumnNames() error {

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

func (rdr *CSVReader) sniffTypes() {

	nFloats, nObs := rdr.countFloats()

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
			if (nFloats[j] == nObs[j]) && (nObs[j] > 0) {
				rdr.DataTypes[j] = "float64"
			} else {
				rdr.DataTypes[j] = "string"
			}
		}
	}
}

func (rdr *CSVReader) rectifyLines() {

	mx := 0

	for _, line := range rdr.lines {
		if len(line) > mx {
			mx = len(line)
		}
	}

	for _, line := range rdr.lines {
		for {
			if len(line) >= mx {
				break
			}
			line = append(line, "")
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

	rdr.rectifyLines()

	if len(rdr.lines) == 0 {
		return fmt.Errorf("file appears to be empty")
	}

	if rdr.ColumnNames == nil {
		err := rdr.getColumnNames()
		if err != nil {
			return err
		}
	}

	if rdr.DataTypes == nil {
		rdr.sniffTypes()
	}

	rdr.initRun = true

	return nil
}

func (rdr *CSVReader) ensureWidth(w int) {

	if len(rdr.ColumnNames) >= w {
		return
	}

	for k := len(rdr.ColumnNames); k < w; k++ {
		rdr.ColumnNames = append(rdr.ColumnNames, fmt.Sprintf("Column %d", k+1))
		rdr.DataTypes = append(rdr.DataTypes, "string")
	}

	for j := 0; j < w; j++ {
		if len(rdr.dataArray) <= j {
			switch rdr.DataTypes[j] {
			case "float64":
				rdr.dataArray = append(rdr.dataArray, make([]float64, rdr.numRows))
			case "string":
				rdr.dataArray = append(rdr.dataArray, make([]string, rdr.numRows))
			}
			miss := make([]bool, rdr.numRows)
			for i := 0; i < rdr.numRows; i++ {
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

	if !rdr.initRun {
		err := rdr.init()
		if err != nil {
			return nil, err
		}
	}

	rdr.dataArray = make([]interface{}, len(rdr.ColumnNames))
	rdr.miss = make([][]bool, len(rdr.ColumnNames))
	for j := range rdr.ColumnNames {
		switch rdr.DataTypes[j] {
		case "float64":
			rdr.dataArray[j] = make([]float64, 0, 100)
		case "string":
			rdr.dataArray[j] = make([]string, 0, 100)
		}
		rdr.miss[j] = make([]bool, 0, 100)
	}

	for {
		if lines > 0 && rdr.numRows >= lines {
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
			rdr.ensureWidth(len(line))
		}

		for j := range rdr.ColumnNames {
			switch rdr.DataTypes[j] {
			case "float64":
				if j >= len(line) {
					rdr.dataArray[j] = append(rdr.dataArray[j].([]float64), 0)
					rdr.miss[j] = append(rdr.miss[j], true)
				} else {
					x, err := strconv.ParseFloat(line[j], 64)
					if err != nil {
						rdr.miss[j] = append(rdr.miss[j], true)
					} else {
						rdr.miss[j] = append(rdr.miss[j], false)
					}
					rdr.dataArray[j] = append(rdr.dataArray[j].([]float64), x)
				}
			case "string":
				if j >= len(line) {
					rdr.dataArray[j] = append(rdr.dataArray[j].([]string), "")
					rdr.miss[j] = append(rdr.miss[j], true)
				} else {
					rdr.miss[j] = append(rdr.miss[j], false)
					rdr.dataArray[j] = append(rdr.dataArray[j].([]string), line[j])
				}
			}
		}

		rdr.numRows++
	}

	dataSeries := make([]*Series, len(rdr.dataArray))
	for j := 0; j < len(rdr.dataArray); j++ {
		var name string
		if len(rdr.ColumnNames) >= j {
			name = rdr.ColumnNames[j]
		} else {
			name = fmt.Sprintf("Column %d", j+1)
		}
		var err error
		dataSeries[j], err = NewSeries(name, rdr.dataArray[j], rdr.miss[j])
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
	}
	return dataSeries, nil
}

// countFloats returns the number of elements of each column of array
// that can be converted to float64 type.
func (rdr *CSVReader) countFloats() ([]int, []int) {

	// Find the longest record in the cache
	m := 0
	for _, v := range rdr.lines {
		if len(v) > m {
			m = len(v)
		}
	}

	numFloats := make([]int, m)
	numObs := make([]int, m)

	for _, x := range rdr.lines {
		for j, y := range x {
			y = strings.TrimSpace(y)
			// Skip blanks
			if len(y) == 0 {
				continue
			}
			numObs[j] += 1
			_, err := strconv.ParseFloat(y, 64)
			if err == nil {
				numFloats[j] += 1
			}
		}
	}

	return numFloats, numObs
}
