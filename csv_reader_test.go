package datareader

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestCSV1(t *testing.T) {

	file, err := os.Open(filepath.Join("test_files", "data", "testcsv1.csv"))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		t.Fail()
	}
	rdr := NewCSVReader(file)
	data, err := rdr.Read(-1)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		t.Fail()
	}

	expected := make([]*Series, 3)
	expected[0], _ = NewSeries("Var1", []float64{1, 4, 7}, nil)
	expected[1], _ = NewSeries("Var2", []float64{2, 5, 8}, nil)
	expected[2], _ = NewSeries("Var3", []float64{3, 6, 9}, nil)

	f, _, _ := SeriesArray(data).AllEqual(expected)
	if !f {
		t.Fail()
	}
}

func TestCSV2(t *testing.T) {

	file, err := os.Open(filepath.Join("test_files", "data", "testcsv2.csv"))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		t.Fail()
	}
	rdr := NewCSVReader(file)
	rdr.HasHeader = false
	data, err := rdr.Read(-1)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		t.Fail()
	}

	expected := make([]*Series, 3)
	expected[0], _ = NewSeries("", []string{"a", "1", "4", "7"}, nil)
	expected[1], _ = NewSeries("", []string{"b", "2", "5", "8"}, nil)
	expected[2], _ = NewSeries("", []string{"c", "3", "6", "9"}, nil)

	f, _, _ := SeriesArray(data).AllEqual(expected)
	if !f {
		t.Fail()
	}
}

func TestCSV3(t *testing.T) {

	file, err := os.Open(filepath.Join("test_files", "data", "testcsv2.csv"))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		t.Fail()
	}

	rdr := NewCSVReader(file)
	rdr.HasHeader = false
	rdr.SkipRows = 2
	data, err := rdr.Read(-1)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		t.Fail()
	}

	expected := make([]*Series, 3)
	expected[0], _ = NewSeries("", []float64{4, 7}, nil)
	expected[1], _ = NewSeries("", []float64{5, 8}, nil)
	expected[2], _ = NewSeries("", []float64{6, 9}, nil)

	f, _, _ := SeriesArray(data).AllEqual(expected)
	if !f {
		t.Fail()
	}
}

func TestCSV4(t *testing.T) {

	file, err := os.Open(filepath.Join("test_files", "data", "testcsv2.csv"))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		t.Fail()
	}

	rdr := NewCSVReader(file)
	rdr.HasHeader = false
	rdr.TypeHintsName = map[string]string{
		"Column 1": "float64",
		"Column 2": "float64",
		"Column 3": "float64"}

	data, err := rdr.Read(-1)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		t.Fail()
	}

	expected := make([]*Series, 3)
	miss := []bool{true, false, false, false}
	expected[0], _ = NewSeries("", []float64{0, 1, 4, 7}, miss)
	expected[1], _ = NewSeries("", []float64{0, 2, 5, 8}, miss)
	expected[2], _ = NewSeries("", []float64{0, 3, 6, 9}, miss)

	f, _, _ := SeriesArray(data).AllEqual(expected)
	if !f {
		t.Fail()
	}
}

func TestRaggedCSV(t *testing.T) {

	file, err := os.Open(filepath.Join("test_files", "data", "testcsv3.csv"))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		t.Fail()
	}

	rdr := NewCSVReader(file)
	rdr.HasHeader = true

	data, err := rdr.Read(-1)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		t.Fail()
	}
	for j := 0; j < len(data); j++ {
		data[j] = data[j].ForceNumeric()
	}

	expected := make([]*Series, 4)
	expected[0], _ = NewSeries("a", []float64{1, 2, 3},
		[]bool{false, false, false})
	expected[1], _ = NewSeries("b", []float64{2, 3, 4},
		[]bool{false, false, false})
	expected[2], _ = NewSeries("c", []float64{0, 4, 5},
		[]bool{true, false, false})
	expected[3], _ = NewSeries("c", []float64{0, 0, 6},
		[]bool{true, true, false})

	f, _, _ := SeriesArray(data).AllEqual(expected)
	if !f {
		t.Fail()
	}
}
