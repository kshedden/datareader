package datareader

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestCSV1(t *testing.T) {

	file, err := os.Open(filepath.Join("test_files", "testcsv1.csv"))
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

	if !SeriesArray(data).AllEqual(expected) {
		t.Fail()
	}
}

func TestCSV2(t *testing.T) {

	file, err := os.Open(filepath.Join("test_files", "testcsv2.csv"))
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

	if !SeriesArray(data).AllEqual(expected) {
		t.Fail()
	}
}

func TestCSV3(t *testing.T) {

	file, err := os.Open(filepath.Join("test_files", "testcsv2.csv"))
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

	if !SeriesArray(data).AllEqual(expected) {
		t.Fail()
	}
}

func TestCSV4(t *testing.T) {

	file, err := os.Open(filepath.Join("test_files", "testcsv2.csv"))
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
	expected[0], _ = NewSeries("", []float64{math.NaN(), 1, 4, 7}, nil)
	expected[1], _ = NewSeries("", []float64{math.NaN(), 2, 5, 8}, nil)
	expected[2], _ = NewSeries("", []float64{math.NaN(), 3, 6, 9}, nil)

	if !SeriesArray(data).AllEqual(expected) {
		t.Fail()
	}
}
