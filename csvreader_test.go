package datareader

import (
	//"fmt"
	"math"
	"os"
	"testing"
)

func Test1(t *testing.T) {

	file, _ := os.Open("test1.csv")
	rdr := NewCSVReader(file)
	data := rdr.Read(-1)

	expected := make([]*Series, 3)
	expected[0], _ = NewSeries("", []float64{1, 4, 7}, nil)
	expected[1], _ = NewSeries("", []float64{2, 5, 8}, nil)
	expected[2], _ = NewSeries("", []float64{3, 6, 9}, nil)

	if !SeriesArray(data).AllEqual(expected) {
		t.Fail()
	}
}

func Test2(t *testing.T) {

	file, _ := os.Open("test1.csv")
	rdr := NewCSVReader(file)
	rdr.HasHeader = false
	data := rdr.Read(-1)

	expected := make([]*Series, 3)
	expected[0], _ = NewSeries("", []string{"a", "1", "4", "7"}, nil)
	expected[1], _ = NewSeries("", []string{"b", "2", "5", "8"}, nil)
	expected[2], _ = NewSeries("", []string{"c", "3", "6", "9"}, nil)

	if !SeriesArray(data).AllEqual(expected) {
		t.Fail()
	}
}

func Test3(t *testing.T) {

	file, _ := os.Open("test1.csv")
	rdr := NewCSVReader(file)
	rdr.HasHeader = false
	rdr.SkipRows = 2
	data := rdr.Read(-1)

	expected := make([]*Series, 3)
	expected[0], _ = NewSeries("", []float64{4, 7}, nil)
	expected[1], _ = NewSeries("", []float64{5, 8}, nil)
	expected[2], _ = NewSeries("", []float64{6, 9}, nil)

	if !SeriesArray(data).AllEqual(expected) {
		t.Fail()
	}
}

func Test4(t *testing.T) {

	file, _ := os.Open("test1.csv")
	rdr := NewCSVReader(file)
	rdr.HasHeader = false
	rdr.TypeHintsName = map[string]string{
		"Column 1": "float64",
		"Column 2": "float64",
		"Column 3": "float64"}

	data := rdr.Read(-1)

	expected := make([]*Series, 3)
	expected[0], _ = NewSeries("", []float64{math.NaN(), 1, 4, 7}, nil)
	expected[1], _ = NewSeries("", []float64{math.NaN(), 2, 5, 8}, nil)
	expected[2], _ = NewSeries("", []float64{math.NaN(), 3, 6, 9}, nil)

	if !SeriesArray(data).AllEqual(expected) {
		t.Fail()
	}
}
