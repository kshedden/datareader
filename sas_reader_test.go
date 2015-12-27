package datareader

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func sas_base_test(fname_csv, fname_sas string) bool {

	f, err := os.Open(filepath.Join("test_files", fname_csv))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}

	rt := NewCSVReader(f)
	rt.HasHeader = false
	dt, err := rt.Read(-1)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}

	r, err := os.Open(filepath.Join("test_files", fname_sas))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}

	sas, err := NewSAS7BDATReader(r)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}
	sas.TrimStrings = true

	ncol := len(sas.ColumnNames())

	ds, err := sas.Read(10000)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}

	if len(ds) != len(dt) {
		return false
	}

	base := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)

	for j := 0; j < ncol; j++ {
		switch sas.ColumnFormats[j] {
		default:
			os.Stderr.WriteString(fmt.Sprintf("unknown format for column %d: %s\n", j, sas.ColumnFormats[j]))
		case "":
			if !ds[j].AllEqual(dt[j]) {
				a := ds[j].Data().([]string)
				b := dt[j].Data().([]string)
				for i := 0; i < ds[j].Length(); i++ {
					fmt.Printf("%v %v %v %v\n", a[i], b[i], len(a[i]), len(b[i]))
				}
				return false
			}
		case "MMDDYY":
			vec := ds[j].Data().([]float64)
			n := len(vec)
			vect := dt[j].Data().([]float64)
			for i := 0; i < n; i++ {
				t1 := base.Add(time.Duration(vec[i]) * 24 * time.Hour)
				d := fmt.Sprintf("%8.0f", vect[i])
				year, _ := strconv.Atoi(d[0:4])
				month, _ := strconv.Atoi(d[4:6])
				day, _ := strconv.Atoi(d[6:8])
				t2 := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
				if t1 != t2 {
					return false
				}
			}
		}
	}

	return true
}

func TestSAS1(t *testing.T) {

	r := sas_base_test("test1.csv", "test1_compression_no.sas7bdat")
	if !r {
		t.Fail()
	}
}

func TestSAS2(t *testing.T) {

	r := sas_base_test("test1.csv", "test1_compression_char.sas7bdat")
	if !r {
		t.Fail()
	}
}

func TestSAS3(t *testing.T) {

	r := sas_base_test("test1.csv", "test1_compression_binary.sas7bdat")
	if !r {
		t.Fail()
	}
}
