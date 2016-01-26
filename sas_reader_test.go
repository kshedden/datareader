package datareader

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func to_date_yyyymmdd(vec []float64) []time.Time {

	n := len(vec)
	rslt := make([]time.Time, n)

	for i, x := range vec {
		if math.IsNaN(x) {
			continue
		}
		y := int(x)
		day := y % 100
		y = (y - day) / 100
		month := y % 100
		y = (y - month) / 100
		year := y
		rslt[i] = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	}

	return rslt
}

func max(x, y int) int {
	if x >= y {
		return x
	}
	return y
}

func sas_base_test(fname_csv, fname_sas string) bool {

	f, err := os.Open(filepath.Join("test_files", "data", fname_csv))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}
	defer f.Close()

	rt := NewCSVReader(f)
	rt.HasHeader = true
	rt.TypeHintsName = map[string]string{"Column 1": "float64"}
	dt, err := rt.Read(-1)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}
	r, err := os.Open(filepath.Join("test_files", "data", fname_sas))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}
	defer r.Close()

	sas, err := NewSAS7BDATReader(r)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}
	sas.TrimStrings = true
	sas.ConvertDates = true

	ncol := len(sas.ColumnNames())

	ds, err := sas.Read(-1)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}

	base := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	for j := 0; j < len(dt); j++ {
		if sas.ColumnFormats[j] == "MMDDYY" {
			dt[j] = dt[j].ForceNumeric()
			dt[j], err = dt[j].Date_from_duration(base, "days")
			if err != nil {
				panic(err)
			}
		}
	}

	if len(ds) != len(dt) {
		return false
	}

	for j := 0; j < ncol; j++ {
		fl, ix := ds[j].AllClose(dt[j], 1e-5)
		if !fl {
			fmt.Printf("Not equal:\nSAS:\n")
			if ix == -1 {
				fmt.Printf("  Unequal lengths\n")
			} else if ix == -2 {
				fmt.Printf("  Unequal types\n")
			} else {
				fmt.Printf("  Unequal in column %d, row %d\n", j, ix)
				ds[j].Print()
				dt[j].Print()
			}
			return false
		}
	}

	return true
}

func TestSAS_generated(t *testing.T) {

	for k := 1; k < 16; k++ {
		fname := fmt.Sprintf("test%d.sas7bdat", k)
		r := sas_base_test("test1.csv", fname)
		if !r {
			fmt.Printf("Failing %s\n", fname)
			t.Fail()
		}
	}
}
