package datareader

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	//"time"
)

func stata_base_test(fname_csv, fname_stata string) bool {

	f, err := os.Open(filepath.Join("test_files", "data", fname_csv))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}
	defer f.Close()

	rt := NewCSVReader(f)
	rt.HasHeader = true
	dt, err := rt.Read(-1)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}

	r, err := os.Open(filepath.Join("test_files", "data", fname_stata))
	stata, err := NewStataReader(r)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}
	defer r.Close()
	stata.InsertCategoryLabels = false

	ds, err := stata.Read(-1)
	if err != nil {
		return false
	}

	for j := 0; j < len(ds); j++ {
		ds[j].UpcastNumeric()
	}

	formats := stata.Formats
	base := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	for j := 0; j < len(ds); j++ {
		ds[j] = ds[j].UpcastNumeric()
		if strings.Contains(formats[j], "%td") {
			dt[j] = dt[j].ForceNumeric()
			dt[j], err = dt[j].Date_from_duration(base, "days")
			if err != nil {
				os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
				return false
			}
		}
	}

	fl, jx, ix := SeriesArray(ds).AllClose(dt, 1e-6)
	if !fl {
		if ix == -1 {
			fmt.Printf("Unequal lengths\n")
		} else if ix == -2 {
			fmt.Printf("Unequal types\n")
		} else {
			fmt.Printf("Unequal values at column %d row %d\n", jx, ix)
			ds[jx].Print()
			dt[jx].Print()
		}
		return false
	}

	return true
}

func TestStata1(t *testing.T) {

	fnames := []string{"test1_115.dta", "test1_115b.dta", "test1_117.dta", "test1_118.dta"}

	for _, fname := range fnames {

		r := stata_base_test("test1.csv", fname)
		if !r {
			t.Fail()
		}
	}
}

func TestStata2(t *testing.T) {

	fnames := []string{"test2_115.dta", "test2_115b.dta", "test2_117.dta", "test2_118.dta"}

	for _, fname := range fnames {

		r := stata_base_test("test2.csv", fname)
		if !r {
			t.Fail()
		}
	}
}
