package datareader

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	rt.HasHeader = false
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
	for j := 0; j < len(ds); j++ {
		if strings.Contains(formats[j], "%td") {
			ti := to_date_yyyymmdd(dt[j].Data().([]float64))
			dt[j], err = NewSeries(dt[j].Name, ti, dt[j].Missing())
			if err != nil {
				os.Stderr.Write([]byte(fmt.Sprintf("%v\n", err)))
				return false
			}
		}
	}

	for j := 0; j < len(ds); j++ {
		if !ds[j].AllClose(dt[j], 1e-6) {
			return false
		}
	}

	return true
}

func TestStata1(t *testing.T) {

	r := stata_base_test("test1.csv", "test1_118.dta")
	if !r {
		t.Fail()
	}

}

func TestStata2(t *testing.T) {

	r := stata_base_test("test1.csv", "test1_117.dta")
	if !r {
		t.Fail()
	}

}

func TestStata3(t *testing.T) {

	r := stata_base_test("test1.csv", "test1_115.dta")
	if !r {
		t.Fail()
	}

}

func TestStata4(t *testing.T) {

	r := stata_base_test("test1.csv", "test1_115b.dta")
	if !r {
		t.Fail()
	}

}
