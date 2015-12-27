package datareader

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func stata_base_test(fname_csv, fname_stata string) bool {

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

	r, err := os.Open(filepath.Join("test_files", fname_stata))
	stata, err := NewStataReader(r)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}
	stata.InsertCategoryLabels = false

	ds, err := stata.Read(-1)
	if err != nil {
		return false
	}

	for j := 0; j < len(ds); j++ {
		ds[j].UpcastNumeric()
	}

	if !SeriesArray(ds).AllClose(dt, 1e-6) {
		return false
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
