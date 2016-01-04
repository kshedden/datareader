package datareader

import (
	//	"bytes"
	//	"compress/gzip"
	"fmt"
	//	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	//	"strings"
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

	ds, err := sas.Read(10000)
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
				fmt.Printf("Unequal lengths\n")
			} else if ix == -2 {
				fmt.Printf("Unequal types\n")
			} else {
				fmt.Printf("Unequal at position %d\n", ix)
			}
			return false
		}
	}

	return true
}

func TestSAS_no_compression(t *testing.T) {

	r := sas_base_test("test1.csv", "test1_compression_no.sas7bdat")
	if !r {
		t.Fail()
	}
}

func TestSAS_char_compression(t *testing.T) {

	r := sas_base_test("test1.csv", "test1_compression_char.sas7bdat")
	if !r {
		t.Fail()
	}
}

func TestSAS_binary_compression(t *testing.T) {

	r := sas_base_test("test1.csv", "test1_compression_binary.sas7bdat")
	if !r {
		t.Fail()
	}
}

/*
// Too messy to use as a test
func TestSAS_prds(t *testing.T) {

	f, err := os.Open("/var/tmp/prds_hosp10_yr2012.csv.gz")
	if err != nil {
		panic(err)
	}
	g, err := gzip.NewReader(f)
	if err != nil {
		panic(err)
	}
	rdr := NewCSVReader(g)
	chunk_csv, err := rdr.Read(1000)
	if err != nil {
		panic(err)
	}

	f, err = os.Open("/var/tmp/prds_hosp10_yr2012.sas7bdat.gz")
	if err != nil {
		panic(err)
	}
	g, err = gzip.NewReader(f)
	if err != nil {
		panic(err)
	}
	b, err := ioutil.ReadAll(g)
	if err != nil {
		panic(err)
	}
	br := bytes.NewReader(b)
	sas, err := NewSAS7BDATReader(br)
	if err != nil {
		panic(err)
	}
	sas.TrimStrings = true
	sas.ConvertDates = true

	chunk, err := sas.Read(1000)
	if err != nil {
		panic(err)
	}

	if len(chunk_csv) != len(chunk) {
		t.Fail()
	}

	for j := 0; j < len(chunk_csv); j++ {
		if sas.ColumnFormats[j] == "MMDDYY" {
			n := chunk_csv[j].Length()
			x := make([]time.Time, n)
			for i, v := range chunk_csv[j].Data().([]string) {
				x[i], _ = time.Parse("01/02/2006", v)
			}
			chunk_csv[j], _ = NewSeries(chunk_csv[j].Name, x, chunk_csv[j].Missing())
		}
	}

	for j := 0; j < len(chunk_csv); j++ {

		fl, _ := chunk[j].AllClose(chunk_csv[j], 1e-5)

		// Try again with different types
		if !fl {
			// First try numeric
			xn := chunk[j].ForceNumeric()
			yn := chunk_csv[j].ForceNumeric()
			m1 := xn.CountMissing()
			m2 := yn.CountMissing()
			fl2, _ := xn.AllClose(yn, 1e-5)
			if (float64(m1) < 0.8*float64(xn.Length())) && (float64(m2) < 0.8*float64(yn.Length())) {
				if fl2 {
					continue
				}
			}

			// Now try string
			xs := chunk[j].ToString().StringFunc(strings.TrimSpace).NullStringMissing()
			ys := chunk_csv[j].ToString().StringFunc(strings.TrimSpace).NullStringMissing()
			fl3, j3 := xs.AllClose(ys, 1e-5)

			if fl3 {
				continue
			}

			if (m1 > 950) && (m2 > 950) {
				continue
			}

			fmt.Printf("#missing: %v %v\n", m1, m2)
			xs.PrintRange(j3, j3+1)
			ys.PrintRange(j3, j3+1)
		}
	}
}
*/
