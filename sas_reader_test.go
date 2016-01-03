package datareader

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
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

func sas_base_test(fname_csv, fname_sas string) bool {

	f, err := os.Open(filepath.Join("test_files", "data", fname_csv))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}
	defer f.Close()

	rt := NewCSVReader(f)
	rt.HasHeader = false
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

	ncol := len(sas.ColumnNames())

	ds, err := sas.Read(10000)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}

	if len(ds) != len(dt) {
		return false
	}

	for j := 0; j < ncol; j++ {
		switch sas.ColumnFormats[j] {
		default:
			os.Stderr.WriteString(fmt.Sprintf("unknown format for column %d: %s\n", j, sas.ColumnFormats[j]))
		case "":
			if !ds[j].AllClose(dt[j], 1e-5) {
				switch ds[j].Data().(type) {
				default:
					panic("unknown types")
				case []string:
					a := ds[j].Data().([]string)
					b := dt[j].Data().([]string)
					for i := 0; i < ds[j].Length(); i++ {
						if a[i] != b[i] {
							fmt.Printf("%v :%v: :%v: %v %v\n", i, a[i], b[i], len(a[i]), len(b[i]))
						}
					}
				case []float64:
					a := ds[j].Data().([]float64)
					b := dt[j].Data().([]float64)
					for i := 0; i < ds[j].Length(); i++ {
						if ds[j].Missing()[i] && dt[j].Missing()[i] {
							continue
						}
						aa := fmt.Sprintf("%v", a[i])
						if ds[j].Missing()[i] {
							aa = ""
						}
						bb := fmt.Sprintf("%v", b[i])
						if ds[j].Missing()[i] {
							bb = ""
						}
						if aa != bb {
							fmt.Printf("%d %v %v\n", i, aa, bb)
						}
					}
				}
				return false
			}
		case "MMDDYY":
			base := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
			sas_series, err := ds[j].date_from_duration(base, "days")
			if err != nil {
				return false
			}
			vec_sas := sas_series.Data().([]time.Time)
			vec_txt := to_date_yyyymmdd(dt[j].Data().([]float64))
			if len(vec_sas) != len(vec_txt) {
				return false
			}
			n := len(vec_sas)
			for i := 0; i < n; i++ {
				if ds[j].Missing()[i] && dt[j].Missing()[i] {
					continue
				}
				if vec_sas[i] != vec_txt[i] {
					return false
				}
			}
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

func TestSAS_prds(t *testing.T) {

	fmt.Printf("START\n")
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

	for j := 0; j < len(chunk); j++ {

		_, ok1 := chunk[j].Data().([]string)
		_, ok2 := chunk_csv[j].Data().([]float64)

		if ok1 && ok2 {
			chunk[j] = chunk[j].ForceNumeric()
		}

		if !chunk[j].AllClose(chunk_csv[j], 1e-5) {
			chunk[j].PrintRange(0, 5)
			chunk_csv[j].PrintRange(0, 5)
			fmt.Printf("%v\n", sas.ColumnFormats[j])
			fmt.Printf("\n")

			x1, ok1 := chunk_csv[j].Data().([]string)
			x2, ok2 := chunk[j].Data().([]string)
			if ok1 && ok2 {
				for i := 0; i < len(x1); i++ {
					if x1[i] != x2[i] {
						fmt.Printf("%v :%s: :%s:\n", i, x1[i], x2[i])
					}
				}
			}

			z1, ok1 := chunk_csv[j].Data().([]float64)
			z2, ok2 := chunk[j].Data().([]float64)
			miss1 := chunk_csv[j].Missing()
			miss2 := chunk_csv[j].Missing()
			if ok1 && ok2 {
				fmt.Printf("::->:: %v %v\n", len(z1), len(z2))
				for i := 0; i < len(z1); i++ {
					if miss1[i] && miss2[i] {
						continue
					}
					if z1[i] != z2[i] {
						fmt.Printf("%v %v %v %v %v\n", i, z1[i], z2[i], miss1[i], miss2[i])
					}
				}
			}

		}
	}
}
