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
				fmt.Printf("%T %T\n", ds[j].Data(), dt[j].Data())
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
