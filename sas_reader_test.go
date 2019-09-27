package datareader

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func sasBaseTest(fnameCSV, fnameSAS string, factorizeStrings bool) bool {

	f, err := os.Open(filepath.Join("test_files", "data", fnameCSV))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}
	defer f.Close()

	// Read the whole CSV file
	rt := NewCSVReader(f)
	rt.HasHeader = true
	rt.TypeHintsName = map[string]string{"Column 1": "float64"}
	dt, err := rt.Read(-1)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}

	// Open the SAS file
	r, err := os.Open(filepath.Join("test_files", "data", fnameSAS))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}
	defer r.Close()

	// Set up the SAS reader
	sas, err := NewSAS7BDATReader(r)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}
	sas.TrimStrings = true
	sas.ConvertDates = true
	sas.FactorizeStrings = factorizeStrings

	// Check the column names
	for k, na := range sas.ColumnNames() {
		ename := fmt.Sprintf("Column%d", k+1)
		if na != ename {
			fmt.Printf("Column name %d is %s, should be %s\n", k, na, ename)
			return false
		}
	}

	// Check the column labels
	if sas.ColumnLabels()[1] != "Column 2 label" {
		fmt.Printf("Column label 1 is incorrect\n")
		return false
	}
	if sas.ColumnLabels()[99] != "Column 100 label" {
		fmt.Printf("Column label 100 is incorrect\n")
		return false
	}

	// Read the whole SAS file
	ds, err := sas.Read(-1)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return false
	}

	// Convert the dates from the CSV file so that they are comparable to the SAS dates.
	base := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	for j := 0; j < len(dt); j++ {
		if sas.ColumnFormats[j] == "MMDDYY" {
			dt[j] = dt[j].ForceNumeric()
			dt[j], err = dt[j].DateFromDuration(base, "days")
			if err != nil {
				panic(err)
			}
		}
	}

	// Compare the data values
	if factorizeStrings {
		return compare(ds, dt, sas, sas.StringFactorMap())
	} else {
		return compare(ds, dt, sas, nil)
	}
}

func compare(ds, dt []*Series, sas *SAS7BDAT, stringFactorMap map[uint64]string) bool {

	if len(ds) != len(dt) {
		return false
	}

	for j := range ds {

		if sas.columnTypes[j] == SASStringType && stringFactorMap != nil {
			// Compare factored strings
			x := ds[j].Data().([]uint64)
			y := dt[j].Data().([]string)
			if len(x) != len(y) {
				return false
			}
			for i := range x {
				if stringFactorMap[x[i]] != y[i] {
					return false
				}
			}
		} else {
			// Compare numbers or unfactored strings
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
	}

	return true
}

// TestSASGenerated1 tests against a file with ASCII/latin-1 characters.
// See test_files/data for a CSV version of the data file.
func TestSASGenerated1(t *testing.T) {

	for k := 1; k < 16; k++ {
		fname := fmt.Sprintf("test%d.sas7bdat", k)
		for _, factorizeStrings := range []bool{false, true} {
			r := sasBaseTest("test1.csv", fname, factorizeStrings)
			if !r {
				fmt.Printf("Failing %s\n", fname)
				t.Fail()
			}
		}
	}
}

// TestSASGenerated2 tests against a file with many non-latin1 characters.
// See test_files/data for a CSV version of the data file.
func TestSASGenerated2(t *testing.T) {

	for k := 16; k < 22; k++ {
		fname := fmt.Sprintf("test%d.sas7bdat", k)
		for _, factorizeStrings := range []bool{false, true} {
			r := sasBaseTest("test2.csv", fname, factorizeStrings)
			if !r {
				fmt.Printf("Failing %s\n", fname)
				t.Fail()
			}
		}
	}
}
