package main

// Convert a binary SAS7BDAT or Stata dta file to a CSV file.  The CSV
// contents are sent to standard output.  Date variables are returned
// as numeric values with interpretation depending on the date format
// (e.g. it may be the number of days since January 1, 1960).

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/kshedden/datareader"
)

func doConversion(rdr datareader.StatfileReader) {

	w := csv.NewWriter(os.Stdout)

	ncol := len(rdr.ColumnNames())
	if err := w.Write(rdr.ColumnNames()); err != nil {
		panic(err)
	}

	row := make([]string, ncol)

	for {
		chunk, err := rdr.Read(1000)
		if err != nil && err != io.EOF {
			panic(err)
		} else if chunk == nil || err == io.EOF {
			break
		}

		for j := 0; j < len(chunk); j++ {
			chunk[j] = chunk[j].UpcastNumeric()
		}

		nrow := chunk[0].Length()

		numbercols := make([][]float64, ncol)
		stringcols := make([][]string, ncol)
		timecols := make([][]time.Time, ncol)

		missing := make([][]bool, ncol)

		for j := 0; j < ncol; j++ {
			missing[j] = chunk[j].Missing()
			dcol := chunk[j].Data()
			switch dcol.(type) {
			case []time.Time:
				timecols[j] = dcol.([]time.Time)
			case []float64:
				numbercols[j] = dcol.([]float64)
			case []string:
				stringcols[j] = dcol.([]string)
			default:
				panic(fmt.Sprintf("unknown type: %T", dcol))
			}
		}

		for i := 0; i < nrow; i++ {
			for j := 0; j < ncol; j++ {
				if numbercols[j] != nil {
					if missing[j] == nil || !missing[j][i] {
						row[j] = fmt.Sprintf("%f", numbercols[j][i])
					} else {
						row[j] = ""
					}
				} else if stringcols[j] != nil {
					if missing[j] == nil || !missing[j][i] {
						row[j] = stringcols[j][i]
					} else {
						row[j] = ""
					}
				} else if timecols[j] != nil {
					if missing[j] == nil || !missing[j][i] {
						row[j] = fmt.Sprintf("%v", timecols[j][i])
					} else {
						row[j] = ""
					}
				}
			}
			if err := w.Write(row); err != nil {
				panic(err)
			}
		}
	}

	w.Flush()
}

func main() {

	if len(os.Args) == 1 {
		fmt.Printf("usage: %s filename\n", os.Args[0])
		return
	}

	fname := os.Args[1]
	f, err := os.Open(fname)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		return
	}
	defer f.Close()

	// Determine the file type
	fl := strings.ToLower(fname)
	filetype := ""
	if strings.HasSuffix(fl, "sas7bdat") {
		filetype = "sas"
	} else if strings.HasSuffix(fl, "dta") {
		filetype = "stata"
	} else {
		os.Stderr.WriteString(fmt.Sprintf("%s file cannot be read", fname))
		return
	}

	// Get a reader for either a Stata or SAS file
	var rdr datareader.StatfileReader
	if filetype == "sas" {
		sas, err := datareader.NewSAS7BDATReader(f)
		if err != nil {
			panic(err)
		}
		sas.ConvertDates = true
		sas.TrimStrings = true
		rdr = sas
	} else if filetype == "stata" {
		stata, err := datareader.NewStataReader(f)
		if err != nil {
			panic(err)
		}
		stata.ConvertDates = true
		stata.InsertCategoryLabels = true
		stata.InsertStrls = true
		rdr = stata
	}

	doConversion(rdr)
}
