package main

// Convert a binary SAS7BDAT or Stata dta file to a CSV file.  The CSV
// contents are sent to standard output.  Date variables are returned
// as numeric values with interpretation depending on the date format
// (e.g. it may be the number of days since January 1, 1960).

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/kshedden/datareader"
)

func do_conversion(rdr datareader.Statfilereader) {

	w := csv.NewWriter(os.Stdout)

	ncol := len(rdr.ColumnNames())
	w.Write(rdr.ColumnNames())

	row := make([]string, ncol)

	for {
		chunk, err := rdr.Read(1000)
		if err != nil {
			panic(err)
		}
		if chunk == nil {
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
			default:
				panic(fmt.Sprintf("unknown type: %T", dcol))
			case []time.Time:
				timecols[j] = dcol.([]time.Time)
			case []float64:
				numbercols[j] = dcol.([]float64)
			case []string:
				stringcols[j] = dcol.([]string)
			}
		}

		for i := 0; i < nrow; i++ {
			for j := 0; j < ncol; j++ {
				if numbercols[j] != nil {
					if missing[j] == nil || missing[j][i] == false {
						row[j] = fmt.Sprintf("%f", numbercols[j][i])
					} else {
						row[j] = ""
					}
				} else if stringcols[j] != nil {
					if missing[j] == nil || missing[j][i] == false {
						row[j] = fmt.Sprintf("%s", stringcols[j][i])
					} else {
						row[j] = ""
					}
				} else if timecols[j] != nil {
					if missing[j] == nil || missing[j][i] == false {
						row[j] = fmt.Sprintf("%v", timecols[j][i])
					} else {
						row[j] = ""
					}
				}
			}
			w.Write(row)
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

	var rdr datareader.Statfilereader
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

	do_conversion(rdr)
}
