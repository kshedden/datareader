package main

// Convert a binary SAS7BDAT or Stata dta file to a CSV file.  The CSV
// contents are sent to standard output.  Date variables are returned
// as numeric values with interpretation depending on the date format
// (e.g. it may be the number of days since January 1, 1960).

import (
	"encoding/csv"
	"fmt"
	"github.com/kshedden/datareader"
	"os"
	"strings"
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

		for j := 0; j < ncol; j++ {
			chunk[j].UpcastNumeric()
		}

		nrow := chunk[0].Length()

		numbercols := make([][]float64, ncol)
		stringcols := make([][]string, ncol)

		missing := make([][]bool, ncol)

		for j := 0; j < ncol; j++ {
			missing[j] = chunk[j].Missing()
			dcol := chunk[j].Data()
			switch dcol.(type) {
			default:
				panic("unknown type")
			case []float64:
				numbercols[j] = dcol.([]float64)
			case []string:
				stringcols[j] = dcol.([]string)
			}
		}

		for i := 0; i < nrow; i++ {
			for j := 0; j < ncol; j++ {
				if numbercols[j] != nil {
					if (missing[j] == nil) || (missing[j][i] == false) {
						row[j] = fmt.Sprintf("%v", numbercols[j][i])
					} else {
						row[j] = ""
					}
				} else {
					if (missing[j] == nil) || (missing[j][i] == false) {
						row[j] = fmt.Sprintf("%v", stringcols[j][i])
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
		rdr, err = datareader.NewSAS7BDATReader(f)
		if err != nil {
			panic(err)
		}
	} else if filetype == "stata" {
		rdr, err = datareader.NewStataReader(f)
		fmt.Printf("%v\n", rdr.ColumnTypes())
		if err != nil {
			panic(err)
		}
	}

	do_conversion(rdr)
}
