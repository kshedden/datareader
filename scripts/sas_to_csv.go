package main

// Convert a SAS7BDAT format file to a CSV file.  The CSV contents are
// sent to standard output.  Date variables are returned as numeric
// values with interpretation depending on the date format (e.g. it
// may be the number of days since January 1, 1960).

import (
	"datareader"
	"encoding/csv"
	"fmt"
	"os"
)

func main() {

	if len(os.Args) == 1 {
		fmt.Printf("usage: %s file.sas7bdat\n", os.Args[0])
		return
	}

	fname := os.Args[1]
	f, _ := os.Open(fname)

	sas, err := datareader.NewSAS7BDATReader(f)
	if err != nil {
		panic(err)
	}

	w := csv.NewWriter(os.Stdout)

	ncol := len(sas.ColumnNames)
	w.Write(sas.ColumnNames)

	row := make([]string, ncol)

	for {
		da, err := sas.Read(1000)
		if err != nil {
			panic(err)
		}
		if da == nil {
			break
		}
		nrow := da[0].Length()

		numbercols := make([][]float64, ncol)
		stringcols := make([][]string, ncol)

		for j := 0; j < ncol; j++ {
			dcol := da[j].Data()
			switch sas.ColumnTypes[j] {
			default:
				panic("unknown type")
			case 0:
				numbercols[j] = dcol.([]float64)
			case 1:
				stringcols[j] = dcol.([]string)
			}
		}

		for i := 0; i < nrow; i++ {
			for j := 0; j < ncol; j++ {
				switch sas.ColumnTypes[j] {
				case 0:
					row[j] = fmt.Sprintf("%v", numbercols[j][i])
				case 1:
					row[j] = fmt.Sprintf("%v", stringcols[j][i])
				}
			}
			w.Write(row)
		}
	}
	w.Flush()
}
