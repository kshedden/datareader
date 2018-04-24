package main

// columnize takes a binary SAS (SAS7BDAT) or Stata (dta) file and
// saves the data from each column into a separate file.  Character
// data is stored in raw format, with values separated by newline
// characters.  Numeric data can be stored either in text or binary
// format.  A text file containing the column names is also generated.

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/kshedden/datareader"
)

func do_split(rdr datareader.Statfilereader, col_dir string, mode string) {

	ncol := len(rdr.ColumnNames())
	columns := make([]io.Writer, ncol)

	cf, err := os.Create(filepath.Join(col_dir, "columns.txt"))
	defer cf.Close()
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("unable to create file in %s: %v\n", col_dir, err))
		return
	}
	for i, c := range rdr.ColumnNames() {
		cf.WriteString(fmt.Sprintf("%d,%s\n", i+1, c))
	}

	for j, _ := range rdr.ColumnNames() {
		fn := filepath.Join(col_dir, fmt.Sprintf("%d", j))
		f, err := os.Create(fn)
		defer f.Close()
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("unable to create file for column %d: %v\n", j+1, err))
		}
		columns[j] = f
	}

	for {
		chunk, _ := rdr.Read(10000)
		if chunk == nil {
			return
		}

		missing := make([][]bool, ncol)
		for j := 0; j < ncol; j++ {
			missing[j] = chunk[j].Missing()
		}

		for j := 0; j < len(chunk); j++ {
			chunk[j].UpcastNumeric()
		}

		for j := 0; j < ncol; j++ {
			ds := chunk[j].Data()
			switch ds.(type) {
			case []float64:
				if mode == "binary" {
					buf := new(bytes.Buffer)
					for i, x := range ds.([]float64) {
						if (missing[j] == nil) || (missing[j][i] == false) {
							binary.Write(buf, binary.LittleEndian, x)
						} else {
							binary.Write(buf, binary.LittleEndian, math.NaN())
						}
					}
					columns[j].Write(buf.Bytes())
				} else {
					vec := ds.([]float64)
					for i, x := range vec {
						if (missing[j] == nil) || (missing[j][i] == false) {
							columns[j].Write([]byte(fmt.Sprintf("%v\n", x)))
						} else {
							columns[j].Write([]byte("\n"))
						}
					}
				}
			case []string:
				for _, x := range ds.([]string) {
					columns[j].Write([]byte(x))
					columns[j].Write([]byte("\n"))
				}
			}
		}
	}
}

func main() {

	if len(os.Args) != 4 {
		os.Stderr.WriteString(fmt.Sprintf("usage: %s -in=file -out=directory -mode=mode\n", os.Args[0]))
		return
	}

	infile := flag.String("in", "", "A SAS7BDAT or Stata dta file name")
	col_dir := flag.String("out", "", "A directory for writing the columns")
	mode := flag.String("mode", "text", "Write numeric data as 'text' or 'binary'")

	flag.Parse()

	if (*mode != "text") && (*mode != "binary") {
		os.Stderr.WriteString("mode must be either 'text' or 'binary'\n")
		return
	}

	fl := strings.ToLower(*infile)
	filetype := ""
	if strings.HasSuffix(fl, "sas7bdat") {
		filetype = "sas"
	} else if strings.HasSuffix(fl, "dta") {
		filetype = "stata"
	} else {
		os.Stderr.WriteString(fmt.Sprintf("%s file cannot be read", *infile))
		return
	}

	r, err := os.Open(*infile)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("unable to open %s\n", *infile))
	}
	defer r.Close()

	var rdr datareader.Statfilereader
	if filetype == "sas" {
		rdr, err = datareader.NewSAS7BDATReader(r)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("unable to open SAS file: %v\n", err))
			return
		}
	} else if filetype == "stata" {
		rdr, err = datareader.NewStataReader(r)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("unable to open Stata file: %v\n", err))
			return
		}
	}

	do_split(rdr, *col_dir, *mode)
}
