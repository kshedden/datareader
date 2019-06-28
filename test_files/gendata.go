package main

// Generate data files for testing.

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
)

func gen1(words []string, fname string) {

	r := rand.New(rand.NewSource(99))

	fid, err := os.Create(filepath.Join("data", fname))
	if err != nil {
		panic("Unable to open file.")
	}

	w := csv.NewWriter(fid)

	ncol := 100
	nrow := 10

	rowdata := make([]string, ncol)

	for k := 0; k < ncol; k++ {
		rowdata[k] = fmt.Sprintf("Column%d", k+1)
	}
	if err := w.Write(rowdata); err != nil {
		panic(err)
	}

	for i := 0; i < nrow; i++ {

		for j := 0; j < ncol; j++ {
			switch j % 4 {
			case 0:
				if r.Float64() < 0.1 {
					rowdata[j] = ""
				} else {
					rowdata[j] = fmt.Sprintf("%.3f", r.Float64())
				}
			case 1:
				if r.Float64() < 0.1 {
					rowdata[j] = ""
				} else {
					rowdata[j] = words[r.Int63n(4)]
				}
			case 2:
				if r.Float64() < 0.1 {
					rowdata[j] = ""
				} else {
					rowdata[j] = fmt.Sprintf("%d", r.Int63n(100))
				}
			case 3:
				// dates
				if r.Float64() < 0.1 {
					rowdata[j] = ""
				} else {
					rowdata[j] = fmt.Sprintf("%d", r.Int63n(10000))
				}
			}
		}

		if err := w.Write(rowdata); err != nil {
			panic(err)
		}
	}

	w.Flush()
}

func main() {

	words := []string{"apple", "dog", "pear", "crocodile", "banana"}
	gen1(words, "test1.csv")

	words = []string{"부산", "Иркутск", "高雄市", "鱷魚", "ひらがな"}
	gen1(words, "test2.csv")
}
