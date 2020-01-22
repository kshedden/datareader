// +build ignore

package main

import (
	"fmt"
	"log"

	"github.com/kshedden/datareader/cmd/sas_to_parquet/test1"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func main() {

	fr, err := local.NewLocalFileReader("tmp/test1.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, new(test1.Data), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num := int(pr.GetNumRows())
	fmt.Printf("num=%v\n", num)

	x := make([]test1.Data, 3)
	for i := 0; i < 4; i++ {
		if err = pr.Read(&x); err != nil {
			log.Println("Read error", err)
		}
		log.Printf("%+v\n", len(x))
	}

	pr.ReadStop()
	fr.Close()
}
