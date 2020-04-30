// sas_to_parquet generates a script that can be used to convert a
// SAS7bdat file to a parquet file.  It also produces Go struct
// definitions that can be used to read the resulting parquet file
// from Go.  This script emits a Go script called `convert_***.go`,
// where `***` is the name of the struct that holds one record of
// data.  This struct name is defined through command line flags.
// This script does not actually do the data conversion, you must run
// the generated Go file to do this.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"io"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/kshedden/datareader"
)

const (
	// The Go code template for the convert***.go file to be emitted.
	tcode = `// +build ignore

package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/kshedden/datareader"
	"{{ .Importpath}}/{{ .Pkgname }}"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func main() {

	sasfile := "{{ .SASfile }}"

	rdr, err := os.Open(sasfile)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

	sas, err := datareader.NewSAS7BDATReader(rdr)
	if err != nil {
		panic(err)
	}

	fw, err := local.NewLocalFileWriter("{{ .Outfile }}")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new({{ .Pkgname }}.{{ .Structname }}), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	chunksize := 1024 * 1024
	ntot := 0
	for chunk := 0; ; chunk++ {

		dat, err := sas.Read(chunksize)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		nrow := dat[0].Length()
		ntot += nrow
		fmt.Printf("Read %d records, %d total\n", nrow, ntot)

		vx := 0
		{{ range .VarBlocks }}
	  	    {{ .Name }}, _, err := dat[vx].As{{ .Type }}Slice()
		    if err != nil {
			    panic(err)
		    }
		    vx++
		{{ end }}

		for i := 0; i < nrow; i++ {

			rec := {{.Pkgname }}.{{ .Structname }} {
			    {{- range .VarBlocks }}
				    {{ .Name }}: {{ .Name }}[i],
				{{- end }}
			}

			if err = pw.Write(rec); err != nil {
				log.Println("{{ .SASfile }} write error", err)
			}
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Println("{{ .SASfile }} WriteStop error", err)
		return
	}

	log.Println("{{ .SASfile }} finished")
	fw.Close()
}
`
)

func writeSchema(cnames []string, ctypes []datareader.ColumnTypeT, pkgname, structname string) {

	var buf bytes.Buffer

	s := fmt.Sprintf("package %s\n\n", pkgname)
	if _, err := io.WriteString(&buf, s); err != nil {
		panic(err)
	}

	s = fmt.Sprintf("type %s struct {\n", structname)
	if _, err := io.WriteString(&buf, s); err != nil {
		panic(err)
	}

	for i := range cnames {

		// The go version of the variable name must be exported
		gname := strings.Title(cnames[i])

		switch ctypes[i] {
		case datareader.SASNumericType:
			s = fmt.Sprintf("    %s float64 `parquet:\"name=%s,type=DOUBLE\"`\n", gname, cnames[i])
			if _, err := io.WriteString(&buf, s); err != nil {
				panic(err)
			}
		case datareader.SASStringType:
			s = fmt.Sprintf("    %s string `parquet:\"name=%s,type=BYTE_ARRAY\"`\n", gname, cnames[i])
			if _, err := io.WriteString(&buf, s); err != nil {
				panic(err)
			}
		default:
			panic("Unkown data type\n")
		}
	}

	if _, err := io.WriteString(&buf, "}\n"); err != nil {
		panic(err)
	}

	// Format the source
	var p []byte
	var err error
	p, err = format.Source(buf.Bytes())
	if err != nil {
		panic(err)
	}

	if err := os.MkdirAll(pkgname, 0777); err != nil {
		panic(err)
	}

	fname := strings.ToLower(structname)
	fname = fmt.Sprintf("%s_def.go", fname)
	out, err := os.Create(path.Join(pkgname, fname))
	if err != nil {
		panic(err)
	}
	if _, err := out.Write(p); err != nil {
		panic(err)
	}
	defer out.Close()
}

func printUsage() {
	panic("!!!\n")
}

func getImportPath() string {

	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	tk := strings.Split(dir, "/")

	k := 0
	for k = range tk {
		if tk[k] == "src" {
			tk = tk[k+1:]
			break
		}
	}

	return path.Join(tk...)
}

func writeCode(cnames []string, ctypes []datareader.ColumnTypeT, pkgname, structname, sasfile, outdir string) {

	type vart struct {
		Name string
		Type string
	}

	var vt []*vart
	for i := range cnames {
		x := &vart{
			Name: cnames[i],
		}
		switch ctypes[i] {
		case datareader.SASNumericType:
			x.Type = "Float64"
		case datareader.SASStringType:
			x.Type = "String"
		default:
			panic("Unkown data type\n")
		}

		vt = append(vt, x)
	}

	tmpl := template.Must(template.New("code").Parse(tcode))

	outname := path.Base(sasfile)
	e := path.Ext(outname)
	if e == "" {
		outname += ".parquet"
	} else {
		outname = strings.Replace(outname, e, ".parquet", -1)
	}
	outfile := path.Join(outdir, outname)

	tvals := &struct {
		Importpath string
		Pkgname    string
		Structname string
		SASfile    string
		Outfile    string
		VarBlocks  []*vart
	}{
		Importpath: getImportPath(),
		Pkgname:    pkgname,
		Structname: structname,
		SASfile:    sasfile,
		Outfile:    outfile,
		VarBlocks:  vt,
	}

	var buf bytes.Buffer
	err := tmpl.Execute(&buf, tvals)
	if err != nil {
		panic(err)
	}

	// Format the source
	var p []byte
	p, err = format.Source(buf.Bytes())
	if err != nil {
		panic(err)
	}

	convert := fmt.Sprintf("convert_%s.go", strings.ToLower(structname))
	fmt.Printf("Generating %s\n", convert)
	out, err := os.Create(convert)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Type 'go run %s' to generate the parquet file.\n", convert)

	out.WriteString("// GENERATED CODE, DO NOT EDIT\n\n")
	_, err = out.Write(p)
	if err != nil {
		panic(err)
	}

	out.Close()
}

func main() {

	sasfile := flag.String("sasfile", "", "Path to the SAS file")
	structname := flag.String("structname", "", "Name of the struct to create")
	pkgname := flag.String("pkgname", "", "Name of the package to create")
	outdir := flag.String("outdir", "", "Path where the output parquet file is written")
	flag.Parse()

	if *sasfile == "" {
		io.WriteString(os.Stderr, "'sasfile' is a required argument\n")
		os.Exit(1)
	}

	if *structname == "" {
		io.WriteString(os.Stderr, "'structname' is a required argument\n")
		os.Exit(1)
	}

	if *pkgname == "" {
		io.WriteString(os.Stderr, "'pkgname' is a required argument\n")
		os.Exit(1)
	}

	if *outdir == "" {
		io.WriteString(os.Stderr, "'outdir' is a required argument\n")
		os.Exit(1)
	}

	// Make sure the destination directory exists and is writeable.
	if _, err := os.Stat(*outdir); os.IsNotExist(err) {
		msg := fmt.Sprintf("Directory '%s' does not exist, exiting.\n", *outdir)
		io.WriteString(os.Stderr, msg)
		os.Exit(1)
	}

	rdr, err := os.Open(*sasfile)
	if err != nil {
		msg := fmt.Sprintf("Cannot open file '%s'.\n", *sasfile)
		io.WriteString(os.Stderr, msg)
		panic(err)
	}
	defer rdr.Close()

	sas, err := datareader.NewSAS7BDATReader(rdr)
	if err != nil {
		panic(err)
	}

	cnames := sas.ColumnNames()
	ctypes := sas.ColumnTypes()

	writeSchema(cnames, ctypes, *pkgname, *structname)
	writeCode(cnames, ctypes, *pkgname, *structname, *sasfile, *outdir)
}
