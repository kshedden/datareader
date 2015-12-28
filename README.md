datareader : read SAS and Stata files in Go
=========================

__datareader__ is a pure [Go](https://golang.org) (Golang) package
that can read binary SAS format (SAS7BDAT) and Stata format (dta) data
files into native Go data structures.

The Stata reader is based on the Stata documentation for the [dta file
format](http://www.stata.com/help.cgi?dta) and supports dta versions
115, 117, and 118.

There is no official documentation for SAS binary format files.  The
code here is translated from the Python
[sas7bdat](https://pypi.python.org/pypi/sas7bdat) package, which in
turn is based on an [R
package](https://github.com/BioStatMatt/sas7bdat).  Also see
[here](https://cran.r-project.org/web/packages/sas7bdat/vignettes/sas7bdat.pdf)
for more information about the SAS7BDAT file structure.

This package also contains a simple column-oriented data container
called a `Series`.  Both the SAS reader and Stata reader return the
data as an array of `Series` objects, corresponding to the columns of
the data file.  These can in turn be converted to other formats as
needed.

Both the Stata and SAS reader support streaming access to the data
(i.e. reading the file by chunks of consecutive records).

## SAS

Here is an example of how the SAS reader can be used in a Go program:

```
import (
        "datareader"
        "os"
)

// Create a SAS7BDAT object
f := os.Open("filename.sas7bdat")
sas, err := datareader.NewSAS7BDATReader(f)
if err != nil {
        panic(err)
}

// Read the first 10000 records
ds, err := sas.Read(10000)
if err != nil {
        panic(err)
}
```

## Stata

Here is an example of how the Stata reader can be used in a Go program:

```
import (
        "datareader"
        "os"
)

// Create a StataReader object
f := os.Open("filename.dta")
stata, err := datareader.NewStataReader(f)
if err != nil {
        panic(err)
}

// Read the first 10000 records
ds, err := stata.Read(10000)
if err != nil {
        panic(err)
}
```

## CSV

The package includes a CSV reader with type inference for the column data types.

```
import (
        "datareader"
)

f := os.Open("filename.csv")
rt := datareader.NewCSVReader(f)
rt.HasHeader = true
dt, err := rt.Read(-1)
if err != nil {
        panic(err)
}
```

## Commands

Two command-line utilities use the datareader package to allow
conversion of SAS and Stata datasets to other formats without using
Go.  Run the Makefile to compile these commands and place them into
your GOBIN directory.

The `stattocsv` command converts a SAS7BDAT or Stata dta file to a csv
file.

The `columnize` command takes the data from either a SAS7BDAT or a
Stata dta file, and writes the data from each column into a separate
file.  Numeric data can be stored in either binary (native 8 byte
floats) or text format (binary is considerably faster).

## Notes

The SAS reader does not convert dates to Go date or time formats.
Instead, a `float64` is returned, whose meaning depends on the
underlying SAS date/time format (which is available as the
`ColumnFormats` field of the `SAS7BDAT` struct).  For example, the
value may represent the number of days since January 1, 1960.

This package has not been extensively tested, but has been checked on
several files (including both compressed and uncompressed SAS files
and Stata dta versions 115, 117, and 118) and found to give correct
results.