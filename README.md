__datareader__ is a pure Go (Golang) package that can read binary SAS
format (SAS7BDAT) and Stata format (dta) data files.

The Stata reader is based on the Stata documentation for the [dta file
format](http://www.stata.com/help.cgi?dta) and supports recent dta
versions.

There is no official documentation for SAS binary format files.  The
code here is translated from the Python
[sas7bdat](https://pypi.python.org/pypi/sas7bdat) package, which in
turn is based on an [R
package](https://github.com/BioStatMatt/sas7bdat).  Also see
[here](https://cran.r-project.org/web/packages/sas7bdat/vignettes/sas7bdat.pdf)
for more information about the SAS7BDAT file structure.  The Go
version has not been extensively tested, but has been checked on
several files (both compressed and uncompressed) and found to give
correct results.

This package also contains a simple columnar data container called a
`Series`.  Both the SAS reader and Stata reader return the data as an
array of `Series` objects.  These can in turn be converted to other
formats as needed.

Both the Stata and SAS reader support streaming access to the data
(i.e. reading the file by chunks of consecutive records).


Here is an example of how the SAS reader can be used:

```
import (
        "datareader"
        "os"
)


f := os.Open("filename.sas7bdat")

sas, err := NewSAS7BDATReader(f)
if err != nil {
        panic(err)
}


ds, err := sas.Read(10000)
if err != nil {
        panic(err)
}
```

__Notes__

In the SAS reader, dates are not converted to Go date or time formats.
Instead, a `float64` is returned, whose meaning depends on the
underlying SAS date/time format.  For example, it may represent the
number of days since January 1, 1960.

