package datareader

// Copyright 2015 Kerby Shedden

/*

Package datareader reads binary datasets from the SAS and Stata
commercial statistical packages.  The Stata dta file format is
well-documented, and this code reads dta formats 115, 116, and 117.
There is no official documentation of the SAS7BDAT format.  This code
is based on previous efforts to reverse-engineer the format.

Package datareader also includes a simple column-oriented data
container called a Series, and a function that reads CSV files, infers
the datatype of each column, and places them into an array of Series
objects.  The SAS and Stata readers return the data as an array of
Series objects.

The SAS and Stata objects behave similarly, and both satisfy the
Statfilereader interface.  Both readers can read a file by chunks
(ranges of consecutive records) to facilitate processing of extremely
large files.

*/
