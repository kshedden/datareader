package datareader

// Copyright 2015 Kerby Shedden

/*

Package datareader provides utilities for working with statistical
data sets.  Currently the main things that are useful here are the
StataReader class for reading Stata files (version 118 only) and the
CSVReader for reading csv files with type-conversion and optional
column labels.  The package also provides a simple Series class for
working with homogeneously typed data.

All functions in this package produce as a dataset a vector of Series
objects.  Each Series object contains a sequence of values of the same
type.
*/
