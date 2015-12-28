package datareader

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

// A Series is a homogeneously-typed one-dimensional sequence of data
// values, with an optional mask for missing values.
type Series struct {

	// A name describing what is in this series.
	Name string

	// The length of the series.
	length int

	// The data, must be a homogeneous array, e.g. []float64.
	data interface{}

	// Indicators that data values are missing.  If nil, there are
	// no missing values.
	missing []bool
}

// NewSeries returns a new Series object with the given name and data
// contents.  The data parameter must be an array of floats, ints, or
// strings.
func NewSeries(name string, data interface{}, missing []bool) (*Series, error) {

	var length int

	switch data.(type) {
	default:
		return nil, errors.New("Unknown data type in NewSeries")
	case []float64:
		length = len(data.([]float64))
	case []string:
		length = len(data.([]string))
	case []int64:
		length = len(data.([]int64))
	case []int32:
		length = len(data.([]int32))
	case []float32:
		length = len(data.([]float32))
	case []int16:
		length = len(data.([]int16))
	case []int8:
		length = len(data.([]int8))
	case []uint64:
		length = len(data.([]uint64))
	case []time.Time:
		length = len(data.([]time.Time))
	}

	ser := Series{
		Name:    name,
		length:  length,
		data:    data,
		missing: missing}

	return &ser, nil
}

// Write writes the entire Series to the given writer.
func (ser *Series) Write(w io.Writer) {
	ser.WriteRange(w, 0, ser.length)
}

// WriteRange writes the given subinterval of the Series to the given writer.
func (ser *Series) WriteRange(w io.Writer, first, last int) {

	io.WriteString(w, fmt.Sprintf("Name: %s\n", ser.Name))
	ty := fmt.Sprintf("%T", ser.data)
	io.WriteString(w, fmt.Sprintf("Type: %s\n", ty[2:len(ty)]))

	switch ser.data.(type) {
	default:
		panic("Unknown type in WriteRange")
	case []float64:
		data := ser.data.([]float64)
		for j := first; j < last; j++ {
			if (ser.missing == nil) || !ser.missing[j] {
				io.WriteString(w, fmt.Sprintf("  %v\n", data[j]))
			} else {
				io.WriteString(w, fmt.Sprintf("\n"))
			}
		}
	case []float32:
		data := ser.data.([]float32)
		for j := first; j < last; j++ {
			if (ser.missing == nil) || !ser.missing[j] {
				io.WriteString(w, fmt.Sprintf("  %v\n", data[j]))
			} else {
				io.WriteString(w, fmt.Sprintf("\n"))
			}
		}
	case []int64:
		data := ser.data.([]int64)
		for j := first; j < last; j++ {
			if (ser.missing == nil) || !ser.missing[j] {
				io.WriteString(w, fmt.Sprintf("  %v\n", data[j]))
			} else {
				io.WriteString(w, fmt.Sprintf("\n"))
			}
		}
	case []int32:
		data := ser.data.([]int32)
		for j := first; j < last; j++ {
			if (ser.missing == nil) || !ser.missing[j] {
				io.WriteString(w, fmt.Sprintf("  %v\n", data[j]))
			} else {
				io.WriteString(w, fmt.Sprintf("\n"))
			}
		}
	case []int16:
		data := ser.data.([]int16)
		for j := first; j < last; j++ {
			if (ser.missing == nil) || !ser.missing[j] {
				io.WriteString(w, fmt.Sprintf("  %v\n", data[j]))
			} else {
				io.WriteString(w, fmt.Sprintf("\n"))
			}
		}
	case []int8:
		data := ser.data.([]int8)
		for j := first; j < last; j++ {
			if (ser.missing == nil) || !ser.missing[j] {
				io.WriteString(w, fmt.Sprintf("  %v\n", data[j]))
			} else {
				io.WriteString(w, fmt.Sprintf("\n"))
			}
		}
	case []uint64:
		data := ser.data.([]uint64)
		for j := first; j < last; j++ {
			if (ser.missing == nil) || !ser.missing[j] {
				io.WriteString(w, fmt.Sprintf("  %v\n", data[j]))
			} else {
				io.WriteString(w, fmt.Sprintf("\n"))
			}
		}
	case []string:
		data := ser.data.([]string)
		for j := first; j < last; j++ {
			if (ser.missing == nil) || !ser.missing[j] {
				io.WriteString(w, fmt.Sprintf("  %v\n", data[j]))
			} else {
				io.WriteString(w, fmt.Sprintf("\n"))
			}
		}
	case []time.Time:
		data := ser.data.([]time.Time)
		for j := first; j < last; j++ {
			if (ser.missing == nil) || !ser.missing[j] {
				io.WriteString(w, fmt.Sprintf("  %v\n", data[j]))
			} else {
				io.WriteString(w, fmt.Sprintf("\n"))
			}
		}
	}
}

// Print prints the entire Series to the standard output.
func (ser *Series) Print() {
	ser.Write(os.Stdout)
}

// PrintRange printes a slice of the Series to the standard output.
func (ser *Series) PrintRange(first, last int) {
	ser.WriteRange(os.Stdout, first, last)
}

// Data returns the data component of the Series.
func (ser *Series) Data() interface{} {
	return ser.data
}

// Missing returns the array of missing value indicators.
func (ser *Series) Missing() []bool {
	return ser.missing
}

// Length returns the number of elements in a Series.
func (ser *Series) Length() int {
	return ser.length
}

// AllClose returns true if the Series is within tol of the other
// series.  If the Series contains non-floating point values, tol is
// ignored and this is equivalent to testing equality.
func (ser *Series) AllClose(other *Series, tol float64) bool {

	if (ser.missing != nil) && (other.missing != nil) {
		for j := 0; j < ser.length; j++ {
			if ser.missing[j] != other.missing[j] {
				return false
			}
		}
	}

	// Utility function for missing mask
	cmiss := func(j int) int {
		f1 := (ser.missing == nil) || (ser.missing[j] == false)
		f2 := (other.missing == nil) || (other.missing[j] == false)
		if f1 != f2 {
			return 0 // inconsistent
		} else if f1 {
			return 1 // both non-missing
		} else {
			return 2 // both missing
		}
	}

	switch ser.data.(type) {
	default:
		panic("Unknown type in Series.AllClose")
	case []float64:
		for j := 0; j < ser.length; j++ {
			c := cmiss(j)
			if c == 0 {
				return false
			}
			if (c == 1) && (math.Abs(ser.data.([]float64)[j]-other.data.([]float64)[j]) > tol) {
				return false
			}
		}
	case []float32:
		for j := 0; j < ser.length; j++ {
			c := cmiss(j)
			if c == 0 {
				return false
			}
			d := ser.data.([]float32)[j] - other.data.([]float32)[j]
			if (c == 1) && (math.Abs(float64(d)) > tol) {
				return false
			}
		}
	case []int64:
		for j := 0; j < ser.length; j++ {
			c := cmiss(j)
			if c == 0 {
				return false
			}
			if (c == 1) && (ser.data.([]int64)[j] != other.data.([]int64)[j]) {
				return false
			}
		}
	case []int32:
		for j := 0; j < ser.length; j++ {
			c := cmiss(j)
			if c == 0 {
				return false
			}
			if (c == 1) && (ser.data.([]int32)[j] != other.data.([]int32)[j]) {
				return false
			}
		}
	case []int16:
		for j := 0; j < ser.length; j++ {
			c := cmiss(j)
			if c == 0 {
				return false
			}
			if (c == 1) && (ser.data.([]int16)[j] != other.data.([]int16)[j]) {
				return false
			}
		}
	case []int8:
		for j := 0; j < ser.length; j++ {
			c := cmiss(j)
			if c == 0 {
				return false
			}
			if (c == 1) && (ser.data.([]int8)[j] != other.data.([]int8)[j]) {
				return false
			}
		}
	case []uint64:
		for j := 0; j < ser.length; j++ {
			c := cmiss(j)
			if c == 0 {
				return false
			}
			if (c == 1) && (ser.data.([]uint64)[j] != other.data.([]uint64)[j]) {
				return false
			}
		}
	case []string:
		for j := 0; j < ser.length; j++ {
			c := cmiss(j)
			if c == 0 {
				return false
			}
			if (c == 1) && (ser.data.([]string)[j] != other.data.([]string)[j]) {
				return false
			}
		}
	}
	return true
}

// AllEqual returns true if and only if the two Series are identical.
func (ser *Series) AllEqual(other *Series) bool {
	return ser.AllClose(other, 0.0)
}

// UpcastNumeric converts in-place all numeric type variables to
// float64 values.  Non-numeric data is not affected.
func (ser *Series) UpcastNumeric() {

	switch ser.data.(type) {

	default:
		panic(fmt.Sprintf("unknown data type: %T\n", ser.data))
	case []float64:
		// do nothing
	case []string:
		// do nothing
	case []time.Time:
		// do nothing
	case []float32:
		d := ser.data.([]float32)
		n := len(d)
		a := make([]float64, n)
		for i := 0; i < n; i++ {
			a[i] = float64(d[i])
		}
		ser.data = a
	case []int64:
		d := ser.data.([]int64)
		n := len(d)
		a := make([]float64, n)
		for i := 0; i < n; i++ {
			a[i] = float64(d[i])
		}
		ser.data = a
	case []int32:
		d := ser.data.([]int32)
		n := len(d)
		a := make([]float64, n)
		for i := 0; i < n; i++ {
			a[i] = float64(d[i])
		}
		ser.data = a
	case []int16:
		d := ser.data.([]int16)
		n := len(d)
		a := make([]float64, n)
		for i := 0; i < n; i++ {
			a[i] = float64(d[i])
		}
		ser.data = a
	case []int8:
		d := ser.data.([]int8)
		n := len(d)
		a := make([]float64, n)
		for i := 0; i < n; i++ {
			a[i] = float64(d[i])
		}
		ser.data = a
	}
}

// SeriesArray is an array of pointers to Series objects.  It can represent
// a dataset consisting of several variables.
type SeriesArray []*Series

// AllClose returns true if the numeric values in the two series are
// within the given tolerance.  The behavior is identical to AllEqual
// for string data.
func (ser SeriesArray) AllClose(other []*Series, tol float64) bool {

	if len(ser) != len(other) {
		return false
	}

	for j := 0; j < len(ser); j++ {
		if !ser[j].AllClose(other[j], tol) {
			return false
		}
	}

	return true
}

// AllEqual returns true if the elements in the two series are identical.
func (ser SeriesArray) AllEqual(other []*Series) bool {
	return ser.AllClose(other, 0.0)
}
