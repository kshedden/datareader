package datareader

import (
	"fmt"
)

// StatfileReader is an interface that can be used to work
// interchangeably with StataReader and SAS7BDAT objects.
type StatfileReader interface {
	ColumnNames() []string
	ColumnTypes() []ColumnTypeT
	RowCount() int
	Read(int) ([]*Series, error)
}

func upcastNumeric(vector interface{}) ([]float64, error) {

	switch vec := vector.(type) {
	default:
		return nil, fmt.Errorf("unknown type %T in upcast_numeric", vec)
	case []float64:
		return vec, nil
	case []float32:
		n := len(vec)
		x := make([]float64, n)
		for i := 0; i < n; i++ {
			x[i] = float64(vec[i])
		}
		return x, nil
	case []int64:
		n := len(vec)
		x := make([]float64, n)
		for i := 0; i < n; i++ {
			x[i] = float64(vec[i])
		}
		return x, nil
	case []int32:
		n := len(vec)
		x := make([]float64, n)
		for i := 0; i < n; i++ {
			x[i] = float64(vec[i])
		}
		return x, nil
	case []int16:
		n := len(vec)
		x := make([]float64, n)
		for i := 0; i < n; i++ {
			x[i] = float64(vec[i])
		}
		return x, nil
	case []int8:
		n := len(vec)
		x := make([]float64, n)
		for i := 0; i < n; i++ {
			x[i] = float64(vec[i])
		}
		return x, nil
	}
}

func castToInt(x interface{}) ([]int64, error) {

	switch v := x.(type) {
	default:
		return nil, fmt.Errorf("cannot cast %T to integer", x)
	case []int64:
		return x.([]int64), nil
	case []float64:
		y := make([]int64, len(v))
		for i, z := range v {
			y[i] = int64(z)
		}
		return y, nil
	case []float32:
		y := make([]int64, len(v))
		for i, z := range v {
			y[i] = int64(z)
		}
		return y, nil
	case []int32:
		y := make([]int64, len(v))
		for i, z := range v {
			y[i] = int64(z)
		}
		return y, nil
	case []int16:
		y := make([]int64, len(v))
		for i, z := range v {
			y[i] = int64(z)
		}
		return y, nil
	case []int8:
		y := make([]int64, len(v))
		for i, z := range v {
			y[i] = int64(z)
		}
		return y, nil
	}
}
