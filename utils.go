package datareader

import (
	"errors"
	"fmt"
)

// Statfilereader is an interface that can be used to work
// interchangeably with StataReader and SAS7BDAT objects.
type Statfilereader interface {
	ColumnNames() []string
	ColumnTypes() []int
	RowCount() int
	Read(int) ([]*Series, error)
}

func upcast_numeric(vec interface{}) ([]float64, error) {

	switch vec.(type) {
	default:
		return nil, errors.New("unknown type in upcast_numeric")
	case []float64:
		return vec.([]float64), nil
	case []float32:
		vec1 := vec.([]float32)
		n := len(vec1)
		x := make([]float64, n)
		for i := 0; i < n; i++ {
			x[i] = float64(vec1[i])
		}
		return x, nil
	case []int64:
		vec1 := vec.([]int64)
		n := len(vec1)
		x := make([]float64, n)
		for i := 0; i < n; i++ {
			x[i] = float64(vec1[i])
		}
		return x, nil
	case []int32:
		vec1 := vec.([]int32)
		n := len(vec1)
		x := make([]float64, n)
		for i := 0; i < n; i++ {
			x[i] = float64(vec1[i])
		}
		return x, nil
	case []int16:
		vec1 := vec.([]int16)
		n := len(vec1)
		x := make([]float64, n)
		for i := 0; i < n; i++ {
			x[i] = float64(vec1[i])
		}
		return x, nil
	case []int8:
		vec1 := vec.([]int8)
		n := len(vec1)
		x := make([]float64, n)
		for i := 0; i < n; i++ {
			x[i] = float64(vec1[i])
		}
		return x, nil
	}
}

func cast_to_int(x interface{}) ([]int64, error) {

	switch x.(type) {

	default:
		return nil, errors.New(fmt.Sprintf("cannot cast %T to integer", x))
	case []int64:
		return x.([]int64), nil
	case []float64:
		v := x.([]float64)
		y := make([]int64, len(v))
		for i, z := range v {
			y[i] = int64(z)
		}
		return y, nil
	case []float32:
		v := x.([]float32)
		y := make([]int64, len(v))
		for i, z := range v {
			y[i] = int64(z)
		}
		return y, nil
	case []int32:
		v := x.([]int32)
		y := make([]int64, len(v))
		for i, z := range v {
			y[i] = int64(z)
		}
		return y, nil
	case []int16:
		v := x.([]int16)
		y := make([]int64, len(v))
		for i, z := range v {
			y[i] = int64(z)
		}
		return y, nil
	case []int8:
		v := x.([]int8)
		y := make([]int64, len(v))
		for i, z := range v {
			y[i] = int64(z)
		}
		return y, nil
	}
}
