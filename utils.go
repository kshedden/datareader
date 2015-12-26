package datareader

import (
	//"fmt"
	"math"
)

// DataEqual returns true if and only if the two lists of arrays are identical.
func ArrayListEqual(a, b []interface{}) bool {

	if len(a) != len(b) {
		return false
	}

	for k, _ := range a {

		// Float
		af, af_ok := a[k].([]float64)
		bf, bf_ok := b[k].([]float64)
		if af_ok && bf_ok {
			if !compare_floatarrays(af, bf) {
				return false
			}
			continue
		}

		// String
		as, as_ok := a[k].([]string)
		bs, bs_ok := b[k].([]string)
		if as_ok && bs_ok {
			if !compare_stringarrays(as, bs) {
				return false
			}
			continue
		}

		// If we reach here, the types are mismatched
		return false
	}

	return true
}

// compare_floatarrays returns true if and only if the two arrays hold identical values.
func compare_floatarrays(a, b []float64) bool {

	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if (a[i] != b[i]) && !math.IsNaN(a[i]) && !math.IsNaN(b[i]) {
			return false
		}
	}

	return true
}

// compare_stringarrays returns true if and only if the two arrays hold identical values.
func compare_stringarrays(a, b []string) bool {

	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
