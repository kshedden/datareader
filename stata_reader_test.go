package datareader

import (
	//"fmt"
	"os"
	"testing"
)

func TestStata1(t *testing.T) {

	r, _ := os.Open("stata_files/stata14_118.dta")

	sr, err := NewStataReader(r)
	if err != nil {
		t.Fail()
	}

	_, err = sr.Read(10)
	if err != nil {
		t.Fail()
	}

	// need more testing
}
