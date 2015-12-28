package datareader

import (
	"crypto/md5"
	"encoding/json"
	//"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const (
	generate_stattocsv = false
)

// Not really a test function, used to generate md5 sums for the results.
func Test_generate_stattocsv(t *testing.T) {

	if !generate_stattocsv {
		return
	}

	ms := make(map[string][16]byte)

	for _, f := range all_test_files {
		m := stattocsv_base(f)
		ms[f] = m
	}

	b, err := json.Marshal(ms)
	if err != nil {
		panic(err)
	}

	cf, err := os.Create(filepath.Join("test_files", "stattocsv_checksums.json"))
	if err != nil {
		panic(err)
	}

	cf.Write(b)
	cf.Close()
}

func stattocsv_base(fname string) [16]byte {

	cmd_name := filepath.Join(os.Getenv("GOBIN"), "stattocsv")
	infile := filepath.Join("test_files", fname)
	args := []string{infile}
	rslt, err := exec.Command(cmd_name, args...).Output()
	if err != nil {
		panic(err)
	}

	m := md5.Sum(rslt)

	return m
}

func Test_stattocsv_1(t *testing.T) {

	if generate_stattocsv {
		return
	}

	cf, err := os.Open(filepath.Join("test_files", "stattocsv_checksums.json"))
	if err != nil {
		panic(err)
	}

	var checksum map[string][]byte
	b, err := ioutil.ReadAll(cf)
	if err != nil {
		panic(err)
	}
	json.Unmarshal(b, &checksum)

	for _, f := range all_test_files {
		m := stattocsv_base(f)
		m1 := checksum[f]

		for j, _ := range m {
			if m[j] != m1[j] {
				t.Fail()
			}
		}
	}
}
