package datareader

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

var all_test_files = []string{"test1_115.dta", "test1_115b.dta", "test1_117.dta", "test1_118.dta",
	"test1_compression_no.sas7bdat", "test1_compression_binary.sas7bdat",
	"test1_compression_char.sas7bdat"}

const (
	generate_columnize = false
)

// Not really a test function, used to generate md5 sums for the results.
func Test_generate_columnize(t *testing.T) {

	if !generate_columnize {
		return
	}

	ms := make(map[string][16]byte)

	for _, f := range all_test_files {
		for _, mode := range []string{"text", "binary"} {
			m := columnize_base(f, mode)
			k := f + "::" + mode
			ms[k] = m
		}
	}

	b, err := json.Marshal(ms)
	if err != nil {
		panic(err)
	}

	cf, err := os.Create(filepath.Join("test_files", "columnize_checksums.json"))
	if err != nil {
		panic(err)
	}

	cf.Write(b)
	cf.Close()
}

func columnize_base(fname, mode string) [16]byte {

	outpath := filepath.Join("test_files", "cols")
	os.RemoveAll(outpath)
	os.Mkdir(outpath, os.ModeDir)

	cmd_name := filepath.Join(os.Getenv("GOBIN"), "columnize")
	infile := filepath.Join("test_files", fname)
	args := []string{fmt.Sprintf("-in=%s", infile), fmt.Sprintf("-out=%s", outpath),
		fmt.Sprintf("-mode=%s", mode)}
	_, err := exec.Command(cmd_name, args...).Output()
	if err != nil {
		panic(err)
	}

	files, _ := ioutil.ReadDir(outpath)
	file_names := make([]string, 0, 10)
	for _, v := range files {
		file_names = append(file_names, v.Name())
	}
	sort.Strings(file_names)

	var buf bytes.Buffer
	for _, f := range file_names {
		if strings.HasPrefix(f, ".") {
			continue
		}
		gname := filepath.Join("test_files", "cols", f)
		g, err := os.Open(gname)
		if err != nil {
			panic(err)
		}
		ba, err := ioutil.ReadAll(g)
		if err != nil {
			panic(err)
		}
		buf.Write(ba)
	}
	m := md5.Sum(buf.Bytes())

	return m
}

func Test_columnize_1(t *testing.T) {

	if generate_columnize {
		return
	}

	cf, err := os.Open(filepath.Join("test_files", "columnize_checksums.json"))
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
		for _, mode := range []string{"text", "binary"} {

			m := columnize_base(f, mode)
			k := f + "::" + mode
			m1 := checksum[k]

			for j, _ := range m {
				if m[j] != m1[j] {
					t.Fail()
				}
			}
		}

	}
}
