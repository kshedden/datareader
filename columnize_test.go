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

const (
	generateColumnize = false
)

// Not really a test function, used to generate md5 sums for the results.
func TestGenerateColumnize(t *testing.T) {

	if !generateColumnize {
		return
	}

	ms := make(map[string][16]byte)

	all_test_files := getFilenames()

	for _, f := range all_test_files {
		for _, mode := range []string{"text", "binary"} {
			m := columnizeBase(f, mode)
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

	if _, err := cf.Write(b); err != nil {
		panic(err)
	}
	cf.Close()
}

func columnizeBase(fname, mode string) [16]byte {

	outpath := filepath.Join("test_files", "tmp", "cols")
	os.RemoveAll(outpath)
	if err := os.MkdirAll(outpath, os.ModeDir); err != nil {
		panic(err)
	}

	cmdName := filepath.Join(os.Getenv("GOBIN"), "columnize")
	infile := filepath.Join("test_files", "tmp", "cols", fname)
	args := []string{fmt.Sprintf("-in=%s", infile), fmt.Sprintf("-out=%s", outpath),
		fmt.Sprintf("-mode=%s", mode)}
	_, err := exec.Command(cmdName, args...).Output()
	if err != nil {
		panic(err)
	}

	files, _ := ioutil.ReadDir(outpath)
	fileNames := make([]string, 0, 10)
	for _, v := range files {
		fileNames = append(fileNames, v.Name())
	}
	sort.Strings(fileNames)

	var buf bytes.Buffer
	for _, f := range fileNames {
		if strings.HasPrefix(f, ".") {
			continue
		}
		gname := filepath.Join("test_files", "tmp", "cols", f)
		g, err := os.Open(gname)
		if err != nil {
			panic(err)
		}
		defer g.Close()
		ba, err := ioutil.ReadAll(g)
		if err != nil {
			panic(err)
		}
		buf.Write(ba)
	}
	m := md5.Sum(buf.Bytes())

	return m
}

func TestColumnize1(t *testing.T) {

	if generateColumnize {
		return
	}

	cf, err := os.Open(filepath.Join("test_files", "columnize_checksums.json"))
	if err != nil {
		panic(err)
	}
	defer cf.Close()

	var checksum map[string][]byte
	b, err := ioutil.ReadAll(cf)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(b, &checksum); err != nil {
		panic(err)
	}

	allTestFiles := getFilenames()

	for _, f := range allTestFiles {
		for _, mode := range []string{"text", "binary"} {

			m := columnizeBase(f, mode)
			k := f + "::" + mode
			m1 := checksum[k]

			for j := range m {
				if m[j] != m1[j] {
					t.Fail()
				}
			}
		}

	}
}
