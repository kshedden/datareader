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
	// Leave false when testing
	generateColumnize = false
)

// Not really a test function, used to generate md5 sums for the results.
func TestGenerateColumnize(t *testing.T) {

	if !generateColumnize {
		return
	}

	ms := make(map[string][16]byte)

	allTestFiles := getFilenames()

	for _, f := range allTestFiles {
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

	// Clear the workspace and set up the subdirectories.
	outpath := filepath.Join("test_files", "tmp", "cols")
	if err := os.RemoveAll(outpath); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(outpath, os.ModePerm); err != nil {
		panic(err)
	}

	// Run columnize on the file
	cmdName := filepath.Join(os.Getenv("GOBIN"), "columnize")
	infile := filepath.Join("test_files", "data", fname)
	args := []string{
		fmt.Sprintf("-in=%s", infile),
		fmt.Sprintf("-out=%s", outpath),
		fmt.Sprintf("-mode=%s", mode),
	}
	cmd := exec.Command(cmdName, args...)
	cmd.Stderr = os.Stderr
	if _, err := cmd.Output(); err != nil {
		panic(err)
	}

	files, err := ioutil.ReadDir(outpath)
	if err != nil {
		panic(err)
	}
	var fileNames []string
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

		if _, err := buf.Write(ba); err != nil {
			panic(err)
		}
	}

	return md5.Sum(buf.Bytes())
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

	// Read the stored checksums
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
			m1 := checksum[f+"::"+mode]

			for j := range m {
				if m[j] != m1[j] {
					fmt.Printf("Failing %s    mode: %s\n", f, mode)
					fmt.Printf("Expected %v, got %v\n", m[j], m1[j])
					t.Fail()
				}
			}
		}
	}
}
