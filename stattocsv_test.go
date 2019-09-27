package datareader

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runStattocsv runs stattocsv on all the files in the test_files
// directory, and calculates an md5 checksum on each output file.
func runStattocsv(filenames []string) map[string][16]byte {

	checksums := make(map[string][16]byte)

	cmdName := filepath.Join(os.Getenv("GOBIN"), "stattocsv")
	for _, file := range filenames {
		infile := filepath.Join("test_files", "data", file)
		args := []string{infile}
		cmd := exec.Command(cmdName, args...)
		cmd.Stderr = os.Stderr
		rslt, err := cmd.Output()
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("runStattocsv:: %v %v\n", cmdName, infile))
			panic(err)
		}
		checksums[file] = md5.Sum(rslt)
	}

	return checksums
}

func refChecksums(filenames []string) map[string][16]byte {

	checksums := make(map[string][16]byte)

	for _, file := range filenames {

		file1 := strings.Replace(file, ".dta", ".csv", -1)
		file1 = strings.Replace(file1, ".sas7bdat", ".csv", -1)

		var b []byte

		infile := filepath.Join("test_files", "ref", file1)
		fid, err := os.Open(infile)
		if os.IsNotExist(err) {
			fid, err = os.Open(infile)
			if err != nil {
				panic(err)
			}
			b, err = ioutil.ReadAll(fid)
			if err != nil {
				panic(err)
			}
		} else if err != nil {
			panic(err)
		} else {
			b, err = ioutil.ReadAll(fid)
			if err != nil {
				panic(err)
			}
		}

		checksums[file] = md5.Sum(b)
	}

	return checksums
}

func getFilenames() []string {

	files, err := ioutil.ReadDir(filepath.Join("test_files", "data"))
	if err != nil {
		panic(err)
	}

	var filenames []string
	for _, f := range files {
		name := f.Name()
		if strings.HasSuffix(name, ".dta") || strings.HasSuffix(name, ".sas7bdat") {
			filenames = append(filenames, name)
		}
	}

	return filenames
}

func TestStattocsv1(t *testing.T) {

	testFiles := getFilenames()
	newChecksums := runStattocsv(testFiles)
	oldChecksums := refChecksums(testFiles)

	for ky := range oldChecksums {

		for j := 0; j < 16; j++ {
			if newChecksums[ky][j] != oldChecksums[ky][j] {
				fmt.Printf("%v\n%v\n%v\n\n", ky, newChecksums[ky], oldChecksums[ky])
				t.Fail()
			}
		}
	}
}
