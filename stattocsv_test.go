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

func run_stattocsv(filenames []string) map[string][16]byte {

	checksums := make(map[string][16]byte)

	cmd_name := filepath.Join(os.Getenv("GOBIN"), "stattocsv")
	for _, file := range filenames {
		infile := filepath.Join("test_files", "data", file)
		args := []string{infile}
		rslt, err := exec.Command(cmd_name, args...).Output()
		if err != nil {
			fmt.Printf("xx:: %v %v\n", cmd_name, infile)
			panic(err)
		}
		checksums[file] = md5.Sum(rslt)
	}

	return checksums
}

func ref_checksums(filenames []string) map[string][16]byte {

	checksums := make(map[string][16]byte)

	for _, file := range filenames {
		file1 := strings.Replace(file, ".dta", ".csv", -1)
		file1 = strings.Replace(file1, ".sas7bdat", ".csv", -1)
		infile := filepath.Join("test_files", "ref", file1)
		fid, err := os.Open(infile)
		if err != nil {
			panic(err)
		}
		b, err := ioutil.ReadAll(fid)
		if err != nil {
			panic(err)
		}
		checksums[file] = md5.Sum(b)
	}

	return checksums
}

func get_filenames() []string {
	files, err := ioutil.ReadDir(filepath.Join("test_files", "data"))
	if err != nil {
		panic(err)
	}
	filenames := make([]string, 0, 10)
	for _, f := range files {
		name := f.Name()

		if strings.Contains(name, "binary") {
			continue
		}

		if !strings.HasPrefix(name, ".") && (strings.HasSuffix(name, ".dta") || strings.HasSuffix(name, ".sas7bdat")) {
			filenames = append(filenames, name)
		}
	}

	return filenames
}

func Test_stattocsv_1(t *testing.T) {

	test_files := get_filenames()
	new_checksums := run_stattocsv(test_files)
	old_checksums := ref_checksums(test_files)

	for ky, _ := range old_checksums {

		for j := 0; j < 16; j++ {
			if new_checksums[ky][j] != old_checksums[ky][j] {
				fmt.Printf("%v\n%v\n%v\n\n", ky, new_checksums[ky], old_checksums[ky])
				t.Fail()
			}
		}
	}
}
