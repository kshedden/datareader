// +build ignore

package main

import (
	"os"
	"os/exec"
)

func main() {

	os.MkdirAll("tmp", 0777)

	args := []string{"run", "sas_to_parquet.go", "-sasfile=../../test_files/data/test1.sas7bdat", "-structname=Data",
		"-pkgname=test1", "-outdir=tmp"}

	cmd := exec.Command("go", args...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	if err := cmd.Run(); err != nil {
		panic(err)
	}

}
