// +build ignore

package main

import (
	"os"
	"os/exec"
)

func main() {

	args := []string{"./sas_to_parquet", "-sasfile=../../test_files/data/test1.sas7bdat", "-structname=Data",
		"-pkgname=test1", "-outdir=tmp"}

	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	if err := cmd.Run(); err != nil {
		panic(err)
	}

}
