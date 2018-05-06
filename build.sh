#!/bin/bash

for GOOS in darwin linux windows; do
    for GOARCH in 386 amd64; do
	env GOOS=$GOOS GOARCH=$GOARCH go build -v -o bin/stattocsv-$GOOS-$GOARCH cmd/stattocsv/stattocsv.go
    done
done
