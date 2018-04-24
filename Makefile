all:
	go install
	go install cmd/columnize/columnize.go
	go install cmd/stattocsv/stattocsv.go
