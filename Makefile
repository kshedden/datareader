SRC_DIR := $(dir $(abspath $(firstword $(MAKEFILE_LIST))))

make_tools := brew gotestsum golangci-lint

$(make_tools):
	$(shell command -v $@ 2>&1> /dev/null || (echo "ERROR: $@ is required: run 'make deps' to install"; exit 1))

.PHONY: build
# -s -w can be used to strip debug symbols to reduce binary size, but having debug symbols is needed for analyzing dumps
build:
	CGO_ENABLED=0 go build -trimpath -o columnize ./cmd/columnize/main.go
	CGO_ENABLED=0 go build -trimpath -o sas_to_parquet ./cmd/sas_to_parquet/main.go
	CGO_ENABLED=0 go build -trimpath -o stattocsv ./cmd/stattocsv/main.go

.PHONY: fmt
fmt:
	@echo "=== Running go fmt ==="
	go fmt ./...

.PHONY: test-all
test-all: gotestsum ## Run all tests
	$(touch .env)
	gotestsum --format=pkgname-and-test-fails --format-hivis -- -coverprofile=coverage/cover.out ./... --tags="unit"

.PHONY: common-deps
common-deps:
	go install github.com/go-delve/delve/cmd/dlv@latest
	go install gotest.tools/gotestsum@latest

.PHONY: mac-deps
mac-deps: common-deps ## Install dependencies on mac.
	brew install golangci-lint

.PHONY: linux-deps
linux-deps: common-deps ## Install dependencies on linux
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.58.2

# NOTE: docker variant runs much slower
# docker run -t --rm -v $$(pwd):/app -v ~/.cache/golangci-lint/v1.58.2:/root/.cache -w /app golangci/golangci-lint:v1.58.2 golangci-lint run -v
.PHONY: lint
lint: golangci-lint
	@requiredver="1.58.2"; \
	lintver=$$(golangci-lint version | sed "s/^.*has version \([0-9.]*\) .*/\1/"); \
 	if [ "$${lintver}" != "$${requiredver}" ] && [ "$$(printf '%s\n' "$$requiredver" "$$lintver" | sort -V | head -n1)" = "$$lintver" ]; then \
        echo "Linter is $${lintver}, but must be at least $${requiredver} to match CI. Upgrade golangci-lint" && exit 1; \
 	fi;
	@echo "=== Running golangci-lint ==="
	golangci-lint --timeout 5m run -v --out-format line-number:stdout --fix

.PHONY: test-short
test-short: gotestsum
	gotestsum --format=pkgname-and-test-fails --format-hivis -- -coverprofile=coverage/cover.out ./... -short

.PHONY: test
test: fmt lint test-all

.PHONY: test-coverage
test-coverage: ## Run all tests and generate a coverage report
	$(touch .env)
	gotestsum --format=pkgname-and-test-fails --format-hivis -- -covermode=count -coverpkg=./... -coverprofile=coverage/cover.out.tmp -v  ./... --tags="unit"

	# Stripping out API docs and internal/app/mocks from the coverage report
	cat coverage/cover.out.tmp | grep -v "api/docs.go" | grep -v "mock_" > coverage/cover.out

	go tool cover -html coverage/cover.out -o coverage/cover.html
