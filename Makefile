MODULE  := github.com/bibekgupta3333/raftkv-distributed-store
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
SHA     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS := -X $(MODULE)/internal/version.Version=$(VERSION) -X $(MODULE)/internal/version.GitSHA=$(SHA)
SEED    ?= 1

.PHONY: all fmt fmt-check lint build test race proto clean help tools sim

all: fmt-check lint build test ## fmt-check, lint, build, test

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

tools: ## install pinned tools (go tool directives)
	go mod download
	go tool golangci-lint --version
	go tool buf --version

fmt: ## format code
	gofmt -w .

fmt-check: ## fail if code is not gofmt'd
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

lint: ## golangci-lint + buf lint
	go tool golangci-lint run ./...
	go tool buf lint

build: ## build binaries into bin/
	mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o bin/ ./cmd/...

test: ## unit tests
	go test ./...

race: ## unit tests with race detector
	go test -race ./...

sim: ## run the deterministic simulator (SEED=n)
	go test ./test/sim/... -run Sim -seeds=500 -start=$(SEED)

proto: ## regenerate protobuf code
	go tool buf generate

clean: ## remove build output
	rm -rf bin
