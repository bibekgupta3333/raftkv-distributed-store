MODULE  := github.com/bibekgupta3333/raftkv-distributed-store
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
SHA     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS := -X $(MODULE)/internal/version.Version=$(VERSION) -X $(MODULE)/internal/version.GitSHA=$(SHA)
SEED    ?= 1
IMAGE   ?= raftkvd:e2e
COMPOSE := IMAGE=$(IMAGE) docker compose -f deploy/compose/docker-compose.yml
E2E_PROFILE ?= pr

.PHONY: all fmt fmt-check lint build test race proto clean help tools sim e2e e2e-image cluster-up cluster-down cluster-logs

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

e2e-image: ## build the e2e node image (debugpropose, iptables, tc)
	docker build --target e2e --build-arg GOFLAGS_TAGS=debugpropose --build-arg VERSION=$(VERSION) --build-arg SHA=$(SHA) -t $(IMAGE) .

cluster-up: e2e-image ## start the 5-node compose cluster + Prometheus (localhost:19090)
	$(COMPOSE) up -d --no-build

cluster-down: ## stop the compose cluster and delete its volumes
	$(COMPOSE) down -v --remove-orphans

cluster-logs: ## follow the compose cluster logs
	$(COMPOSE) logs -f --tail=100

e2e: e2e-image ## docker-compose e2e (E2E_PROFILE=pr|nightly E2E_SEED=n E2E_ROUNDS=n)
	IMAGE=$(IMAGE) E2E_PROFILE=$(E2E_PROFILE) go test -tags e2e -count=1 -timeout 120m -v ./test/e2e/...

clean: ## remove build output
	rm -rf bin
