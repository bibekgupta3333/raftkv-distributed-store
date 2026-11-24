MODULE  := github.com/bibekgupta3333/raftkv-distributed-store
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
SHA     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS := -X $(MODULE)/internal/version.Version=$(VERSION) -X $(MODULE)/internal/version.GitSHA=$(SHA)
SEED    ?= 1
IMAGE   ?= raftkvd:e2e
COMPOSE := IMAGE=$(IMAGE) docker compose -f deploy/compose/docker-compose.yml
E2E_PROFILE ?= pr
SEEDS   ?= 500
FAULTS  ?= full
TRACEDIR ?= out/sim
SIMRACE ?=

.PHONY: all fmt fmt-check lint build test race proto clean help tools sim e2e e2e-image mutation cluster-up cluster-down cluster-logs fuzz docs-check

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

sim: ## deterministic simulator: SEEDS=500 SEED=1 (first seed) FAULTS=full [SIMRACE=-race]
	go test $(SIMRACE) ./test/sim/... -run Sim -seeds=$(SEEDS) -start=$(SEED) -faults=$(FAULTS) -tracedir=$(TRACEDIR) -timeout=60m

proto: ## regenerate protobuf code
	go tool buf generate

e2e-image: ## build the e2e node image (debugpropose, iptables, tc)
	docker build --target e2e --build-arg GOFLAGS_TAGS=debugpropose --build-arg VERSION=$(VERSION) --build-arg SHA=$(SHA) -t $(IMAGE) .

cluster-up: e2e-image ## 5-node compose cluster + Prometheus :19090, Grafana :13000, observer :18080 (localhost)
	$(COMPOSE) up -d --no-build

cluster-down: ## stop the compose cluster and delete its volumes
	$(COMPOSE) down -v --remove-orphans

cluster-logs: ## follow the compose cluster logs
	$(COMPOSE) logs -f --tail=100

e2e: e2e-image ## docker-compose e2e (E2E_PROFILE=pr|nightly E2E_SEED=n E2E_ROUNDS=n)
	IMAGE=$(IMAGE) E2E_PROFILE=$(E2E_PROFILE) go test -tags e2e -count=1 -timeout 120m -v ./test/e2e/...

# WBS 7.5.1: native Go fuzz targets for every decoder of bytes from disk or the
# network. `go test -fuzz` runs one target per invocation. Crashers are written
# to <pkg>/testdata/fuzz/<Target>/ and must be committed as regression inputs.
FUZZTIME ?= 60s
FUZZ_TARGETS := \
	./internal/wal:FuzzDecodeRecord ./internal/wal:FuzzDecodeSegmentHeader \
	./internal/wal:FuzzParseSegmentName ./internal/wal:FuzzReplaySegment \
	./internal/snapshot:FuzzDecodeMeta ./internal/snapshot:FuzzParseName \
	./internal/command:FuzzUnmarshal ./internal/server:FuzzRangePageToken \
	./internal/storage:FuzzDecodeValue ./internal/storage:FuzzDecodeSession \
	./internal/storage:FuzzDecodeReply ./internal/node:FuzzParseConfigYAML \
	./internal/node:FuzzParsePeers

fuzz: ## run every fuzz target for FUZZTIME each (default 60s); FUZZ=<name> runs one
	@failed=""; for t in $(FUZZ_TARGETS); do \
		pkg=$${t%%:*}; name=$${t##*:}; \
		if [ -n "$(FUZZ)" ] && [ "$(FUZZ)" != "$$name" ]; then continue; fi; \
		echo "== $$pkg $$name ($(FUZZTIME))"; \
		go test $$pkg -run '^$$' -fuzz "^$$name\$$" -fuzztime=$(FUZZTIME) || failed="$$failed $$name"; \
	done; \
	if [ -n "$$failed" ]; then echo "fuzz targets with crashers:$$failed"; exit 1; fi

docs-check: ## fail if docs/configuration.md misses a flag or YAML key (WBS 7.6.1)
	go test ./cmd/... -run 'TestConfigurationDoc' -count=1

mutation: ## checker vs a deliberately broken read path (testing.md 3.4): must report a violation
	go test -tags raftkv_mutation -count=1 -v -run TestCheckerCatchesBrokenReadPath ./test/linearizability/

# --- Kubernetes on kind (D-16, WBS 7.1) and Chaos Mesh (D-47, WBS 7.4) ---
# Every kubectl/helm call names the kind context explicitly, so these targets
# never touch whatever cluster the current context points at.
KIND_VERSION   ?= v0.30.0
KIND           ?= $(shell command -v kind 2>/dev/null || echo $(CURDIR)/bin/kind)
KIND_CLUSTER   ?= raftkv
KIND_IMAGE     ?= raftkvd:kind
KIND_CTX       := kind-$(KIND_CLUSTER)
KUBECTL        := kubectl --context $(KIND_CTX)
K8S_NS         := raftkv
CHAOS_MESH_VERSION ?= 2.8.8
CHAOS_NS       := chaos-mesh
# A private repo config and cache under out/ keep the install independent of
# (and from changing) the user's own helm repositories.
HELM           := HELM_REPOSITORY_CONFIG=$(CURDIR)/out/helm/repositories.yaml \
                  HELM_REPOSITORY_CACHE=$(CURDIR)/out/helm/cache helm --kube-context $(KIND_CTX)

.PHONY: kind-image kind-up kind-down kind-status chaos-up chaos-down chaos-apply chaos-clear chaos-image chaos

$(CURDIR)/bin/kind:
	GOBIN=$(CURDIR)/bin go install sigs.k8s.io/kind@$(KIND_VERSION)

kind-image: ## build the release image as raftkvd:kind
	docker build --target release --build-arg VERSION=$(VERSION) --build-arg SHA=$(SHA) -t $(KIND_IMAGE) .

kind-up: kind-image $(KIND) ## create kind cluster "raftkv", load the image, deploy the 5-node StatefulSet
	@$(KIND) get clusters 2>/dev/null | grep -qx '$(KIND_CLUSTER)' || \
		$(KIND) create cluster --name $(KIND_CLUSTER) --config deploy/k8s/overlays/kind/kind-config.yaml --wait 120s
	$(KIND) load docker-image $(KIND_IMAGE) --name $(KIND_CLUSTER)
	@# A reloaded image under the same tag only takes effect in new pods, so an
	@# already-deployed StatefulSet gets a rolling restart.
	@existed=$$($(KUBECTL) -n $(K8S_NS) get statefulset raftkv -o name 2>/dev/null); \
		$(KUBECTL) apply -k deploy/k8s/overlays/kind && \
		if [ -n "$$existed" ]; then $(KUBECTL) -n $(K8S_NS) rollout restart statefulset/raftkv; fi
	$(KUBECTL) -n $(K8S_NS) rollout status statefulset/raftkv --timeout=300s

kind-status: ## show RaftKV pods and each node's readiness
	$(KUBECTL) -n $(K8S_NS) get pods -o wide

kind-down: $(KIND) ## delete the kind cluster
	$(KIND) delete cluster --name $(KIND_CLUSTER)

chaos-up: kind-up ## kind-up, then install Chaos Mesh (pinned chart) into the cluster
	$(HELM) upgrade --install chaos-mesh chaos-mesh \
		--repo https://charts.chaos-mesh.org --version $(CHAOS_MESH_VERSION) \
		--namespace $(CHAOS_NS) --create-namespace \
		--set chaosDaemon.runtime=containerd \
		--set chaosDaemon.socketPath=/run/containerd/containerd.sock \
		--set dashboard.create=false \
		--wait --timeout 10m
	$(KUBECTL) -n $(CHAOS_NS) get pods

chaos-apply: ## apply one fault: FAULT=pod-kill|pod-kill-two|network-partition|network-delay|network-loss
	@test -n "$(FAULT)" || (echo "usage: make chaos-apply FAULT=<name of test/chaos/manifests/*.yaml>" && exit 2)
	$(KUBECTL) apply -f test/chaos/manifests/$(FAULT).yaml

chaos-clear: ## delete every chaos experiment in the RaftKV namespace
	$(KUBECTL) -n $(K8S_NS) delete podchaos,networkchaos --all --ignore-not-found

CHAOS_IMAGE    ?= raftkv-chaos:kind
CHAOS_DURATION ?= 10m
CHAOS_SEED     ?=

chaos-image: ## build the in-cluster chaos workload image as raftkv-chaos:kind
	docker build --target chaos -t $(CHAOS_IMAGE) .

chaos: chaos-image $(KIND) ## fault schedule + Porcupine check on a `make chaos-up` cluster: CHAOS_DURATION=10m CHAOS_SEED=n
	$(KIND) load docker-image $(CHAOS_IMAGE) --name $(KIND_CLUSTER)
	CHAOS_DURATION=$(CHAOS_DURATION) CHAOS_SEED=$(CHAOS_SEED) CHAOS_IMAGE=$(CHAOS_IMAGE) \
		CHAOS_KUBE_CONTEXT=$(KIND_CTX) go test -tags chaos -count=1 -timeout 240m -v -run TestChaos ./test/chaos

chaos-down: kind-down ## delete the kind cluster (Chaos Mesh and all experiments go with it)

clean: ## remove build output
	rm -rf bin
