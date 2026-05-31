.PHONY: build test test-race test-short lint vet \
        docker-build up down logs clean bench demo help

BINARY      := bin/broker
IMAGE       := mini-kafka:latest
GO_FLAGS    := -ldflags="-s -w"
TEST_FLAGS  := -count=1 -timeout=120s

# ── Build ─────────────────────────────────────────────────────────────────────

build:
	@echo "==> Building broker binary"
	@mkdir -p bin
	go build $(GO_FLAGS) -o $(BINARY) ./cmd/broker

# ── Test ──────────────────────────────────────────────────────────────────────

test:
	@echo "==> Running all tests"
	go test $(TEST_FLAGS) ./...

test-race:
	@echo "==> Running all tests with race detector"
	go test -race $(TEST_FLAGS) ./...

test-short:
	@echo "==> Running short tests (skips large-volume and integration)"
	go test -short $(TEST_FLAGS) ./...

# ── Static analysis ───────────────────────────────────────────────────────────

vet:
	@echo "==> Running go vet"
	go vet ./...

lint: vet
	@echo "==> Running staticcheck (install: go install honnef.co/go/tools/cmd/staticcheck@latest)"
	staticcheck ./... || true

# ── Docker ────────────────────────────────────────────────────────────────────

docker-build:
	@echo "==> Building Docker image $(IMAGE)"
	docker build -t $(IMAGE) .

up: docker-build
	@echo "==> Starting 3-broker cluster + Prometheus + Grafana"
	docker compose up -d
	@echo ""
	@echo "  Broker 1:   localhost:9092"
	@echo "  Broker 2:   localhost:9093"
	@echo "  Broker 3:   localhost:9094"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana:    http://localhost:3000  (admin/admin)"
	@echo ""
	@echo "  Run 'make demo' to run the smoke test against the live cluster."

down:
	@echo "==> Stopping cluster"
	docker compose down

logs:
	docker compose logs -f

clean:
	@echo "==> Cleaning build artifacts and Docker volumes"
	rm -rf bin/
	docker compose down -v --remove-orphans 2>/dev/null || true

# ── Demo ──────────────────────────────────────────────────────────────────────

# demo: spins up a local single-broker (no Docker needed) and runs a smoke test.
# Uses Go directly so Docker is not required.
demo: build
	@echo "==> Starting single-broker demo on :9092"
	@rm -rf /tmp/mini-kafka-demo
	@$(BINARY) --addr=:9092 --data-dir=/tmp/mini-kafka-demo --node-id=1 --host=localhost --port=9092 &
	@BROKER_PID=$$!; \
	sleep 1; \
	echo "==> Running smoke test..."; \
	go run ./scripts/smoke_test/main.go --addr=localhost:9092 && \
	echo "==> Smoke test PASSED" || echo "==> Smoke test FAILED"; \
	kill $$BROKER_PID 2>/dev/null; \
	rm -rf /tmp/mini-kafka-demo

# bench: runs a throughput benchmark against a running broker on :9092.
# Start the broker first with 'make demo' or 'make up'.
bench: build
	@echo "==> Running throughput benchmark against localhost:9092"
	go run ./scripts/bench/main.go --addr=localhost:9092 --messages=100000 --batch=100

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "mini-kafka Makefile targets:"
	@echo ""
	@echo "  build        Build the broker binary to bin/broker"
	@echo "  test         Run all tests"
	@echo "  test-race    Run tests with race detector"
	@echo "  test-short   Run short tests only"
	@echo "  vet          Run go vet"
	@echo "  docker-build Build the Docker image"
	@echo "  up           Build image and start 3-broker cluster"
	@echo "  down         Stop the cluster"
	@echo "  logs         Tail cluster logs"
	@echo "  clean        Remove build artifacts and Docker volumes"
	@echo "  demo         Start single-broker and run smoke test (no Docker)"
	@echo "  bench        Throughput benchmark against localhost:9092"
	@echo ""
