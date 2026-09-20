# Kafka Application Gateway — developer and CI entrypoints (TECH-SPEC §5.1).
# Every CI stage in TECH-SPEC §4.10 has a target here so it runs identically locally.

SHELL := sh

MODULE  := github.com/misterkafkagod/kafka3o
BINARY  := kafka3o-gateway
GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# Pinned tool versions (Renovate keeps them current; TECH-SPEC §1.1 / §1.3).
GOLANGCI_LINT_VERSION ?= v2.13.2
GOVULNCHECK_VERSION   ?= latest
BENCHSTAT_VERSION     ?= latest

# Size guard (TECH-SPEC S6) and fuzz budget per PR (TECH-SPEC T4).
MAX_FILE_LINES ?= 400
FUZZ_TIME      ?= 10s

# Coverage gate (TECH-SPEC T1): packages measured; franz, app, cmd are excluded.
COVER_MIN  ?= 90
COVER_PKGS := ./internal/config/... ./internal/command/... ./internal/kafka \
              ./internal/kafka/fake/... ./internal/scan/... ./internal/audit/... \
              ./internal/service/... ./internal/api/...
COVER_FULL := ./internal/service/gates ./internal/api/errors

# Static binaries (TECH-SPEC §1.1) — applied to build only; the race detector needs cgo.
RACE ?= -race
GOFLAGS_BUILD := -trimpath

# Tools run through `go run <module>@<version>`: pinned, cached after the first build,
# and free of GOPATH/GOBIN path handling that differs between Windows and Linux shells.
EXE           := $(if $(filter Windows_NT,$(OS)),.exe,)
GOLANGCI_LINT := $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
GOVULNCHECK   := $(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
BENCHSTAT     := $(GO) run golang.org/x/perf/cmd/benchstat@$(BENCHSTAT_VERSION)

.PHONY: all help tools lint lint-golangci lint-filelen lint-negative test cover fuzz bench \
        govulncheck build image openapi acceptance report ci clean

all: build

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-14s %s\n", $$1, $$2}'

# ---------------------------------------------------------------- tools

tools: ## pre-build the pinned dev tools into the Go build cache
	$(GOLANGCI_LINT) version
	$(GOVULNCHECK) -version
	$(BENCHSTAT) -h >/dev/null 2>&1 || true

# ---------------------------------------------------------------- lint

lint: lint-golangci lint-filelen ## golangci-lint (depguard, gosec, ...) + file-length guard

lint-golangci:
	$(GOLANGCI_LINT) run ./...

lint-filelen: ## TECH-SPEC S6: non-test Go files must be <= $(MAX_FILE_LINES) lines
	@find . -name '*.go' -not -path './.git/*' -not -name '*_test.go' -print0 \
	  | xargs -0 wc -l \
	  | awk -v max=$(MAX_FILE_LINES) '$$2 != "total" && $$1 > max { printf "lint-filelen: %s has %d lines (max %d)\n", $$2, $$1, max; bad = 1 } END { exit bad }'
	@echo "lint-filelen: OK"

lint-negative: ## prove depguard rejects a franz-go import inside internal/kafka (TECH-SPEC D1)
	@cp internal/kafka/depguard_probe_test.go.txt internal/kafka/zz_depguard_probe_test.go
	@if $(GOLANGCI_LINT) run ./internal/kafka/ >/dev/null 2>&1; then \
	  rm -f internal/kafka/zz_depguard_probe_test.go; \
	  echo "lint-negative: FAIL - probe import was not rejected"; exit 1; \
	else \
	  rm -f internal/kafka/zz_depguard_probe_test.go; \
	  echo "lint-negative: OK - probe import rejected"; \
	fi

# ---------------------------------------------------------------- test

test: ## unit + component + contract tests with the race detector
	$(GO) test $(RACE) -covermode=atomic -coverpkg=$(COVER_PKG_LIST) -coverprofile=coverage.out ./...

empty :=
space := $(empty) $(empty)
comma := ,
COVER_PKG_LIST := $(subst $(space),$(comma),$(strip $(COVER_PKGS)))

cover: test ## enforce TECH-SPEC T1: >= $(COVER_MIN)% overall, 100% on gates and error tables
	@$(GO) tool cover -func=coverage.out | awk -v min=$(COVER_MIN) '/^total:/ { gsub("%", "", $$3); if ($$3+0 < min) { printf "cover: %.1f%% < %d%%\n", $$3, min; exit 1 } printf "cover: %.1f%% >= %d%%\n", $$3, min }'
	@for p in $(COVER_FULL); do \
	  if [ -d "$$p" ]; then \
	    pct=$$($(GO) test -cover $$p 2>/dev/null | awk '/coverage:/ { gsub("%", "", $$(NF-2)); print $$(NF-2) }'); \
	    if [ "$$pct" != "100.0" ]; then echo "cover: $$p at $${pct:-0}% (must be 100%)"; exit 1; fi; \
	    echo "cover: $$p at 100%"; \
	  fi; \
	done

fuzz: ## run every Fuzz* target for $(FUZZ_TIME) (TECH-SPEC §4.6)
	@for f in $$(grep -rls --include='*_test.go' '^func Fuzz' ./internal ./cmd ./tools 2>/dev/null); do \
	  pkg=./$$(dirname $$f); \
	  for fn in $$(grep -o '^func Fuzz[A-Za-z0-9_]*' $$f | sed 's/^func //'); do \
	    echo "fuzz: $$pkg $$fn"; \
	    $(GO) test -run='^$$' -fuzz="^$$fn$$" -fuzztime=$(FUZZ_TIME) $$pkg || exit 1; \
	  done; \
	done; echo "fuzz: done"

bench: ## run benchmarks; output in bench.txt for benchstat (TECH-SPEC §4.7, T3)
	$(GO) test -run='^$$' -bench=. -benchmem ./... | tee bench.txt

govulncheck: ## TECH-SPEC §1.3 CVE policy
	$(GOVULNCHECK) ./...

# ---------------------------------------------------------------- build

build: ## static binary in bin/
	CGO_ENABLED=0 $(GO) build $(GOFLAGS_BUILD) -ldflags="-s -w -X main.version=$(VERSION)" -o bin/$(BINARY)$(EXE) ./cmd/gateway

image: ## container image (Dockerfile arrives in Phase 14)
	docker build --build-arg VERSION=$(VERSION) -t $(BINARY):$(VERSION) .

openapi: ## export docs/api/openapi.json from the tested golden (Task 13.2)
	@test -f internal/api/testdata/openapi.golden.json || { echo "openapi: golden not generated yet (Tasks 1.8 / 13.2)"; exit 1; }
	@mkdir -p docs/api && cp internal/api/testdata/openapi.golden.json docs/api/openapi.json && echo "openapi: docs/api/openapi.json updated"

# ---------------------------------------------------------------- acceptance (Level 2)

acceptance: ## run the acceptance + integration suites against KAFKA_BOOTSTRAP (Phase 16)
	$(GO) test -json -tags acceptance,integration ./test/... ./internal/kafka/franz/... > acceptance.json

report: ## render docs/acceptance/<version>-<date>.md from acceptance.json (Phase 16)
	$(GO) run ./tools/acceptance-report < acceptance.json

# ---------------------------------------------------------------- CI

ci: lint cover fuzz govulncheck build ## everything the PR pipeline runs (TECH-SPEC §4.10)

clean:
	rm -rf bin/ coverage.out bench.txt acceptance.json
