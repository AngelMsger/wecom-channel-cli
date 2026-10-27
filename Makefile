GO ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS = -s -w -X github.com/angelmsger/wecom-channel-cli/pkg/constants.Version=$(VERSION) -X github.com/angelmsger/wecom-channel-cli/pkg/constants.Commit=$(COMMIT) -X github.com/angelmsger/wecom-channel-cli/pkg/constants.BuildTime=$(BUILD_TIME)

.PHONY: build lint test e2e docs docs-check cross
build:
	mkdir -p bin
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/wecom-channel-cli ./cmd/wecom-channel-cli

lint:
	test -z "$$($(GO) fmt ./...)"
	$(GO) vet ./...

test:
	$(GO) test ./...

e2e: build
	WECOM_CHANNEL_TEST_BINARY="$(CURDIR)/bin/wecom-channel-cli" $(GO) test ./internal/app -run TestCLIProcess -count=1

docs:
	$(GO) run ./cmd/gen-docs

docs-check:
	$(GO) run ./cmd/gen-docs --check

cross:
	VERSION=$(VERSION) COMMIT=$(COMMIT) ./scripts/build.sh

.PHONY: site-check release-tests isolation-check

release-tests:
	python3 -m unittest discover -s scripts -p 'test_*.py'

isolation-check:
	python3 scripts/run-isolation-check.py

site-check:
	python3 scripts/check-site.py

.PHONY: vuln
vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
