GO ?= go
VERSION ?= 0.1.0
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo uncommitted)
BUILD_TIME ?= $(shell git show -s --format=%cI HEAD 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)
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
	mkdir -p dist
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o dist/wecom-channel-cli-linux-amd64 ./cmd/wecom-channel-cli
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o dist/wecom-channel-cli-linux-arm64 ./cmd/wecom-channel-cli
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o dist/wecom-channel-cli-darwin-arm64 ./cmd/wecom-channel-cli
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o dist/wecom-channel-cli-darwin-amd64 ./cmd/wecom-channel-cli

.PHONY: package release-check site-check release-tests isolation-check
package: cross
	cd build/npm && npm pack --ignore-scripts --pack-destination ../../dist
	python3 scripts/package.py

release-tests:
	python3 -m unittest discover -s scripts -p 'test_*.py'

isolation-check:
	python3 scripts/run-isolation-check.py

release-check:
	python3 scripts/release-check.py

site-check:
	python3 scripts/check-site.py

.PHONY: vuln
vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
