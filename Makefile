# Development entry points for the cordis-go kernel.
#
# The kernel must remain standard-library-only (enforced by internal/archtest),
# so every target here relies on nothing beyond the Go toolchain.

GO ?= go
GOFMT ?= gofmt

# Every Go source in the module except the Rust reference tree and VCS data.
GO_FILES := $(shell find . -type f -name '*.go' -not -path './.git/*' -not -path './reference/*' | sort)

.PHONY: all build check fmt vet test race cover ci clean

all: check build test

# Compile every package.
build:
	$(GO) build ./...

# Formatting and static analysis gate.
check:
	@out="$$($(GOFMT) -l $(GO_FILES))" || exit $$?; \
	if [ -n "$$out" ]; then \
		echo "gofmt required for:"; \
		echo "$$out"; \
		echo "run 'make fmt' to format"; \
		exit 1; \
	fi
	$(GO) vet ./...

# Format the kernel root and all Go helper trees.
fmt:
	$(GOFMT) -w $(GO_FILES)

vet:
	$(GO) vet ./...

# Run the full test suite without relying on cached results.
test:
	$(GO) test ./... -count=1

# Race-detector run for concurrency-heavy kernel tests.
race:
	$(GO) test -race ./... -timeout 10m

cover:
	$(GO) test -cover ./...

# CI gate: static checks, build, then tests (in that order).
ci: check build test

clean:
	rm -rf dist
	rm -f *.test *.out
