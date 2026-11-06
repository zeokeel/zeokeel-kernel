#!/usr/bin/env bash
# CI entry point for cordis-go. Mirrors the local Makefile gates so a green
# `scripts/ci.sh` run means the same thing locally and in GitHub Actions.
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$ROOT"

section() {
	printf '\n==> %s\n' "$1"
}

section "gofmt -l (format check)"
mapfile -t go_files < <(find . -type f -name '*.go' -not -path './.git/*' -not -path './reference/*' | sort)
if ((${#go_files[@]} == 0)); then
	printf 'error: no Go source files found\n' >&2
	exit 1
fi
unformatted="$(gofmt -l "${go_files[@]}")"
if [[ -n "$unformatted" ]]; then
	printf 'error: the following files need gofmt:\n%s\n' "$unformatted" >&2
	exit 1
fi

section "go vet ./..."
go vet ./...

section "go build ./..."
go build ./...

section "go test ./... -count=1"
go test ./... -count=1

section "go test -race ./... -timeout 10m"
go test -race ./... -timeout 10m

section "CI passed"
