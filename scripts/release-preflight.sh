#!/usr/bin/env bash
# Release pre-flight for the zeokeel-kernel Go module.
#
# Usage: scripts/release-preflight.sh
#
# Runs the local gates a tagged Go module release should satisfy (format, vet,
# build, tests, race, coverage, module hygiene, cross-compilation, API docs) and
# checks the release metadata that cannot be validated locally without a
# published repository (remote URL vs module path, release tag).
#
# Exit status: 0 when all gates pass; 1 when any gate fails (including the
# "publishing metadata" blockers, which is accurate for a release pre-flight);
# 2 when the toolchain is missing.
set -uo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$ROOT"

GO="${GO:-go}"
GOFMT="${GOFMT:-gofmt}"
TMPDIR_PREFLIGHT="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_PREFLIGHT"' EXIT

PASS=0
WARN=0
FAIL=0

section() { printf '\n==> %s\n' "$1"; }
ok()   { PASS=$((PASS + 1)); printf 'PASS  %s\n' "$1"; }
warn() { WARN=$((WARN + 1)); printf 'WARN  %s\n' "$1"; }
bad()  { FAIL=$((FAIL + 1)); printf 'FAIL  %s\n' "$1"; }

run_check() {
	local name="$1"
	shift
	if "$@" >"$TMPDIR_PREFLIGHT/step.log" 2>&1; then
		ok "$name"
	else
		bad "$name"
		tail -20 "$TMPDIR_PREFLIGHT/step.log" | sed 's/^/      /'
	fi
}

section "toolchain"
if ! command -v "$GO" >/dev/null 2>&1; then
	printf 'fail: go toolchain not found (set GO=/path/to/go)\n' >&2
	exit 2
fi
GO_VERSION="$("$GO" version)"
printf 'go: %s\n' "$GO_VERSION"
MODULE_PATH="$("$GO" list -m)"
GO_DIRECTIVE="$(awk '/^go /{print $2; exit}' go.mod)"
printf 'module: %s\n' "$MODULE_PATH"
printf 'go.mod go directive: %s\n' "$GO_DIRECTIVE"

section "worktree"
if command -v git >/dev/null 2>&1; then
	if [[ -n "$(git status --porcelain)" ]]; then
		warn "git worktree is dirty (release should be built from a clean commit)"
	else
		ok "git worktree is clean"
	fi
else
	warn "git not available; skipping worktree/release metadata checks"
fi

section "source hygiene"
mapfile -t go_files < <(find . -type f -name '*.go' -not -path './.git/*' | sort)
unformatted="$("$GOFMT" -l "${go_files[@]}" 2>/dev/null || true)"
if [[ -z "$unformatted" ]]; then ok "gofmt: clean"; else bad "gofmt: unformatted files:"; printf '%s\n' "$unformatted" | sed 's/^/      /'; fi
run_check "go vet ./..." "$GO" vet ./...
run_check "go build ./..." "$GO" build ./...

section "module hygiene"
run_check "go mod verify" "$GO" mod verify
if tidy_diff="$("$GO" mod tidy -diff 2>&1)" && [[ -z "$tidy_diff" ]]; then
	ok "go mod tidy: no diff"
else
	bad "go mod tidy: go.mod/go.sum not tidy"
	printf '%s\n' "$tidy_diff" | head -20 | sed 's/^/      /'
fi
if grep -Eq '^[[:space:]]*(replace|exclude|retract)' go.mod; then
	bad "go.mod contains replace/exclude/retract directives (not release-safe)"
else
	ok "go.mod has no replace/exclude/retract directives"
fi
external="$("$GO" list -m all 2>/dev/null | tail -n +2 || true)"
if [[ -z "$external" ]]; then
	ok "zero external module dependencies (stdlib only)"
else
	bad "external module dependencies present:"
	printf '%s\n' "$external" | sed 's/^/      /'
fi

section "tests"
run_check "go test ./... -count=1" "$GO" test ./... -count=1
run_check "go test -race ./... -count=1" "$GO" test -race ./... -count=1
if "$GO" test -count=1 ./... -coverpkg="$MODULE_PATH,$MODULE_PATH/module" -coverprofile="$TMPDIR_PREFLIGHT/cover.out" >"$TMPDIR_PREFLIGHT/cover.log" 2>&1; then
	COVERAGE="$("$GO" tool cover -func="$TMPDIR_PREFLIGHT/cover.out" | awk '/^total:/{gsub(/%/,"",$3); print $3}')"
	if [[ -n "$COVERAGE" ]] && awk -v c="$COVERAGE" 'BEGIN{exit !(c+0 >= 75)}'; then
		ok "aggregate coverage ${COVERAGE}% (threshold 75%)"
	else
		warn "aggregate coverage ${COVERAGE:-unknown}% is below the 75% release threshold"
	fi
else
	bad "coverage run failed"
	tail -20 "$TMPDIR_PREFLIGHT/cover.log" | sed 's/^/      /'
fi

section "cross-compilation"
for target in "linux amd64" "linux arm64" "darwin amd64" "darwin arm64" "windows amd64"; do
	set -- $target
	if GOOS="$1" GOARCH="$2" "$GO" build ./... >"$TMPDIR_PREFLIGHT/cross.log" 2>&1; then
		ok "GOOS=$1 GOARCH=$2 go build ./..."
	else
		bad "GOOS=$1 GOARCH=$2 go build ./..."
		tail -20 "$TMPDIR_PREFLIGHT/cross.log" | sed 's/^/      /'
	fi
done

section "examples (runtime smoke)"
for example in examples/hello examples/services examples/events examples/async_plugin; do
	if command -v timeout >/dev/null 2>&1 && timeout 20 "$GO" run "./$example" >"$TMPDIR_PREFLIGHT/example.log" 2>&1; then
		ok "go run ./$example"
	elif "$GO" run "./$example" >"$TMPDIR_PREFLIGHT/example.log" 2>&1; then
		ok "go run ./$example"
	else
		bad "go run ./$example"
		tail -10 "$TMPDIR_PREFLIGHT/example.log" | sed 's/^/      /'
	fi
done

section "public API documentation"
if "$GO" run scripts/doccheck.go >"$TMPDIR_PREFLIGHT/doccheck.log" 2>&1; then
	ok "all exported symbols in cordis/module have doc comments"
else
	bad "exported symbols without doc comments"
	tail -30 "$TMPDIR_PREFLIGHT/doccheck.log" | sed 's/^/      /'
fi

section "release metadata"
for file in README.md LICENSE go.mod docs/COMPILE-TIME-REGISTRY.md .github/workflows/ci.yml; do
	if [[ -f "$file" ]]; then ok "present: $file"; else bad "missing: $file"; fi
done
if command -v git >/dev/null 2>&1; then
	remote="$(git remote get-url origin 2>/dev/null || true)"
	if [[ -z "$remote" ]]; then
		bad "no git remote 'origin': the module path ($MODULE_PATH) is not published, so 'go get' cannot resolve it"
	else
		case "$remote" in
		*"${MODULE_PATH#github.com/}"*)
			ok "git remote matches module path: $remote"
			if timeout 10 git ls-remote --exit-code origin HEAD >/dev/null 2>&1; then
				ok "git remote is reachable"
			else
				warn "git remote is not reachable yet (repository not created/pushed, or network blocked)"
			fi
			;;
		*) warn "git remote ($remote) does not obviously match module path ($MODULE_PATH)" ;;
		esac
	fi
	latest_tag="$(git tag -l 'v*' | sort -V | tail -n 1 || true)"
	if [[ -z "$latest_tag" ]]; then
		warn "no semantic-version git tag yet (expected for the first release, e.g. v0.1.0)"
	else
		ok "latest release tag: $latest_tag"
	fi
fi

section "summary"
printf 'passed=%d warnings=%d failed=%d\n' "$PASS" "$WARN" "$FAIL"
if ((FAIL > 0)); then
	printf 'release pre-flight: NOT READY (resolve the FAIL items above)\n'
	exit 1
fi
printf 'release pre-flight: READY (warnings are non-blocking advisories)\n'
