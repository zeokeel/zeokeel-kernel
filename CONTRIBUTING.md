# Contributing

Thanks for using Zeokeel Kernel. The project follows a **PR-only, protected-`main`** workflow.

## Requirements

- Go **1.27+** (the kernel uses Go 1.27 generic methods; `go.mod` declares `go 1.27`).
- No third-party dependencies: the kernel must remain standard-library only (`internal/archtest` enforces it).
- `gofmt`-clean code and exported API doc comments.

## Workflow

1. Fork the repository and create a topic branch from `main` (`feat/...`, `fix/...`, `docs/...`).
2. Make one logical change per PR; include tests for behavior changes.
3. Run the local gates before opening the PR:

   ```bash
   make ci          # gofmt + vet + build + test
   make race        # go test -race ./...
   make preflight   # full release-style preflight (optional locally; required before tags)
   ```

4. Open a PR against `main` and fill in the pull request template.
5. All required status checks must pass and review conversations must be resolved before merge.
   `main` is protected: direct pushes and force-pushes are disabled; merge commits are disabled
   (squash/rebase only, per repository setup script).

## Required CI checks

`lint`, `test`, `race`, `coverage`, `cross`, `govulncheck` — see
[`docs/REPOSITORY-SETUP.md`](docs/REPOSITORY-SETUP.md). Coverage must stay ≥ 75% aggregate.

## Design constraints for changes

- **Kernel boundary**: root package imports must stay stdlib-only and must not do I/O/network/os.
  Subpackages used by hosts (for example `module/`) may use standard-library I/O.
- **Effects/lifecycle**: new side effects must be registered through the kernel so they are reversible
  and owned by a fiber; do not add hidden global state.
- **Panic containment**: every kernel call into user/module code must recover; document any new
  uncontained boundary explicitly.
- **No in-process config hot reload**: configuration changes are applied by restarting the process; do
  not add `Update`/`Reload` hooks to the default module contract.
- **Docs**: update `README.md` and the relevant `docs/` page for user-facing behavior or contract changes.

## Commit messages

Prefer concise, conventional-style subjects (`feat:`, `fix:`, `docs:`, `refactor:`, `chore:`, `test:`).
Squash-merge keeps one logical commit per PR.

## Security

Please follow [`SECURITY.md`](SECURITY.md) instead of public issues for vulnerabilities.

## Release process (maintainers)

See [`docs/RELEASE-PREFLIGHT.md`](docs/RELEASE-PREFLIGHT.md) and [`docs/REPOSITORY-SETUP.md`](docs/REPOSITORY-SETUP.md).
Tags matching `v*` trigger the release workflow, which re-runs the strict CI gate and creates a GitHub
Release from generated notes.
