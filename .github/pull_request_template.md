## Summary

<!-- What changed and why? Link the issue: "Closes #123". -->

## Checklist

- [ ] `make ci` passes locally (`gofmt`/`go vet`/`go build`/`go test`)
- [ ] `make race` passes locally
- [ ] `make preflight` passes locally (no new FAIL items)
- [ ] New/changed behavior has tests (kernel or conformance/module suites)
- [ ] Exported API has doc comments (`go run scripts/doccheck.go`)
- [ ] No new third-party dependencies (the kernel is standard-library only)
- [ ] Docs/README updated if user-facing behavior or the module contract changed
- [ ] The change is limited to one logical area (split unrelated changes into separate PRs)

## Notes for reviewers

<!-- Risk, migration, follow-up work, or intentionally skipped checks. -->
