#!/usr/bin/env bash
# Configure the GitHub repository for Zeokeel Kernel.
#
# Prerequisites:
#   - gh CLI installed and authenticated as a repository admin (gh auth login);
#   - the repository exists, `main` has been pushed, and the CI workflow has run
#     at least once so the required check contexts exist;
#   - run from the repository root (`./scripts/setup-github.sh`).
#
# Usage:
#   REPO=zeokeel/zeokeel-kernel ./scripts/setup-github.sh
#   MIN_APPROVALS=1 ./scripts/setup-github.sh   # require one human approval
#
# What it configures:
#   - default branch = main;
#   - squash-only merges, auto-delete merged branches, always update branches;
#   - branch protection on main: PR-only, required status checks, linear history,
#     no force pushes/deletions, require resolved conversations, enforce for admins;
#   - vulnerability alerts / automated security fixes where available.
set -euo pipefail

REPO="${REPO:-zeokeel/zeokeel-kernel}"
BRANCH="${BRANCH:-main}"
MIN_APPROVALS="${MIN_APPROVALS:-0}"
REQUIRED_CHECKS="${REQUIRED_CHECKS:-lint test race coverage cross govulncheck}"

if ! command -v gh >/dev/null 2>&1; then
  echo "error: gh CLI is required (https://cli.github.com/)" >&2
  exit 1
fi

if ! [[ "$MIN_APPROVALS" =~ ^[0-6]$ ]]; then
  echo "error: MIN_APPROVALS must be an integer between 0 and 6 (got: $MIN_APPROVALS)" >&2
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "error: gh is not authenticated; run 'gh auth login'" >&2
  exit 1
fi

echo "==> configuring repository settings for $REPO"
gh api -X PATCH "repos/$REPO" \
  -f default_branch="$BRANCH" \
  -F allow_squash_merge=true \
  -F allow_merge_commit=false \
  -F allow_rebase_merge=false \
  -F delete_branch_on_merge=true \
  -F allow_update_branch=true \
  >/dev/null

echo "==> configuring branch protection for $REPO:$BRANCH"
checks_json=""
for check in $REQUIRED_CHECKS; do
  if [[ -n "$checks_json" ]]; then checks_json+=", "; fi
  checks_json+="\"$check\""
done

payload="$(cat <<JSON
{
  "required_status_checks": {
    "strict": true,
    "contexts": [${checks_json}]
  },
  "enforce_admins": true,
  "required_pull_request_reviews": {
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": false,
    "required_approving_review_count": ${MIN_APPROVALS}
  },
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "required_conversation_resolution": true
}
JSON
)"

printf '%s' "$payload" | gh api -X PUT "repos/$REPO/branches/$BRANCH/protection" --input - >/dev/null

echo "==> enabling vulnerability alerts / automated security fixes (best effort)"
gh api -X PUT "repos/$REPO/vulnerability-alerts" >/dev/null 2>&1 || true
gh api -X PUT "repos/$REPO/automated-security-fixes" >/dev/null 2>&1 || true

echo
echo "done:"
echo "  - main requires a pull request (direct pushes blocked, admins enforced)"
echo "  - required checks: ${REQUIRED_CHECKS}"
echo "  - required approvals: ${MIN_APPROVALS} (set MIN_APPROVALS=1+ when a second maintainer exists)"
echo "  - squash-only merges, linear history, conversations resolved"
echo
echo "Verify with: gh api repos/$REPO/branches/$BRANCH/protection"
