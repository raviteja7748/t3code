#!/usr/bin/env bash
# sync-t3-local.sh — local helper to mirror the GitHub automation.
# Fetches pingdotgg/t3code:main, updates fork's main, rebases pi-nightly, runs checks.
# Usage:
#   bash scripts/sync-t3-local.sh           # full rebase + vp check/typecheck/test
#   bash scripts/sync-t3-local.sh --no-test  # only rebase, skip heavy checks
set -euo pipefail

SKIP_TESTS=false
if [[ "${1:-}" == "--no-test" ]]; then SKIP_TESTS=true; fi

git_config_once() {
  git config --global rerere.enabled true 2>/dev/null || true
  git config --global rerere.autoupdate true 2>/dev/null || true
  git config rerere.enabled true 2>/dev/null || true
  git config rerere.autoupdate true 2>/dev/null || true
}
git_config_once

if ! git remote get-url upstream >/dev/null 2>&1; then
  echo "Adding upstream -> https://github.com/pingdotgg/t3code.git"
  git remote add upstream https://github.com/pingdotgg/t3code.git
fi

echo "→ fetching upstream/main and origin/main"
git fetch upstream main --prune
git fetch origin main --prune 2>/dev/null || true

# Keep fork main tracking upstream
upstream_main=$(git rev-parse upstream/main)
origin_main=$(git ls-remote origin main 2>/dev/null | awk '{print $1}' || echo "")
if [ -z "$origin_main" ]; then
  echo "→ origin/main missing, creating from upstream/main"
  git push origin upstream/main:main || true
elif [ "$origin_main" != "$upstream_main" ]; then
  echo "→ syncing origin/main $origin_main -> $upstream_main"
  git push --force origin upstream/main:main || echo "push main failed (check permissions)"
else
  echo "→ origin/main already up to date"
fi

current_branch=$(git branch --show-current)
echo "→ rebasing pi-nightly onto upstream/main (from $current_branch)"

git fetch origin pi-nightly --prune 2>/dev/null || true
if git show-ref --verify --quiet refs/heads/pi-nightly; then
  git checkout pi-nightly
else
  git checkout -b pi-nightly origin/pi-nightly 2>/dev/null || git checkout -b pi-nightly upstream/main
fi

before=$(git rev-parse HEAD)
echo "Before: $before"

if ! git rebase upstream/main; then
  echo ""
  echo "✗ Rebase hit conflicts. Resolve, then:"
  echo "  git add -A && git rebase --continue"
  echo "  vp check && vp run --filter t3 --filter @t3tools/web typecheck && vp run --filter t3 test"
  echo "  git push --force-with-lease origin pi-nightly"
  echo ""
  echo "Rerere is enabled — next similar conflict will auto-resolve."
  exit 1
fi

after=$(git rev-parse HEAD)
if [ "$before" = "$after" ]; then
  echo "No new upstream commits — pi-nightly already up to date ($after)"
else
  echo "Rebased pi-nightly $before -> $after"
fi

if [ "$SKIP_TESTS" = true ]; then
  echo "→ --no-test: skipping vp checks (push with: git push --force-with-lease origin pi-nightly)"
  exit 0
fi

VP="vp"
if ! command -v vp >/dev/null 2>&1; then
  if [ -x "./node_modules/.bin/vp" ]; then VP="./node_modules/.bin/vp"
  elif command -v pnpm >/dev/null 2>&1; then VP="pnpm exec vp"
  elif command -v npx >/dev/null 2>&1; then VP="npx vp"
  fi
fi
echo "→ running vp check (via $VP)"
$VP check
echo "→ running typecheck"
$VP run --filter t3 --filter @t3tools/web typecheck
echo "→ running tests"
$VP run --filter t3 test

echo ""
echo "✓ Local sync green — push with:"
echo "  git push --force-with-lease origin pi-nightly"
if [ "$current_branch" != "pi-nightly" ] && [ -n "$current_branch" ]; then
  echo "  git checkout $current_branch"
fi
