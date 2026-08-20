#!/usr/bin/env bash
# update-t3-pi.sh — pull the already-tested pi-nightly release and restart T3.
# Separates integration (GitHub rebase+test) from deployment (this script).
# Usage:
#   bash scripts/update-t3-pi.sh                  # update this machine
#   bash scripts/update-t3-pi.sh --check          # only check, don't install
#   bash scripts/update-t3-pi.sh --host buggy     # ssh to host and update there
#   BUGGY_HOST=buggy T3_PI_REPO=raviteja7748/t3code bash scripts/update-t3-pi.sh --host buggy
set -euo pipefail

REPO="${T3_PI_REPO:-raviteja7748/t3code}"
HOST="${BUGGY_HOST:-}"
CHECK_ONLY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) CHECK_ONLY=true; shift ;;
    --host) HOST="$2"; shift 2 ;;
    --repo) REPO="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

run_on_host() {
  if [ -n "$HOST" ]; then
    # Uses ssh; assumes Host entry in ~/.ssh/config (e.g., Host buggy)
    ssh "$HOST" "$@"
  else
    bash -c "$*"
  fi
}

if [ "$CHECK_ONLY" = true ]; then
  if [ -n "$HOST" ]; then
    run_on_host "gh api repos/$REPO/releases?per_page=10 --jq '[.[] | select(.prerelease and (.tag_name|startswith(\"pi-v\")))] | first | .tag_name' 2>/dev/null || echo 'no release'"
    # also show local tag
    run_on_host 'cat ~/.local/share/t3-pi/.installed-tag 2>/dev/null || cat /Applications/T3\ Code\ Pi\ Nightly.app/Contents/Resources/app.asar 2>/dev/null | head -c 0; echo "(check .installed-tag)"'
  else
    gh api "repos/$REPO/releases?per_page=10" --jq '[.[] | select(.prerelease and (.tag_name|startswith("pi-v")))] | first | .tag_name'
  fi
  exit 0
fi

if [ -n "$HOST" ]; then
  echo "→ updating $HOST via ssh ($REPO)"
  # Copy installer via ssh execution (no file copy needed — remote has its own scripts)
  ssh "$HOST" "bash -c 'curl -fsSL https://raw.githubusercontent.com/$REPO/pi-nightly/scripts/install-pi-nightly.sh | bash' " \
  || ssh "$HOST" "T3_PI_REPO=$REPO bash \$(find ~ -name install-pi-nightly.sh 2>/dev/null | head -1)" \
  || {
    echo "Remote install failed — falling back to git pull on host"
    ssh "$HOST" "bash -lc 'cd ~/t3code 2>/dev/null || cd ~/Experiments/t3code-pi 2>/dev/null || exit 0; git fetch origin pi-nightly && git checkout pi-nightly && git reset --hard origin/pi-nightly && echo \"pulled pi-nightly\"'"
  }
  echo "✓ $HOST updated (check with: ssh $HOST 'cat ~/.local/share/t3-pi/.installed-tag')"
else
  echo "→ updating this machine ($REPO)"
  bash scripts/install-pi-nightly.sh
  echo "✓ updated. Restart T3 Code Pi Nightly if needed."
fi
