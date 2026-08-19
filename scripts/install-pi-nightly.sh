#!/usr/bin/env bash
set -euo pipefail

repo=${T3_PI_REPO:-raviteja7748/t3code}
command -v gh >/dev/null || { echo "gh is required" >&2; exit 1; }
tag=$(gh api "repos/$repo/releases?per_page=30" --jq '[.[] | select(.prerelease and (.tag_name | startswith("pi-v")))] | first | .tag_name')
if [[ -z "$tag" ]]; then
  echo "No Pi nightly release found in $repo; leaving the current install unchanged." >&2
  exit 0
fi
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Installing $tag"
case "$(uname -s)" in
  Darwin)
    [[ $(uname -m) == arm64 ]] || { echo "Only Apple Silicon is configured" >&2; exit 1; }
    gh release download "$tag" -R "$repo" -p '*-arm64.dmg' -D "$tmp"
    dmg=$(find "$tmp" -name '*.dmg' -print -quit)
    mount=$(hdiutil attach -nobrowse "$dmg" | sed -n $'s/^.*\t\(\/Volumes\/.*\)$/\1/p' | tail -1)
    [[ -n "$mount" ]] || { echo "Could not mount $dmg" >&2; exit 1; }
    trap 'hdiutil detach "$mount" -quiet 2>/dev/null || true; rm -rf "$tmp"' EXIT
    app=$(find "$mount" -maxdepth 1 -name '*.app' -print -quit)
    osascript -e 'tell application "T3 Code (Nightly)" to quit' 2>/dev/null || true
    rm -rf '/Applications/T3 Code Pi Nightly.app'
    ditto "$app" '/Applications/T3 Code Pi Nightly.app'
    xattr -dr com.apple.quarantine '/Applications/T3 Code Pi Nightly.app'
    echo "Installed /Applications/T3 Code Pi Nightly.app"
    ;;
  Linux)
    install_root="$HOME/.local/share/t3-pi"
    if [[ -f "$install_root/.installed-tag" && $(<"$install_root/.installed-tag") == "$tag" ]]; then
      echo "$tag is already installed"
      exit 0
    fi
    gh release download "$tag" -R "$repo" -p 't3-pi-server.tar.gz' -D "$tmp"
    mkdir -p "$HOME/.local/bin" "$tmp/unpack"
    tar -xzf "$tmp/t3-pi-server.tar.gz" -C "$tmp/unpack"
    rm -rf "$install_root.next"
    mv "$tmp/unpack/t3-pi-server" "$install_root.next"
    rm -rf "$install_root"
    mv "$install_root.next" "$install_root"
    cat > "$HOME/.local/bin/t3-pi" <<'EOF'
#!/usr/bin/env bash
exec node "$HOME/.local/share/t3-pi/dist/bin.mjs" "$@"
EOF
    chmod +x "$HOME/.local/bin/t3-pi"
    printf '%s\n' "$tag" > "$install_root/.installed-tag"
    systemctl --user try-restart t3-pi.service 2>/dev/null || true
    echo "Installed $HOME/.local/bin/t3-pi"
    ;;
  *)
    echo "Unsupported OS" >&2
    exit 1
    ;;
esac
