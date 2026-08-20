# T3 Code + Pi nightly — automated rebased patch

This branch keeps one direct **Pi RPC integration** on top of official `pingdotgg/t3code`.
No other fork or adapter is used. Official T3 is the base; Pi is a rebased patch.

```
              pingdotgg/t3code:main
                      │
                every 3h / manual
                      ▼
           ┌──────────────────────┐
           │  Pi Nightly workflow │
           │  .github/workflows/  │
           │  pi-nightly.yml      │
           │                      │
           │  1. sync fork main   │
           │     → upstream/main  │
           │  2. rebase pi-nightly│
           │     → upstream/main  │
           │  3. vp check         │
           │     typecheck / test │
           └──────────┬───────────┘
                      │
                ┌─────┴─────┐
                ▼           ▼
              PASS        FAIL / conflict
               │               │
               ▼               ▼
        push pi-nightly   open GitHub issue
        build DMG/AppImage  "Pi rebase conflict"
        publish prerelease  "Pi patch broken"
```

## Branch model

```
raviteja7748/t3code

main        → mirrors pingdotgg/t3code:main (updated by workflow)
pi-nightly  → main + Pi integration (rebase, never merge)
```

- `main` is always clean upstream — no Pi commits. The workflow force-pushes `upstream/main` to `origin/main`.
- `pi-nightly` is the integration branch you run. It is **rebased**, never merged, so the Pi patch stays a small stack on top.

Use two local branches if you want stable vs next:

```
upstream/main → pi-nightly (next, auto) → pi-stable (manual promote, runs on buggy)
```

Promotion is one command (see Deployment).

## Workflow — `pi-nightly.yml` (GitHub free APIs only)

- **Schedule:** `20 */3 * * *` and `workflow_dispatch` (`force`).
- **Permissions:** `contents: write`, `issues: write`, `pull-requests: write` — free.
- **Steps:**
  1. `checkout pi-nightly`, `rerere.enabled=true` (remembers conflict resolutions).
  2. Sync `origin/main` to `upstream/main`.
  3. `git rebase upstream/main` on `pi-nightly`. On conflict → abort, create issue `Pi rebase conflict — manual fix needed`, no push.
  4. `vp check` / `typecheck` / `vp test`. On failure → create issue `Pi patch broken after upstream rebase`.
  5. Only on PASS → `git push --force-with-lease origin pi-nightly`, then build DMG/AppImage/server and publish `pi-v*` prerelease.

> The workflow never force-pushes broken code. Human attention happens only when upstream actually breaks the Pi patch.

## Local helpers

Enable `rerere` once:

```bash
git config --global rerere.enabled true
git config --global rerere.autoupdate true
```

Sync locally (mirrors CI):

```bash
bash scripts/sync-t3-local.sh           # rebase + vp check/typecheck/test
bash scripts/sync-t3-local.sh --no-test # only rebase
```

If the workflow reported a conflict:

```bash
git fetch upstream main
git switch pi-nightly
git rebase upstream/main
# resolve, then
git add -A && git rebase --continue
vp check && vp run --filter t3 --filter @t3tools/web typecheck && vp run --filter t3 test
git push --force-with-lease origin pi-nightly
# close the GitHub issue — next schedule will verify
```

## Deployment — separate from integration

Integration (rebase+test) is automatic. Deployment to a machine is explicit.

Update this machine:

```bash
bash scripts/install-pi-nightly.sh          # pulls latest pi-v* release, restarts service
```

Check or update `buggy` (your linux box, via `~/.ssh/config` Host `buggy`):

```bash
bash scripts/update-t3-pi.sh --check
bash scripts/update-t3-pi.sh --host buggy
# or
BUGGY_HOST=buggy T3_PI_REPO=raviteja7748/t3code bash scripts/update-t3-pi.sh --host buggy
```

On Linux the installer writes `~/.local/bin/t3-pi` and `~/.local/share/t3-pi/.installed-tag` and does `systemctl --user try-restart t3-pi.service`.

For a **stable** host, point it at a `pi-stable` branch/tag you promote manually after `pi-nightly` is green:

```bash
git checkout pi-stable
git reset --hard origin/pi-nightly
git push --force origin pi-stable
# then on buggy: git pull stable or install release from pi-stable
```

## Install

```bash
bash scripts/install-pi-nightly.sh
```

- macOS: installs `/Applications/T3 Code Pi Nightly.app` (Apple Silicon, unsigned) alongside the official app.
- Linux: installs `~/.local/bin/t3-pi`.

## When upstream ships native Pi

Delete the patch and the workflow:

```
pingdotgg/t3code (native Pi)
        │
   delete pi-nightly
   delete pi-nightly.yml
   delete scripts/install-pi-nightly.sh
```

Then return to stock `upstream/main` updates.
