# T3 Code + Pi nightly

This branch keeps one direct Pi RPC integration on top of official `pingdotgg/t3code`.
No other T3 Code fork or Pi adapter is used.

## Updates

`.github/workflows/pi-nightly.yml` runs every three hours:

1. fetches `pingdotgg/t3code:main`;
2. rebases `pi-nightly` onto it;
3. validates the server and web app;
4. builds an unsigned Apple Silicon DMG, Linux AppImage, and Linux server package;
5. publishes a prerelease in this repository.

Install or update either host with:

```bash
bash scripts/install-pi-nightly.sh
```

The macOS app is installed separately as `/Applications/T3 Code Pi Nightly.app` so the official app remains available as a fallback. The Linux server is installed as `~/.local/bin/t3-pi`.

## When an upstream update conflicts

The workflow stops before publishing a release. Open the failed **Pi Nightly** run, reproduce locally, resolve only the Pi commits, validate, then push:

```bash
git fetch upstream main
git switch pi-nightly
git rebase upstream/main
# resolve conflicts
git add -A
git rebase --continue
vp check
vp run --filter t3 --filter @t3tools/web typecheck
vp run --filter t3 test
git push --force-with-lease origin pi-nightly
```

Do not merge upstream into this branch: keeping Pi changes as a small rebased commit stack makes nightly conflicts visible and removable.

## Exit when Pi becomes official

Once `pingdotgg/t3code` ships working Pi support, archive this fork and install the official nightly. Do not keep two Pi implementations.
