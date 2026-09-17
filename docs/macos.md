# macOS hybrid setup (macFUSE + mega2 compose)

ScorpioFS on macOS is a **hybrid** deployment: the mega2 backend runs in Docker,
and the `scorpio` daemon mounts FUSE on the host through **macFUSE**.

FUSE-T is not supported. `asyncfuse` only looks for
`/Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse`.

Do **not** start ScorpioFS inside Docker Desktop, and do **not** use mega2's
`docker-compose.test.yml` `--profile scorpio` path (that stack does container
FUSE and publishes `127.0.0.1:12725`).

Do **not** run `install.sh` on macOS. That script is Linux-only (systemd +
`user_allow_other`). On Darwin it refuses to run so it cannot download a Linux
tarball by mistake.

Apple Silicon release binaries are published as
`scorpiofs-<ver>-aarch64-apple-darwin.tar.gz`. There is no Intel Mac artifact;
on `x86_64-apple-darwin` build from source with `cargo build --release`.

These binaries are **not notarized**. Gatekeeper may block the first launch:
Control-click → Open, or `xattr -d com.apple.quarantine scorpio antares`.

## 1. Install macFUSE

1. Install [macFUSE](https://macfuse.io).
2. On Apple Silicon, allow third-party kernel extensions: reboot into Recovery,
   set **Reduced Security**, and enable kernel extensions. Then approve the
   macFUSE system extension in System Settings.
3. Confirm the mount helper exists:

```bash
ls /Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse
```

## 2. Start the mega2 eval backend

Use the sibling mega2 eval compose file. It already maps the API to
`127.0.0.1:9000` and does not include a FUSE service:

```bash
cd /path/to/mega2
docker compose -f mega2-compose.yml up -d --wait
curl -fsS http://127.0.0.1:9000/api/openapi.json
```

The first start initializes an empty trunk monorepo. That is enough for a
mount smoke test. An anonymous `git push` to `http://127.0.0.1:9000/` is
optional if you want a richer tree.

## 3. Download the Apple Silicon tarball

Pick the latest `v*` [GitHub Release](https://github.com/gitmono-dev/scorpiofs/releases)
and verify the checksum:

```bash
VER=v0.4.1   # or the tag you downloaded
curl -fL -O "https://github.com/gitmono-dev/scorpiofs/releases/download/${VER}/scorpiofs-${VER}-aarch64-apple-darwin.tar.gz"
curl -fL -O "https://github.com/gitmono-dev/scorpiofs/releases/download/${VER}/scorpiofs-${VER}-aarch64-apple-darwin.tar.gz.sha256"
shasum -a 256 -c "scorpiofs-${VER}-aarch64-apple-darwin.tar.gz.sha256"
tar -xzf "scorpiofs-${VER}-aarch64-apple-darwin.tar.gz"
cd "scorpiofs-${VER}-aarch64-apple-darwin"
```

The archive contains `scorpio`, `antares`, `LICENSE-MIT`, `LICENSE-APACHE`, and
`README.md`.

## 4. Configure and run

```toml
# scorpio.toml — do not copy example `/lfs`; mega2 trunk serves `/api/v1/lfs`
base_url = "http://127.0.0.1:9000"
lfs_url  = "http://127.0.0.1:9000/api/v1/lfs"
workspace = "/tmp/megadir-$USER/mount"
store_path = "/tmp/megadir-$USER/store"
```

`workspace` must be an empty directory. Then:

```bash
mkdir -p /tmp/megadir-$USER/mount /tmp/megadir-$USER/store
./scorpio --config-path scorpio.toml doctor
./scorpio --config-path scorpio.toml serve --http-addr 127.0.0.1:2725
```

Developers can still build from a checkout instead of the tarball:

```bash
cd /path/to/scorpiofs
cargo build --release
./target/release/scorpio --config-path scorpio.toml serve --http-addr 127.0.0.1:2725
```

## 5. Browse and unmount

- `curl -sS http://127.0.0.1:2725/health`
- Terminal: `ls` the `workspace` path
- Finder: open the same directory

macOS mounts do **not** request `allow_other`. Same-user Finder and Terminal
access is enough. `force_readdir_plus` is Linux-only; macFUSE does not
advertise READDIRPLUS.

Ctrl-C the daemon. `mount` should no longer list the workspace, and the
directory should be reusable for the next `serve`.

## 6. What `scorpio doctor` checks on macOS

- **FAIL** if `mount_macfuse` is missing
- **WARN** if FUSE-T is installed but macFUSE is not
- Skips `/proc/filesystems` and `/etc/fuse.conf`
- Directory writability and mega HTTP reachability (`http://127.0.0.1:9000`)
