# Deploying ScorpioFS

This directory and the repo-root artifacts (`Dockerfile`, `docker-compose.yml`,
`install.sh`) cover the supported deployment paths. ScorpioFS mounts a FUSE
filesystem, so **every** path needs a FUSE-capable host.

## FUSE prerequisites (read first)

ScorpioFS cannot run where FUSE is unavailable. On the host (or container) you
need:

- the `fuse` kernel module loaded and the `/dev/fuse` device present;
- the `fuse3` userspace package (provides the setuid `fusermount3` helper);
- `findmnt` from `util-linux`, used to prevent ownership migration across nested
  mounts during upgrades;
- permission to mount — either `CAP_SYS_ADMIN`, or unprivileged mounting via
  `fusermount3` (the `rfuse3` `unprivileged` feature is enabled in this build).

Run `scorpio doctor` to check these on a given host.

> Not every environment can run FUSE (many managed/rootless container platforms
> block `/dev/fuse` or `CAP_SYS_ADMIN`). There is no workaround — pick a host
> that allows it.

## ⚠️ The HTTP API is unauthenticated

`/api/fs/*`, `/api/config`, `/antares/*`, and `/health` have **no authentication**.
Anyone who can reach the port can trigger mounts/unmounts. Therefore:

- The systemd unit and the compose example bind the HTTP port to **loopback
  (`127.0.0.1`)** by default.
- The code default (`scorpio serve`) is still `0.0.0.0:2725`; only change the
  deployment to a routable address when it sits behind a **firewall and/or an
  authenticating reverse proxy**.
- Never expose port 2725/2726 directly to an untrusted network.

## Container (Docker / Compose)

```bash
docker build -t scorpiofs .

docker run --rm \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  --security-opt apparmor:unconfined \
  -e SCORPIO_BASE_URL=http://your-mega:8000 \
  -e SCORPIO_LFS_URL=http://your-mega:8000/lfs \
  -p 127.0.0.1:2725:2725 \
  scorpiofs
```

`SCORPIO_BASE_URL` and `SCORPIO_LFS_URL` are **required** for `serve`; the
container entrypoint refuses to start without them (so it never silently points
at localhost).

`docker compose up` brings up ScorpioFS plus a `mega` backend; **set the `mega`
image** in `docker-compose.yml` to the one you run (the default tag is a
placeholder). Configuration is entirely env-driven (`SCORPIO_*`); no developer
paths are baked into the image. The image ships a `HEALTHCHECK` against
`GET /health`.

### Security notes (containers)

- `CAP_SYS_ADMIN` is broad. Grant it only to this workload, and prefer a
  dedicated, otherwise-unprivileged container.
- `--security-opt apparmor:unconfined` is often required for FUSE mount
  propagation; scope it tightly.
- The reverse proxy (if any) only needs the HTTP port — the FUSE mount lives
  inside the container and cannot be proxied over HTTP.

## systemd (bare metal)

Unit files live in [`systemd/`](./systemd/). Typical install:

```bash
sudo useradd --system --no-create-home --user-group scorpiofs
sudo usermod -aG fuse scorpiofs
sudo install -D -m0644 deploy/systemd/scorpiofs.service /etc/systemd/system/scorpiofs.service

# Install a config whose paths match the unit's /var/lib/scorpiofs tree. Do NOT
# just copy scorpio.toml.example — its /tmp paths and relative config_file are
# for local dev, and the service (User=scorpiofs, no WorkingDirectory) would
# write state relative to `/`, failing with permission denied.
sudo install -d /etc/scorpiofs
sudo tee /etc/scorpiofs/scorpio.toml >/dev/null <<'EOF'
base_url = "http://your-mega:8000"
lfs_url = "http://your-mega:8000/lfs"
workspace = "/var/lib/scorpiofs/mount"
store_path = "/var/lib/scorpiofs/store"
config_file = "/var/lib/scorpiofs/config.toml"
git_author = "MEGA"
git_email = "admin@mega.org"
log_level = "info"
antares_upper_root = "/var/lib/scorpiofs/antares/upper"
antares_cl_root = "/var/lib/scorpiofs/antares/cl"
antares_mount_root = "/var/lib/scorpiofs/antares/mnt"
antares_state_file = "/var/lib/scorpiofs/antares/state.toml"
EOF
sudo "${EDITOR:-vi}" /etc/scorpiofs/scorpio.toml   # set base_url / lfs_url

sudo systemctl daemon-reload
sudo systemctl enable --now scorpiofs
systemctl status scorpiofs
```

(`install.sh` already generates a `/var/lib/scorpiofs`-based config for you, so
if you used it you can skip the config step above and just edit `base_url`/`lfs_url`.)

The unit uses `Type=simple` with `/health` as the external readiness probe
(`Type=notify` is intentionally **not** used — the daemon does not implement
`sd_notify`). `TimeoutStopSec=45` exceeds the in-process shutdown budget
(daemon join 20s + Antares cleanup 15s) so graceful unmount completes before
`SIGKILL`. `Restart=on-failure` with `StartLimitBurst` rate-limiting handles
crashes; `ExecStopPost` lazily unmounts any residual mountpoint.

### Capabilities vs setuid

- **`AmbientCapabilities=CAP_SYS_ADMIN`** (used by the unit): the service user
  gets the mount capability without running as root. High privilege — review
  before enabling.
- **setuid `fusermount3`**: the `fuse3` package's helper is setuid root and can
  perform unprivileged mounts. If your environment allows it, you can drop
  `CAP_SYS_ADMIN` and rely on `fusermount3` instead. Verify with `scorpio doctor`.
- This is FUSE (libfuse-fs userspace OverlayFs), **not** kernel `overlayfs`; do
  not try to `mount -t overlay` these paths.

## install.sh

`install.sh` is the recommended interactive installer. It asks for the
Mega/monorepo `base_url`, `lfs_url`, local paths, HTTP bind address, FUSE
permission, and whether to create a systemd service. It downloads a release
tarball, **verifies its SHA256 checksum**, installs `scorpio`/`antares`, creates
the `scorpiofs` service user, prepares the FUSE group and data directories, and
generates `/etc/scorpiofs/scorpio.toml` with absolute runtime paths.

When a systemd service is selected, the installer waits for the configured
`/health` endpoint before reporting success. During an upgrade, a failure after
stopping the existing service restores the previous binaries, config, and unit
before attempting to restart it. Existing data-directory permissions are
preserved; only newly created directories use the installer's default `0755`
mode.

- Run `sudo bash install.sh` for the interactive flow. It supports `curl | bash`
  because prompts are read from a verified controlling terminal.
- The API defaults to `127.0.0.1:2725` because it has no authentication. A
  non-loopback bind requires `--allow-public-api` and an external firewall or
  authenticating reverse proxy.
- Always supports `--dry-run` to preview every action.
- It only modifies `/etc/fuse.conf` after the explicit interactive confirmation
  or when `--enable-user-allow-other` is passed.
- System packages are installed via apt/dnf/pacman (skip with `--no-deps`).
- `--uninstall` removes the binaries and service unit and leaves config/data in
  place.
- `--overwrite-config` (or `SCORPIO_OVERWRITE_CONFIG=1`) is required when an
  automated upgrade should replace an existing `scorpio.toml`; otherwise its
  contents are retained. Relative runtime paths in retained configs are resolved
  against the explicit or inferred data root.
- Retained upgrades recursively reconcile ownership for the local store and
  Antares upper/CL data. FUSE workspace and mount roots are not traversed, and
  an upgrade is rejected until any nested mount below a migrated tree is
  unmounted. A managed active unit is stopped before its binary, config, or unit
  is replaced and then started under the configured account.
- On an existing systemd installation, `--no-service` leaves the unit untouched
  and preserves ownership for its configured `User` rather than reassigning the
  config and data to the invoking sudo user.
- Service setup verifies that systemd is reachable before creating the service
  account or changing ownership. Use `--no-service` on hosts without systemd.
- Installer config checks clear ambient `SCORPIO_*` daemon overrides so a
  retained file is validated exactly as the generated systemd unit will load it.
- A retained-config `--dry-run` resolves effective paths with the already
  installed `scorpio` binary. It fails explicitly when that binary is missing,
  because the preview could not otherwise validate the real upgrade paths. A
  non-root preview may request sudo to detect and read a protected config;
  real non-root installs use the same elevated existence check rather than
  treating an unsearchable config directory as an absent configuration.
- `--workspace` and `--store-path` must be children of the dedicated
  `--data-root`; filesystem roots, symlink escapes, shell metacharacters, and
  nonempty directories without an existing ScorpioFS config are rejected.
- FUSE workspace/Antares mount roots must not equal, contain, or sit inside
  persistent store, config, upper, CL, or state paths; mount roots also may not
  overlap each other. Installed binaries and the main `scorpio.toml` must also
  remain outside both mount roots.
- Git author/email values may contain tabs, which are escaped in TOML; other
  control characters are rejected from every generated TOML string before
  installation begins.
- Inaccessible stale FUSE workspace/Antares mounts are lazily detached before
  directory preparation. A non-FUSE mount at either configured root is rejected
  and never detached. Service setup rejects an accessible mount not owned by an
  existing managed unit; stop the user-run daemon and unmount it before retrying.
  Managed upgrades retain the old configured mount paths until the active unit
  is stopped, then clean any FUSE roots it leaves behind before installing the
  new binary, config, and unit. This also applies when `--overwrite-config`
  migrates to an empty new data root; all-relative old configs are rejected in
  that case because their previous mount locations cannot be inferred safely.
- `--release-base-url` (or `SCORPIO_RELEASE_BASE_URL`) points the installer at
  a mirror or local HTTP server using the same `<base>/<version>/<asset>`
  layout as GitHub releases. The PR build uses this to exercise a complete
  local package/checksum/install/config-validation flow.

```bash
# Recommended interactive install: download, inspect, and execute.
curl -fsSLO https://raw.githubusercontent.com/gitmono-dev/scorpiofs/main/install.sh
less install.sh
sudo bash install.sh

# One-line interactive install.
curl -fsSL https://raw.githubusercontent.com/gitmono-dev/scorpiofs/main/install.sh | sudo bash

# Automated install or reconfiguration.
curl -fsSL https://raw.githubusercontent.com/gitmono-dev/scorpiofs/main/install.sh | \
  sudo bash -s -- --non-interactive --overwrite-config \
    --base-url https://mega.example.com \
    --lfs-url https://mega.example.com/lfs \
    --http-addr 127.0.0.1:2725

# Keep /etc/fuse.conf unchanged when user_allow_other is already enabled.
sudo bash install.sh --non-interactive --no-service --no-user-allow-other \
  --base-url https://mega.example.com \
  --lfs-url https://mega.example.com/lfs

# On a host without user_allow_other, use this instead of the command above.
sudo bash install.sh --non-interactive --no-service --enable-user-allow-other \
  --base-url https://mega.example.com \
  --lfs-url https://mega.example.com/lfs

# Remove binaries and the unit while retaining config and data.
sudo bash install.sh --uninstall
```

## Releases & supply chain

Pushing a `v*` tag triggers `.github/workflows/release.yml`, which:

- builds `x86_64-unknown-linux-gnu` (and best-effort `aarch64-unknown-linux-musl`
  via `cross`; a failure there does not block the x86_64 release);
- packages each target as `scorpiofs-<version>-<target>.tar.gz` containing
  `scorpio`, `antares`, `LICENSE-MIT`, `LICENSE-APACHE`, and `README.md`;
- generates a `<tarball>.sha256` per artifact plus a combined `SHA256SUMS`;
- creates a GitHub Release with all artifacts attached.

`install.sh` downloads the per-target tarball **and its `.sha256`**, then runs
`sha256sum -c` and refuses to install on mismatch. Before replacing installed
binaries, it also runs both extracted binaries with `--version`; this catches
CPU, dynamic-linker, and glibc incompatibilities without leaving a broken
installation behind. GNU release binaries are built on Ubuntu 22.04 to retain
glibc 2.35 compatibility.

Publishing to **crates.io is decoupled** from the binary release: the
`publish-crate` job targets a protected GitHub Environment (`crates-io`).
Configure a required reviewer on that environment so an ordinary tag push cannot
publish the crate without manual approval, and store `CARGO_REGISTRY_TOKEN` as an
environment secret (least privilege).

This project is dual-licensed under MIT (`LICENSE-MIT`) OR Apache-2.0
(`LICENSE-APACHE`).
