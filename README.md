![ScorpioFS](docs/images/banner.png)

## Scorpio - FUSE Support for Mega/Monorepo Client

### What's the Fuse?

FUSE is the abbreviation for "FileSystem in Userspace".It's an interface for userspace programs to export a filesystem to the linux kernel.
The FUSE project consists of two components: the fuse kernel module (maintained in the regular kernel repositories) and the libfuse userspace library (maintained in this repository).
![FUSE](docs/images/FUSE_VFS.png)

When VFS receives a file access request from the user process and this file belongs to a certain fuse file system, it will forward the request to a kernel module named "fuse". Then, "fuse" converts the request into the protocol format agreed upon with the daemon and transmits it to the daemon process.

Currently, there have been many successful fuse based projects,

- [s3fs](https://github.com/s3fs-fuse/s3fs-fuse)
 makes you operate files and directories in S3 bucket like a local file system
 ![Github stars](https://img.shields.io/github/stars/s3fs-fuse/s3fs-fuse.svg)
- [sshfs](https://github.com/libfuse/sshfs) 
allows you to mount a remote filesystem using SFTP
![Github stars](https://img.shields.io/github/stars/libfuse/sshfs.svg)
- [google-drive-ocamlfuse](https://github.com/astrada/google-drive-ocamlfuse.git) lets you mount your Google Drive on Linux.
![Github stars](https://img.shields.io/github/stars/astrada/google-drive-ocamlfuse.svg)

### Why the Monorepo need a FUSE?

Because the code organization requirements are different from the existing popular distributed version management software Git, clients targeting Monorepo need to implement various additional features to support code pull tasks for large repositories. These requirements include:

1. **Partial clone**: reduces the time required to obtain a working repository by not immediately downloading all Git objects.

2. **Background prefetch**: Download Git object data from all remote sources every hour, reducing the time required for front-end Git fetch calls.

3. **Sparse checkout**: Restrict the size of the working directory.

4. **File system monitor**: tracks recently modified files, eliminating the need for Git to scan the entire work tree.

5. **Submit graph**: Accelerate submission traversal and reachability calculations, and speed up commands such as git log.

6. **Multi pack index**: Implement fast object lookup in many package files.

7. **Incremental repackage**: Using multiple package indexes, repackage packaged Git data into fewer package files without interrupting parallel commands.

### Some Related

#### [VFS for Git](https://github.com/microsoft/VFSForGit) from Microsoft
VFS For Git is a preliminary attempt by Microsoft on the Monorepo client, which implemented the FUSE system based on Sqlite and Mutli pack index, achieving on-demand partial pull functionality. The client will perceive the user's "open directory" operation before pulling the code content under the corresponding directory.

#### [Sapling](https://sapling-scm.com/) from Meta 
The structure of Sapling is achieved through a multi-layered architecture, with each checkout corresponding to a mount point, followed by an Overlay layer. At the same time, it provides third-party interfaces for other programs to use, so that some heavy IO and computational parts do not need to be consumed by the performance of the virtual layer.

### Rust Crate

Scorpio is a Rust project, and the crate is named `scorpiofs`.

https://crates.io/crates/scorpiofs

### How to Use?

**Prerequisites:** Linux with FUSE enabled, `libfuse-dev`, and a running Mega/monorepo server. See [docs/develop.md](docs/develop.md) for system setup (may require `sudo` for FUSE).

1. Start the mono server (e.g. `http://localhost:8000`).
2. Edit **`scorpio.toml`** (not `config.toml`): set `base_url`, `workspace`, and `store_path`. The `config.toml` file is a **runtime state file** (tracks mounted workspaces), created automatically on first run.
3. Build the binaries (`scorpio` and the deprecated `antares` alias) and run the daemon:

```bash
cargo build --release
./target/release/scorpio serve            # or: cargo run --release -- serve
```

The unified `scorpio` binary uses subcommands:

```bash
scorpio serve [--http-addr 0.0.0.0:2725]   # run the workspace daemon (FUSE mount + HTTP API)
scorpio mount <job_id> [--cl <cl>]         # mount an Antares job instance
scorpio umount <job_id>                    # unmount an Antares job instance
scorpio list                               # list tracked Antares instances
scorpio http-mount <path> [--job-id <id>] [--cl <cl>] [--endpoint <url>]  # mount via a running HTTP daemon
scorpio config init|validate|show          # generate / check / print configuration
scorpio doctor                             # diagnose FUSE/permissions/mega connectivity
scorpio completions <bash|zsh|fish|...>    # print a shell completion script
```

`http-mount --endpoint` defaults to `http://127.0.0.1:2725/antares` (the Antares
API nested in a local `scorpio serve`); point it at another daemon's base URL to
mount against a remote/standalone daemon (e.g. `http://host:2726` for a
standalone `antares serve`).

Generate and install completions, e.g. for bash:

```bash
scorpio completions bash > /usr/share/bash-completion/completions/scorpio
```

Global options (`--config-path`, `--log-level`, `--http-addr`, and the Antares
path overrides `--upper-root`/`--cl-root`/`--mount-root`/`--state-file`) work
with any subcommand. Running `scorpio` with **no** subcommand is a deprecated
shorthand for `scorpio serve` (it still honors `-c`/`--http-addr`).

The CLI returns stable exit codes for scripting: `0` success, `2` config error,
`3` mount/unmount failure, `4` HTTP bind failure, `1` other internal error.

The `antares` binary is a **deprecated compatibility alias** for the
`mount`/`umount`/`list`/`http-mount` commands and a standalone HTTP daemon
(`antares serve --bind 0.0.0.0:2726`). It does **not** include `completions`;
prefer the `scorpio` binary. The alias is retained for at least one minor release.

### How to Interact?

`scorpio serve` exposes an HTTP API on `--http-addr` (default `0.0.0.0:2725`).

> ⚠️ **The HTTP API is unauthenticated** — anyone who can reach the port can
> trigger mounts/unmounts. Bind it to loopback or put it behind a firewall /
> authenticating reverse proxy. The systemd unit and `docker-compose.yml`
> default to `127.0.0.1`. See [deploy/README.md](deploy/README.md).

**Liveness — `GET /health`** (root, lightweight; no remote/FUSE probing, no path
leakage; use it for container/systemd health checks):

```bash
curl http://localhost:2725/health
# {"status":"ok","version":"0.2.2","uptime_secs":42,"mount_count":0}
```

**Recommended — Antares API** (nested under the main server at `/antares/*`):

```bash
curl http://localhost:2725/antares/health
curl -X POST http://localhost:2725/antares/mounts \
  -H "Content-Type: application/json" \
  -d '{"job_id":"job-1","path":"/third-party/mega"}'
curl http://localhost:2725/antares/mounts
```

See [docs/antares.md](docs/antares.md) for the full Antares API (including per-mount readiness `GET /antares/mounts/{id}/ready`).

**Legacy API** (`/api/fs/*` and `/api/config`) — **deprecated**: still works for
at least one minor release, but every response carries a `Deprecation: true`
header and a server-side warning log. Prefer the Antares API.

```bash
curl -X POST http://localhost:2725/api/fs/mount \
  -H "Content-Type: application/json" \
  -d '{"path": "third-party/mega/scorpio"}'
curl http://localhost:2725/api/fs/mpoint
curl http://localhost:2725/api/fs/select/<request_id>
curl -X POST http://localhost:2725/api/fs/unmount \
  -H "Content-Type: application/json" \
  -d '{"path": "third-party/mega/scorpio"}'
```

See [docs/api.md](docs/api.md) for request/response details.

### How to Configure?

A minimal `scorpio.toml` — usually only `base_url` / `lfs_url` need changing:

```toml
base_url = "http://localhost:8000"
lfs_url = "http://localhost:8000/lfs"
store_path = "/tmp/scorpio-megadir/store"
workspace = "/tmp/scorpio-megadir/mount"
config_file = "config.toml"
git_author = "MEGA"
git_email = "admin@mega.org"
dicfuse_readable = "true"
load_dir_depth = "3"
fetch_file_thread = "10"
```

A fully-commented template with every key is in
[`scorpio.toml.example`](scorpio.toml.example). You can also manage config from
the CLI:

```bash
scorpio config init myconfig.toml        # write a template
scorpio config validate                  # offline-check a file, reporting all problems
scorpio config show                      # print the effective merged config
```

### `scorpio.toml` Configuration Guide

- **`base_url`** — Mega/monorepo service base URL (e.g. `http://localhost:8000`).
- **`lfs_url`** — LFS endpoint URL (typically same host as `base_url`).
- **`workspace`** — FUSE mount point visible to users (not `mount_path`).
- **`store_path`** — Local directory for cached/stored files (must be writable).
- **`config_file`** — Runtime state file path (default `config.toml`; records `works=[]` mounted paths). This is **not** the main config file.
- **`git_author`** / **`git_email`** — Default Git author metadata.
- **`log_level`** — Default tracing filter directive (e.g. `"info"`, `"scorpio=debug"`). See *Logging* below.
- **`dicfuse_readable`** — Allow reading from read-only directories (`"true"` / `"false"`).
- **`load_dir_depth`** — Directory preload depth during initialization.
- **`fetch_file_thread`** — Concurrent download thread count.

### Logging

All runtime diagnostics go through `tracing` and are written to stderr (so
journald / `docker logs` collect them). The active filter is chosen by this
precedence (highest first):

```
--log-level <directive>  >  SCORPIO_LOG  >  RUST_LOG  >  config log_level  >  "info"
```

A directive is a standard `EnvFilter` string, e.g. `info`, `scorpio=debug`, or
`warn,scorpiofs::dicfuse=trace`. An invalid directive falls back to `info`
rather than aborting startup.

Antares-specific keys use flat names in `scorpio.toml` (e.g. `antares_mount_root`, `antares_upper_root`). See [docs/antares.md](docs/antares.md#配置) for the full list.

### Environment Variable Overrides

Every configuration key can be overridden with an environment variable, which is
convenient for containers and 12-factor deployments. The resolution precedence is:

```
CLI overrides  >  environment (SCORPIO_*)  >  config file  >  built-in defaults
```

The environment variable name is `SCORPIO_` followed by the upper-cased flat key.
For example:

| Config key            | Environment variable           |
|-----------------------|--------------------------------|
| `base_url`            | `SCORPIO_BASE_URL`             |
| `lfs_url`             | `SCORPIO_LFS_URL`             |
| `workspace`           | `SCORPIO_WORKSPACE`           |
| `store_path`          | `SCORPIO_STORE_PATH`         |
| `load_dir_depth`      | `SCORPIO_LOAD_DIR_DEPTH`     |
| `antares_upper_root`  | `SCORPIO_ANTARES_UPPER_ROOT` |

```bash
SCORPIO_BASE_URL=http://mega.example.com SCORPIO_WORKSPACE=/tmp/ws \
  scorpio serve
```

The config file is also read in a forward-looking sectioned form
(`[server]` / `[dicfuse]` / `[antares]`), which coexists with the legacy flat
keys for backward compatibility. Invalid values (a non-numeric `load_dir_depth`,
an unknown `dicfuse_stat_mode`, a malformed URL, an out-of-range number) fail
fast at startup with the offending field name. The main `scorpio.toml` is treated
as read-only input and is never rewritten.

### How to Deploy?

ScorpioFS mounts a FUSE filesystem, so every deployment target needs a
FUSE-capable host (`/dev/fuse` + the `fuse` module + `fuse3`). Run
`scorpio doctor` to check a host. Full guidance is in
[deploy/README.md](deploy/README.md).

**Docker / Compose** — a multi-stage [`Dockerfile`](Dockerfile) and
[`docker-compose.yml`](docker-compose.yml) are provided; config is entirely
env-driven (`SCORPIO_*`), and FUSE needs `/dev/fuse` + `CAP_SYS_ADMIN`:

```bash
docker build -t scorpiofs .
docker run --rm --device /dev/fuse --cap-add SYS_ADMIN \
  --security-opt apparmor:unconfined \
  -e SCORPIO_BASE_URL=http://your-mega:8000 -e SCORPIO_LFS_URL=http://your-mega:8000/lfs \
  -p 127.0.0.1:2725:2725 scorpiofs
```

**systemd** — unit files are in [`deploy/systemd/`](deploy/systemd/)
(`Type=simple`, `AmbientCapabilities=CAP_SYS_ADMIN`, `TimeoutStopSec=45`,
loopback bind by default, journald logging).

**install.sh** — [`install.sh`](install.sh) downloads a release tarball,
**verifies its SHA256 checksum**, and installs the binaries + a generated config.
It supports `--dry-run`, `--uninstall`, and never edits `/etc/fuse.conf` unless
you pass `--enable-user-allow-other`:

```bash
bash install.sh --version v0.3.0 --dry-run   # preview
sudo bash install.sh --version v0.3.0        # install
```

Pushing a `v*` tag runs [`.github/workflows/release.yml`](.github/workflows/release.yml),
which builds the binaries, produces `scorpiofs-<version>-<target>.tar.gz` +
SHA256 checksums, publishes a GitHub Release, and (behind a protected
environment for manual approval) can publish to crates.io.

### How to Contribute?

Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Submit a pull request with a clear description of your changes.

For local load/performance testing, see [script/README.md](script/README.md) and
the read benchmark `cargo run --release --example fs_read_perf -- <dir>`.

### Reference
[1] Rachel Potvin and Josh Levenberg. 2016. Why Google stores billions of lines of code in a single repository. Commun. ACM 59, 7 (July 2016), 78–87. https://doi.org/10.1145/2854146
[2] Nicolas Brousse. 2019. The issue of monorepo and polyrepo in large enterprises. In Companion Proceedings of the 3rd International Conference on the Art, Science, and Engineering of Programming (Programming '19). Association for Computing Machinery, New York, NY, USA, Article 2, 1–4. https://doi.org/10.1145/3328433.3328435
[3] [libfuse](https://github.com/libfuse/libfuse.git) is the reference implementation of the Linux FUSE (Filesystem in Userspace) interface.
[4] [CS135 FUSE Documentation (hmc.edu)](https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#function-purposes)
[5] [sapling](https://github.com/facebook/sapling.git) : A cross-platform, highly scalable, Git-compatible source control system.
[6] [fuser](https://github.com/cberner/fuser.git) : A Rust library crate for easy implementation of FUSE filesystems in userspace.
[7] [Scalar](https://github.com/microsoft/git/blob/HEAD/contrib/scalar/docs/index.md) : Scalar is a tool that helps Git scale to some of the largest Git repositories. Initially, it was a single standalone git plugin based on Vfs for git, inheriting GVFS. No longer using FUSE. It implements aware partial directory management. Users need to manage and register the required workspace directory on their own. Ease of use can be improved through the fuse mechanism.

# Scorpio RoadMap

## **1. [libufse-fs] overlayFS + passthroughFS**
1. **Performance Optimization**  
   - Enhance performance by leveraging `mmap` and `eBPF`.

2. **Encryption Experimentation**  
   - Explore `rencfs` for file encryption capabilities.

3. **File Layer Management**  
   - Support file layer management for `Docker Build`.


## **2. Git Operation Functionality**
- Support More basic Git operations:  
  - `git log`  
  - `git status`  
  - `git add`  
  - Support `.gitignore` functionality.

## **3. Git LFS Support**

Integrate Git Large File Storage (LFS) for managing large files.

after mount: 
1. read the .libra_attribute in monorepo , store the patterns in the store path ..
2. get all maybe lfs point(blob);
3. if the file is lfs point, then download it.

before git push:

1. read the `.libra_attribute` in the store path ..
2. get all change lfs point(blob);
3. push changed blob to the lfs server;
4. get the lfs point(blob) from the lfs server;
5. build the commit with the lfs point(blob);


## **4. Directory Management**

1. **Local Directory Storage Recovery**  
   - Implement recovery functionality for local directory storage.

2. **Directory Change Monitoring**  
   - Monitor and address inconsistencies between local and remote storage directories.


