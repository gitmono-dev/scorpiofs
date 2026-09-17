# syntax=docker/dockerfile:1

# ---- build stage -------------------------------------------------------------
# Pin the Rust toolchain (reproducible builds; matches the mega2 builder
# generation) and the Debian release so the OpenSSL runtime package name is
# stable (`libssl3` on bookworm; trixie/noble renamed it to `libssl3t64`).
#
# `.dockerignore` keeps `target/`, `.libra/` and other host artifacts out of the
# build context, so `COPY . .` only ships sources + the few runtime files below.
FROM rust:1.97-slim-bookworm AS build
WORKDIR /src

# Build-time system dependencies: FUSE headers + OpenSSL + pkg-config, plus
# clang/libclang for bindgen (`zstd-sys`, pulled in by `git-internal`).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config \
        libfuse3-dev \
        libssl-dev \
        clang \
        libclang-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

# BuildKit cache mounts keep the cargo registry/git checkouts and the `target`
# dir across builds so a source change only recompiles the scorpiofs crate.
# `target` lives on the cache mount (not in the layer), so the binaries are
# copied out inside the same RUN step where the mount is still visible.
ARG TARGETARCH
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=scorpiofs-cargo-registry-${TARGETARCH},sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,id=scorpiofs-cargo-git-${TARGETARCH},sharing=locked \
    --mount=type=cache,target=/src/target,id=scorpiofs-target-${TARGETARCH},sharing=locked \
    cargo build --release --locked --bin scorpio --bin antares \
    && install -D -m0755 /src/target/release/scorpio /out/scorpio \
    && install -D -m0755 /src/target/release/antares /out/antares

# ---- runtime stage -----------------------------------------------------------
# debian:bookworm-slim (not distroless, version-pinned) so the FUSE userspace
# helper + TLS libs are present and the package names are stable.
FROM debian:bookworm-slim AS runtime

# Runtime dependencies:
#   fuse3            - provides the setuid `fusermount3` helper used for unprivileged mounts
#   libssl3          - TLS for reqwest (native-tls)
#   ca-certificates  - trust roots for HTTPS
#   curl             - used by the container HEALTHCHECK against /health
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        fuse3 \
        libssl3 \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/scorpio /usr/local/bin/scorpio
COPY --from=build /out/antares /usr/local/bin/antares
COPY deploy/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# A baseline config file must exist (it is parsed before env overrides apply).
# Its values are overridden by the SCORPIO_* environment variables below and at
# `docker run -e ...` time. base_url / lfs_url are intentionally NOT baked here
# (no sensible default) and MUST be provided at run time.
COPY scorpio.toml.example /etc/scorpiofs/scorpio.toml

# Runtime directories live under /var/lib/scorpiofs and are env-overridable.
ENV SCORPIO_WORKSPACE=/var/lib/scorpiofs/mount \
    SCORPIO_STORE_PATH=/var/lib/scorpiofs/store \
    SCORPIO_CONFIG_FILE=/var/lib/scorpiofs/config.toml \
    SCORPIO_ANTARES_UPPER_ROOT=/var/lib/scorpiofs/antares/upper \
    SCORPIO_ANTARES_CL_ROOT=/var/lib/scorpiofs/antares/cl \
    SCORPIO_ANTARES_MOUNT_ROOT=/var/lib/scorpiofs/antares/mnt \
    SCORPIO_ANTARES_STATE_FILE=/var/lib/scorpiofs/antares/state.toml

# Pre-create the runtime tree so `scorpio doctor` passes its directory checks
# out of the box and a fresh named volume mounted at /var/lib/scorpiofs starts
# populated (Docker copies the image's directory contents into an empty volume).
# No `VOLUME` directive on purpose: the compose file decides whether state is
# persisted, so plain `docker run` never leaves anonymous volumes behind.
RUN install -d -m0755 \
        /var/lib/scorpiofs/mount \
        /var/lib/scorpiofs/store \
        /var/lib/scorpiofs/antares/upper \
        /var/lib/scorpiofs/antares/cl \
        /var/lib/scorpiofs/antares/mnt

EXPOSE 2725

# FUSE inside a container requires the host's /dev/fuse device and CAP_SYS_ADMIN;
# CAP_DAC_READ_SEARCH lets the passthrough layer use open_by_handle_at(2) instead
# of falling back to fd-backed inodes (see deploy/README.md).
# The container listens on 0.0.0.0 *inside* its network namespace; host exposure
# is controlled by the `-p` mapping (bind to 127.0.0.1 on the host unless you
# have a firewall / reverse proxy — the HTTP API is unauthenticated):
#   docker run --device /dev/fuse --cap-add SYS_ADMIN --cap-add DAC_READ_SEARCH \
#              --security-opt apparmor:unconfined \
#              -e SCORPIO_BASE_URL=http://mega:8000 -e SCORPIO_LFS_URL=http://mega:8000/lfs \
#              -p 127.0.0.1:2725:2725 scorpiofs
# Not every Docker host can run FUSE; see deploy/README.md.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:2725/health || exit 1

# The entrypoint requires SCORPIO_BASE_URL/SCORPIO_LFS_URL before `serve`, so a
# misconfigured container fails loudly instead of silently pointing at localhost.
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["serve", "--http-addr", "0.0.0.0:2725"]
