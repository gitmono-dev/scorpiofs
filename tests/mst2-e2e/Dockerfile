# MST/2 acceptance image: the scorpiofs MST/2 binaries plus the tools the
# end-to-end checks need (git for the truth comparison, fuse3 to mount).
#
# Build context is the scorpiofs checkout; the shared codec arrives as a named
# build context (`mst2codec`) because it is a path dependency.
FROM rust:1.97-bookworm AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        clang cmake git libfuse3-dev libssl-dev pkg-config fuse3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src/scorpiofs
COPY . /src/scorpiofs
# Dev-only sources the production context excludes.
COPY --from=examplesctx ./ /src/scorpiofs/examples/
COPY --from=mst2codec ./ /src/mst2-codec/

ARG TARGETARCH
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=scorpiofs-cargo-registry-${TARGETARCH},sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,id=scorpiofs-cargo-git-${TARGETARCH},sharing=locked \
    --mount=type=cache,target=/src/scorpiofs/target,id=scorpiofs-target-${TARGETARCH},sharing=locked \
    cargo build --examples \
    && install -D -m0755 target/debug/examples/mst2_mount /out/mst2_mount \
    && install -D -m0755 target/debug/examples/mst2_sync /out/mst2_sync

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        coreutils \
        curl \
        fuse3 \
        git \
        libssl3 \
        python3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/mst2_mount /usr/local/bin/mst2_mount
COPY --from=build /out/mst2_sync /usr/local/bin/mst2_sync
COPY --from=e2efiles acceptance.sh /usr/local/bin/mst2-acceptance.sh
RUN chmod 0755 /usr/local/bin/mst2-acceptance.sh

# The acceptance is a one-shot job: its exit status is the verdict.
ENTRYPOINT ["/usr/local/bin/mst2-acceptance.sh"]
