#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
: "${MEDIUM_WORKDIR:?Directory holding fixture manifests and repo/.git}"
: "${MEDIUM_REGISTRY:?Registry/repository for the output images}"
: "${MEDIUM_TAG:?Unique tag prefix for this run}"
: "${MEDIUM_RELEASE_DIR:?Directory containing the release scorpio binary}"
: "${MEDIUM_LIBRA_BINARY:?Path to the Linux Libra executable for development tests}"
: "${MEDIUM_BASE_IMAGE:?Linux base with Bash, Git, Python, curl, GNU coreutils and FUSE runtime}"
: "${MEDIUM_BACKEND_IMAGE:?Backend image built from the frozen source version}"
RUN="$MEDIUM_WORKDIR"
REG="$MEDIUM_REGISTRY"
TAG="$MEDIUM_TAG"
for binary in "$MEDIUM_RELEASE_DIR/scorpio" "$MEDIUM_LIBRA_BINARY"; do
  test -f "$binary" && test -x "$binary" || { echo "Missing executable: $binary" >&2; exit 1; }
done
for f in manifest.jsonl manifest.refs.json manifest.summary.json repo/.git/HEAD; do
  test -f "$RUN/$f" || { echo "Missing fixture input: $f" >&2; exit 1; }
done
for d in runtime-context fixture-context; do
  test ! -e "$RUN/$d" || { echo "Preserve existing build context: $d" >&2; exit 1; }
done
mkdir -p "$RUN/runtime-context"
cp "$MEDIUM_RELEASE_DIR/scorpio" "$RUN/runtime-context/"
cp "$MEDIUM_LIBRA_BINARY" "$RUN/runtime-context/libra"
# Windows checkouts may have CRLF; a Linux shebang must not retain CR.
sed 's/\r$//' "$ROOT/deploy/docker-entrypoint.sh" > "$RUN/runtime-context/docker-entrypoint.sh"
cp "$ROOT/scorpio.toml.example" "$RUN/runtime-context/scorpio.toml"
cp "$ROOT/bench/bin/first-directory-ready.py" "$ROOT/bench/cases/medium-profile.py" \
   "$ROOT/bench/workload/medium-monorepo.py" "$ROOT/bench/infra/medium-cloud-run.py" \
   "$ROOT/bench/cases/git-shallow-gate.py" "$RUN/runtime-context/"
cat > "$RUN/runtime-context/Dockerfile" <<'EOF'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
COPY scorpio libra docker-entrypoint.sh /usr/local/bin/
COPY scorpio.toml /etc/scorpiofs/scorpio.toml
COPY *.py /bench-medium/
ENV SCORPIO_STORE_PATH=/var/lib/scorpiofs/store \
    SCORPIO_MST2_BASE_URL=http://mega2:8000
RUN bash --version >/dev/null && git --version >/dev/null && curl --version >/dev/null && \
    fusermount3 --version >/dev/null && timeout --kill-after=1 1 true && du -sB1 /tmp >/dev/null && \
    chmod +x /usr/local/bin/docker-entrypoint.sh && \
    mkdir -p "$SCORPIO_STORE_PATH" && \
    /usr/local/bin/docker-entrypoint.sh --help >/dev/null && \
    libra --help >/dev/null && python3 -m py_compile /bench-medium/*.py
ENTRYPOINT []
CMD ["sleep", "infinity"]
EOF
docker build --build-arg "BASE_IMAGE=$MEDIUM_BASE_IMAGE" -t "$REG:$TAG-runner" "$RUN/runtime-context"
docker tag "$MEDIUM_BACKEND_IMAGE" "$REG:$TAG-mega2"
mkdir -p "$RUN/fixture-context"
cp "$RUN/manifest.jsonl" "$RUN/manifest.refs.json" "$RUN/manifest.summary.json" "$RUN/fixture-context/"
cp -a "$RUN/repo/.git" "$RUN/fixture-context/git"
git --git-dir="$RUN/fixture-context/git" fsck --full
cat > "$RUN/fixture-context/Dockerfile" <<'EOF'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
COPY git /fixture/repo.git
COPY manifest* /fixture/
ENTRYPOINT []
CMD ["sleep", "infinity"]
EOF
docker build --build-arg "BASE_IMAGE=$MEDIUM_BASE_IMAGE" -t "$REG:$TAG-fixture" "$RUN/fixture-context"
if [[ "${MEDIUM_PUSH_IMAGES:-0}" == "1" ]]; then
  for kind in runner mega2 fixture; do docker push "$REG:$TAG-$kind"; done
fi
docker image inspect "$REG:$TAG-runner" "$REG:$TAG-mega2" "$REG:$TAG-fixture" \
  --format '{{.RepoTags}} {{.RepoDigests}}' > "$RUN/image-digests.txt"
