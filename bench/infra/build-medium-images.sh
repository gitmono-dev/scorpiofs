#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
: "${MEDIUM_WORKDIR:?Directory holding fixture manifests and repo/.git}"
: "${MEDIUM_REGISTRY:?Registry/repository for the output images}"
: "${MEDIUM_TAG:?Unique tag prefix for this run}"
: "${MEDIUM_RELEASE_DIR:?Directory containing release scorpio and antares}"
: "${MEDIUM_BASE_IMAGE:?Runner base image with Git, Python and FUSE runtime}"
: "${MEDIUM_BACKEND_IMAGE:?Backend image built from the frozen source version}"
RUN="$MEDIUM_WORKDIR"
REG="$MEDIUM_REGISTRY"
TAG="$MEDIUM_TAG"
for f in manifest.jsonl manifest.refs.json manifest.summary.json repo/.git/HEAD; do
  test -f "$RUN/$f" || { echo "Missing fixture input: $f" >&2; exit 1; }
done
for d in runtime-context fixture-context; do
  test ! -e "$RUN/$d" || { echo "Preserve existing build context: $d" >&2; exit 1; }
done
mkdir -p "$RUN/runtime-context"
cp "$MEDIUM_RELEASE_DIR/scorpio" "$MEDIUM_RELEASE_DIR/antares" "$RUN/runtime-context/"
cp "$ROOT/bench/bin/first-directory-ready.py" "$ROOT/bench/cases/medium-profile.py" \
   "$ROOT/bench/workload/medium-monorepo.py" "$ROOT/bench/infra/medium-cloud-run.py" "$RUN/runtime-context/"
cat > "$RUN/runtime-context/Dockerfile" <<'EOF'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
COPY scorpio antares /usr/local/bin/
COPY *.py /bench-medium/
RUN scorpio --help >/dev/null && antares --help >/dev/null && python3 -m py_compile /bench-medium/*.py
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
