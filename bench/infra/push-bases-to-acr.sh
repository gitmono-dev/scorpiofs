#!/usr/bin/env bash
# Push ALL stack images to the ACR personal repo, so ACK nodes (which cannot
# reach Docker Hub / the daocloud mirror) can pull everything from the ACR
# VPC endpoint instead. Idempotent: skips tags already present.
#
# Images: their own build (mega2, scorpiofs) + the public bases the compose/
# k8s files reference (postgres, redis, rustfs, rustfs-rc).
set -uo pipefail
REG=crpi-r1oa3ys0csfxkdeq.cn-hangzhou.personal.cr.aliyuncs.com
REPO=gitmono/gitmono1

printf '%s' "${ACR_PASS:?set ACR_PASS}" | docker login "$REG" -u "${ACR_USER:-aliyun8193774171}" --password-stdin >/dev/null

push_one() { # <local-image> <tag-suffix>
  local src="$1" tag="$2" full="$REG/$REPO:$2"
  if docker manifest inspect "$full" >/dev/null 2>&1; then
    echo "  skip (exists): $tag"; return 0
  fi
  echo "  push $src -> $full"
  docker tag "$src" "$full" && docker push "$full" 2>&1 | tail -1
}

# public bases: present locally already (from earlier mirror pulls)
echo "== ensure sources exist locally (pull from mirror if missing) =="
declare -A SRC=(
  ["postgres:18.6-alpine3.24"]="docker.m.daocloud.io/library/postgres:18.6-alpine3.24"
  ["redis:8.10.1-alpine3.23"]="docker.m.daocloud.io/library/redis:8.10.1-alpine3.23"
  ["rustfs/rustfs:1.0.0"]="docker.m.daocloud.io/rustfs/rustfs:1.0.0"
  ["rustfs/rc:v0.1.36"]="rustfs/rc:v0.1.36"
)
for local_img in "${!SRC[@]}"; do
  docker image inspect "$local_img" >/dev/null 2>&1 || docker pull "${SRC[$local_img]}" >/dev/null 2>&1 || true
done

echo "== push all =="
push_one "postgres:18.6-alpine3.24"       "base-postgres"
push_one "redis:8.10.1-alpine3.23"        "base-redis"
push_one "rustfs/rustfs:1.0.0"            "base-rustfs"
push_one "rustfs/rc:v0.1.36"              "base-rustfs-rc"

docker logout "$REG" >/dev/null
echo "== done =="
