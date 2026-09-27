#!/usr/bin/env bash
# Push the runtime images to the user's ACR *personal* repository.
#
# Credentials are read from the environment (never written to disk, never echoed):
#   ACR_USER / ACR_PASS / ACR_REG
# This script prints only the *stripped* forms it needs for confirmation.
set -uo pipefail

REG="${ACR_REG:-crpi-r1oa3ys0csfxkdeq.cn-hangzhou.personal.cr.aliyuncs.com}"
NS="${ACR_NS:-gitmono}"
REPO="${ACR_REPO:-gitmono1}"
USER="${ACR_USER:?set ACR_USER (e.g. aliyun8193774171)}"
PASS="${ACR_PASS:?set ACR_PASS}"
TAG="${TAG:-v0.3.0}"

echo "== target =="
echo "  registry : $REG"
echo "  repo     : $NS/$REPO"
echo "  tag      : $TAG"
echo "  user     : ${USER}"

echo "== login (password via stdin; not echoed) =="
printf '%s' "$PASS" | docker login "$REG" -u "$USER" --password-stdin 2>&1 | tail -2

echo "== tag + push =="
push_one() { # <local-image> <artifact-name>
  local src="$1" art="$2" full="$REG/$NS/$REPO:$TAG-$2"
  echo "  -- $src -> $full"
  docker tag "$src" "$full" || return 1
  docker push "$full" 2>&1 | tail -3
}
push_one "mega2:local"       "mega2"
push_one "scorpiofs:runtime" "scorpiofs"

echo "== verify in registry =="
curl -sS --noproxy '*' -u "$USER:$PASS" "https://$REG/v2/$NS/$REPO/tags/list" 2>/dev/null | head -c 400
echo
docker images | grep "$REG/$NS" || true
