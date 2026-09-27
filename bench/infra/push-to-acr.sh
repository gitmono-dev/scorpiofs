#!/usr/bin/env bash
# Push the runtime images to Aliyun ACR.
#
# Usage:
#   ACR_REGISTRY=registry.cn-hangzhou.aliyuncs.com \
#   ACR_NAMESPACE=<your-namespace> \
#   ACR_USER=<login-user> \
#   ACR_PASSWORD=<registry-password> \
#   bash push-to-acr.sh
#
# The registry password is the ACR "访问凭证 -> 固定密码" (NOT the AK secret).
# Create the namespace once in the ACR console (个人版 is free).
set -euo pipefail

REG="${ACR_REGISTRY:?set ACR_REGISTRY, e.g. registry.cn-hangzhou.aliyuncs.com}"
NS="${ACR_NAMESPACE:?set ACR_NAMESPACE (create it in the ACR console first)}"
USER="${ACR_USER:?set ACR_USER (aliyun account name or ram user@alias)}"
PASS="${ACR_PASSWORD:?set ACR_PASSWORD (ACR fixed registry password)}"
TAG="${TAG:-0.3.0}"

echo "== login $REG =="
printf '%s' "$PASS" | docker login "$REG" -u "$USER" --password-stdin

echo "== tag + push images =="
for pair in "mega2:local:mega2" "scorpiofs:runtime:scorpiofs"; do
  src="${pair%%:*:*}"; rest="${pair#*:}"; name="${rest%%:*}"
  full="$REG/$NS/$name:$TAG"
  echo "-- $src -> $full"
  docker tag "$src" "$full"
  docker push "$full"
done

echo "== done =="
docker images | grep "$REG/$NS" || true
