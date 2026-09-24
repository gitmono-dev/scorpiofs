#!/usr/bin/env bash
# bench/workload/seed-git.sh — 把与 seed-mono.sh 完全相同的过滤树 push 到 gitea，
# 保证 git 对照侧输入一致。
# 用法: seed-git.sh --tree /path/filtered --repo <name> [--gitea-org bench]
# 前置: gitea 上已建 org；token 在 $GITEA_TOKEN（init-runner-git.sh 里创建）。
set -euo pipefail
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=../bin/common.sh
source "$SELF_DIR/../bin/common.sh"
TREE=""; REPO=""; ORG="${GITEA_ORG:-bench}"
while [ $# -gt 0 ]; do case "$1" in
  --tree) TREE="$2"; shift 2;; --repo) REPO="$2"; shift 2;; --gitea-org) ORG="$2"; shift 2;;
  *) echo "unknown arg $1" >&2; exit 1;;
esac; done
[ -d "$TREE" ] && [ -n "$REPO" ] || { echo "usage: $0 --tree DIR --repo NAME" >&2; exit 1; }
[ -n "$GITEA_TOKEN" ] || { echo "GITEA_TOKEN required" >&2; exit 1; }
git_id

AUTH="$GITEA_USER:$GITEA_TOKEN"
# gitea host: strip scheme from $GITEA
GHOST="${GITEA#http://}"; GHOST="${GITEA#https://}"
REMOTE="http://$AUTH@$GHOST/$ORG/$REPO.git"

# 幂等建 repo（gitea API）
curl -fsS -X POST "http://$AUTH@$GHOST/api/v1/orgs/$ORG/repos" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"$REPO\",\"private\":false,\"auto_init\":false}" >/dev/null 2>&1 || true

STAGE="/tmp/git-seed-$REPO"
rm -rf "$STAGE"
git clone -q "http://$AUTH@$GHOST/$ORG/$REPO.git" "$STAGE" 2>/dev/null \
  || { mkdir -p "$STAGE"; ( cd "$STAGE" && git init -q -b main ); }

# 同步树内容（镜像：删除已删项）
rsync -a --delete --exclude .git "$TREE/" "$STAGE/"
git -C "$STAGE" add -A
git -C "$STAGE" commit -qm "seed $REPO ($(find "$TREE" -type f | wc -l) files)" || true
git -C "$STAGE" push -q "http://$AUTH@$GHOST/$ORG/$REPO.git" HEAD:refs/heads/main

remote_tip=$(git ls-remote "http://$AUTH@$GHOST/$ORG/$REPO.git" refs/heads/main | cut -f1)
echo "GITEA SEED OK: $ORG/$REPO at $remote_tip"
