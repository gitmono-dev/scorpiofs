#!/usr/bin/env bash
# bench/workload/seed-mono.sh — 把评测树导入 mega2（与 demo.sh 已验证的 seed 路径一致）。
# 用法:
#   seed-mono.sh --src /tmp/synth-100k --name synth100k [--filter-kb 200]
# 步骤: clone mega2 monorepo → 拷贝源码到 /project/<name> → commit → push → 校验 tip。
# 说明: >--filter-kb 的文件被剔除（mega2 的 1MB LFS 阈值留余量）；
#       评测两侧必须用同一棵过滤后的树（先跑本脚本，再用 --tree 复用）。
set -euo pipefail
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=../bin/common.sh
source "$SELF_DIR/../bin/common.sh"
SRC=""; NAME=""; FILTER_KB=200; TREE=""; REUSE=0
while [ $# -gt 0 ]; do case "$1" in
  --src) SRC="$2"; shift 2;; --name) NAME="$2"; shift 2;;
  --filter-kb) FILTER_KB="$2"; shift 2;; --tree) TREE="$2"; REUSE=1; shift 2;;
  *) echo "unknown arg $1" >&2; exit 1;;
esac; done
TREE="${TREE:-/tmp/filtered-${NAME:-anon}}"
require_env
git_id
STAGE="/tmp/mono-seed-stage"

if [ "$REUSE" = 1 ]; then
  [ -d "$TREE" ] || { echo "--tree $TREE not found" >&2; exit 1; }
  echo "reusing filtered tree: $TREE"
  rm -rf "$STAGE"; git clone -q "$M2/project" "$STAGE"
  FILES=$(find "$TREE" -type f | wc -l)
else
  [ -d "$SRC" ] || { echo "--src $SRC not found" >&2; exit 1; }
  [ -n "$NAME" ] || { echo "--name required" >&2; exit 1; }
  echo "building filtered tree (> ${FILTER_KB}KB dropped) ..."
  rm -rf "$TREE"; mkdir -p "$TREE"
  ( cd "$SRC" && git archive HEAD ) | tar -x -C "$TREE"
  find "$TREE" -type f -size +"${FILTER_KB}k" -delete
  FILES=$(find "$TREE" -type f | wc -l)
  SIZE=$(du -sh "$TREE" | cut -f1)
  echo "filtered tree: $FILES files, $SIZE"

  rm -rf "$STAGE"; git clone -q "$M2/project" "$STAGE"
  rm -rf "$STAGE/$NAME"
  cp -a "$TREE" "$STAGE/$NAME"
  git -C "$STAGE" add -A
  git -C "$STAGE" -c user.name=bench -c user.email=bench@gitmono.local \
      commit -qm "seed $NAME (files=$FILES)" || true
  git -C "$STAGE" push -q origin HEAD:refs/heads/main
fi

tip=$(git -C "$STAGE" rev-parse HEAD)
remote_tip=$(git ls-remote "$M2/project" refs/heads/main | cut -f1)
if [ "$tip" = "$remote_tip" ]; then
  echo "SEED OK: /project at $remote_tip"
  echo "$remote_tip" > "$RESULTS_DIR/seed-tip.txt"
else
  echo "SEED FAILED: local=$tip remote=$remote_tip" >&2; exit 1
fi
echo "$TREE" > "$RESULTS_DIR/last-filtered-tree.txt"
