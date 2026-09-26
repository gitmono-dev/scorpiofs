#!/usr/bin/env bash
# bench/cases/exp1-status-duel.sh — E1-STATUS-SCALE
# git status（本地 bare + clone，git 最佳条件）vs ScorpioFS fast-path status。
# 变量：树规模 SIZE × 改动量 CHANGES；git 侧 fsmonitor on/off 双线。
# 用法: SIZE=2000 bash cases/exp1-status-duel.sh
#       SIZE=20000 ROUNDS=3 bash cases/exp1-status-duel.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=../bin/common.sh
source "$HERE/../bin/common.sh"

SIZE="${SIZE:-2000}"
ROUNDS="${ROUNDS:-5}"
CHANGE_COUNTS="${CHANGE_COUNTS:-0 1 100}"
REPO_NAME="statusduel$SIZE"
TREE="${TREE:-/tmp/statusduel-tree-$SIZE}"
GBARE="/tmp/statusduel-git-$SIZE.git"
GWT="$WORK/statusduel-git-$SIZE"
MONO_MAIN="$WORK/statusduel-mono-main-$SIZE"
MONO_WT="$WORK/statusduel-mono-wt-$SIZE"
MARK="// status-duel-marker"
if [ "${COLD:-0}" = 1 ]; then COLDJSON=true; else COLDJSON=false; fi

# ---------- setup ----------
echo "== [1/3] workload tree ($SIZE files) =="
if [ -d "$TREE" ]; then
  echo "  reusing $TREE"
else
  bash "$HERE/../workload/gen-repo.sh" --out "$TREE" --files "$SIZE" --avg-size 3 --seed 11 >/dev/null
fi

echo "== [2/3] git side: bare (local disk) + clone =="
if [ ! -d "$GBARE" ]; then
  git init -q --bare -b main "$GBARE"
  ( cd "$TREE" && git push -q "$GBARE" main:main ) || {
    git -C "$TREE" remote add bench "$GBARE" 2>/dev/null
    git -C "$TREE" push -q bench main:main
  }
fi
rm -rf "$GWT"; git clone -q "$GBARE" "$GWT"
[ -n "$(ls -A "$GWT" 2>/dev/null)" ] || { echo "git clone produced empty worktree (bare HEAD broken)" >&2; exit 1; }
# git 最优配置（报告附录）
git -C "$GWT" config core.untrackedCache true
git -C "$GWT" config fetch.parallel 8

echo "== [3/3] mono side: seed (before attach!) + attach =="
bash "$HERE/../workload/seed-mono.sh" --src "$TREE" --name "$REPO_NAME" >/dev/null || {
  echo "seed failed" >&2; exit 1; }
detach_mount "$MONO_WT" 2>/dev/null; rm -rf "$MONO_MAIN" "$MONO_WT"
libra_env "$LIBRA" clone -q -b main --no-checkout "$M2/project" "$MONO_MAIN" >/dev/null 2>&1
libra "$MONO_MAIN" config set user.name bench >/dev/null
libra "$MONO_MAIN" config set user.email bench@gitmono.local >/dev/null
libra "$MONO_MAIN" worktree add --backend scorpiofs -b "duel-$SIZE" "$MONO_WT" >/dev/null 2>&1 \
  || { echo "attach failed" >&2; exit 1; }
sleep 3

# ---------- helpers ----------
apply_changes() { # <dir> <n>：给 n 个 .rs 文件追加 marker 行
  local dir="$1" n="$2" i=0 f
  [ "$n" = 0 ] && return 0
  while IFS= read -r f; do
    echo "$MARK $i" >> "$dir/$f"
    i=$((i + 1))
    [ $i -ge "$n" ] && break
  done < <( cd "$dir" && find . -name '*.rs' -type f | sort | sed 's|^\./||' | head -"$n" )
}
clear_changes() { # git 侧用 checkout；mono 侧用 marker 删除
  local dir="$1"
  if git -C "$dir" rev-parse >/dev/null 2>&1; then
    git -C "$dir" checkout -q -- . 2>/dev/null
  fi
  grep -rlF "$MARK" "$dir" 2>/dev/null | while IFS= read -r f; do
    sed -i "/status-duel-marker/d" "$f"
  done
}

# ---------- measurement ----------
for N in $CHANGE_COUNTS; do
  echo "== changes=$N =="
  for ((r = 1; r <= ROUNDS; r++)); do
    # --- git (fsmonitor off) ---
    clear_changes "$GWT"; git -C "$GWT" config core.fsmonitor false
    apply_changes "$GWT" "$N"
    [ "${COLD:-0}" = 1 ] && cold_cache
    local_t0=$(now_ms)
    ( cd "$GWT" && git status --porcelain >/dev/null )
    local_t1=$(now_ms)
    record E1 STATUS-SCALE git-off "$r" status_ms $((local_t1 - local_t0)) \
      "{\"size\":$SIZE,\"changes\":$N,\"fsmonitor\":false,\"cold\":$COLDJSON}"
    # --- git (fsmonitor on) ---
    clear_changes "$GWT"; git -C "$GWT" config core.fsmonitor true
    git -C "$GWT" update-index --fsmonitor 2>/dev/null || true
    apply_changes "$GWT" "$N"
    [ "${COLD:-0}" = 1 ] && cold_cache
    local_t0=$(now_ms)
    ( cd "$GWT" && git status --porcelain >/dev/null )
    local_t1=$(now_ms)
    record E1 STATUS-SCALE git-on "$r" status_ms $((local_t1 - local_t0)) \
      "{\"size\":$SIZE,\"changes\":$N,\"fsmonitor\":true,\"cold\":$COLDJSON}"
    # --- mono fast path ---
    clear_changes "$MONO_WT"
    [ "${COLD:-0}" = 1 ] && cold_cache
    apply_changes "$MONO_WT" "$N"

    local_t0=$(now_ms)
    libra "$MONO_WT" status >/dev/null 2>&1
    local_t1=$(now_ms)
    record E1 STATUS-SCALE mono "$r" status_ms $((local_t1 - local_t0)) \
      "{\"size\":$SIZE,\"changes\":$N,\"path\":\"fastpath\",\"cold\":$COLDJSON}"
    # 正确性抽查：改动数应被两侧一致感知（仅 changes=1 时）
    if [ "$N" = 1 ] && [ "$r" = 1 ]; then
      gcount=$(cd "$GWT" && git status --porcelain | wc -l)
      mcount=$(libra "$MONO_WT" status 2>/dev/null | grep -cE "^\s+(modified|new file|deleted):")
      echo "  correctness: git=$gcount mono=$mcount (expect both >=1)"
    fi
  done
done
clear_changes "$GWT"; clear_changes "$MONO_WT"
echo "E1-STATUS-SCALE done (size=$SIZE) → $RESULTS_DIR"
