#!/usr/bin/env bash
# bench/cases/exp2.sh — 实验二：多仓库关联（1 公共库 + 5 服务）
# 用法: bash cases/exp2.sh <case> ...   case ∈ fetch|upgrade|partial|all
# 前置: gen-multirepo.sh + seed-git.sh(每个 repo) + seed-mono.sh 已完成；
#       gitea org 下有 common/svc-a..e（submodule 相对路径）。
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=../bin/common.sh
source "$HERE/../bin/common.sh"

ROUNDS="${ROUNDS:-5}"
SVCS="${SVCS:-5}"
ORG="${GITEA_ORG:-bench}"
BR="bench2-$(date +%s)"
GIT_BASE="$WORK/exp2"
MONO_MAIN="$WORK/exp2-mono-main"
MONO_WT="$WORK/exp2-mono-wt"

ghost() { local g="${GITEA#http://}"; echo "${g#https://}"; }
gurl() { echo "http://$GITEA_USER:$GITEA_TOKEN@$(ghost)/$ORG/$1.git"; }
fresh_mono() {
  detach_mount "$MONO_WT" 2>/dev/null
  rm -rf "$MONO_MAIN" "$MONO_WT"
  libra_env "$LIBRA" clone -q -b main --no-checkout "$M2/project" "$MONO_MAIN" >/dev/null 2>&1
  libra "$MONO_MAIN" config set user.name bench >/dev/null
  libra "$MONO_MAIN" config set user.email bench@gitmono.local >/dev/null
}
attach_mono() {
  libra "$MONO_MAIN" worktree add --backend scorpiofs -b "$BR-$ROUND" "$MONO_WT" >/dev/null 2>&1 \
    && [ -f "$MONO_WT/.libra/scorpiofs_mount_id" ]
}

# ---------- E2-FETCH ----------
case_fetch() {
  local r t0 t1 out
  for ((r = 1; r <= ROUNDS; r++)); do
    rm -rf "$GIT_BASE/fetch-$r"
    cold_cache; t0=$(now_ms)
    git clone -q --recurse-submodules "$(gurl svc-a)" "$GIT_BASE/fetch-$r" 2>/dev/null
    t1=$(now_ms)
    record E2 FETCH git "$r" fetch_ms $((t1 - t0)) "{\"note\":\"svc-a + submodule common\"}"
  done
  for ((r = 1; r <= ROUNDS; r++)); do
    fresh_mono
    cold_cache; t0=$(now_ms)
    attach_mono
    t1=$(now_ms)
    record E2 FETCH mono "$r" fetch_ms $((t1 - t0)) "{\"note\":\"attach monorepo (all services)\"}"
  done
  rm -rf "$GIT_BASE"/fetch-* "$MONO_MAIN" "$MONO_WT"
}

# ---------- E2-UPGRADE ----------
# git: common 改版 → 5 个服务各 submodule update --remote + commit + push
# mono: common/ 改版 → 一次 libra sync
case_upgrade() {
  local r t0 t1 steps
  for ((r = 1; r <= ROUNDS; r++)); do
    local VER="1.$((10 + r))"
    # --- git 侧 ---
    rm -rf "$GIT_BASE/up"
    t0=$(now_ms)
    git clone -q "$(gurl common)" "$GIT_BASE/up/common"
    sed -i "s/pub const VERSION: &str = \".*\";/pub const VERSION: \&str = \"$VER\";/" \
      "$GIT_BASE/up/common/src/api.rs"
    ( cd "$GIT_BASE/up/common" && git add -A && git commit -qm "common v$VER" \
      && git push -q origin main )
    steps=2   # commit + push
    for name in $(printf 'svc-%c ' $(seq 97 $((96 + SVCS)))); do
      git clone -q --recurse-submodules "$(gurl "$name")" "$GIT_BASE/up/$name" 2>/dev/null
      ( cd "$GIT_BASE/up/$name" \
        && git -c protocol.file.allow=always submodule update --remote common \
        && git add -A && git commit -qm "bump common -> v$VER" \
        && git push -q origin HEAD:refs/heads/main )
      steps=$((steps + 4))  # clone+update+commit+push
    done
    t1=$(now_ms)
    record E2 UPGRADE git "$r" upgrade_ms $((t1 - t0)) "{\"steps\":$steps,\"ver\":\"$VER\"}"
    # --- mono 侧 ---
    fresh_mono; attach_mono || { echo "attach failed r$r" >&2; continue; }
    t0=$(now_ms)
    sed -i "s/pub const VERSION: &str = \".*\";/pub const VERSION: \&str = \"$VER\";/" \
      "$MONO_WT/common/src/api.rs"
    libra "$MONO_WT" sync -m "common v$VER (atomic)" >/dev/null
    t1=$(now_ms)
    record E2 UPGRADE mono "$r" upgrade_ms $((t1 - t0)) "{\"steps\":2,\"ver\":\"$VER\"}"
  done
  rm -rf "$GIT_BASE/up" "$MONO_MAIN" "$MONO_WT"
}

# ---------- E2-PARTIAL：只要 svc-a ----------
case_partial() {
  local r t0 t1
  for ((r = 1; r <= ROUNDS; r++)); do
    rm -rf "$GIT_BASE/partial-$r"
    cold_cache; t0=$(now_ms)
    git clone -q --recurse-submodules --depth 1 "$(gurl svc-a)" "$GIT_BASE/partial-$r" 2>/dev/null \
      || git clone -q --recurse-submodules "$(gurl svc-a)" "$GIT_BASE/partial-$r" 2>/dev/null
    t1=$(now_ms)
    record E2 PARTIAL git "$r" partial_ms $((t1 - t0)) "{\"note\":\"best-effort shallow\"}"
  done
  for ((r = 1; r <= ROUNDS; r++)); do
    fresh_mono
    cold_cache; t0=$(now_ms)
    attach_mono
    head -5 "$MONO_WT/svc-a/src/main.rs" > /dev/null 2>&1
    t1=$(now_ms)
    record E2 PARTIAL mono "$r" partial_ms $((t1 - t0)) "{\"note\":\"attach + svc-a first read\"}"
  done
  rm -rf "$GIT_BASE"/partial-* "$MONO_MAIN" "$MONO_WT"
}

# ---------- dispatch ----------
CASES=("$@"); [ ${#CASES[@]} -eq 0 ] && CASES=(all)
for c in "${CASES[@]}"; do
  case "$c" in
    all) for f in fetch upgrade partial; do echo "### E2-$f"; "case_$f"; done ;;
    fetch|upgrade|partial) echo "### E2-$c"; "case_$c" ;;
    *) echo "unknown case: $c" >&2; exit 1 ;;
  esac
done
echo "E2 done → $RESULTS_DIR"
