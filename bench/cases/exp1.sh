#!/usr/bin/env bash
# bench/cases/exp1.sh — 实验一：普通项目对比（tokio / cpython / synth）
# 用法: bash cases/exp1.sh <case> ...   case ∈ ttfw|disk|read|status|commit|net|all
# 环境变量: REPO_NAME(默认 tokio) ROUNDS(默认5) TREE(过滤树, read 用) GITEA_REPO(可选对照)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=../bin/common.sh
source "$HERE/../bin/common.sh"

REPO_NAME="${REPO_NAME:-tokio}"
ROUNDS="${ROUNDS:-5}"
TREE="${TREE:-$(cat "$RESULTS_DIR/last-filtered-tree.txt" 2>/dev/null || echo "")}"
GIT_WT="$WORK/exp1-git"
MONO_MAIN="$WORK/exp1-mono-main"
MONO_WT="$WORK/exp1-mono-wt"
BR="bench-$(date +%s)"

fresh_dirs() { rm -rf "$GIT_WT" "$MONO_MAIN" "$MONO_WT"; mkdir -p "$WORK"; }
attach_mono() { # 照 demo.sh 已验证路径：clone --no-checkout → worktree add
  libra_env "$LIBRA" clone -q -b main --no-checkout "$M2/project" "$MONO_MAIN" >/dev/null 2>&1
  libra "$MONO_MAIN" config set user.name bench >/dev/null
  libra "$MONO_MAIN" config set user.email bench@gitmono.local >/dev/null
  libra "$MONO_MAIN" worktree add --backend scorpiofs -b "$BR-$ROUND" "$MONO_WT" >/dev/null 2>&1
  [ -f "$MONO_WT/.libra/scorpiofs_mount_id" ]
}
probe_first_write() { # 首写探测 = 第一个可写文件落盘
  local d="$1" f
  f="$d/.bench-probe-$ROUND"
  echo ok > "$f" && rm -f "$f"
}
export -f probe_first_write

# ---------- E1-TTFW ----------
case_ttfw() {
  require_env; fresh_dirs
  run_metric E1 TTFW git "$ROUNDS" ttfw bash -c '
    cold_cache
    t0=$(now_ms)
    git clone -q "'"$M2"'/project" "'"$GIT_WT"'"-$ROUND 2>/dev/null
    probe_first_write "'"$GIT_WT"'-$ROUND"
    t1=$(now_ms); echo "ttfw_ms $((t1-t0))"'
  run_metric E1 TTFW mono "$ROUNDS" ttfw bash -c '
    cold_cache
    t0=$(now_ms)
    attach_mono && probe_first_write "'"$MONO_WT"'"
    t1=$(now_ms); echo "ttfw_ms $((t1-t0))"'
  fresh_dirs
}

# ---------- E1-DISK ----------
case_disk() { # 每侧准备一次（非计时），测 du；3 个位置：clone / mount / store
  require_env; fresh_dirs
  git clone -q "$M2/project" "$GIT_WT"
  ROUND=1 attach_mono || { echo "attach failed" >&2; exit 1; }
  sleep 2 # mount ready
  local g m store total
  g=$(du_bytes "$GIT_WT")
  m=$(du_bytes "$MONO_WT")
  store=0
  for d in "$HOME/.scorpio"* "$HOME/.cache/scorpiofs" /var/lib/scorpiofs; do
    [ -d "$d" ] && store=$((store + $(du_bytes "$d")))
  done
  record E1 DISK git 1 disk_bytes "$g" "{\"repo\":\"$REPO_NAME\"}"
  record E1 DISK mono 1 disk_bytes "$((m + store))" "{\"mount\":$m,\"store\":$store,\"repo\":\"$REPO_NAME\"}"
  echo "git=$g mono_mount=$m mono_store=$store"
  fresh_dirs
}

# ---------- E1-READ ----------
case_read() {
  require_env
  [ -d "$TREE" ] || { echo "TREE (filtered tree) required for read list" >&2; exit 1; }
  local LIST="$RESULTS_DIR/read-list-100.txt"
  [ -f "$LIST" ] || ( cd "$TREE" && find . -type f | sed 's|^\./||' \
      | sort | awk 'NR%37==0' | head -100 > "$LIST" )
  [ "$(wc -l < "$LIST")" -ge 50 ] || { echo "read list too small" >&2; exit 1; }
  fresh_dirs
  git clone -q "$M2/project" "$GIT_WT"
  ROUND=1 attach_mono && sleep 2

  run_metric E1 READ git 1 read_ms bash -c '
    cold_cache; t0=$(now_ms)
    while IFS= read -r f; do cat "'"$GIT_WT"'/$f" > /dev/null 2>&1 || true; done < "'"$LIST"'"
    t1=$(now_ms); echo "read_ms $((t1-t0))"'
  run_metric E1 READ mono-cold 1 read_ms bash -c '
    cold_cache; t0=$(now_ms)
    while IFS= read -r f; do cat "'"$MONO_WT"'/$f" > /dev/null 2>&1 || true; done < "'"$LIST"'"
    t1=$(now_ms); echo "read_ms $((t1-t0))"'
  run_metric E1 READ mono-hot 1 read_ms bash -c '
    t0=$(now_ms)
    while IFS= read -r f; do cat "'"$MONO_WT"'/$f" > /dev/null 2>&1 || true; done < "'"$LIST"'"
    t1=$(now_ms); echo "read_ms $((t1-t0))"'
  fresh_dirs
}

# ---------- E1-STATUS ----------
case_status() {
  require_env; fresh_dirs
  git clone -q "$M2/project" "$GIT_WT"
  ROUND=1 attach_mono && sleep 2
  run_metric E1 STATUS git "$ROUNDS" status_ms bash -c '
    t0=$(now_ms)
    ( cd "'"$GIT_WT"'" && git status --porcelain > /dev/null )
    t1=$(now_ms); echo "status_ms $((t1-t0))"'
  # fsmonitor 变体
  git -C "$GIT_WT" config core.fsmonitor true
  git -C "$GIT_WT" update-index --fsmonitor 2>/dev/null || true
  run_metric E1 STATUS git-fsmonitor "$ROUNDS" status_ms bash -c '
    t0=$(now_ms)
    ( cd "'"$GIT_WT"'" && git status --porcelain > /dev/null )
    t1=$(now_ms); echo "status_ms $((t1-t0))"'
  git -C "$GIT_WT" config core.fsmonitor false
  run_metric E1 STATUS mono "$ROUNDS" status_ms bash -c '
    t0=$(now_ms); libra "'"$MONO_WT"'" status > /dev/null
    t1=$(now_ms); echo "status_ms $((t1-t0))"'
  fresh_dirs
}

# ---------- E1-COMMIT ----------
case_commit() {
  require_env; fresh_dirs
  git clone -q "$M2/project" "$GIT_WT"
  ROUND=1 attach_mono && sleep 2
  run_metric E1 COMMIT git "$ROUNDS" commit_ms bash -c '
    t0=$(now_ms)
    ( cd "'"$GIT_WT"'" \
      && echo "bench $ROUND $(date +%s)" >> bench-notes.txt \
      && git add -A && git commit -qm "bench $ROUND" && git push -q origin HEAD:refs/heads/main )
    t1=$(now_ms); echo "commit_ms $((t1-t0))"'
  run_metric E1 COMMIT mono "$ROUNDS" commit_ms bash -c '
    t0=$(now_ms)
    echo "bench $ROUND $(date +%s)" >> "'"$MONO_WT"'/bench-notes.txt"
    libra "'"$MONO_WT"'" sync -m "bench $ROUND" > /dev/null
    t1=$(now_ms); echo "commit_ms $((t1-t0))"'
  fresh_dirs
}

# ---------- E1-NET ----------
case_net() {
  require_env; fresh_dirs
  local IFACE="${IFACE:-eth0}"
  local b0 b1
  b0=$(net_bytes "$IFACE")
  git clone -q "$M2/project" "$GIT_WT"
  b1=$(net_bytes "$IFACE")
  record E1 NET git 1 net_bytes $((b1 - b0)) "{\"iface\":\"$IFACE\"}"
  detach_mount "$MONO_WT" 2>/dev/null; rm -rf "$MONO_MAIN" "$MONO_WT"
  b0=$b1
  ROUND=1 attach_mono && sleep 2
  # 触发全树元数据 + 抽样内容读取（与 git clone 提供的能力对齐：可浏览可读）
  find "$MONO_WT" \( -name .libra -prune \) -o -type f -print > /dev/null 2>&1
  b1=$(net_bytes "$IFACE")
  record E1 NET mono 1 net_bytes $((b1 - b0)) "{\"iface\":\"$IFACE\",\"note\":\"metadata+partial content\"}"
  fresh_dirs
}

# ---------- dispatch ----------
CASES=("$@"); [ ${#CASES[@]} -eq 0 ] && CASES=(all)
for c in "${CASES[@]}"; do
  case "$c" in
    all)    for f in ttfw disk read status commit net; do echo "### E1-$f"; "case_$f"; done ;;
    ttfw|disk|read|status|commit|net) echo "### E1-$c"; "case_$c" ;;
    *) echo "unknown case: $c (see TEST-PLAN)" >&2; exit 1 ;;
  esac
done
echo "E1 done → $RESULTS_DIR"
