#!/usr/bin/env bash
# bench/cases/exp3.sh — 实验三：大仓库性能（synth-100k / synth-200k / linux-kernel）
# 用法: bash cases/exp3.sh <case> ...   case ∈ init|read|multi|switch|concur|soak|all
# 前置: gen-repo.sh 生成的仓库已 seed-mono.sh 导入 mega2（REPO_NAME 对应 /project 子树）
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=../bin/common.sh
source "$HERE/../bin/common.sh"

REPO_NAME="${REPO_NAME:-synth100k}"
ROUNDS="${ROUNDS:-3}"           # 大仓实验默认 3 轮
TREE="${TREE:-$(cat "$RESULTS_DIR/last-filtered-tree.txt" 2>/dev/null || echo "")}"
BR="bench3-$(date +%s)"
B="$WORK/exp3"
MONO_MAIN="$B/mono-main"
WT="$B/mono-wt"

fresh_mono() {
  detach_mount "$WT" 2>/dev/null
  rm -rf "$MONO_MAIN" "$WT"
  libra_env "$LIBRA" clone -q -b main --no-checkout "$M2/project" "$MONO_MAIN" >/dev/null 2>&1
  libra "$MONO_MAIN" config set user.name bench >/dev/null
  libra "$MONO_MAIN" config set user.email bench@gitmono.local >/dev/null
  libra "$MONO_MAIN" worktree add --backend scorpiofs -b "$BR" "$WT" >/dev/null 2>&1 \
    && [ -f "$WT/.libra/scorpiofs_mount_id" ]
}
mono_store_bytes() { # mount + store 总量
  local m s=0 d
  m=$(du_bytes "$WT")
  for d in "$HOME/.scorpio"* "$HOME/.cache/scorpiofs" /var/lib/scorpiofs; do
    [ -d "$d" ] && s=$((s + $(du_bytes "$d")))
  done
  echo $((m + s))
}

# ---------- E3-INIT ----------
case_init() {
  local r t0 t1
  for ((r = 1; r <= ROUNDS; r++)); do
    rm -rf "$B/git-full-$r"
    cold_cache; t0=$(now_ms)
    git clone -q "$M2/project" "$B/git-full-$r"
    t1=$(now_ms)
    record E3 INIT git "$r" init_ms $((t1 - t0)) "{\"repo\":\"$REPO_NAME\",\"variant\":\"full\"}"
    record E3 INIT git "$r" disk_bytes "$(du_bytes "$B/git-full-$r")" "{}"
  done
  for ((r = 1; r <= ROUNDS; r++)); do
    rm -rf "$B/git-shallow-$r"
    cold_cache; t0=$(now_ms)
    git clone -q --depth=1 "$M2/project" "$B/git-shallow-$r" 2>/dev/null \
      || echo "  (mega2 git endpoint: shallow clone unsupported?)" >&2
    t1=$(now_ms)
    record E3 INIT git "$r" init_ms $((t1 - t0)) "{\"repo\":\"$REPO_NAME\",\"variant\":\"depth1\"}"
  done
  for ((r = 1; r <= ROUNDS; r++)); do
    detach_mount "$WT" 2>/dev/null; rm -rf "$MONO_MAIN" "$WT"
    cold_cache; t0=$(now_ms)
    fresh_mono
    t1=$(now_ms)
    record E3 INIT mono "$r" init_ms $((t1 - t0)) "{\"repo\":\"$REPO_NAME\",\"variant\":\"attach\"}"
    record E3 INIT mono "$r" disk_bytes "$(mono_store_bytes)" "{}"
  done
  rm -rf "$B"/git-full-* "$B"/git-shallow-*
}

# ---------- E3-READ ----------
case_read() {
  [ -d "$TREE" ] || { echo "TREE required" >&2; exit 1; }
  local LIST="$RESULTS_DIR/read-list-100-$REPO_NAME.txt"
  [ -f "$LIST" ] || ( cd "$TREE" && find . -type f | sed 's|^\./||' \
      | sort | awk 'NR%211==0' | head -100 > "$LIST" )
  fresh_mono || return 1
  sleep 2
  local r t0 t1
  for ((r = 1; r <= ROUNDS; r++)); do
    cold_cache; t0=$(now_ms)
    while IFS= read -r f; do cat "$WT/$f" > /dev/null 2>&1 || true; done < "$LIST"
    t1=$(now_ms)
    record E3 READ mono "$r" read_ms $((t1 - t0)) "{\"cold\":true}"
  done
  for ((r = 1; r <= ROUNDS; r++)); do
    t0=$(now_ms)
    while IFS= read -r f; do cat "$WT/$f" > /dev/null 2>&1 || true; done < "$LIST"
    t1=$(now_ms)
    record E3 READ mono "$r" read_ms $((t1 - t0)) "{\"cold\":false}"
  done
}

# ---------- E3-MULTI：多工作区 ×5（核心赢面） ----------
case_multi() {
  local N="${MULTI_N:-5}" r i t0 t1 gwork mwork
  # --- git: 主 clone + worktree add ×N ---
  rm -rf "$B/multi-git"
  cold_cache; t0=$(now_ms)
  git clone -q "$M2/project" "$B/multi-git"
  for ((i = 1; i <= N; i++)); do
    git -C "$B/multi-git" worktree add -q "$B/multi-git-wt$i" -b "wt$i" 2>/dev/null
  done
  t1=$(now_ms)
  gwork=$(du_bytes "$B/multi-git")
  for ((i = 1; i <= N; i++)); do gwork=$((gwork + $(du_bytes "$B/multi-git-wt$i"))); done
  record E3 MULTI git 1 multi_ms $((t1 - t0)) "{\"worktrees\":$N}"
  record E3 MULTI git 1 disk_bytes "$gwork" "{\"worktrees\":$N}"
  rm -rf "$B/multi-git" "$B"/multi-git-wt*
  # --- mono: attach + chain fork ×N（零拷贝） ---
  detach_mount "$WT" 2>/dev/null; rm -rf "$MONO_MAIN" "$WT" "$B"/mono-fork-*
  cold_cache; t0=$(now_ms)
  fresh_mono || return 1
  for ((i = 1; i <= N; i++)); do
    libra "$WT" fork -b "fork$i-$ROUND" "$B/mono-fork-$i" >/dev/null 2>&1
  done
  t1=$(now_ms)
  mwork=$(mono_store_bytes)
  for ((i = 1; i <= N; i++)); do mwork=$((mwork + $(du_bytes "$B/mono-fork-$i"))); done
  record E3 MULTI mono 1 multi_ms $((t1 - t0)) "{\"worktrees\":$N,\"note\":\"attach+chain forks\"}"
  record E3 MULTI mono 1 disk_bytes "$mwork" "{\"worktrees\":$N,\"note\":\"mount+store+fork uppers\"}"
  rm -rf "$MONO_MAIN" "$WT" "$B"/mono-fork-*
}

# ---------- E3-SWITCH：基线切换（10% 变更平行分支） ----------
case_switch() {
  # 造 10% 变更的平行基线（git 分支 + mega2 新 revision，同一改动）
  local CHANGE_MARK="switch-bench-$(date +%s)"
  local r t0 t1 new_rev
  # git 侧: 在 $B/sw-git 上建平行分支
  rm -rf "$B/sw-git"
  git clone -q "$M2/project" "$B/sw-git"
  ( cd "$B/sw-git" \
    && git checkout -q -b variant \
    && find . -type f -name '*.rs' -o -name '*.py' -o -name '*.c' 2>/dev/null | head -10000 \
       | awk 'NR%10==0' | while IFS= read -r f; do echo "$CHANGE_MARK" >> "$f"; done \
    && git add -A && git commit -qm "variant baseline (10% files)" \
    && git push -q origin variant )
  for ((r = 1; r <= ROUNDS; r++)); do
    ( cd "$B/sw-git" && git checkout -q main )
    cold_cache; t0=$(now_ms)
    ( cd "$B/sw-git" && git checkout -q variant )
    t1=$(now_ms)
    record E3 SWITCH git "$r" switch_ms $((t1 - t0)) "{}"
  done
  # mono 侧: 同一改动 push 成新 revision，refresh 计时
  rm -rf "$B/sw-mono-stage"
  git clone -q "$M2/project" "$B/sw-mono-stage"
  ( cd "$B/sw-mono-stage" \
    && git checkout -q -B variant origin/main \
    && find . -type f -name '*.rs' -o -name '*.py' -o -name '*.c' 2>/dev/null | head -10000 \
       | awk 'NR%10==0' | while IFS= read -r f; do echo "$CHANGE_MARK" >> "$f"; done \
    && git add -A && git commit -qm "variant baseline (10% files)" \
    && git push -q origin variant )
  new_rev=$(git ls-remote "$M2/project" refs/heads/variant | cut -f1)
  fresh_mono || return 1
  sleep 2
  for ((r = 1; r <= ROUNDS; r++)); do
    cold_cache; t0=$(now_ms)
    curl -sS --noproxy '*' -X POST "$API/worktrees/$(mount_id "$WT")/refresh" \
      -H 'Content-Type: application/json' -d "{\"revision\": \"$new_rev\"}" >/dev/null
    t1=$(now_ms)
    record E3 SWITCH mono "$r" switch_ms $((t1 - t0)) "{\"variant\":\"refresh\",\"rev\":\"${new_rev:0:8}\"}"
  done
  rm -rf "$B/sw-git" "$B/sw-mono-stage" "$MONO_MAIN" "$WT"
}

# ---------- E3-CONCUR：4 并发 agent ----------
case_concur() {
  fresh_mono || return 1
  sleep 2
  echo "  (invoking bench-agent.sh ×4 in parallel; ensure opencode configured)"
  local pids=() i
  for ((i = 1; i <= 4; i++)); do
    ( WT="$WT" AGENT_ROUND="$i" bash "$HERE/../bin/bench-agent.sh" T1 mono ) &
    pids+=($!)
  done
  wait "${pids[@]}"
  rm -rf "$MONO_MAIN" "$WT"
}

# ---------- E3-SOAK：30 分钟稳定性 ----------
case_soak() {
  local DUR="${SOAK_MIN:-30}"
  fresh_mono || return 1
  sleep 2
  local mid; mid=$(mount_id "$WT")
  echo "soak ${DUR}min: find/status/sync loop on $WT"
  local end=$((SECONDS + DUR * 60)) i=0
  while [ $SECONDS -lt $end ]; do
    i=$((i + 1))
    find "$WT" \( -name .libra -prune \) -o -type f -print > /dev/null 2>&1
    libra "$WT" status > /dev/null 2>&1
    if [ $((i % 20)) -eq 0 ]; then
      echo "bench-soak $i" >> "$WT/soak-notes.txt"
      libra "$WT" sync -m "soak $i" > /dev/null 2>&1
    fi
    # daemon 进程 fd/RSS 采样（与 prometheus 互补）
    local pid; pid=$(pgrep -x scorpio | head -1)
    if [ -n "$pid" ]; then
      local fds rss
      fds=$(ls /proc/$pid/fd 2>/dev/null | wc -l)
      rss=$(awk '/VmRSS/{print $2}' /proc/$pid/status 2>/dev/null)
      record E3 SOAK mono 1 sample_bytes "$rss" "{\"iter\":$i,\"fds\":$fds,\"metric\":\"rss_kb\"}"
    fi
    sleep 5
  done
  echo "soak done ($i iters)"
}

# ---------- dispatch ----------
CASES=("$@"); [ ${#CASES[@]} -eq 0 ] && CASES=(all)
for c in "${CASES[@]}"; do
  case "$c" in
    all) for f in init read multi switch concur soak; do echo "### E3-$f"; "case_$f"; done ;;
    init|read|multi|switch|concur|soak) echo "### E3-$c"; "case_$c" ;;
    *) echo "unknown case: $c" >&2; exit 1 ;;
  esac
done
echo "E3 done → $RESULTS_DIR"
