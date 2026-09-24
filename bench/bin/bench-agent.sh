#!/usr/bin/env bash
# bench/bin/bench-agent.sh — opencode 任务驱动 + 计时 + 判分。
# 用法: bench-agent.sh <T1|T2|T3|T4|T5> <git|mono>
# 环境变量: WT(git 工作区或 mono 挂载点) AGENT_MODEL ROUNDS(默认5)
# 前置: 对应工作区已就绪（clone 完成或 attach 完成）；opencode 可用且模型已配置。
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=common.sh
source "$HERE/common.sh"

TASK="${1:?T1|T2|T3|T4|T5}"; SIDE="${2:?git|mono}"
WT="${WT:?WT env required (workdir or mountpoint)}"
ROUNDS="${ROUNDS:-5}"
EXP="${EXP:-E1}"
OUT="$WORK/agent-out"
mkdir -p "$OUT"

# ---------- 任务定义（prompt + judge） ----------
prompt_T1='在 src/ 下任选一个 Rust 源文件，文件末尾新增函数 `pub fn bench_t1_marker(x: u64) -> u64 { x.wrapping_mul(7) }`（若全仓无 .rs 文件则选任一 .py 文件加 `def bench_t1_marker(x): return x * 7`）。只加这一个函数，不要改其他内容。'
judge_T1() { # 0=pass
  grep -rq "bench_t1_marker" "$WT" --include='*.rs' --include='*.py' 2>/dev/null
}
prompt_T2='统计当前工作区中标识符 `fn `（rust 函数定义）出现的总次数，把数字写入工作区根目录的 grep-count.txt，只写数字。若仓库无 .rs 文件则统计 `def ` 并写入。'
judge_T2() {
  local expect actual
  expect=$(grep -r "fn " "$TREE_FOR_COUNT" --include='*.rs' 2>/dev/null | wc -l)
  [ "$expect" -gt 0 ] || { expect=$(grep -r "def " "$TREE_FOR_COUNT" --include='*.py' 2>/dev/null | wc -l); }
  actual=$(cat "$WT/grep-count.txt" 2>/dev/null | tr -dc '0-9')
  [ -n "$actual" ] && [ "$actual" = "$expect" ] && { echo "expect=$expect actual=$actual" >&2; return 0; }
  echo "MISMATCH expect=$expect actual=$actual" >&2; return 1
}
prompt_T3='把工作区中所有文件里的字符串 `synth file` 替换为 `synth FILE`（仅这个字符串，保持其他内容不变）。'
judge_T3() {
  local before after
  before=$(grep -r "synth file" "$TREE_FOR_COUNT" 2>/dev/null | wc -l)
  after=$(grep -r "synth file" "$WT" 2>/dev/null | wc -l)
  [ "$before" -gt 0 ] && [ "$after" -eq 0 ] # 基线树必须含该串，替换后归零
}
prompt_T4='把公共库 common 的版本号 VERSION 从当前值改为 9.9.9，并确保所有服务（svc-*）对 common 的依赖引用同步到该版本。完成后不需要运行任何服务。'
judge_T4() {
  grep -rq "9.9.9" "$WT/common" 2>/dev/null \
    && ! grep -rq "VERSION.*1\.0" "$WT/common/src/api.rs" 2>/dev/null
}
prompt_T5='把 common 库中的函数 dep 重命名为 dep_v2，并同步更新所有 svc-* 中对该函数的引用，保持全部服务代码一致可用。'
judge_T5() {
  ! grep -rq "pub fn dep(" "$WT/common/src/api.rs" 2>/dev/null \
    && grep -rq "dep_v2" "$WT/common/src/api.rs" 2>/dev/null \
    && ! grep -rn "shim::dep(" "$WT" --include='*.rs' 2>/dev/null | grep -v dep_v2 >/dev/null
}
PROMPT_VAR="prompt_$TASK"; JUDGE_VAR="judge_$TASK"
[ -n "${!PROMPT_VAR:-}" ] || { echo "unknown task $TASK" >&2; exit 1; }

# T2/T3 判分需要参考树（未改动基线）；mono 侧用 lower 对应的过滤树，git 侧用 clone 基线
TREE_FOR_COUNT="${TREE_FOR_COUNT:-$TREE}"
export TREE_FOR_COUNT
export -f judge_T1 judge_T2 judge_T3 judge_T4 judge_T5 2>/dev/null || true

# ---------- 运行 ----------
MODEL_ARGS=()
[ -n "$AGENT_MODEL" ] && MODEL_ARGS=(--model "$AGENT_MODEL")

for ((r = 1; r <= ROUNDS; r++)); do
  # 每轮还原基线（避免上一轮改动干扰判分）：git 侧 reset --hard；mono 侧由调用方决定
  if [ "$SIDE" = git ] && git -C "$WT" rev-parse >/dev/null 2>&1; then
    git -C "$WT" checkout -q -- . 2>/dev/null; git -C "$WT" clean -qfd 2>/dev/null
    git -C "$WT" reset -q --hard origin/main 2>/dev/null || true
  fi
  rm -f "$WT/grep-count.txt"
  cold_cache
  local_rc=0
  t0=$(now_ms)
  ( cd "$WT" && $OPENCODE run "${MODEL_ARGS[@]}" "${!PROMPT_VAR}" ) \
    > "$OUT/$TASK-$SIDE-r$r.log" 2>&1 || local_rc=$?
  t1=$(now_ms)
  record "$EXP" "A$TASK" "$SIDE" "$r" agent_ms $((t1 - t0)) "{\"rc\":$local_rc}"
  if "judge_$TASK"; then
    record "$EXP" "A$TASK" "$SIDE" "$r" judge_pass 1 "{}"
  else
    record "$EXP" "A$TASK" "$SIDE" "$r" judge_pass 0 "{\"log\":\"$OUT/$TASK-$SIDE-r$r.log\"}"
  fi
done
echo "agent $TASK $SIDE done → $RESULTS_DIR"
