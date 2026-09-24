#!/usr/bin/env bash
# bench/bin/common.sh — 共享库：source 本文件，不要执行。
# 约定：所有结果以 JSONL 行落盘 results/raw/<EXP>-<CASE>.jsonl
set -uo pipefail
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY no_proxy NO_PROXY all_proxy ALL_PROXY 2>/dev/null || true
export GIT_TERMINAL_PROMPT=0

BENCH_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="${RESULTS_DIR:-$BENCH_ROOT/results/raw}"
mkdir -p "$RESULTS_DIR"

# ---- 环境探测（可被环境变量覆盖；全部 export 供内层 bash -c 使用） ----
export M2="${M2:-http://127.0.0.1:19000}"
export GITEA="${GITEA:-http://127.0.0.1:30080}"           # gitea HTTP
export GITEA_USER="${GITEA_USER:-bench}"
export GITEA_TOKEN="${GITEA_TOKEN:-}"
export API="${API:-http://127.0.0.1:37251}/antares"       # scorpiofs daemon
export LIBRA="${LIBRA:-$HOME/.local/bin/libra}"
export SCORPIO="${SCORPIO:-$HOME/.local/bin/scorpio}"
export OPENCODE="${OPENCODE:-opencode}"
export AGENT_MODEL="${AGENT_MODEL:-}"                     # 传给 opencode 的 model，可空
export WORK="${WORK:-$HOME/bench-work}"                   # 工作区根（各实验子目录自建）
export RESULTS_DIR
mkdir -p "$WORK"

need() { command -v "$1" >/dev/null || { echo "missing: $1" >&2; exit 2; }; }
require_env() { # 用例前置检查
  curl -fsS --noproxy '*' "$M2/api/v1/latest-commit?path=/project" >/dev/null \
    || { echo "mega2 not reachable at $M2" >&2; exit 2; }
}

# ---- 计时 ----
now_ms() { date +%s%3N; }
# timed <label> <cmd...>：运行命令，回显 "<label>_ms <ms>"，返回命令退出码
timed() {
  local label="$1"; shift
  local t0=$(now_ms)
  "$@"; local rc=$?
  local t1=$(now_ms)
  echo "${label}_ms $((t1 - t0))"
  return $rc
}

# ---- 冷缓存（需 root 或 sudo 免密） ----
cold_cache() {
  sync
  if [ "$(id -u)" = "0" ]; then
    echo 3 > /proc/sys/vm/drop_caches
  else
    sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
  fi
}

# ---- JSONL 落盘 ----
# record <exp> <case> <side> <round> <metric> <value> [meta_json]
record() {
  local exp="$1" case_="$2" side="$3" round="$4" metric="$5" value="$6"
  local meta="${7:-}"; [ -n "$meta" ] || meta='{}'
  local line
  line=$(python3 - "$exp" "$case_" "$side" "$round" "$metric" "$value" "$meta" <<'PY'
import json, sys, datetime
exp, case, side, rnd, metric, value, meta = sys.argv[1:8]
try: value = int(value)
except ValueError:
    try: value = float(value)
    except ValueError: pass
m = json.loads(meta) if meta and meta != "{}" else {}
m.setdefault("ts", datetime.datetime.now(datetime.timezone.utc).strftime("%FT%TZ"))
print(json.dumps({"exp": exp, "case": case, "side": side, "round": int(rnd),
                  "metric": metric, "value": value, "meta": m}, ensure_ascii=False))
PY
) || { echo "record: python failed" >&2; return 1; }
  echo "$line" >> "$RESULTS_DIR/${exp}-${case_}.jsonl"
  echo "$line" # 也打到 stdout，便于人工观察
}

# ---- 采样循环：for_round <n> -- <cmd...>，环境变量 ROUND 注入轮次
for_round() {
  local n="$1"; shift; shift # 去掉 "--"
  local r
  for ((r = 1; r <= n; r++)); do
    echo "== round $r/$n =="
    ROUND="$r" "$@"
  done
}

# ---- 通用采样循环：run_metric <exp> <case> <side> <rounds> <metric> -- <cmd...>
#      cmd 需把测量值打到 stdout 最后一行 "<metric>_ms <value>"（timed 的格式）
run_metric() {
  local exp="$1" case_="$2" side="$3" rounds="$4" metric="$5"; shift 5
  [ "${1:-}" = "--" ] && shift
  local r out val
  for ((r = 1; r <= rounds; r++)); do
    out=$("$@") || { echo "run failed (round $r), see meta" >&2; record "$exp" "$case_" "$side" "$r" "$metric" -1 "{\"note\":\"run failed\"}"; continue; }
    val=$(echo "$out" | awk -v m="${metric}_ms" '$1==m{print $2}' | tail -1)
    echo "$out"
    [ -n "$val" ] && record "$exp" "$case_" "$side" "$r" "$metric" "$val" '{}'
  done
}

# ---- 磁盘占用（bytes） ----
du_bytes() { du -sb "$1" 2>/dev/null | awk '{print $1}'; }

# ---- 网络流量差分（vnstat 不在时降级 /proc/net/dev） ----
net_bytes() { # 输出 rx+tx 总字节
  local iface="${1:-eth0}"
  awk -v i="$iface" '$1==i":"{rx+=$2; tx+=$10} END{print rx+tx}' /proc/net/dev
}

# ---- scorpio mount helpers（与 demo.sh 同源） ----
mount_id() { # <worktree> -> mount id
  cat "$1/.libra/scorpiofs_mount_id" 2>/dev/null
}
detach_mount() { # <worktree>：DELETE mount（若 id 存在）并无条件尝试摘 FUSE 挂载
  local mid; mid=$(mount_id "$1")
  if [ -n "$mid" ]; then
    curl -sS --noproxy '*' -X DELETE "$API/mounts/$mid" >/dev/null 2>&1 || true
    sleep 1
  fi
  fusermount3 -u "$1" 2>/dev/null || sudo fusermount3 -uz "$1" 2>/dev/null || true
  return 0
}

# scorpio store 目录列表（本机 e2e 布局 + 常见位置；云上由 SCORPIO_STORE_PATH 覆盖）
store_dirs() {
  echo "${SCORPIO_STORE_PATH:-$HOME/e2e/store}"
  echo "$HOME/.scorpio"*
  echo "$HOME/.cache/scorpiofs"
  echo /var/lib/scorpiofs
}
store_bytes() { # store 总字节
  local s=0 d
  while IFS= read -r d; do
    [ -d "$d" ] && s=$((s + $(du_bytes "$d")))
  done < <(store_dirs)
  echo "$s"
}
mono_total_bytes() { # <worktree> mount + store 总量
  echo $(( $(du_bytes "$1") + $(store_bytes) ))
}
# libra 运行环境（与 demo.sh runlibra 同源）
libra_env() { env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u no_proxy -u NO_PROXY LIBRA_SCORPIOFS_ENDPOINT="$API" "$@"; }
libra() { local dir="$1"; shift; (cd "$dir" && libra_env "$LIBRA" "$@"); }

# ---- git 身份（一次性） ----
git_id() { git config --global user.name bench; git config --global user.email bench@gitmono.local; }

# 内层 `bash -c` 子进程需要直接调用的函数
export -f now_ms cold_cache net_bytes du_bytes store_bytes mono_total_bytes record \
  mount_id detach_mount libra_env libra need require_env
