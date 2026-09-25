#!/usr/bin/env bash
# bench/bin/collect.sh — 采集当前系统快照（磁盘/网络/prometheus），供报告附录引用。
# 用法: collect.sh [label]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=common.sh
source "$HERE/common.sh"
LABEL="${1:-$(date +%s)}"
OUT="$RESULTS_DIR/collect-$LABEL"
mkdir -p "$OUT"

echo "== disk =="
df -h > "$OUT/df.txt"
du -sb "$WORK"/* 2>/dev/null | sort -rn > "$OUT/du-work.txt" || true

echo "== network counters =="
cat /proc/net/dev > "$OUT/netdev.txt"

echo "== prometheus instant queries =="
PROM="${PROM:-http://127.0.0.1:9090}"
for q in 'node_memory_MemAvailable_bytes' 'node_load1' \
         'node_filesystem_avail_bytes{mountpoint!~"/(run|var/lib|proc|sys).*"}'; do
  name=$(echo "$q" | md5sum | cut -c1-8)
  curl -fsS --noproxy '*' "$PROM/api/v1/query?query=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$q")" \
    > "$OUT/prom-$name.json" 2>/dev/null || echo "(prometheus unreachable at $PROM)" >&2
done

echo "== libra/scorpio daemon process snapshot =="
for p in libra-new scorpio; do
  pid=$(pgrep -x "$p" | head -1)
  if [ -n "$pid" ] && [ -d "/proc/$pid" ]; then
    {
      echo "pid=$pid fds=$(ls /proc/$pid/fd 2>/dev/null | wc -l)"
      grep -E "VmRSS|Threads" "/proc/$pid/status"
    } >> "$OUT/process.txt"
  fi
done
[ -f "$OUT/process.txt" ] && cat "$OUT/process.txt"
echo "snapshot → $OUT"
