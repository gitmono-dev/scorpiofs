#!/usr/bin/env bash
# Replay the mono commit with full stderr, then dump the daemon log around it.
#
# W3's status and add beat git (13 ms vs 73/94 ms). The commit alone fails after 41 s,
# with stderr suppressed in the W3 run. This replays it verbosely: clone (depth 1, the
# verified path), attach, one change, add, commit — every rc and every message.
set -uo pipefail

API="http://127.0.0.1:2725/antares"
M2="${M2:-http://mega2:8000}"
export HOME=/root
export LIBRA_SCORPIOFS_ENDPOINT="$API"
export LIBRA_FETCH_IDLE_TIMEOUT_MS=900000
export SCORPIO_BASE_URL="$M2"
export SCORPIO_LFS_URL="$M2/api/v1/lfs"
export SCORPIO_MST2_LOWER_ENABLED=true
export SCORPIO_MST2_BASE_URL="$M2"
export SCORPIO_MST2_SCOPE=/project
export SCORPIO_MOUNT_OWNER=0:0

echo "=== [0] daemon ==="
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY 2>/dev/null || true
/usr/local/bin/docker-entrypoint.sh serve --http-addr 0.0.0.0:2725 >/tmp/daemon.log 2>&1 &
DP=$!
for i in $(seq 1 60); do curl -fsS -m 3 "$API/health" >/dev/null 2>&1 && break; sleep 1; done
echo "  health: $(curl -sS -o /dev/null -w '%{http_code}' -m 3 "$API/health")"

echo
echo "=== [1] clone + attach ==="
L=/tmp/libra-filter/libra
rm -rf /tmp/mc /tmp/mwt
$L clone -q --no-checkout -b main --depth 1 http://mega2:8000/project /tmp/mc 2>&1 | tail -2
echo "  clone rc=$?"
( cd /tmp/mc && $L config set user.name w3d >/dev/null 2>&1 && $L config set user.email w3d@b.l >/dev/null 2>&1 )
t0=$(date +%s%3N)
( cd /tmp/mc && $L worktree add --backend scorpiofs -b w3diag /tmp/mwt ) 2>&1 | tail -3
echo "  attach rc=$? in $(( $(date +%s%3N) - t0 )) ms"

echo
echo "=== [2] modify + add (verified working in W3) ==="
echo "diag change" >> /tmp/mwt/bench50k/svc00/pkg000/mod00/f00000.rs
( cd /tmp/mwt && $L add -A ) 2>&1 | tail -2
echo "  add rc=$?"

echo
echo "=== [3] THE COMMIT, verbosely ==="
t0=$(date +%s%3N)
( cd /tmp/mwt && $L commit -m "w3 diag commit" ) 2>&1 | tail -8
RC=$?
echo "  commit rc=$RC in $(( $(date +%s%3N) - t0 )) ms"

echo
echo "=== [4] status after (did anything land?) ==="
( cd /tmp/mwt && $L status ) 2>&1 | head -8

echo
echo "=== [5] daemon log around the commit ==="
grep -iE 'error|panic|fail|warn|commit|finalize' /tmp/daemon.log 2>/dev/null | tail -20 | sed 's/^/  /'

kill $DP 2>/dev/null || true
echo "RESULT: diag done"
