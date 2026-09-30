#!/usr/bin/env bash
# W3: do mono's daily operations degrade? status / add / commit, side by side with git.
#
# Gate: mono's status/add/commit must not exceed git's 3x. Both sides work on the same
# 50k tree; the git side is a plain clone with checkout, the mono side is a ScorpioFS
# worktree whose lower is the MST/2 snapshot (the architecture under test — its status
# walks the FUSE mount, which is the point).
#
# Runs in the privileged probe pod (needs /dev/fuse + the daemon). Uses the NEW libra
# binary uploaded from the host, so the clone can use --filter=blob:none like a real
# ScorpioFS workspace.
set -uo pipefail

API="http://127.0.0.1:2725/antares"
M2="${M2:-http://mega2:8000}"
SCOPE="${SCOPE:-/project}"
export HOME=/root
export LIBRA_SCORPIOFS_ENDPOINT="$API"
export LIBRA_FETCH_IDLE_TIMEOUT_MS=900000

export SCORPIO_BASE_URL="$M2"
export SCORPIO_LFS_URL="$M2/api/v1/lfs"
export SCORPIO_MST2_LOWER_ENABLED=true
export SCORPIO_MST2_BASE_URL="$M2"
export SCORPIO_MST2_SCOPE="$SCOPE"
export SCORPIO_MOUNT_OWNER=0:0

ms(){ date +%s%3N; }
res(){ echo "R $1=$2"; }
timed(){ # <label> <n> <cmd...>  -> prints "R label per-op ms" (median of n)
  local label="$1" n="$2"; shift 2
  local vals=()
  for i in $(seq 1 "$n"); do
    local t0 t1
    t0=$(ms); "$@" >/dev/null 2>&1; t1=$(ms)
    vals+=($((t1-t0)))
  done
  # median for n=3: sort, take middle
  local sorted=$(printf '%s\n' "${vals[@]}" | sort -n)
  local med=$(echo "$sorted" | sed -n "$(( (n+1)/2 ))p")
  echo "  $label: ${vals[*]} ms -> median ${med} ms"
  res "${label}_ms" "$med"
}

echo "HOST=$(hostname)"
FAILED=0

echo
echo "=== [0] daemon (MST/2 lower) ==="
ls -l /dev/fuse 2>&1 | sed 's/^/  /'
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY 2>/dev/null || true
/usr/local/bin/docker-entrypoint.sh serve --http-addr 0.0.0.0:2725 >/tmp/daemon.log 2>&1 &
DAEMON_PID=$!
for i in $(seq 1 60); do curl -fsS -m 3 "$API/health" >/dev/null 2>&1 && break; sleep 1; done
H=$(curl -sS -o /dev/null -w '%{http_code}' -m 3 "$API/health" 2>&1)
echo "  daemon health: $H"
[ "$H" = "200" ] || { echo "  FAIL daemon"; tail -10 /tmp/daemon.log | sed 's/^/    /'; echo "RESULT: INCOMPLETE"; exit 1; }

echo
echo "=== [1] prepare both sides ==="
NEWLIBRA=/tmp/libra-filter/libra
if [ -x "$NEWLIBRA" ]; then LIBRA="$NEWLIBRA"; else LIBRA=libra; fi
echo "  libra under test: $LIBRA ($($LIBRA --version 2>&1 | head -1))"

rm -rf /tmp/gc /tmp/mc /tmp/mwt
t0=$(ms)
git clone -q "$M2/project" /tmp/gc 2>/dev/null
t1=$(ms)
git -C /tmp/gc config user.email w3@b.l; git -C /tmp/gc config user.name w3
echo "  git clone (W1 baseline): $((t1-t0)) ms, $(find /tmp/gc -type f -not -path '*/.git/*' | wc -l) files"
res git_clone_w1_ms "$((t1-t0))"

# NOTE: not --filter here yet. A filtered clone carries no blobs, and libra's
# worktree-add populate path still reads blobs from the local pack, so attach fails
# with "failed to read object" (diagnosed 2026-09-30). Depth-1 is the verified path;
# wiring filtered clones into the MST/2-backed attach is recorded as follow-up work.
$LIBRA clone -q --no-checkout -b main --depth 1 "$M2/project" /tmp/mc 2>/tmp/mc.err
( cd /tmp/mc && $LIBRA config set user.name w3 >/dev/null 2>&1 && $LIBRA config set user.email w3@b.l >/dev/null 2>&1 )
t0=$(ms)
( cd /tmp/mc && $LIBRA worktree add --backend scorpiofs -b w3 /tmp/mwt >/dev/null 2>&1 ); RC=$?
t1=$(ms)
echo "  mono attach: rc=$RC in $((t1-t0)) ms"
res w3_attach_ms "$((t1-t0))"
if [ "$RC" != 0 ]; then echo "  FAIL attach"; tail -3 /tmp/mc.err 2>/dev/null | sed 's/^/    /'; echo "RESULT: INCOMPLETE"; kill $DAEMON_PID 2>/dev/null; exit 1; fi
sleep 3
MNT=$(timeout 300 find /tmp/mwt -type f 2>/dev/null | wc -l)
echo "  mount files: $MNT"
res w3_mount_files "$MNT"
[ "$MNT" -ge 40000 ] || { echo "  FAIL mount under-populated"; echo "RESULT: INCOMPLETE"; kill $DAEMON_PID 2>/dev/null; exit 1; }

echo
echo "=== [2] W3a: status on a clean tree (n=3, median) ==="
# `libra <dir> <cmd>` is NOT a libra invocation: it exits 129 ('not a libra command')
# in ~11 ms, which a timing harness happily records as a fast success. Everything on
# the mono side must run with the worktree as CWD.
mono_status(){ ( cd /tmp/mwt && $LIBRA status ); }
mono_add(){ ( cd /tmp/mwt && $LIBRA add -A ); }
timed git_status 3 git -C /tmp/gc status --porcelain
timed mono_status 3 mono_status

echo
echo "=== [3] W3b: add with one changed file ==="
# one modification on each side, then add -A
echo "w3 change" >> /tmp/gc/bench50k/svc00/pkg000/mod00/f00000.rs
echo "w3 change" >> /tmp/mwt/bench50k/svc00/pkg000/mod00/f00000.rs
timed git_add 3 git -C /tmp/gc add -A
timed mono_add 3 mono_add

echo
echo "=== [4] W3c: commit (n=1 each — commits move the tree) ==="
t0=$(ms); git -C /tmp/gc commit -qm "w3 git commit" 2>/dev/null; RC=$?; t1=$(ms)
echo "  git commit: rc=$RC $((t1-t0)) ms"
res git_commit_ms "$((t1-t0))"
[ "$RC" = 0 ] || { echo "  FAIL git commit"; FAILED=1; }

t0=$(ms); ( cd /tmp/mwt && $LIBRA commit -m "w3 mono commit" >/dev/null 2>&1 ); RC=$?; t1=$(ms)
echo "  mono commit: rc=$RC $((t1-t0)) ms"
res mono_commit_ms "$((t1-t0))"
[ "$RC" = 0 ] || { echo "  FAIL mono commit"; grep -iE 'error|panic' /tmp/daemon.log | tail -4 | sed 's/^/    /'; FAILED=1; }

echo
echo "=== [5] ratios are computed by the runner from the R lines above ==="
echo "=== cleanup ==="
rm -rf /tmp/gc /tmp/mc /tmp/mwt
kill $DAEMON_PID 2>/dev/null || true
[ "$FAILED" = 0 ] && echo "RESULT: W3 COMPLETE" || echo "RESULT: W3 HAD FAILURES"
