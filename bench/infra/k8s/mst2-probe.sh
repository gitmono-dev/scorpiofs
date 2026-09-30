#!/usr/bin/env bash
# D1 + D2 probe: mount an MST/2-lowered ScorpioFS worktree in the cluster and exercise
# the read and write paths against it.
#
# Cluster facts this relies on:
#   * The MST/2-lowered daemon runs in the BENCH image (scorpio/libra live there);
#     the mega2 pod has no scorpio at all, so the daemon talks to it over HTTP.
#   * mega2's MST/2 base URL is `http://mega2:8000` — the client appends
#     `/api/v2/snapshots` itself (scorpiofs/src/snapshot/client.rs:170), so the
#     `/api/v2` suffix must NOT be included.
#   * Config precedence is CLI > SCORPIO_<FLAT_KEY_UPPERCASE> env > file > default
#     (scorpiofs/src/util/config.rs:238), so env alone is enough.
#
# Known boundaries from mst2-impl/MST2-LOWER-KNOWN-ISSUES.md that this checks for:
#   * O_APPEND to an existing lower file used to fail ENOENT (fh=0 write fallback)
#   * `tail` EBADF after the append fallback
#   * large-file chunked read EIO
# The fix for the first was written into a local libfuse-fs checkout and never
# committed, so whether THIS image has it is exactly what the append step decides.
set -uo pipefail

API="http://127.0.0.1:2725/antares"
M2="${M2:-http://mega2:8000}"
# Scope must match the repo root, not a subdirectory: the mount root IS the scope,
# while libra's index paths are repo-relative. Scope=/project/bench50k makes every
# index path (bench50k/svc00/...) invisible from the mount and `status` reports the
# whole tree as deleted. /project currently holds exactly the 50k fixture.
SCOPE="${SCOPE:-/project}"
export HOME=/root
export LIBRA_SCORPIOFS_ENDPOINT="$API"

# Fixture layout, from the generator in bench-script.sh: the file index determines
# every directory component — svc is (i % 20), pkg is ((i // 2500) % 25), mod is
# (i % 20). So f00001.rs lives in mod01, not mod00; hardcoding mod00 tests a path
# that does not exist and reports a product failure that is really a test bug.
fpath(){ printf 'bench50k/svc%02d/pkg%03d/mod%02d/f%05d.rs' \
  "$(( $1 % 20 ))" "$(( ($1 / 2500) % 25 ))" "$(( $1 % 20 ))" "$1"; }

# --- MST/2 lower configuration -------------------------------------------------
export SCORPIO_MST2_LOWER_ENABLED=true
export SCORPIO_MST2_BASE_URL="$M2"
export SCORPIO_MST2_SCOPE="$SCOPE"
export SCORPIO_BASE_URL="$M2"
# Both URLs are hard requirements of the entrypoint's serve-mode check
# (scorpiofs/deploy/docker-entrypoint.sh:25-26); without them it exits 22 before
# scorpio is ever exec'd. bench-ack.yaml sets both.
export SCORPIO_LFS_URL="$M2/api/v1/lfs"
export SCORPIO_MOUNT_OWNER=0:0

# libra's HTTP client defaults to a 60 s *idle* read timeout
# (libra/src/internal/protocol/https_client.rs:38) and these overrides beat it
# (fetch.rs:174, env > config > default). Measured on this cluster: the server needs
# 81 s to produce the FIRST byte of git-upload-pack (pack enumeration+compression), so
# the default fired on every attempt at every tree size. git's own client has no such
# cap, which is why `git clone` of the same tree completes. This is a workaround for a
# real server-side cost, not a substitute for fixing it.
export LIBRA_FETCH_IDLE_TIMEOUT_MS=900000
export LIBRA_FETCH_CONNECT_TIMEOUT_MS=120000

FAILED=0
pass(){ echo "  PASS  $*"; }
fail(){ echo "  FAIL  $*"; FAILED=1; }
result(){ echo "R $1=$2"; }

echo "HOST=$(hostname)"
echo "SCOPE=$SCOPE  M2=$M2"

echo
echo "=== [0] start the daemon with the MST/2 lower enabled ==="
# Clear any inherited proxy dead-ends, then start the daemon. `unset` rather than
# wrapping the command in `env -u ...`: the wrapper form silently dropped
# SCORPIO_BASE_URL and the daemon refused to start ("SCORPIO_BASE_URL must be set"),
# which made every later step run against a plain empty directory.
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY
echo "  config the daemon will see:"
echo "    SCORPIO_BASE_URL           = $SCORPIO_BASE_URL"
echo "    SCORPIO_LFS_URL            = $SCORPIO_LFS_URL"
echo "    SCORPIO_MST2_LOWER_ENABLED = $SCORPIO_MST2_LOWER_ENABLED"
echo "    SCORPIO_MST2_BASE_URL      = $SCORPIO_MST2_BASE_URL"
echo "    SCORPIO_MST2_SCOPE         = $SCORPIO_MST2_SCOPE"
/usr/local/bin/docker-entrypoint.sh serve --http-addr 0.0.0.0:2725 \
  >/tmp/daemon.log 2>&1 &
DAEMON_PID=$!
echo "  daemon pid=$DAEMON_PID"
for i in $(seq 1 60); do curl -fsS -m 3 "$API/health" >/dev/null 2>&1 && break; sleep 1; done
HEALTH=$(curl -sS -o /dev/null -w '%{http_code}' -m 3 "$API/health" 2>&1)
echo "  health: $HEALTH"
if [ "$HEALTH" != "200" ]; then
  fail "daemon is not healthy — every later step would test an empty directory"
  echo "  daemon log:"; tail -20 /tmp/daemon.log | sed 's/^/    /'
  result daemon_health "$HEALTH"
  echo "RESULT: INCOMPLETE (daemon)"
  exit 1
fi
result daemon_health "$HEALTH"

echo
echo "=== [1] libra clone (50k tree, must complete now) ==="
git config --global user.name  probe
git config --global user.email probe@b.l
git config --global http.postBuffer 1073741824

# Shallow clone is the correct shape here, not a shortcut. Measured on this cluster:
# a FULL clone transfers 550,001 blobs / 75 MB (the 500k-file ancestor commit) and
# costs 83-98 s, of which 81 s is the server's silent pack build; `--depth 1` carries
# only the tip's 50,000 blobs / 9.4 MB and costs ~9.6 s. The mono workspace needs even
# less than that, because file content comes from the MST/2 snapshot on read, so the
# cloned blobs are never the source of truth for the mount.
CLONE_ARGS="${CLONE_ARGS:---depth 1}"
echo "  clone args: $CLONE_ARGS"
rm -rf /tmp/main /tmp/wt
t0=$(date +%s%3N)
# shellcheck disable=SC2086
libra clone -q -b main --no-checkout $CLONE_ARGS "$M2/project" /tmp/main 2>/tmp/clone.err; RC=$?
t1=$(date +%s%3N)
CLONE_MS=$((t1-t0))
echo "  libra clone rc=$RC in $CLONE_MS ms"
result clone_rc "$RC"; result clone_ms "$CLONE_MS"
result clone_disk_bytes "$(du -sb /tmp/main 2>/dev/null | cut -f1)"
if [ "$RC" != 0 ]; then
  echo "  stderr: $(tail -3 /tmp/clone.err)"
  fail "libra clone did not complete — the remaining probe cannot run"
  echo "RESULT: INCOMPLETE (clone)"
  kill $DAEMON_PID 2>/dev/null || true
  exit 1
fi
( cd /tmp/main && libra config set user.name probe >/dev/null 2>&1 \
  && libra config set user.email probe@b.l >/dev/null 2>&1 )

echo
echo "=== [2] attach a ScorpioFS worktree (D1) ==="
t0=$(date +%s%3N)
( cd /tmp/main && libra worktree add --backend scorpiofs -b probe /tmp/wt ) 2>/tmp/attach.err; RC=$?
t1=$(date +%s%3N)
ATTACH_MS=$((t1-t0))
echo "  worktree add rc=$RC in $ATTACH_MS ms"
result attach_rc "$RC"; result attach_ms "$ATTACH_MS"
[ "$RC" = 0 ] || echo "  stderr: $(tail -3 /tmp/attach.err)"

echo
echo "=== [3] D1: is the lower really MST/2? ==="
if grep -q 'MST/2 snapshot view as the lower layer' /tmp/daemon.log; then
  pass "daemon log carries the MST/2 lower message"
  grep -o 'serving MST/2 snapshot view as the lower layer.*' /tmp/daemon.log | head -1 | sed 's/^/    /'
  result d1_log 1
else
  fail "daemon log has NO MST/2 lower message"
  grep -iE 'mst2|snapshot|lower' /tmp/daemon.log | tail -5 | sed 's/^/    log: /'
  result d1_log 0
fi
mount | grep -q " /tmp/wt " && pass "/tmp/wt is a live mount" || fail "/tmp/wt is not mounted"
result d1_mounted "$(mount | grep -c " /tmp/wt ")"

MOUNT_FILES=$(timeout 900 find /tmp/wt -type f 2>/dev/null | wc -l)
echo "  files visible through the mount: $MOUNT_FILES"
result mount_files "$MOUNT_FILES"
[ "$MOUNT_FILES" -ge 50000 ] && pass "mount exposes the full 50k" \
  || fail "mount exposes $MOUNT_FILES files (expected 50000)"

echo
echo "=== [4] D1 read path ==="
DEEP="/tmp/wt/$(fpath 0)"
echo "  probe path: $DEEP"
if [ -f "$DEEP" ]; then
  BODY=$(head -c 40 "$DEEP" 2>&1)
  echo "  cat -> $BODY"
  result read_deep_file_ok 1
else
  fail "deep file not readable: $DEEP"
  result read_deep_file_ok 0
fi
if [ -d /tmp/wt/bench50k/svc19 ]; then pass "directory traversal works"; else fail "directory traversal broken"; fi
result read_traverse_ok "$([ -d /tmp/wt/bench50k/svc19 ] && echo 1 || echo 0)"

echo
echo "=== [5] D2 write path (the decider) ==="
cd /tmp/wt || { fail "cannot enter the mount"; echo "RESULT: INCOMPLETE"; exit 1; }

F_OVERWRITE=$(fpath 0)     # svc00/pkg000/mod00/f00000.rs
F_APPEND=$(fpath 1)        # svc01/pkg000/mod01/f00001.rs
F_DELETE=$(fpath 2)        # svc02/pkg000/mod02/f00002.rs
echo "  targets: overwrite=$F_OVERWRITE append=$F_APPEND delete=$F_DELETE"

op(){ # op <name> <command...>
  local name="$1"; shift
  local out
  if out=$("$@" 2>&1); then pass "$name"; result "w_$name" 1
  else fail "$name -> $out"; result "w_$name" 0; fi
}

# create a new file at the lower root
if echo hello > newfile.txt 2>/tmp/e1; then pass "create new file"; result w_create 1
else fail "create new file -> $(cat /tmp/e1)"; result w_create 0; fi

# mkdir (root and inside a copied-up lower directory)
if mkdir -p newdir/sub 2>/tmp/e2; then pass "mkdir -p"; result w_mkdir 1
else fail "mkdir -p -> $(cat /tmp/e2)"; result w_mkdir 0; fi
if mkdir -p bench50k/svc00/inside 2>/tmp/e3; then pass "mkdir inside a lower dir (copy-up)"; result w_mkdir_lower 1
else fail "mkdir inside a lower dir -> $(cat /tmp/e3)"; result w_mkdir_lower 0; fi

# every write target must exist in the lower BEFORE we touch it, or the result says
# nothing about the write path
for f in "$F_OVERWRITE" "$F_APPEND" "$F_DELETE"; do
  [ -f "$f" ] || fail "lower file missing before the write: $f (layout bug, not a product bug)"
done

# overwrite an existing lower file (truncate) -- expected to work
if echo overwritten > "$F_OVERWRITE" 2>/tmp/e4; then pass "overwrite lower file (truncate)"; result w_overwrite 1
else fail "overwrite lower file -> $(cat /tmp/e4)"; result w_overwrite 0; fi

# append -- the case MST2-LOWER-KNOWN-ISSUES.md records as a known boundary
if echo appended >> "$F_APPEND" 2>/tmp/e5; then pass "append to lower file"; result w_append 1
else fail "append to lower file -> $(cat /tmp/e5)"; result w_append 0; fi
# the follow-up read that used to give EBADF
if TAIL=$(tail -1 "$F_APPEND" 2>/tmp/e6); then pass "tail after append ($TAIL)"; result w_tail_after_append 1
else fail "tail after append -> $(cat /tmp/e6)"; result w_tail_after_append 0; fi

# delete a lower file
if rm -f "$F_DELETE" 2>/tmp/e7; then pass "delete lower file"; result w_delete 1
else fail "delete lower file -> $(cat /tmp/e7)"; result w_delete 0; fi

# rename
if mv newfile.txt renamed.txt 2>/tmp/e8; then pass "rename"; result w_rename 1
else fail "rename -> $(cat /tmp/e8)"; result w_rename 0; fi

# a large-ish file write+read (exercises the chunked path)
if head -c 3000000 /dev/urandom > big.bin 2>/tmp/e9; then
  SZ=$(stat -c %s big.bin 2>/dev/null)
  if [ "${SZ:-0}" = 3000000 ]; then pass "3 MB write + stat"; result w_bigfile 1
  else fail "3 MB write truncated to ${SZ:-?}"; result w_bigfile 0; fi
else fail "3 MB write -> $(cat /tmp/e9)"; result w_bigfile 0; fi

echo
echo "=== [6] status sees the changes ==="
libra status 2>&1 | head -12 | sed 's/^/  /'
result status_rc "$?"

echo
echo "=== [6b] D3: ready-time comparison against git, same tree, same node ==="
# Both sides measured in ONE run: the server's pack build is uncached, so a figure
# carried over from an earlier run is not comparable.
gms(){ date +%s%3N; }

rm -rf /tmp/gfull
t0=$(gms); git clone -q "$M2/project" /tmp/gfull 2>/dev/null; t1=$(gms)
GIT_FULL=$((t1-t0))
result git_full_clone_ms "$GIT_FULL"
result git_full_clone_files "$(find /tmp/gfull -type f -not -path '*/.git/*' 2>/dev/null | wc -l)"

MONO_READY=$((CLONE_MS + ATTACH_MS))
result mono_ready_ms "$MONO_READY"
echo "  git  full clone (working workspace ready) : $GIT_FULL ms"
echo "  mono clone + attach                       : $MONO_READY ms  (clone $CLONE_MS + attach $ATTACH_MS)"
if [ "$GIT_FULL" -gt 0 ]; then
  RATIO_X1000=$(( MONO_READY * 1000 / GIT_FULL ))
  result ready_ratio_x1000 "$RATIO_X1000"
  echo "  ratio mono/git = $RATIO_X1000/1000   (D3 target <= 200)"
  [ "$RATIO_X1000" -le 200 ] \
    && pass "D3 MET: mono ready is under 0.2x of a git clone" \
    || fail "D3 NOT met: ratio $RATIO_X1000/1000 exceeds the 200 target"
fi
rm -rf /tmp/gfull

echo
echo "=== [7] daemon-side errors worth reporting ==="
grep -iE 'ERROR|panic|EIO|EBADF' /tmp/daemon.log | tail -8 | sed 's/^/  /' || echo "  (none)"

echo
echo "======== SUMMARY ========"
grep '^R ' /dev/null >/dev/null 2>&1 || true
if [ "$FAILED" = 0 ]; then echo "RESULT: D1/D2 ALL PASS"; else echo "RESULT: D1/D2 HAS FAILURES (see FAIL lines)"; fi
kill $DAEMON_PID 2>/dev/null || true
