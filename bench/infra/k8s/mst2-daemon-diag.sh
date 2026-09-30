#!/usr/bin/env bash
# Why does the daemon not come up in the probe pod?
#
# In the probe run, `/usr/local/bin/docker-entrypoint.sh serve --http-addr 0.0.0.0:2725`
# was backgrounded and never answered /health on 127.0.0.1:2725 after 60 s. The same
# command line works in bench-ack.yaml, so the difference is either the `env -u ...`
# wrapper (proxy scrubbing), a missing prerequisite, or a startup error that the probe
# script never printed.
#
# This job starts the daemon exactly as the probe does, then dumps the ENTIRE
# daemon.log plus the process state — no grep filtering, so nothing is hidden.
set -uo pipefail

API="http://127.0.0.1:2725/antares"
export HOME=/root

echo "HOST=$(hostname)"
echo "=== /dev/fuse ==="
ls -l /dev/fuse 2>&1 | sed 's/^/  /'
echo "=== binaries ==="
ls -l /usr/local/bin/docker-entrypoint.sh /usr/local/bin/scorpio 2>&1 | sed 's/^/  /'

echo
echo "=== start the daemon exactly as the probe does (proxy scrubbed) ==="
env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u all_proxy -u ALL_PROXY \
  /usr/local/bin/docker-entrypoint.sh serve --http-addr 0.0.0.0:2725 \
  >/tmp/daemon.log 2>&1 &
DAEMON_PID=$!
echo "  pid=$DAEMON_PID"

for i in $(seq 1 45); do
  C=$(curl -sS -o /dev/null -w '%{http_code}' -m 3 "$API/health" 2>/dev/null)
  [ "$C" = "200" ] && { echo "  health 200 after ${i}s"; break; }
  sleep 1
done
echo "  final health: $(curl -sS -o /dev/null -w '%{http_code}' -m 3 "$API/health" 2>&1)"

echo
echo "=== is the process alive? ==="
if kill -0 "$DAEMON_PID" 2>/dev/null; then echo "  alive"; else echo "  DEAD (exited)"; fi
ps -ef 2>/dev/null | grep -E 'scorpio|entrypoint' | grep -v grep | sed 's/^/  /'

echo
echo "=== listening sockets ==="
(ss -ltnp 2>/dev/null || netstat -ltnp 2>/dev/null) | head -15 | sed 's/^/  /'

echo
echo "=== FULL daemon.log ==="
cat /tmp/daemon.log 2>&1 | sed 's/^/  /'

echo
echo "=== try the proxy-scrubbing variant vs plain, for contrast ==="
kill "$DAEMON_PID" 2>/dev/null || true
sleep 2
echo "  --- plain invocation (no env wrapper) ---"
/usr/local/bin/docker-entrypoint.sh serve --http-addr 0.0.0.0:2726 >/tmp/daemon2.log 2>&1 &
P2=$!
sleep 20
echo "  health 2726: $(curl -sS -o /dev/null -w '%{http_code}' -m 3 http://127.0.0.1:2726/antares/health 2>&1)"
kill -0 "$P2" 2>/dev/null && echo "  alive" || echo "  DEAD"
echo "  --- its log (first 40 lines) ---"
head -40 /tmp/daemon2.log 2>&1 | sed 's/^/  /'
kill "$P2" 2>/dev/null || true
