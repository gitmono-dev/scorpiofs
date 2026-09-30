#!/usr/bin/env bash
# Low-overhead resource sampler for the benchmark pods.
#
# Design constraints:
#   * must NOT perturb what it measures -> one read of /proc per target per interval,
#     no subprocess per sample beyond the tiny awk, no writes to the measured tree
#   * output must be alignable with the benchmark's phase markers -> every line is
#     prefixed with a monotonic-ish epoch-ms timestamp, and 1 Hz is enough to see
#     curves without generating noise
#
# Emits ONE line per target per tick:
#   S <ts_ms> <label> rss_kb=<n> fds=<n> cpu_ticks=<utime+stime> threads=<n>
# and, once per tick:
#   N <ts_ms> rx_bytes=<n> tx_bytes=<n>
# Phase markers from the driver appear as:
#   M <ts_ms> <phase-name>
set -uo pipefail

OUT="${1:?usage: sampler.sh <outfile> [interval_ms] [label:pid ...]}"
IVL_MS="${2:-1000}"
shift 2 2>/dev/null || true

# Targets are passed as label:pid pairs. If a pid dies mid-run (e.g. a short-lived
# libra/git command) we keep sampling the label with rss_kb=0 so the series stays
# regular instead of silently dropping points.
TARGETS=("$@")

net_sum() { # rx + tx bytes over non-loopback interfaces
  awk 'NR>2 && $1 !~ /^lo:/ {gsub(":", "", $1); rx+=$2; tx+=$10} END {printf "%d %d", rx, tx}' /proc/net/dev
}

# /proc/PID/stat field 2 (comm) can contain spaces and parens, which breaks naive
# awk field indexing. Strip it first so $14/$15 are utime/stime.
proc_stat() { # <pid> -> "utime stime threads"
  awk '{ for(i=3;i<=NF;i++) printf "%s ", $i; print "" }' "/proc/$1/stat" 2>/dev/null \
    | awk '{ printf "%s %s %s", $12, $13, $16 }' 2>/dev/null
}

proc_rss() { awk '/^VmRSS:/{print $2}' "/proc/$1/status" 2>/dev/null | head -1; }
proc_fds() { ls "/proc/$1/fd" 2>/dev/null | wc -l; }

: > "$OUT"

while :; do
  TS=$(date +%s%3N)

  read -r RX TX <<<"$(net_sum)"
  echo "N $TS rx_bytes=$RX tx_bytes=$TX" >> "$OUT"

  for t in "${TARGETS[@]}"; do
    label="${t%%:*}"; pid="${t#*:}"
    if [ -d "/proc/$pid" ] 2>/dev/null; then
      rss=$(proc_rss "$pid"); fds=$(proc_fds "$pid")
      read -r ut st th <<<"$(proc_stat "$pid")"
      cpu=$(( ${ut:-0} + ${st:-0} ))
      echo "S $TS $label rss_kb=${rss:-0} fds=${fds:-0} cpu_ticks=$cpu threads=${th:-0}" >> "$OUT"
    else
      echo "S $TS $label rss_kb=0 fds=0 cpu_ticks=0 threads=0 dead=1" >> "$OUT"
    fi
  done

  sleep "$(awk -v ms="$IVL_MS" 'BEGIN{printf "%.3f", ms/1000}')"
done
