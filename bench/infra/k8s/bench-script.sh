#!/usr/bin/env bash
# L-scale ACK benchmark driver (single monorepo, both sides).
#
# Design notes that matter:
#   * ONE repo. mega2's own git endpoint serves the SAME repository the FUSE mount
#     projects, so the git and mono sides read identical storage — the server
#     variable is zeroed and only the client-side access pattern differs.
#   * A 1 Hz resource sampler runs alongside (sampler.sh) and is phase-marked, so
#     the curves can be aligned with the timings afterwards. It reads /proc only.
#   * Seeding is the real bottleneck at this scale. One push of 500k files needs
#     http.postBuffer raised past the pack size, otherwise git switches to chunked
#     transfer-encoding and the server hangs up. We try ONE big push, then fall back
#     to large batches.
set -uo pipefail

N="${NFILES:-500000}"
M2="${M2:-http://mega2:8000}"
API="http://127.0.0.1:2725/antares"
SAMP=/tmp/sampler.tsv
export HOME=/root
export LIBRA_SCORPIOFS_ENDPOINT="$API"

now(){ date +%s%3N; }
mark(){ echo "M $(now) $1" >> "$SAMP"; echo "[phase] $1"; }
res(){ echo "R $(now) $1=$2"; }   # machine-readable result line

# Fail-fast: a failed prerequisite must end the run, not silently degrade every later
# measurement. The last run's mono numbers were all garbage because one attach failed
# and the script kept going (and `cd` failed silently on top of it).
FAILED=0
abort(){ echo "ABORT: $*" >&2; echo "R $(now) aborted=1"; FAILED=1; exit 1; }
need(){ [ "$1" = 0 ] || abort "$2 (rc=$1)"; }

echo "HOST=$(hostname)"
echo "NFILES=$N   M2=$M2"
echo "cpu=$(nproc)  mem_kb=$(awk '/MemTotal/{print $2}' /proc/meminfo)"

echo
echo "[0] start the scorpio daemon + the sampler"
/usr/local/bin/docker-entrypoint.sh serve --http-addr 0.0.0.0:2725 >/tmp/daemon.log 2>&1 &
DAEMON_PID=$!
for i in $(seq 1 60); do curl -fsS "$API/health" >/dev/null 2>&1 && break; sleep 1; done
echo "    daemon health : $(curl -sS -o /dev/null -w '%{http_code}' "$API/health")"
echo "    daemon pid    : $DAEMON_PID"
echo "    libra         : $(libra --version 2>&1 | head -1)"
echo "    git           : $(git --version)"

# Sampler: the daemon, plus a placeholder label for the short-lived client commands
# (they exit too fast to sample meaningfully, so the interesting series is the daemon
# and the pod-level network).
bash /bench/sampler.sh "$SAMP" 1000 "scorpio:$DAEMON_PID" >/dev/null 2>&1 &
SAMPLER_PID=$!
sleep 2
mark "setup-done"

echo
echo "[1] wait for mega2"
for i in $(seq 1 150); do curl -fsS -o /dev/null "$M2/api/v1/latest-commit?path=/project" && break; sleep 2; done
mark "mega2-ready"

echo
echo "[2] seed ONE repo of $N files into mega2"
git config --global user.name  ack-bench
git config --global user.email ack@bench.local
git config --global init.defaultBranch main
git config --global --add safe.directory '*'
# A single huge push wedges (server stops reading at its buffer limit, TCP window
# fills, both sides wait). Keep each push small and bound it with `timeout` so a
# stalled batch fails loudly. postBuffer stays high so a batch never goes chunked.
git config --global http.postBuffer 1073741824
git config --global core.compression 1
git config --global pack.threads "$(nproc)"
SEED_BATCH="${SEED_BATCH:-25000}"     # files per push
SEED_PUSH_TIMEOUT="${SEED_PUSH_TIMEOUT:-900}"   # seconds; a stalled batch must die

# Re-seeding is expensive (regenerate 500k files, re-push in batches). If the server
# already holds a complete tree, skip straight to verification.
PRESEED=$(curl -sS -m 30 "$M2/api/v1/latest-commit?path=/project" >/dev/null 2>&1 && echo ok || echo no)
SEED_NEEDED=1
if [ "$PRESEED" = ok ]; then
  rm -rf /tmp/precheck
  if git clone -q --depth 1 "$M2/project" /tmp/precheck 2>/dev/null      || git clone -q "$M2/project" /tmp/precheck 2>/dev/null; then
    HAVE=$(find /tmp/precheck -type f -not -path '*/.git/*' 2>/dev/null | wc -l)
    echo "    /project already holds $HAVE files"
    [ "$HAVE" -ge "$N" ] && { SEED_NEEDED=0; echo "    seeding SKIPPED (tree already complete)"; res seed_skipped 1; }
  fi
  rm -rf /tmp/precheck
fi

if [ "$SEED_NEEDED" = 1 ]; then
rm -rf /seed && mkdir -p /seed && cd /seed
if git clone -q "$M2/project" /seed/repo 2>/dev/null; then
  echo "    /project already existed; reusing"
else
  mkdir -p /seed/repo && cd /seed/repo && git init -q
  echo "    /project absent; creating it via a push"
fi
cd /seed/repo

mark "seed-generate-begin"
# python3 -c (no heredoc): this script is embedded in a ConfigMap, so every line is
# indented and a heredoc terminator at column 0 would never match.
python3 -c '
import os, sys
n = int(sys.argv[1])
# Monorepo-ish: 40 services x 125 packages x 100 modules -> 500k files at n=500000.
# Structure matters: a flat 500k-file directory would not exercise tree walking.
per = max(1, n // 40)
for i in range(n):
    d = os.path.join("svc%02d" % (i % 40),
                     "pkg%03d" % ((i // per) % 125),
                     "mod%02d" % (i % 100))
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "f%06d.rs" % i), "w") as fh:
        fh.write("// synth %d\npub fn f%d(x: u64) -> u64 { x.wrapping_mul(%d) }\n"
                 % (i, i, (i % 97) + 1))
print("    generated", n, "files")
' "$N"
mark "seed-generate-end"
echo "    tree on disk: $(du -sh . | cut -f1)"

mark "seed-add-begin"
t0=$(now); git add -A; t1=$(now)
echo "    git add -A     : $((t1-t0)) ms"
res seed_add_ms $((t1-t0))
mark "seed-add-end"

git commit -qm "ack L-scale seed: $N files" || true
mark "seed-push-begin"
# batch the whole seed: this is the only strategy that completed at smaller scales
BATCH="$SEED_BATCH"; i=0; nb=0; fails=0; t_all0=$(now)
while [ "$i" -lt "$N" ]; do
  hi=$(( i + BATCH )); [ "$hi" -gt "$N" ] && hi="$N"
  git add -A >/dev/null 2>&1
  git commit -qm "seed batch $nb" >/dev/null 2>&1 || true
  b0=$(now)
  if timeout "$SEED_PUSH_TIMEOUT" git push -q "$M2/project" HEAD:refs/heads/main 2>/tmp/pusherr; then
    b1=$(now); echo "    batch $nb ($i..$hi): $((b1-b0)) ms"
    res seed_batch_ms $((b1-b0))
  else
    rc=$?; b1=$(now); fails=$((fails+1))
    echo "    batch $nb ($i..$hi) FAILED rc=$rc after $((b1-b0)) ms: $(tail -1 /tmp/pusherr)"
    res seed_batch_failed 1
  fi
  i="$hi"; nb=$((nb+1))
  echo "    progress: $i/$N batches=$nb failed=$fails elapsed=$(( $(now) - t_all0 )) ms"
done
t_all1=$(now)
echo "    seed total: $((t_all1-t_all0)) ms  batches=$nb failed=$fails"
res seed_total_ms $((t_all1-t_all0)); res seed_batches "$nb"; res seed_failed "$fails"
mark "seed-push-end"

# The seed is only usable if the server really has the whole tree. A half-pushed
# seed once produced a full set of "clean" numbers on a 1-file repo.
mark "seed-verify-begin"
rm -rf /tmp/verify
git clone -q "$M2/project" /tmp/verify 2>/dev/null
SEEDED=$(find /tmp/verify -type f -not -path '*/.git/*' 2>/dev/null | wc -l)
echo "    verified file count on the server: $SEEDED  (expected $N)"
res seed_verified_files "$SEEDED"
rm -rf /tmp/verify
mark "seed-verify-end"
# A short seed means every later comparison is meaningless.
[ "$SEEDED" -ge "$N" ] || abort "seed incomplete: $SEEDED of $N files on the server"
fi   # end of "if seeding was needed"

echo
echo "======== RESULTS ========"
mark "bench-begin"

echo "[R-READY] git clone (whole tree, server on another node)"
rm -rf /tmp/gitclone /tmp/gclone2 /tmp/wt; sync
t0=$(now); git clone -q "$M2/project" /tmp/gitclone || abort "git clone failed"
t1=$(now)
GIT_READY=$((t1-t0))
GIT_FILES=$(find /tmp/gitclone -type f -not -path '*/.git/*' | wc -l)
echo "    git-clone-ms=$GIT_READY  files=$GIT_FILES  disk=$(du -sb /tmp/gitclone | cut -f1)"
res git_ready_ms "$GIT_READY"
res git_clone_disk_bytes "$(du -sb /tmp/gitclone | cut -f1)"
# A clone that came back short means the server gave us a partial tree; every later
# measurement would be against the wrong baseline.
[ "$GIT_FILES" -ge "$N" ] || abort "git clone returned $GIT_FILES files, expected $N"

echo "[R-READY] libra worktree add (server on another node)"
rm -rf /tmp/main /tmp/wt
# Time the main clone SEPARATELY. Comparing an attach-only figure against git's
# full clone would overstate mono: both sides download the same objects here.
t0=$(now); libra clone -q -b main --no-checkout "$M2/project" /tmp/main 2>/tmp/clone.err; RC=$?; t1=$(now)
MONO_CLONE=$((t1-t0))
echo "    libra-clone-ms=$MONO_CLONE (rc=$RC)"
[ "$RC" = 0 ] || { echo "    clone stderr: $(tail -2 /tmp/clone.err)" >&2; abort "libra clone (main) failed"; }
res mono_clone_ms "$MONO_CLONE"
( cd /tmp/main && libra config set user.name ack >/dev/null 2>&1 && libra config set user.email ack@b.l >/dev/null 2>&1 )
t0=$(now); ( cd /tmp/main && libra worktree add --backend scorpiofs -b "L-$$" /tmp/wt ) 2>/tmp/attach.err; RC=$?; t1=$(now)
MONO_ATTACH=$((t1-t0))
MONO_READY=$((MONO_CLONE + MONO_ATTACH))
echo "    worktree-add-ms=$MONO_ATTACH  (mono total clone+attach=$MONO_READY)  rc=$RC"
if [ "$RC" != 0 ] || [ ! -f /tmp/wt/.libra/scorpiofs_mount_id ]; then
  echo "    attach stderr: $(tail -2 /tmp/attach.err)" >&2
  abort "scorpiofs attach failed — the mono side cannot be measured"
fi

# THE gate that was missing: an attach can "succeed" while projecting nothing, and
# then worktree-add looks instantaneous and every later mono number is fiction.
# Count what the mount actually exposes before trusting any of it.
MOUNT_FILES=$(timeout 600 find /tmp/wt -type f 2>/dev/null | wc -l)
echo "    files visible through the mount: $MOUNT_FILES (expected ~$N)"
res mono_mount_files "$MOUNT_FILES"
[ "$MOUNT_FILES" -ge "$((N / 2))" ] || abort "mount is empty ($MOUNT_FILES files) — refusing to measure the mono side"
res mono_attach_ms "$MONO_ATTACH"
# Both figures are reported: the full one is the honest "time to a working
# workspace", the attach-only one is the projection cost.
res mono_ready_full_ms "$MONO_READY"
if [ "$GIT_READY" -gt 0 ] && [ "$MONO_READY" -gt 0 ]; then
  res ready_ratio_x1000 "$(( GIT_READY * 1000 / MONO_READY ))"
fi
res git_ready_ms_vs_mono_clone_x1000 "$(( MONO_CLONE > 0 ? GIT_READY * 1000 / MONO_CLONE : 0 ))"

echo "[R-DISK] real backing (NOT du on the FUSE mountpoint)"
# The store is daemon-wide and cumulative; upper/sealed are the per-mount layers.
res mono_store_bytes "$(du -sb /var/lib/scorpiofs/store 2>/dev/null | cut -f1)"
res mono_upper_bytes "$(du -sb /var/lib/scorpiofs/antares/upper 2>/dev/null | cut -f1)"
res mono_sealed_bytes "$(du -sb /var/lib/scorpiofs/antares/cl 2>/dev/null | cut -f1)"

cd /tmp/wt || abort "cannot enter the worktree"

echo "[R-STATUS] libra status vs git status"
for r in 1 2 3; do
  t0=$(now); libra status >/dev/null 2>&1; t1=$(now); echo "    libra-status-ms r$r=$((t1-t0))"; res libra_status_ms $((t1-t0))
done
for r in 1 2 3; do
  t0=$(now); ( cd /tmp/gitclone && git status --porcelain >/dev/null 2>&1 ); t1=$(now)
  echo "    git-status-ms   r$r=$((t1-t0))"; res git_status_ms $((t1-t0))
done

echo "[R-LSFILES] ls-files (index dump) vs -t (needs worktree state)"
t0=$(now); libra ls-files >/dev/null 2>&1; t1=$(now); echo "    ls-files-ms=$((t1-t0))";   res libra_lsfiles_ms $((t1-t0))
t0=$(now); libra ls-files -t >/dev/null 2>&1; t1=$(now); echo "    ls-files-t-ms=$((t1-t0))"; res libra_lsfiles_t_ms $((t1-t0))

echo "[R-ADD] add -A"
t0=$(now); libra add -A >/dev/null 2>&1; t1=$(now); echo "    add-0change-ms=$((t1-t0))"; res libra_add_0_ms $((t1-t0))
echo "bench $(now)" >> bench-notes.txt
t0=$(now); libra add -A >/dev/null 2>&1; t1=$(now); echo "    add-1change-ms=$((t1-t0))"; res libra_add_1_ms $((t1-t0))

echo "[R-COMMIT] commit"
t0=$(now); libra commit -m "L bench" >/tmp/commit.err; RC=$?; t1=$(now)
echo "    commit-ms=$((t1-t0)) (rc=$RC)"
[ "$RC" = 0 ] || { echo "    commit stderr: $(tail -2 /tmp/commit.err)" >&2; abort "commit failed"; }
res libra_commit_ms $((t1-t0))

echo "[R-READ] 100 random files, cold then hot"
head -100 /dev/urandom >/dev/null 2>&1 || true
find /tmp/wt -type f 2>/dev/null | awk 'NR%5000==0' | head -100 > /tmp/readlist
sync; echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
t0=$(now); while IFS= read -r f; do head -c 64 "$f" >/dev/null 2>&1; done < /tmp/readlist; t1=$(now)
echo "    read-cold-ms=$((t1-t0))"; res libra_read_cold_ms $((t1-t0))
t0=$(now); while IFS= read -r f; do head -c 64 "$f" >/dev/null 2>&1; done < /tmp/readlist; t1=$(now)
echo "    read-hot-ms=$((t1-t0))";  res libra_read_hot_ms $((t1-t0))

echo "[R-MULTI] 5 extra worktrees: git worktree vs libra chain fork"
N5=5
# git side: one clone already exists; add 5 worktrees and measure the delta
GIT_DISK_BEFORE=$(du -sb /tmp/gitclone | cut -f1)
t0=$(now)
for i in 1 2 3 4 5; do git -C /tmp/gitclone worktree add -q "/tmp/gwt$i" -b "gwt$i" 2>/dev/null; done
t1=$(now)
GIT_MULTI_MS=$((t1-t0))
GIT_DISK_AFTER=$(du -sb /tmp/gitclone | cut -f1)
GIT_EXTRA=0
for i in 1 2 3 4 5; do GIT_EXTRA=$((GIT_EXTRA + $(du -sb "/tmp/gwt$i" 2>/dev/null | cut -f1))); done
echo "    git:   multi-ms=$GIT_MULTI_MS  extra-disk=$GIT_EXTRA"
res git_multi_ms "$GIT_MULTI_MS"; res git_multi_extra_disk_bytes "$GIT_EXTRA"

# mono side: chain forks are zero-copy renames of the upper layer
MONO_UPPER_BEFORE=$(du -sb /var/lib/scorpiofs/antares/upper 2>/dev/null | cut -f1)
t0=$(now)
for i in 1 2 3 4 5; do libra /tmp/wt fork -b "Lfork$i-$$" "/tmp/mfork$i" >/dev/null 2>&1; done
t1=$(now)
MONO_MULTI_MS=$((t1-t0))
MONO_UPPER_AFTER=$(du -sb /var/lib/scorpiofs/antares/upper 2>/dev/null | cut -f1)
echo "    mono:  multi-ms=$MONO_MULTI_MS  upper-delta=$((MONO_UPPER_AFTER-MONO_UPPER_BEFORE))"
res mono_multi_ms "$MONO_MULTI_MS"
res mono_multi_extra_disk_bytes "$((MONO_UPPER_AFTER-MONO_UPPER_BEFORE))"

echo "[R-SWITCH] baselines"
# git: switch between two branches that differ in 10% of files
( cd /tmp/gitclone && git checkout -q -b variant 2>/dev/null \
  && find . -type f -name '*.rs' 2>/dev/null | head -10000 | awk 'NR%10==0' \
     | while IFS= read -r f; do echo "SW" >> "$f"; done \
  && git add -A >/dev/null 2>&1 && git commit -qm variant >/dev/null 2>&1 ) || true
for r in 1 2; do
  ( cd /tmp/gitclone && git checkout -q main 2>/dev/null )
  sync
  t0=$(now); ( cd /tmp/gitclone && git checkout -q variant 2>/dev/null ); t1=$(now)
  echo "    git-switch-ms r$r=$((t1-t0))"; res git_switch_ms $((t1-t0))
done

mark "bench-end"
echo
echo "[R-RESOURCE] sampler summary (from the phase-marked series)"
echo "    raw lines: $(wc -l < $SAMP)"
awk '
  /^S / { if ($5 ~ /rss_kb=/) { split($5,a,"="); if (a[2]+0>max) max=a[2]+0 }
           if ($6 ~ /fds=/)    { split($6,a,"="); if (a[2]+0>fdmax) fdmax=a[2]+0 } }
  END { printf "    peak rss_kb=%d  peak fds=%d\n", max, fdmax }
' "$SAMP"

echo
echo "======== PHASE MARKERS (ms epoch) ========"
grep '^M ' "$SAMP" | head -30 | sed 's/^/    /'

echo
echo "======== SAMPLER SERIES (1 Hz, full) ========"
cat "$SAMP" | sed 's/^/    /'

echo "======== DONE ========"
if [ "${FAILED:-0}" = 0 ]; then
  echo "RUN STATUS: COMPLETE — all prerequisites verified"
else
  echo "RUN STATUS: INCOMPLETE"
fi
kill $SAMPLER_PID 2>/dev/null || true
kill $DAEMON_PID  2>/dev/null || true
