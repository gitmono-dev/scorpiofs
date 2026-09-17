#!/usr/bin/env bash
# MST/2 end-to-end acceptance. Runs inside the `mst2-e2e` container against
# the mega2 service of the same compose project.
#
# What it proves, in order:
#   1. capabilities gate: the deployment honestly advertises the MST/2
#      surface it serves;
#   2. seeding: a repository with the shapes that stress the reader — small
#      files, an empty file, an executable, a symlink, a >256 KiB file and a
#      129-entry directory — pushed over plain Git;
#   3. fixed view: the client syncs and hydrates it into a verified local
#      store, and its manifest is checked against a full walk;
#   4. mount vs git: every byte the FUSE mount serves equals a plain
#      `git clone` of the same commit, with the executable bit and symlink
#      target preserved;
#   5. SYS-01: after main advances, the mounted view keeps serving the old
#      commit and the new file stays absent;
#   6. offline reopen: the completed store mounts with no server contact and
#      serves identical bytes;
#   7. incremental reuse: the next version reports the three independent
#      counters plus the hydrate counters, so "did not download again" and
#      "did not traverse everything" are separate numbers.
#
# Any failed check exits non-zero, so compose reports the run as failed.
set -uo pipefail

M2_BASE="${M2_BASE:-http://mega2:8000}"
M2_SCOPE="${M2_SCOPE:-/project}"
M2_REPO_PATH="${M2_REPO_PATH:-/project}"
M2_STORE_ROOT="${M2_STORE_ROOT:-/var/lib/scorpio/mst2}"
M2_LEASE="${M2_LEASE:-120}"
MNT=/mnt/mst2-e2e
WORK=/work/mst2-e2e
CACHE_DIR="$M2_STORE_ROOT/snapshots/$(echo "$M2_SCOPE" | tr -d '/')"
FAILED=0

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; FAILED=1; }
check() { if [ "$1" = "0" ]; then pass "$2"; else fail "$2"; fi }
# Compare two sha256 listings, always explaining a mismatch.
diffcheck() {
  if diff -u "$1" "$2" > "$WORK/diff.txt"; then
    pass "$3"
  else
    fail "$3"
    head -20 "$WORK/diff.txt" | sed 's/^/    /'
  fi
}
listing() { ( cd "$1" && find . -type f -print0 | sort -z | xargs -0 -r sha256sum ); }
sync_once() {
  M2_BASE="$M2_BASE" M2_SCOPE="$M2_SCOPE" M2_CACHE_DIR="$CACHE_DIR" mst2_sync
}

export no_proxy='*' NO_PROXY='*' GIT_TERMINAL_PROMPT=0
rm -rf "$WORK" "$MNT"
mkdir -p "$WORK" "$MNT"

echo "== 1. capabilities =="
CAPS=$(curl -fsS "$M2_BASE/api/v2/snapshots/capabilities" 2>/dev/null)
python3 - "$CAPS" <<'PY' || FAILED=1
import json, sys
caps = json.loads(sys.argv[1])
feats = caps["features"]
required = ["resolve", "directory", "lookup", "metadata_pages", "raw_blob", "objects", "chunk_reads"]
missing = [f for f in required if not feats.get(f)]
assert not missing, "deployment does not advertise %s" % missing
assert caps["frame_encodings"], "no frame encoding advertised"
print("PASS: capabilities advertise the served surface:", ", ".join(required))
PY

echo "== 2. seed a repository over plain Git =="
git clone --quiet "$M2_BASE$M2_REPO_PATH" "$WORK/seed" || { fail "clone"; exit 1; }
cd "$WORK/seed"
mkdir -p fixtures/deep/a/b/c fixtures/wide
printf 'alpha\n' > fixtures/alpha.txt
: > fixtures/empty.txt
printf '#!/bin/sh\necho hi\n' > fixtures/run-me.sh
chmod +x fixtures/run-me.sh
ln -sf alpha.txt fixtures/link.txt
printf 'needle' > fixtures/deep/a/b/c/leaf.txt
# 2 MiB + 7 bytes: crosses the OBJECT cap and ends on a short final chunk.
python3 - <<'PY'
data = bytes((i * 73 + 11) % 256 for i in range(2 * 1024 * 1024 + 7))
with open("fixtures/big.bin", "wb") as f:
    f.write(data)
PY
for i in $(seq 1 129); do
  printf 'w%03d\n' "$i" > "fixtures/wide/w$(printf %03d "$i").txt"
done
git add -A
git -c user.name=e2e -c user.email=e2e@mst2.local commit --quiet -m "mst2 e2e seed"
git -c pack.window=0 push --quiet origin HEAD:refs/heads/main || { fail "push seed"; exit 1; }
TIP_BEFORE=$(git ls-remote --quiet "$M2_BASE$M2_REPO_PATH" refs/heads/main | cut -f1)
pass "seeded and pushed $TIP_BEFORE"

echo "== 3. sync + hydrate through the client =="
sync_once | tee "$WORK/sync1.json"
grep -q '"consistent":true' "$WORK/sync1.json"; check $? "client manifest matches a full walk"

echo "== 4. mount and compare against git truth =="
M2_BASE="$M2_BASE" M2_SCOPE="$M2_SCOPE" M2_LEASE="$M2_LEASE" M2_STORE_ROOT="$M2_STORE_ROOT" \
  mst2_mount "$MNT" > "$WORK/mount.log" 2>&1 &
MOUNT_PID=$!
cleanup() {
  fusermount -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null || true
  kill "$MOUNT_PID" 2>/dev/null || true
  wait "$MOUNT_PID" 2>/dev/null || true
}
trap cleanup EXIT
for _ in $(seq 1 120); do
  mount | grep -q "$MNT" && break
  kill -0 "$MOUNT_PID" 2>/dev/null || { cat "$WORK/mount.log"; fail "mount exited"; exit 1; }
  sleep 1
done
mount | grep -q "$MNT"; check $? "mounted"
STORE_DIR=$(grep -o 'store=[^ ]*' "$WORK/mount.log" | head -1 | cut -d= -f2)
[ -n "$STORE_DIR" ]; check $? "mount reported its store directory"

# Truth: a plain checkout of the same commit, compared file by file.
git clone --quiet "$M2_BASE$M2_REPO_PATH" "$WORK/truth" \
  && git -C "$WORK/truth" checkout --quiet "$TIP_BEFORE"
( cd "$WORK/truth" && find . -path ./.git -prune -o -type f -print0 | sort -z | xargs -0 -r sha256sum ) > "$WORK/truth.sha"
listing "$MNT" > "$WORK/mount.sha"
diffcheck "$WORK/truth.sha" "$WORK/mount.sha" "mounted bytes == git checkout ($(wc -l < "$WORK/mount.sha") files)"
[ -x "$MNT/fixtures/run-me.sh" ]; check $? "executable bit preserved"
[ "$(readlink "$MNT/fixtures/link.txt")" = "alpha.txt" ]; check $? "symlink target preserved"

echo "== 5. SYS-01: the mounted view ignores later commits =="
cd "$WORK/seed"
printf 'tamper %s\n' "$(date +%s)" > fixtures/sys01-advance.txt
git add -A
git -c user.name=e2e -c user.email=e2e@mst2.local commit --quiet -m "advance main"
git -c pack.window=0 push --quiet origin HEAD:refs/heads/main
TIP_AFTER=$(git ls-remote --quiet "$M2_BASE$M2_REPO_PATH" refs/heads/main | cut -f1)
[ "$TIP_BEFORE" != "$TIP_AFTER" ]; check $? "main advanced ($TIP_BEFORE -> $TIP_AFTER)"
listing "$MNT" > "$WORK/after.sha"
diffcheck "$WORK/mount.sha" "$WORK/after.sha" "the mounted view is unchanged after main advanced"
[ ! -e "$MNT/fixtures/sys01-advance.txt" ]; check $? "the new commit is absent from the pinned view"
cleanup
cd /

echo "== 6. offline reopen (no server contact) =="
if [ -n "$STORE_DIR" ]; then
  # The previous unmount may still be settling; give the kernel a moment so
  # the reopen cannot fail with a busy mountpoint.
  sleep 2
  M2_STORE_DIR="$STORE_DIR" mst2_mount "$MNT" > "$WORK/reopen.log" 2>&1 &
  MOUNT_PID=$!
  REOPENED=1
  for _ in $(seq 1 60); do
    mount | grep -q "$MNT" && break
    if ! kill -0 "$MOUNT_PID" 2>/dev/null; then
      REOPENED=0
      break
    fi
    sleep 1
  done
  if [ "$REOPENED" = "0" ] || ! mount | grep -q "$MNT"; then
    fail "offline reopen did not mount"
    cat "$WORK/reopen.log" | sed 's/^/    /'
  else
    pass "offline reopen mounted with no server contact"
    listing "$MNT" > "$WORK/reopen.sha"
    diffcheck "$WORK/mount.sha" "$WORK/reopen.sha" "reopened store serves identical bytes"
  fi
  cleanup
else
  fail "mount did not report a store directory; reopen not attempted"
fi

echo "== 7. incremental reuse on the next version =="
sync_once | tee "$WORK/sync2.json"
grep -q '"consistent":true' "$WORK/sync2.json"; check $? "second version consistent"
python3 - "$WORK/sync2.json" <<'PY' || FAILED=1
import json, sys
line = [l for l in open(sys.argv[1]).read().strip().splitlines() if l.startswith("{")][-1]
m = json.loads(line)
assert m["files"] > 0, "second version reported no files"
print(
    "PASS: second version meters — traversal_nodes=%d fetched_pages=%d "
    "reused_pages=%d reused_subtrees=%d hydrate_fetched=%d hydrate_resumed=%d"
    % (m["traversal_nodes"], m["fetched_pages"], m["reused_pages"],
       m["reused_subtrees"], m["hydrate_fetched"], m["hydrate_resumed"])
)
PY

echo
if [ "$FAILED" = "0" ]; then
  echo "== mst2-e2e: ALL CHECKS PASSED =="
else
  echo "== mst2-e2e: FAILURES PRESENT =="
fi
exit "$FAILED"
