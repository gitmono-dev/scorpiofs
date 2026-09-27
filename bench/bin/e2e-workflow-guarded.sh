#!/usr/bin/env bash
# Complete Libra + ScorpioFS workflow, now WITH a safety guard.
#
# The guard exists because of a real data-loss incident: when mega2 hit its
# fd limit it returned HTTP 500 for tree fetches, the FUSE mount showed empty
# directories, libra's scan read "absent" as "deleted", and `libra sync`
# committed 124008 deletions. Never sync on a scan you have not sanity-checked.
set -uo pipefail
export LIBRA="$HOME/libra-rel"
export LIBRA_SCORPIOFS_ENDPOINT=http://127.0.0.1:37251/antares
M2=http://127.0.0.1:19000
DAEMON=$LIBRA_SCORPIOFS_ENDPOINT
W="$HOME/gitmono-flow2"
MAIN="$W/main"; WT="$W/wt"
MAX_DELETIONS="${MAX_DELETIONS:-50}"   # abort if a scan reports more than this

banner() { echo; echo "======== $1 ========"; }

banner "1) clone + attach ScorpioFS worktree"
rm -rf "$W"; mkdir -p "$W"
( cd "$HOME" && "$LIBRA" clone -q -b main --no-checkout "$M2/project" "$MAIN" )
cd "$MAIN"
"$LIBRA" config set user.name flow2 >/dev/null
"$LIBRA" config set user.email flow2@gitmono.local >/dev/null
"$LIBRA" worktree add --backend scorpiofs -b flow2-$RANDOM "$WT" 2>&1 | tail -3
sleep 3
MID=$(cat "$WT/.libra/scorpiofs_mount_id" 2>/dev/null)
echo "  mount_id: ${MID:-<MISSING>}"
[ -n "$MID" ] || { echo "ABORT: attach did not produce a mount"; exit 1; }

banner "2) sanity: the mount must be populated"
FILES=$(find "$WT" -type f 2>/dev/null | wc -l)
echo "  files visible through the mount: $FILES"
[ "$FILES" -gt 1000 ] || { echo "ABORT: mount looks empty"; exit 1; }

banner "3) baseline status (must be clean)"
DEL_BEFORE=$(timeout 300 "$LIBRA" status 2>/dev/null | grep -cE '^\s*deleted:' || true)
echo "  deletions reported at baseline: $DEL_BEFORE"
if [ "${DEL_BEFORE:-0}" -gt "$MAX_DELETIONS" ]; then
  echo "ABORT: baseline reports $DEL_BEFORE deletions (limit $MAX_DELETIONS) —"
  echo "       this is the fd-exhaustion signature; refusing to proceed."
  exit 1
fi

banner "4) modify code through the mount"
cat > "$WT/scorpiofs-demo-note.md" <<EOF
# ScorpioFS workflow demo

Written **through the FUSE mount** at $(date -u +%FT%TZ).
EOF
SRC=$(find "$WT" -type f -name '*.rs' 2>/dev/null | head -1)
[ -n "$SRC" ] && { echo "// touched $(date -u +%FT%TZ)" >> "$SRC"; echo "  modified ${SRC#$WT/}"; }
echo "  created scorpiofs-demo-note.md"

banner "5) status after the edit"
"$LIBRA" status 2>&1 | tail -8
DEL_AFTER=$(timeout 300 "$LIBRA" status 2>/dev/null | grep -cE '^\s*deleted:' || true)
echo "  deletions: $DEL_AFTER (limit $MAX_DELETIONS)"
if [ "${DEL_AFTER:-0}" -gt "$MAX_DELETIONS" ]; then
  echo "ABORT: refusing to sync a scan with $DEL_AFTER deletions"; exit 1
fi

banner "6) sync"
"$LIBRA" sync -m "demo: scorpiofs workflow (guarded)" 2>&1 | tail -6

banner "7) verify on the server"
curl -sS --noproxy '*' "$M2/api/v1/blob?path=/project/scorpiofs-demo-note.md"; echo
echo -n "  tree still intact? file count via a fresh clone: "
rm -rf "$W/verify"; ( cd "$W" && "$LIBRA" clone -q -b main "$M2/project" verify )
find "$W/verify" -type f -not -path '*/.libra/*' -not -path '*/.git/*' 2>/dev/null | wc -l
cat "$W/verify/scorpiofs-demo-note.md" 2>/dev/null | head -3
