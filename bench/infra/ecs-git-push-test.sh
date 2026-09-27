#!/usr/bin/env bash
# Smoke-test the ECS-hosted mega2: clone, commit, push, and read back.
set -uo pipefail
M2=http://127.0.0.1:19001
W=/tmp/gitpush-test

echo "== 1) git version / repo reachable =="
git --version
curl -sS -o /dev/null -w "  openapi: %{http_code}\n" "$M2/api/openapi.json"

echo "== 2) clone the monorepo (mega2 serves git over HTTP) =="
rm -rf "$W"; mkdir -p "$W"; cd "$W"
timeout 60 git clone -q "$M2/project" repo 2>&1 | tail -2
cd "$W/repo" 2>/dev/null || { echo "clone failed"; exit 1; }
git config user.name  smoke
git config user.email smoke@gitmono.local
echo "  files: $(ls -A | wc -l)"

echo "== 3) commit + push =="
STAMP=$(date -u +%FT%TZ)
echo "ecs push test $STAMP" > ecs-test.txt
git add ecs-test.txt
git commit -qm "test: ecs push $STAMP"
echo "  local head: $(git rev-parse --short HEAD)"
timeout 60 git push origin HEAD:refs/heads/main 2>&1 | tail -3

echo "== 4) verify on the server side =="
echo -n "  remote main: "
git ls-remote origin refs/heads/main 2>/dev/null | cut -f1 | head -c 12; echo
echo -n "  file via blob api: "
curl -sS "$M2/api/v1/blob?path=/project/ecs-test.txt" 2>/dev/null | head -c 120; echo

echo "== 5) fresh clone shows the pushed file =="
cd "$W" && rm -rf verify && timeout 60 git clone -q "$M2/project" verify 2>&1 | tail -1
cat "$W/verify/ecs-test.txt" 2>/dev/null || echo "  (not found)"
