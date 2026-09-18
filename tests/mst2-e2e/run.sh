#!/usr/bin/env bash
# One-command MST/2 end-to-end acceptance.
#
#   tests/mst2-e2e/run.sh          # build, run, tear down
#   tests/mst2-e2e/run.sh --logs   # also stream the stack logs on failure
#
# Requires: Docker Engine with Compose v2, /dev/fuse on the host, and the
# sibling `monoengine` / `mst2-codec` checkouts next to this repository.
set -uo pipefail
cd "$(dirname "$0")/../.."

COMPOSE=(docker compose
  -f docker-compose.yml
  -f docker-compose.mst2-e2e.yml
  -p mst2-e2e)

cleanup() {
  status=$?
  if [ "$status" != "0" ] && [ "${1:-}" = "--logs" ]; then
    "${COMPOSE[@]}" logs --no-color | tail -80
  fi
  "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1
  exit "$status"
}
trap 'cleanup "${1:-}"' EXIT

"${COMPOSE[@]}" build
"${COMPOSE[@]}" up \
  --abort-on-container-exit \
  --exit-code-from mst2-e2e
