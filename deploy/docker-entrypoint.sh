#!/bin/sh
# Container entrypoint for ScorpioFS.
#
# For `serve`, require the mega backend URLs so a container started without them
# fails loudly instead of silently defaulting to localhost (which would look
# healthy via /health but never reach a real backend).
set -e

cmd="${1:-serve}"
if [ "$cmd" = "serve" ]; then
    : "${SCORPIO_BASE_URL:?SCORPIO_BASE_URL must be set (mega server base URL)}"
    : "${SCORPIO_LFS_URL:?SCORPIO_LFS_URL must be set (mega LFS URL)}"
fi

exec scorpio --config-path /etc/scorpiofs/scorpio.toml "$@"
