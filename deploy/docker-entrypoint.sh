#!/bin/sh
# Container entrypoint for ScorpioFS.
#
# For `serve`, require the mega backend URLs so a container started without them
# fails loudly instead of silently defaulting to localhost (which would look
# healthy via /health but never reach a real backend).
set -e

# We're in "serve" mode when the effective subcommand is `serve` — either
# explicit, or the default when no subcommand is given (possibly after global
# flags like `--log-level x serve`). Detect it by the ABSENCE of any other
# subcommand or a help/version flag among the arguments, so a global flag before
# `serve` can't slip past the backend-URL check.
is_serve=1
for arg in "$@"; do
    case "$arg" in
    workspace | config | doctor | completions | help | -h | --help | -V | --version)
        is_serve=0
        break
        ;;
    esac
done

if [ "$is_serve" -eq 1 ]; then
    : "${SCORPIO_BASE_URL:?SCORPIO_BASE_URL must be set (mega server base URL)}"
    : "${SCORPIO_LFS_URL:?SCORPIO_LFS_URL must be set (mega LFS URL)}"
fi

exec scorpio --config-path /etc/scorpiofs/scorpio.toml "$@"

