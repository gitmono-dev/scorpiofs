#!/bin/sh
# Container entrypoint for ScorpioFS.
#
# For `serve`, require the MST/2 endpoint so a container started without it
# fails loudly instead of silently defaulting to localhost (which would look
# healthy via /health but never reach a real backend).
set -e

# We're in "serve" mode when the effective subcommand is `serve` — either
# explicit, or the default when no subcommand is given (possibly after global
# flags like `--log-level x serve`). Detect it by the ABSENCE of any other
# subcommand or a help/version flag among the arguments, so a global flag before
# `serve` can't slip past the backend-URL check.
is_serve=1
has_mst2_endpoint=0
expect_mst2_endpoint=0
for arg in "$@"; do
    if [ "$expect_mst2_endpoint" -eq 1 ]; then
        [ -z "$arg" ] || has_mst2_endpoint=1
        expect_mst2_endpoint=0
    fi
    case "$arg" in
    --mst2-base-url) expect_mst2_endpoint=1 ;;
    --mst2-base-url=*) [ -z "${arg#*=}" ] || has_mst2_endpoint=1 ;;
    workspace | config | doctor | completions | help | -h | --help | -V | --version)
        is_serve=0
        break
        ;;
    esac
done

if [ "$is_serve" -eq 1 ] && [ "$has_mst2_endpoint" -eq 0 ]; then
    : "${SCORPIO_MST2_BASE_URL:?SCORPIO_MST2_BASE_URL must be set (MST/2 snapshot service URL)}"
fi

exec scorpio --config-path /etc/scorpiofs/scorpio.toml "$@"

