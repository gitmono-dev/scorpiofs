#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
    printf 'usage: %s <version> <release-base-url> <test-root>\n' "$0" >&2
    exit 2
fi
[ "$(id -u)" -eq 0 ] || {
    printf 'this test must run as root\n' >&2
    exit 2
}

version="$1"
release_base_url="$2"
test_root="$3"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
service_user="${SUDO_USER:-root}"
unit_capture="${test_root}/scorpiofs.service"
systemctl_log="${test_root}/systemctl.log"
mock_service_active=0

mkdir -p "$test_root"
: >"$systemctl_log"

report_test_failure() {
    local status=$?
    printf 'v3 installer systemd test failed at line %s: %s\n' \
        "${BASH_LINENO[0]}" "$BASH_COMMAND" >&2
    for log in "$test_root"/*/install.log "$test_root"/*.log; do
        [ -f "$log" ] || continue
        printf '%s:\n' "$log" >&2
        tail -20 "$log" >&2
    done
    exit "$status"
}
trap report_test_failure ERR

install() {
    local destination="${*: -1}"
    if [ "$destination" = "/etc/systemd/system/scorpiofs.service" ]; then
        local source="${*: -2:1}"
        command cp "$source" "$unit_capture"
    else
        command /usr/bin/install "$@"
    fi
}

systemctl() {
    printf '%s\n' "$*" >>"$systemctl_log"
    case "${1:-}" in
        show)
            if [ -r "$unit_capture" ]; then
                awk -F= '$1 == "User" { print $2; exit }' "$unit_capture"
            fi
            ;;
        is-active) [ "$mock_service_active" -eq 1 ] ;;
        stop) mock_service_active=0 ;;
        start|restart) mock_service_active=1 ;;
        *) return 0 ;;
    esac
}

curl() {
    local arg saw_noproxy=0
    for arg in "$@"; do
        [ "$arg" = "--noproxy" ] && saw_noproxy=1
        if [[ "$arg" == */health ]]; then
            [ "$saw_noproxy" -eq 1 ] || return 64
            printf 'ok\n'
            return 0
        fi
    done
    command curl "$@"
}

usermod() { return 0; }
export -f install systemctl curl usermod
export unit_capture systemctl_log service_user mock_service_active

v3_root="${test_root}/v3"
mkdir -p "$v3_root"
SCORPIO_SERVICE_USER="$service_user" bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --overwrite-config \
    --no-deps \
    --no-user-allow-other \
    --mst2-base-url https://mega.example.com \
    --prefix "${v3_root}/prefix" \
    --config-dir "${v3_root}/etc" \
    --data-root "${v3_root}/data" \
    --store-path "${v3_root}/data/store" \
    --http-addr 127.0.0.1:2925 >"${v3_root}/install.log" 2>&1

config="${v3_root}/etc/scorpio.toml"
grep -Fxq 'mst2_base_url = "https://mega.example.com"' "$config"
grep -Fxq "store_path = \"${v3_root}/data/store\"" "$config"
if ! grep -Fq 'ExecStart='"${v3_root}"'/prefix/bin/scorpio --config-path '"${v3_root}"'/etc/scorpio.toml serve' \
    "$unit_capture"; then
    printf 'generated unit does not contain the v3 daemon command:\n' >&2
    cat "$unit_capture" >&2
    exit 1
fi
grep -Fq 'User='"${service_user}" "$unit_capture"
grep -Fxq 'start scorpiofs.service' "$systemctl_log"

if bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --dry-run \
    --no-deps \
    --no-service \
    --no-user-allow-other \
    --mst2-base-url file:///invalid \
    --prefix "${test_root}/invalid-prefix" \
    --config-dir "${test_root}/invalid-etc" \
    --data-root "${test_root}/invalid-data" \
    --store-path "${test_root}/invalid-data/store" \
    --http-addr 127.0.0.1:2926 >"${test_root}/invalid.log" 2>&1; then
    printf 'installer accepted a non-HTTP MST/2 URL\n' >&2
    exit 1
fi
grep -Fq 'mst2_base_url must start with http:// or https://' "${test_root}/invalid.log"

printf 'v3 installer systemd tests passed\n'
