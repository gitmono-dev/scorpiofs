#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
    printf 'usage: %s <version> <release-base-url> <test-root>\n' "$0" >&2
    exit 2
fi
[ "$(id -u)" -eq 0 ] || { printf 'this test must run as root\n' >&2; exit 2; }

version="$1"
release_base_url="$2"
test_root="$3"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
service_user="${SUDO_USER:-root}"
unit_capture="${test_root}/scorpiofs.service"
systemctl_log="${test_root}/systemctl.log"
mock_service_active=1
mock_stale_detached=0

mkdir -p "$test_root"
: >"$systemctl_log"

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

findmnt() {
    if [ -n "${MOCK_STALE_MOUNT:-}" ] && [ "$mock_stale_detached" -eq 0 ]; then
        printf '%s\n' "$MOCK_STALE_MOUNT"
    fi
    if [ -n "${MOCK_NESTED_MOUNT:-}" ]; then
        printf '%s\n' "$MOCK_NESTED_MOUNT"
    fi
}

stat() {
    if [ -n "${MOCK_STALE_MOUNT:-}" ] && [ "${*: -1}" = "$MOCK_STALE_MOUNT" ] && \
        [ "$mock_stale_detached" -eq 0 ]; then
        return 1
    fi
    command /usr/bin/stat "$@"
}

fusermount3() {
    printf 'fusermount3 %s\n' "$*" >>"$systemctl_log"
    if [ -n "${MOCK_STALE_MOUNT:-}" ] && [ "${*: -1}" = "$MOCK_STALE_MOUNT" ]; then
        mock_stale_detached=1
    fi
}

usermod() {
    return 0
}

export -f install systemctl findmnt stat fusermount3 usermod
export unit_capture systemctl_log service_user mock_service_active mock_stale_detached

MOCK_STALE_MOUNT="${test_root}/data/mount" \
SCORPIO_SERVICE_USER="$service_user" bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --overwrite-config \
    --no-deps \
    --no-user-allow-other \
    --base-url https://mega.example.com \
    --lfs-url https://mega.example.com/lfs \
    --prefix "${test_root}/prefix" \
    --config-dir "${test_root}/etc" \
    --data-root "${test_root}/data" \
    --workspace "${test_root}/data/mount" \
    --store-path "${test_root}/data/store" \
    --http-addr 127.0.0.1:2925

grep -Fq 'ExecStopPost=-/bin/sh -c' "$unit_capture"
grep -Fq -- "--submounts ${test_root}/data/antares/mnt" "$unit_capture"
grep -Fq "ExecStopPost=-/usr/bin/fusermount3 -u -z ${test_root}/data/mount" "$unit_capture"
grep -Fxq 'enable scorpiofs.service' "$systemctl_log"
grep -Fxq 'is-active --quiet scorpiofs.service' "$systemctl_log"
grep -Fxq 'restart scorpiofs.service' "$systemctl_log"
grep -Fxq "fusermount3 -u -z ${test_root}/data/mount" "$systemctl_log"

SCORPIO_SERVICE_USER=nobody bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --no-deps \
    --no-user-allow-other \
    --base-url https://ignored.example.com \
    --lfs-url https://ignored.example.com/lfs \
    --prefix "${test_root}/prefix" \
    --config-dir "${test_root}/etc" \
    --data-root "${test_root}/data" \
    --workspace "${test_root}/data/mount" \
    --store-path "${test_root}/data/store" \
    --http-addr 127.0.0.1:2925
grep -Fxq 'stop scorpiofs.service' "$systemctl_log"
grep -Fxq 'start scorpiofs.service' "$systemctl_log"
grep -Fxq 'User=nobody' "$unit_capture"
test "$(stat -c '%U' "${test_root}/etc/scorpio.toml")" = nobody

SUDO_USER=daemon bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --no-service \
    --no-deps \
    --no-user-allow-other \
    --base-url https://ignored.example.com \
    --lfs-url https://ignored.example.com/lfs \
    --prefix "${test_root}/prefix" \
    --config-dir "${test_root}/etc" \
    --data-root "${test_root}/data" \
    --workspace "${test_root}/data/mount" \
    --store-path "${test_root}/data/store" \
    --http-addr 127.0.0.1:2925
test "$(stat -c '%U' "${test_root}/etc/scorpio.toml")" = nobody

nested_mount="${test_root}/data/store/external-mount"
if MOCK_NESTED_MOUNT="$nested_mount" bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --no-service \
    --no-deps \
    --no-user-allow-other \
    --base-url https://ignored.example.com \
    --lfs-url https://ignored.example.com/lfs \
    --prefix "${test_root}/prefix" \
    --config-dir "${test_root}/etc" \
    --data-root "${test_root}/data" \
    --workspace "${test_root}/data/mount" \
    --store-path "${test_root}/data/store" \
    --http-addr 127.0.0.1:2925 >"${test_root}/nested-mount.log" 2>&1; then
    printf 'installer accepted a nested mount during ownership migration\n' >&2
    exit 1
fi
grep -Fq "refusing ownership migration across nested mount ${nested_mount}" \
    "${test_root}/nested-mount.log"

printf 'installer systemd generation and restart test passed\n'
