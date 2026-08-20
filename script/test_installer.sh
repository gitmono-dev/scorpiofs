#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
installer="${repo_root}/install.sh"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT

common=(
    --version v0.0.0-test
    --non-interactive
    --dry-run
    --no-deps
    --no-service
    --no-user-allow-other
    --base-url https://mega.example.com
    --lfs-url https://mega.example.com/lfs
    --prefix "$test_root/prefix"
    --config-dir "$test_root/etc"
    --data-root "$test_root/data"
    --workspace "$test_root/data/mount"
    --store-path "$test_root/data/store"
    --http-addr 127.0.0.1:2725
)

expect_failure() {
    local label="$1" expected="$2"
    shift 2
    if output="$(bash "$installer" "$@" 2>&1)"; then
        printf 'expected failure for %s\n' "$label" >&2
        exit 1
    fi
    if [[ "$output" != *"$expected"* ]]; then
        printf 'unexpected error for %s:\n%s\n' "$label" "$output" >&2
        exit 1
    fi
}

bash "$installer" "${common[@]}" --http-addr 192.168.1.10:2725 --allow-public-api >/dev/null
bash "$installer" "${common[@]}" \
    --base-url 'http://[::1]:8000' \
    --lfs-url 'http://[::1]:8000/lfs' \
    --http-addr '[::1]:2725' >/dev/null

interactive_public=(
    --version v0.0.0-test
    --dry-run
    --no-deps
    --no-service
    --no-user-allow-other
    --yes
    --allow-public-api
    --base-url https://mega.example.com
    --lfs-url https://mega.example.com/lfs
    --prefix "$test_root/interactive-prefix"
    --config-dir "$test_root/interactive-etc"
    --data-root "$test_root/interactive-data"
    --workspace "$test_root/interactive-data/mount"
    --store-path "$test_root/interactive-data/store"
    --http-addr 192.168.1.10:2725
)
interactive_command="$(printf '%q ' bash "$installer" "${interactive_public[@]}")"
output="$(script -qec "$interactive_command" /dev/null)"
if [[ "$output" != *"health: curl http://192.168.1.10:2725/health"* ]]; then
    printf 'interactive --allow-public-api did not preserve the explicit bind:\n%s\n' "$output" >&2
    exit 1
fi

expect_failure "missing URL host" "must include a host" "${common[@]}" --base-url http://
expect_failure "URL port overflow" "between 1 and 65535" "${common[@]}" --lfs-url http://host:99999
expect_failure "bind port overflow" "between 1 and 65535" "${common[@]}" --http-addr 127.0.0.1:99999
expect_failure "unbracketed IPv6 bind" "IPv4:port or [IPv6]:port" "${common[@]}" --http-addr ::1:2725
expect_failure "unauthorized public bind" "without --allow-public-api" \
    "${common[@]}" --http-addr 192.168.1.10:2725
expect_failure "filesystem root data path" "data-root is too broad" \
    "${common[@]}" --data-root / --workspace /mount --store-path /store
expect_failure "broad /var/lib data path" "data-root is too broad" \
    "${common[@]}" --data-root /var/lib --workspace /var/lib/mount --store-path /var/lib/store
expect_failure "workspace outside data root" "workspace must be inside data-root" \
    "${common[@]}" --workspace /home
expect_failure "workspace overlaps store" "workspace must not overlap store-path" \
    "${common[@]}" --store-path "$test_root/data/mount"
expect_failure "workspace contains installed binary" "workspace must not contain scorpio-binary" \
    "${common[@]}" --prefix "$test_root/data/mount/tools"
expect_failure "Antares mount root contains main config" "antares-mount-root must not contain main-config" \
    "${common[@]}" --config-dir "$test_root/data/antares/mnt/config"
expect_failure "systemd path specifier" "unsafe for shell or systemd" \
    "${common[@]}" --data-root "$test_root/scorpio%Q" \
    --workspace "$test_root/scorpio%Q/mount" --store-path "$test_root/scorpio%Q/store"
expect_failure "cleanup shell metacharacter" "unsafe for shell or systemd" \
    "${common[@]}" --data-root "$test_root/scorpio;false" \
    --workspace "$test_root/scorpio;false/mount" --store-path "$test_root/scorpio;false/store"
SCORPIO_GIT_AUTHOR=$'Bob\bBuilder' expect_failure \
    "TOML author control character" "git author must not contain control characters" \
    "${common[@]}"

no_systemctl_bin="$test_root/no-systemctl-bin"
mkdir -p "$no_systemctl_bin"
for tool in curl tar realpath find findmnt; do
    ln -s "$(command -v "$tool")" "$no_systemctl_bin/$tool"
done
if output="$(PATH="$no_systemctl_bin" /bin/bash "$installer" \
    --version v0.0.0-test \
    --non-interactive \
    --dry-run \
    --no-deps \
    --no-user-allow-other \
    --base-url https://mega.example.com \
    --lfs-url https://mega.example.com/lfs \
    --prefix "$test_root/service-prefix" \
    --config-dir "$test_root/service-etc" \
    --data-root "$test_root/service-data" \
    --workspace "$test_root/service-data/mount" \
    --store-path "$test_root/service-data/store" \
    --http-addr 127.0.0.1:2725 2>&1)"; then
    printf 'expected failure without systemctl\n' >&2
    exit 1
fi
if [[ "$output" != *"systemctl is required for service setup"* ]]; then
    printf 'unexpected missing-systemctl error:\n%s\n' "$output" >&2
    exit 1
fi

mkdir -p "$test_root/missing-bin-etc"
: >"$test_root/missing-bin-etc/scorpio.toml"
expect_failure "retained dry-run without installed binary" \
    "dry-run cannot faithfully resolve retained config" \
    "${common[@]}" \
    --prefix "$test_root/missing-bin-prefix" \
    --config-dir "$test_root/missing-bin-etc"

ln -s / "$test_root/root-link"
expect_failure "symlinked ancestor to broad root" "data-root is too broad" \
    "${common[@]}" --data-root "$test_root/root-link/home" \
    --workspace "$test_root/root-link/home/mount" --store-path "$test_root/root-link/home/store"

mkdir -p "$test_root/existing-data"
: >"$test_root/existing-data/unrelated-file"
expect_failure "nonempty unowned data root" "refusing to change ownership of a nonempty data-root" \
    "${common[@]}" --data-root "$test_root/existing-data" \
    --workspace "$test_root/existing-data/mount" --store-path "$test_root/existing-data/store"

if command -v setsid >/dev/null 2>&1; then
    if output="$(setsid bash "$installer" --version v0.0.0-test --dry-run </dev/null 2>&1)"; then
        printf 'expected failure without a controlling terminal\n' >&2
        exit 1
    fi
    if [[ "$output" != *"requires a controlling terminal"* ]]; then
        printf 'unexpected no-terminal error:\n%s\n' "$output" >&2
        exit 1
    fi
fi

printf 'installer validation tests passed\n'
