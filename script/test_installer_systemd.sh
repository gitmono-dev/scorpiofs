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

report_test_failure() {
    local status=$?
    printf 'installer systemd test failed at line %s: %s\n' \
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
        stop)
            mock_service_active=0
            if [ -n "${MOCK_STALE_AFTER_STOP:-}" ]; then
                MOCK_ACTIVE_MOUNT=""
                MOCK_STALE_MOUNT="$MOCK_STALE_AFTER_STOP"
                mock_stale_detached=0
            fi
            ;;
        start|restart) mock_service_active=1 ;;
        *) return 0 ;;
    esac
}

findmnt() {
    local include_fstype=0 fstype="${MOCK_MOUNT_FSTYPE:-fuse.scorpiofs}"
    case " $* " in *' TARGET,FSTYPE '*) include_fstype=1 ;; esac
    if [ -n "${MOCK_STALE_MOUNT:-}" ] && [ "$mock_stale_detached" -eq 0 ]; then
        if [ "$include_fstype" -eq 1 ]; then
            printf '%s %s\n' "$MOCK_STALE_MOUNT" "$fstype"
        else
            printf '%s\n' "$MOCK_STALE_MOUNT"
        fi
    fi
    if [ -n "${MOCK_ACTIVE_MOUNT:-}" ]; then
        if [ "$include_fstype" -eq 1 ]; then
            printf '%s %s\n' "$MOCK_ACTIVE_MOUNT" "$fstype"
        else
            printf '%s\n' "$MOCK_ACTIVE_MOUNT"
        fi
    fi
    if [ -n "${MOCK_NESTED_MOUNT:-}" ]; then
        if [ "$include_fstype" -eq 1 ]; then
            printf '%s ext4\n' "$MOCK_NESTED_MOUNT"
        else
            printf '%s\n' "$MOCK_NESTED_MOUNT"
        fi
    fi
}

stat() {
    if [ -n "${MOCK_STALE_MOUNT:-}" ] && [ "${*: -1}" = "$MOCK_STALE_MOUNT" ] && \
        [ "$mock_stale_detached" -eq 0 ]; then
        return 1
    fi
    if [ -n "${MOCK_ACTIVE_MOUNT:-}" ] && [ "${*: -1}" = "$MOCK_ACTIVE_MOUNT" ]; then
        return 0
    fi
    command /usr/bin/stat "$@"
}

fusermount3() {
    printf 'fusermount3 %s\n' "$*" >>"$systemctl_log"
    if [ -n "${MOCK_STALE_MOUNT:-}" ] && [ "${*: -1}" = "$MOCK_STALE_MOUNT" ]; then
        mock_stale_detached=1
    fi
}

umount() {
    printf 'umount %s\n' "$*" >>"$systemctl_log"
    return 0
}

curl() {
    local arg saw_noproxy=0
    for arg in "$@"; do
        [ "$arg" = "--noproxy" ] && saw_noproxy=1
        if [[ "$arg" == */health ]]; then
            [ "$saw_noproxy" -eq 1 ] || return 64
            [ "${MOCK_HEALTH_FAIL:-0}" -eq 0 ] || return 7
            printf 'ok\n'
            return 0
        fi
    done
    command curl "$@"
}

usermod() {
    return 0
}

export -f install systemctl findmnt stat fusermount3 umount usermod curl
export unit_capture systemctl_log service_user mock_service_active mock_stale_detached

assert_systemctl_log_line() {
    local expected="$1"
    if ! grep -Fxq "$expected" "$systemctl_log"; then
        printf 'missing systemctl mock log entry: %s\nactual log:\n' "$expected" >&2
        cat "$systemctl_log" >&2
        exit 1
    fi
}

assert_unit_contains() {
    local expected="$1"
    if ! grep -Fq "$expected" "$unit_capture"; then
        printf 'missing generated unit entry: %s\nactual unit:\n' "$expected" >&2
        cat "$unit_capture" >&2
        exit 1
    fi
}

non_fuse_root="${test_root}/non-fuse"
mkdir -p "$non_fuse_root"
if MOCK_STALE_MOUNT="${non_fuse_root}/data/mount" MOCK_MOUNT_FSTYPE=nfs \
    SCORPIO_SERVICE_USER="$service_user" bash "${repo_root}/install.sh" \
        --version "$version" \
        --release-base-url "$release_base_url" \
        --non-interactive \
        --overwrite-config \
        --no-deps \
        --no-user-allow-other \
        --base-url https://mega.example.com \
        --lfs-url https://mega.example.com/lfs \
        --prefix "${non_fuse_root}/prefix" \
        --config-dir "${non_fuse_root}/etc" \
        --data-root "${non_fuse_root}/data" \
        --workspace "${non_fuse_root}/data/mount" \
        --store-path "${non_fuse_root}/data/store" \
        --http-addr 127.0.0.1:2925 >"${non_fuse_root}/install.log" 2>&1; then
    printf 'installer detached a non-FUSE mount\n' >&2
    exit 1
fi
grep -Fq 'mounted with non-FUSE filesystem type nfs' "${non_fuse_root}/install.log"
if grep -Eq '^(fusermount3|umount) ' "$systemctl_log"; then
    printf 'installer invoked an unmount helper for a non-FUSE mount\n' >&2
    exit 1
fi
test ! -e "${non_fuse_root}/prefix/bin/scorpio"

unmanaged_root="${test_root}/unmanaged"
mkdir -p "$unmanaged_root"
if MOCK_ACTIVE_MOUNT="${unmanaged_root}/data/mount" \
    SCORPIO_SERVICE_USER="$service_user" bash "${repo_root}/install.sh" \
        --version "$version" \
        --release-base-url "$release_base_url" \
        --non-interactive \
        --overwrite-config \
        --no-deps \
        --no-user-allow-other \
        --base-url https://mega.example.com \
        --lfs-url https://mega.example.com/lfs \
        --prefix "${unmanaged_root}/prefix" \
        --config-dir "${unmanaged_root}/etc" \
        --data-root "${unmanaged_root}/data" \
        --workspace "${unmanaged_root}/data/mount" \
        --store-path "${unmanaged_root}/data/store" \
        --http-addr 127.0.0.1:2925 >"${unmanaged_root}/install.log" 2>&1; then
    printf 'installer accepted an unmanaged active FUSE mount\n' >&2
    exit 1
fi
grep -Fq 'stop the ScorpioFS daemon, unmount this path, and retry' \
    "${unmanaged_root}/install.log"
test ! -e "${unmanaged_root}/prefix/bin/scorpio"
: >"$systemctl_log"

if [ "$service_user" != root ]; then
    inaccessible_root="${test_root}/inaccessible-data"
    inaccessible_user=nobody
    mkdir -p "${inaccessible_root}/etc" "${inaccessible_root}/data"
    chown "$inaccessible_user" "${inaccessible_root}/etc"
    chmod 0711 "${inaccessible_root}/data"
    : >"${inaccessible_root}/data/sentinel"
    chown root:root "${inaccessible_root}/data/sentinel"
    if inaccessible_output="$(sudo -u "$inaccessible_user" -H env -u SUDO_USER \
        bash "${repo_root}/install.sh" \
            --version "$version" \
            --release-base-url "$release_base_url" \
            --non-interactive \
            --dry-run \
            --overwrite-config \
            --no-service \
            --no-deps \
            --no-user-allow-other \
            --base-url https://mega.example.com \
            --lfs-url https://mega.example.com/lfs \
            --prefix "${inaccessible_root}/prefix" \
            --config-dir "${inaccessible_root}/etc" \
            --data-root "${inaccessible_root}/data" \
            --workspace "${inaccessible_root}/data/mount" \
            --store-path "${inaccessible_root}/data/store" \
            --http-addr 127.0.0.1:2925 2>&1)"; then
        printf 'installer accepted an inaccessible nonempty data-root\n' >&2
        exit 1
    fi
    grep -Fq 'could not inspect data-root; refusing to change ownership' <<<"$inaccessible_output"
fi

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

cp "${test_root}/prefix/bin/scorpio" "${test_root}/installed-scorpio"
rm -f "${test_root}/prefix/bin/scorpio"
printf '#!/usr/bin/env bash\nexit 64\n' >"${test_root}/prefix/bin/scorpio"
chmod 0755 "${test_root}/prefix/bin/scorpio"
SCORPIO_SERVICE_USER="$service_user" bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --dry-run \
    --no-service \
    --no-deps \
    --no-user-allow-other \
    --base-url https://ignored.example.com \
    --lfs-url https://ignored.example.com/lfs \
    --prefix "${test_root}/prefix" \
    --config-dir "${test_root}/etc" \
    --http-addr 127.0.0.1:2925 >"${test_root}/old-binary-dry-run.log"
grep -Fq "using runtime paths from retained ${test_root}/etc/scorpio.toml" \
    "${test_root}/old-binary-dry-run.log"
mv "${test_root}/installed-scorpio" "${test_root}/prefix/bin/scorpio"

antares_job_mount="${test_root}/data/antares/mnt/job-1"
MOCK_STALE_MOUNT="$antares_job_mount" \
SCORPIO_SERVICE_USER="$service_user" bash "${repo_root}/install.sh" \
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
    --http-addr 127.0.0.1:2925 >"${test_root}/antares-child-mount.log" 2>&1 || {
        printf 'Antares child-mount installer invocation failed:\n' >&2
        cat "${test_root}/antares-child-mount.log" >&2
        exit 1
    }
assert_systemctl_log_line "fusermount3 -u -z ${antares_job_mount}"

assert_unit_contains 'ExecStopPost=-/bin/sh -c'
assert_unit_contains 'findmnt -rno TARGET 2>/dev/null | sort -r'
assert_unit_contains "ExecStopPost=-/usr/bin/fusermount3 -u -z ${test_root}/data/mount"
assert_systemctl_log_line 'enable scorpiofs.service'
assert_systemctl_log_line 'is-active --quiet scorpiofs.service'
assert_systemctl_log_line 'restart scorpiofs.service'
assert_systemctl_log_line "fusermount3 -u -z ${test_root}/data/mount"
: >"$systemctl_log"

cp "${test_root}/prefix/bin/scorpio" "${test_root}/marked-scorpio"
printf 'old-binary-marker' >>"${test_root}/marked-scorpio"
mv "${test_root}/marked-scorpio" "${test_root}/prefix/bin/scorpio"
cp "${test_root}/etc/scorpio.toml" "${test_root}/config-before-health-failure.toml"
if MOCK_HEALTH_FAIL=1 SCORPIO_SERVICE_USER=nobody bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --overwrite-config \
    --no-deps \
    --no-user-allow-other \
    --base-url https://unhealthy.example.com \
    --lfs-url https://unhealthy.example.com/lfs \
    --prefix "${test_root}/prefix" \
    --config-dir "${test_root}/etc" \
    --data-root "${test_root}/data" \
    --workspace "${test_root}/data/mount" \
    --store-path "${test_root}/data/store" \
    --http-addr 127.0.0.1:2925 >"${test_root}/health-failure.log" 2>&1; then
    printf 'installer accepted a service that failed its health check\n' >&2
    exit 1
fi
grep -Fq 'did not become healthy' "${test_root}/health-failure.log"
grep -Fq 'restoring ScorpioFS artifacts from before the failed upgrade' \
    "${test_root}/health-failure.log"
cmp "${test_root}/config-before-health-failure.toml" "${test_root}/etc/scorpio.toml"
tail -c 17 "${test_root}/prefix/bin/scorpio" | grep -Fxq 'old-binary-marker'
test "$(stat -c '%U' "${test_root}/data/store")" = "$service_user"
test "$(stat -c '%U' "${test_root}/data/antares/upper")" = "$service_user"
test "$(stat -c '%U' "${test_root}/data/antares/cl")" = "$service_user"
test "$(stat -c '%U' "${test_root}/data")" = "$service_user"
grep -Fxq 'stop scorpiofs.service' "$systemctl_log"
test "$(grep -Fc 'start scorpiofs.service' "$systemctl_log")" -ge 2
: >"$systemctl_log"

sed -i 's/^User=.*/User=root/' "$unit_capture"
chmod 0750 "${test_root}/etc"
if SCORPIO_SERVICE_USER=nobody bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --no-deps \
    --no-user-allow-other \
    --base-url https://ignored.example.com \
    --lfs-url https://ignored.example.com/lfs \
    --prefix "${test_root}/prefix" \
    --config-dir "${test_root}/etc" \
    --http-addr 127.0.0.1:2925 >"${test_root}/config-traversal.log" 2>&1; then
    printf 'installer accepted a config directory inaccessible to the new service user\n' >&2
    exit 1
fi
grep -Fq 'cannot traverse config-dir' "${test_root}/config-traversal.log"
if grep -Fxq 'stop scorpiofs.service' "$systemctl_log"; then
    printf 'installer stopped the service before validating config traversal\n' >&2
    exit 1
fi
chmod 0755 "${test_root}/etc"

inactive_root="${test_root}/inactive-service"
mkdir -p "${inactive_root}/data"
mock_service_active=0
if MOCK_ACTIVE_MOUNT="${inactive_root}/data/mount" \
    SCORPIO_SERVICE_USER="$service_user" bash "${repo_root}/install.sh" \
        --version "$version" \
        --release-base-url "$release_base_url" \
        --non-interactive \
        --overwrite-config \
        --no-deps \
        --no-user-allow-other \
        --base-url https://ignored.example.com \
        --lfs-url https://ignored.example.com/lfs \
        --prefix "${test_root}/prefix" \
        --config-dir "${test_root}/etc" \
        --data-root "${inactive_root}/data" \
        --workspace "${inactive_root}/data/mount" \
        --store-path "${inactive_root}/data/store" \
        --http-addr 127.0.0.1:2925 >"${inactive_root}/install.log" 2>&1; then
    printf 'installer accepted an active mount while the existing service was inactive\n' >&2
    exit 1
fi
grep -Fq 'stop the ScorpioFS daemon, unmount this path, and retry' \
    "${inactive_root}/install.log"
mock_service_active=1

MOCK_STALE_MOUNT="${test_root}/data/mount" \
SCORPIO_SERVICE_USER="$service_user" bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --dry-run \
    --no-deps \
    --no-user-allow-other \
    --base-url https://ignored.example.com \
    --lfs-url https://ignored.example.com/lfs \
    --prefix "${test_root}/prefix" \
    --config-dir "${test_root}/etc" \
    --data-root "${test_root}/data" \
    --workspace "${test_root}/data/mount" \
    --store-path "${test_root}/data/store" \
    --http-addr 127.0.0.1:2925 >"${test_root}/mounted-dry-run.log"
grep -Fq "[dry-run] fusermount3 -u -z ${test_root}/data/mount" \
    "${test_root}/mounted-dry-run.log"

: >"$systemctl_log"
new_workspace="${test_root}/data/mount-new"
MOCK_ACTIVE_MOUNT="${test_root}/data/mount" \
MOCK_STALE_AFTER_STOP="${test_root}/data/mount" \
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
    --workspace "$new_workspace" \
    --store-path "${test_root}/data/store" \
    --http-addr 127.0.0.1:2925
grep -Fxq 'stop scorpiofs.service' "$systemctl_log"
grep -Fxq "fusermount3 -u -z ${test_root}/data/mount" "$systemctl_log"
grep -Fxq 'start scorpiofs.service' "$systemctl_log"
grep -Fq "ExecStopPost=-/usr/bin/fusermount3 -u -z ${new_workspace}" "$unit_capture"

if [ "$service_user" != root ] && \
    sudo -u "$service_user" -H env -u SUDO_USER sudo -n -v 2>/dev/null; then
    protected_root="${test_root}/protected-config"
    SCORPIO_SERVICE_USER="$service_user" bash "${repo_root}/install.sh" \
        --version "$version" \
        --release-base-url "$release_base_url" \
        --non-interactive \
        --overwrite-config \
        --no-service \
        --no-deps \
        --no-user-allow-other \
        --base-url https://protected.example.com \
        --lfs-url https://protected.example.com/lfs \
        --prefix "${protected_root}/prefix" \
        --config-dir "${protected_root}/etc" \
        --data-root "${protected_root}/data" \
        --workspace "${protected_root}/data/mount" \
        --store-path "${protected_root}/data/store" \
        --http-addr 127.0.0.1:2925
    chown root:root "${protected_root}/etc"
    chmod 0750 "${protected_root}/etc"
    protected_upgrade_output="$(sudo -u "$service_user" -H env -u SUDO_USER \
        bash "${repo_root}/install.sh" \
            --version "$version" \
            --release-base-url "$release_base_url" \
            --non-interactive \
            --no-service \
            --no-deps \
            --no-user-allow-other \
            --base-url https://ignored.example.com \
            --lfs-url https://ignored.example.com/lfs \
            --prefix "${protected_root}/prefix" \
            --config-dir "${protected_root}/etc" \
            --data-root "${protected_root}/data" \
            --workspace "${protected_root}/data/mount" \
            --store-path "${protected_root}/data/store" \
            --http-addr 127.0.0.1:2925)"
    grep -Fq "using runtime paths from retained ${protected_root}/etc/scorpio.toml" \
        <<<"$protected_upgrade_output"
    grep -Fq 'base_url = "https://protected.example.com"' \
        "${protected_root}/etc/scorpio.toml"
fi

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

chmod 0700 "${test_root}/data/store"
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
test "$(stat -c '%a' "${test_root}/data/store")" = 700

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

runtime_mount="${test_root}/data/store"
if MOCK_NESTED_MOUNT="$runtime_mount" SCORPIO_SERVICE_USER=nobody \
    bash "${repo_root}/install.sh" \
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
        --http-addr 127.0.0.1:2925 >"${test_root}/runtime-root-mount.log" 2>&1; then
    printf 'installer accepted a mount at a persistent runtime root\n' >&2
    exit 1
fi
grep -Fq "refusing ownership migration across mount ${runtime_mount} at runtime directory" \
    "${test_root}/runtime-root-mount.log"

: >"$systemctl_log"
migrated_data_root="${test_root}/migrated-data"
MOCK_ACTIVE_MOUNT="$new_workspace" \
MOCK_STALE_AFTER_STOP="$new_workspace" \
SCORPIO_SERVICE_USER=nobody bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --overwrite-config \
    --no-deps \
    --no-user-allow-other \
    --base-url https://migrated.example.com \
    --lfs-url https://migrated.example.com/lfs \
    --prefix "${test_root}/prefix" \
    --config-dir "${test_root}/etc" \
    --data-root "$migrated_data_root" \
    --http-addr 127.0.0.1:2925
grep -Fxq 'stop scorpiofs.service' "$systemctl_log"
grep -Fxq "fusermount3 -u -z ${new_workspace}" "$systemctl_log"
grep -Fxq 'start scorpiofs.service' "$systemctl_log"
grep -Fq "ExecStopPost=-/usr/bin/fusermount3 -u -z ${migrated_data_root}/mount" \
    "$unit_capture"
grep -Fq "workspace = \"${migrated_data_root}/mount\"" "${test_root}/etc/scorpio.toml"

nested_runtime_root="${test_root}/nested-runtime"
SCORPIO_SERVICE_USER=nobody bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --overwrite-config \
    --no-service \
    --no-deps \
    --no-user-allow-other \
    --base-url https://nested.example.com \
    --lfs-url https://nested.example.com/lfs \
    --prefix "${nested_runtime_root}/prefix" \
    --config-dir "${nested_runtime_root}/etc" \
    --data-root "${nested_runtime_root}/data" \
    --workspace "${nested_runtime_root}/data/mount" \
    --store-path "${nested_runtime_root}/data/private/store" \
    --http-addr 127.0.0.1:2925
test "$(stat -c '%U' "${nested_runtime_root}/data/private")" = nobody

relative_root="${test_root}/relative-config"
mkdir -p "${relative_root}/etc" "${relative_root}/data"
cp "${test_root}/etc/scorpio.toml" "${relative_root}/etc/scorpio.toml"
sed -i \
    -e 's|^workspace = .*|workspace = "mount"|' \
    -e 's|^store_path = .*|store_path = "store"|' \
    -e 's|^config_file = .*|config_file = "config.toml"|' \
    -e 's|^antares_upper_root = .*|antares_upper_root = "antares/upper"|' \
    -e 's|^antares_cl_root = .*|antares_cl_root = "antares/cl"|' \
    -e 's|^antares_mount_root = .*|antares_mount_root = "antares/mnt"|' \
    -e 's|^antares_state_file = .*|antares_state_file = "antares/state.toml"|' \
    "${relative_root}/etc/scorpio.toml"
: >"${relative_root}/data/sentinel"
if SCORPIO_SERVICE_USER=nobody bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --overwrite-config \
    --no-service \
    --no-deps \
    --no-user-allow-other \
    --base-url https://ignored.example.com \
    --lfs-url https://ignored.example.com/lfs \
    --prefix "${test_root}/prefix" \
    --config-dir "${relative_root}/etc" \
    --data-root "${relative_root}/data" \
    --http-addr 127.0.0.1:2925 >"${relative_root}/install.log" 2>&1; then
    printf 'installer accepted an ambiguous all-relative config migration\n' >&2
    exit 1
fi
grep -Fq 'cannot safely use a nonempty data-root with an all-relative retained config' \
    "${relative_root}/install.log"

relative_empty_root="${test_root}/relative-empty"
mkdir -p "${relative_empty_root}/etc" "${relative_empty_root}/data"
cp "${relative_root}/etc/scorpio.toml" "${relative_empty_root}/etc/scorpio.toml"
if SCORPIO_SERVICE_USER=nobody bash "${repo_root}/install.sh" \
    --version "$version" \
    --release-base-url "$release_base_url" \
    --non-interactive \
    --no-service \
    --no-deps \
    --no-user-allow-other \
    --base-url https://ignored.example.com \
    --lfs-url https://ignored.example.com/lfs \
    --prefix "${test_root}/prefix" \
    --config-dir "${relative_empty_root}/etc" \
    --data-root "${relative_empty_root}/data" \
    --http-addr 127.0.0.1:2925 >"${relative_empty_root}/install.log" 2>&1; then
    printf 'installer accepted an empty data-root with an all-relative retained config\n' >&2
    exit 1
fi
grep -Fq 'cannot safely use an empty data-root with an all-relative retained config' \
    "${relative_empty_root}/install.log"

symlinked_data_root="${test_root}/data-link"
ln -s "$migrated_data_root" "$symlinked_data_root"
sed -i "s|${migrated_data_root}|${symlinked_data_root}|g" "${test_root}/etc/scorpio.toml"
SCORPIO_SERVICE_USER=nobody bash "${repo_root}/install.sh" \
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
    --http-addr 127.0.0.1:2925 >"${test_root}/symlinked-root.log"
grep -Fq "using data-root inferred from retained config: ${migrated_data_root}" \
    "${test_root}/symlinked-root.log"

printf 'installer systemd generation and restart test passed\n'
