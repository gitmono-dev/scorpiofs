#!/usr/bin/env bash
set -Eeuo pipefail
if [ "$#" -ne 3 ]; then
    printf 'usage: %s <version> <release-base-url> <test-root>\n' "$0" >&2
    exit 2
fi
[ "$(id -u)" -eq 0 ] || { printf 'this test must run as root\n' >&2; exit 2; }
version="$1"
release_base_url="$2"
test_root="$3"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
service_user="$(printenv SUDO_USER || id -un)"
unit_capture="$test_root/scorpiofs.service"
systemctl_log="$test_root/systemctl.log"
mock_phase="$test_root/mock-phase"
mock_mount_reads="$test_root/mock-mount-reads"
mock_pid_reads="$test_root/mock-pid-reads"
mock_capture_reads="$test_root/mock-capture-reads"
mock_service_active=0
mock_stale_detached=0
mock_pid=424242
mock_workspace_id=11111111-2222-4333-8444-555555555555
mock_generation=aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee
MOCK_RAW_ACTIVE=0
MOCK_ATTACK=""
MOCK_STORE=""
MOCK_PREFIX=""
MOCK_FOREIGN_ROOT=""
MOCK_HEALTH_FAIL=0
MOCK_RETIRED_ALIAS_PATH=""
MOCK_TARGET_USER="$service_user"
mkdir -p "$test_root"
: >"$systemctl_log"
printf 'before\n' >"$mock_phase"
printf '0\n' >"$mock_mount_reads"
printf '0\n' >"$mock_pid_reads"
printf '0\n' >"$mock_capture_reads"
report_test_failure() {
    local status=$?
    printf 'installer v3 fixture failed: %s\n' "$BASH_COMMAND" >&2
    for log in "$test_root"/*.log; do
        [ -f "$log" ] || continue
        printf '%s:\n' "$log" >&2
        tail -15 "$log" >&2
    done
    exit "$status"
}
trap report_test_failure ERR
phase() {
    local current
    IFS= read -r current <"$mock_phase"
    printf '%s' "$current"
}
mount_target() { printf '%s/workspaces-v3/%s/mount' "$MOCK_STORE" "$mock_workspace_id"; }
mount_records() {
    [ "$MOCK_RAW_ACTIVE" -eq 1 ] || return 0
    [ "$mock_stale_detached" -eq 0 ] || return 0
    local target uid source=scorpiofs-v3 fstype=fuse mount_id=8123 current reads
    target="$(mount_target)"
    uid="$(id -u "$service_user")"
    current="$(phase)"
    IFS= read -r reads <"$mock_mount_reads"
    case "$MOCK_ATTACK" in
        source) source=foreign-fuse ;;
        mount-uid) uid=123456 ;;
        non-fuse) fstype=nfs ;;
        foreign-path) target="$MOCK_STORE/external-mount" ;;
        foreign-data) target="$(dirname "$MOCK_STORE")/external-mount" ;;
        changed-mount) [ "$current" != stopped ] || mount_id=8124 ;;
        late-target) [ "$reads" -lt 2 ] || mount_id=8124 ;;
    esac
    printf '%s 1 0:71 / %s rw,nosuid,nodev - %s %s rw,user_id=%s,group_id=%s\n' \
        "$mount_id" "$target" "$fstype" "$source" "$uid" "$uid"
    if [ "$MOCK_ATTACK" = stacked ]; then
        printf '8124 1 0:72 / %s rw,nosuid,nodev - %s %s rw,user_id=%s,group_id=%s\n' \
            "$target" "$fstype" "$source" "$uid" "$uid"
    fi
    if { [ "$MOCK_ATTACK" = new-foreign ] && [ "$current" = stopped ]; } ||
        { [ "$MOCK_ATTACK" = late-foreign ] && [ "$reads" -ge 2 ]; }; then
        printf '8124 1 0:72 / %s/external-mount rw - ext4 foreign rw\n' "$MOCK_STORE"
    fi
    if [ "$MOCK_ATTACK" = late-data-foreign ] && [ "$reads" -ge 2 ]; then
        printf '8124 1 0:72 / %s/external-mount rw - ext4 foreign rw\n' "$MOCK_FOREIGN_ROOT"
    fi
}
install() {
    local destination source
    for destination; do :; done
    if [ "$destination" = /etc/systemd/system/scorpiofs.service ]; then
        while [ "$#" -gt 2 ]; do shift; done
        source="$1"
        command cp "$source" "$unit_capture"
    else command /usr/bin/install "$@"
    fi
}
# Unit artifacts stay inside the fixture for both installation and rollback.
# Map only this exact system path; leave all production validation untouched.
test() {
    local argument
    local -a arguments=()
    for argument; do
        [ "$argument" != /etc/systemd/system/scorpiofs.service ] || argument="$unit_capture"
        arguments+=("$argument")
    done
    command test "${arguments[@]}"
}
cp() {
    local argument
    local -a arguments=()
    for argument; do
        [ "$argument" != /etc/systemd/system/scorpiofs.service ] || argument="$unit_capture"
        arguments+=("$argument")
    done
    command /bin/cp "${arguments[@]}"
}
rm() {
    local argument
    local -a arguments=()
    for argument; do
        [ "$argument" != /etc/systemd/system/scorpiofs.service ] || argument="$unit_capture"
        arguments+=("$argument")
    done
    command /bin/rm "${arguments[@]}"
}
chown() {
    printf 'chown %s\n' "$*" >>"$systemctl_log"
    command /usr/bin/chown "$@"
}
systemctl() {
    printf '%s\n' "$*" >>"$systemctl_log"
    case "$1" in
        show)
            case " $* " in
                *' --property=MainPID '*)
                    local reads
                    IFS= read -r reads <"$mock_pid_reads"
                    reads=$((reads + 1))
                    printf '%s\n' "$reads" >"$mock_pid_reads"
                    if { [ "$MOCK_ATTACK" = pid ] && [ "$(phase)" != before ]; } ||
                        { [ "$MOCK_ATTACK" = late-pid ] && [ "$reads" -ge 3 ]; }; then
                        printf '%s\n' "$((mock_pid + 1))"
                    else printf '%s\n' "$mock_pid"; fi
                    ;;
                *) [ ! -r "$unit_capture" ] || awk -F= '$1 == "User" { print $2; exit }' "$unit_capture" ;;
            esac
            ;;
        is-active) [ "$mock_service_active" -eq 1 ] ;;
        stop) mock_service_active=0; printf 'stopped\n' >"$mock_phase" ;;
        start|restart) mock_service_active=1 ;;
        *) return 0 ;;
    esac
}
cat() {
    local argument="" uid current
    for argument; do :; done
    if [ "$argument" = /proc/self/mountinfo ]; then
        if [ "$(phase)" = stopped ]; then
            local reads
            IFS= read -r reads <"$mock_mount_reads"
            printf '%s\n' "$((reads + 1))" >"$mock_mount_reads"
        fi
        command /bin/cat /proc/self/mountinfo
        mount_records
        return
    fi
    [ "$MOCK_RAW_ACTIVE" -eq 1 ] || { command /bin/cat "$@"; return; }
    uid="$(id -u "$service_user")"
    current="$(phase)"
    case "$argument" in
        "/proc/$mock_pid/status")
            local reads
            IFS= read -r reads <"$mock_capture_reads"
            printf '%s\n' "$((reads + 1))" >"$mock_capture_reads"
            [ "$MOCK_ATTACK" != process-uid ] || uid=123456
            printf 'Name:\tscorpio\nUid:\t%s\t%s\t%s\t%s\nVmRSS:\t%s kB\n' \
                "$uid" "$uid" "$uid" "$uid" "$([ "$current" = before ] && printf 100 || printf 200)"
            ;;
        "/proc/$mock_pid/stat")
            local start=99
            if [ "$MOCK_ATTACK" = starttime ] && [ "$current" != before ]; then start=100; fi
            printf '%s (scorpio daemon) S' "$mock_pid"
            for _ in {1..18}; do printf ' 0'; done
            printf ' %s 0 0\n' "$start"
            ;;
        "/proc/$mock_pid/cmdline")
            printf '%s\0' "$MOCK_PREFIX/bin/scorpio" serve --http-addr 127.0.0.1:2925
            if [ "$MOCK_ATTACK" = cmdline ] && [ "$current" != before ]; then
                printf '%s\0' --log-level debug
            fi
            ;;
        /proc/net/tcp)
            local inode=9001 socket_uid="$uid" state=0A
            [ "$MOCK_ATTACK" != socket ] || inode=9999
            [ "$MOCK_ATTACK" != socket-uid ] || socket_uid=123456
            [ "$MOCK_ATTACK" != socket-state ] || state=01
            if [ "$MOCK_ATTACK" = socket-change ] && [ "$current" != before ]; then inode=9002; fi
            local reads
            IFS= read -r reads <"$mock_capture_reads"
            if [ "$MOCK_ATTACK" = late-socket ] && [ "$reads" -ge 3 ]; then inode=9002; fi
            printf '  sl local_address rem_address st tx_queue:rx_queue tr:tm->when retrnsmt uid timeout inode\n'
            printf '  0: 0100007F:0B6D 00000000:0000 %s 00000000:00000000 00:00000000 00000000 %s 0 %s\n' \
                "$state" "$socket_uid" "$inode"
            ;;
        /proc/net/tcp6)
            printf '  sl local_address rem_address st tx_queue:rx_queue tr:tm->when retrnsmt uid timeout inode\n' ;;
        *) command /bin/cat "$@" ;;
    esac
}
stat() {
    local argument
    for argument; do :; done
    if [ "$MOCK_RAW_ACTIVE" -eq 1 ] && [ "$argument" = "/proc/$mock_pid/exe" ]; then
        if [ "$MOCK_ATTACK" = exe ]; then printf '123:456\n'
        else command /usr/bin/stat -Lc '%d:%i' "$MOCK_PREFIX/bin/scorpio"; fi
        return
    fi
    command /usr/bin/stat "$@"
}
find() {
    if [ "$MOCK_RAW_ACTIVE" -eq 1 ] && [ "$1" = "/proc/$mock_pid/fd" ]; then
        local inode=9001
        if [ "$MOCK_ATTACK" = socket-change ] && [ "$(phase)" != before ]; then inode=9002; fi
        local reads
        IFS= read -r reads <"$mock_capture_reads"
        if [ "$MOCK_ATTACK" = late-socket ] && [ "$reads" -ge 3 ]; then inode=9002; fi
        printf 'socket:[%s]\n/dev/fuse\n' "$inode"
    else command /usr/bin/find "$@"; fi
}
findmnt() {
    local line target fstype
    while IFS= read -r line; do
        target="$(awk '{print $5}' <<<"$line")"
        fstype="$(awk -F ' - ' '{print $2}' <<<"$line" | awk '{print $1}')"
        case " $* " in
            *' TARGET,FSTYPE '*) printf '%s %s\n' "$target" "$fstype" ;;
            *) printf '%s\n' "$target" ;;
        esac
    done < <(mount_records)
}
fusermount3() {
    printf 'fusermount3 %s\n' "$*" >>"$systemctl_log"
    local argument
    for argument; do :; done
    [ "$(phase)" = stopped ]
    [ "$argument" = "$(mount_target)" ]
    mock_stale_detached=1
}
umount() { printf 'umount %s\n' "$*" >>"$systemctl_log"; return 64; }
curl() {
    local argument output="" previous="" saw_noproxy=0
    for argument in "$@"; do
        [ "$previous" != -o ] || output="$argument"
        [ "$argument" != --noproxy ] || saw_noproxy=1
        previous="$argument"
        if [[ "$argument" == */v3/workspaces ]]; then
            [ "$saw_noproxy" -eq 1 ] && [ "$argument" = http://127.0.0.1:2925/v3/workspaces ] || return 64
            [ -n "$output" ] || return 64
            local id="$mock_workspace_id" state=mounted
            [ "$MOCK_ATTACK" != api-uuid ] || id=invalid-uuid
            [ "$MOCK_ATTACK" != api-state ] || state=failed
            if [ "$MOCK_ATTACK" = api-absent ]; then printf '[]\n' >"$output"
            else
                printf '[{"workspace_id":"%s","generation":"%s","mountpoint":"%s","mount_state":"%s","metadata_ready":true}]\n' \
                    "$id" "$mock_generation" "$(mount_target)" "$state" >"$output"
            fi
            printf 'after\n' >"$mock_phase"
            printf '200'
            return
        fi
        if [[ "$argument" == */health ]]; then
            [ "$saw_noproxy" -eq 1 ] || return 64
            if [ -n "$MOCK_RETIRED_ALIAS_PATH" ]; then
                [ ! -e "$MOCK_RETIRED_ALIAS_PATH" ] || return 64
                printf 'retired alias absent at health check\n' >>"$systemctl_log"
            fi
            [ "$MOCK_HEALTH_FAIL" -eq 0 ] || return 7
            printf 'ok\n'
            return
        fi
    done
    command curl "$@"
}
sleep() {
    # Synchronous failures still execute every health attempt, without waiting.
    [ "$MOCK_HEALTH_FAIL" -eq 1 ] || command sleep "$@"
}
usermod() { return 0; }
export -f phase mount_target mount_records install test cp rm chown systemctl cat stat find findmnt fusermount3 umount curl sleep usermod
export unit_capture systemctl_log service_user mock_phase mock_mount_reads mock_pid_reads mock_capture_reads mock_service_active mock_stale_detached mock_pid mock_workspace_id mock_generation
export MOCK_RAW_ACTIVE MOCK_ATTACK MOCK_STORE MOCK_PREFIX MOCK_FOREIGN_ROOT MOCK_HEALTH_FAIL MOCK_RETIRED_ALIAS_PATH MOCK_TARGET_USER
invoke() {
    local root="$1"
    shift
    SCORPIO_SERVICE_USER="$MOCK_TARGET_USER" bash "$repo_root/install.sh" \
        --version "$version" --release-base-url "$release_base_url" \
        --non-interactive --no-deps --no-user-allow-other \
        --mst2-base-url https://mst2.example.com \
        --prefix "$root/prefix" --config-dir "$root/etc" --data-root "$root/data" \
        --store-path "$root/data/store" --http-addr 127.0.0.1:2925 "$@"
}
reset_evidence() {
    : >"$systemctl_log"; printf 'before\n' >"$mock_phase"; printf '0\n' >"$mock_mount_reads"
    printf '0\n' >"$mock_pid_reads"; printf '0\n' >"$mock_capture_reads"; mock_stale_detached=0
}
assert_log() { grep -Fxq "$1" "$systemctl_log"; }
assert_zero_mutations() {
    if grep -Eq '^(chown|fusermount3|umount) ' "$systemctl_log"; then
        printf 'untrusted evidence caused detach or ownership change:\n' >&2
        command cat "$systemctl_log" >&2
        exit 1
    fi
}
# Exact release binary and the same 3-NUL layout consumed by the installer.
invoke "$test_root" --overwrite-config >"$test_root/fresh.log" 2>&1 || {
    status=$?
    command cat "$test_root/fresh.log" >&2
    exit "$status"
}
mock_service_active=1
test -x "$test_root/prefix/bin/scorpio"
test ! -e "$test_root/prefix/bin/antares"
grep -Fxq "WorkingDirectory=$test_root/data" "$unit_capture"
grep -Fxq "User=$service_user" "$unit_capture"
if grep -q '^ExecStopPost=' "$unit_capture"; then exit 1; fi
if grep -q '^ExecStartPre=' "$unit_capture"; then exit 1; fi
"$test_root/prefix/bin/scorpio" --config-path "$test_root/etc/scorpio.toml" config installer-paths >"$test_root/paths.bin"
python3 - "$test_root/paths.bin" "$test_root/data/store" <<'PY'
import pathlib, sys
data = pathlib.Path(sys.argv[1]).read_bytes().split(b"\0")
store = sys.argv[2].encode()
assert data == [store, store + b"/workspaces-v3", store + b"/mst2-cache", b""]
PY
printf 'V3_INSTALLER_PATHS_RUN\n'
mkdir -p "$test_root/data/store/workspaces-v3/$mock_workspace_id/mount" \
    "$test_root/data/store/workspaces-v3/$mock_workspace_id/upper" "$test_root/data/store/mst2-cache"
printf 'keep dirty upper bytes\n' >"$test_root/data/store/workspaces-v3/$mock_workspace_id/upper/dirty.txt"
printf 'keep verified CAS bytes\n' >"$test_root/data/store/mst2-cache/sentinel"
command chown -R "$service_user:$(id -gn "$service_user")" "$test_root/data"
upper_digest="$(sha256sum "$test_root/data/store/workspaces-v3/$mock_workspace_id/upper/dirty.txt" | awk '{print $1}')"
cas_digest="$(sha256sum "$test_root/data/store/mst2-cache/sentinel" | awk '{print $1}')"
# Execute production raw proc/TCP/mountinfo/HTTP validators, never stub success.
for attack in non-fuse source mount-uid foreign-path foreign-data stacked process-uid exe \
    socket socket-uid socket-state api-absent api-uuid api-state pid starttime \
    cmdline socket-change late-pid late-socket changed-mount new-foreign late-target late-foreign; do
    reset_evidence
    if MOCK_RAW_ACTIVE=1 MOCK_STORE="$test_root/data/store" MOCK_PREFIX="$test_root/prefix" \
        MOCK_ATTACK="$attack" invoke "$test_root" >"$test_root/reject-$attack.log" 2>&1; then
        printf 'installer accepted untrusted %s evidence\n' "$attack" >&2; exit 1
    fi
    assert_zero_mutations
    if [ "$attack" = late-pid ] || [ "$attack" = late-socket ]; then
        test "$(command cat "$mock_pid_reads")" -eq 3
        if grep -Fxq 'stop scorpiofs.service' "$systemctl_log"; then exit 1; fi
        if [ "$attack" = late-socket ]; then test "$(command cat "$mock_capture_reads")" -eq 3; fi
    fi
done
# Recheck both data roots outside either store immediately before detaching
# an old-generation mount during migration to a separate new data root.
for scope in previous current; do
    migration_root="$test_root/migration-$scope"
    foreign_root="$test_root/data"
    [ "$scope" != current ] || foreign_root="$migration_root/data"
    reset_evidence
    if MOCK_RAW_ACTIVE=1 MOCK_STORE="$test_root/data/store" MOCK_PREFIX="$test_root/prefix" \
        MOCK_ATTACK=late-data-foreign MOCK_FOREIGN_ROOT="$foreign_root" \
        invoke "$test_root" --overwrite-config --data-root "$migration_root/data" \
        --store-path "$migration_root/data/store" >"$test_root/reject-late-$scope-data.log" 2>&1; then exit 1; fi
    assert_zero_mutations
done
printf 'V3_INSTALLER_FOREIGN_AUTHORITY_REJECT_RUN\n'

mkdir -p "$test_root/foreign-target/mount"
symlink_id=22222222-3333-4444-8555-666666666666
ln -s "$test_root/foreign-target" "$test_root/data/store/workspaces-v3/$symlink_id"
reset_evidence
if MOCK_RAW_ACTIVE=1 MOCK_STORE="$test_root/data/store" MOCK_PREFIX="$test_root/prefix" \
    mock_workspace_id="$symlink_id" invoke "$test_root" >"$test_root/reject-symlink.log" 2>&1; then exit 1; fi
assert_zero_mutations
rm "$test_root/data/store/workspaces-v3/$symlink_id"
reset_evidence
mock_service_active=0
if MOCK_RAW_ACTIVE=1 MOCK_STORE="$test_root/data/store" MOCK_PREFIX="$test_root/prefix" \
    invoke "$test_root" >"$test_root/reject-inactive.log" 2>&1; then exit 1; fi
assert_zero_mutations
mock_service_active=1
reset_evidence
if MOCK_RAW_ACTIVE=1 MOCK_STORE="$test_root/data/store" MOCK_PREFIX="$test_root/prefix" \
    invoke "$test_root" --no-service >"$test_root/reject-no-service.log" 2>&1; then exit 1; fi
assert_zero_mutations
# A root-owned executable is valid for an independently identified service UID.
test "$(command stat -c '%U' "$test_root/prefix/bin/scorpio")" = root
reset_evidence
printf '#!/usr/bin/env bash\nexit 64\n' >"$test_root/prefix/bin/antares"
chmod 0755 "$test_root/prefix/bin/antares"
MOCK_RAW_ACTIVE=1 MOCK_STORE="$test_root/data/store" MOCK_PREFIX="$test_root/prefix" \
    invoke "$test_root" >"$test_root/managed-upgrade.log" 2>&1
assert_log 'stop scorpiofs.service'
assert_log "fusermount3 -u -z $test_root/data/store/workspaces-v3/$mock_workspace_id/mount"
assert_log 'start scorpiofs.service'
python3 - "$systemctl_log" <<'PY'
import pathlib, sys
lines = pathlib.Path(sys.argv[1]).read_text().splitlines()
stop = lines.index("stop scorpiofs.service")
detach = next(i for i, line in enumerate(lines) if line.startswith("fusermount3 "))
ownership = next(i for i, line in enumerate(lines) if line.startswith("chown "))
assert stop < detach < ownership
PY
test ! -e "$test_root/prefix/bin/antares"
test "$(sha256sum "$test_root/data/store/workspaces-v3/$mock_workspace_id/upper/dirty.txt" | awk '{print $1}')" = "$upper_digest"
test "$(sha256sum "$test_root/data/store/mst2-cache/sentinel" | awk '{print $1}')" = "$cas_digest"
printf 'V3_INSTALLER_OWNED_MOUNT_UPGRADE_RUN\n'
# Dry-run exercises authority but leaves artifacts, mounts and ownership alone.
reset_evidence
config_digest="$(sha256sum "$test_root/etc/scorpio.toml" | awk '{print $1}')"
MOCK_RAW_ACTIVE=1 MOCK_STORE="$test_root/data/store" MOCK_PREFIX="$test_root/prefix" \
    invoke "$test_root" --dry-run >"$test_root/managed-dry-run.log" 2>&1
assert_zero_mutations
grep -Fq "[dry-run] fusermount3 -u -z $test_root/data/store/workspaces-v3/$mock_workspace_id/mount" "$test_root/managed-dry-run.log"
test "$(sha256sum "$test_root/etc/scorpio.toml" | awk '{print $1}')" = "$config_digest"
# Active failure restores original binary/config/unit/alias bytes and old owner.
reset_evidence
printf 'old-binary-marker' >>"$test_root/prefix/bin/scorpio"
printf '#!/usr/bin/env bash\nexit 65\n# previous alias bytes\n' >"$test_root/prefix/bin/antares"
chmod 0755 "$test_root/prefix/bin/antares"
alias_digest="$(sha256sum "$test_root/prefix/bin/antares" | awk '{print $1}')"
cp "$test_root/etc/scorpio.toml" "$test_root/config-before-failure.toml"
cp "$unit_capture" "$test_root/unit-before-failure"
rollback_user=root
[ "$service_user" != root ] || rollback_user=nobody
if MOCK_HEALTH_FAIL=1 MOCK_RETIRED_ALIAS_PATH="$test_root/prefix/bin/antares" \
    MOCK_TARGET_USER="$rollback_user" invoke "$test_root" --overwrite-config >"$test_root/health-failure.log" 2>&1; then exit 1; fi
grep -Fq 'did not become healthy' "$test_root/health-failure.log"
grep -Fq 'restoring ScorpioFS artifacts from before the failed upgrade' "$test_root/health-failure.log"
cmp "$test_root/config-before-failure.toml" "$test_root/etc/scorpio.toml"
cmp "$test_root/unit-before-failure" "$unit_capture"
tail -c 17 "$test_root/prefix/bin/scorpio" | grep -Fxq old-binary-marker
assert_log 'retired alias absent at health check'
test -x "$test_root/prefix/bin/antares"
test "$(sha256sum "$test_root/prefix/bin/antares" | awk '{print $1}')" = "$alias_digest"
for directory in data data/store data/store/workspaces-v3 data/store/mst2-cache; do
    test "$(command stat -c '%U' "$test_root/$directory")" = "$service_user"
done
test "$(grep -Fc 'start scorpiofs.service' "$systemctl_log")" -ge 2
printf 'V3_INSTALLER_ACTIVE_ROLLBACK_RUN\n'
# Inactive rollback stops only this replacement, restores the alias exactly,
# and never restarts the previously inactive service.
reset_evidence
mock_service_active=0
printf '#!/usr/bin/env bash\nexit 66\n# inactive alias bytes\n' >"$test_root/prefix/bin/antares"
chmod 0755 "$test_root/prefix/bin/antares"
inactive_digest="$(sha256sum "$test_root/prefix/bin/antares" | awk '{print $1}')"
if MOCK_HEALTH_FAIL=1 MOCK_RETIRED_ALIAS_PATH="$test_root/prefix/bin/antares" \
    invoke "$test_root" >"$test_root/inactive-health-failure.log" 2>&1; then exit 1; fi
grep -Fq 'did not become healthy' "$test_root/inactive-health-failure.log"
assert_log 'retired alias absent at health check'
test -x "$test_root/prefix/bin/antares"
test "$(sha256sum "$test_root/prefix/bin/antares" | awk '{print $1}')" = "$inactive_digest"
test "$(grep -Fc 'start scorpiofs.service' "$systemctl_log")" -eq 1
test "$(grep -Fc 'stop scorpiofs.service' "$systemctl_log")" -eq 1
if grep -Fxq 'restart scorpiofs.service' "$systemctl_log"; then exit 1; fi
mock_service_active=1
printf 'V3_INSTALLER_INACTIVE_ALIAS_ROLLBACK_RUN\n'
# Retained endpoint bytes, ignored obsolete fields and hardened store mode.
printf '\nbase_url = "https://retired.example"\nconfig_file = "invalid-legacy-state.toml"\n' >>"$test_root/etc/scorpio.toml"
printf 'not TOML' >"$test_root/data/invalid-legacy-state.toml"
cp "$test_root/etc/scorpio.toml" "$test_root/retained.toml"
chmod 0700 "$test_root/data/store"
reset_evidence
invoke "$test_root" --no-service --mst2-base-url https://ignored.example >"$test_root/retained.log" 2>&1
cmp "$test_root/retained.toml" "$test_root/etc/scorpio.toml"
test "$(command stat -c '%a' "$test_root/data/store")" = 700
test "$(command stat -c '%U' "$test_root/data/store")" = "$service_user"
test "$(command cat "$test_root/data/invalid-legacy-state.toml")" = 'not TOML'
test ! -e "$test_root/data/antares"
nested_root="$test_root/nested-runtime"
invoke "$nested_root" --overwrite-config --no-service \
    --store-path "$nested_root/data/private/store" >"$test_root/nested-store.log" 2>&1
test "$(command stat -c '%U' "$nested_root/data/private")" = "$service_user"
# Relative stores cannot authorize migrating either an unrelated nonempty root
# or a fresh empty root while retaining the previous relative config.
for kind in nonempty empty; do
    relative_root="$test_root/relative-$kind"
    mkdir -p "$relative_root/etc" "$relative_root/data"
    cp "$test_root/etc/scorpio.toml" "$relative_root/etc/scorpio.toml"
    sed -i 's|^store_path = .*|store_path = "store"|' "$relative_root/etc/scorpio.toml"
    if [ "$kind" = nonempty ]; then printf 'keep\n' >"$relative_root/data/sentinel"; fi
    reset_evidence
    if invoke "$relative_root" --no-service >"$test_root/relative-$kind.log" 2>&1; then exit 1; fi
    article=a
    [ "$kind" != empty ] || article=an
    grep -Fq "cannot safely use $article $kind data-root with an all-relative retained config" "$test_root/relative-$kind.log"
    assert_zero_mutations
done

# A root-owned 0400 retained config is migrated to the new service user without
# changing its endpoint bytes. The release binary must really parse it as that UID.
sed -i 's/^User=.*/User=root/' "$unit_capture"
command chown root:root "$test_root/etc/scorpio.toml"
chmod 0400 "$test_root/etc/scorpio.toml"
cp "$test_root/etc/scorpio.toml" "$test_root/protected-before.toml"
reset_evidence
MOCK_TARGET_USER=nobody invoke "$test_root" >"$test_root/protected-migration.log" 2>&1
cmp "$test_root/protected-before.toml" "$test_root/etc/scorpio.toml"
test "$(command stat -c '%U' "$test_root/etc/scorpio.toml")" = nobody
runuser -u nobody -- "$test_root/prefix/bin/scorpio" \
    --config-path "$test_root/etc/scorpio.toml" config validate
printf 'V3_INSTALLER_RETAINED_USER_MIGRATION_RUN\n'
# Refuse unreadable config traversal before stopping the active old owner.
reset_evidence
command chown root:root "$test_root/etc"
chmod 0700 "$test_root/etc"
if MOCK_TARGET_USER=nobody invoke "$test_root" >"$test_root/config-traversal.log" 2>&1; then exit 1; fi
grep -Eq 'cannot traverse config-dir|cannot read retained config' "$test_root/config-traversal.log"
assert_zero_mutations
if grep -Fxq 'stop scorpiofs.service' "$systemctl_log"; then exit 1; fi
chmod 0755 "$test_root/etc"
printf 'installer v3 authority, ownership and rollback test passed\n'
