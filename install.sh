#!/usr/bin/env bash
#
# ScorpioFS interactive installer.
#
# With no arguments this script asks for the MST/2 URL, local paths,
# HTTP bind address, and whether to install a systemd service. It also keeps a
# non-interactive mode for automation and the original release-install flags.
#
# Interactive use (including curl | bash) on Linux only:
#   curl -fsSL https://raw.githubusercontent.com/gitmono-dev/scorpiofs/main/install.sh | bash
# macOS: do not run this script. Download the aarch64-apple-darwin tarball
# from GitHub Releases and follow docs/macos.md.
#
# Safer use:
#   curl -fsSLO https://raw.githubusercontent.com/gitmono-dev/scorpiofs/main/install.sh
#   less install.sh
#   bash install.sh
#
set -euo pipefail

REPO="gitmono-dev/scorpiofs"
VERSION="${SCORPIO_VERSION:-}"
RELEASE_BASE_URL="${SCORPIO_RELEASE_BASE_URL:-https://github.com/${REPO}/releases/download}"
PREFIX="${SCORPIO_PREFIX:-/usr/local}"
CONFDIR="${SCORPIO_CONFDIR:-/etc/scorpiofs}"
DATA_ROOT="${SCORPIO_DATA_ROOT:-/var/lib/scorpiofs}"
MST2_BASE_URL="${SCORPIO_MST2_BASE_URL:-}"
STORE_PATH="${SCORPIO_STORE_PATH:-}"
WORKSPACE_ROOT=""
CACHE_ROOT=""
PREVIOUS_WORKSPACE_ROOT=""
PREVIOUS_CACHE_ROOT=""
OWNED_MOUNTS_FILE=""
OWNED_SERVICE_PID=""
HTTP_ADDR="${SCORPIO_HTTP_ADDR:-127.0.0.1:2725}"
SERVICE_USER="${SCORPIO_SERVICE_USER:-scorpiofs}"

DRY_RUN=0
DO_UNINSTALL=0
INSTALL_DEPS=1
ENABLE_USER_ALLOW_OTHER=0
SETUP_SERVICE=1
INTERACTIVE=1
ASSUME_YES=0
ALLOW_PUBLIC_API=0
OVERWRITE_CONFIG=0
SERVICE_CHOICE_SET=0
FUSE_CHOICE_SET=0
CONFIG_CHOICE_SET=0
DATA_ROOT_SET=0
STORE_PATH_SET=0
EXISTING_CONFIG=0
RETAIN_CONFIG=0
EXISTING_SERVICE_USER=""
EXISTING_SERVICE_ACTIVE=0
SERVICE_STOPPED_FOR_UPGRADE=0
SERVICE_HEALTH_CONFIRMED=0
ARTIFACT_BACKUP_DIR=""
ARTIFACT_BACKUP_READY=0
HAD_OLD_SCORPIO=0
HAD_OLD_CONFIG=0
HAD_OLD_UNIT=0
PREVIOUS_DATA_ROOT=""
PREVIOUS_STORE_PATH=""
EXTRACTED_RELEASE=""
REQUESTED_STORE_PATH=""
WORKDIR=""
SUDO_BIN=""
TARGET_USER=""
TARGET_GROUP=""
TTY_FD=""
BIND_HOST=""
BIND_PORT=""

[ -z "${SCORPIO_DATA_ROOT:-}" ] || DATA_ROOT_SET=1
[ -z "${SCORPIO_STORE_PATH:-}" ] || STORE_PATH_SET=1

cleanup() {
    local exit_status=$?
    if [ "$exit_status" -ne 0 ] && [ "$SERVICE_STOPPED_FOR_UPGRADE" -eq 1 ] && \
        [ "$SERVICE_HEALTH_CONFIRMED" -ne 1 ] && command -v systemctl >/dev/null 2>&1; then
        if run_root systemctl is-active --quiet scorpiofs.service; then
            warn "stopping the failed replacement service before rollback"
            if ! run_root systemctl stop scorpiofs.service; then
                warn "could not stop the failed replacement service before rollback"
            fi
        fi
        if [ "$ARTIFACT_BACKUP_READY" -eq 1 ]; then
            if ! restore_upgrade_artifacts; then
                warn "could not restore all previous ScorpioFS artifacts"
            fi
        fi
        if ! restore_runtime_ownership; then
            warn "could not restore runtime ownership for the previous service user"
        fi
        warn "installation failed after stopping scorpiofs.service; attempting to restore the managed service"
        if ! run_root systemctl start scorpiofs.service; then
            warn "could not restore scorpiofs.service; inspect: systemctl status scorpiofs"
        fi
    fi
    if [ -n "${WORKDIR:-}" ] && [ -d "$WORKDIR" ]; then
        rm -rf "$WORKDIR"
    fi
    return "$exit_status"
}
trap cleanup EXIT

usage() {
    cat <<'EOF'
Usage: install.sh [options]

Interactive mode is the default. It asks for the MST/2 URL, local paths,
HTTP bind address, FUSE permission, and whether to install a systemd service.

Options:
  --version <vX.Y.Z>        Release tag (default: latest GitHub release).
  --release-base-url <url>  Release mirror root (default: GitHub releases).
  --prefix <dir>            Binary prefix (default: /usr/local).
  --config-dir <dir>        Config directory (default: /etc/scorpiofs).
  --data-root <dir>         Runtime/data root (default: /var/lib/scorpiofs).
  --mst2-base-url <url>     MST/2 service URL.
  --store-path <dir>        Local cache/store inside data-root.
  --http-addr <socket>      IPv4:port or [IPv6]:port (default: 127.0.0.1:2725).
  --allow-public-api        Permit a non-loopback HTTP bind (use a firewall/auth proxy).
  --no-service              Do not create or start a systemd service.
  --non-interactive         Use arguments/environment without prompting.
  --overwrite-config        Replace an existing scorpio.toml during an upgrade.
  --yes                     Accept safe defaults in interactive mode.
  --dry-run                 Print actions without changing the system.
  --uninstall               Stop/remove binaries and the systemd unit; keep data.
  --no-deps                 Skip system package installation.
  --enable-user-allow-other Enable user_allow_other in /etc/fuse.conf.
  --no-user-allow-other     Do not change /etc/fuse.conf (it must already enable user_allow_other).
  -h, --help                Show this help message.

Examples:
  sudo bash install.sh
  bash install.sh --mst2-base-url https://mega.example.com
  bash install.sh --version v0.4.0 --non-interactive --overwrite-config --dry-run

The HTTP API has no authentication. The installer therefore defaults to
127.0.0.1 and asks for explicit confirmation before accepting a public bind.
EOF
}

note() { printf '==> %s\n' "$*"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

run_root() {
    if [ "$DRY_RUN" -eq 1 ]; then
        printf '  [dry-run]'
        if [ -n "$SUDO_BIN" ]; then printf ' %s' "$SUDO_BIN"; fi
        printf ' %s\n' "$*"
        return 0
    fi
    if [ -n "$SUDO_BIN" ]; then
        "$SUDO_BIN" "$@"
    else
        "$@"
    fi
}

run_readonly() {
    if [ -n "$SUDO_BIN" ]; then
        "$SUDO_BIN" "$@"
    else
        "$@"
    fi
}

run_as_existing_service_user() {
    [ -n "$EXISTING_SERVICE_USER" ] || { "$@"; return; }
    [ "$EXISTING_SERVICE_USER" = "$(id -un)" ] && { "$@"; return; }
    if [ "$(id -u)" -eq 0 ]; then
        runuser -u "$EXISTING_SERVICE_USER" -- "$@"
        return
    fi
    command -v sudo >/dev/null 2>&1 || \
        die "sudo is required to resolve retained runtime paths as $EXISTING_SERVICE_USER"
    sudo -n -v || \
        die "could not obtain non-interactive sudo privileges to resolve retained runtime paths as $EXISTING_SERVICE_USER"
    sudo -n -u "$EXISTING_SERVICE_USER" -- "$@"
}

require_privileges() {
    [ "$DRY_RUN" -eq 1 ] && return 0
    if [ "$(id -u)" -eq 0 ]; then
        SUDO_BIN=""
        return 0
    fi
    command -v sudo >/dev/null 2>&1 || die "root privileges are required; install sudo or run as root"
    if [ "$INTERACTIVE" -eq 1 ]; then
        sudo -v || die "could not obtain sudo privileges"
    else
        sudo -n -v || die "could not obtain non-interactive sudo privileges; run as root or pre-authorize sudo"
    fi
    SUDO_BIN="sudo"
}

read_tty() {
    [ -n "$TTY_FD" ] || die "interactive input is unavailable; use --non-interactive"
    IFS= read -r REPLY <&"$TTY_FD" || die "could not read interactive input from /dev/tty"
}

prompt_value() {
    local variable="$1" label="$2" default="$3"
    if [ "$ASSUME_YES" -eq 1 ]; then
        printf -v "$variable" '%s' "$default"
        return 0
    fi
    printf '%s [%s]: ' "$label" "$default" >&"$TTY_FD"
    read_tty
    if [ -z "$REPLY" ]; then REPLY="$default"; fi
    printf -v "$variable" '%s' "$REPLY"
}

prompt_yes_no() {
    local variable="$1" label="$2" default="$3" answer=""
    if [ "$ASSUME_YES" -eq 1 ]; then
        case "$default" in
            y|Y|yes|YES|Yes) printf -v "$variable" '%s' 1 ;;
            *) printf -v "$variable" '%s' 0 ;;
        esac
        return 0
    fi
    while :; do
        printf '%s [%s]: ' "$label" "$default" >&"$TTY_FD"
        read_tty
        answer="${REPLY:-$default}"
        case "$answer" in
            y|Y|yes|YES|Yes) printf -v "$variable" '%s' 1; return 0 ;;
            n|N|no|NO|No)    printf -v "$variable" '%s' 0; return 0 ;;
            *) printf 'Please answer yes or no.\n' >&2 ;;
        esac
    done
}

validate_url() {
    local field="$1" value="$2"
    local LC_ALL=C
    [[ ! "$value" =~ [[:cntrl:]] ]] || die "$field must not contain control characters"
    case "$value" in
        http://*|https://*) ;;
        *) die "$field must start with http:// or https:// (got: $value)" ;;
    esac
    [[ "$value" != *[[:space:]]* ]] || die "$field must not contain whitespace"
    [[ "$value" != *'"'* && "$value" != *"'"* ]] || die "$field must not contain quotes"
    [[ "$value" != *'`'* && "$value" != *\\* ]] || die "$field contains an unsafe character"
    [[ "$value" != *'?'* && "$value" != *'#'* ]] || die "$field must not contain a query or fragment"

    local remainder authority host port=""
    remainder="${value#*://}"
    authority="${remainder%%/*}"
    [ -n "$authority" ] || die "$field must include a host"
    [[ "$authority" != *'@'* ]] || die "$field must not contain credentials"

    if [[ "$authority" =~ ^\[([^]]+)\](:([0-9]+))?$ ]]; then
        host="${BASH_REMATCH[1]}"
        port="${BASH_REMATCH[3]:-}"
        validate_ipv6 "$field host" "$host"
    else
        host="$authority"
        if [[ "$authority" == *:* ]]; then
            host="${authority%:*}"
            port="${authority##*:}"
            [[ "$host" != *:* ]] || die "$field has an IPv6 host without brackets"
        fi
        validate_url_host "$field host" "$host"
    fi
    [ -z "$port" ] || validate_port "$field port" "$port"
}

validate_port() {
    local field="$1" value="$2"
    [[ "$value" =~ ^[0-9]{1,5}$ ]] || die "$field must be a numeric port"
    (( 10#$value >= 1 && 10#$value <= 65535 )) || die "$field must be between 1 and 65535"
}

validate_ipv4() {
    local field="$1" value="$2" a b c d extra octet
    IFS=. read -r a b c d extra <<<"$value"
    if [ -n "${extra:-}" ] || [ -z "${a:-}" ] || [ -z "${b:-}" ] || \
        [ -z "${c:-}" ] || [ -z "${d:-}" ]; then
        die "$field is not a valid IPv4 address"
    fi
    for octet in "$a" "$b" "$c" "$d"; do
        if ! [[ "$octet" =~ ^[0-9]{1,3}$ ]] || (( 10#$octet > 255 )); then
            die "$field is not a valid IPv4 address"
        fi
    done
}

validate_ipv6() {
    local field="$1" value="$2"
    [[ "$value" == *:* && "$value" =~ ^[0-9A-Fa-f:.]+$ ]] || \
        die "$field is not a valid IPv6 address"
    command -v getent >/dev/null 2>&1 || die "getent is required to validate IPv6 addresses"
    getent ahostsv6 "$value" >/dev/null 2>&1 || die "$field is not a valid IPv6 address"
}

validate_url_host() {
    local field="$1" value="$2" label
    local -a labels
    [ -n "$value" ] || die "$field must not be empty"
    if [[ "$value" =~ ^[0-9.]+$ ]]; then
        validate_ipv4 "$field" "$value"
        return 0
    fi
    [[ "$value" != *'..'* ]] || die "$field is not a valid hostname"
    IFS=. read -ra labels <<<"$value"
    for label in "${labels[@]}"; do
        [[ "$label" =~ ^[A-Za-z0-9]([A-Za-z0-9_-]{0,61}[A-Za-z0-9])?$ ]] || \
            die "$field is not a valid hostname"
    done
}

normalize_path() {
    local value="$1"
    while [ "$value" != "/" ] && [ "${value%/}" != "$value" ]; do value="${value%/}"; done
    printf '%s' "$value"
}

validate_path() {
    local field="$1" value="$2"
    [ -n "$value" ] || die "$field must not be empty"
    [[ "$value" == /* ]] || die "$field must be an absolute path: $value"
    [[ "$value" != *[[:space:]]* ]] || die "$field must not contain whitespace"
    [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] || die "$field must not contain a newline"
    [[ "$value" =~ ^/[A-Za-z0-9._+:/-]*$ ]] || \
        die "$field contains a character unsafe for shell or systemd use"
    [[ "$value" != *'//'* ]] || die "$field must not contain repeated slashes"
    case "/${value#/}/" in
        *'/../'*|*'/./'*) die "$field must not contain . or .. path components" ;;
    esac
    [ ! -L "$value" ] || die "$field must not be a symbolic link: $value"
}

canonicalize_paths() {
    PREFIX="$(realpath -m -- "$PREFIX")"
    CONFDIR="$(realpath -m -- "$CONFDIR")"
    DATA_ROOT="$(realpath -m -- "$DATA_ROOT")"
    STORE_PATH="$(realpath -m -- "$STORE_PATH")"
}

validate_data_root() {
    case "$DATA_ROOT" in
        /|/bin|/boot|/dev|/etc|/home|/lib|/lib64|/media|/mnt|/opt|/proc|/root|/run|/sbin|/srv|/sys|/tmp|/usr|/usr/local|/var|/var/lib|/var/log)
            die "data-root is too broad and must be a dedicated ScorpioFS directory: $DATA_ROOT"
            ;;
    esac
}

data_root_is_nonempty() {
    [ -d "$DATA_ROOT" ] || return 1
    local data_root_entry
    if ! data_root_entry="$(run_readonly find "$DATA_ROOT" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)"; then
        if [ "$(id -u)" -ne 0 ] && [ -z "$SUDO_BIN" ]; then
            command -v sudo >/dev/null 2>&1 || \
                die "could not inspect data-root; refusing to change ownership without verifying it is empty: $DATA_ROOT"
            sudo -n -v || \
                die "could not inspect data-root; refusing to change ownership without verifying it is empty: $DATA_ROOT"
            SUDO_BIN="sudo"
            data_root_entry="$(run_readonly find "$DATA_ROOT" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" || \
                die "could not inspect data-root; refusing to change ownership without verifying it is empty: $DATA_ROOT"
        else
            die "could not inspect data-root; refusing to change ownership without verifying it is empty: $DATA_ROOT"
        fi
    fi
    [ -n "$data_root_entry" ]
}

require_path_in_data_root() {
    local field="$1" value="$2"
    case "$value" in
        "$DATA_ROOT"/*) ;;
        *) die "$field must be inside data-root ($DATA_ROOT): $value" ;;
    esac
}

validate_data_paths() {
    validate_data_root
    require_path_in_data_root store-path "$STORE_PATH"
    [ ! -L "$CONFDIR/scorpio.toml" ] || die "config file must not be a symbolic link: $CONFDIR/scorpio.toml"
    if [ "$EXISTING_CONFIG" -eq 0 ] && data_root_is_nonempty; then
        die "refusing to change ownership of a nonempty data-root without an existing ScorpioFS config: $DATA_ROOT"
    fi
}

canonicalize_runtime_paths() {
    STORE_PATH="$(realpath -m -- "$STORE_PATH")"
    WORKSPACE_ROOT="$STORE_PATH/workspaces-v3"
    CACHE_ROOT="$STORE_PATH/mst2-cache"
}

validate_runtime_paths() {
    local field value i
    local -a fields=(store-path workspace-root cache-root)
    local -a values=("$STORE_PATH" "$WORKSPACE_ROOT" "$CACHE_ROOT")
    for ((i = 0; i < ${#fields[@]}; i++)); do
        validate_path "${fields[$i]}" "${values[$i]}"
    done
    canonicalize_runtime_paths
    validate_data_root
    values=("$STORE_PATH" "$WORKSPACE_ROOT" "$CACHE_ROOT")
    for ((i = 0; i < ${#fields[@]}; i++)); do
        field="${fields[$i]}"
        value="${values[$i]}"
        validate_path "$field" "$value"
        require_path_in_data_root "$field" "$value"
    done
    validate_runtime_path_separation
}

paths_overlap() {
    local left="$1" right="$2"
    case "$left" in "$right"|"$right"/*) return 0 ;; esac
    case "$right" in "$left"|"$left"/*) return 0 ;; esac
    return 1
}

path_is_at_or_below() {
    local path="$1" parent="$2"
    case "$path" in "$parent"|"$parent"/*) return 0 ;; esac
    return 1
}

validate_runtime_path_separation() {
    local installer_path
    for installer_path in "${PREFIX}/bin/scorpio" "${CONFDIR}/scorpio.toml"; do
        installer_path="$(realpath -m -- "$installer_path")"
        if paths_overlap "$STORE_PATH" "$installer_path"; then
            die "store-path must not overlap an installer artifact: $installer_path"
        fi
    done
}

normalize_runtime_paths() {
    validate_runtime_path_value store-path "$STORE_PATH"
    validate_runtime_path_value workspace-root "$WORKSPACE_ROOT"
    validate_runtime_path_value cache-root "$CACHE_ROOT"
    [ "$WORKSPACE_ROOT" = "$STORE_PATH/workspaces-v3" ] && [ "$CACHE_ROOT" = "$STORE_PATH/mst2-cache" ] ||
        die "installer received paths outside the v3 store layout"
}

validate_runtime_path_value() {
    local field="$1" value="$2"
    [ -n "$value" ] || die "$field must not be empty"
    [[ "$value" != *[[:space:]]* ]] || die "$field must not contain whitespace"
    [[ "$value" =~ ^/?[A-Za-z0-9._+:/-]+$ ]] || \
        die "$field contains a character unsafe for shell or systemd use"
    [[ "$value" != *'//'* ]] || die "$field must not contain repeated slashes"
    case "/${value#/}/" in
        *'/../'*|*'/./'*) die "$field must not contain . or .. path components" ;;
    esac
}

infer_data_root() {
    local failure_message="${1:-cannot infer data-root from an all-relative retained config; pass --data-root}"
    local root_label="${2:-data-root}"
    [[ "$STORE_PATH" == /* ]] || die "$failure_message"
    DATA_ROOT="$(realpath -m -- "$(dirname -- "$STORE_PATH")")"
    note "using $root_label inferred from retained config: $DATA_ROOT"
}

retained_config_has_absolute_anchor() {
    [[ "$STORE_PATH" == /* ]]
}

resolve_relative_runtime_paths() {
    if [[ "$STORE_PATH" != /* ]]; then STORE_PATH="$DATA_ROOT/$STORE_PATH"; fi
    canonicalize_runtime_paths
}

set_generated_runtime_paths() {
    if [ "$STORE_PATH_SET" -eq 1 ]; then STORE_PATH="$REQUESTED_STORE_PATH"; else STORE_PATH="$DATA_ROOT/store"; fi
    WORKSPACE_ROOT="$STORE_PATH/workspaces-v3"
    CACHE_ROOT="$STORE_PATH/mst2-cache"
}

run_scorpio_config_without_overrides() {
    local binary="$1" variable config_path="${CONFDIR}/scorpio.toml"
    local effective_binary="$binary" temporary_binary="" temporary_config="" status=0
    shift
    local -a command=(env)
    while IFS= read -r variable; do
        case "$variable" in
            SCORPIO_*) command+=(-u "$variable") ;;
        esac
    done < <(compgen -e)

    if [ -n "$EXISTING_SERVICE_USER" ]; then
        temporary_binary="$(mktemp /tmp/scorpiofs-installer-scorpio.XXXXXX)" || \
            die "could not create a temporary ScorpioFS binary for retained path resolution"
        cp -- "$binary" "$temporary_binary" || {
            rm -f -- "$temporary_binary"
            die "could not stage ScorpioFS binary for retained path resolution"
        }
        chmod 0755 "$temporary_binary" || {
            rm -f -- "$temporary_binary"
            die "could not make the temporary ScorpioFS binary executable"
        }
        effective_binary="$temporary_binary"
    fi

    if ! run_as_existing_service_user test -r "${CONFDIR}/scorpio.toml"; then
        local -a read_config=(cat)
        if [ "$(id -u)" -ne 0 ]; then
            command -v sudo >/dev/null 2>&1 || \
                die "sudo is required to read protected retained config"
            [ "$DRY_RUN" -eq 0 ] || \
                sudo -v || die "could not obtain read access for retained config during dry-run"
            read_config=(sudo cat)
        fi
        temporary_config="$(run_as_existing_service_user mktemp /tmp/scorpiofs-installer-config.XXXXXX)" || \
            die "could not create a protected retained config copy"
        config_path="$temporary_config"
        if ! "${read_config[@]}" -- "${CONFDIR}/scorpio.toml" | \
            run_as_existing_service_user tee "$config_path" >/dev/null; then
            rm -f -- "$temporary_config"
            [ -n "$temporary_binary" ] && rm -f -- "$temporary_binary"
            die "could not read protected retained config: ${CONFDIR}/scorpio.toml"
        fi
    fi

    if run_as_existing_service_user "${command[@]}" "$effective_binary" \
        --config-path "$config_path" config "$@"; then
        status=0
    else
        status=$?
    fi
    [ -n "$temporary_config" ] && rm -f -- "$temporary_config"
    [ -n "$temporary_binary" ] && rm -f -- "$temporary_binary"
    return "$status"
}

load_configured_runtime_paths() {
    local binary="$1" output_file="${WORKDIR}/installer-paths"
    local -a paths=()
    if ! run_scorpio_config_without_overrides "$binary" installer-paths >"$output_file"; then
        die "could not safely resolve runtime paths from retained config: ${CONFDIR}/scorpio.toml"
    fi
    python3 - "$output_file" <<'PY'
import pathlib, sys
fields = pathlib.Path(sys.argv[1]).read_bytes().split(b"\0")
if len(fields) != 4 or fields[-1] != b"" or any(not field for field in fields[:3]):
    raise SystemExit("installer requires exactly three terminated runtime path records")
PY
    mapfile -d '' -t paths <"$output_file"
    [ "${#paths[@]}" -eq 3 ] || die "installer received an invalid runtime-path response from scorpio"
    STORE_PATH="${paths[0]}"
    WORKSPACE_ROOT="${paths[1]}"
    CACHE_ROOT="${paths[2]}"
}

prepare_effective_runtime_paths() {
    local binary="$1" selected_root_nonempty=0 target_data_root="$DATA_ROOT"
    if data_root_is_nonempty; then
        selected_root_nonempty=1
    fi

    if [ "$EXISTING_CONFIG" -eq 1 ]; then
        load_configured_runtime_paths "$binary"
        normalize_runtime_paths
        if [ "$DATA_ROOT_SET" -eq 0 ]; then
            infer_data_root
        elif [ "$selected_root_nonempty" -eq 1 ]; then
            retained_config_has_absolute_anchor || \
                die "cannot safely use a nonempty data-root with an all-relative retained config; pass the previous data-root or empty the new root"
            DATA_ROOT="$target_data_root"
        elif [ "$RETAIN_CONFIG" -eq 1 ]; then
            retained_config_has_absolute_anchor || \
                die "cannot safely use an empty data-root with an all-relative retained config; pass --overwrite-config or use the previous data-root"
            DATA_ROOT="$target_data_root"
        else
            infer_data_root \
                "cannot resolve previous mount paths from an all-relative config while changing to an empty data-root; rerun with the previous data-root or unmount the old paths manually" \
                "previous data-root"
        fi
        resolve_relative_runtime_paths
        validate_runtime_paths
        PREVIOUS_DATA_ROOT="$(dirname -- "$STORE_PATH")"
        PREVIOUS_STORE_PATH="$STORE_PATH"
        PREVIOUS_WORKSPACE_ROOT="$WORKSPACE_ROOT"
        PREVIOUS_CACHE_ROOT="$CACHE_ROOT"
    fi

    if [ "$RETAIN_CONFIG" -eq 1 ]; then
        note "using runtime paths from retained ${CONFDIR}/scorpio.toml"
        return 0
    fi

    DATA_ROOT="$target_data_root"
    set_generated_runtime_paths
    canonicalize_runtime_paths
    validate_runtime_paths
}

detect_existing_service_user() {
    local candidate=""
    if command -v systemctl >/dev/null 2>&1; then
        candidate="$(systemctl show --property=User --value scorpiofs.service 2>/dev/null || true)"
    fi
    if [ -z "$candidate" ] && [ -r /etc/systemd/system/scorpiofs.service ]; then
        candidate="$(awk -F= '$1 == "User" { print $2; exit }' /etc/systemd/system/scorpiofs.service)"
    fi
    [ -n "$candidate" ] || return 0
    [[ "$candidate" =~ ^([a-z_][a-z0-9_-]{0,31}|[0-9]+)$ ]] || \
        die "existing scorpiofs.service has an unsupported User value: $candidate"
    getent passwd "$candidate" >/dev/null 2>&1 || \
        die "existing scorpiofs.service user does not exist: $candidate"
    EXISTING_SERVICE_USER="$candidate"
    if command -v systemctl >/dev/null 2>&1 && \
        systemctl is-active --quiet scorpiofs.service 2>/dev/null; then
        EXISTING_SERVICE_ACTIVE=1
    fi
}

stop_active_service_for_upgrade() {
    [ "$SETUP_SERVICE" -eq 1 ] || return 0
    [ -n "$EXISTING_SERVICE_USER" ] || return 0
    if run_root systemctl is-active --quiet scorpiofs.service; then
        if [ -n "$OWNED_SERVICE_PID" ]; then
            [ "$(run_readonly systemctl show --property=MainPID --value scorpiofs.service)" = "$OWNED_SERVICE_PID" ] ||
                die "managed service MainPID changed before stopping it"
            capture_service_process "$OWNED_SERVICE_PID" "${WORKDIR}/process-before-stop" ||
                die "managed service disappeared before its planned stop"
            verify_captured_service_process "${WORKDIR}/process-before" "${WORKDIR}/process-before-stop" ||
                die "managed service process/socket authority changed before stopping it"
        fi
        note "stopping the active service before replacing its binary, config, or unit"
        if ! run_root systemctl stop scorpiofs.service; then
            die "could not stop scorpiofs.service before upgrading it"
        fi
        SERVICE_STOPPED_FOR_UPGRADE=1
    fi
}

validate_service_manager() {
    [ "$SETUP_SERVICE" -eq 1 ] || return 0
    command -v systemctl >/dev/null 2>&1 || \
        die "systemctl is required for service setup; pass --no-service for a user-run installation"
    if [ "$DRY_RUN" -eq 0 ] && ! systemctl show-environment >/dev/null 2>&1; then
        die "systemd is not running or cannot be reached; pass --no-service for a user-run installation"
    fi
}

validate_bind() {
    local value="$1" host port
    if [[ "$value" =~ ^\[([^]]+)\]:([0-9]+)$ ]]; then
        host="${BASH_REMATCH[1]}"
        port="${BASH_REMATCH[2]}"
        validate_ipv6 "--http-addr" "$host"
    elif [[ "$value" =~ ^([^:]+):([0-9]+)$ ]]; then
        host="${BASH_REMATCH[1]}"
        port="${BASH_REMATCH[2]}"
        validate_ipv4 "--http-addr" "$host"
    else
        die "--http-addr must be IPv4:port or [IPv6]:port"
    fi
    validate_port "--http-addr" "$port"
    BIND_HOST="$host"
    BIND_PORT="$port"
}

is_loopback_host() {
    [[ "$1" == 127.* || "$1" == "::1" ]]
}

health_endpoint() {
    local host="$BIND_HOST"
    case "$host" in
        0.0.0.0) host="127.0.0.1" ;;
        ::) host="[::1]" ;;
        *:*) host="[$host]" ;;
    esac
    printf 'http://%s:%s/health' "$host" "$BIND_PORT"
}

normalize_url() {
    local value="$1"
    while [ "${value%/}" != "$value" ]; do
        case "$value" in http://|https://) break ;; esac
        value="${value%/}"
    done
    printf '%s' "$value"
}

parse_args() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --version) [ "$#" -ge 2 ] || die "--version needs a value"; VERSION="$2"; shift 2 ;;
            --release-base-url) [ "$#" -ge 2 ] || die "--release-base-url needs a value"; RELEASE_BASE_URL="$2"; shift 2 ;;
            --prefix) [ "$#" -ge 2 ] || die "--prefix needs a value"; PREFIX="$2"; shift 2 ;;
            --config-dir) [ "$#" -ge 2 ] || die "--config-dir needs a value"; CONFDIR="$2"; shift 2 ;;
            --data-root) [ "$#" -ge 2 ] || die "--data-root needs a value"; DATA_ROOT="$2"; DATA_ROOT_SET=1; shift 2 ;;
            --mst2-base-url) [ "$#" -ge 2 ] || die "--mst2-base-url needs a value"; MST2_BASE_URL="$2"; shift 2 ;;
            --store-path) [ "$#" -ge 2 ] || die "--store-path needs a value"; STORE_PATH="$2"; STORE_PATH_SET=1; shift 2 ;;
            --http-addr) [ "$#" -ge 2 ] || die "--http-addr needs a value"; HTTP_ADDR="$2"; shift 2 ;;
            --allow-public-api) ALLOW_PUBLIC_API=1; shift ;;
            --no-service) SETUP_SERVICE=0; SERVICE_CHOICE_SET=1; shift ;;
            --non-interactive) INTERACTIVE=0; shift ;;
            --overwrite-config) OVERWRITE_CONFIG=1; CONFIG_CHOICE_SET=1; shift ;;
            --yes) ASSUME_YES=1; shift ;;
            --dry-run) DRY_RUN=1; shift ;;
            --uninstall) DO_UNINSTALL=1; INTERACTIVE=0; shift ;;
            --no-deps) INSTALL_DEPS=0; shift ;;
            --enable-user-allow-other) ENABLE_USER_ALLOW_OTHER=1; FUSE_CHOICE_SET=1; shift ;;
            --no-user-allow-other) ENABLE_USER_ALLOW_OTHER=0; FUSE_CHOICE_SET=1; shift ;;
            -h|--help) usage; exit 0 ;;
            *) die "unknown option: $1 (see --help)" ;;
        esac
    done
}

apply_environment_options() {
    case "${SCORPIO_OVERWRITE_CONFIG:-}" in
        ""|0|false|FALSE|no|NO) ;;
        1|true|TRUE|yes|YES) OVERWRITE_CONFIG=1; CONFIG_CHOICE_SET=1 ;;
        *) die "SCORPIO_OVERWRITE_CONFIG must be 1/0, true/false, or yes/no" ;;
    esac
}

open_interactive_tty() {
    [ "$INTERACTIVE" -eq 1 ] && [ "$ASSUME_YES" -eq 0 ] || return 0
    if ! { exec 3<>/dev/tty; } 2>/dev/null; then
        die "interactive input requires a controlling terminal; use --non-interactive with --mst2-base-url"
    fi
    TTY_FD=3
}

refuse_darwin() {
    case "$(uname -s)" in
        Darwin)
            die "install.sh is Linux-only. Download scorpiofs-<ver>-aarch64-apple-darwin.tar.gz from GitHub Releases and follow docs/macos.md"
            ;;
    esac
}

detect_target() {
    refuse_darwin
    case "$(uname -m)" in
        x86_64|amd64) echo "x86_64-unknown-linux-gnu" ;;
        aarch64|arm64) echo "aarch64-unknown-linux-musl" ;;
        *) die "unsupported architecture: $(uname -m)" ;;
    esac
}

check_tools() {
    if command -v curl >/dev/null 2>&1; then
        DOWNLOADER="curl"
    elif command -v wget >/dev/null 2>&1; then
        DOWNLOADER="wget"
    else
        die "curl or wget is required"
    fi
    command -v tar >/dev/null 2>&1 || die "tar is required"
    command -v realpath >/dev/null 2>&1 || die "realpath is required"
    command -v find >/dev/null 2>&1 || die "find is required"
}

fetch() {
    local url="$1" dest="$2"
    if [ "$DOWNLOADER" = "curl" ]; then
        curl -fsSL --connect-timeout 10 --max-time 300 "$url" -o "$dest"
    else
        wget -q --timeout=30 --tries=3 "$url" -O "$dest"
    fi
}

resolve_version() {
    [ -n "$VERSION" ] && return 0
    note "resolving the latest ScorpioFS release"
    local release_json
    if [ "$DOWNLOADER" = "curl" ]; then
        release_json="$(curl -fsSL --connect-timeout 10 --max-time 30 "https://api.github.com/repos/${REPO}/releases/latest")" \
            || die "could not query the latest release; pass --version vX.Y.Z"
    else
        release_json="$(wget -q --timeout=30 --tries=3 -O - "https://api.github.com/repos/${REPO}/releases/latest")" \
            || die "could not query the latest release; pass --version vX.Y.Z"
    fi
    VERSION="$(printf '%s' "$release_json" | awk -F'"' '/"tag_name"[[:space:]]*:/ { print $4; exit }')"
    [ -n "$VERSION" ] || die "GitHub returned no release tag; pass --version vX.Y.Z"
}

normalize_version() {
    [[ "$VERSION" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+[A-Za-z0-9.+-]*$ ]] || die "invalid version: $VERSION"
    case "$VERSION" in v*) ;; *) VERSION="v$VERSION" ;; esac
}

pkg_install() {
    [ "$INSTALL_DEPS" -eq 1 ] || { note "skipping dependency installation (--no-deps)"; return 0; }
    if command -v apt-get >/dev/null 2>&1; then
        run_root apt-get update
        run_root apt-get install -y --no-install-recommends fuse3 openssl ca-certificates util-linux python3
    elif command -v dnf >/dev/null 2>&1; then
        run_root dnf install -y fuse3 openssl ca-certificates util-linux python3
    elif command -v pacman >/dev/null 2>&1; then
        run_root pacman -Sy --noconfirm fuse3 openssl ca-certificates util-linux python
    else
        warn "no supported package manager found; install fuse3, openssl, ca-certificates, util-linux, and python3 manually"
    fi
}

check_runtime_tools() {
    command -v findmnt >/dev/null 2>&1 || die "findmnt is required (install util-linux)"
    command -v runuser >/dev/null 2>&1 || die "runuser is required (install util-linux)"
    command -v timeout >/dev/null 2>&1 || die "timeout is required (install coreutils)"
    command -v python3 >/dev/null 2>&1 || die "python3 is required to verify live workspace and mount identities"
}

check_fuse() {
    if [ ! -e /dev/fuse ] && command -v modprobe >/dev/null 2>&1; then
        run_root modprobe fuse 2>/dev/null || true
    fi
    [ -e /dev/fuse ] || warn "/dev/fuse is missing; ScorpioFS will not mount until FUSE is enabled"
    command -v fusermount3 >/dev/null 2>&1 || warn "fusermount3 is missing; verify the fuse3 package installation"
}

configure_interactively() {
    [ "$INTERACTIVE" -eq 1 ] || return 0
    printf '\nScorpioFS interactive installer\n'
    printf 'The remote HTTP API is unauthenticated and will default to loopback.\n\n'
    prompt_value MST2_BASE_URL "MST/2 service URL" "${MST2_BASE_URL:-http://localhost:8000}"
    local previous_data_root="$DATA_ROOT" store_default
    prompt_value DATA_ROOT "Data root" "$DATA_ROOT"
    [ "$DATA_ROOT" = "$previous_data_root" ] || DATA_ROOT_SET=1
    store_default="${STORE_PATH:-$DATA_ROOT/store}"
    prompt_value STORE_PATH "Local v3 store" "$store_default"
    [ "$STORE_PATH" = "$store_default" ] || STORE_PATH_SET=1
    prompt_value HTTP_ADDR "HTTP listen address" "$HTTP_ADDR"
    if [ "$FUSE_CHOICE_SET" -eq 0 ]; then
        prompt_yes_no ENABLE_USER_ALLOW_OTHER "Enable user_allow_other in /etc/fuse.conf" "y"
    fi
    if [ "$SERVICE_CHOICE_SET" -eq 0 ]; then
        prompt_yes_no SETUP_SERVICE "Install and start systemd service" "y"
    fi
    if [ "$SETUP_SERVICE" -eq 1 ]; then
        prompt_value SERVICE_USER "systemd service user" "$SERVICE_USER"
    fi
    if [ -f "${CONFDIR}/scorpio.toml" ] && [ "$CONFIG_CHOICE_SET" -eq 0 ]; then
        prompt_yes_no OVERWRITE_CONFIG "Overwrite existing ${CONFDIR}/scorpio.toml" "n"
    fi
    validate_bind "$HTTP_ADDR"
    if ! is_loopback_host "$BIND_HOST" && [ "$ALLOW_PUBLIC_API" -ne 1 ]; then
        warn "${HTTP_ADDR} is not loopback. ScorpioFS has no HTTP authentication."
        prompt_yes_no PUBLIC_API_OK "Continue with an externally reachable API only behind a firewall/auth proxy" "n"
        if [ "$PUBLIC_API_OK" -eq 1 ]; then ALLOW_PUBLIC_API=1; else HTTP_ADDR="127.0.0.1:2725"; fi
    fi
}

current_user_can_search_config_dir() {
    local directory="$CONFDIR" parent
    while [ ! -e "$directory" ] && [ "$directory" != / ]; do
        parent="${directory%/*}"
        [ -n "$parent" ] || parent=/
        [ "$parent" != "$directory" ] || break
        directory="$parent"
    done
    [ -d "$directory" ] && [ -x "$directory" ]
}

detect_existing_config() {
    local config_path="${CONFDIR}/scorpio.toml"
    local -a privileged_test
    if [ -L "$config_path" ]; then
        die "config file must not be a symbolic link: $config_path"
    elif [ -f "$config_path" ]; then
        EXISTING_CONFIG=1
    elif [ -e "$config_path" ]; then
        die "config path exists but is not a regular file: $config_path"
    elif current_user_can_search_config_dir; then
        return 0
    else
        if [ "$(id -u)" -eq 0 ]; then
            return 0
        fi
        command -v sudo >/dev/null 2>&1 || \
            die "cannot inspect protected config path without sudo: $config_path"
        sudo -v || die "could not obtain permission to inspect protected config path: $config_path"
        SUDO_BIN="sudo"
        privileged_test=(sudo test)
        if "${privileged_test[@]}" -L "$config_path"; then
            die "config file must not be a symbolic link: $config_path"
        elif "${privileged_test[@]}" -f "$config_path"; then
            EXISTING_CONFIG=1
        elif "${privileged_test[@]}" -e "$config_path"; then
            die "config path exists but is not a regular file: $config_path"
        else
            return 0
        fi
    fi
    if [ "$OVERWRITE_CONFIG" -ne 1 ]; then RETAIN_CONFIG=1; fi
}

validate_inputs() {
    MST2_BASE_URL="$(normalize_url "$MST2_BASE_URL")"
    PREFIX="$(normalize_path "$PREFIX")"
    CONFDIR="$(normalize_path "$CONFDIR")"
    DATA_ROOT="$(normalize_path "$DATA_ROOT")"
    STORE_PATH="$(normalize_path "$STORE_PATH")"
    validate_url mst2_base_url "$MST2_BASE_URL"
    validate_url release-base-url "$RELEASE_BASE_URL"
    validate_path prefix "$PREFIX"
    validate_path config-dir "$CONFDIR"
    validate_path data-root "$DATA_ROOT"
    validate_path store-path "$STORE_PATH"
    canonicalize_paths
    validate_path prefix "$PREFIX"
    validate_path config-dir "$CONFDIR"
    validate_path data-root "$DATA_ROOT"
    validate_path store-path "$STORE_PATH"
    canonicalize_runtime_paths
    detect_existing_config
    validate_service_manager
    detect_existing_service_user
    validate_data_paths
    validate_runtime_paths
    validate_bind "$HTTP_ADDR"
    is_loopback_host "$BIND_HOST" || [ "$ALLOW_PUBLIC_API" -eq 1 ] ||
        die "refusing non-loopback HTTP bind without --allow-public-api"
    [[ "$SERVICE_USER" =~ ^[a-z_][a-z0-9_-]{0,31}$ ]] || die "invalid service user: $SERVICE_USER"
}

prepare_release_binaries() {
    local target tarball base url sumurl
    target="$(detect_target)"
    tarball="scorpiofs-${VERSION}-${target}.tar.gz"
    base="${RELEASE_BASE_URL%/}/${VERSION}"
    url="${base}/${tarball}"
    sumurl="${url}.sha256"

    if [ "$DRY_RUN" -eq 1 ]; then
        if [ "$EXISTING_CONFIG" -eq 1 ]; then
            prepare_release_archive
            prepare_effective_runtime_paths "${EXTRACTED_RELEASE}/scorpio"
        else
            note "would download ${url}"
            note "would verify ${sumurl} with SHA256"
        fi
        note "would install ${PREFIX}/bin/scorpio"
        return 0
    fi

    prepare_release_archive
    prepare_effective_runtime_paths "${EXTRACTED_RELEASE}/scorpio"
}

prepare_release_archive() {
    local target tarball base url sumurl extracted
    target="$(detect_target)"
    tarball="scorpiofs-${VERSION}-${target}.tar.gz"
    base="${RELEASE_BASE_URL%/}/${VERSION}"
    url="${base}/${tarball}"
    sumurl="${url}.sha256"
    WORKDIR="$(mktemp -d)"
    note "downloading ${tarball}"
    fetch "$url" "${WORKDIR}/${tarball}" || die "release asset unavailable for ${target} and ${VERSION}"
    fetch "$sumurl" "${WORKDIR}/${tarball}.sha256" || die "release checksum unavailable: ${sumurl}"
    note "verifying SHA256 checksum"
    if command -v sha256sum >/dev/null 2>&1; then
        (cd "$WORKDIR" && sha256sum -c "${tarball}.sha256") || die "checksum verification failed"
    elif command -v shasum >/dev/null 2>&1; then
        local expected actual
        expected="$(awk '{print $1; exit}' "${WORKDIR}/${tarball}.sha256")"
        actual="$(shasum -a 256 "${WORKDIR}/${tarball}" | awk '{print $1}')"
        [ "$expected" = "$actual" ] || die "checksum verification failed"
    else
        die "sha256sum or shasum is required for checksum verification"
    fi

    note "extracting and checking the scorpio release binary"
    tar -xzf "${WORKDIR}/${tarball}" -C "$WORKDIR"
    extracted="${WORKDIR}/scorpiofs-${VERSION}-${target}"
    if [ ! -x "${extracted}/scorpio" ]; then
        die "release archive has an unexpected layout"
    fi
    local smoke_output
    if ! smoke_output="$("${extracted}/scorpio" --version 2>&1)"; then
        die "downloaded scorpio cannot run on this host: ${smoke_output}. Install a release built for this Linux distribution"
    fi
    EXTRACTED_RELEASE="$extracted"
}

ensure_workdir() {
    [ -n "$WORKDIR" ] && [ -d "$WORKDIR" ] && return 0
    WORKDIR="$(mktemp -d)" || die "could not create installer working directory"
}

install_release_binaries() {
    if [ "$DRY_RUN" -eq 1 ]; then
        return 0
    fi
    [ -n "$EXTRACTED_RELEASE" ] || die "internal error: release binaries were not prepared"
    note "installing release binaries to ${PREFIX}/bin"
    run_root install -d "${PREFIX}/bin"
    run_root install -m 0755 "${EXTRACTED_RELEASE}/scorpio" "${PREFIX}/bin/scorpio"
    # Remove the retired v2 entry point after the v3 binary is installed.
    run_root rm -f -- "${PREFIX}/bin/antares"
}

backup_upgrade_artifacts() {
    [ "$DRY_RUN" -eq 0 ] || return 0
    [ -n "$WORKDIR" ] || die "internal error: upgrade backup requires a working directory"
    ARTIFACT_BACKUP_DIR="${WORKDIR}/previous-install"
    mkdir -m 0700 -- "$ARTIFACT_BACKUP_DIR"
    [ "$SERVICE_STOPPED_FOR_UPGRADE" -eq 1 ] || return 0
    if run_root test -e "${PREFIX}/bin/scorpio"; then
        HAD_OLD_SCORPIO=1
        run_root cp -a -- "${PREFIX}/bin/scorpio" "${ARTIFACT_BACKUP_DIR}/scorpio"
    fi
    if run_root test -e "${CONFDIR}/scorpio.toml"; then
        HAD_OLD_CONFIG=1
        run_root cp -a -- "${CONFDIR}/scorpio.toml" "${ARTIFACT_BACKUP_DIR}/scorpio.toml"
    fi
    if run_root test -e /etc/systemd/system/scorpiofs.service; then
        HAD_OLD_UNIT=1
        run_root cp -a -- /etc/systemd/system/scorpiofs.service \
            "${ARTIFACT_BACKUP_DIR}/scorpiofs.service"
    fi
    ARTIFACT_BACKUP_READY=1
}

restore_upgrade_artifact() {
    local had_old="$1" backup="$2" target="$3"
    if ! run_root rm -f -- "$target"; then
        return 1
    fi
    if [ "$had_old" -eq 1 ]; then
        run_root test -f "$backup" || return 1
        run_root cp -a -- "$backup" "$target"
    fi
}

restore_upgrade_artifacts() {
    local restore_failed=0
    warn "restoring ScorpioFS artifacts from before the failed upgrade"
    restore_upgrade_artifact "$HAD_OLD_SCORPIO" \
        "${ARTIFACT_BACKUP_DIR}/scorpio" "${PREFIX}/bin/scorpio" || restore_failed=1
    restore_upgrade_artifact "$HAD_OLD_CONFIG" \
        "${ARTIFACT_BACKUP_DIR}/scorpio.toml" "${CONFDIR}/scorpio.toml" || restore_failed=1
    restore_upgrade_artifact "$HAD_OLD_UNIT" \
        "${ARTIFACT_BACKUP_DIR}/scorpiofs.service" \
        /etc/systemd/system/scorpiofs.service || restore_failed=1
    if [ "$HAD_OLD_UNIT" -eq 1 ]; then
        run_root systemctl daemon-reload || restore_failed=1
    fi
    [ "$restore_failed" -eq 0 ]
}

restore_runtime_ownership() {
    [ -n "$EXISTING_SERVICE_USER" ] || return 0
    [ "$EXISTING_SERVICE_USER" = "$SERVICE_USER" ] && return 0
    local previous_group runtime_dir restore_failed=0
    previous_group="$(id -gn "$EXISTING_SERVICE_USER")" || return 1
    if ! validate_mount_targets "$PREVIOUS_DATA_ROOT" "$PREVIOUS_STORE_PATH"; then
        warn "${MOUNT_VALIDATION_ERROR}; skipping ownership restore"
        return 1
    fi
    if [ -n "$PREVIOUS_DATA_ROOT" ] && run_root test -d "$PREVIOUS_DATA_ROOT" &&
        ! run_root chown "$EXISTING_SERVICE_USER:$previous_group" -- "$PREVIOUS_DATA_ROOT"; then
        restore_failed=1
    fi
    if [ -n "$PREVIOUS_STORE_PATH" ] && run_root test -d "$PREVIOUS_STORE_PATH" &&
        ! run_root chown -R -h -P "$EXISTING_SERVICE_USER:$previous_group" -- "$PREVIOUS_STORE_PATH"; then
        restore_failed=1
    fi
    [ "$restore_failed" -eq 0 ]
}

ensure_service_account() {
    [ "$SETUP_SERVICE" -eq 1 ] || return 0
    if [ "$DRY_RUN" -eq 1 ]; then
        TARGET_USER="$SERVICE_USER"
        TARGET_GROUP="$SERVICE_USER"
        note "would create system user ${SERVICE_USER} and add it to the fuse group"
        return 0
    fi
    if ! getent passwd "$SERVICE_USER" >/dev/null 2>&1; then
        note "creating system user ${SERVICE_USER}"
        run_root useradd --system --user-group --home-dir "$DATA_ROOT" --shell /usr/sbin/nologin "$SERVICE_USER"
    fi
    TARGET_USER="$SERVICE_USER"
    TARGET_GROUP="$(id -gn "$SERVICE_USER")" || die "could not determine the primary group for $SERVICE_USER"
    if getent group fuse >/dev/null 2>&1; then
        run_root usermod -aG fuse "$SERVICE_USER"
    else
        warn "fuse group does not exist; the service may not be able to access /dev/fuse"
    fi
}

validate_service_config_traversal() {
    [ "$SETUP_SERVICE" -eq 1 ] || return 0
    [ "$EXISTING_CONFIG" -eq 1 ] || return 0
    [ "$DRY_RUN" -eq 0 ] || return 0
    if ! run_root runuser -u "$SERVICE_USER" -- test -x "$CONFDIR"; then
        die "service user $SERVICE_USER cannot traverse config-dir $CONFDIR; grant directory execute access before changing service users"
    fi
}

prepare_directories() {
    local path parent
    local -a directories
    if [ "$SETUP_SERVICE" -eq 1 ]; then
        TARGET_USER="$SERVICE_USER"
        if [ "$DRY_RUN" -eq 0 ]; then
            TARGET_GROUP="$(id -gn "$SERVICE_USER")" || die "could not determine the primary group for $SERVICE_USER"
        else
            TARGET_GROUP="$SERVICE_USER"
        fi
    else
        if [ -n "$EXISTING_SERVICE_USER" ]; then
            TARGET_USER="$EXISTING_SERVICE_USER"
            TARGET_GROUP="$(id -gn "$TARGET_USER")" || \
                die "could not determine the primary group for existing service user $TARGET_USER"
            note "preserving ownership for existing scorpiofs.service user: $TARGET_USER"
        else
            TARGET_USER="${SUDO_USER:-$(id -un)}"
            TARGET_GROUP="$(id -gn "$TARGET_USER" 2>/dev/null || id -gn)"
        fi
    fi
    directories=("$DATA_ROOT" "$STORE_PATH" "$WORKSPACE_ROOT" "$CACHE_ROOT")
    parent="$(dirname -- "$STORE_PATH")"
    while [[ "$parent" == "$DATA_ROOT"/* ]]; do
        directories+=("$parent")
        parent="$(dirname -- "$parent")"
    done
    validate_runtime_migration_mounts
    # mkdir preserves modes on existing directories; install -d would reset
    # hardened data directories to its 0755 default during every upgrade.
    run_root mkdir -p -m 0755 -- "${directories[@]}"
    run_root chown "$TARGET_USER:$TARGET_GROUP" "${directories[@]}"
    run_root mkdir -p -m 0755 -- "$CONFDIR"
}

capture_service_process() {
    local pid="$1" output="$2"
    mkdir -p "$output"
    run_readonly cat "/proc/$pid/status" >"$output/status" &&
    run_readonly cat "/proc/$pid/stat" >"$output/stat" &&
    run_readonly cat "/proc/$pid/cmdline" >"$output/cmdline" &&
    run_readonly stat -Lc '%d:%i' "/proc/$pid/exe" >"$output/exe" &&
    run_readonly stat -Lc '%d:%i' "${PREFIX}/bin/scorpio" >"$output/installed-exe" &&
    run_readonly find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 -type l -printf '%l\n' >"$output/fds" &&
    run_readonly cat /proc/net/tcp >"$output/tcp" &&
    run_readonly cat /proc/net/tcp6 >"$output/tcp6"
}

verify_captured_service_process() {
    python3 - "$1" "$2" <<'PY'
import json, pathlib, sys
before, after = map(pathlib.Path, sys.argv[1:])
for name in ("cmdline", "exe", "installed-exe"):
    if before.joinpath(name).read_bytes() != after.joinpath(name).read_bytes():
        raise SystemExit("managed process authority changed")
def uids(root):
    return [line.split()[1:] for line in root.joinpath("status").read_text().splitlines() if line.startswith("Uid:")]
if uids(before) != uids(after):
    raise SystemExit("managed process UID changed")
def started(root):
    return root.joinpath("stat").read_text().rsplit(")", 1)[1].split()[19]
if started(before) != started(after):
    raise SystemExit("managed process was replaced")
inode = before.joinpath("http-inode").read_text()
if "socket:[" + inode + "]" not in after.joinpath("fds").read_text().splitlines():
    raise SystemExit("managed HTTP listener was replaced")
name, address, uid, inode = json.loads(before.joinpath("http-listener").read_text())
matches = []
for line in after.joinpath(name).read_text().splitlines()[1:]:
    fields = line.split()
    if len(fields) >= 10 and fields[1] == address and fields[3] == "0A" and fields[7] == uid and fields[9] == inode:
        matches.append(line)
if len(matches) != 1:
    raise SystemExit("managed HTTP socket no longer proves the captured LISTEN bind")
PY
}

capture_managed_runtime_mounts() {
    OWNED_MOUNTS_FILE="${WORKDIR}/owned-mounts.json"
    local mountinfo="${WORKDIR}/mountinfo-before" candidates="${WORKDIR}/mount-candidates.json"
    local uid="" pid endpoint status
    if [ -n "$EXISTING_SERVICE_USER" ]; then uid="$(id -u "$EXISTING_SERVICE_USER")"; fi
    run_readonly cat /proc/self/mountinfo >"$mountinfo" || die "could not inspect kernel mount identities"
    python3 - "$mountinfo" "$candidates" "$uid" "$DATA_ROOT" "$PREVIOUS_DATA_ROOT" "$STORE_PATH" "$PREVIOUS_STORE_PATH" <<'PY'
import json, pathlib, sys
mountinfo, output, uid, data_root, old_data_root, *stores = sys.argv[1:]
stores = [root for root in stores if root]
roots = [root for root in (data_root, old_data_root, *stores) if root]
records = []
for line in pathlib.Path(mountinfo).read_text().splitlines():
    left, separator, right = line.partition(" - ")
    if not separator:
        raise SystemExit("invalid kernel mountinfo")
    fields, tail = left.split(), right.split()
    if len(fields) < 6 or len(tail) < 3:
        raise SystemExit("invalid kernel mountinfo fields")
    target = fields[4]
    if not any(target == root or target.startswith(root + "/") for root in roots):
        continue
    workspace_roots = [root + "/workspaces-v3" for root in stores if target.startswith(root + "/workspaces-v3/")]
    allowed = bool(workspace_roots) and target.endswith("/mount")
    options = (fields[5] + "," + tail[2]).split(",")
    if not allowed or tail[0] not in ("fuse", "fuse.scorpiofs-v3") or tail[1] != "scorpiofs-v3" or options.count("user_id=" + uid) != 1:
        raise SystemExit("refusing unknown or foreign runtime mount before any ownership change: " + target)
    if any(item["target"] == target for item in records):
        raise SystemExit("refusing stacked runtime mounts: " + target)
    for parent in pathlib.Path(target).parents:
        if any(str(parent) == root or str(parent).startswith(root + "/") for root in stores) and parent.is_symlink():
            raise SystemExit("workspace mount ancestor is a symbolic link: " + str(parent))
    records.append({"target": target, "line": line, "workspace_root": workspace_roots[0]})
pathlib.Path(output).write_text(json.dumps(records))
PY
    [ "$?" -eq 0 ] || die "runtime mounts have no trusted v3 ownership"
    if [ "$(cat "$candidates")" = "[]" ]; then
        cp "$candidates" "$OWNED_MOUNTS_FILE"
        return 0
    fi
    [ "$SETUP_SERVICE" -eq 1 ] && [ "$EXISTING_SERVICE_ACTIVE" -eq 1 ] && [ -n "$EXISTING_SERVICE_USER" ] ||
        die "runtime mounts require an active managed service; unmount unknown mounts manually and retry"
    command -v curl >/dev/null 2>&1 || die "curl is required to verify managed workspace ownership"
    if [ "$DRY_RUN" -eq 1 ] && [ "$(id -u)" -ne 0 ] && [ -z "$SUDO_BIN" ]; then
        command -v sudo >/dev/null 2>&1 || die "sudo is required to inspect the managed service during dry-run"
        sudo -n -v || die "could not obtain read access to the managed service during dry-run"
        SUDO_BIN="sudo"
    fi
    pid="$(run_readonly systemctl show --property=MainPID --value scorpiofs.service)" ||
        die "could not identify the running managed service"
    [[ "$pid" =~ ^[1-9][0-9]*$ ]] || die "managed service has no live MainPID"
    capture_service_process "$pid" "${WORKDIR}/process-before" ||
        die "could not inspect the managed service process"
    endpoint="$(python3 - "${WORKDIR}/process-before" "$uid" <<'PY'
import ipaddress, json, pathlib, sys
directory, uid = pathlib.Path(sys.argv[1]), sys.argv[2]
status = directory.joinpath("status").read_text().splitlines()
ids = [line.split()[1:] for line in status if line.startswith("Uid:")]
if len(ids) != 1 or ids[0] != [uid] * 4:
    raise SystemExit("managed service process UID does not match its unit")
exe = directory.joinpath("exe").read_text().split()
if len(exe) != 1 or exe[0] != directory.joinpath("installed-exe").read_text().strip():
    raise SystemExit("managed service executable is not the installed Scorpio binary")
args = directory.joinpath("cmdline").read_bytes().split(b"\0")
args = [arg.decode("utf-8") for arg in args if arg]
if "serve" not in args:
    raise SystemExit("managed process is not the workspace daemon")
binds = []
for i, arg in enumerate(args):
    if arg == "--http-addr" and i + 1 < len(args):
        binds.append(args[i + 1])
    elif arg.startswith("--http-addr="):
        binds.append(arg.split("=", 1)[1])
if len(binds) != 1:
    raise SystemExit("managed process has no unique explicit HTTP bind")
bind = binds[0]
if bind.startswith("["):
    host, port = bind[1:].split("]:")
else:
    host, port = bind.rsplit(":", 1)
address, port = ipaddress.ip_address(host), int(port)
if not 1 <= port <= 65535:
    raise SystemExit("invalid managed process port")
fds = set()
for line in directory.joinpath("fds").read_text().splitlines():
    if line.startswith("socket:[") and line.endswith("]"):
        fds.add(line[8:-1])
listeners = []
for name, version in (("tcp", 4), ("tcp6", 6)):
    for line in directory.joinpath(name).read_text().splitlines()[1:]:
        fields = line.split()
        if len(fields) < 10 or fields[3] != "0A" or fields[9] not in fds or fields[7] != uid:
            continue
        raw, candidate_port = fields[1].split(":")
        encoded = bytes.fromhex(raw)
        decoded = encoded[::-1] if version == 4 else b"".join(encoded[i:i+4][::-1] for i in range(0, 16, 4))
        if ipaddress.ip_address(decoded) == address and int(candidate_port, 16) == port:
            listeners.append((name, fields[1], fields[7], fields[9]))
if len(listeners) != 1:
    raise SystemExit("HTTP bind is not a unique LISTEN socket owned by the managed process")
directory.joinpath("http-inode").write_text(listeners[0][3])
directory.joinpath("http-listener").write_text(json.dumps(listeners[0]))
host = str(address)
if address.is_unspecified:
    host = "127.0.0.1" if address.version == 4 else "::1"
if address.version == 6:
    host = "[" + host + "]"
print("http://" + host + ":" + str(port))
PY
)" || die "managed service HTTP endpoint has no process/socket authority"
    status="$(curl -fsS --noproxy '*' --connect-timeout 2 --max-time 5 --max-filesize 4194304 \
        -o "${WORKDIR}/managed-workspaces.json" -w '%{http_code}' "$endpoint/v3/workspaces")" ||
        die "could not obtain live managed workspace identities"
    [ "$status" = 200 ] || die "managed workspace list did not return HTTP 200"
    [ "$(run_readonly systemctl show --property=MainPID --value scorpiofs.service)" = "$pid" ] ||
        die "managed service changed during mount ownership verification"
    capture_service_process "$pid" "${WORKDIR}/process-after" ||
        die "managed service disappeared during ownership verification"
    verify_captured_service_process "${WORKDIR}/process-before" "${WORKDIR}/process-after" ||
        die "managed service authority changed during the workspace request"
    python3 - "$candidates" "${WORKDIR}/managed-workspaces.json" "$OWNED_MOUNTS_FILE" <<'PY'
import json, pathlib, sys, uuid
payload = pathlib.Path(sys.argv[2]).read_bytes()
if len(payload) > 4194304:
    raise SystemExit("managed workspace list exceeds its response cap")
workspaces = json.loads(payload)
if not isinstance(workspaces, list):
    raise SystemExit("invalid managed workspace list")
records = json.loads(pathlib.Path(sys.argv[1]).read_text())
for record in records:
    matches = [w for w in workspaces if isinstance(w, dict) and w.get("mountpoint") == record["target"]]
    if len(matches) != 1:
        raise SystemExit("kernel mount lacks one exact live workspace: " + record["target"])
    workspace = matches[0]
    for name in ("workspace_id", "generation"):
        value = workspace.get(name)
        if not isinstance(value, str) or str(uuid.UUID(value)) != value:
            raise SystemExit("invalid managed workspace identity")
    if workspace.get("mount_state") != "mounted" or workspace.get("metadata_ready") is not True:
        raise SystemExit("live daemon cannot prove its native mount identity")
    if record["target"] != record["workspace_root"] + "/" + workspace["workspace_id"] + "/mount":
        raise SystemExit("workspace UUID does not identify the exact mountpoint")
    record.update(workspace_id=workspace["workspace_id"], generation=workspace["generation"])
pathlib.Path(sys.argv[3]).write_text(json.dumps(records))
PY
    [ "$?" -eq 0 ] || die "runtime mounts lack live managed ownership"
    OWNED_SERVICE_PID="$pid"
}

recover_managed_runtime_mounts() {
    local mountinfo="${WORKDIR}/mountinfo-after" targets="${WORKDIR}/detach-targets"
    local target
    run_readonly cat /proc/self/mountinfo >"$mountinfo" || die "could not inspect mounts after stopping the service"
    python3 - "$mountinfo" "$OWNED_MOUNTS_FILE" "$targets" "$DATA_ROOT" "$PREVIOUS_DATA_ROOT" "$STORE_PATH" "$PREVIOUS_STORE_PATH" <<'PY'
import json, pathlib, sys
mountinfo, owned, output, *roots = sys.argv[1:]
roots = [root for root in roots if root]
records = {item["target"]: item["line"] for item in json.loads(pathlib.Path(owned).read_text())}
targets = []
for line in pathlib.Path(mountinfo).read_text().splitlines():
    fields = line.split()
    if len(fields) < 6:
        raise SystemExit("invalid kernel mountinfo")
    target = fields[4]
    if not any(target == root or target.startswith(root + "/") for root in roots):
        continue
    if records.get(target) != line:
        raise SystemExit("runtime mount identity changed or became foreign: " + target)
    if target in targets:
        raise SystemExit("runtime mount became stacked: " + target)
    targets.append(target)
pathlib.Path(output).write_text("\n".join(sorted(targets, key=len, reverse=True)) + ("\n" if targets else ""))
PY
    [ "$?" -eq 0 ] || die "refusing changed runtime mounts after stopping service"
    while IFS= read -r target; do
        [ -n "$target" ] || continue
        [ "$SERVICE_STOPPED_FOR_UPGRADE" -eq 1 ] || die "refusing to detach a mount while its owned service is running"
        run_readonly cat /proc/self/mountinfo >"${WORKDIR}/mountinfo-detach" ||
            die "could not reinspect the exact residual mount"
        python3 - "${WORKDIR}/mountinfo-detach" "$OWNED_MOUNTS_FILE" "$target" "$DATA_ROOT" "$PREVIOUS_DATA_ROOT" "$STORE_PATH" "$PREVIOUS_STORE_PATH" <<'PY'
import json, pathlib, sys
current, owned, target, *roots = sys.argv[1:]
roots = [root for root in roots if root]
frozen = {entry["target"]: entry["line"] for entry in json.loads(pathlib.Path(owned).read_text())}
actual = {}
for line in pathlib.Path(current).read_text().splitlines():
    fields = line.split()
    if len(fields) < 6 or " - " not in line:
        raise SystemExit("invalid kernel mountinfo immediately before detach")
    mounted = fields[4]
    if not any(mounted == root or mounted.startswith(root + "/") for root in roots):
        continue
    if mounted in actual:
        raise SystemExit("runtime mount became stacked immediately before detach: " + mounted)
    if frozen.get(mounted) != line:
        raise SystemExit("runtime mount identity changed or became foreign immediately before detach: " + mounted)
    actual[mounted] = line
if target not in frozen or actual.get(target) != frozen[target]:
    raise SystemExit("residual mount identity changed immediately before detach: " + target)
PY
        [ "$?" -eq 0 ] || die "refusing a changed residual mount"
        note "detaching verified v3 workspace mount left by the stopped service: $target"
        if command -v fusermount3 >/dev/null 2>&1; then
            run_root fusermount3 -u -z "$target" || die "could not detach verified v3 workspace mount: $target"
        else
            die "fusermount3 is required to detach the verified v3 workspace mount"
        fi
    done <"$targets"
    if [ "$DRY_RUN" -eq 0 ]; then
        validate_mount_targets "$STORE_PATH" "$PREVIOUS_STORE_PATH" || die "$MOUNT_VALIDATION_ERROR"
    fi
}

validate_mount_targets() {
    local runtime_dir mount_target mount_targets
    MOUNT_VALIDATION_ERROR=""
    mount_targets="$(run_readonly findmnt --noheadings --raw --output TARGET)" || {
        MOUNT_VALIDATION_ERROR="could not inspect mounts before migrating runtime ownership"
        return 1
    }
    for runtime_dir in "$@"; do
        [ -n "$runtime_dir" ] || continue
        if run_readonly test -d "$runtime_dir"; then
            while IFS= read -r mount_target; do
                case "$mount_target" in
                    "$runtime_dir")
                        MOUNT_VALIDATION_ERROR="refusing ownership migration across mount $mount_target at runtime directory; unmount it and retry"
                        return 1
                        ;;
                    "$runtime_dir"/*)
                        MOUNT_VALIDATION_ERROR="refusing ownership migration across nested mount $mount_target under $runtime_dir; unmount it and retry"
                        return 1
                        ;;
                esac
            done <<<"$mount_targets"
        fi
    done
    return 0
}

validate_runtime_migration_mounts() {
    if [ "$DRY_RUN" -eq 1 ] && [ "$SERVICE_STOPPED_FOR_UPGRADE" -eq 1 ]; then return 0; fi
    validate_mount_targets "$DATA_ROOT" "$STORE_PATH" || die "$MOUNT_VALIDATION_ERROR"
}

reconcile_runtime_directories() {
    validate_runtime_migration_mounts
    if run_readonly test -d "$STORE_PATH"; then
        run_root chown -R -h -P "$TARGET_USER:$TARGET_GROUP" -- "$STORE_PATH"
    fi
}

toml_escape() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    value="${value//$'\t'/\\t}"
    printf '%s' "$value"
}

write_config() {
    local config_tmp="${WORKDIR:-${TMPDIR:-/tmp}}/scorpio.toml"
    if [ "$RETAIN_CONFIG" -eq 1 ]; then
        warn "${CONFDIR}/scorpio.toml already exists; leaving its contents unchanged"
        run_root chown "$TARGET_USER:$TARGET_GROUP" "${CONFDIR}/scorpio.toml"
        run_root chmod 0640 "${CONFDIR}/scorpio.toml"
        return 0
    fi
    if [ "$DRY_RUN" -eq 1 ]; then
        note "would write ${CONFDIR}/scorpio.toml with the supplied endpoint and paths"
        return 0
    fi
    local escaped_mst2_base_url escaped_store_path
    escaped_mst2_base_url="$(toml_escape "$MST2_BASE_URL")"
    escaped_store_path="$(toml_escape "$STORE_PATH")"
    umask 077
    cat > "$config_tmp" <<EOF
# Generated by ScorpioFS install.sh.
mst2_base_url = "$escaped_mst2_base_url"
store_path = "$escaped_store_path"
log_level = "info"
EOF
    run_root install -m 0640 -o "$TARGET_USER" -g "$TARGET_GROUP" "$config_tmp" "${CONFDIR}/scorpio.toml"
    rm -f "$config_tmp"
}

validate_installed_config() {
    [ "$DRY_RUN" -eq 0 ] || return 0
    local previous_service_user="$EXISTING_SERVICE_USER"
    if [ "$SETUP_SERVICE" -eq 1 ]; then
        EXISTING_SERVICE_USER="$SERVICE_USER"
    fi
    if ! run_scorpio_config_without_overrides "${PREFIX}/bin/scorpio" validate; then
        EXISTING_SERVICE_USER="$previous_service_user"
        die "generated or retained config is invalid: ${CONFDIR}/scorpio.toml"
    fi
    EXISTING_SERVICE_USER="$previous_service_user"
}

validate_user_allow_other() {
    [ "$ENABLE_USER_ALLOW_OTHER" -eq 1 ] && return 0
    if [ "$DRY_RUN" -eq 1 ] && [ "$(id -u)" -ne 0 ] && \
        [ -e /etc/fuse.conf ] && [ ! -r /etc/fuse.conf ]; then
        command -v sudo >/dev/null 2>&1 || \
            die "sudo is required to inspect protected /etc/fuse.conf during dry-run"
        sudo -v || die "could not obtain read access for /etc/fuse.conf during dry-run"
        SUDO_BIN="sudo"
    fi
    if run_readonly grep -qE '^[[:space:]]*user_allow_other[[:space:]]*$' /etc/fuse.conf 2>/dev/null; then
        note "using the existing user_allow_other setting in /etc/fuse.conf"
        return 0
    fi
    die "user_allow_other is required because ScorpioFS uses allow_other FUSE mounts; rerun with --enable-user-allow-other"
}

enable_user_allow_other() {
    [ "$ENABLE_USER_ALLOW_OTHER" -eq 1 ] || { note "leaving /etc/fuse.conf unchanged"; return 0; }
    if [ "$DRY_RUN" -eq 1 ]; then
        run_root sh -c "grep -qE '^[[:space:]]*user_allow_other[[:space:]]*$' /etc/fuse.conf || printf '%s\\n' user_allow_other >> /etc/fuse.conf"
        return 0
    fi
    if run_root grep -qE '^[[:space:]]*user_allow_other[[:space:]]*$' /etc/fuse.conf 2>/dev/null; then
        note "/etc/fuse.conf already enables user_allow_other"
    else
        note "enabling user_allow_other in /etc/fuse.conf"
        printf 'user_allow_other\n' | run_root tee -a /etc/fuse.conf >/dev/null
    fi
}

install_systemd_service() {
    [ "$SETUP_SERVICE" -eq 1 ] || return 0
    if [ "$DRY_RUN" -eq 1 ]; then
        note "would install /etc/systemd/system/scorpiofs.service and enable it"
        return 0
    fi
    local unit_tmp="${WORKDIR}/scorpiofs.service"
    local fuse_group_line=""
    if getent group fuse >/dev/null 2>&1; then fuse_group_line="SupplementaryGroups=fuse"; fi
    cat > "$unit_tmp" <<EOF
[Unit]
Description=ScorpioFS workspace daemon (FUSE mount + HTTP API)
Documentation=https://github.com/${REPO}
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
Type=simple
User=${SERVICE_USER}
Group=${TARGET_GROUP}
${fuse_group_line}
AmbientCapabilities=CAP_SYS_ADMIN
CapabilityBoundingSet=CAP_SYS_ADMIN
WorkingDirectory=${DATA_ROOT}
ExecStart=${PREFIX}/bin/scorpio --config-path ${CONFDIR}/scorpio.toml serve --http-addr ${HTTP_ADDR}
Restart=on-failure
RestartSec=5s
TimeoutStopSec=45
KillMode=mixed
LimitNOFILE=65536
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
    run_root install -m 0644 "$unit_tmp" /etc/systemd/system/scorpiofs.service
    run_root systemctl daemon-reload
    if ! run_root systemctl enable scorpiofs.service; then
        die "could not enable scorpiofs.service; inspect: systemctl status scorpiofs"
    fi
    local service_action="start"
    if [ "$SERVICE_STOPPED_FOR_UPGRADE" -eq 1 ]; then
        note "starting ScorpioFS after the managed upgrade"
    elif run_root systemctl is-active --quiet scorpiofs.service; then
        service_action="restart"
        note "restarting the active ScorpioFS service to load the new binary and config"
    fi
    if ! run_root systemctl "$service_action" scorpiofs.service; then
        die "systemd unit could not ${service_action}; inspect: systemctl status scorpiofs"
    fi
    wait_for_service_health
}

wait_for_service_health() {
    local endpoint attempt
    endpoint="$(health_endpoint)"
    for ((attempt = 1; attempt <= 30; attempt++)); do
        if ! run_root systemctl is-active --quiet scorpiofs.service; then
            die "scorpiofs.service is not active after starting; inspect: systemctl status scorpiofs"
        fi
        if [ "$DOWNLOADER" = "curl" ]; then
            if curl -fsS --noproxy '*' --connect-timeout 2 --max-time 5 "$endpoint" >/dev/null 2>&1 && \
                run_root systemctl is-active --quiet scorpiofs.service; then
                sleep 1
                if run_root systemctl is-active --quiet scorpiofs.service; then
                    SERVICE_HEALTH_CONFIRMED=1
                    note "scorpiofs.service is healthy at ${endpoint}"
                    return 0
                fi
            fi
        elif wget -q --no-proxy --timeout=2 --tries=1 -O /dev/null "$endpoint" && \
            run_root systemctl is-active --quiet scorpiofs.service; then
            sleep 1
            if run_root systemctl is-active --quiet scorpiofs.service; then
                SERVICE_HEALTH_CONFIRMED=1
                note "scorpiofs.service is healthy at ${endpoint}"
                return 0
            fi
        fi
        sleep 1
    done
    die "scorpiofs.service did not become healthy at ${endpoint}; inspect: systemctl status scorpiofs"
}

uninstall() {
    require_privileges
    if command -v systemctl >/dev/null 2>&1; then
        if run_readonly test -e /etc/systemd/system/scorpiofs.service || \
            run_root systemctl is-active --quiet scorpiofs.service; then
            if ! run_root systemctl disable --now scorpiofs.service 2>/dev/null; then
                if run_root systemctl is-active --quiet scorpiofs.service; then
                    die "could not stop scorpiofs.service; refusing to remove its unit or binaries"
                fi
                warn "systemd did not report a successful stop; service is inactive, continuing uninstall"
            fi
            if run_root systemctl is-active --quiet scorpiofs.service; then
                die "scorpiofs.service is still active; refusing to remove its unit or binaries"
            fi
        fi
        run_root rm -f /etc/systemd/system/scorpiofs.service
        run_root systemctl daemon-reload 2>/dev/null || true
    fi
    run_root rm -f "${PREFIX}/bin/scorpio" "${PREFIX}/bin/antares"
    note "removed ScorpioFS binaries and service; kept ${CONFDIR} and ${DATA_ROOT}"
}

main() {
    parse_args "$@"
    refuse_darwin
    if [ "$DO_UNINSTALL" -eq 1 ]; then uninstall; exit 0; fi
    apply_environment_options
    open_interactive_tty

    check_tools
    resolve_version
    normalize_version
    configure_interactively
    if [ -z "$MST2_BASE_URL" ]; then MST2_BASE_URL="http://localhost:8000"; fi
    if [ -z "$STORE_PATH" ]; then STORE_PATH="$DATA_ROOT/store"; fi
    REQUESTED_STORE_PATH="$STORE_PATH"
    set_generated_runtime_paths
    require_privileges
    validate_inputs
    validate_user_allow_other

    note "installing ScorpioFS ${VERSION} for $(detect_target)"
    pkg_install
    check_runtime_tools
    check_fuse
    prepare_release_binaries
    # A dry-run without a retained config skips archive preparation, but the
    # ownership audit still needs a private workspace for its evidence files.
    ensure_workdir
    capture_managed_runtime_mounts
    ensure_service_account
    validate_service_config_traversal
    stop_active_service_for_upgrade
    recover_managed_runtime_mounts
    backup_upgrade_artifacts
    install_release_binaries
    prepare_directories
    reconcile_runtime_directories
    write_config
    validate_installed_config
    enable_user_allow_other
    install_systemd_service

    note "installation complete"
    note "config: ${CONFDIR}/scorpio.toml"
    note "check:  ${PREFIX}/bin/scorpio --config-path ${CONFDIR}/scorpio.toml doctor"
    note "health: curl $(health_endpoint)"
    if [ "$SETUP_SERVICE" -eq 1 ]; then
        note "logs:   journalctl -u scorpiofs -f"
    else
        note "run:    cd ${DATA_ROOT} && ${PREFIX}/bin/scorpio --config-path ${CONFDIR}/scorpio.toml serve --http-addr ${HTTP_ADDR}"
    fi
}

main "$@"
