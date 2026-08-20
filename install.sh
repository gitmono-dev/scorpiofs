#!/usr/bin/env bash
#
# ScorpioFS interactive installer.
#
# With no arguments this script asks for the Mega/monorepo URLs, local paths,
# HTTP bind address, and whether to install a systemd service. It also keeps a
# non-interactive mode for automation and the original release-install flags.
#
# Interactive use (including curl | bash):
#   curl -fsSL https://raw.githubusercontent.com/gitmono-dev/scorpiofs/main/install.sh | bash
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
BASE_URL="${SCORPIO_BASE_URL:-}"
LFS_URL="${SCORPIO_LFS_URL:-}"
WORKSPACE="${SCORPIO_WORKSPACE:-}"
STORE_PATH="${SCORPIO_STORE_PATH:-}"
HTTP_ADDR="${SCORPIO_HTTP_ADDR:-127.0.0.1:2725}"
GIT_AUTHOR="${SCORPIO_GIT_AUTHOR:-MEGA}"
GIT_EMAIL="${SCORPIO_GIT_EMAIL:-admin@mega.org}"
SERVICE_USER="${SCORPIO_SERVICE_USER:-scorpiofs}"
CONFIG_FILE=""
ANTARES_UPPER_ROOT=""
ANTARES_CL_ROOT=""
ANTARES_MOUNT_ROOT=""
ANTARES_STATE_FILE=""

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
WORKSPACE_SET=0
STORE_PATH_SET=0
EXISTING_CONFIG=0
RETAIN_CONFIG=0
EXISTING_SERVICE_USER=""
SERVICE_STOPPED_FOR_MIGRATION=0
REQUESTED_WORKSPACE=""
REQUESTED_STORE_PATH=""
WORKDIR=""
SUDO_BIN=""
TARGET_USER=""
TARGET_GROUP=""
TTY_FD=""
BIND_HOST=""
BIND_PORT=""

[ -z "${SCORPIO_DATA_ROOT:-}" ] || DATA_ROOT_SET=1
[ -z "${SCORPIO_WORKSPACE:-}" ] || WORKSPACE_SET=1
[ -z "${SCORPIO_STORE_PATH:-}" ] || STORE_PATH_SET=1

cleanup() {
    if [ -n "${WORKDIR:-}" ] && [ -d "$WORKDIR" ]; then
        rm -rf "$WORKDIR"
    fi
}
trap cleanup EXIT

usage() {
    cat <<'EOF'
Usage: install.sh [options]

Interactive mode is the default. It asks for the remote URLs, local paths,
HTTP bind address, FUSE permission, and whether to install a systemd service.

Options:
  --version <vX.Y.Z>        Release tag (default: latest GitHub release).
  --release-base-url <url>  Release mirror root (default: GitHub releases).
  --prefix <dir>            Binary prefix (default: /usr/local).
  --config-dir <dir>        Config directory (default: /etc/scorpiofs).
  --data-root <dir>         Runtime/data root (default: /var/lib/scorpiofs).
  --base-url <url>          Mega/monorepo service URL.
  --lfs-url <url>           Git LFS endpoint URL.
  --workspace <dir>         FUSE workspace inside data-root.
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
  --no-user-allow-other     Do not change /etc/fuse.conf.
  -h, --help                Show this help message.

Examples:
  sudo bash install.sh
  bash install.sh --base-url https://mega.example.com --lfs-url https://mega.example.com/lfs
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

require_privileges() {
    [ "$DRY_RUN" -eq 1 ] && return 0
    if [ "$(id -u)" -eq 0 ]; then
        SUDO_BIN=""
        return 0
    fi
    command -v sudo >/dev/null 2>&1 || die "root privileges are required; install sudo or run as root"
    sudo -v || die "could not obtain sudo privileges"
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
    WORKSPACE="$(realpath -m -- "$WORKSPACE")"
    STORE_PATH="$(realpath -m -- "$STORE_PATH")"
}

validate_data_root() {
    case "$DATA_ROOT" in
        /|/bin|/boot|/dev|/etc|/home|/lib|/lib64|/media|/mnt|/opt|/proc|/root|/run|/sbin|/srv|/sys|/tmp|/usr|/usr/local|/var|/var/lib|/var/log)
            die "data-root is too broad and must be a dedicated ScorpioFS directory: $DATA_ROOT"
            ;;
    esac
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
    require_path_in_data_root workspace "$WORKSPACE"
    require_path_in_data_root store-path "$STORE_PATH"
    local runtime_file
    for runtime_file in "$DATA_ROOT/config.toml" "$DATA_ROOT/antares/state.toml"; do
        [ ! -L "$runtime_file" ] || die "runtime state file must not be a symbolic link: $runtime_file"
    done
    [ ! -L "$CONFDIR/scorpio.toml" ] || die "config file must not be a symbolic link: $CONFDIR/scorpio.toml"
    if [ -d "$DATA_ROOT" ] && [ "$EXISTING_CONFIG" -eq 0 ] && \
        [ -n "$(find "$DATA_ROOT" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
        die "refusing to change ownership of a nonempty data-root without an existing ScorpioFS config: $DATA_ROOT"
    fi
}

canonicalize_runtime_paths() {
    WORKSPACE="$(realpath -m -- "$WORKSPACE")"
    STORE_PATH="$(realpath -m -- "$STORE_PATH")"
    CONFIG_FILE="$(realpath -m -- "$CONFIG_FILE")"
    ANTARES_UPPER_ROOT="$(realpath -m -- "$ANTARES_UPPER_ROOT")"
    ANTARES_CL_ROOT="$(realpath -m -- "$ANTARES_CL_ROOT")"
    ANTARES_MOUNT_ROOT="$(realpath -m -- "$ANTARES_MOUNT_ROOT")"
    ANTARES_STATE_FILE="$(realpath -m -- "$ANTARES_STATE_FILE")"
}

validate_runtime_paths() {
    local field value i
    local -a fields=(workspace store-path config-file antares-upper-root antares-cl-root antares-mount-root antares-state-file)
    local -a values=("$WORKSPACE" "$STORE_PATH" "$CONFIG_FILE" "$ANTARES_UPPER_ROOT" "$ANTARES_CL_ROOT" "$ANTARES_MOUNT_ROOT" "$ANTARES_STATE_FILE")
    for ((i = 0; i < ${#fields[@]}; i++)); do
        validate_path "${fields[$i]}" "${values[$i]}"
    done
    canonicalize_runtime_paths
    values=("$WORKSPACE" "$STORE_PATH" "$CONFIG_FILE" "$ANTARES_UPPER_ROOT" "$ANTARES_CL_ROOT" "$ANTARES_MOUNT_ROOT" "$ANTARES_STATE_FILE")
    validate_data_root
    for ((i = 0; i < ${#fields[@]}; i++)); do
        field="${fields[$i]}"
        value="${values[$i]}"
        validate_path "$field" "$value"
        require_path_in_data_root "$field" "$value"
    done
    [ ! -L "$CONFIG_FILE" ] || die "runtime state file must not be a symbolic link: $CONFIG_FILE"
    [ ! -L "$ANTARES_STATE_FILE" ] || die "runtime state file must not be a symbolic link: $ANTARES_STATE_FILE"
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
    local mount_field mount_path persistent_field persistent_path installer_field installer_path i j
    local -a mount_fields=(workspace antares-mount-root)
    local -a mount_paths=("$WORKSPACE" "$ANTARES_MOUNT_ROOT")
    local -a persistent_fields=(store-path config-file antares-upper-root antares-cl-root antares-state-file)
    local -a persistent_paths=("$STORE_PATH" "$CONFIG_FILE" "$ANTARES_UPPER_ROOT" "$ANTARES_CL_ROOT" "$ANTARES_STATE_FILE")
    local -a installer_fields=(scorpio-binary antares-binary main-config)
    local -a installer_paths=(
        "$(realpath -m -- "${PREFIX}/bin/scorpio")"
        "$(realpath -m -- "${PREFIX}/bin/antares")"
        "$(realpath -m -- "${CONFDIR}/scorpio.toml")"
    )
    for ((i = 0; i < ${#mount_fields[@]}; i++)); do
        mount_field="${mount_fields[$i]}"
        mount_path="${mount_paths[$i]}"
        for ((j = 0; j < ${#persistent_fields[@]}; j++)); do
            persistent_field="${persistent_fields[$j]}"
            persistent_path="${persistent_paths[$j]}"
            if paths_overlap "$mount_path" "$persistent_path"; then
                die "$mount_field must not overlap $persistent_field: $mount_path and $persistent_path"
            fi
        done
        for ((j = 0; j < ${#installer_fields[@]}; j++)); do
            installer_field="${installer_fields[$j]}"
            installer_path="${installer_paths[$j]}"
            if path_is_at_or_below "$installer_path" "$mount_path"; then
                die "$mount_field must not contain $installer_field: $installer_path"
            fi
        done
    done
    if paths_overlap "$WORKSPACE" "$ANTARES_MOUNT_ROOT"; then
        die "workspace must not overlap antares-mount-root: $WORKSPACE and $ANTARES_MOUNT_ROOT"
    fi
}

normalize_runtime_paths() {
    local i
    local -a fields=(workspace store-path config-file antares-upper-root antares-cl-root antares-mount-root antares-state-file)
    local -a values=("$WORKSPACE" "$STORE_PATH" "$CONFIG_FILE" "$ANTARES_UPPER_ROOT" "$ANTARES_CL_ROOT" "$ANTARES_MOUNT_ROOT" "$ANTARES_STATE_FILE")
    for ((i = 0; i < ${#fields[@]}; i++)); do
        validate_runtime_path_value "${fields[$i]}" "${values[$i]}"
    done
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

common_path_ancestor() {
    local candidate="$1" value
    shift
    for value in "$@"; do
        while [ "$candidate" != "/" ]; do
            case "$value" in
                "$candidate"|"$candidate"/*) break ;;
                *) candidate="${candidate%/*}"; [ -n "$candidate" ] || candidate="/" ;;
            esac
        done
    done
    printf '%s' "$candidate"
}

infer_data_root() {
    local value
    local -a anchors=()
    for value in "$WORKSPACE" "$STORE_PATH" "$ANTARES_UPPER_ROOT" "$ANTARES_CL_ROOT" "$ANTARES_MOUNT_ROOT"; do
        if [[ "$value" == /* ]]; then anchors+=("$(dirname -- "$value")"); fi
    done
    for value in "$CONFIG_FILE" "$ANTARES_STATE_FILE"; do
        if [[ "$value" == /* ]]; then anchors+=("$(dirname -- "$value")"); fi
    done
    [ "${#anchors[@]}" -gt 0 ] || \
        die "cannot infer data-root from an all-relative retained config; pass --data-root"
    DATA_ROOT="$(common_path_ancestor "${anchors[@]}")"
    note "using data-root inferred from retained config: $DATA_ROOT"
}

resolve_relative_runtime_paths() {
    if [[ "$WORKSPACE" != /* ]]; then WORKSPACE="$DATA_ROOT/$WORKSPACE"; fi
    if [[ "$STORE_PATH" != /* ]]; then STORE_PATH="$DATA_ROOT/$STORE_PATH"; fi
    if [[ "$CONFIG_FILE" != /* ]]; then CONFIG_FILE="$DATA_ROOT/$CONFIG_FILE"; fi
    if [[ "$ANTARES_UPPER_ROOT" != /* ]]; then ANTARES_UPPER_ROOT="$DATA_ROOT/$ANTARES_UPPER_ROOT"; fi
    if [[ "$ANTARES_CL_ROOT" != /* ]]; then ANTARES_CL_ROOT="$DATA_ROOT/$ANTARES_CL_ROOT"; fi
    if [[ "$ANTARES_MOUNT_ROOT" != /* ]]; then ANTARES_MOUNT_ROOT="$DATA_ROOT/$ANTARES_MOUNT_ROOT"; fi
    if [[ "$ANTARES_STATE_FILE" != /* ]]; then ANTARES_STATE_FILE="$DATA_ROOT/$ANTARES_STATE_FILE"; fi
    canonicalize_runtime_paths
}

set_generated_runtime_paths() {
    if [ "$WORKSPACE_SET" -eq 1 ]; then WORKSPACE="$REQUESTED_WORKSPACE"; else WORKSPACE="$DATA_ROOT/mount"; fi
    if [ "$STORE_PATH_SET" -eq 1 ]; then STORE_PATH="$REQUESTED_STORE_PATH"; else STORE_PATH="$DATA_ROOT/store"; fi
    CONFIG_FILE="$DATA_ROOT/config.toml"
    ANTARES_UPPER_ROOT="$DATA_ROOT/antares/upper"
    ANTARES_CL_ROOT="$DATA_ROOT/antares/cl"
    ANTARES_MOUNT_ROOT="$DATA_ROOT/antares/mnt"
    ANTARES_STATE_FILE="$DATA_ROOT/antares/state.toml"
}

run_scorpio_config_without_overrides() {
    local binary="$1" variable config_path="${CONFDIR}/scorpio.toml"
    shift
    local -a command=(env)
    while IFS= read -r variable; do
        case "$variable" in
            SCORPIO_*) command+=(-u "$variable") ;;
        esac
    done < <(compgen -e)
    if [ "$DRY_RUN" -eq 1 ]; then
        if [ "$(id -u)" -eq 0 ] || [ -r "${CONFDIR}/scorpio.toml" ]; then
            "${command[@]}" "$binary" --config-path "${CONFDIR}/scorpio.toml" config "$@"
        else
            command -v sudo >/dev/null 2>&1 || \
                die "sudo is required to read protected retained config during dry-run"
            sudo -v || die "could not obtain read access for retained config during dry-run"
            config_path="${WORKDIR}/retained-config.toml"
            (umask 077; : >"$config_path") || \
                die "could not create a protected config copy for retained dry-run"
            if ! sudo cat -- "${CONFDIR}/scorpio.toml" | tee "$config_path" >/dev/null; then
                die "could not read protected retained config during dry-run: ${CONFDIR}/scorpio.toml"
            fi
            "${command[@]}" "$binary" --config-path "$config_path" config "$@"
        fi
    else
        run_root "${command[@]}" "$binary" --config-path "${CONFDIR}/scorpio.toml" config "$@"
    fi
}

load_configured_runtime_paths() {
    local binary="$1" output_file="${WORKDIR}/installer-paths"
    local -a paths=()
    if ! run_scorpio_config_without_overrides "$binary" installer-paths >"$output_file"; then
        die "could not safely resolve runtime paths from retained config: ${CONFDIR}/scorpio.toml"
    fi
    mapfile -d '' -t paths <"$output_file"
    [ "${#paths[@]}" -eq 7 ] || die "installer received an invalid runtime-path response from scorpio"
    WORKSPACE="${paths[0]}"
    STORE_PATH="${paths[1]}"
    CONFIG_FILE="${paths[2]}"
    ANTARES_UPPER_ROOT="${paths[3]}"
    ANTARES_CL_ROOT="${paths[4]}"
    ANTARES_MOUNT_ROOT="${paths[5]}"
    ANTARES_STATE_FILE="${paths[6]}"
}

prepare_effective_runtime_paths() {
    local binary="$1" selected_root_nonempty=0 load_existing_paths=0
    if [ -d "$DATA_ROOT" ] && [ -n "$(find "$DATA_ROOT" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
        selected_root_nonempty=1
    fi

    if [ "$RETAIN_CONFIG" -eq 1 ]; then
        load_existing_paths=1
    elif [ "$EXISTING_CONFIG" -eq 1 ] && \
        { [ "$DATA_ROOT_SET" -eq 0 ] || [ "$selected_root_nonempty" -eq 1 ]; }; then
        # Before overwriting a nonempty root, prove that the old config belongs
        # to it. An unspecified root is inferred from that same old config.
        load_existing_paths=1
    fi

    if [ "$load_existing_paths" -eq 1 ]; then
        load_configured_runtime_paths "$binary"
        normalize_runtime_paths
        if [ "$DATA_ROOT_SET" -eq 0 ]; then infer_data_root; fi
        resolve_relative_runtime_paths
        validate_runtime_paths
    fi

    if [ "$RETAIN_CONFIG" -eq 1 ]; then
        note "using runtime paths from retained ${CONFDIR}/scorpio.toml"
        return 0
    fi

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
}

stop_active_service_for_user_migration() {
    [ "$SETUP_SERVICE" -eq 1 ] || return 0
    [ -n "$EXISTING_SERVICE_USER" ] || return 0
    [ "$EXISTING_SERVICE_USER" != "$SERVICE_USER" ] || return 0
    if run_root systemctl is-active --quiet scorpiofs.service; then
        note "stopping the active service before migrating data from $EXISTING_SERVICE_USER to $SERVICE_USER"
        if ! run_root systemctl stop scorpiofs.service; then
            die "could not stop scorpiofs.service before changing its service user"
        fi
        SERVICE_STOPPED_FOR_MIGRATION=1
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
            --base-url) [ "$#" -ge 2 ] || die "--base-url needs a value"; BASE_URL="$2"; shift 2 ;;
            --lfs-url) [ "$#" -ge 2 ] || die "--lfs-url needs a value"; LFS_URL="$2"; shift 2 ;;
            --workspace) [ "$#" -ge 2 ] || die "--workspace needs a value"; WORKSPACE="$2"; WORKSPACE_SET=1; shift 2 ;;
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
        die "interactive input requires a controlling terminal; use --non-interactive with --base-url and --lfs-url"
    fi
    TTY_FD=3
}

detect_target() {
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
        run_root apt-get install -y --no-install-recommends fuse3 openssl ca-certificates util-linux
    elif command -v dnf >/dev/null 2>&1; then
        run_root dnf install -y fuse3 openssl ca-certificates util-linux
    elif command -v pacman >/dev/null 2>&1; then
        run_root pacman -Sy --noconfirm fuse3 openssl ca-certificates util-linux
    else
        warn "no supported package manager found; install fuse3, openssl, ca-certificates, and util-linux manually"
    fi
}

check_runtime_tools() {
    command -v findmnt >/dev/null 2>&1 || die "findmnt is required (install util-linux)"
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
    prompt_value BASE_URL "Mega/monorepo base URL" "${BASE_URL:-http://localhost:8000}"
    prompt_value LFS_URL "Git LFS URL" "${LFS_URL:-$(normalize_url "$BASE_URL")/lfs}"
    local previous_data_root="$DATA_ROOT" workspace_default="${WORKSPACE:-$DATA_ROOT/mount}"
    local store_default="${STORE_PATH:-$DATA_ROOT/store}"
    prompt_value DATA_ROOT "Data root" "$DATA_ROOT"
    [ "$DATA_ROOT" = "$previous_data_root" ] || DATA_ROOT_SET=1
    workspace_default="${WORKSPACE:-$DATA_ROOT/mount}"
    store_default="${STORE_PATH:-$DATA_ROOT/store}"
    prompt_value WORKSPACE "FUSE workspace" "$workspace_default"
    [ "$WORKSPACE" = "$workspace_default" ] || WORKSPACE_SET=1
    prompt_value STORE_PATH "Local store/cache" "$store_default"
    [ "$STORE_PATH" = "$store_default" ] || STORE_PATH_SET=1
    prompt_value HTTP_ADDR "HTTP listen address" "$HTTP_ADDR"
    prompt_value GIT_AUTHOR "Default Git author" "$GIT_AUTHOR"
    prompt_value GIT_EMAIL "Default Git email" "$GIT_EMAIL"
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

validate_inputs() {
    BASE_URL="$(normalize_url "$BASE_URL")"
    LFS_URL="$(normalize_url "$LFS_URL")"
    PREFIX="$(normalize_path "$PREFIX")"
    CONFDIR="$(normalize_path "$CONFDIR")"
    DATA_ROOT="$(normalize_path "$DATA_ROOT")"
    WORKSPACE="$(normalize_path "$WORKSPACE")"
    STORE_PATH="$(normalize_path "$STORE_PATH")"
    validate_url base_url "$BASE_URL"
    validate_url lfs_url "$LFS_URL"
    validate_url release-base-url "$RELEASE_BASE_URL"
    validate_path prefix "$PREFIX"
    validate_path config-dir "$CONFDIR"
    validate_path data-root "$DATA_ROOT"
    validate_path workspace "$WORKSPACE"
    validate_path store-path "$STORE_PATH"
    canonicalize_paths
    canonicalize_runtime_paths
    validate_path prefix "$PREFIX"
    validate_path config-dir "$CONFDIR"
    validate_path data-root "$DATA_ROOT"
    validate_path workspace "$WORKSPACE"
    validate_path store-path "$STORE_PATH"
    if [ -f "${CONFDIR}/scorpio.toml" ]; then
        EXISTING_CONFIG=1
        if [ "$OVERWRITE_CONFIG" -ne 1 ]; then RETAIN_CONFIG=1; fi
    fi
    validate_service_manager
    detect_existing_service_user
    validate_data_paths
    validate_runtime_paths
    validate_bind "$HTTP_ADDR"
    is_loopback_host "$BIND_HOST" || [ "$ALLOW_PUBLIC_API" -eq 1 ] || \
        die "refusing non-loopback HTTP bind without --allow-public-api"
    validate_toml_text "git author" "$GIT_AUTHOR"
    validate_toml_text "git email" "$GIT_EMAIL"
    [ -n "$GIT_AUTHOR" ] || die "git author must not be empty"
    [ -n "$GIT_EMAIL" ] || die "git email must not be empty"
    [[ "$SERVICE_USER" =~ ^[a-z_][a-z0-9_-]{0,31}$ ]] || die "invalid service user: $SERVICE_USER"
}

install_binaries() {
    local target tarball base url sumurl extracted installed_binary
    target="$(detect_target)"
    tarball="scorpiofs-${VERSION}-${target}.tar.gz"
    base="${RELEASE_BASE_URL%/}/${VERSION}"
    url="${base}/${tarball}"
    sumurl="${url}.sha256"

    if [ "$DRY_RUN" -eq 1 ]; then
        if [ "$EXISTING_CONFIG" -eq 1 ]; then
            installed_binary="${PREFIX}/bin/scorpio"
            [ -x "$installed_binary" ] || \
                die "dry-run cannot faithfully resolve retained config without $installed_binary"
            WORKDIR="$(mktemp -d)"
            prepare_effective_runtime_paths "$installed_binary"
        fi
        note "would download ${url}"
        note "would verify ${sumurl} with SHA256"
        note "would install ${PREFIX}/bin/scorpio and ${PREFIX}/bin/antares"
        return 0
    fi

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

    note "extracting and installing to ${PREFIX}/bin"
    tar -xzf "${WORKDIR}/${tarball}" -C "$WORKDIR"
    extracted="${WORKDIR}/scorpiofs-${VERSION}-${target}"
    if [ ! -x "${extracted}/scorpio" ] || [ ! -x "${extracted}/antares" ]; then
        die "release archive has an unexpected layout"
    fi
    local smoke_output
    if ! smoke_output="$("${extracted}/scorpio" --version 2>&1)"; then
        die "downloaded scorpio cannot run on this host: ${smoke_output}. Install a release built for this Linux distribution"
    fi
    if ! smoke_output="$("${extracted}/antares" --version 2>&1)"; then
        die "downloaded antares cannot run on this host: ${smoke_output}. Install a release built for this Linux distribution"
    fi
    prepare_effective_runtime_paths "${extracted}/scorpio"
    run_root install -d "${PREFIX}/bin"
    run_root install -m 0755 "${extracted}/scorpio" "${PREFIX}/bin/scorpio"
    run_root install -m 0755 "${extracted}/antares" "${PREFIX}/bin/antares"
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

prepare_directories() {
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
    run_root install -d -o "$TARGET_USER" -g "$TARGET_GROUP" \
        "$DATA_ROOT" "$WORKSPACE" "$STORE_PATH" \
        "$ANTARES_UPPER_ROOT" "$ANTARES_CL_ROOT" "$ANTARES_MOUNT_ROOT" \
        "$(dirname -- "$CONFIG_FILE")" "$(dirname -- "$ANTARES_STATE_FILE")"
    run_root install -d "$CONFDIR"
}

validate_runtime_migration_mounts() {
    local runtime_dir mount_target mount_targets
    mount_targets="$(findmnt --noheadings --raw --output TARGET)" || \
        die "could not inspect mounts before migrating runtime ownership"
    for runtime_dir in "$STORE_PATH" "$ANTARES_UPPER_ROOT" "$ANTARES_CL_ROOT"; do
        if [ -d "$runtime_dir" ]; then
            while IFS= read -r mount_target; do
                case "$mount_target" in
                    "$runtime_dir"/*)
                        die "refusing ownership migration across nested mount $mount_target under $runtime_dir; unmount it and retry"
                        ;;
                esac
            done <<<"$mount_targets"
        fi
    done
}

reconcile_runtime_directories() {
    local runtime_dir
    # These are persistent local data trees. Workspace and mount roots are
    # intentionally excluded because they may currently be FUSE mountpoints.
    validate_runtime_migration_mounts
    for runtime_dir in "$STORE_PATH" "$ANTARES_UPPER_ROOT" "$ANTARES_CL_ROOT"; do
        if [ -d "$runtime_dir" ]; then
            run_root chown -R -h -P "$TARGET_USER:$TARGET_GROUP" -- "$runtime_dir"
        fi
    done
}

reconcile_runtime_files() {
    local runtime_file
    for runtime_file in "$CONFIG_FILE" "$ANTARES_STATE_FILE"; do
        if [ -e "$runtime_file" ]; then
            run_root chown "$TARGET_USER:$TARGET_GROUP" "$runtime_file"
        fi
    done
}

toml_escape() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    value="${value//$'\t'/\\t}"
    printf '%s' "$value"
}

validate_toml_text() {
    local field="$1" value_without_tabs="${2//$'\t'/}"
    local LC_ALL=C
    if [[ "$value_without_tabs" =~ [[:cntrl:]] ]]; then
        die "$field must not contain control characters other than tab"
    fi
}

write_config() {
    local config_tmp="${WORKDIR:-${TMPDIR:-/tmp}}/scorpio.toml"
    if [ -f "${CONFDIR}/scorpio.toml" ] && [ "$OVERWRITE_CONFIG" -ne 1 ]; then
        warn "${CONFDIR}/scorpio.toml already exists; leaving its contents unchanged"
        run_root chown "$TARGET_USER:$TARGET_GROUP" "${CONFDIR}/scorpio.toml"
        run_root chmod 0640 "${CONFDIR}/scorpio.toml"
        return 0
    fi
    if [ "$DRY_RUN" -eq 1 ]; then
        note "would write ${CONFDIR}/scorpio.toml with the supplied URLs and paths"
        return 0
    fi
    local escaped_base_url escaped_lfs_url escaped_workspace escaped_store_path
    local escaped_config_file escaped_author escaped_email
    local escaped_antares_upper escaped_antares_cl escaped_antares_mount escaped_antares_state
    escaped_base_url="$(toml_escape "$BASE_URL")"
    escaped_lfs_url="$(toml_escape "$LFS_URL")"
    escaped_workspace="$(toml_escape "$WORKSPACE")"
    escaped_store_path="$(toml_escape "$STORE_PATH")"
    escaped_config_file="$(toml_escape "$CONFIG_FILE")"
    escaped_author="$(toml_escape "$GIT_AUTHOR")"
    escaped_email="$(toml_escape "$GIT_EMAIL")"
    escaped_antares_upper="$(toml_escape "$ANTARES_UPPER_ROOT")"
    escaped_antares_cl="$(toml_escape "$ANTARES_CL_ROOT")"
    escaped_antares_mount="$(toml_escape "$ANTARES_MOUNT_ROOT")"
    escaped_antares_state="$(toml_escape "$ANTARES_STATE_FILE")"
    umask 077
    cat > "$config_tmp" <<EOF
# Generated by ScorpioFS install.sh. Edit base_url/lfs_url when the backend changes.
base_url = "$escaped_base_url"
lfs_url = "$escaped_lfs_url"
workspace = "$escaped_workspace"
store_path = "$escaped_store_path"
config_file = "$escaped_config_file"
git_author = "$escaped_author"
git_email = "$escaped_email"
log_level = "info"
antares_upper_root = "$escaped_antares_upper"
antares_cl_root = "$escaped_antares_cl"
antares_mount_root = "$escaped_antares_mount"
antares_state_file = "$escaped_antares_state"
EOF
    run_root install -m 0640 -o "$TARGET_USER" -g "$TARGET_GROUP" "$config_tmp" "${CONFDIR}/scorpio.toml"
    rm -f "$config_tmp"
}

validate_installed_config() {
    [ "$DRY_RUN" -eq 0 ] || return 0
    if ! run_scorpio_config_without_overrides "${PREFIX}/bin/scorpio" validate; then
        die "generated or retained config is invalid: ${CONFDIR}/scorpio.toml"
    fi
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
ExecStopPost=-/bin/sh -c 'for m in \$(findmnt -rno TARGET --submounts ${ANTARES_MOUNT_ROOT} 2>/dev/null | sort -r); do fusermount3 -u -z "\$m"; done'
ExecStopPost=-/usr/bin/fusermount3 -u -z ${WORKSPACE}
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
    if [ "$SERVICE_STOPPED_FOR_MIGRATION" -eq 1 ]; then
        note "starting ScorpioFS with the new service user"
    elif run_root systemctl is-active --quiet scorpiofs.service; then
        service_action="restart"
        note "restarting the active ScorpioFS service to load the new binary and config"
    fi
    if ! run_root systemctl "$service_action" scorpiofs.service; then
        die "systemd unit could not ${service_action}; inspect: systemctl status scorpiofs"
    fi
}

uninstall() {
    require_privileges
    if command -v systemctl >/dev/null 2>&1; then
        run_root systemctl disable --now scorpiofs.service 2>/dev/null || true
        run_root rm -f /etc/systemd/system/scorpiofs.service
        run_root systemctl daemon-reload 2>/dev/null || true
    fi
    run_root rm -f "${PREFIX}/bin/scorpio" "${PREFIX}/bin/antares"
    note "removed ScorpioFS binaries and service; kept ${CONFDIR} and ${DATA_ROOT}"
}

main() {
    parse_args "$@"
    if [ "$DO_UNINSTALL" -eq 1 ]; then uninstall; exit 0; fi
    apply_environment_options
    open_interactive_tty

    check_tools
    resolve_version
    normalize_version
    configure_interactively
    if [ -z "$BASE_URL" ]; then BASE_URL="http://localhost:8000"; fi
    if [ -z "$LFS_URL" ]; then LFS_URL="$(normalize_url "$BASE_URL")/lfs"; fi
    if [ -z "$WORKSPACE" ]; then WORKSPACE="$DATA_ROOT/mount"; fi
    if [ -z "$STORE_PATH" ]; then STORE_PATH="$DATA_ROOT/store"; fi
    REQUESTED_WORKSPACE="$WORKSPACE"
    REQUESTED_STORE_PATH="$STORE_PATH"
    set_generated_runtime_paths
    validate_inputs
    require_privileges

    note "installing ScorpioFS ${VERSION} for $(detect_target)"
    pkg_install
    check_runtime_tools
    check_fuse
    install_binaries
    validate_runtime_migration_mounts
    ensure_service_account
    stop_active_service_for_user_migration
    prepare_directories
    reconcile_runtime_directories
    reconcile_runtime_files
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
