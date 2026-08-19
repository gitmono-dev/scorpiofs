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

DRY_RUN=0
DO_UNINSTALL=0
INSTALL_DEPS=1
ENABLE_USER_ALLOW_OTHER=0
SETUP_SERVICE=1
INTERACTIVE=1
ASSUME_YES=0
ALLOW_PUBLIC_API=0
OVERWRITE_CONFIG=0
WORKDIR=""
SUDO_BIN=""
TARGET_USER=""
TARGET_GROUP=""

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
  --prefix <dir>            Binary prefix (default: /usr/local).
  --config-dir <dir>        Config directory (default: /etc/scorpiofs).
  --data-root <dir>         Runtime/data root (default: /var/lib/scorpiofs).
  --base-url <url>          Mega/monorepo service URL.
  --lfs-url <url>           Git LFS endpoint URL.
  --workspace <dir>         FUSE workspace directory.
  --store-path <dir>        Local cache/store directory.
  --http-addr <ip:port>     HTTP API bind address (default: 127.0.0.1:2725).
  --allow-public-api        Permit a non-loopback HTTP bind (use a firewall/auth proxy).
  --no-service              Do not create or start a systemd service.
  --non-interactive         Use arguments/environment without prompting.
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
  bash install.sh --version v0.4.0 --non-interactive --dry-run

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
    if [ -r /dev/tty ]; then
        IFS= read -r REPLY </dev/tty || REPLY=""
    else
        IFS= read -r REPLY || REPLY=""
    fi
}

prompt_value() {
    local variable="$1" label="$2" default="$3"
    if [ "$ASSUME_YES" -eq 1 ]; then
        printf -v "$variable" '%s' "$default"
        return 0
    fi
    printf '%s [%s]: ' "$label" "$default" >/dev/tty 2>/dev/null || printf '%s [%s]: ' "$label" "$default"
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
        printf '%s [%s]: ' "$label" "$default" >/dev/tty 2>/dev/null || printf '%s [%s]: ' "$label" "$default"
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
    [[ "$value" != *'"'* ]] || die "$field must not contain a double quote"
    [[ "$value" != *"'"* ]] || die "$field must not contain a single quote"
    [[ "$value" != *'`'* && "$value" != *'\\'* ]] || die "$field contains an unsafe character"
}

validate_path() {
    local field="$1" value="$2"
    [ -n "$value" ] || die "$field must not be empty"
    [[ "$value" == /* ]] || die "$field must be an absolute path: $value"
    [[ "$value" != *[[:space:]]* ]] || die "$field must not contain whitespace"
    [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] || die "$field must not contain a newline"
    [[ "$value" != *'"'* && "$value" != *'`'* && "$value" != *'$'* && "$value" != *'\\'* ]] || die "$field contains an unsafe shell character"
}

validate_bind() {
    local value="$1"
    [[ "$value" =~ ^(127\.0\.0\.1|0\.0\.0\.0|::1|\[::1\]):[0-9]{1,5}$ ]] || \
        die "--http-addr must be an IPv4/IPv6 loopback or wildcard address such as 127.0.0.1:2725"
}

normalize_url() {
    local value="$1"
    while [ "${value%/}" != "$value" ]; do value="${value%/}"; done
    printf '%s' "$value"
}

parse_args() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --version) [ "$#" -ge 2 ] || die "--version needs a value"; VERSION="$2"; shift 2 ;;
            --prefix) [ "$#" -ge 2 ] || die "--prefix needs a value"; PREFIX="$2"; shift 2 ;;
            --config-dir) [ "$#" -ge 2 ] || die "--config-dir needs a value"; CONFDIR="$2"; shift 2 ;;
            --data-root) [ "$#" -ge 2 ] || die "--data-root needs a value"; DATA_ROOT="$2"; shift 2 ;;
            --base-url) [ "$#" -ge 2 ] || die "--base-url needs a value"; BASE_URL="$2"; shift 2 ;;
            --lfs-url) [ "$#" -ge 2 ] || die "--lfs-url needs a value"; LFS_URL="$2"; shift 2 ;;
            --workspace) [ "$#" -ge 2 ] || die "--workspace needs a value"; WORKSPACE="$2"; shift 2 ;;
            --store-path) [ "$#" -ge 2 ] || die "--store-path needs a value"; STORE_PATH="$2"; shift 2 ;;
            --http-addr) [ "$#" -ge 2 ] || die "--http-addr needs a value"; HTTP_ADDR="$2"; shift 2 ;;
            --allow-public-api) ALLOW_PUBLIC_API=1; shift ;;
            --no-service) SETUP_SERVICE=0; shift ;;
            --non-interactive) INTERACTIVE=0; shift ;;
            --yes) ASSUME_YES=1; shift ;;
            --dry-run) DRY_RUN=1; shift ;;
            --uninstall) DO_UNINSTALL=1; INTERACTIVE=0; shift ;;
            --no-deps) INSTALL_DEPS=0; shift ;;
            --enable-user-allow-other) ENABLE_USER_ALLOW_OTHER=1; shift ;;
            --no-user-allow-other) ENABLE_USER_ALLOW_OTHER=0; shift ;;
            -h|--help) usage; exit 0 ;;
            *) die "unknown option: $1 (see --help)" ;;
        esac
    done
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
        run_root apt-get install -y --no-install-recommends fuse3 openssl ca-certificates
    elif command -v dnf >/dev/null 2>&1; then
        run_root dnf install -y fuse3 openssl ca-certificates
    elif command -v pacman >/dev/null 2>&1; then
        run_root pacman -Sy --noconfirm fuse3 openssl ca-certificates
    else
        warn "no supported package manager found; install fuse3, openssl, and ca-certificates manually"
    fi
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
    prompt_value DATA_ROOT "Data root" "$DATA_ROOT"
    prompt_value WORKSPACE "FUSE workspace" "${WORKSPACE:-$DATA_ROOT/mount}"
    prompt_value STORE_PATH "Local store/cache" "${STORE_PATH:-$DATA_ROOT/store}"
    prompt_value HTTP_ADDR "HTTP listen address" "$HTTP_ADDR"
    prompt_value GIT_AUTHOR "Default Git author" "$GIT_AUTHOR"
    prompt_value GIT_EMAIL "Default Git email" "$GIT_EMAIL"
    prompt_yes_no ENABLE_USER_ALLOW_OTHER "Enable user_allow_other in /etc/fuse.conf" "y"
    prompt_yes_no SETUP_SERVICE "Install and start systemd service" "y"
    if [ "$SETUP_SERVICE" -eq 1 ]; then
        prompt_value SERVICE_USER "systemd service user" "$SERVICE_USER"
    fi
    if [ -f "${CONFDIR}/scorpio.toml" ]; then
        prompt_yes_no OVERWRITE_CONFIG "Overwrite existing ${CONFDIR}/scorpio.toml" "n"
    fi

    case "$HTTP_ADDR" in
        127.0.0.1:*|::1:*|\[::1\]:*) ;;
        *)
            warn "${HTTP_ADDR} is not loopback. ScorpioFS has no HTTP authentication."
            prompt_yes_no PUBLIC_API_OK "Continue with an externally reachable API only behind a firewall/auth proxy" "n"
            if [ "$PUBLIC_API_OK" -eq 1 ]; then ALLOW_PUBLIC_API=1; else HTTP_ADDR="127.0.0.1:2725"; fi
            ;;
    esac
}

validate_inputs() {
    BASE_URL="$(normalize_url "$BASE_URL")"
    LFS_URL="$(normalize_url "$LFS_URL")"
    validate_url base_url "$BASE_URL"
    validate_url lfs_url "$LFS_URL"
    validate_path prefix "$PREFIX"
    validate_path config-dir "$CONFDIR"
    validate_path data-root "$DATA_ROOT"
    validate_path workspace "$WORKSPACE"
    validate_path store-path "$STORE_PATH"
    validate_bind "$HTTP_ADDR"
    case "$HTTP_ADDR" in
        127.0.0.1:*|::1:*|\[::1\]:*) ;;
        *) [ "$ALLOW_PUBLIC_API" -eq 1 ] || die "refusing non-loopback HTTP bind without --allow-public-api" ;;
    esac
    [[ "$GIT_AUTHOR" != *$'\n'* && "$GIT_AUTHOR" != *'"'* ]] || die "git author contains an unsafe character"
    [[ "$GIT_EMAIL" != *$'\n'* && "$GIT_EMAIL" != *'"'* ]] || die "git email contains an unsafe character"
    [[ "$SERVICE_USER" =~ ^[a-z_][a-z0-9_-]{0,31}$ ]] || die "invalid service user: $SERVICE_USER"
}

install_binaries() {
    local target tarball base url sumurl extracted
    target="$(detect_target)"
    tarball="scorpiofs-${VERSION}-${target}.tar.gz"
    base="https://github.com/${REPO}/releases/download/${VERSION}"
    url="${base}/${tarball}"
    sumurl="${url}.sha256"

    if [ "$DRY_RUN" -eq 1 ]; then
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
    [ -x "${extracted}/scorpio" ] && [ -x "${extracted}/antares" ] || die "release archive has an unexpected layout"
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
        TARGET_USER="${SUDO_USER:-$(id -un)}"
        TARGET_GROUP="$(id -gn "$TARGET_USER" 2>/dev/null || id -gn)"
    fi
    run_root install -d -o "$TARGET_USER" -g "$TARGET_GROUP" \
        "$DATA_ROOT" "$WORKSPACE" "$STORE_PATH" \
        "$DATA_ROOT/antares" "$DATA_ROOT/antares/upper" \
        "$DATA_ROOT/antares/cl" "$DATA_ROOT/antares/mnt"
    run_root install -d "$CONFDIR"
}

write_config() {
    local config_tmp="${WORKDIR:-${TMPDIR:-/tmp}}/scorpio.toml"
    if [ "$DRY_RUN" -eq 1 ]; then
        note "would write ${CONFDIR}/scorpio.toml with the supplied URLs and paths"
        return 0
    fi
    umask 077
    cat > "$config_tmp" <<EOF
# Generated by ScorpioFS install.sh. Edit base_url/lfs_url when the backend changes.
base_url = "$BASE_URL"
lfs_url = "$LFS_URL"
workspace = "$WORKSPACE"
store_path = "$STORE_PATH"
config_file = "$DATA_ROOT/config.toml"
git_author = "$GIT_AUTHOR"
git_email = "$GIT_EMAIL"
log_level = "info"
antares_upper_root = "$DATA_ROOT/antares/upper"
antares_cl_root = "$DATA_ROOT/antares/cl"
antares_mount_root = "$DATA_ROOT/antares/mnt"
antares_state_file = "$DATA_ROOT/antares/state.toml"
EOF
    if [ -f "${CONFDIR}/scorpio.toml" ] && [ "$OVERWRITE_CONFIG" -ne 1 ]; then
        warn "${CONFDIR}/scorpio.toml already exists; leaving it unchanged"
        rm -f "$config_tmp"
        return 0
    fi
    run_root install -m 0640 -o "$TARGET_USER" -g "$TARGET_GROUP" "$config_tmp" "${CONFDIR}/scorpio.toml"
    rm -f "$config_tmp"
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
    if ! command -v systemctl >/dev/null 2>&1; then
        warn "systemctl is unavailable; binaries and config were installed without a service"
        return 0
    fi
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
    if ! run_root systemctl enable --now scorpiofs.service; then
        warn "systemd unit installed but could not start; inspect: systemctl status scorpiofs"
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
    if [ "$INTERACTIVE" -eq 1 ] && [ ! -e /dev/tty ] && [ "$ASSUME_YES" -eq 0 ]; then
        die "interactive input requires a terminal; use --non-interactive with --base-url and --lfs-url"
    fi

    check_tools
    resolve_version
    normalize_version
    configure_interactively
    if [ -z "$BASE_URL" ]; then BASE_URL="http://localhost:8000"; fi
    if [ -z "$LFS_URL" ]; then LFS_URL="$(normalize_url "$BASE_URL")/lfs"; fi
    if [ -z "$WORKSPACE" ]; then WORKSPACE="$DATA_ROOT/mount"; fi
    if [ -z "$STORE_PATH" ]; then STORE_PATH="$DATA_ROOT/store"; fi
    validate_inputs
    require_privileges

    note "installing ScorpioFS ${VERSION} for $(detect_target)"
    pkg_install
    check_fuse
    install_binaries
    ensure_service_account
    prepare_directories
    write_config
    enable_user_allow_other
    install_systemd_service

    note "installation complete"
    note "config: ${CONFDIR}/scorpio.toml"
    note "check:  ${PREFIX}/bin/scorpio --config-path ${CONFDIR}/scorpio.toml doctor"
    note "health: curl http://127.0.0.1:${HTTP_ADDR##*:}/health"
    if [ "$SETUP_SERVICE" -eq 1 ]; then
        note "logs:   journalctl -u scorpiofs -f"
    else
        note "run:    ${PREFIX}/bin/scorpio --config-path ${CONFDIR}/scorpio.toml serve --http-addr ${HTTP_ADDR}"
    fi
}

main "$@"
