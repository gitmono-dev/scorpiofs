#!/usr/bin/env bash
#
# ScorpioFS installer.
#
# Downloads a release binary tarball (scorpio + antares), verifies its SHA256
# checksum, installs the binaries, and generates a baseline config. System
# package installation and any /etc/fuse.conf change are explicit, never silent.
#
# Quick (convenience) path:
#   curl -fsSL https://raw.githubusercontent.com/web3infra-foundation/scorpiofs/main/install.sh | bash -s -- --version v0.3.0
#
# Safer path (download, inspect, then run):
#   curl -fsSLO https://raw.githubusercontent.com/web3infra-foundation/scorpiofs/main/install.sh
#   less install.sh
#   bash install.sh --version v0.3.0
#
set -euo pipefail

REPO="web3infra-foundation/scorpiofs"
VERSION=""
PREFIX="/usr/local"
CONFDIR="/etc/scorpiofs"
DRY_RUN=0
DO_UNINSTALL=0
INSTALL_DEPS=1
ENABLE_USER_ALLOW_OTHER=0
WORKDIR=""

# Clean up the download workdir on exit. Guarded so it is a no-op (returning 0)
# when nothing was created — otherwise a failed test would leak into the script's
# exit status — and it never removes anything in --dry-run.
cleanup() {
    if [ "$DRY_RUN" -eq 0 ] && [ -n "${WORKDIR:-}" ]; then
        rm -rf "$WORKDIR"
    fi
    return 0
}
trap cleanup EXIT

usage() {
    cat <<'EOF'
Usage: install.sh [options]

Options:
  --version <vX.Y.Z>        Release tag to install (required unless --uninstall).
  --prefix <dir>            Install prefix (default: /usr/local; binaries go to <prefix>/bin).
  --dry-run                 Print every action without making changes.
  --uninstall               Remove installed binaries (keeps config and data).
  --no-deps                 Skip system package installation.
  --enable-user-allow-other Append 'user_allow_other' to /etc/fuse.conf (off by default).
  -h, --help                Show this help.

Exit codes: 0 success; non-zero on error (dependency, download, or checksum failure).
EOF
}

note() { printf '==> %s\n' "$*"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# run <cmd...> : execute, or just print in dry-run mode.
run() {
    if [ "$DRY_RUN" -eq 1 ]; then
        printf '  [dry-run] %s\n' "$*"
    else
        "$@"
    fi
}

require_root() {
    if [ "$(id -u)" -ne 0 ] && [ "$DRY_RUN" -eq 0 ]; then
        die "this step needs root; re-run with sudo (or use --dry-run to preview)"
    fi
}

detect_target() {
    local arch
    arch="$(uname -m)"
    case "$arch" in
        x86_64|amd64)  echo "x86_64-unknown-linux-gnu" ;;
        aarch64|arm64) echo "aarch64-unknown-linux-musl" ;;
        *) die "unsupported architecture: $arch" ;;
    esac
}

pkg_install() {
    # Install runtime dependencies via the detected package manager. We install
    # the `openssl` package (rather than a version-specific `libssl3` / `libssl3t64`)
    # so the correct OpenSSL runtime is pulled regardless of the distro release.
    local pkgs_apt="fuse3 openssl ca-certificates"
    local pkgs_dnf="fuse3 openssl ca-certificates"
    local pkgs_pacman="fuse3 openssl ca-certificates"

    if command -v apt-get >/dev/null 2>&1; then
        require_root
        run apt-get update
        run apt-get install -y --no-install-recommends $pkgs_apt
    elif command -v dnf >/dev/null 2>&1; then
        require_root
        run dnf install -y $pkgs_dnf
    elif command -v pacman >/dev/null 2>&1; then
        require_root
        run pacman -Sy --noconfirm $pkgs_pacman
    else
        warn "no supported package manager (apt/dnf/pacman) found; install fuse3 + openssl + ca-certificates manually"
    fi
}

maybe_enable_user_allow_other() {
    [ "$ENABLE_USER_ALLOW_OTHER" -eq 1 ] || return 0
    require_root
    if [ -f /etc/fuse.conf ] && grep -qE '^[[:space:]]*user_allow_other[[:space:]]*$' /etc/fuse.conf; then
        note "/etc/fuse.conf already has user_allow_other"
        return 0
    fi
    note "enabling user_allow_other in /etc/fuse.conf (explicitly requested)"
    if [ "$DRY_RUN" -eq 1 ]; then
        printf "  [dry-run] echo 'user_allow_other' >> /etc/fuse.conf\n"
    else
        printf 'user_allow_other\n' >> /etc/fuse.conf
    fi
}

fetch() {
    # fetch <url> <dest>
    local url="$1" dest="$2"
    if command -v curl >/dev/null 2>&1; then
        run curl -fsSL "$url" -o "$dest"
    elif command -v wget >/dev/null 2>&1; then
        run wget -qO "$dest" "$url"
    else
        die "need curl or wget to download release assets"
    fi
}

install_binaries() {
    local target tarball base url sumurl tmp
    target="$(detect_target)"
    tarball="scorpiofs-${VERSION}-${target}.tar.gz"
    base="https://github.com/${REPO}/releases/download/${VERSION}"
    url="${base}/${tarball}"
    sumurl="${url}.sha256"

    # In --dry-run use a synthetic path so we make no filesystem changes at all.
    if [ "$DRY_RUN" -eq 1 ]; then
        WORKDIR="/tmp/scorpiofs-install.dryrun"
    else
        WORKDIR="$(mktemp -d)"
    fi
    tmp="$WORKDIR"

    note "downloading ${tarball}"
    fetch "$url" "${tmp}/${tarball}"
    note "downloading checksum"
    fetch "$sumurl" "${tmp}/${tarball}.sha256"

    note "verifying SHA256 checksum"
    if [ "$DRY_RUN" -eq 1 ]; then
        printf '  [dry-run] sha256sum -c %s\n' "${tarball}.sha256"
    else
        ( cd "$tmp" && sha256sum -c "${tarball}.sha256" ) \
            || die "checksum verification failed — refusing to install"
    fi

    note "extracting and installing to ${PREFIX}/bin"
    run tar -xzf "${tmp}/${tarball}" -C "$tmp"
    run install -d "${PREFIX}/bin"
    # Tarball layout: scorpiofs-<version>-<target>/{scorpio,antares,LICENSE,README}
    run install -m 0755 "${tmp}/scorpiofs-${VERSION}-${target}/scorpio" "${PREFIX}/bin/scorpio"
    run install -m 0755 "${tmp}/scorpiofs-${VERSION}-${target}/antares" "${PREFIX}/bin/antares"
}

generate_config() {
    note "ensuring config at ${CONFDIR}/scorpio.toml"
    run install -d "$CONFDIR"
    if [ -f "${CONFDIR}/scorpio.toml" ]; then
        note "config already exists; leaving it unchanged"
        return 0
    fi
    if [ "$DRY_RUN" -eq 1 ]; then
        printf '  [dry-run] write generic %s/scorpio.toml\n' "$CONFDIR"
        return 0
    fi
    cat > "${CONFDIR}/scorpio.toml" <<'EOF'
# Generated by install.sh. Set base_url/lfs_url to your mega server.
base_url = "http://localhost:8000"
lfs_url = "http://localhost:8000/lfs"
workspace = "/var/lib/scorpiofs/mount"
store_path = "/var/lib/scorpiofs/store"
config_file = "/var/lib/scorpiofs/config.toml"
git_author = "MEGA"
git_email = "admin@mega.org"
log_level = "info"
antares_upper_root = "/var/lib/scorpiofs/antares/upper"
antares_cl_root = "/var/lib/scorpiofs/antares/cl"
antares_mount_root = "/var/lib/scorpiofs/antares/mnt"
antares_state_file = "/var/lib/scorpiofs/antares/state.toml"
EOF
}

uninstall() {
    note "removing binaries from ${PREFIX}/bin (config and data are kept)"
    require_root
    run rm -f "${PREFIX}/bin/scorpio" "${PREFIX}/bin/antares"
    note "to remove config/data, delete ${CONFDIR} and /var/lib/scorpiofs manually"
}

main() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --version) VERSION="${2:?--version needs a value}"; shift 2 ;;
            --prefix)  PREFIX="${2:?--prefix needs a value}"; shift 2 ;;
            --dry-run) DRY_RUN=1; shift ;;
            --uninstall) DO_UNINSTALL=1; shift ;;
            --no-deps) INSTALL_DEPS=0; shift ;;
            --enable-user-allow-other) ENABLE_USER_ALLOW_OTHER=1; shift ;;
            -h|--help) usage; exit 0 ;;
            *) die "unknown option: $1 (see --help)" ;;
        esac
    done

    if [ "$DRY_RUN" -eq 1 ]; then
        note "dry-run: no changes will be made"
    fi

    if [ "$DO_UNINSTALL" -eq 1 ]; then
        uninstall
        note "uninstall complete"
        exit 0
    fi

    [ -n "$VERSION" ] || die "--version is required (e.g. --version v0.3.0); see --help"

    if [ "$INSTALL_DEPS" -eq 1 ]; then
        pkg_install
    else
        note "skipping system dependency installation (--no-deps)"
    fi

    install_binaries
    generate_config
    maybe_enable_user_allow_other

    note "done. Edit ${CONFDIR}/scorpio.toml (set base_url/lfs_url), then run:"
    note "  scorpio --config-path ${CONFDIR}/scorpio.toml doctor"
    note "  scorpio --config-path ${CONFDIR}/scorpio.toml serve"
}

main "$@"
