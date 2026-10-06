use std::ffi::{OsStr, OsString};

use asyncfuse::{
    raw::{Filesystem, MountHandle, Session},
    MountOptions,
};

/// Compatibility name used by existing Antares filesystem consumers.
pub async fn mount_filesystem_with_antares_cache<
    F: Filesystem + std::marker::Sync + Send + 'static,
>(
    fs: F,
    mountpoint: &OsStr,
    enable_antares_cache: bool,
) -> std::io::Result<MountHandle> {
    mount_filesystem_with_writeback_cache(fs, mountpoint, enable_antares_cache).await
}

fn apply_writeback_mount_options(options: &mut MountOptions) {
    // Enable write-back cache for better write performance.
    // This negotiates FUSE_WRITEBACK_CACHE flag during FUSE_INIT.
    //
    // NOTE: Caching timeouts (entry_timeout, attr_timeout, etc.) are NOT
    // configurable via mount options in Linux kernel FUSE. They must be
    // set in the filesystem implementation's ReplyEntry/ReplyAttr TTL fields.
    options.write_back(true);
}

#[allow(unused)]
pub async fn mount_filesystem<F: Filesystem + std::marker::Sync + Send + 'static>(
    fs: F,
    mountpoint: &OsStr,
) -> std::io::Result<MountHandle> {
    mount_filesystem_with_writeback_cache(fs, mountpoint, false).await
}

/// Make `path` ready to serve as a FUSE mountpoint, or explain why it cannot.
///
/// Creates the directory if it is missing, then requires it to be a directory and to
/// be **empty**. An unreadable directory counts as non-empty: mounting over contents
/// that cannot even be enumerated is not a recoverable mistake.
///
/// Split out of [`mount_filesystem_with_writeback_cache`] so that an operation which
/// creates a workspace *before* mounting it can run the identical
/// check up front and fail without leaving a half-created worktree behind.
pub fn prepare_mountpoint(path: &std::path::Path) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};

    if !path.exists() {
        std::fs::create_dir_all(path).map_err(|e| {
            Error::new(
                e.kind(),
                format!("failed to create mountpoint {}: {e}", path.display()),
            )
        })?;
    }
    if !path.exists() {
        return Err(Error::new(
            ErrorKind::NotFound,
            format!("mountpoint does not exist: {}", path.display()),
        ));
    }
    if !path.is_dir() {
        return Err(Error::new(
            ErrorKind::NotADirectory,
            format!("mountpoint is not a directory: {}", path.display()),
        ));
    }
    let has_entries = std::fs::read_dir(path)
        .map(|mut it| it.next().is_some())
        .unwrap_or(true);
    if has_entries {
        return Err(Error::other(format!(
            "mountpoint is not empty or is inaccessible: {}",
            path.display()
        )));
    }
    Ok(())
}

#[allow(unused)]
pub async fn mount_filesystem_with_writeback_cache<
    F: Filesystem + std::marker::Sync + Send + 'static,
>(
    fs: F,
    mountpoint: &OsStr,
    enable_writeback_cache: bool,
) -> std::io::Result<MountHandle> {
    use std::io::{Error, ErrorKind};

    // This library function does not install a logger. The scorpio
    // binaries call `util::logging::init` once at startup, which installs the
    // tracing subscriber and the `log` -> `tracing` bridge; a library consumer
    // that wants `log::` records captured must initialize tracing itself.
    //let logfs = LoggingFileSystem::new(fs);

    let mount_path: OsString = OsString::from(mountpoint);
    let path = std::path::Path::new(&mount_path);
    prepare_mountpoint(path)?;
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let mut mount_options = MountOptions::default();
    // The kernel source label supports installer checks against a live owner's
    // workspace observations. The label alone never establishes ownership.
    mount_options.uid(uid).gid(gid).fs_name("scorpiofs-v3");
    // allow_other / force_readdir_plus are Linux FUSE concepts. macFUSE does
    // not advertise READDIRPLUS, and same-user Finder/Terminal access does not
    // need allow_other.
    #[cfg(target_os = "linux")]
    mount_options.allow_other(true).force_readdir_plus(true);
    if enable_writeback_cache {
        apply_writeback_mount_options(&mut mount_options);
    }

    tracing::debug!("about to mount FUSE filesystem at: {:?}", mount_path);
    let session = Session::<F>::new(mount_options);
    // Linux's direct mount syscall requires CAP_SYS_ADMIN. Ordinary users
    // must let the FUSE helper create the connection and own its unmount path.
    #[cfg(target_os = "linux")]
    let mounted = if unsafe { libc::geteuid() } == 0 {
        session.mount(fs, mount_path).await
    } else {
        session.mount_with_unprivileged(fs, mount_path).await
    };
    #[cfg(not(target_os = "linux"))]
    let mounted = session.mount(fs, mount_path).await;
    mounted.map_err(|e| {
        tracing::error!(
            "FUSE mount failed at {:?}: {:?} (os error code: {:?})",
            mountpoint,
            e,
            e.raw_os_error()
        );
        e
    })
}
