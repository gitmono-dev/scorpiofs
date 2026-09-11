// use std::{path::Path, sync::Arc, thread::JoinHandle};

// use fuse_backend_rs::{api::{filesystem::FileSystem, server::Server}, transport::{FuseChannel, FuseSession}};
// #[allow(unused)]
// pub struct FuseServer<T: FileSystem + Send + Sync> {
//     pub server: Arc<Server<T>>,
//     pub ch: FuseChannel,
// }
// pub fn run<T: FileSystem + Send + Sync+ 'static>(fuse:Arc<T>,path:&str )->JoinHandle<Result<(), std::io::Error>>{
//     let mut se = FuseSession::new(Path::new(path), "dic", "", false).unwrap();
//     se.mount().unwrap();
//     let ch: FuseChannel = se.new_channel().unwrap();
//     let server = Arc::new(Server::new(fuse));
//     let mut fuse_server = FuseServer { server, ch };
//     // Spawn server thread
//     std::thread::spawn( move || {
//         fuse_server.svc_loop()
//     })

// }
// #[allow(unused)]
// impl <FS:FileSystem+ Send + Sync>FuseServer<FS> {
//     pub fn svc_loop(&mut self) -> Result<(), std::io::Error> {
//         let _ebadf = std::io::Error::from_raw_os_error(libc::EBADF);
//         println!("entering server loop");
//         loop {
//             if let Some((reader, writer)) = self
//                 .ch
//                 .get_request()
//                 .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?
//             {
//                 if let Err(e) = self
//                     .server
//                     .handle_message(reader, writer.into(), None, None)
//                 {
//                     match e {
//                         fuse_backend_rs::Error::EncodeMessage(_ebadf) => {
//                             break;
//                         }
//                         _ => {
//                             print!("Handling fuse message failed");
//                             continue;
//                         }
//                     }
//                 }
//             } else {
//                 print!("fuse server exits");
//                 break;
//             }
//         }
//         Ok(())
//     }
// }

use std::{
    env,
    ffi::{OsStr, OsString},
    io,
    path::Path,
    sync::OnceLock,
};

use asyncfuse::{
    raw::{Filesystem, MountHandle, Session},
    MountOptions,
};

fn apply_antares_cache_mount_options(options: &mut MountOptions) {
    // Enable write-back cache for better write performance.
    // This negotiates FUSE_WRITEBACK_CACHE flag during FUSE_INIT.
    //
    // NOTE: Caching timeouts (entry_timeout, attr_timeout, etc.) are NOT
    // configurable via mount options in Linux kernel FUSE. They must be
    // set in the filesystem implementation's ReplyEntry/ReplyAttr TTL fields.
    options.write_back(true);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FuseOwner {
    pub uid: u32,
    pub gid: u32,
}

static FUSE_OWNER: OnceLock<FuseOwner> = OnceLock::new();

fn parse_id_pair(uid: Option<&str>, gid: Option<&str>) -> Option<FuseOwner> {
    Some(FuseOwner {
        uid: uid?.parse().ok()?,
        gid: gid?.parse().ok()?,
    })
}

fn resolve_fuse_owner(
    current: FuseOwner,
    explicit: Option<FuseOwner>,
    sudo: Option<FuseOwner>,
) -> FuseOwner {
    if current.uid != 0 {
        return current;
    }
    explicit.or(sudo).unwrap_or(current)
}

/// Resolve the identity that should own the mounted view when ScorpioFS is launched by sudo.
///
/// A root daemon still services requests on behalf of the FUSE caller. Presenting the caller's
/// UID/GID in FUSE attributes keeps normal agent processes from being blocked by root-owned
/// upper-layer files. Explicit SCORPIO_FUSE_UID/GID values take precedence over sudo's identity.
pub(crate) fn fuse_owner() -> FuseOwner {
    *FUSE_OWNER.get_or_init(|| {
        let current = FuseOwner {
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
        };
        let explicit = parse_id_pair(
            env::var("SCORPIO_FUSE_UID").ok().as_deref(),
            env::var("SCORPIO_FUSE_GID").ok().as_deref(),
        );
        let sudo = parse_id_pair(
            env::var("SUDO_UID").ok().as_deref(),
            env::var("SUDO_GID").ok().as_deref(),
        );
        let owner = resolve_fuse_owner(current, explicit, sudo);
        if owner != current {
            tracing::info!(
                current_uid = current.uid,
                current_gid = current.gid,
                mount_uid = owner.uid,
                mount_gid = owner.gid,
                "aligning FUSE ownership with agent identity"
            );
        }
        owner
    })
}

/// Make a daemon-created directory accessible to the target FUSE owner.
pub(crate) fn align_directory_owner(path: &Path, owner: FuseOwner) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{chown, MetadataExt};

        let metadata = std::fs::metadata(path)?;
        if metadata.uid() != owner.uid || metadata.gid() != owner.gid {
            chown(path, Some(owner.uid), Some(owner.gid))?;
        }
    }
    #[cfg(not(unix))]
    let _ = (path, owner);
    Ok(())
}

#[allow(unused)]
pub async fn mount_filesystem<F: Filesystem + std::marker::Sync + Send + 'static>(
    fs: F,
    mountpoint: &OsStr,
) -> std::io::Result<MountHandle> {
    mount_filesystem_with_antares_cache(fs, mountpoint, false).await
}

#[allow(unused)]
pub async fn mount_filesystem_with_antares_cache<
    F: Filesystem + std::marker::Sync + Send + 'static,
>(
    fs: F,
    mountpoint: &OsStr,
    enable_antares_cache: bool,
) -> std::io::Result<MountHandle> {
    use std::io::{Error, ErrorKind};

    // This library function does not install a logger. The scorpio/antares
    // binaries call `util::logging::init` once at startup, which installs the
    // tracing subscriber and the `log` -> `tracing` bridge; a library consumer
    // that wants `log::` records captured must initialize tracing itself.
    //let logfs = LoggingFileSystem::new(fs);

    let mount_path: OsString = OsString::from(mountpoint);
    let path = std::path::Path::new(&mount_path);
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
    let owner = fuse_owner();

    let mut mount_options = MountOptions::default();
    mount_options
        .allow_other(true)
        .force_readdir_plus(true)
        .uid(owner.uid)
        .gid(owner.gid);
    if enable_antares_cache {
        apply_antares_cache_mount_options(&mut mount_options);
    }

    tracing::debug!("about to mount FUSE filesystem at: {:?}", mount_path);
    let session = Session::<F>::new(mount_options);
    session.mount(fs, mount_path).await.map_err(|e| {
        tracing::error!(
            "FUSE mount failed at {:?}: {:?} (os error code: {:?})",
            mountpoint,
            e,
            e.raw_os_error()
        );
        e
    })
}

#[cfg(test)]
mod tests {
    use super::{resolve_fuse_owner, FuseOwner};

    #[test]
    fn non_root_owner_cannot_be_overridden() {
        let current = FuseOwner {
            uid: 1000,
            gid: 1000,
        };
        assert_eq!(
            resolve_fuse_owner(
                current,
                Some(FuseOwner {
                    uid: 2000,
                    gid: 2000
                }),
                Some(FuseOwner {
                    uid: 3000,
                    gid: 3000
                }),
            ),
            current
        );
    }

    #[test]
    fn explicit_owner_precedes_sudo_owner_for_root() {
        let current = FuseOwner { uid: 0, gid: 0 };
        let explicit = FuseOwner {
            uid: 1001,
            gid: 1002,
        };
        let sudo = FuseOwner {
            uid: 1000,
            gid: 1000,
        };
        assert_eq!(
            resolve_fuse_owner(current, Some(explicit), Some(sudo)),
            explicit
        );
    }

    #[test]
    fn sudo_owner_is_used_when_explicit_owner_is_absent() {
        let current = FuseOwner { uid: 0, gid: 0 };
        let sudo = FuseOwner {
            uid: 1000,
            gid: 1000,
        };
        assert_eq!(resolve_fuse_owner(current, None, Some(sudo)), sudo);
    }
}
