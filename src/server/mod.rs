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

use std::ffi::{OsStr, OsString};

use rfuse3::{
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

    // Logging is initialized once by the binary entry point (tracing). This
    // library function no longer installs its own logger; `log::` records are
    // bridged into the global tracing subscriber.
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
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let mut mount_options = MountOptions::default();
    // .allow_other(true)
    mount_options.force_readdir_plus(true).uid(uid).gid(gid);
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
