//! A v3 mount owns one fixed snapshot and one private writable layer.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use asyncfuse::raw::{logfs::LoggingFileSystem, MountHandle};
use libfuse_fs::{
    passthrough::{config::Config as UpperConfig, PassthroughFs},
    unionfs::{config::Config as OverlayConfig, layer::Layer, OverlayFs},
    util::whiteout::WhiteoutFormat,
};

use crate::{
    snapshot::fuse::Mst2Fuse,
    util::{fenced_fs::FencedFilesystem, fuse_platform, mutation_fence::MutationFence},
};

/// The control owner retains the actual overlay after unmount, until its final
/// upper scan finishes. It exposes neither a native filesystem nor Weak owner.
pub(crate) struct WorkspaceMount {
    mountpoint: PathBuf,
    overlay: FencedFilesystem<OverlayFs>,
    handle: Option<MountHandle>,
    mounted_identity: Option<(u64, u64)>,
    plain_identity: (u64, u64),
    retry_unmount: bool,
    retired: bool,
}

impl WorkspaceMount {
    pub(crate) async fn new(
        lower: Arc<Mst2Fuse>,
        upper: &Path,
        mountpoint: PathBuf,
    ) -> std::io::Result<Self> {
        crate::server::prepare_mountpoint(&mountpoint)?;
        use std::os::unix::fs::MetadataExt;
        let plain = std::fs::symlink_metadata(&mountpoint)?;
        if !plain.is_dir() || plain.file_type().is_symlink() {
            return Err(std::io::Error::other(
                "workspace mountpoint is not a real directory",
            ));
        }
        let plain_identity = (plain.dev(), plain.ino());
        let fs = PassthroughFs::<()>::new(UpperConfig {
            root_dir: upper.to_path_buf(),
            xattr: true,
            do_import: true,
            writeback: false,
            whiteout_format: WhiteoutFormat::OciWhiteout,
            ..Default::default()
        })?;
        #[cfg(target_os = "linux")]
        fs.import().await?;
        let read_profile = lower.read_profile().cloned();
        let overlay = OverlayFs::new(
            Some(Arc::new(fs)),
            vec![lower as Arc<dyn Layer>],
            OverlayConfig {
                mountpoint: mountpoint.clone(),
                do_import: true,
                ..Default::default()
            },
            1,
        )?;
        Ok(Self {
            mountpoint,
            overlay: FencedFilesystem::with_read_profile(overlay, read_profile),
            handle: None,
            mounted_identity: None,
            plain_identity,
            retry_unmount: false,
            retired: false,
        })
    }

    pub(crate) fn fence(&self) -> &MutationFence {
        self.overlay.fence()
    }

    pub(crate) async fn is_ready(&self) -> std::io::Result<bool> {
        if self.handle.is_none() || self.retry_unmount || self.retired {
            return Ok(false);
        }
        let path = self.mountpoint.clone();
        let Some(expected) = self.mounted_identity else {
            return Ok(false);
        };
        tokio::time::timeout(
            Duration::from_secs(1),
            tokio::task::spawn_blocking(move || -> std::io::Result<bool> {
                if !kernel_mount_present(&path)? {
                    return Ok(false);
                }
                use std::os::unix::fs::MetadataExt;
                let meta = std::fs::metadata(path)?;
                Ok(meta.is_dir() && (meta.dev(), meta.ino()) == expected)
            }),
        )
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "workspace readiness check timed out",
            )
        })?
        .map_err(|error| std::io::Error::other(error.to_string()))?
    }

    pub(crate) async fn mount(&mut self) -> std::io::Result<()> {
        if self.retired || self.retry_unmount {
            return Err(std::io::Error::other(
                "workspace generation cannot be remounted",
            ));
        }
        if self.handle.is_some() {
            return Ok(());
        }
        self.handle = Some(
            crate::server::mount_filesystem_with_writeback_cache(
                LoggingFileSystem::new(self.overlay.clone()),
                self.mountpoint.as_os_str(),
                false,
            )
            .await?,
        );
        // A timeout is an observable failure with the real handle retained for
        // teardown. It must not become a successful metadata-ready response.
        let identity = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match tokio::fs::metadata(&self.mountpoint).await {
                    Ok(meta) if meta.is_dir() && kernel_mount_present(&self.mountpoint)? => {
                        use std::os::unix::fs::MetadataExt;
                        return Ok((meta.dev(), meta.ino()));
                    }
                    Ok(meta) if meta.is_dir() => {
                        tokio::time::sleep(Duration::from_millis(20)).await
                    }
                    Ok(_) => return Err(std::io::Error::other("mounted root is not a directory")),
                    Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "workspace mount was not ready",
            )
        })??;
        self.mounted_identity = Some(identity);
        Ok(())
    }

    async fn recover(&self) -> std::io::Result<()> {
        self.fence().seal().await?;
        self.overlay.inner().recover_all_copyups().await?;
        self.fence().seal().await
    }

    /// End all native capabilities before a final dirty check or any removal.
    /// Every error and cancellation keeps this owner and its retry obligation.
    pub(crate) async fn unmount(&mut self) -> std::io::Result<()> {
        if self.retired {
            return self.recover().await;
        }
        use std::os::unix::fs::MetadataExt;
        let meta = tokio::fs::symlink_metadata(&self.mountpoint).await?;
        let actual = (meta.dev(), meta.ino());
        if !meta.is_dir()
            || meta.file_type().is_symlink()
            || (actual != self.plain_identity && Some(actual) != self.mounted_identity)
        {
            return Err(std::io::Error::other(
                "workspace kernel mount or mountpoint was replaced",
            ));
        }
        self.recover().await?;
        let result = if let Some(handle) = self.handle.take() {
            self.retry_unmount = true;
            match tokio::time::timeout(Duration::from_millis(1200), handle.unmount()).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) | Err(_) => fuse_platform::unmount_path(&self.mountpoint, true).await,
            }
        } else if self.retry_unmount {
            fuse_platform::unmount_path(&self.mountpoint, true).await
        } else {
            Ok(())
        };
        result?;
        // Session teardown alone does not join asyncfuse's late worker requests.
        // Keep the original private Arc until Acquire-synchronized uniqueness.
        self.retry_unmount = true;
        self.overlay
            .wait_for_retired_owners(Duration::from_secs(5))
            .await?;
        self.recover().await?;
        self.retired = true;
        self.retry_unmount = false;
        Ok(())
    }
}

fn kernel_mount_present(path: &Path) -> std::io::Result<bool> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "mount path contains NUL")
        })?;
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // statfs writes the complete result only on success. Check the actual
        // kernel filesystem, so a dead session cannot fall back to the plain
        // empty mount directory and be declared ready.
        if unsafe { libc::statfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { stat.assume_init() }.f_type == 0x6573_5546)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(fuse_platform::is_mounted(path))
    }
}
