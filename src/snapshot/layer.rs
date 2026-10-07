//! Fixed MST/2 snapshot view as a workspace overlay lower layer (spec 12 §1).
//!
//! [`Mst2Fuse`] already implements the read-only FUSE semantics over a fixed
//! snapshot view (lookup/getattr/read/readdir/readlink, T10-verified). This
//! module adds the [`Layer`] implementation so a workspace OverlayFs stacks
//! the fixed view beneath its private writable upper.
//!
//! The layer is read-only by construction: every mutation answers `EROFS`
//! (see `fuse.rs`), so the overlay routes all writes to its upper layer and
//! the upper layer retains its OCI whiteout convention. A committed snapshot
//! is a complete namespace, not an OCI delta: `.wh.*` names are ordinary files.

use std::ffi::OsStr;

use asyncfuse::{
    raw::reply::{ReplyCreated, ReplyEntry},
    FileType, Result,
};
use libfuse_fs::{
    context::OperationContext,
    unionfs::{layer::Layer, Inode},
    util::whiteout::WhiteoutFormat,
};

use super::fuse::{self, Mst2Fuse, Node};

#[cfg(target_os = "linux")]
type Stat64 = libc::stat64;
#[cfg(target_os = "macos")]
type Stat64 = libc::stat;

#[async_trait::async_trait]
impl Layer for Mst2Fuse {
    fn root_inode(&self) -> Inode {
        fuse::ROOT_INODE
    }

    /// Snapshot entries cannot be character devices, so none is a whiteout.
    /// Using OCI here would hide ordinary committed `.wh.*` names and probe
    /// each child's contents while importing only its parent's directory.
    fn whiteout_format(&self) -> WhiteoutFormat {
        WhiteoutFormat::CharDev
    }

    async fn is_opaque(&self, _ctx: asyncfuse::raw::Request, inode: Inode) -> Result<bool> {
        match self.metadata_node(inode)? {
            Node::Dir(_) => Ok(false),
            Node::File(_) => Err(std::io::Error::from_raw_os_error(libc::ENOTDIR).into()),
        }
    }

    /// The union filesystem's copy-up path asks the lower layer for a raw
    /// `stat64` when it materializes a node in the upper layer. Answering the
    /// trait default (ENOSYS) makes every write that needs a copy-up fail with
    /// "Function not implemented", so a writable worktree over this lower must
    /// serve this. Sizes come from the verified snapshot entries — never a 0
    /// placeholder (spec 12 §1).
    async fn getattr_with_mapping(
        &self,
        inode: Inode,
        _handle: Option<u64>,
        _mapping: bool,
    ) -> std::io::Result<(Stat64, std::time::Duration)> {
        let _phase = crate::util::read_profile::phase(
            self.read_profile(),
            crate::util::read_profile::Phase::LowerGetattrMapping,
        );
        let node = self.metadata_node(inode).map_err(|e| {
            let raw = i32::from(e);
            tracing::warn!(
                inode,
                errno = raw,
                "mst2 lower: getattr_with_mapping on unknown inode"
            );
            std::io::Error::from_raw_os_error(raw.saturating_abs())
        })?;
        let attr = match &node {
            Node::Dir(_) => fuse::dir_attr(inode),
            Node::File(f) => fuse::file_attr(inode, f),
        };
        let type_bits: libc::mode_t = match attr.kind {
            FileType::Directory => libc::S_IFDIR,
            FileType::Symlink => libc::S_IFLNK,
            _ => libc::S_IFREG,
        };
        let mut stat: Stat64 = unsafe { std::mem::zeroed() };
        stat.st_dev = 0;
        stat.st_ino = inode;
        stat.st_nlink = attr.nlink as _;
        stat.st_mode = type_bits | attr.perm as libc::mode_t;
        stat.st_uid = attr.uid;
        stat.st_gid = attr.gid;
        stat.st_rdev = 0;
        stat.st_size = attr.size as i64;
        stat.st_blksize = 4096;
        stat.st_blocks = attr.blocks as i64;
        stat.st_atime = attr.atime.sec;
        stat.st_atime_nsec = attr.atime.nsec.into();
        stat.st_mtime = attr.mtime.sec;
        stat.st_mtime_nsec = attr.mtime.nsec.into();
        stat.st_ctime = attr.ctime.sec;
        stat.st_ctime_nsec = attr.ctime.nsec.into();
        Ok((stat, fuse::TTL))
    }

    // Everything else keeps the trait defaults:
    // - `host_path_of` → None: the view has no 1:1 host-fs mapping.
    // - `create_whiteout`/`delete_whiteout` → default bodies reach `lookup`
    //   and `mknod`/`unlink`, which answer EROFS here — a whiteout can never
    //   be recorded in an immutable snapshot view.

    /// The overlay's copy-up asks each layer, in order, to create the node.
    /// A read-only lower must answer EROFS (not the trait default ENOSYS) so
    /// the union filesystem moves on to the *upper* layer and creates the
    /// copy-up node with the requesting user's credentials. Answering ENOSYS
    /// makes copy-up fall back to a daemon-owned (root) creation, after which
    /// the user cannot write into the copied-up directory (EACCES).
    async fn create_with_context(
        &self,
        _ctx: OperationContext,
        _parent: Inode,
        _name: &OsStr,
        _mode: u32,
        _flags: u32,
    ) -> Result<ReplyCreated> {
        Err(std::io::Error::from_raw_os_error(libc::EROFS).into())
    }

    async fn mkdir_with_context(
        &self,
        _ctx: OperationContext,
        _parent: Inode,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry> {
        Err(std::io::Error::from_raw_os_error(libc::EROFS).into())
    }

    async fn symlink_with_context(
        &self,
        _ctx: OperationContext,
        _parent: Inode,
        _name: &OsStr,
        _link: &OsStr,
    ) -> Result<ReplyEntry> {
        Err(std::io::Error::from_raw_os_error(libc::EROFS).into())
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use asyncfuse::{raw::prelude::*, Errno};

    use super::*;

    /// An empty view: no reader, no store, no entries. Enough to exercise the
    /// layer surface without a server.
    fn empty_view() -> Mst2Fuse {
        Mst2Fuse::build(None, None, Vec::new()).expect("empty view builds")
    }

    fn erofs(e: Errno) -> bool {
        i32::from(e) == -libc::EROFS
    }

    #[test]
    fn lower_layer_identity_and_complete_namespace_convention() {
        let fs = empty_view();
        assert_eq!(Layer::root_inode(&fs), 1);
        assert_eq!(Layer::whiteout_format(&fs), WhiteoutFormat::CharDev);
    }

    /// A lower RELEASE error must not escape from copy-up through OPEN: Linux
    /// treats ENOSYS there as permission to skip all subsequent OPEN requests.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn copy_up_then_truncate_closes_lower_handle_successfully() {
        use std::sync::Arc;

        use libfuse_fs::{
            passthrough::{config::Config as UpperConfig, PassthroughFs},
            unionfs::{config::Config as OverlayConfig, OverlayFs},
        };

        use crate::snapshot::{
            durable::{digest_of, DurableStore, ViewMeta},
            SnapshotFile,
        };

        let temp = tempfile::tempdir().unwrap();
        let original = b"lower bytes\n";
        let manifest = vec![SnapshotFile {
            rel_path: "file.txt".into(),
            fs_kind: "file".into(),
            size: original.len() as u64,
            content_digest: digest_of(original),
        }];
        let store = Arc::new(DurableStore::open(temp.path().join("cas")).unwrap());
        store
            .hydrate_with(
                &ViewMeta {
                    snapshot_id: "sha256:test-snapshot".into(),
                    namespace_view_id: "sha256:test-view".into(),
                    scope: "/project".into(),
                    lease_id: "test-lease".into(),
                },
                &manifest,
                |_| async { Ok(original.to_vec()) },
            )
            .await
            .unwrap();
        let lower = Arc::new(Mst2Fuse::from_store(store).unwrap());
        let upper_path = temp.path().join("upper");
        std::fs::create_dir(&upper_path).unwrap();
        let upper = PassthroughFs::<()>::new(UpperConfig {
            root_dir: upper_path.clone(),
            do_import: true,
            writeback: false,
            whiteout_format: WhiteoutFormat::OciWhiteout,
            ..Default::default()
        })
        .unwrap();
        upper.import().await.unwrap();
        let overlay = OverlayFs::new(
            Some(Arc::new(upper)),
            vec![lower.clone()],
            OverlayConfig {
                do_import: true,
                ..Default::default()
            },
            1,
        )
        .unwrap();
        let req = Request::default();
        overlay.init(req).await.unwrap();
        let inode = overlay
            .lookup(req, 1, OsStr::new("file.txt"))
            .await
            .unwrap()
            .attr
            .ino;

        // This first writable OPEN performs copy-up and releases the lower.
        let opened = overlay
            .open(req, inode, (libc::O_WRONLY | libc::O_APPEND) as u32)
            .await
            .expect("copy-up OPEN must succeed, never return ENOSYS");
        overlay
            .write(
                req,
                inode,
                opened.fh,
                original.len() as u64,
                b"old tail",
                0,
                0,
            )
            .await
            .unwrap();
        overlay
            .release(req, inode, opened.fh, 0, 0, false)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(upper_path.join("file.txt")).unwrap(),
            [original.as_slice(), b"old tail"].concat()
        );

        let truncated = overlay
            .open(req, inode, (libc::O_WRONLY | libc::O_TRUNC) as u32)
            .await
            .unwrap();
        assert_eq!(
            std::fs::metadata(upper_path.join("file.txt"))
                .unwrap()
                .len(),
            0
        );
        overlay
            .write(req, inode, truncated.fh, 0, b"new", 0, 0)
            .await
            .unwrap();
        overlay
            .release(req, inode, truncated.fh, 0, 0, false)
            .await
            .unwrap();
        let reopened = overlay
            .open(req, inode, libc::O_RDONLY as u32)
            .await
            .unwrap();
        assert_eq!(
            overlay
                .read(req, inode, reopened.fh, 0, 64)
                .await
                .unwrap()
                .data
                .as_ref(),
            b"new"
        );
        overlay
            .release(req, inode, reopened.fh, 0, 0, false)
            .await
            .unwrap();
        assert_eq!(std::fs::read(upper_path.join("file.txt")).unwrap(), b"new");

        // Copy-up must leave the fixed snapshot content intact.
        let lower_inode = lower
            .lookup(req, 1, OsStr::new("file.txt"))
            .await
            .unwrap()
            .attr
            .ino;
        assert_eq!(
            lower
                .read(req, lower_inode, lower_inode, 0, 64)
                .await
                .unwrap()
                .data
                .as_ref(),
            original
        );
    }

    /// Every mutation must answer EROFS — not the trait default ENOSYS — so the
    /// overlay treats the fixed snapshot as read-only (spec 12 §7).
    #[tokio::test]
    async fn every_mutation_answers_erofs() {
        let fs = empty_view();
        let req = Request::default();
        let name = OsStr::new("probe");

        let err = fs.create(req, 1, name, 0o644, 0).await.unwrap_err();
        assert!(erofs(err), "create: {err:?}");
        let err = fs.mkdir(req, 1, name, 0o755, 0).await.unwrap_err();
        assert!(erofs(err), "mkdir: {err:?}");
        let err = fs.mknod(req, 1, name, 0o644, 0).await.unwrap_err();
        assert!(erofs(err), "mknod: {err:?}");
        let err = fs.symlink(req, 1, name, OsStr::new("t")).await.unwrap_err();
        assert!(erofs(err), "symlink: {err:?}");
        let err = fs.link(req, 1, 1, name).await.unwrap_err();
        assert!(erofs(err), "link: {err:?}");
        let err = fs.unlink(req, 1, name).await.unwrap_err();
        assert!(erofs(err), "unlink: {err:?}");
        let err = fs.rmdir(req, 1, name).await.unwrap_err();
        assert!(erofs(err), "rmdir: {err:?}");
        let err = fs.rename(req, 1, name, 1, name).await.unwrap_err();
        assert!(erofs(err), "rename: {err:?}");
        let err = fs.rename2(req, 1, name, 1, name, 0).await.unwrap_err();
        assert!(erofs(err), "rename2: {err:?}");
        let err = fs.write(req, 1, 0, 0, b"x", 0, 0).await.unwrap_err();
        assert!(erofs(err), "write: {err:?}");
        let err = fs
            .setattr(req, 1, None, SetAttr::default())
            .await
            .unwrap_err();
        assert!(erofs(err), "setattr: {err:?}");
        let err = fs.setxattr(req, 1, name, b"v", 0, 0).await.unwrap_err();
        assert!(erofs(err), "setxattr: {err:?}");
        let err = fs.removexattr(req, 1, name).await.unwrap_err();
        assert!(erofs(err), "removexattr: {err:?}");
        let err = fs.fallocate(req, 1, 0, 0, 0, 0).await.unwrap_err();
        assert!(erofs(err), "fallocate: {err:?}");
    }
}
