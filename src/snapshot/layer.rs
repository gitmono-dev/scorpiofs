//! MST/2 snapshot view as an Antares overlay **lower layer** (spec 12 §1).
//!
//! [`Mst2Fuse`] already implements the read-only FUSE semantics over a fixed
//! snapshot view (lookup/getattr/read/readdir/readlink, T10-verified). This
//! module adds the [`Layer`] impl so the Antares `OverlayFs` can stack it in
//! the position `Dicfuse` occupies today — spec 12: "现有 user-space Layer
//! 适配到 SnapshotReader".
//!
//! The layer is read-only by construction: every mutation answers `EROFS`
//! (see `fuse.rs`), so the overlay routes all writes to its upper layer and
//! the whiteout format stays the Antares-wide OCI convention.

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

    /// Same convention as every other Antares layer: deletions are recorded as
    /// OCI `.wh.<name>` markers, never character devices (no `CAP_MKNOD`).
    fn whiteout_format(&self) -> WhiteoutFormat {
        WhiteoutFormat::OciWhiteout
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
        let node = self.node(inode).map_err(|e| {
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
    use super::*;
    use asyncfuse::{raw::prelude::*, Errno};
    use std::ffi::OsStr;

    /// An empty view: no reader, no store, no entries. Enough to exercise the
    /// layer surface without a server.
    fn empty_view() -> Mst2Fuse {
        Mst2Fuse::build(None, None, Vec::new()).expect("empty view builds")
    }

    fn erofs(e: Errno) -> bool {
        i32::from(e) == -libc::EROFS
    }

    #[test]
    fn lower_layer_identity_and_whiteout_convention() {
        let fs = empty_view();
        assert_eq!(Layer::root_inode(&fs), 1);
        assert_eq!(Layer::whiteout_format(&fs), WhiteoutFormat::OciWhiteout);
    }

    /// Every mutation must answer EROFS — not the trait default ENOSYS — so the
    /// overlay treats the layer as read-only exactly like Dicfuse (spec 12 §7).
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
        let err = fs
            .setxattr(req, 1, name, b"v", 0, 0)
            .await
            .unwrap_err();
        assert!(erofs(err), "setxattr: {err:?}");
        let err = fs.removexattr(req, 1, name).await.unwrap_err();
        assert!(erofs(err), "removexattr: {err:?}");
        let err = fs.fallocate(req, 1, 0, 0, 0, 0).await.unwrap_err();
        assert!(erofs(err), "fallocate: {err:?}");
    }
}
