use asyncfuse::{raw::reply::FileAttr, FileType, Timestamp};

/// Build a [`FileAttr`] that compiles on Linux and macOS.
///
/// macOS `FileAttr` requires `crtime` and `flags`; Linux does not have those
/// fields. Callers should go through this helper instead of struct literals.
#[allow(clippy::too_many_arguments)]
pub fn make_file_attr(
    ino: u64,
    size: u64,
    blocks: u64,
    atime: Timestamp,
    mtime: Timestamp,
    ctime: Timestamp,
    kind: FileType,
    perm: u16,
    nlink: u32,
    uid: u32,
    gid: u32,
    rdev: u32,
    blksize: u32,
) -> FileAttr {
    FileAttr {
        ino,
        size,
        blocks,
        atime,
        mtime,
        ctime,
        #[cfg(target_os = "macos")]
        crtime: ctime,
        kind,
        perm,
        nlink,
        uid,
        gid,
        rdev,
        #[cfg(target_os = "macos")]
        flags: 0,
        blksize,
    }
}

/// Zeroed attributes used for default / negative FUSE entries.
///
/// Ownership is the mount owner's, not the daemon's: the overlay's copy-up
/// preserves the lower layer's uid/gid when it materializes a node in the
/// upper layer, so reporting root here would create root-owned upper
/// directories the user cannot write into.
pub fn empty_file_attr(ino: u64, kind: FileType, perm: u16) -> FileAttr {
    let owner = crate::util::mount_owner::mount_owner();
    make_file_attr(
        ino,
        0,
        0,
        Timestamp::new(0, 0),
        Timestamp::new(0, 0),
        Timestamp::new(0, 0),
        kind,
        perm,
        0,
        owner.uid,
        owner.gid,
        0,
        0,
    )
}
