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
pub fn empty_file_attr(ino: u64, kind: FileType, perm: u16) -> FileAttr {
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
        0,
        0,
        0,
        0,
    )
}
