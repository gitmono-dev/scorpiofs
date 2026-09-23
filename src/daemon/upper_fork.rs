//! Copying an Antares upper layer, for `fork`.
//!
//! An upper layer is a passthrough directory holding a worktree's private edits:
//! the files a user modified, plus a *whiteout* entry for every file they deleted
//! from the read-only lower layer.
//!
//! `fork` derives a second worktree from a first one, so the child must start out
//! seeing exactly what the parent saw at fork time. Copying the upper layer is how
//! that state is transferred when the two worktrees are not sharing layers.
//!
//! Two properties are load-bearing and easy to get wrong:
//!
//! * **A hard link is not a copy here.** The Antares upper is a *passthrough*
//!   directory: the overlay writes to upper files in place, through the same
//!   inode. Hard-linking a child's file to the parent's would make the child
//!   observe every later write in the parent, silently and with no error. So a
//!   file is either *reflinked* (`FICLONE`, a copy-on-write clone with its own
//!   inode) or really copied — never linked.
//!
//! * **The parent may be writing while we read.** A file can be captured
//!   half-written. Every file is therefore re-`stat`ed after it is copied and the
//!   copy is retried if the source moved; if it will not settle, the fork fails
//!   rather than producing a child that never existed.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

/// The VCS metadata directory inside a mount. It is a reconstructable pointer, not
/// part of the writable delta, so it is never copied — the child gets its own.
const VCS_POINTER_DIR: &str = ".libra";

/// `FICLONE` from `linux/fs.h` (values `_IOW(0x94, 9, int)`).
const FICLONE: libc::c_ulong = 0x4004_9409;

/// Per-file retries before a run of the delta copy is declared torn.
const FILE_ATTEMPTS: u32 = 3;

/// Whole-delta passes before the source is declared too busy to fork from.
const DELTA_ATTEMPTS: u32 = 3;

/// What a completed fork actually copied.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ForkCopyStats {
    /// Number of files written into the child upper layer.
    pub files: u64,
    /// Total bytes of those files (reflinked files count their logical size).
    pub bytes: u64,
    /// How many files were cloned with `FICLONE` instead of byte-copied.
    pub reflink_used: u64,
    /// How many per-file retries were needed because the source was moving.
    pub retries: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ForkCopyError {
    #[error("upper layer I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(
        "the source upper layer contains a {kind} at {path}, which fork does not know how to copy"
    )]
    UnsupportedEntry { path: PathBuf, kind: &'static str },
    #[error(
        "the source upper layer kept changing while it was copied ({attempts} attempts); \
         quiesce the source worktree and fork again"
    )]
    SourceBusy { attempts: u32 },
}

fn io_err(path: &Path) -> impl FnOnce(io::Error) -> ForkCopyError + '_ {
    move |source| ForkCopyError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Copy the writable delta of `src_upper` into `dst_upper`.
///
/// `dst_upper` is created if missing; it must not already contain entries, because
/// merging a second delta into a non-empty layer has no defined precedence.
///
/// `.libra` is skipped at the top level only — the child owns its own pointer.
pub fn fork_upper(src_upper: &Path, dst_upper: &Path) -> Result<ForkCopyStats, ForkCopyError> {
    fs::create_dir_all(dst_upper).map_err(io_err(dst_upper))?;
    ensure_destination_empty(dst_upper)?;

    let mut total = ForkCopyStats::default();

    for attempt in 1..=DELTA_ATTEMPTS {
        let delta = list_delta(src_upper)?;
        let mut stats = ForkCopyStats::default();

        for rel in &delta {
            copy_one(src_upper, dst_upper, rel, &mut stats)?;
        }

        // Re-list: a file added or removed while we walked would otherwise be lost.
        // (Content changes within a stable path set are caught per-file instead.)
        let after = list_delta(src_upper)?;
        if after == delta {
            stats.retries += total.retries;
            return Ok(stats);
        }

        total.retries += stats.retries + 1;
        if attempt == DELTA_ATTEMPTS {
            return Err(ForkCopyError::SourceBusy {
                attempts: DELTA_ATTEMPTS,
            });
        }
        // Retry with a fresh listing; already-copied paths are re-verified.
        reset_destination(dst_upper)?;
    }

    unreachable!("the loop either returns Ok or Err")
}

fn ensure_destination_empty(dst: &Path) -> Result<(), ForkCopyError> {
    let mut entries = fs::read_dir(dst).map_err(io_err(dst))?;
    if let Some(entry) = entries.next() {
        let entry = entry.map_err(io_err(dst))?;
        return Err(ForkCopyError::Io {
            path: entry.path(),
            source: io::Error::new(
                io::ErrorKind::AlreadyExists,
                "fork destination upper layer is not empty",
            ),
        });
    }
    Ok(())
}

/// Empty the destination between delta attempts.
fn reset_destination(dst: &Path) -> Result<(), ForkCopyError> {
    for entry in fs::read_dir(dst).map_err(io_err(dst))? {
        let entry = entry.map_err(io_err(dst))?;
        let path = entry.path();
        let meta = fs::symlink_metadata(&path).map_err(io_err(&path))?;
        if meta.is_dir() {
            fs::remove_dir_all(&path).map_err(io_err(&path))?;
        } else {
            fs::remove_file(&path).map_err(io_err(&path))?;
        }
    }
    Ok(())
}

/// Every path in the layer, relative to its root, excluding the VCS pointer dir.
///
/// Directories are included so that empty directories survive the copy.
fn list_delta(root: &Path) -> Result<BTreeSet<PathBuf>, ForkCopyError> {
    let mut out = BTreeSet::new();
    let mut pending = vec![PathBuf::new()];

    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(root.join(&dir)).map_err(io_err(&root.join(&dir)))? {
            let entry = entry.map_err(io_err(&root.join(&dir)))?;
            let name = entry.file_name();
            let rel = dir.join(&name);

            if dir.as_os_str().is_empty() && name == VCS_POINTER_DIR {
                continue;
            }

            let meta = fs::symlink_metadata(entry.path()).map_err(io_err(&entry.path()))?;
            if meta.is_dir() {
                pending.push(rel.clone());
            }
            out.insert(rel);
        }
    }
    Ok(out)
}

fn copy_one(
    src_root: &Path,
    dst_root: &Path,
    rel: &Path,
    stats: &mut ForkCopyStats,
) -> Result<(), ForkCopyError> {
    // `rel` comes from our own walk, but this function is a public-facing seam:
    // refuse anything that could escape the layer root.
    if rel.is_absolute()
        || rel
            .components()
            .any(|c| c == std::path::Component::ParentDir)
    {
        return Err(ForkCopyError::UnsupportedEntry {
            path: rel.to_path_buf(),
            kind: "path outside the layer",
        });
    }

    let src = src_root.join(rel);
    let dst = dst_root.join(rel);
    let meta = fs::symlink_metadata(&src).map_err(io_err(&src))?;

    if meta.is_dir() {
        fs::create_dir_all(&dst).map_err(io_err(&dst))?;
        fs::set_permissions(&dst, fs::Permissions::from_mode(meta.mode() & 0o7777))
            .map_err(io_err(&dst))?;
        return Ok(());
    }

    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).map_err(io_err(parent))?;
    }

    if meta.file_type().is_symlink() {
        let target = fs::read_link(&src).map_err(io_err(&src))?;
        let _ = fs::remove_file(&dst);
        std::os::unix::fs::symlink(&target, &dst).map_err(io_err(&dst))?;
        return Ok(());
    }

    if !meta.is_file() {
        // A char/block device here is a Linux char-device whiteout: the form
        // Antares does not use (see `ANTARES_WHITEOUT_FORMAT`). Refuse loudly
        // rather than copy something whose meaning we would be guessing at.
        let kind = if meta.file_type().is_char_device() {
            "character device"
        } else if meta.file_type().is_block_device() {
            "block device"
        } else if meta.file_type().is_fifo() {
            "fifo"
        } else if meta.file_type().is_socket() {
            "socket"
        } else {
            "special file"
        };
        return Err(ForkCopyError::UnsupportedEntry {
            path: rel.to_path_buf(),
            kind,
        });
    }

    copy_file_verified(&src, &dst, stats)
}

/// Copy one regular file, retrying while the source is being written.
///
/// Reflink first (an O(1) CoW clone with its own inode), falling back to a byte
/// copy on filesystems without `FICLONE` (e.g. ext4 without reflink support).
fn copy_file_verified(
    src: &Path,
    dst: &Path,
    stats: &mut ForkCopyStats,
) -> Result<(), ForkCopyError> {
    for attempt in 0..FILE_ATTEMPTS {
        let before = fs::symlink_metadata(src).map_err(io_err(src))?;
        if !before.is_file() {
            // Replaced by a non-file while we looked at it; let the next delta pass
            // see whatever it became.
            return Err(ForkCopyError::SourceBusy {
                attempts: FILE_ATTEMPTS,
            });
        }

        let cloned = try_reflink(src, dst, before.size());
        let written_bytes = if cloned {
            stats.reflink_used += 1;
            before.size()
        } else {
            fs::copy(src, dst).map_err(io_err(dst))?
        };
        fs::set_permissions(dst, fs::Permissions::from_mode(before.mode() & 0o7777))
            .map_err(io_err(dst))?;

        let after = fs::symlink_metadata(src).map_err(io_err(src))?;
        if same_file_state(&before, &after) {
            stats.files += 1;
            stats.bytes += written_bytes;
            return Ok(());
        }

        // The source moved under us; discard and retry.
        stats.retries += 1;
        let _ = fs::remove_file(dst);
        if attempt + 1 == FILE_ATTEMPTS {
            return Err(ForkCopyError::SourceBusy {
                attempts: FILE_ATTEMPTS,
            });
        }
    }
    unreachable!("the loop either returns Ok or Err")
}

/// Everything that must be unchanged for a copy to be a real snapshot of one instant.
fn same_file_state(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    a.ino() == b.ino()
        && a.size() == b.size()
        && a.mtime() == b.mtime()
        && a.mtime_nsec() == b.mtime_nsec()
        && a.ctime() == b.ctime()
        && a.ctime_nsec() == b.ctime_nsec()
}

/// Try `FICLONE`. Returns whether the file was cloned into `dst`.
///
/// Any failure (unsupported filesystem, cross-device, ...) is reported as "not
/// cloned" so the caller falls back to a byte copy.
fn try_reflink(src: &Path, dst: &Path, size: u64) -> bool {
    let Ok(src_file) = fs::File::open(src) else {
        return false;
    };
    let Ok(dst_file) = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst)
    else {
        return false;
    };

    // SAFETY: both fds are valid for the duration of the call; FICLONE takes an
    // int argument (the source fd) and reads no user memory.
    let rc = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE, src_file.as_raw_fd()) };
    if rc != 0 {
        return false;
    }

    // A clone that did not carry the data would be worse than no clone at all.
    match dst_file.metadata() {
        Ok(meta) => meta.size() == size,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, contents).unwrap();
    }

    #[test]
    fn copies_modified_files_and_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write(&src.join("a.txt"), "alpha");
        write(&src.join("deep/nested/b.txt"), "beta");
        fs::create_dir_all(src.join("empty-dir")).unwrap();

        let stats = fork_upper(&src, &dst).unwrap();

        assert_eq!(stats.files, 2);
        assert_eq!(fs::read_to_string(dst.join("a.txt")).unwrap(), "alpha");
        assert_eq!(
            fs::read_to_string(dst.join("deep/nested/b.txt")).unwrap(),
            "beta"
        );
        assert!(dst.join("empty-dir").is_dir(), "empty dir must survive");
    }

    #[test]
    fn skips_the_vcs_pointer_at_the_top_level_only() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write(&src.join(".libra/commondir"), "/host/main/.libra\n");
        write(&src.join(".libra/worktree_id"), "wt-a\n");
        // A *nested* `.libra` is an ordinary directory and must be copied.
        write(&src.join("vendor/.libra/keep.txt"), "keep");

        fork_upper(&src, &dst).unwrap();

        assert!(
            !dst.join(".libra").exists(),
            "the child must get its own pointer, not the parent's"
        );
        assert_eq!(
            fs::read_to_string(dst.join("vendor/.libra/keep.txt")).unwrap(),
            "keep"
        );
    }

    #[test]
    fn copies_whiteouts_as_ordinary_files() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        // The OCI whiteout form: an empty regular `.wh.<name>` file.
        write(&src.join("src/.wh.gone.rs"), "");
        write(&src.join("src/.wh..wh..opq"), "");

        fork_upper(&src, &dst).unwrap();

        let wh = dst.join("src/.wh.gone.rs");
        let meta = fs::symlink_metadata(&wh).unwrap();
        assert!(
            meta.is_file(),
            "whiteout must stay a regular file, not become a device"
        );
        assert_eq!(meta.size(), 0, "an OCI whiteout is empty");
        assert!(
            dst.join("src/.wh..wh..opq").exists(),
            "opaque marker copied"
        );
    }

    #[test]
    fn copies_symlinks_without_following_them() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write(&src.join("real.txt"), "real");
        std::os::unix::fs::symlink("real.txt", src.join("link.txt")).unwrap();

        fork_upper(&src, &dst).unwrap();

        let meta = fs::symlink_metadata(dst.join("link.txt")).unwrap();
        assert!(meta.file_type().is_symlink());
        assert_eq!(
            fs::read_link(dst.join("link.txt")).unwrap(),
            PathBuf::from("real.txt")
        );
    }

    #[test]
    fn refuses_a_non_empty_destination() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write(&src.join("a.txt"), "alpha");
        write(&dst.join("already-there.txt"), "clash");

        let err = fork_upper(&src, &dst).unwrap_err();
        assert!(matches!(err, ForkCopyError::Io { .. }), "got {err:?}");
    }

    #[test]
    fn preserves_the_executable_bit() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write(&src.join("run.sh"), "#!/bin/sh\n");
        fs::set_permissions(src.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();

        fork_upper(&src, &dst).unwrap();

        let mode = fs::symlink_metadata(dst.join("run.sh"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o755, "mode was {mode:o}");
    }

    #[test]
    fn copied_files_are_separate_inodes_not_hard_links() {
        // The whole point of the hard-link ban: a later in-place write to the
        // parent's file must not appear in the child.
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write(&src.join("shared.txt"), "parent-before");
        fork_upper(&src, &dst).unwrap();

        let src_ino = fs::symlink_metadata(src.join("shared.txt")).unwrap().ino();
        let dst_ino = fs::symlink_metadata(dst.join("shared.txt")).unwrap().ino();
        assert_ne!(src_ino, dst_ino, "copy must not be a hard link");

        // Simulate the overlay writing the parent's file in place.
        fs::write(src.join("shared.txt"), "parent-after-write").unwrap();

        assert_eq!(
            fs::read_to_string(dst.join("shared.txt")).unwrap(),
            "parent-before",
            "the child must not observe later writes in the parent"
        );
    }

    #[test]
    fn rejects_a_path_escaping_the_layer() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();

        let mut stats = ForkCopyStats::default();
        let err = copy_one(&src, &dst, Path::new("../escape.txt"), &mut stats).unwrap_err();
        assert!(
            matches!(err, ForkCopyError::UnsupportedEntry { .. }),
            "got {err:?}"
        );
    }
}
