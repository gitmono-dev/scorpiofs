//! Small, platform-specific helpers for reading local integrity metadata.
//!
//! Paths in the durable CAS and the scope cache are derived from already
//! validated identifiers, but path validation alone does not prevent a
//! final path component from being replaced by a symlink between the check
//! and the read.  Open the file with the platform's no-follow flag and then
//! validate the descriptor.  The descriptor also makes a replacement after
//! open harmless: callers read and hash the object that they opened.

use std::{
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

/// Open a local object for reading without following a final symlink.
///
/// The returned descriptor is guaranteed to refer to a regular file.  The
/// helper deliberately leaves size and digest checks to the caller because
/// different users have different fixed-view invariants.
pub(crate) fn open_regular(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    add_no_follow(&mut options);

    open_checked(options, path)
}

/// Open a fixed lock/record for read-write coordination without following a
/// final symlink.  The caller decides whether to lock or write the handle.
pub(crate) fn open_rw_create(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    add_no_follow(&mut options);
    open_checked(options, path)
}

/// Open a fixed record for replacement without following an existing final
/// symlink.  This is used only for files whose name is controlled by the
/// caller and whose contents are replaced atomically afterwards.
pub(crate) fn open_write_truncate(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    add_no_follow(&mut options);
    open_checked(options, path)
}

/// Open an existing local record for append without following a final
/// symlink.  The journal is append-only during normal hydration, but it is
/// still integrity metadata and must not be redirected outside the cache.
pub(crate) fn open_append_create(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.append(true).create(true);
    add_no_follow(&mut options);
    open_checked(options, path)
}

/// Open an existing local record for in-place updates without following a
/// final symlink.
pub(crate) fn open_write(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true);
    add_no_follow(&mut options);
    open_checked(options, path)
}

/// Create a unique local temporary file without following a final symlink.
pub(crate) fn open_create_new(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    add_no_follow(&mut options);
    open_checked(options, path)
}

fn open_checked(options: OpenOptions, path: &Path) -> io::Result<File> {
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "local integrity object is not a regular file",
        ));
    }
    Ok(file)
}

fn add_no_follow(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // Keep the reparse point itself open so metadata checks below reject
        // a symlink/junction instead of silently traversing it.
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
}

/// Read a local integrity object through [`open_regular`].
pub(crate) fn read(path: &Path) -> io::Result<Vec<u8>> {
    let mut file = open_regular(path)?;
    let mut bytes = Vec::new();
    io::Read::read_to_end(&mut file, &mut bytes)?;
    Ok(bytes)
}

/// Create a directory chain after checking every existing component is a
/// real directory.  `std::fs::create_dir_all` follows intermediate symlinks;
/// that is unsafe for cache and authority roots because a redirected parent
/// can move integrity state outside the configured domain.  The second check
/// catches a symlink already present at the requested leaf after creation.
///
/// This is a conservative preflight.  Callers still use regular descriptor
/// opens for files, so a concurrent component replacement cannot turn a file
/// read into a symlink traversal.  A future dirfd/openat implementation can
/// replace this helper without changing call sites.
pub(crate) fn create_dir_all_no_symlink(path: &Path) -> io::Result<()> {
    validate_directory_chain(path)?;
    std::fs::create_dir_all(path)?;
    validate_directory_chain(path)
}

fn validate_directory_chain(path: &Path) -> io::Result<()> {
    let mut cursor = PathBuf::from(path);
    loop {
        match std::fs::symlink_metadata(&cursor) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("directory component is a symlink: {}", cursor.display()),
                    ));
                }
                if !metadata.is_dir() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotADirectory,
                        format!(
                            "directory component is not a directory: {}",
                            cursor.display()
                        ),
                    ));
                }
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let parent = cursor.parent().map(Path::to_path_buf);
                match parent {
                    Some(parent) if parent != cursor => cursor = parent,
                    _ => return Ok(()),
                }
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    #[test]
    fn all_fixed_record_writers_reject_a_final_symlink() {
        use std::{fs, os::unix::fs::symlink};

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("outside");
        let link = temp.path().join("record");
        fs::write(&target, b"sentinel").unwrap();
        for open in [
            super::open_rw_create as fn(&std::path::Path) -> std::io::Result<std::fs::File>,
            super::open_write_truncate,
            super::open_append_create,
            super::open_write,
            super::open_create_new,
        ] {
            symlink(&target, &link).unwrap();
            assert!(open(&link).is_err());
            fs::remove_file(&link).unwrap();
            assert_eq!(fs::read(&target).unwrap(), b"sentinel");
        }
    }
}
