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

/// Open a read-only regular object without blocking on a substituted FIFO.
///
/// On Unix, nonblocking open reaches the descriptor type check even when no
/// FIFO writer is present. It does not change regular-file read semantics.
/// Other platforms retain the existing no-follow read-only open behavior.
pub(crate) fn open_regular_nonblocking(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(not(unix))]
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

/// Delete the currently opened regular entry through directory descriptors.
/// The caller owns the scope lifecycle fence; no saved digest list authorizes
/// this operation. Intermediate components and the final entry are no-follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RegularIdentity {
    pub size: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified: (i64, i64),
    #[cfg(unix)]
    changed: (i64, i64),
}

impl RegularIdentity {
    pub(crate) fn from_metadata(metadata: &std::fs::Metadata) -> io::Result<Self> {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cache entry is not regular",
            ));
        }
        Ok(Self {
            size: metadata.len(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(unix)]
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            #[cfg(unix)]
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
}

#[cfg(unix)]
pub(crate) struct PreparedRemoval {
    parent: File,
    entry: File,
    name: std::ffi::CString,
    expected: RegularIdentity,
}

#[cfg(unix)]
pub(crate) fn prepare_current_regular(
    directory: &Path,
    name: &str,
    expected: RegularIdentity,
) -> io::Result<Option<PreparedRemoval>> {
    use std::{
        ffi::CString,
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::ffi::OsStrExt,
        },
        path::Component,
    };
    if !directory.is_absolute()
        || name.is_empty()
        || name.contains(['/', '\\', '\0'])
        || name == "."
        || name == ".."
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid cache retirement path",
        ));
    }
    let root = CString::new("/").unwrap();
    let fd = unsafe {
        libc::open(
            root.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut parent = unsafe { File::from_raw_fd(fd) };
    for component in directory.components() {
        match component {
            Component::RootDir | Component::CurDir => continue,
            Component::Normal(part) => {
                let part = CString::new(part.as_bytes())
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
                let fd = unsafe {
                    libc::openat(
                        parent.as_raw_fd(),
                        part.as_ptr(),
                        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                    )
                };
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                parent = unsafe { File::from_raw_fd(fd) };
            }
            _ => return Err(io::Error::from(io::ErrorKind::InvalidInput)),
        }
    }
    let name = CString::new(name).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(error)
        };
    }
    let entry = unsafe { File::from_raw_fd(fd) };
    let opened = entry.metadata()?;
    if RegularIdentity::from_metadata(&opened)? != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cache retirement entry changed",
        ));
    }
    match entry.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Err(io::ErrorKind::WouldBlock.into()),
        Err(std::fs::TryLockError::Error(error)) => return Err(error),
    }
    let removal = PreparedRemoval {
        parent,
        entry,
        name,
        expected,
    };
    removal.verify()?;
    Ok(Some(removal))
}

#[cfg(unix)]
impl PreparedRemoval {
    pub(crate) fn verify(&self) -> io::Result<()> {
        use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
        let opened = self.entry.metadata()?;
        let mut current: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe {
            libc::fstatat(
                self.parent.as_raw_fd(),
                self.name.as_ptr(),
                &mut current,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        // MetadataExt::dev uses u64 on Unix, while libc::dev_t is signed on
        // macOS. Use the same conversion as the standard metadata accessor.
        #[allow(clippy::unnecessary_cast)]
        let current_device = current.st_dev as u64;
        if current_device != opened.dev()
            || current.st_ino != opened.ino()
            || current.st_size < 0
            || current.st_size as u64 != self.expected.size
            || current.st_mode & libc::S_IFMT != libc::S_IFREG
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cache retirement lifetime changed",
            ));
        }
        if RegularIdentity::from_metadata(&opened)? != self.expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cache retirement object changed",
            ));
        }
        Ok(())
    }
    pub(crate) fn remove(self) -> io::Result<bool> {
        use std::os::fd::AsRawFd;
        self.verify()?;
        if unsafe { libc::unlinkat(self.parent.as_raw_fd(), self.name.as_ptr(), 0) } < 0 {
            return Err(io::Error::last_os_error());
        }
        self.parent.sync_all()?;
        Ok(true)
    }
}

#[cfg(not(unix))]
pub(crate) struct PreparedRemoval;

#[cfg(not(unix))]
pub(crate) fn prepare_current_regular(
    _directory: &Path,
    _name: &str,
    _expected: RegularIdentity,
) -> io::Result<Option<PreparedRemoval>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "managed cache retirement requires directory-descriptor support",
    ))
}

#[cfg(not(unix))]
impl PreparedRemoval {
    pub(crate) fn verify(&self) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    pub(crate) fn remove(self) -> io::Result<bool> {
        Err(io::ErrorKind::Unsupported.into())
    }
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
