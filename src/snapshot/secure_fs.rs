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
    path::Path,
};

/// Open a local object for reading without following a final symlink.
///
/// The returned descriptor is guaranteed to refer to a regular file.  The
/// helper deliberately leaves size and digest checks to the caller because
/// different users have different fixed-view invariants.
pub(crate) fn open_regular(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);

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

    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "local integrity object is not a regular file",
        ));
    }
    Ok(file)
}

/// Read a local integrity object through [`open_regular`].
pub(crate) fn read(path: &Path) -> io::Result<Vec<u8>> {
    let mut file = open_regular(path)?;
    let mut bytes = Vec::new();
    io::Read::read_to_end(&mut file, &mut bytes)?;
    Ok(bytes)
}
