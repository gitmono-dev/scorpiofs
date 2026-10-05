//! Bounded effective upper diff against one fixed snapshot's metadata.
//!
//! The caller holds the actual mount's mutation pause for the whole scan.
//! Content is hashed from local upper fds; lower bodies are never fetched.

use std::{path::Path, sync::Arc};

use serde::Serialize;

use super::{fuse::Mst2Fuse, SnapshotError, SnapshotErrorCode, SnapshotNodeIdentity};
use crate::util::mutation_fence::MutationPause;

#[derive(Debug, Clone, Copy)]
pub struct DiffLimits {
    pub max_nodes: usize,
    pub max_bytes: u64,
    pub max_depth: usize,
}
impl Default for DiffLimits {
    fn default() -> Self {
        Self {
            max_nodes: 100_000,
            max_bytes: 4 * 1024 * 1024 * 1024,
            max_depth: 256,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UpperNodeIdentity {
    Directory {
        mode: u32,
        uid: u32,
        gid: u32,
    },
    Regular {
        mode: u32,
        uid: u32,
        gid: u32,
        size: u64,
        content_digest: String,
    },
    Symlink {
        mode: u32,
        uid: u32,
        gid: u32,
        size: u64,
        content_digest: String,
    },
}
impl UpperNodeIdentity {
    fn matches(&self, base: &SnapshotNodeIdentity) -> bool {
        let owner = crate::util::mount_owner::mount_owner();
        let (uid, gid) = match self {
            Self::Directory { uid, gid, .. }
            | Self::Regular { uid, gid, .. }
            | Self::Symlink { uid, gid, .. } => (*uid, *gid),
        };
        if uid != owner.uid || gid != owner.gid {
            return false;
        }
        match (self, base) {
            (Self::Directory { mode, .. }, SnapshotNodeIdentity::Directory { .. }) => {
                *mode == base.mode()
            }
            (
                Self::Regular {
                    mode,
                    size,
                    content_digest,
                    ..
                },
                SnapshotNodeIdentity::Regular { .. } | SnapshotNodeIdentity::Executable { .. },
            )
            | (
                Self::Symlink {
                    mode,
                    size,
                    content_digest,
                    ..
                },
                SnapshotNodeIdentity::Symlink { .. },
            ) => *mode == base.mode() && base.content() == Some((*size, content_digest.as_str())),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpperChangeKind {
    Added,
    Modified,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UpperChange {
    pub rel_path: String,
    pub kind: UpperChangeKind,
    pub base: Option<SnapshotNodeIdentity>,
    pub upper: Option<UpperNodeIdentity>,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct DiffMeters {
    pub upper_nodes: u64,
    pub hash_bytes: u64,
    pub lower_queries: u64,
    pub lower_directory_entries: u64,
    pub xattr_checks: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpperDiff {
    pub changes: Vec<UpperChange>,
    pub meters: DiffMeters,
}
impl UpperDiff {
    pub fn is_clean(&self) -> bool {
        self.changes.is_empty()
    }
}

fn unknown(message: impl Into<String>) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::IntegrityError, message)
}
fn budget(message: impl Into<String>) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::ProofBudgetExceeded, message)
}

/// Scan while the caller owns this workspace's real mount pause. A Result
/// error means Unknown, never clean. The pause is not consumed or sealed.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub async fn scan_upper(
    lower: &Arc<Mst2Fuse>,
    upper: &Path,
    pause: &MutationPause,
    limits: DiffLimits,
) -> Result<UpperDiff, SnapshotError> {
    use std::collections::{BTreeMap, HashSet};

    use super::SnapshotPathState;

    pause
        .ensure_certain()
        .map_err(|error| unknown(error.to_string()))?;
    if limits.max_nodes == 0 || limits.max_depth > 256 {
        return Err(SnapshotError::new(
            SnapshotErrorCode::InvalidRequest,
            "invalid upper diff bounds",
        ));
    }
    let path = upper.to_path_buf();
    let scanned = tokio::task::spawn_blocking(move || unix::collect(&path, limits))
        .await
        .map_err(|error| unknown(format!("upper scan task failed: {error}")))??;
    let mut meters = scanned.meters;
    meters.lower_queries += 1;
    let root_base = match lower.path_state("").await? {
        SnapshotPathState::Present(identity @ SnapshotNodeIdentity::Directory { .. }) => identity,
        _ => return Err(unknown("fixed lower scope root is not a proved directory")),
    };
    let mut changes = BTreeMap::new();
    if !scanned.facts[""].identity.matches(&root_base) {
        changes.insert(
            String::new(),
            UpperChange {
                rel_path: String::new(),
                kind: UpperChangeKind::Modified,
                base: Some(root_base),
                upper: Some(scanned.facts[""].identity.clone()),
            },
        );
    }
    // lower_directory=false is derived from a proved absent/non-directory
    // parent, never from swallowing a lookup error.
    let mut pending = vec![(String::new(), false, true)];
    while let Some((directory, inherited_opaque, lower_directory)) = pending.pop() {
        let fact = &scanned.facts[&directory];
        let names = &fact.children;
        let opaque = inherited_opaque || names.iter().any(|name| name == ".wh..wh..opq");
        let visible: HashSet<_> = names
            .iter()
            .filter(|name| !name.starts_with(".wh."))
            .map(String::as_str)
            .collect();
        for name in names {
            let path = join(&directory, name);
            let node = &scanned.facts[&path].identity;
            if name.starts_with(".wh.") {
                if !matches!(node, UpperNodeIdentity::Regular { size: 0, .. }) {
                    return Err(unknown(
                        "OCI whiteout/opaque marker must be an empty regular file",
                    ));
                }
                if name == ".wh..wh..opq" {
                    continue;
                }
                let target = name.strip_prefix(".wh.").unwrap();
                super::auth::validate_scope(&format!("/{target}"))?;
                if target.is_empty() || visible.contains(target) {
                    return Err(unknown("invalid or conflicting OCI whiteout target"));
                }
                let target_path = join(&directory, target);
                let base = base_at(lower, &target_path, lower_directory, &mut meters).await?;
                if let Some(base) = base {
                    changes.insert(
                        target_path.clone(),
                        UpperChange {
                            rel_path: target_path,
                            kind: UpperChangeKind::Deleted,
                            base: Some(base),
                            upper: None,
                        },
                    );
                }
                continue;
            }
            let base = base_at(lower, &path, lower_directory, &mut meters).await?;
            let base_directory = matches!(base, Some(SnapshotNodeIdentity::Directory { .. }));
            if base.as_ref().is_none_or(|identity| !node.matches(identity)) {
                changes.insert(
                    path.clone(),
                    UpperChange {
                        rel_path: path.clone(),
                        kind: if base.is_some() {
                            UpperChangeKind::Modified
                        } else {
                            UpperChangeKind::Added
                        },
                        base,
                        upper: Some(node.clone()),
                    },
                );
            }
            if matches!(node, UpperNodeIdentity::Directory { .. }) {
                pending.push((path, opaque, base_directory));
            }
        }
        if opaque && lower_directory {
            meters.lower_queries += 1;
            let entries = lower.directory_entries(&directory).await?;
            meters.lower_directory_entries = meters
                .lower_directory_entries
                .checked_add(entries.len() as u64)
                .ok_or_else(|| budget("lower entry meter overflow"))?;
            if meters.lower_directory_entries > limits.max_nodes as u64 {
                return Err(budget("opaque lower metadata entry budget exceeded"));
            }
            for entry in entries {
                if !visible.contains(entry.name.as_str()) {
                    let path = join(&directory, &entry.name);
                    changes.insert(
                        path.clone(),
                        UpperChange {
                            rel_path: path,
                            kind: UpperChangeKind::Deleted,
                            base: Some(entry.identity),
                            upper: None,
                        },
                    );
                }
            }
        }
        if changes.len() > limits.max_nodes {
            return Err(budget("upper diff result budget exceeded"));
        }
        pause
            .ensure_certain()
            .map_err(|error| unknown(error.to_string()))?;
    }
    // Recheck the anchored directory tree after all awaited metadata reads.
    // A host-side replacement or write must not turn into a clean result.
    tokio::task::spawn_blocking(move || unix::verify(&scanned))
        .await
        .map_err(|error| unknown(format!("upper verification task failed: {error}")))??;
    pause
        .ensure_certain()
        .map_err(|error| unknown(error.to_string()))?;
    Ok(UpperDiff {
        changes: changes.into_values().collect(),
        meters,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn base_at(
    lower: &Mst2Fuse,
    path: &str,
    parent_directory: bool,
    meters: &mut DiffMeters,
) -> Result<Option<SnapshotNodeIdentity>, SnapshotError> {
    lower.validate_metadata_path(path)?;
    if !parent_directory {
        return Ok(None);
    }
    meters.lower_queries += 1;
    match lower.path_state(path).await? {
        super::SnapshotPathState::Present(identity) => Ok(Some(identity)),
        super::SnapshotPathState::AbsentProven => Ok(None),
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn join(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub async fn scan_upper(
    _: &Arc<Mst2Fuse>,
    _: &Path,
    _: &MutationPause,
    _: DiffLimits,
) -> Result<UpperDiff, SnapshotError> {
    Err(SnapshotError::new(
        SnapshotErrorCode::UnsupportedEntry,
        "anchored upper diff requires Linux or macOS",
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod unix {
    use std::{
        collections::BTreeMap,
        ffi::{CStr, CString, OsStr},
        fs::{File, OpenOptions},
        io::Read,
        os::{
            fd::{AsRawFd, FromRawFd, IntoRawFd},
            unix::{ffi::OsStrExt, fs::OpenOptionsExt},
        },
        path::{Component, Path, PathBuf},
    };

    use ring::digest::{Context, SHA256};

    use super::*;

    const BUFFER_BYTES: usize = 64 * 1024;
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Fingerprint {
        dev: u64,
        ino: u64,
        mode: u32,
        size: i64,
        nlink: u64,
        uid: u32,
        gid: u32,
        mtime: (i64, i64),
        ctime: (i64, i64),
    }
    pub(super) struct Fact {
        pub identity: UpperNodeIdentity,
        pub children: Vec<String>,
        fingerprint: Fingerprint,
    }
    pub(super) struct Scanned {
        pub facts: BTreeMap<String, Fact>,
        pub meters: DiffMeters,
        root: File,
        root_path: PathBuf,
    }

    fn io(error: std::io::Error) -> SnapshotError {
        unknown(format!("upper filesystem state is unproved: {error}"))
    }
    fn c_name(name: &OsStr) -> Result<CString, SnapshotError> {
        CString::new(name.as_bytes()).map_err(|_| unknown("NUL in upper component"))
    }
    fn open_at(parent: &File, name: &OsStr, directory: bool) -> Result<File, SnapshotError> {
        let name = c_name(name)?;
        let flags = libc::O_RDONLY
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK
            | if directory { libc::O_DIRECTORY } else { 0 };
        // SAFETY: the name is NUL-terminated, parent is live, and a successful
        // descriptor is immediately transferred to its only File owner.
        let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(io(std::io::Error::last_os_error()));
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }
    fn open_root(path: &Path) -> Result<File, SnapshotError> {
        if !path.is_absolute() {
            return Err(unknown("upper root must be absolute"));
        }
        let mut directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open("/")
            .map_err(io)?;
        for component in path.components() {
            match component {
                Component::RootDir => {}
                Component::Normal(name) => directory = open_at(&directory, name, true)?,
                _ => return Err(unknown("upper root has a noncanonical component")),
            }
        }
        Ok(directory)
    }
    #[allow(clippy::unnecessary_cast)]
    fn fingerprint(stat: libc::stat) -> Fingerprint {
        Fingerprint {
            dev: stat.st_dev as u64,
            ino: stat.st_ino as u64,
            mode: stat.st_mode as u32,
            size: stat.st_size as i64,
            nlink: stat.st_nlink as u64,
            uid: stat.st_uid,
            gid: stat.st_gid,
            mtime: (stat.st_mtime as i64, stat.st_mtime_nsec as i64),
            ctime: (stat.st_ctime as i64, stat.st_ctime_nsec as i64),
        }
    }
    fn stat_fd(file: &File) -> Result<Fingerprint, SnapshotError> {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: fstat writes the entire stat only on success.
        if unsafe { libc::fstat(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(io(std::io::Error::last_os_error()));
        }
        Ok(fingerprint(unsafe { stat.assume_init() }))
    }
    fn stat_at(parent: &File, name: &str) -> Result<Fingerprint, SnapshotError> {
        let name = c_name(OsStr::new(name))?;
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: parent/name are valid; do not follow a symlink entry.
        if unsafe {
            libc::fstatat(
                parent.as_raw_fd(),
                name.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(io(std::io::Error::last_os_error()));
        }
        Ok(fingerprint(unsafe { stat.assume_init() }))
    }
    struct DirectoryStream(*mut libc::DIR);
    impl Drop for DirectoryStream {
        fn drop(&mut self) {
            unsafe {
                libc::closedir(self.0);
            }
        }
    }
    #[cfg(target_os = "linux")]
    unsafe fn errno() -> *mut libc::c_int {
        unsafe { libc::__errno_location() }
    }
    #[cfg(target_os = "macos")]
    unsafe fn errno() -> *mut libc::c_int {
        unsafe { libc::__error() }
    }
    fn names(parent: &File, remaining: usize) -> Result<Vec<String>, SnapshotError> {
        // Open a fresh description: dup would share the directory offset and
        // make the later verification listing accidentally empty.
        let descriptor = open_at(parent, OsStr::new("."), true)?.into_raw_fd();
        let raw = unsafe { libc::fdopendir(descriptor) };
        if raw.is_null() {
            unsafe {
                libc::close(descriptor);
            }
            return Err(io(std::io::Error::last_os_error()));
        }
        let stream = DirectoryStream(raw);
        let mut out = Vec::new();
        loop {
            // SAFETY: stream is live and this thread exclusively reads it;
            // d_name remains valid until the next readdir call.
            unsafe {
                *errno() = 0;
            }
            let entry = unsafe { libc::readdir(stream.0) };
            if entry.is_null() {
                if unsafe { *errno() } != 0 {
                    return Err(io(std::io::Error::last_os_error()));
                }
                break;
            }
            let bytes = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
            if bytes == b"." || bytes == b".." {
                continue;
            }
            if out.len() >= remaining {
                return Err(budget("upper node budget exceeded"));
            }
            let name = std::str::from_utf8(bytes)
                .map_err(|_| unknown("non-UTF-8 upper filename"))?
                .to_owned();
            super::super::auth::validate_scope(&format!("/{name}"))?;
            out.push(name);
        }
        out.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        if out.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(unknown("duplicate upper directory name"));
        }
        Ok(out)
    }
    fn consume_bytes(
        meters: &mut DiffMeters,
        size: u64,
        limits: DiffLimits,
    ) -> Result<(), SnapshotError> {
        meters.hash_bytes = meters
            .hash_bytes
            .checked_add(size)
            .filter(|total| *total <= limits.max_bytes)
            .ok_or_else(|| budget("upper streaming byte budget exceeded"))?;
        Ok(())
    }
    fn xattr_result(count: libc::ssize_t, meters: &mut DiffMeters) -> Result<(), SnapshotError> {
        meters.xattr_checks += 1;
        if count < 0 {
            return Err(io(std::io::Error::last_os_error()));
        }
        if count > 65_536 {
            return Err(budget("upper xattr name budget exceeded"));
        }
        if count != 0 {
            return Err(SnapshotError::new(
                SnapshotErrorCode::UnsupportedEntry,
                "upper xattrs cannot be represented by the fixed snapshot profile",
            ));
        }
        Ok(())
    }
    fn no_xattrs(file: &File, meters: &mut DiffMeters) -> Result<(), SnapshotError> {
        // A size query proves an empty list without allocating or reading
        // arbitrary attribute values. Nonempty or unsupported is Unknown.
        #[cfg(target_os = "linux")]
        let count = unsafe { libc::flistxattr(file.as_raw_fd(), std::ptr::null_mut(), 0) };
        #[cfg(target_os = "macos")]
        let count = unsafe { libc::flistxattr(file.as_raw_fd(), std::ptr::null_mut(), 0, 0) };
        xattr_result(count, meters)
    }
    fn no_symlink_xattrs(
        parent: &File,
        name: &str,
        before: Fingerprint,
        meters: &mut DiffMeters,
    ) -> Result<(), SnapshotError> {
        #[cfg(target_os = "linux")]
        {
            // llistxattr applies to the final link itself. The parent fd path
            // stays anchored even if a host-side ancestor is renamed.
            let path = CString::new(format!("/proc/self/fd/{}/{name}", parent.as_raw_fd()))
                .map_err(|_| unknown("NUL in anchored symlink path"))?;
            let count = unsafe { libc::llistxattr(path.as_ptr(), std::ptr::null_mut(), 0) };
            xattr_result(count, meters)?;
        }
        #[cfg(target_os = "macos")]
        {
            let name = c_name(OsStr::new(name))?;
            let fd = unsafe {
                libc::openat(
                    parent.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_SYMLINK,
                )
            };
            if fd < 0 {
                return Err(io(std::io::Error::last_os_error()));
            }
            let file = unsafe { File::from_raw_fd(fd) };
            if stat_fd(&file)? != before {
                return Err(unknown("upper symlink replaced before xattr check"));
            }
            no_xattrs(&file, meters)?;
        }
        if stat_at(parent, name)? != before {
            return Err(unknown("upper symlink changed during xattr check"));
        }
        Ok(())
    }
    fn regular(
        parent: &File,
        name: &str,
        before: Fingerprint,
        meters: &mut DiffMeters,
        limits: DiffLimits,
    ) -> Result<UpperNodeIdentity, SnapshotError> {
        let size = u64::try_from(before.size).map_err(|_| unknown("negative upper file size"))?;
        consume_bytes(meters, size, limits)?;
        let mut file = open_at(parent, OsStr::new(name), false)?;
        if stat_fd(&file)? != before {
            return Err(unknown("upper file replaced before hashing"));
        }
        no_xattrs(&file, meters)?;
        let mut context = Context::new(&SHA256);
        let mut buffer = [0u8; BUFFER_BYTES];
        let mut read = 0u64;
        loop {
            let count = file.read(&mut buffer).map_err(io)?;
            if count == 0 {
                break;
            }
            read = read
                .checked_add(count as u64)
                .filter(|total| *total <= size)
                .ok_or_else(|| unknown("upper file grew while hashing"))?;
            context.update(&buffer[..count]);
        }
        if read != size || stat_fd(&file)? != before || stat_at(parent, name)? != before {
            return Err(unknown("upper file changed during hashing"));
        }
        Ok(UpperNodeIdentity::Regular {
            mode: before.mode & 0o7777,
            uid: before.uid,
            gid: before.gid,
            size,
            content_digest: format!("sha256:{}", hex::encode(context.finish().as_ref())),
        })
    }
    fn symlink(
        parent: &File,
        name: &str,
        before: Fingerprint,
        meters: &mut DiffMeters,
        limits: DiffLimits,
    ) -> Result<UpperNodeIdentity, SnapshotError> {
        if !(1..=4095).contains(&before.size) {
            return Err(budget("upper symlink exceeds serving profile"));
        }
        consume_bytes(meters, before.size as u64, limits)?;
        no_symlink_xattrs(parent, name, before, meters)?;
        let name_c = c_name(OsStr::new(name))?;
        let mut bytes = [0u8; 4096];
        // SAFETY: buffer has the requested capacity; readlinkat never follows
        // the target and returns an explicit byte count without a terminator.
        let length = unsafe {
            libc::readlinkat(
                parent.as_raw_fd(),
                name_c.as_ptr(),
                bytes.as_mut_ptr().cast(),
                bytes.len(),
            )
        };
        if length < 0 {
            return Err(io(std::io::Error::last_os_error()));
        }
        if length as i64 != before.size || stat_at(parent, name)? != before {
            return Err(unknown("upper symlink changed during scan"));
        }
        let content_digest = format!(
            "sha256:{}",
            hex::encode(ring::digest::digest(&SHA256, &bytes[..length as usize]).as_ref())
        );
        Ok(UpperNodeIdentity::Symlink {
            mode: before.mode & 0o7777,
            uid: before.uid,
            gid: before.gid,
            size: before.size as u64,
            content_digest,
        })
    }
    #[allow(clippy::unnecessary_cast)]
    fn collect_directory(
        directory: &File,
        path: &str,
        depth: usize,
        scanned: &mut Scanned,
        limits: DiffLimits,
    ) -> Result<(), SnapshotError> {
        if depth > limits.max_depth {
            return Err(budget("upper directory depth budget exceeded"));
        }
        let before = stat_fd(directory)?;
        no_xattrs(directory, &mut scanned.meters)?;
        let children = names(
            directory,
            limits.max_nodes.saturating_sub(scanned.facts.len()),
        )?;
        scanned.facts.get_mut(path).unwrap().children = children.clone();
        for name in children {
            if scanned.facts.len() >= limits.max_nodes {
                return Err(budget("upper node budget exceeded"));
            }
            let child_path = join(path, &name);
            super::super::auth::validate_scope(&format!("/{child_path}"))?;
            let fingerprint = stat_at(directory, &name)?;
            let kind = fingerprint.mode & libc::S_IFMT as u32;
            let mut child_directory = None;
            let identity = if kind == libc::S_IFDIR as u32 {
                let child = open_at(directory, OsStr::new(&name), true)?;
                if stat_fd(&child)? != fingerprint {
                    return Err(unknown("upper directory replaced while opening"));
                }
                child_directory = Some(child);
                UpperNodeIdentity::Directory {
                    mode: fingerprint.mode & 0o7777,
                    uid: fingerprint.uid,
                    gid: fingerprint.gid,
                }
            } else if kind == libc::S_IFREG as u32 {
                regular(directory, &name, fingerprint, &mut scanned.meters, limits)?
            } else if kind == libc::S_IFLNK as u32 {
                symlink(directory, &name, fingerprint, &mut scanned.meters, limits)?
            } else {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::UnsupportedEntry,
                    "unsupported upper node kind",
                ));
            };
            scanned.facts.insert(
                child_path.clone(),
                Fact {
                    identity,
                    children: Vec::new(),
                    fingerprint,
                },
            );
            if let Some(child) = child_directory {
                collect_directory(&child, &child_path, depth + 1, scanned, limits)?;
                if stat_at(directory, &name)? != fingerprint {
                    return Err(unknown("upper directory entry changed during scan"));
                }
            }
        }
        if stat_fd(directory)? != before {
            return Err(unknown("upper directory changed during enumeration"));
        }
        Ok(())
    }
    pub(super) fn collect(path: &Path, limits: DiffLimits) -> Result<Scanned, SnapshotError> {
        let root = open_root(path)?;
        let fingerprint = stat_fd(&root)?;
        let facts = BTreeMap::from([(
            String::new(),
            Fact {
                identity: UpperNodeIdentity::Directory {
                    mode: fingerprint.mode & 0o7777,
                    uid: fingerprint.uid,
                    gid: fingerprint.gid,
                },
                children: Vec::new(),
                fingerprint,
            },
        )]);
        let mut scanned = Scanned {
            facts,
            meters: DiffMeters::default(),
            root: root.try_clone().map_err(io)?,
            root_path: path.to_owned(),
        };
        collect_directory(&root, "", 0, &mut scanned, limits)?;
        scanned.meters.upper_nodes = scanned.facts.len() as u64;
        Ok(scanned)
    }
    fn verify_directory(
        directory: &File,
        path: &str,
        scanned: &Scanned,
    ) -> Result<(), SnapshotError> {
        let fact = &scanned.facts[path];
        if stat_fd(directory)? != fact.fingerprint
            || names(directory, fact.children.len())? != fact.children
        {
            return Err(unknown("upper directory changed before final diff"));
        }
        for name in &fact.children {
            let child_path = join(path, name);
            let child = &scanned.facts[&child_path];
            if stat_at(directory, name)? != child.fingerprint {
                return Err(unknown("upper entry changed before final diff"));
            }
            if matches!(child.identity, UpperNodeIdentity::Directory { .. }) {
                verify_directory(
                    &open_at(directory, OsStr::new(name), true)?,
                    &child_path,
                    scanned,
                )?;
            }
        }
        if stat_fd(directory)? != fact.fingerprint {
            return Err(unknown("upper changed during final verification"));
        }
        Ok(())
    }
    pub(super) fn verify(scanned: &Scanned) -> Result<(), SnapshotError> {
        if stat_fd(&open_root(&scanned.root_path)?)? != scanned.facts[""].fingerprint {
            return Err(unknown("upper root path replaced before final diff"));
        }
        verify_directory(&scanned.root, "", scanned)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn replacement_and_host_write_after_collection_are_unknown() {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("file");
            std::fs::write(&path, b"old").unwrap();
            let scanned =
                collect(&temp.path().canonicalize().unwrap(), DiffLimits::default()).unwrap();
            std::fs::write(&path, b"new").unwrap();
            assert!(verify(&scanned).is_err());
            let scanned =
                collect(&temp.path().canonicalize().unwrap(), DiffLimits::default()).unwrap();
            std::fs::rename(&path, temp.path().join("old-file")).unwrap();
            std::os::unix::fs::symlink("/etc/passwd", &path).unwrap();
            assert!(verify(&scanned).is_err());
        }
        #[test]
        fn sparse_files_stop_at_byte_budget_before_body_read_and_symlink_root_is_rejected() {
            let temp = tempfile::tempdir().unwrap();
            File::create(temp.path().join("sparse"))
                .unwrap()
                .set_len(8 * 1024 * 1024 * 1024)
                .unwrap();
            assert_eq!(
                collect(
                    &temp.path().canonicalize().unwrap(),
                    DiffLimits {
                        max_bytes: 64 * 1024,
                        ..Default::default()
                    }
                )
                .err()
                .unwrap()
                .code,
                SnapshotErrorCode::ProofBudgetExceeded
            );
            let link = temp.path().join("alias");
            std::os::unix::fs::symlink(temp.path(), &link).unwrap();
            assert!(collect(&link, DiffLimits::default()).is_err());
        }

        #[test]
        fn symlink_attribute_check_reads_the_link_without_following_its_target() {
            let temp = tempfile::tempdir().unwrap();
            let target = "/missing-scorpiofs-outside-target";
            std::os::unix::fs::symlink(target, temp.path().join("link")).unwrap();
            let scanned =
                collect(&temp.path().canonicalize().unwrap(), DiffLimits::default()).unwrap();
            assert!(
                matches!(scanned.facts["link"].identity, UpperNodeIdentity::Symlink { size, .. } if size == target.len() as u64)
            );
            assert_eq!(scanned.meters.xattr_checks, 2);
            verify(&scanned).unwrap();
        }
    }
}
