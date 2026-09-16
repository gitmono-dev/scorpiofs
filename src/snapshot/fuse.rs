//! Read-only FUSE filesystem backed by a fixed MST/2 snapshot view.
//!
//! Minimal, self-contained mount over [`SnapshotReader`]: every name maps
//! through the fixed view (no live-ref following); file content comes from
//! digest-verified blob reads cached in memory. It does not touch the
//! existing Antares overlay layer (read-only slice; write/upper stay there).

use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use asyncfuse::raw::prelude::*;
use asyncfuse::raw::reply::{DirectoryEntry, DirectoryEntryPlus, ReplyDirectoryPlus};
use asyncfuse::{Errno, FileType, Inode, Result};
use bytes::Bytes;
use futures::stream::iter;

use crate::snapshot::durable::DurableStore;
use crate::snapshot::{SnapshotFile, SnapshotReader};

const ROOT_INODE: u64 = 1;
const TTL: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct DirNode {
    /// Scope-relative path, no leading slash ("" for root).
    #[allow(dead_code)]
    path: String,
    /// child basename -> inode
    children: HashMap<String, u64>,
    /// inode of the parent directory (`..`); the root is its own parent.
    parent: u64,
}

#[derive(Clone)]
struct FileNode {
    path: String,
    fs_kind: String,
    size: u64,
    digest: String,
}

#[derive(Clone)]
enum Node {
    Dir(DirNode),
    File(FileNode),
}

struct State {
    next_inode: u64,
    nodes: HashMap<u64, Node>,
    contents: HashMap<u64, Arc<Vec<u8>>>,
}

/// One directory entry as the kernel sees it, before it is split into the
/// `readdir` and `readdirplus` reply shapes.
struct ListEntry {
    inode: u64,
    kind: FileType,
    name: String,
    offset: i64,
}

/// Read-only FUSE filesystem over one resolved snapshot.
pub struct Mst2Fuse {
    reader: Option<SnapshotReader>,
    store: Option<Arc<DurableStore>>,
    state: StdMutex<State>,
}

impl Mst2Fuse {
    /// Build the FUSE view eagerly from the full file manifest. The view is
    /// fixed, so the inode tree never goes stale.
    pub async fn from_reader(
        reader: SnapshotReader,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        let manifest = reader.file_manifest().await?;
        Ok(Self::build(Some(reader), None, manifest))
    }

    /// Build over a reader *and* a durable store: content is served from the
    /// verified local CAS when present (no network on the read path).
    pub async fn from_reader_with_store(
        reader: SnapshotReader,
        store: Arc<DurableStore>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        let manifest = reader.file_manifest().await?;
        Ok(Self::build(Some(reader), Some(store), manifest))
    }

    /// Reopen a completed, pinned hydration with no server contact at all.
    /// The manifest comes from the store, and every read re-verifies against
    /// the digest the view advertised when it was hydrated.
    pub fn from_store(
        store: Arc<DurableStore>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        let manifest = store.manifest()?;
        Ok(Self::build(None, Some(store), manifest))
    }

    fn build(
        reader: Option<SnapshotReader>,
        store: Option<Arc<DurableStore>>,
        manifest: Vec<SnapshotFile>,
    ) -> Self {
        let mut state = State {
            next_inode: ROOT_INODE,
            nodes: HashMap::new(),
            contents: HashMap::new(),
        };
        state.nodes.insert(
            ROOT_INODE,
            Node::Dir(DirNode {
                path: String::new(),
                children: HashMap::new(),
                parent: ROOT_INODE,
            }),
        );
        for f in manifest {
            let parts: Vec<&str> = f.rel_path.split('/').filter(|s| !s.is_empty()).collect();
            let mut parent = ROOT_INODE;
            for (depth, part) in parts.iter().enumerate() {
                let is_file = depth + 1 == parts.len();
                parent = ensure_child(
                    &mut state,
                    parent,
                    part,
                    is_file,
                    if is_file { Some(&f) } else { None },
                );
            }
        }
        Mst2Fuse {
            reader,
            store,
            state: StdMutex::new(state),
        }
    }

    /// The snapshot this mount is pinned to, when resolved from a live view.
    pub fn snapshot_id(&self) -> Option<&str> {
        self.reader.as_ref().map(|r| r.snapshot_id())
    }

    /// One directory listing from `offset` on, in the offset convention both
    /// `readdir` and `readdirplus` use: entry `n` carries the offset of entry
    /// `n + 1`, so the kernel resumes without duplicates.
    fn listing(&self, inode: Inode, offset: i64) -> Result<Vec<ListEntry>> {
        let state = self.state.lock().unwrap();
        let d = match state.nodes.get(&inode) {
            Some(Node::Dir(d)) => d,
            _ => return Err(Errno::from(libc::ENOTDIR)),
        };
        let mut children: Vec<(u64, FileType, String)> = d
            .children
            .iter()
            .map(|(name, ino)| {
                let kind = match state.nodes.get(ino) {
                    Some(Node::Dir(_)) => FileType::Directory,
                    _ => FileType::RegularFile,
                };
                (*ino, kind, name.clone())
            })
            .collect();
        children.sort_by(|a, b| a.2.cmp(&b.2));
        let parent = d.parent;

        let mut out = Vec::new();
        if offset < 1 {
            out.push(ListEntry {
                inode,
                kind: FileType::Directory,
                name: ".".into(),
                offset: 1,
            });
        }
        if offset < 2 {
            out.push(ListEntry {
                inode: parent,
                kind: FileType::Directory,
                name: "..".into(),
                offset: 2,
            });
        }
        for (i, (ino, kind, name)) in children.into_iter().enumerate() {
            let off = (i + 3) as i64;
            if off > offset {
                out.push(ListEntry {
                    inode: ino,
                    kind,
                    name,
                    offset: off,
                });
            }
        }
        Ok(out)
    }

    /// Content for one file: the verified local CAS when it holds it, the
    /// live reader otherwise. Both paths end in a SHA-256 check against the
    /// digest the fixed view advertised.
    async fn fetch_content(&self, f: &FileNode) -> Result<Vec<u8>> {
        if let Some(store) = &self.store {
            match store.read_blob(&f.digest, f.size) {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    if self.reader.is_none() {
                        return Err(io_err(e));
                    }
                }
            }
        }
        let reader = self.reader.as_ref().ok_or_else(|| Errno::from(libc::EIO))?;
        reader.read_file(&f.path, &f.digest).await.map_err(io_err)
    }

    fn node(&self, inode: u64) -> Result<Node> {
        self.state
            .lock()
            .unwrap()
            .nodes
            .get(&inode)
            .cloned()
            .ok_or_else(|| Errno::from(libc::ENOENT))
    }
}

fn ensure_child(
    state: &mut State,
    parent_inode: u64,
    name: &str,
    is_file: bool,
    file: Option<&SnapshotFile>,
) -> u64 {
    if let Node::Dir(d) = state.nodes.get(&parent_inode).expect("parent exists") {
        if let Some(existing) = d.children.get(name) {
            return *existing;
        }
    }
    let inode = state.next_inode + 1;
    state.next_inode = inode;
    let parent_path = match state.nodes.get(&parent_inode).expect("parent exists") {
        Node::Dir(d) => d.path.clone(),
        Node::File(_) => panic!("file cannot be a parent"),
    };
    let full = if parent_path.is_empty() {
        name.to_string()
    } else {
        format!("{parent_path}/{name}")
    };
    let node = if is_file {
        let f = file.expect("file node needs manifest entry");
        Node::File(FileNode {
            path: full,
            fs_kind: f.fs_kind.clone(),
            size: f.size,
            digest: f.content_digest.clone(),
        })
    } else {
        Node::Dir(DirNode {
            path: full,
            children: HashMap::new(),
            parent: parent_inode,
        })
    };
    state.nodes.insert(inode, node);
    if let Node::Dir(d) = state.nodes.get_mut(&parent_inode).expect("parent exists") {
        d.children.insert(name.to_string(), inode);
    }
    inode
}

fn dir_attr(inode: u64) -> FileAttr {
    FileAttr {
        ino: inode,
        size: 0,
        blocks: 0,
        atime: asyncfuse::Timestamp::new(TTL.as_secs() as i64, 0),
        mtime: asyncfuse::Timestamp::new(TTL.as_secs() as i64, 0),
        ctime: asyncfuse::Timestamp::new(TTL.as_secs() as i64, 0),
        kind: FileType::Directory,
        perm: 0o755,
        nlink: 2,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 4096,
    }
}

fn file_attr(inode: u64, f: &FileNode) -> FileAttr {
    FileAttr {
        ino: inode,
        size: f.size,
        blocks: (f.size / 512) + 1,
        atime: asyncfuse::Timestamp::new(TTL.as_secs() as i64, 0),
        mtime: asyncfuse::Timestamp::new(TTL.as_secs() as i64, 0),
        ctime: asyncfuse::Timestamp::new(TTL.as_secs() as i64, 0),
        kind: FileType::RegularFile,
        perm: if f.fs_kind == "executable" {
            0o755
        } else {
            0o644
        },
        nlink: 1,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 4096,
    }
}

impl Filesystem for Mst2Fuse {
    async fn init(&self, _req: Request) -> Result<ReplyInit> {
        Ok(ReplyInit {
            max_write: std::num::NonZeroU32::new(128 * 1024).unwrap(),
        })
    }

    async fn destroy(&self, _req: Request) {}

    async fn forget(&self, _req: Request, _inode: Inode, _nlookup: u64) {}

    async fn getattr(
        &self,
        _req: Request,
        inode: Inode,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        let node = self.node(inode)?;
        let attr = match &node {
            Node::Dir(_) => dir_attr(inode),
            Node::File(f) => file_attr(inode, f),
        };
        Ok(ReplyAttr { attr, ttl: TTL })
    }

    async fn lookup(&self, _req: Request, parent: Inode, name: &OsStr) -> Result<ReplyEntry> {
        let name = name.to_string_lossy();
        let child = {
            let state = self.state.lock().unwrap();
            match state.nodes.get(&parent) {
                Some(Node::Dir(d)) => d.children.get(name.as_ref()).copied(),
                _ => None,
            }
        };
        let inode = child.ok_or_else(|| Errno::from(libc::ENOENT))?;
        let node = self.node(inode)?;
        let attr = match &node {
            Node::Dir(_) => dir_attr(inode),
            Node::File(f) => file_attr(inode, f),
        };
        Ok(ReplyEntry {
            attr,
            ttl: TTL,
            generation: 0,
        })
    }

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<impl futures::Stream<Item = Result<DirectoryEntry>> + Send + 'a>>
    {
        let listing = self.listing(inode, offset)?;
        let entries: Vec<Result<DirectoryEntry>> = listing
            .into_iter()
            .map(|e| {
                Ok(DirectoryEntry {
                    inode: e.inode,
                    kind: e.kind,
                    name: e.name.into(),
                    offset: e.offset,
                })
            })
            .collect();
        Ok(ReplyDirectory {
            entries: iter(entries),
        })
    }

    /// The kernel negotiates `READDIRPLUS` whenever it is offered, so this is
    /// the path `ls`/`find` actually take. Leaving it at the trait default
    /// (ENOSYS) makes every directory listing fail with "not implemented".
    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<
        ReplyDirectoryPlus<impl futures::Stream<Item = Result<DirectoryEntryPlus>> + Send + 'a>,
    > {
        let listing = self.listing(inode, offset as i64)?;
        let mut entries: Vec<Result<DirectoryEntryPlus>> = Vec::with_capacity(listing.len());
        for e in listing {
            // "." and ".." are the directory itself; children resolve through
            // the same inode table, so attributes come from one place.
            let node = self.node(e.inode)?;
            let attr = match &node {
                Node::Dir(_) => dir_attr(e.inode),
                Node::File(f) => file_attr(e.inode, f),
            };
            entries.push(Ok(DirectoryEntryPlus {
                inode: e.inode,
                generation: 0,
                kind: e.kind,
                name: e.name.into(),
                offset: e.offset,
                attr,
                entry_ttl: TTL,
                attr_ttl: TTL,
            }));
        }
        Ok(ReplyDirectoryPlus {
            entries: iter(entries),
        })
    }

    async fn opendir(&self, _req: Request, inode: Inode, _flags: u32) -> Result<ReplyOpen> {
        // Handle needed only so the kernel's directory-open round trip
        // succeeds; the read-only tree needs no per-handle state.
        match self.node(inode)? {
            Node::Dir(_) => Ok(ReplyOpen {
                fh: inode,
                flags: 0,
            }),
            Node::File(_) => Err(Errno::from(libc::ENOTDIR)),
        }
    }

    async fn releasedir(&self, _req: Request, _inode: Inode, _fh: u64, _flags: u32) -> Result<()> {
        Ok(())
    }

    async fn statfs(&self, _req: Request, _inode: Inode) -> Result<ReplyStatFs> {
        // Read-only view: report the snapshot's shape, not a device's usage.
        let state = self.state.lock().unwrap();
        let files = state
            .nodes
            .values()
            .filter(|n| matches!(n, Node::File(_)))
            .count() as u64;
        Ok(ReplyStatFs {
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files,
            ffree: 0,
            bsize: 4096,
            namelen: 255,
            frsize: 4096,
        })
    }

    async fn open(&self, _req: Request, inode: Inode, _flags: u32) -> Result<ReplyOpen> {
        let _ = _flags;
        let f = match self.node(inode)? {
            Node::File(f) => f,
            Node::Dir(_) => return Err(Errno::from(libc::EISDIR)),
        };
        {
            let state = self.state.lock().unwrap();
            if state.contents.contains_key(&inode) {
                return Ok(ReplyOpen {
                    fh: inode,
                    flags: 0,
                });
            }
        }
        let bytes = self.fetch_content(&f).await?;
        self.state
            .lock()
            .unwrap()
            .contents
            .insert(inode, Arc::new(bytes));
        Ok(ReplyOpen {
            fh: inode,
            flags: 0,
        })
    }

    async fn read(
        &self,
        _req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        let bytes = {
            let state = self.state.lock().unwrap();
            state
                .contents
                .get(&fh)
                .cloned()
                .or_else(|| state.contents.get(&inode).cloned())
                .ok_or_else(|| Errno::from(libc::EBADF))?
        };
        let start = (offset as usize).min(bytes.len());
        let end = (start + size as usize).min(bytes.len());
        Ok(ReplyData {
            data: Bytes::copy_from_slice(&bytes[start..end]),
        })
    }

    async fn getlk(
        &self,
        _req: Request,
        _inode: Inode,
        _fh: u64,
        _lock_owner: u64,
        start: u64,
        end: u64,
        _type: u32,
        _pid: u32,
    ) -> Result<ReplyLock> {
        Ok(ReplyLock {
            start,
            end,
            r#type: libc::F_UNLCK as u32,
            pid: 0,
        })
    }

    async fn setlk(
        &self,
        _req: Request,
        _inode: Inode,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _type: u32,
        _pid: u32,
        _block: bool,
    ) -> Result<()> {
        Err(Errno::from(libc::EROFS))
    }
}

fn io_err(e: crate::snapshot::SnapshotError) -> Errno {
    use crate::snapshot::SnapshotErrorCode::*;
    let code = match e.code {
        PathNotFound | ViewNotFound | SnapshotUnknown => libc::ENOENT,
        ScopeForbidden | LeaseExpired | LeaseUnknown => libc::EACCES,
        NotDirectory => libc::ENOTDIR,
        DigestMismatch => libc::EIO,
        _ => libc::EIO,
    };
    Errno::from(code)
}
