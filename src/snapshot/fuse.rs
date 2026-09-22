//! Read-only FUSE filesystem backed by a fixed MST/2 snapshot view.
//!
//! Minimal, self-contained mount over [`SnapshotReader`]: every name maps
//! through the fixed view (no live-ref following); file content comes from
//! digest-verified blob reads cached in memory. It does not touch the
//! existing Antares overlay layer (read-only slice; write/upper stay there).
//!
//! Symlinks are served with real symlink semantics (spec 07 §1): the view
//! exposes them as `fs_kind = "symlink"` whose content is the target bytes,
//! so `readlink` returns that target and the kernel — not this filesystem —
//! decides how to traverse it. Opening a symlink inode directly is ELOOP.

use std::{
    collections::HashMap,
    ffi::OsStr,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use asyncfuse::{
    raw::{
        prelude::*,
        reply::{DirectoryEntry, DirectoryEntryPlus, ReplyDirectoryPlus},
    },
    Errno, FileType, Inode, Result,
};
use bytes::Bytes;
use futures::stream::iter;

use crate::{
    snapshot::{durable::DurableStore, SnapshotFile, SnapshotReader},
    util::file_attr::make_file_attr,
};

pub(crate) const ROOT_INODE: u64 = 1;
pub(crate) const TTL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(crate) struct DirNode {
    /// Scope-relative path, no leading slash ("" for root).
    #[allow(dead_code)]
    path: String,
    /// child basename -> inode
    children: HashMap<String, u64>,
    /// inode of the parent directory (`..`); the root is its own parent.
    parent: u64,
    /// Lazy mounts: has this directory's page been fetched and its children
    /// created? Eager mounts are born loaded.
    loaded: bool,
    /// The directory's own MTP2 page id (the parent entry commits to it) —
    /// the lazy fetch target. Eager dirs carry it too (cheap, useful).
    page_id: Option<String>,
}

#[derive(Clone)]
pub(crate) struct FileNode {
    path: String,
    fs_kind: String,
    size: u64,
    digest: String,
}

#[derive(Clone)]
pub(crate) enum Node {
    Dir(DirNode),
    File(FileNode),
}

struct State {
    next_inode: u64,
    nodes: HashMap<u64, Node>,
    /// Whole-file content cache (small files, hydrated CAS objects).
    contents: HashMap<u64, Arc<Vec<u8>>>,
    /// Large files opened through the verified chunk reader (range reads).
    chunked: HashMap<u64, Arc<crate::snapshot::range::ChunkedFile>>,
    /// Lazy mounts: directory pages are fetched on first readdir/lookup.
    lazy: bool,
}

/// Kernel file type for one view entry. Symlinks are their own type, not
/// regular files (spec 07 §1); directories are handled by their node.
fn entry_kind(node: &Node) -> FileType {
    match node {
        Node::Dir(_) => FileType::Directory,
        Node::File(f) if f.fs_kind == "symlink" => FileType::Symlink,
        Node::File(_) => FileType::RegularFile,
    }
}

fn is_symlink(node: &Node) -> bool {
    matches!(node, Node::File(f) if f.fs_kind == "symlink")
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
        Self::build(Some(reader), None, manifest)
    }

    /// Build over a reader *and* a durable store: content is served from the
    /// verified local CAS when present (no network on the read path).
    pub async fn from_reader_with_store(
        reader: SnapshotReader,
        store: Arc<DurableStore>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        let manifest = reader.file_manifest().await?;
        Self::build(Some(reader), Some(store), manifest)
    }

    /// Build over a reader, a store and an already-computed manifest (the
    /// incremental sync's result), so the tree is not walked twice.
    pub fn from_manifest(
        reader: SnapshotReader,
        store: Arc<DurableStore>,
        manifest: Vec<SnapshotFile>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        Self::build(Some(reader), Some(store), manifest)
    }

    /// Reopen a completed, pinned hydration with no server contact at all.
    /// The manifest comes from the store, and every read re-verifies against
    /// the digest the view advertised when it was hydrated.
    pub fn from_store(
        store: Arc<DurableStore>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        let manifest = store.manifest()?;
        Self::build(None, Some(store), manifest)
    }

    /// Lazy mount (LAZY-MOUNT-SPEC): the tree starts at the scope root page —
    /// ONE metadata request — and directory pages are fetched on first
    /// readdir/lookup. File content materializes on open through the existing
    /// per-file paths (memory -> CAS -> OBJECT -> chunk ranges). The view is
    /// fixed, so lazily created inodes never go stale.
    pub async fn from_reader_lazy(
        reader: SnapshotReader,
        store: Option<Arc<DurableStore>>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        let root_page_id = reader.descriptor.metadata_root.clone();
        let sid = reader.snapshot_id().to_string();
        let items = [crate::snapshot::frames::MetadataPageItem {
            directory_path: "/".to_string(),
            route: Vec::new(),
            expected_digest: Some(root_page_id.clone()),
        }];
        let pages = reader
            .client
            .metadata_pages(&sid, &items, reader.encoding_hint())
            .await?;
        let root_bytes = pages
            .iter()
            .find(|(pid, _)| {
                format!("sha256:{}", crate::snapshot::frames::hex32(pid)) == root_page_id
            })
            .map(|(_, b)| b.clone())
            .ok_or_else(|| {
                crate::snapshot::SnapshotError::new(
                    crate::snapshot::SnapshotErrorCode::Internal,
                    "metadata/pages did not return the scope root page",
                )
            })?;
        let (page, _) = mst2_codec::metapage::Page::decode(&root_bytes).map_err(|e| {
            crate::snapshot::SnapshotError::new(
                crate::snapshot::SnapshotErrorCode::Internal,
                format!("scope root page decode failed: {e}"),
            )
        })?;

        let mut state = State {
            next_inode: ROOT_INODE,
            nodes: HashMap::new(),
            contents: HashMap::new(),
            chunked: HashMap::new(),
            lazy: true,
        };
        state.nodes.insert(
            ROOT_INODE,
            Node::Dir(DirNode {
                path: String::new(),
                children: HashMap::new(),
                parent: ROOT_INODE,
                loaded: true,
                page_id: Some(root_page_id.clone()),
            }),
        );
        let entries = match &page {
            mst2_codec::metapage::Page::Leaf { entries } => entries,
            mst2_codec::metapage::Page::Branch { .. } => {
                return Err(crate::snapshot::SnapshotError::new(
                    crate::snapshot::SnapshotErrorCode::Internal,
                    "scope root page must be a leaf for the lazy mount root",
                ));
            }
        };
        Self::apply_page_entries(&mut state, ROOT_INODE, "", entries)?;
        Ok(Mst2Fuse {
            reader: Some(reader),
            store,
            state: StdMutex::new(state),
        })
    }

    /// Fetch one directory's MTP2 page tree (the root page plus, for wide
    /// directories, the branch pages the root commits to) and create its
    /// child inodes. The page ids come from the parent entry and the branch
    /// children themselves, so every fetch is bound to what the fixed view
    /// committed to.
    async fn ensure_dir_loaded(
        &self,
        inode: Inode,
    ) -> std::result::Result<(), crate::snapshot::SnapshotError> {
        let (path, page_id) = {
            let state = self.state.lock().unwrap();
            match state.nodes.get(&inode) {
                Some(Node::Dir(d)) if !d.loaded => (d.path.clone(), d.page_id.clone()),
                _ => return Ok(()), // loaded, or not a lazily-fetchable dir
            }
        };
        let reader = self.reader.as_ref().ok_or_else(|| {
            crate::snapshot::SnapshotError::new(
                crate::snapshot::SnapshotErrorCode::Internal,
                "lazy mount requires a live reader",
            )
        })?;
        let page_id = page_id.ok_or_else(|| {
            crate::snapshot::SnapshotError::new(
                crate::snapshot::SnapshotErrorCode::Internal,
                "directory page id missing",
            )
        })?;
        let sid = reader.snapshot_id().to_string();
        let dir_path = format!("/{path}");

        // BFS over the directory's own page tree: root page first, then the
        // branch children each root/branch commits to. Bounded: pages are
        // ≤16KiB and the tree is finite (spec 05 limits).
        let mut all_entries: Vec<mst2_codec::metapage::Entry> = Vec::new();
        let mut routes: Vec<(Vec<u8>, String)> = vec![(Vec::new(), page_id.clone())];
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        seen.insert(page_id.clone());
        while !routes.is_empty() {
            let take = routes.len().min(64);
            let batch: Vec<(Vec<u8>, String)> = routes.drain(..take).collect();
            let items: Vec<crate::snapshot::frames::MetadataPageItem> = batch
                .iter()
                .map(
                    |(route, expected)| crate::snapshot::frames::MetadataPageItem {
                        directory_path: dir_path.clone(),
                        route: route.clone(),
                        expected_digest: Some(expected.clone()),
                    },
                )
                .collect();
            let pages = reader
                .client
                .metadata_pages(&sid, &items, reader.encoding_hint())
                .await
                .map_err(|e| {
                    crate::snapshot::SnapshotError::new(
                        crate::snapshot::SnapshotErrorCode::Internal,
                        format!("lazy page fetch failed: {e}"),
                    )
                })?;
            let mut by_id: HashMap<String, Vec<u8>> = HashMap::new();
            for (pid, bytes) in &pages {
                by_id.insert(
                    format!("sha256:{}", crate::snapshot::frames::hex32(pid)),
                    bytes.clone(),
                );
            }
            for (route, expected) in &batch {
                let bytes = by_id.get(expected).ok_or_else(|| {
                    crate::snapshot::SnapshotError::new(
                        crate::snapshot::SnapshotErrorCode::Internal,
                        "lazy page walk did not return an expected page",
                    )
                })?;
                let (page, _) = mst2_codec::metapage::Page::decode(bytes).map_err(|e| {
                    crate::snapshot::SnapshotError::new(
                        crate::snapshot::SnapshotErrorCode::Internal,
                        format!("lazy page decode failed: {e}"),
                    )
                })?;
                match &page {
                    mst2_codec::metapage::Page::Leaf { entries } => {
                        all_entries.extend(entries.iter().cloned());
                    }
                    mst2_codec::metapage::Page::Branch {
                        terminal, children, ..
                    } => {
                        if let Some(e) = terminal {
                            all_entries.push(e.clone());
                        }
                        for c in children {
                            let mut next = route.clone();
                            next.push(c.label);
                            let child_id = format!(
                                "sha256:{}",
                                crate::snapshot::frames::hex32(&c.child_page_id)
                            );
                            if seen.insert(child_id.clone()) {
                                routes.push((next, child_id));
                            }
                        }
                    }
                }
            }
        }

        let mut state = self.state.lock().unwrap();
        Self::apply_page_entries(&mut state, inode, &path, &all_entries)?;
        if let Node::Dir(d) = state.nodes.get_mut(&inode).expect("inode exists") {
            d.loaded = true;
        }
        Ok(())
    }

    fn apply_page_entries(
        state: &mut State,
        parent_inode: Inode,
        parent_path: &str,
        entries: &[mst2_codec::metapage::Entry],
    ) -> std::result::Result<(), crate::snapshot::SnapshotError> {
        for e in entries {
            let name = std::str::from_utf8(&e.name)
                .map_err(|_| {
                    crate::snapshot::SnapshotError::new(
                        crate::snapshot::SnapshotErrorCode::Internal,
                        "non-utf8 entry name in MTP2 page",
                    )
                })?
                .to_string();
            let full = if parent_path.is_empty() {
                name.clone()
            } else {
                format!("{parent_path}/{name}")
            };
            if let Some(existing) = state
                .nodes
                .get(&parent_inode)
                .and_then(|n| match n {
                    Node::Dir(d) => d.children.get(&name),
                    _ => None,
                })
                .cloned()
            {
                let _ = existing;
                continue; // already created (e.g. concurrent load)
            }
            let inode = state.next_inode + 1;
            state.next_inode = inode;
            let node = match e.kind {
                mst2_codec::metapage::EntryKind::Directory => Node::Dir(DirNode {
                    path: full,
                    children: HashMap::new(),
                    parent: parent_inode,
                    loaded: false,
                    page_id: Some(format!(
                        "sha256:{}",
                        crate::snapshot::frames::hex32(&e.child_root)
                    )),
                }),
                kind => {
                    let fs_kind = match kind {
                        mst2_codec::metapage::EntryKind::Regular => "regular",
                        mst2_codec::metapage::EntryKind::Executable => "executable",
                        mst2_codec::metapage::EntryKind::Symlink => "symlink",
                        mst2_codec::metapage::EntryKind::Directory => unreachable!(),
                    };
                    Node::File(FileNode {
                        path: full,
                        fs_kind: fs_kind.to_string(),
                        size: e.size,
                        digest: format!("sha256:{}", crate::snapshot::frames::hex32(&e.content_id)),
                    })
                }
            };
            state.nodes.insert(inode, node);
            if let Some(Node::Dir(d)) = state.nodes.get_mut(&parent_inode) {
                d.children.insert(name, inode);
            }
        }
        Ok(())
    }

    /// Ensure a directory's children exist before lookup/readdir. No-op for
    /// eager mounts and already-loaded directories.
    async fn ensure_loaded(&self, inode: Inode) -> Result<()> {
        let need = {
            let state = self.state.lock().unwrap();
            state.lazy
                && match state.nodes.get(&inode) {
                    Some(Node::Dir(d)) => !d.loaded,
                    _ => false,
                }
        };
        if need {
            self.ensure_dir_loaded(inode)
                .await
                .map_err(|_| Errno::from(libc::EIO))?;
        }
        Ok(())
    }

    pub(crate) fn build(
        reader: Option<SnapshotReader>,
        store: Option<Arc<DurableStore>>,
        manifest: Vec<SnapshotFile>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        let mut state = State {
            next_inode: ROOT_INODE,
            nodes: HashMap::new(),
            contents: HashMap::new(),
            chunked: HashMap::new(),
            lazy: false,
        };
        state.nodes.insert(
            ROOT_INODE,
            Node::Dir(DirNode {
                path: String::new(),
                children: HashMap::new(),
                parent: ROOT_INODE,
                loaded: true,
                page_id: None,
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
                )?;
            }
        }
        Ok(Mst2Fuse {
            reader,
            store,
            state: StdMutex::new(state),
        })
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
                let kind = state
                    .nodes
                    .get(ino)
                    .map(entry_kind)
                    .unwrap_or(FileType::RegularFile);
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

    pub(crate) fn node(&self, inode: u64) -> Result<Node> {
        self.state
            .lock()
            .unwrap()
            .nodes
            .get(&inode)
            .cloned()
            .ok_or_else(|| Errno::from(libc::ENOENT))
    }
}

type EnsureResult = std::result::Result<u64, crate::snapshot::SnapshotError>;

fn ensure_child(
    state: &mut State,
    parent_inode: u64,
    name: &str,
    is_file: bool,
    file: Option<&SnapshotFile>,
) -> EnsureResult {
    match state.nodes.get(&parent_inode) {
        None => {
            return Err(crate::snapshot::SnapshotError::new(
                crate::snapshot::SnapshotErrorCode::Internal,
                format!("manifest parent inode {parent_inode} missing"),
            ))
        }
        // A manifest where one path is a file and another uses that file as a
        // directory (`a` and `a/b`) is contradictory; that is a server/manifest
        // defect, and panicking the mount thread would take the whole
        // filesystem down. Typed error instead.
        Some(Node::File(_)) => {
            return Err(crate::snapshot::SnapshotError::new(
                crate::snapshot::SnapshotErrorCode::Internal,
                format!("manifest uses file {parent_inode} as a directory (entry {name:?})"),
            ))
        }
        Some(Node::Dir(_)) => {}
    }
    if let Node::Dir(d) = state.nodes.get(&parent_inode).expect("checked above") {
        if let Some(existing) = d.children.get(name) {
            return Ok(*existing);
        }
    }
    let inode = state.next_inode + 1;
    state.next_inode = inode;
    let parent_path = match state.nodes.get(&parent_inode) {
        Some(Node::Dir(d)) => d.path.clone(),
        _ => {
            return Err(crate::snapshot::SnapshotError::new(
                crate::snapshot::SnapshotErrorCode::Internal,
                format!("manifest parent inode {parent_inode} vanished"),
            ))
        }
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
            loaded: true,
            page_id: None,
        })
    };
    state.nodes.insert(inode, node);
    if let Some(Node::Dir(d)) = state.nodes.get_mut(&parent_inode) {
        d.children.insert(name.to_string(), inode);
    }
    Ok(inode)
}

pub(crate) fn dir_attr(inode: u64) -> FileAttr {
    let owner = crate::util::mount_owner::mount_owner();
    make_file_attr(
        inode,
        0,
        0,
        asyncfuse::Timestamp::new(TTL.as_secs() as i64, 0),
        asyncfuse::Timestamp::new(TTL.as_secs() as i64, 0),
        asyncfuse::Timestamp::new(TTL.as_secs() as i64, 0),
        FileType::Directory,
        0o755,
        2,
        owner.uid,
        owner.gid,
        0,
        4096,
    )
}

pub(crate) fn file_attr(inode: u64, f: &FileNode) -> FileAttr {
    let symlink = f.fs_kind == "symlink";
    let owner = crate::util::mount_owner::mount_owner();
    make_file_attr(
        inode,
        // For a symlink this is the target's length, per POSIX.
        f.size,
        (f.size / 512) + 1,
        asyncfuse::Timestamp::new(TTL.as_secs() as i64, 0),
        asyncfuse::Timestamp::new(TTL.as_secs() as i64, 0),
        asyncfuse::Timestamp::new(TTL.as_secs() as i64, 0),
        if symlink {
            FileType::Symlink
        } else {
            FileType::RegularFile
        },
        if symlink {
            0o777
        } else if f.fs_kind == "executable" {
            0o755
        } else {
            0o644
        },
        1,
        owner.uid,
        owner.gid,
        0,
        4096,
    )
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
        self.ensure_loaded(parent).await?;
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
        self.ensure_loaded(inode).await?;
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
        self.ensure_loaded(inode).await?;
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
        let node = self.node(inode)?;
        if is_symlink(&node) {
            // The kernel resolves symlinks itself; opening the link inode
            // directly (e.g. O_NOFOLLOW) is ELOOP, never "serve target text
            // as file content".
            return Err(Errno::from(libc::ELOOP));
        }
        match node {
            Node::File(_) => Ok(ReplyOpen {
                fh: inode,
                flags: 0,
            }),
            Node::Dir(_) => Err(Errno::from(libc::EISDIR)),
        }
    }

    /// Serve `[offset, offset+size)` of a file, fetching only what covers it
    /// (spec 07 §6, spec 11 §6: `open` prepares a handle, `read` starts I/O).
    ///
    /// Order of sources: an already-materialized whole file (small files and
    /// hydrated CAS objects) is sliced; otherwise small files come through
    /// the OBJECT path and large files through the verified chunk reader,
    /// which transfers only the covering chunks.
    async fn read(
        &self,
        _req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        let _ = fh;
        let f = match self.node(inode)? {
            Node::File(f) => f,
            Node::Dir(_) => return Err(Errno::from(libc::EISDIR)),
        };
        if size == 0 || offset >= f.size {
            return Ok(ReplyData { data: Bytes::new() });
        }
        let end = offset.saturating_add(size as u64).min(f.size);

        // 1. Whole content already in memory (verified when it was read).
        if let Some(bytes) = self.state.lock().unwrap().contents.get(&inode).cloned() {
            let start = (offset as usize).min(bytes.len());
            let stop = (end as usize).min(bytes.len());
            return Ok(ReplyData {
                data: Bytes::copy_from_slice(&bytes[start..stop]),
            });
        }

        // 2. Small file: whole content (CAS when hydrated, OBJECT frames
        //    otherwise), cached in memory — a small file's whole bytes are
        //    cheap and repeats are common.
        if f.size <= crate::snapshot::range::OBJECT_CAP {
            if let Some(store) = &self.store {
                if let Ok(bytes) = store.read_blob(&f.digest, f.size) {
                    let arc = Arc::new(bytes);
                    let start = (offset as usize).min(arc.len());
                    let stop = (end as usize).min(arc.len());
                    let out = Bytes::copy_from_slice(&arc[start..stop]);
                    self.state.lock().unwrap().contents.insert(inode, arc);
                    return Ok(ReplyData { data: out });
                }
            }
            let bytes = Arc::new(self.fetch_content(&f).await?);
            let start = (offset as usize).min(bytes.len());
            let stop = (end as usize).min(bytes.len());
            let out = Bytes::copy_from_slice(&bytes[start..stop]);
            self.state.lock().unwrap().contents.insert(inode, bytes);
            return Ok(ReplyData { data: out });
        }

        // 3. Large file: serve the requested range only (spec 07 §6, BODY-12).
        //    A hydrated store is the offline-safe source — a bounded `pread`
        //    that never loads the file whole. The verified chunk reader is the
        //    live-transport path when the CAS does not hold the file.
        if let Some(store) = &self.store {
            if let Some(bytes) = store
                .pread_blob(&f.digest, offset, (end - offset) as usize)
                .map_err(io_err)?
            {
                return Ok(ReplyData {
                    data: Bytes::from(bytes),
                });
            }
        }

        // 4. Large file online: verified chunk reader, transferred range only.
        let reader = self
            .reader
            .as_ref()
            .ok_or_else(|| Errno::from(libc::EIO))?
            .clone();
        let chunked = {
            let cached = self.state.lock().unwrap().chunked.get(&inode).cloned();
            match cached {
                Some(c) => c,
                None => {
                    let path = format!("/{}", f.path);
                    let c = Arc::new(
                        crate::snapshot::range::ChunkedFile::open(
                            &reader, &path, &f.digest, f.size,
                        )
                        .await
                        .map_err(io_err)?,
                    );
                    self.state.lock().unwrap().chunked.insert(inode, c.clone());
                    c
                }
            }
        };
        let data = chunked
            .read_range(offset, end - offset)
            .await
            .map_err(io_err)?;
        Ok(ReplyData {
            data: Bytes::from(data),
        })
    }

    /// Return the symlink target recorded in the fixed view. The content is
    /// the target bytes (git symlink blobs have no NUL terminator), verified
    /// against the digest the view advertised.
    async fn readlink(&self, _req: Request, inode: Inode) -> Result<ReplyData> {
        let node = self.node(inode)?;
        let f = match node {
            Node::File(f) if f.fs_kind == "symlink" => f,
            Node::File(_) => return Err(Errno::from(libc::EINVAL)),
            Node::Dir(_) => return Err(Errno::from(libc::EINVAL)),
        };
        let cached = self.state.lock().unwrap().contents.get(&inode).cloned();
        let target = match cached {
            Some(b) => b,
            None => {
                let arc = Arc::new(self.fetch_content(&f).await?);
                self.state
                    .lock()
                    .unwrap()
                    .contents
                    .insert(inode, arc.clone());
                arc
            }
        };
        Ok(ReplyData {
            data: Bytes::copy_from_slice(target.as_slice()),
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

    // ---- Read-only layer: deny every mutation with EROFS (spec 12 §7).
    //
    // The snapshot view is immutable; writes belong to the upper layer of the
    // overlay. Answering EROFS (not the trait default ENOSYS) keeps the
    // behaviour identical to the Dicfuse lower layer, so the union filesystem
    // and the kernel treat this layer as read-only rather than unsupported.

    async fn setattr(
        &self,
        _req: Request,
        _inode: Inode,
        _fh: Option<u64>,
        _set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        Err(Errno::from(libc::EROFS))
    }

    async fn symlink(
        &self,
        _req: Request,
        _parent: Inode,
        _name: &OsStr,
        _link: &OsStr,
    ) -> Result<ReplyEntry> {
        Err(Errno::from(libc::EROFS))
    }

    async fn mknod(
        &self,
        _req: Request,
        _parent: Inode,
        _name: &OsStr,
        _mode: u32,
        _rdev: u32,
    ) -> Result<ReplyEntry> {
        Err(Errno::from(libc::EROFS))
    }

    async fn mkdir(
        &self,
        _req: Request,
        _parent: Inode,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry> {
        Err(Errno::from(libc::EROFS))
    }

    async fn link(
        &self,
        _req: Request,
        _inode: Inode,
        _new_parent: Inode,
        _new_name: &OsStr,
    ) -> Result<ReplyEntry> {
        Err(Errno::from(libc::EROFS))
    }

    async fn unlink(&self, _req: Request, _parent: Inode, _name: &OsStr) -> Result<()> {
        Err(Errno::from(libc::EROFS))
    }

    async fn rmdir(&self, _req: Request, _parent: Inode, _name: &OsStr) -> Result<()> {
        Err(Errno::from(libc::EROFS))
    }

    async fn rename(
        &self,
        _req: Request,
        _parent: Inode,
        _name: &OsStr,
        _new_parent: Inode,
        _new_name: &OsStr,
    ) -> Result<()> {
        Err(Errno::from(libc::EROFS))
    }

    async fn rename2(
        &self,
        _req: Request,
        _parent: Inode,
        _name: &OsStr,
        _new_parent: Inode,
        _new_name: &OsStr,
        _flags: u32,
    ) -> Result<()> {
        Err(Errno::from(libc::EROFS))
    }

    async fn write(
        &self,
        _req: Request,
        _inode: Inode,
        _fh: u64,
        _offset: u64,
        _data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> Result<ReplyWrite> {
        Err(Errno::from(libc::EROFS))
    }

    async fn create(
        &self,
        _req: Request,
        _parent: Inode,
        _name: &OsStr,
        _mode: u32,
        _flags: u32,
    ) -> Result<ReplyCreated> {
        Err(Errno::from(libc::EROFS))
    }

    async fn fallocate(
        &self,
        _req: Request,
        _inode: Inode,
        _fh: u64,
        _offset: u64,
        _length: u64,
        _mode: u32,
    ) -> Result<()> {
        Err(Errno::from(libc::EROFS))
    }

    #[allow(clippy::too_many_arguments)]
    async fn copy_file_range(
        &self,
        _req: Request,
        _inode: Inode,
        _fh_in: u64,
        _off_in: u64,
        _inode_out: Inode,
        _fh_out: u64,
        _off_out: u64,
        _length: u64,
        _flags: u64,
    ) -> Result<ReplyCopyFileRange> {
        Err(Errno::from(libc::EROFS))
    }

    async fn setxattr(
        &self,
        _req: Request,
        _inode: Inode,
        _name: &OsStr,
        _value: &[u8],
        _flags: u32,
        _position: u32,
    ) -> Result<()> {
        Err(Errno::from(libc::EROFS))
    }

    async fn removexattr(&self, _req: Request, _inode: Inode, _name: &OsStr) -> Result<()> {
        Err(Errno::from(libc::EROFS))
    }
}

fn io_err(e: crate::snapshot::SnapshotError) -> Errno {
    use crate::snapshot::SnapshotErrorCode::*;
    let code = match e.code {
        PathNotFound | ViewNotFound | SnapshotGone => libc::ENOENT,
        Unauthenticated | ScopeForbidden | LeaseExpired | LeaseUnknown => libc::EACCES,
        NotDirectory => libc::ENOTDIR,
        DigestMismatch => libc::EIO,
        _ => libc::EIO,
    };
    Errno::from(code)
}
