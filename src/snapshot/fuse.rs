//! Read-only FUSE filesystem backed by a fixed MST/2 snapshot view.
//!
//! Minimal, self-contained mount over [`SnapshotReader`]: every name maps
//! through the fixed view (no live-ref following); file content comes from
//! digest-verified blob reads cached in memory. A workspace OverlayFs uses
//! this view as its read-only lower layer; writes stay in its private upper.
//!
//! Symlinks are served with real symlink semantics (spec 07 §1): the view
//! exposes them as `fs_kind = "symlink"` whose content is the target bytes,
//! so `readlink` returns that target and the kernel — not this filesystem —
//! decides how to traverse it. Opening a symlink inode directly is ELOOP.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
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
    snapshot::{
        closure::{verify_directory_pages, ValidatedSnapshotClosure},
        durable::DurableStore,
        fuse_owned::{ContentEntry, OwnedFuseCache, RangeEntry, ReplyAdmission},
        FileMembershipError, MetadataProofLimits, OwnedChunkedFile, ProvenSnapshotFile,
        SnapshotDirectoryEntry, SnapshotError, SnapshotErrorCode, SnapshotFile,
        SnapshotNodeIdentity, SnapshotPathState, SnapshotReader,
    },
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
    /// Modern online mounts retain paid payload/reply owners in fixed slots.
    owned: Option<OwnedFuseCache>,
    /// Lazy mounts: directory pages are fetched on first readdir/lookup.
    lazy: bool,
    /// None for legacy file-only manifests, which cannot prove namespace absence.
    namespace_scope: Option<String>,
}

fn owned_cache(
    reader: Option<&SnapshotReader>,
    store: Option<&Arc<DurableStore>>,
) -> std::result::Result<Option<OwnedFuseCache>, crate::snapshot::SnapshotError> {
    reader
        .filter(|reader| store.is_none() && reader.capabilities().features.metadata_pages)
        .map(OwnedFuseCache::new)
        .transpose()
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
    metadata_limits: MetadataProofLimits,
}

impl Mst2Fuse {
    /// Build the FUSE view eagerly from the full file manifest. The view is
    /// fixed, so the inode tree never goes stale.
    pub async fn from_reader(
        reader: SnapshotReader,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        if reader.capabilities().features.metadata_pages {
            let closure = reader.snapshot_closure().await?;
            reader.seed_content_membership(&closure)?;
            return Self::build_snapshot_closure(Some(reader), None, &closure);
        }
        let manifest = reader.file_manifest().await?;
        Self::build(Some(reader), None, manifest)
    }

    /// Build over a reader *and* a durable store: content is served from the
    /// verified local CAS when present (no network on the read path).
    pub async fn from_reader_with_store(
        reader: SnapshotReader,
        store: Arc<DurableStore>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        store.bind_reader(&reader)?;
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
        store.bind_reader(&reader)?;
        for file in &manifest {
            reader
                .authorized_context()
                .validate_relative_path(&file.rel_path)?;
        }
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

    /// Build from a verified complete snapshot closure without walking its
    /// pages again. Explicit directories preserve empty directories and
    /// give each alias path its own inode.
    pub fn from_snapshot_manifest(
        reader: SnapshotReader,
        store: Arc<DurableStore>,
        closure: ValidatedSnapshotClosure,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        store.bind_reader(&reader)?;
        closure.matches_descriptor(reader.descriptor())?;
        for directory in closure.directories() {
            reader
                .authorized_context()
                .validate_relative_path(&directory.rel_path)?;
        }
        for file in closure.files() {
            reader
                .authorized_context()
                .validate_relative_path(&file.rel_path)?;
        }
        reader.seed_content_membership(&closure)?;
        Self::build_snapshot_closure(Some(reader), Some(store), &closure)
    }

    /// Reopen the store's complete snapshot closure using only verified local
    /// metadata and content. This constructor does not grant offline access
    /// authority; the caller must establish any required local access policy.
    /// Legacy file-only completion is accepted only by [`Self::from_store`].
    pub fn from_snapshot_store(
        store: Arc<DurableStore>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        let closure = store.snapshot_manifest()?;
        Self::build_snapshot_closure(None, Some(store), &closure)
    }

    /// Reopen a complete snapshot without contacting the service, but only
    /// when the caller presents the exact server-issued grant and local actor
    /// domain that were committed with the store.
    pub fn from_snapshot_store_with_grant(
        store: Arc<DurableStore>,
        grant: &crate::snapshot::OfflineGrant,
        actor_domain_id: &str,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        store.validate_offline_grant(grant, actor_domain_id)?;
        let closure = store.snapshot_manifest()?;
        Self::build_snapshot_closure(None, Some(store), &closure)
    }

    /// Lazy mount: the tree starts at the scope root's verified page tree,
    /// and child directory pages are fetched on first
    /// readdir/lookup. File content materializes on open through the existing
    /// per-file paths (memory -> CAS -> OBJECT -> chunk ranges). The view is
    /// fixed, so lazily created inodes never go stale.
    pub async fn from_reader_lazy(
        reader: SnapshotReader,
        store: Option<Arc<DurableStore>>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        Self::from_reader_lazy_with_limits(reader, store, MetadataProofLimits::default()).await
    }

    /// Same fixed lazy view, with explicit local namespace-proof bounds.
    pub async fn from_reader_lazy_with_limits(
        reader: SnapshotReader,
        store: Option<Arc<DurableStore>>,
        metadata_limits: MetadataProofLimits,
    ) -> std::result::Result<Self, SnapshotError> {
        if metadata_limits.max_directory_pages == 0
            || metadata_limits.max_directory_entries == 0
            || metadata_limits.max_cached_nodes == 0
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::InvalidRequest,
                "metadata proof bounds must be nonzero",
            ));
        }
        if let Some(store) = &store {
            store.bind_reader(&reader)?;
        }
        let root_page_id = reader.descriptor().metadata_root.clone();
        let mut state = State {
            next_inode: ROOT_INODE,
            nodes: HashMap::new(),
            contents: HashMap::new(),
            chunked: HashMap::new(),
            owned: owned_cache(Some(&reader), store.as_ref())?,
            lazy: true,
            namespace_scope: Some(reader.descriptor().scope.clone()),
        };
        state.nodes.insert(
            ROOT_INODE,
            Node::Dir(DirNode {
                path: String::new(),
                children: HashMap::new(),
                parent: ROOT_INODE,
                loaded: false,
                page_id: Some(root_page_id.clone()),
            }),
        );
        let view = Mst2Fuse {
            reader: Some(reader),
            store,
            state: StdMutex::new(state),
            metadata_limits,
        };
        view.ensure_dir_loaded(ROOT_INODE).await?;
        Ok(view)
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
        reader.ensure_lease().await?;
        let sid = reader.snapshot_id().to_string();
        let dir_path = format!("/{path}");

        reader.authorized_context().validate_relative_path(&path)?;
        // Fetch only this directory's radix dependencies. Hashes alone do
        // not certify namespace absence: the complete canonical partition
        // and every child's actual count are proved before publishing nodes.
        let mut proof_pages = BTreeMap::new();
        let mut routes: Vec<(Vec<u8>, String)> = vec![(Vec::new(), page_id.clone())];
        let mut route_ids = HashMap::new();
        let mut seen = HashSet::new();
        seen.insert(page_id.clone());
        while !routes.is_empty() {
            let take = routes
                .len()
                .min(64)
                .min(reader.client.metadata_item_limit());
            let batch: Vec<(Vec<u8>, String)> = routes.drain(..take).collect();
            for (route, expected) in &batch {
                route_ids.insert(route.clone(), expected.clone());
            }
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
            reader.ensure_lease().await?;
            let pages = reader
                .client
                .metadata_pages(&sid, &items, reader.encoding_hint())
                .await?;
            let mut allowed = HashSet::new();
            for (route, _) in &batch {
                for depth in 0..=route.len() {
                    if let Some(id) = route_ids.get(&route[..depth]) {
                        allowed.insert(id.clone());
                    }
                }
            }
            for (pid, bytes) in pages {
                let id = format!("sha256:{}", crate::snapshot::frames::hex32(&pid));
                if !allowed.contains(&id) || mst2_codec::metapage::page_id(&bytes) != pid {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        "lazy directory returned an uncommitted or corrupt radix page",
                    ));
                }
                proof_pages.insert(id, bytes);
            }
            for (route, expected) in &batch {
                let bytes = proof_pages.get(expected).ok_or_else(|| {
                    SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        "lazy page walk did not return an expected page",
                    )
                })?;
                let page = crate::snapshot::closure::decode_page(bytes)?;
                match &page {
                    mst2_codec::metapage::Page::Leaf { .. } => {}
                    mst2_codec::metapage::Page::Branch { children, .. } => {
                        for c in children {
                            let mut next = route.clone();
                            next.push(c.label);
                            if next.len() > mst2_codec::metapage::MAX_DEPTH {
                                return Err(SnapshotError::new(
                                    SnapshotErrorCode::LimitExceeded,
                                    "metadata radix depth exceeds 255",
                                ));
                            }
                            let child_id = format!(
                                "sha256:{}",
                                crate::snapshot::frames::hex32(&c.child_page_id)
                            );
                            if seen.insert(child_id.clone()) {
                                if seen.len() > self.metadata_limits.max_directory_pages {
                                    return Err(SnapshotError::new(
                                        SnapshotErrorCode::ProofBudgetExceeded,
                                        "lazy directory radix page budget exceeded",
                                    ));
                                }
                                routes.push((next, child_id));
                            }
                        }
                    }
                }
            }
        }

        let all_entries = verify_directory_pages(
            &page_id,
            &proof_pages,
            self.metadata_limits.max_directory_entries,
        )?;
        let max_file_bytes = match reader.capability_advertisement() {
            crate::snapshot::capabilities::CapabilityAdvertisement::Canonical(caps) => {
                caps.limits().max_file_bytes
            }
            _ => 8 * 1024 * 1024 * 1024 * 1024,
        };
        for entry in all_entries.iter() {
            let name = std::str::from_utf8(&entry.name).map_err(|_| {
                SnapshotError::new(SnapshotErrorCode::IntegrityError, "non-UTF-8 MTP2 name")
            })?;
            let full = if path.is_empty() {
                name.to_owned()
            } else {
                format!("{path}/{name}")
            };
            reader.authorized_context().validate_relative_path(&full)?;
            reader.client.validate_path(&full)?;
            if entry.size > max_file_bytes
                || (entry.kind == mst2_codec::metapage::EntryKind::Symlink
                    && !(1..=4095).contains(&entry.size))
            {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::LimitExceeded,
                    "lazy directory file size exceeds serving profile",
                ));
            }
        }

        let mut state = self.state.lock().unwrap();
        if matches!(state.nodes.get(&inode), Some(Node::Dir(d)) if d.loaded) {
            return Ok(());
        }
        if state
            .nodes
            .len()
            .checked_add(all_entries.len())
            .is_none_or(|count| count > self.metadata_limits.max_cached_nodes)
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ProofBudgetExceeded,
                "lazy namespace node budget exceeded",
            ));
        }
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
            owned: owned_cache(reader.as_ref(), store.as_ref())?,
            lazy: false,
            namespace_scope: None,
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
            metadata_limits: MetadataProofLimits::default(),
        })
    }

    fn build_snapshot_closure(
        reader: Option<SnapshotReader>,
        store: Option<Arc<DurableStore>>,
        closure: &ValidatedSnapshotClosure,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        use crate::snapshot::{SnapshotError, SnapshotErrorCode};

        let invalid =
            |message: String| SnapshotError::new(SnapshotErrorCode::IntegrityError, message);
        let mut state = State {
            next_inode: ROOT_INODE,
            nodes: HashMap::new(),
            contents: HashMap::new(),
            chunked: HashMap::new(),
            owned: owned_cache(reader.as_ref(), store.as_ref())?,
            lazy: false,
            namespace_scope: Some(closure.descriptor().scope.clone()),
        };
        let mut directories: Vec<_> = closure.directories().iter().collect();
        directories.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        let mut directory_inodes: HashMap<&str, u64> = HashMap::new();
        for directory in directories {
            let path = directory.rel_path.as_str();
            if directory_inodes.contains_key(path) {
                return Err(invalid(format!("duplicate snapshot directory {path:?}")));
            }
            let (inode, parent, name) = if path.is_empty() {
                (ROOT_INODE, ROOT_INODE, "")
            } else {
                let (parent_path, name) = path.rsplit_once('/').unwrap_or(("", path));
                let parent = *directory_inodes.get(parent_path).ok_or_else(|| {
                    invalid(format!("snapshot directory parent {parent_path:?} missing"))
                })?;
                state.next_inode += 1;
                (state.next_inode, parent, name)
            };
            state.nodes.insert(
                inode,
                Node::Dir(DirNode {
                    path: path.to_string(),
                    children: HashMap::new(),
                    parent,
                    loaded: true,
                    page_id: Some(directory.directory_root.clone()),
                }),
            );
            if !path.is_empty() {
                let Some(Node::Dir(parent_dir)) = state.nodes.get_mut(&parent) else {
                    return Err(invalid(format!("snapshot parent inode {parent} missing")));
                };
                if parent_dir
                    .children
                    .insert(name.to_string(), inode)
                    .is_some()
                {
                    return Err(invalid(format!("duplicate snapshot entry {path:?}")));
                }
            }
            directory_inodes.insert(path, inode);
        }
        if !directory_inodes.contains_key("") {
            return Err(invalid("snapshot root directory missing".to_string()));
        }
        for file in closure.files() {
            let path = file.rel_path.as_str();
            let (parent_path, name) = path.rsplit_once('/').unwrap_or(("", path));
            let parent = *directory_inodes
                .get(parent_path)
                .ok_or_else(|| invalid(format!("snapshot file parent {parent_path:?} missing")))?;
            let Some(Node::Dir(parent_dir)) = state.nodes.get_mut(&parent) else {
                return Err(invalid(format!("snapshot parent inode {parent} missing")));
            };
            if parent_dir.children.contains_key(name) {
                return Err(invalid(format!("duplicate snapshot entry {path:?}")));
            }
            state.next_inode += 1;
            let inode = state.next_inode;
            parent_dir.children.insert(name.to_string(), inode);
            state.nodes.insert(
                inode,
                Node::File(FileNode {
                    path: path.to_string(),
                    fs_kind: file.fs_kind.clone(),
                    size: file.size,
                    digest: file.content_digest.clone(),
                }),
            );
        }
        Ok(Mst2Fuse {
            reader,
            store,
            state: StdMutex::new(state),
            metadata_limits: MetadataProofLimits::default(),
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

    fn owned_reader(&self) -> Option<&SnapshotReader> {
        self.reader
            .as_ref()
            .filter(|reader| self.store.is_none() && reader.capabilities().features.metadata_pages)
    }

    async fn proven_node(
        reader: &SnapshotReader,
        node: &FileNode,
        cached: Option<Arc<ProvenSnapshotFile>>,
    ) -> Result<Arc<ProvenSnapshotFile>> {
        let proven = match cached {
            Some(proven) => proven,
            None => reader
                .prove_file(&node.path)
                .await
                .map_err(membership_io_err)?,
        };
        proven.validate(reader).await.map_err(io_err)?;
        let file = proven.file();
        if file.rel_path != node.path
            || file.fs_kind != node.fs_kind
            || file.size != node.size
            || file.content_digest != node.digest
        {
            return Err(Errno::from(libc::EIO));
        }
        Ok(proven)
    }

    /// The modern branch runs before legacy empty/EOF/cache paths. It cannot
    /// fall back after a proof, authority, quota or body validation failure.
    async fn read_owned(
        &self,
        reader: &SnapshotReader,
        inode: Inode,
        node: &FileNode,
        offset: u64,
        size: u64,
    ) -> Result<ReplyData> {
        let (content, range) = {
            let mut state = self.state.lock().unwrap();
            let cache = state.owned.as_mut().ok_or_else(|| Errno::from(libc::EIO))?;
            if node.size <= crate::snapshot::OBJECT_CAP {
                (cache.contents.get(inode), None)
            } else {
                (None, cache.ranges.get(inode))
            }
        };
        let cached = content
            .as_ref()
            .map(|entry| entry.proven.clone())
            .or_else(|| range.as_ref().map(|entry| entry.proven.clone()));
        let proven = Self::proven_node(reader, node, cached).await?;
        if size == 0 || offset >= node.size {
            return Ok(ReplyData { data: Bytes::new() });
        }
        let end = offset.saturating_add(size).min(node.size);
        let admission = ReplyAdmission::new(reader).map_err(io_err)?;
        if node.size <= crate::snapshot::OBJECT_CAP {
            let owner = match content {
                Some(entry) => entry.content,
                None => reader
                    .read_proven_content(&proven, reader.capabilities().features.objects)
                    .await
                    .map_err(io_err)?,
            };
            if owner.len() as u64 != node.size {
                return Err(Errno::from(libc::EIO));
            }
            proven.validate(reader).await.map_err(io_err)?;
            let data = admission
                .content(owner.clone(), offset as usize, end as usize)
                .map_err(io_err)?;
            self.state
                .lock()
                .unwrap()
                .owned
                .as_mut()
                .unwrap()
                .contents
                .insert(ContentEntry {
                    inode,
                    proven,
                    content: owner,
                });
            Ok(ReplyData { data })
        } else {
            let chunked = match range {
                Some(entry) => entry.range,
                None => Arc::new(
                    OwnedChunkedFile::open_proven(reader, proven.clone())
                        .await
                        .map_err(io_err)?,
                ),
            };
            let owner = chunked
                .read_range_owned(offset, end - offset)
                .await
                .map_err(io_err)?;
            if owner.len() as u64 != end - offset {
                return Err(Errno::from(libc::EIO));
            }
            proven.validate(reader).await.map_err(io_err)?;
            let data = admission.range(owner).map_err(io_err)?;
            self.state
                .lock()
                .unwrap()
                .owned
                .as_mut()
                .unwrap()
                .ranges
                .insert(RangeEntry {
                    inode,
                    proven,
                    range: chunked,
                });
            Ok(ReplyData { data })
        }
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

    /// Query fixed metadata without reading content or following a symlink.
    /// A missing child is proven only after its parent's entire canonical
    /// radix is verified. Legacy file-only manifests cannot prove namespace.
    pub async fn path_state(
        &self,
        rel_path: &str,
    ) -> std::result::Result<SnapshotPathState, SnapshotError> {
        match self.path_inode(rel_path).await? {
            Some(inode) => {
                let state = self.state.lock().unwrap();
                let node = state.nodes.get(&inode).ok_or_else(|| {
                    SnapshotError::new(
                        SnapshotErrorCode::IntegrityError,
                        "fixed metadata inode missing",
                    )
                })?;
                Ok(SnapshotPathState::Present(node_identity(node)?))
            }
            None => Ok(SnapshotPathState::AbsentProven),
        }
    }

    /// Complete, ordered immediate children. Logical child directories are
    /// not expanded; opaque upper diff can inspect only metadata it needs.
    pub async fn directory_entries(
        &self,
        rel_path: &str,
    ) -> std::result::Result<Vec<SnapshotDirectoryEntry>, SnapshotError> {
        let inode = self.path_inode(rel_path).await?.ok_or_else(|| {
            SnapshotError::new(SnapshotErrorCode::PathNotFound, "fixed directory is absent")
        })?;
        self.ensure_dir_loaded(inode).await?;
        let state = self.state.lock().unwrap();
        let directory = match state.nodes.get(&inode) {
            Some(Node::Dir(directory)) => directory,
            Some(Node::File(file)) if file.fs_kind == "symlink" => {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::SymlinkTraversal,
                    "directory query would follow a symlink",
                ));
            }
            _ => {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::NotDirectory,
                    "fixed path is not a directory",
                ))
            }
        };
        let mut entries = Vec::with_capacity(directory.children.len());
        for (name, inode) in &directory.children {
            let node = state.nodes.get(inode).ok_or_else(|| {
                SnapshotError::new(
                    SnapshotErrorCode::IntegrityError,
                    "fixed directory child missing",
                )
            })?;
            entries.push(SnapshotDirectoryEntry {
                name: name.clone(),
                identity: node_identity(node)?,
            });
        }
        entries.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        Ok(entries)
    }

    /// Validate a scope-relative metadata name without fetching its parent.
    /// Derived absence under an upper replacement still obeys these bounds.
    pub fn validate_metadata_path(&self, rel_path: &str) -> std::result::Result<(), SnapshotError> {
        self.metadata_path(rel_path).map(|_| ())
    }

    fn metadata_path(&self, rel_path: &str) -> std::result::Result<String, SnapshotError> {
        let relative = if rel_path.is_empty() {
            "/".to_owned()
        } else if rel_path.starts_with('/') {
            rel_path.to_owned()
        } else {
            format!("/{rel_path}")
        };
        crate::snapshot::auth::validate_scope(&relative)?;
        if let Some(reader) = &self.reader {
            reader
                .authorized_context()
                .validate_relative_path(&relative)?;
            reader.client.validate_path(&relative)?;
        }
        let scope = self
            .state
            .lock()
            .unwrap()
            .namespace_scope
            .clone()
            .ok_or_else(|| {
                SnapshotError::new(
                    SnapshotErrorCode::SnapshotNotReady,
                    "file-only manifest cannot prove fixed namespace",
                )
            })?;
        let full = if scope == "/" {
            relative.clone()
        } else if relative == "/" {
            scope
        } else {
            format!("{scope}{relative}")
        };
        crate::snapshot::auth::validate_scope(&full)?;
        Ok(relative)
    }

    async fn path_inode(&self, rel_path: &str) -> std::result::Result<Option<u64>, SnapshotError> {
        let relative = self.metadata_path(rel_path)?;
        let mut inode = ROOT_INODE;
        if relative == "/" {
            return Ok(Some(inode));
        }
        for name in relative.trim_start_matches('/').split('/') {
            self.ensure_dir_loaded(inode).await?;
            let state = self.state.lock().unwrap();
            match state.nodes.get(&inode) {
                Some(Node::Dir(directory)) => match directory.children.get(name) {
                    Some(child) => inode = *child,
                    None => return Ok(None),
                },
                Some(Node::File(file)) if file.fs_kind == "symlink" => {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::SymlinkTraversal,
                        "fixed path traverses a symlink",
                    ));
                }
                _ => {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::NotDirectory,
                        "fixed ancestor is not a directory",
                    ))
                }
            }
        }
        Ok(Some(inode))
    }
}

fn node_identity(node: &Node) -> std::result::Result<SnapshotNodeIdentity, SnapshotError> {
    match node {
        Node::Dir(directory) => Ok(SnapshotNodeIdentity::Directory {
            directory_root: directory.page_id.clone().ok_or_else(|| {
                SnapshotError::new(
                    SnapshotErrorCode::SnapshotNotReady,
                    "directory identity was not proved",
                )
            })?,
        }),
        Node::File(file) => match file.fs_kind.as_str() {
            "regular" => Ok(SnapshotNodeIdentity::Regular {
                size: file.size,
                content_digest: file.digest.clone(),
            }),
            "executable" => Ok(SnapshotNodeIdentity::Executable {
                size: file.size,
                content_digest: file.digest.clone(),
            }),
            "symlink" => Ok(SnapshotNodeIdentity::Symlink {
                size: file.size,
                content_digest: file.digest.clone(),
            }),
            _ => Err(SnapshotError::new(
                SnapshotErrorCode::UnsupportedEntry,
                "unsupported fixed node kind",
            )),
        },
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
        asyncfuse::Timestamp::new(0, 0),
        asyncfuse::Timestamp::new(0, 0),
        asyncfuse::Timestamp::new(0, 0),
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
        f.size.div_ceil(512),
        asyncfuse::Timestamp::new(0, 0),
        asyncfuse::Timestamp::new(0, 0),
        asyncfuse::Timestamp::new(0, 0),
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
        self.ensure_loaded(inode).await?;
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

    async fn open(&self, _req: Request, inode: Inode, flags: u32) -> Result<ReplyOpen> {
        let node = self.node(inode)?;
        if is_symlink(&node) {
            // The kernel resolves symlinks itself; opening the link inode
            // directly (e.g. O_NOFOLLOW) is ELOOP, never "serve target text
            // as file content".
            return Err(Errno::from(libc::ELOOP));
        }
        if flags & libc::O_ACCMODE as u32 != libc::O_RDONLY as u32
            || flags & (libc::O_TRUNC | libc::O_APPEND | libc::O_CREAT | libc::O_EXCL) as u32 != 0
        {
            return Err(Errno::from(libc::EROFS));
        }
        match node {
            Node::File(_) => Ok(ReplyOpen {
                fh: inode,
                flags: 0,
            }),
            Node::Dir(_) => Err(Errno::from(libc::EISDIR)),
        }
    }

    async fn release(
        &self,
        _req: Request,
        _inode: Inode,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<()> {
        // Handles are inode numbers, with no per-open resources to close.
        // Copy-up releases its lower read handle before opening the upper.
        // Returning the trait default ENOSYS here would escape through OPEN,
        // making the kernel skip later opens, including atomic O_TRUNC.
        Ok(())
    }

    async fn fsync(&self, _req: Request, inode: Inode, _fh: u64, _datasync: bool) -> Result<()> {
        self.node(inode)?;
        Ok(())
    }

    /// Serve `[offset, offset+size)` of a file (spec 07 §6, spec 11 §6:
    /// `open` prepares a handle, `read` starts I/O).
    ///
    /// Verified bytes in memory are sliced. Small files come from verified
    /// local CAS or OBJECT requests. Large local CAS files verify covering chunks after a cold full scan
    /// and hashed with bounded memory, returning the requested range from
    /// those same buffers; large online reads transfer only covering chunks.
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
        if let Some(reader) = self.owned_reader() {
            return self
                .read_owned(reader, inode, &f, offset, size as u64)
                .await;
        }
        if size == 0 || offset >= f.size {
            return Ok(ReplyData { data: Bytes::new() });
        }
        let end = offset.saturating_add(size as u64).min(f.size);

        // 1. Whole content already in memory (verified when it was read).
        if let Some(bytes) = self.state.lock().unwrap().contents.get(&inode).cloned() {
            return Ok(ReplyData {
                data: verified_slice(&bytes, f.size, offset, end)?,
            });
        }

        // 2. Small file: whole content (CAS when hydrated, OBJECT frames
        //    otherwise), cached in memory — a small file's whole bytes are
        //    cheap and repeats are common.
        if f.size <= crate::snapshot::range::OBJECT_CAP {
            if let Some(store) = &self.store {
                if let Ok(bytes) = store.read_blob(&f.digest, f.size) {
                    let arc = Arc::new(bytes);
                    let out = verified_slice(&arc, f.size, offset, end)?;
                    self.state.lock().unwrap().contents.insert(inode, arc);
                    return Ok(ReplyData { data: out });
                }
            }
            let bytes = Arc::new(self.fetch_content(&f).await?);
            let out = verified_slice(&bytes, f.size, offset, end)?;
            self.state.lock().unwrap().contents.insert(inode, bytes);
            return Ok(ReplyData { data: out });
        }

        // 3. Large file: serve the requested range only (spec 07 §6, BODY-12).
        //    Local CAS builds private chunk facts with a cold whole scan,
        //    then verifies complete covering chunks from the returned buffers.
        //    Uncovered mutations are detected when read or by a strict audit.
        //    The verified chunk reader is the live-transport path when the
        //    CAS does not hold the file.
        if let Some(store) = &self.store {
            if let Some(bytes) = store
                .read_indexed_blob_range(&f.digest, f.size, offset, (end - offset) as usize)
                .map_err(io_err)?
            {
                if bytes.len() as u64 != end - offset {
                    return Err(Errno::from(libc::EIO));
                }
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
        if data.len() as u64 != end - offset {
            return Err(Errno::from(libc::EIO));
        }
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
        if let Some(reader) = self.owned_reader() {
            return self.read_owned(reader, inode, &f, 0, f.size).await;
        }
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
    // overlay. Answering EROFS (not the trait default ENOSYS) lets the union
    // filesystem and kernel treat this layer as read-only rather than unsupported.

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

fn verified_slice(bytes: &[u8], file_size: u64, offset: u64, end: u64) -> Result<Bytes> {
    if bytes.len() as u64 != file_size {
        return Err(Errno::from(libc::EIO));
    }
    let start = usize::try_from(offset).map_err(|_| Errno::from(libc::EIO))?;
    let stop = usize::try_from(end).map_err(|_| Errno::from(libc::EIO))?;
    bytes
        .get(start..stop)
        .map(Bytes::copy_from_slice)
        .ok_or_else(|| Errno::from(libc::EIO))
}

fn io_err(e: crate::snapshot::SnapshotError) -> Errno {
    use crate::snapshot::SnapshotErrorCode::*;
    let code = match e.code {
        PathNotFound => libc::ENOENT,
        Unauthenticated | ScopeForbidden | LeaseExpired | LeaseUnknown => libc::EACCES,
        NotDirectory => libc::ENOTDIR,
        DigestMismatch => libc::EIO,
        _ => libc::EIO,
    };
    Errno::from(code)
}

fn membership_io_err(error: FileMembershipError) -> Errno {
    match error {
        FileMembershipError::NotFile { .. } => Errno::from(libc::EIO),
        FileMembershipError::Snapshot(error) => io_err(error),
    }
}

#[cfg(test)]
#[path = "fuse_owned_tests.rs"]
mod owned_tests;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures::StreamExt;
    use mst2_codec::{
        descriptor::ServingDescriptor,
        metapage::{page_id, Entry, EntryKind, Page},
    };

    use super::*;

    fn insert_page(pages: &mut BTreeMap<String, Vec<u8>>, entries: &[Entry]) -> [u8; 32] {
        let bytes = Page::build(entries).unwrap();
        let id = page_id(&bytes);
        pages.insert(format!("sha256:{}", hex::encode(id)), bytes);
        id
    }

    fn closure_for_pages(
        root: [u8; 32],
        pages: BTreeMap<String, Vec<u8>>,
    ) -> ValidatedSnapshotClosure {
        let descriptor = ServingDescriptor {
            instance_uuid: *uuid::Uuid::parse_str("11111111-2222-4333-8444-555555555555")
                .unwrap()
                .as_bytes(),
            namespace_view_id: [0x22; 32],
            scope: "/project".into(),
            metadata_root: root,
        };
        ValidatedSnapshotClosure::from_canonical_pages(&descriptor.encode().unwrap(), pages)
            .unwrap()
    }

    fn directory_closure() -> ValidatedSnapshotClosure {
        let mut pages = BTreeMap::new();
        let empty = insert_page(&mut pages, &[]);
        let content = |kind, name: &[u8], bytes: &[u8]| {
            Entry::file(
                kind,
                name,
                bytes.len() as u64,
                crate::snapshot::frames::parse_digest(&crate::snapshot::durable::digest_of(bytes))
                    .unwrap(),
            )
        };
        let shared = insert_page(
            &mut pages,
            &[
                content(EntryKind::Executable, b"exec", b"#!/bin/sh\n"),
                content(EntryKind::Symlink, b"link", b"plain"),
                Entry::dir(b"nested", empty),
                content(EntryKind::Regular, b"plain", b"data"),
            ],
        );
        let root = insert_page(
            &mut pages,
            &[
                Entry::dir(b"empty", empty),
                Entry::dir(b"left", shared),
                Entry::dir(b"right", shared),
            ],
        );
        closure_for_pages(root, pages)
    }

    #[tokio::test]
    async fn typed_namespace_rejects_file_only_proofs_and_keeps_empty_directory_identity() {
        let legacy = Mst2Fuse::build(None, None, vec![]).unwrap();
        assert_eq!(
            legacy.path_state("missing").await.unwrap_err().code,
            SnapshotErrorCode::SnapshotNotReady
        );
        assert_eq!(
            legacy.path_state("../escape").await.unwrap_err().code,
            SnapshotErrorCode::ScopeInvalid
        );
        let closure = directory_closure();
        let complete = Mst2Fuse::build_snapshot_closure(None, None, &closure).unwrap();
        assert!(matches!(
            complete.path_state("empty").await.unwrap(),
            SnapshotPathState::Present(SnapshotNodeIdentity::Directory { .. })
        ));
        assert_eq!(
            complete.path_state("empty/missing").await.unwrap(),
            SnapshotPathState::AbsentProven
        );
        assert_eq!(
            complete
                .path_state("left/link/child")
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::SymlinkTraversal
        );
        assert_eq!(
            complete
                .path_state("left/plain/child")
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::NotDirectory
        );
    }

    async fn directory_names(fs: &Mst2Fuse, inode: u64) -> Vec<String> {
        fs.readdir(Request::default(), inode, inode, 0)
            .await
            .unwrap()
            .entries
            .map(|entry| entry.unwrap().name.to_string_lossy().into_owned())
            .collect()
            .await
    }

    #[tokio::test]
    async fn complete_snapshot_preserves_an_empty_scope_root() {
        let mut pages = BTreeMap::new();
        let root = insert_page(&mut pages, &[]);
        let closure = closure_for_pages(root, pages);
        assert!(closure.files().is_empty());
        let fs = Mst2Fuse::build_snapshot_closure(None, None, &closure).unwrap();
        assert_eq!(directory_names(&fs, ROOT_INODE).await, [".", ".."]);
        let attr = fs
            .getattr(Request::default(), ROOT_INODE, None, 0)
            .await
            .unwrap()
            .attr;
        assert_eq!(attr.kind, FileType::Directory);
        let Node::Dir(directory) = fs.node(ROOT_INODE).unwrap() else {
            panic!("scope root is not a directory");
        };
        assert_eq!(directory.parent, ROOT_INODE);
        assert_eq!(
            directory.page_id.as_deref(),
            Some(closure.descriptor().metadata_root.as_str())
        );
    }

    #[tokio::test]
    async fn complete_snapshot_preserves_empty_directories_and_alias_inodes() {
        let closure = directory_closure();
        let fs = Mst2Fuse::build_snapshot_closure(None, None, &closure).unwrap();
        let req = Request::default();
        assert_eq!(
            directory_names(&fs, ROOT_INODE).await,
            [".", "..", "empty", "left", "right"]
        );
        let empty = fs
            .lookup(req, ROOT_INODE, OsStr::new("empty"))
            .await
            .unwrap();
        assert_eq!(empty.attr.kind, FileType::Directory);
        assert_eq!(directory_names(&fs, empty.attr.ino).await, [".", ".."]);
        let left = fs
            .lookup(req, ROOT_INODE, OsStr::new("left"))
            .await
            .unwrap()
            .attr
            .ino;
        let right = fs
            .lookup(req, ROOT_INODE, OsStr::new("right"))
            .await
            .unwrap()
            .attr
            .ino;
        assert_ne!(left, right);
        let left_nested = fs
            .lookup(req, left, OsStr::new("nested"))
            .await
            .unwrap()
            .attr
            .ino;
        let right_nested = fs
            .lookup(req, right, OsStr::new("nested"))
            .await
            .unwrap()
            .attr
            .ino;
        assert_ne!(left_nested, right_nested);
        for (inode, parent, path) in [
            (left_nested, left, "left/nested"),
            (right_nested, right, "right/nested"),
        ] {
            assert_eq!(directory_names(&fs, inode).await, [".", ".."]);
            let entries = fs
                .readdir(req, inode, inode, 0)
                .await
                .unwrap()
                .entries
                .collect::<Vec<_>>()
                .await;
            assert_eq!(entries[1].as_ref().unwrap().inode, parent);
            let Node::Dir(directory) = fs.node(inode).unwrap() else {
                panic!("nested empty directory became a file");
            };
            assert_eq!(directory.path, path);
            let expected = closure
                .directories()
                .iter()
                .find(|directory| directory.rel_path == path)
                .unwrap();
            assert_eq!(
                directory.page_id.as_deref(),
                Some(expected.directory_root.as_str())
            );
        }
        let left_plain = fs
            .lookup(req, left, OsStr::new("plain"))
            .await
            .unwrap()
            .attr
            .ino;
        let right_plain = fs
            .lookup(req, right, OsStr::new("plain"))
            .await
            .unwrap()
            .attr
            .ino;
        assert_ne!(left_plain, right_plain);
        assert_eq!(
            fs.path_state("left/plain").await.unwrap(),
            SnapshotPathState::Present(SnapshotNodeIdentity::Regular {
                size: 4,
                content_digest: crate::snapshot::durable::digest_of(b"data"),
            })
        );
        assert_eq!(
            fs.path_state("right/plain").await.unwrap(),
            SnapshotPathState::Present(SnapshotNodeIdentity::Regular {
                size: 4,
                content_digest: crate::snapshot::durable::digest_of(b"data"),
            })
        );
    }

    #[tokio::test]
    async fn complete_snapshot_preserves_executable_and_symlink_semantics() {
        let closure = directory_closure();
        let fs = Mst2Fuse::build_snapshot_closure(None, None, &closure).unwrap();
        let req = Request::default();
        let left = fs
            .lookup(req, ROOT_INODE, OsStr::new("left"))
            .await
            .unwrap()
            .attr
            .ino;
        for (name, kind, permissions) in [
            ("plain", FileType::RegularFile, 0o644),
            ("exec", FileType::RegularFile, 0o755),
            ("link", FileType::Symlink, 0o777),
        ] {
            let attr = fs.lookup(req, left, OsStr::new(name)).await.unwrap().attr;
            assert_eq!(attr.kind, kind);
            assert_eq!(attr.perm, permissions);
        }
        let link = fs
            .lookup(req, left, OsStr::new("link"))
            .await
            .unwrap()
            .attr
            .ino;
        assert_eq!(
            fs.path_state("left/link").await.unwrap(),
            SnapshotPathState::Present(SnapshotNodeIdentity::Symlink {
                size: 5,
                content_digest: crate::snapshot::durable::digest_of(b"plain"),
            })
        );
        let error = fs.open(req, link, libc::O_RDONLY as u32).await.unwrap_err();
        assert_eq!(i32::from(error), -libc::ELOOP);
        fs.state
            .lock()
            .unwrap()
            .contents
            .insert(link, Arc::new(b"plain".to_vec()));
        assert_eq!(
            fs.readlink(req, link).await.unwrap().data.as_ref(),
            b"plain"
        );
        let listing = fs
            .readdir(req, left, left, 0)
            .await
            .unwrap()
            .entries
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            listing.iter().find_map(|entry| {
                let entry = entry.as_ref().unwrap();
                (entry.name.as_os_str() == OsStr::new("link")).then_some(entry.kind)
            }),
            Some(FileType::Symlink)
        );
    }

    fn file_view(size: u64) -> Mst2Fuse {
        Mst2Fuse::build(
            None,
            None,
            vec![SnapshotFile {
                rel_path: "file".into(),
                fs_kind: "regular".into(),
                size,
                content_digest: crate::snapshot::durable::digest_of(b"data"),
            }],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn lower_open_rejects_write_flags_without_fetching_content() {
        let fs = file_view(4);
        let req = Request::default();
        let inode = fs
            .lookup(req, ROOT_INODE, OsStr::new("file"))
            .await
            .unwrap()
            .attr
            .ino;
        fs.open(req, inode, libc::O_RDONLY as u32).await.unwrap();
        for flags in [
            libc::O_WRONLY,
            libc::O_RDWR,
            libc::O_TRUNC,
            libc::O_RDONLY | libc::O_APPEND,
            libc::O_RDONLY | libc::O_CREAT,
        ] {
            let error = fs.open(req, inode, flags as u32).await.unwrap_err();
            assert_eq!(i32::from(error), -libc::EROFS, "flags {flags}");
        }
        assert!(fs.state.lock().unwrap().contents.is_empty());
        fs.fsync(req, inode, inode, false).await.unwrap();
    }

    #[tokio::test]
    async fn lower_attributes_use_exact_blocks_and_fixed_times() {
        for (size, blocks) in [(0, 0), (1, 1), (511, 1), (512, 1), (513, 2)] {
            let fs = file_view(size);
            let entry = fs
                .lookup(Request::default(), ROOT_INODE, OsStr::new("file"))
                .await
                .unwrap();
            assert_eq!(entry.attr.size, size);
            assert_eq!(entry.attr.blocks, blocks);
            assert_eq!(entry.attr.atime, asyncfuse::Timestamp::new(0, 0));
            assert_eq!(entry.attr.mtime, asyncfuse::Timestamp::new(0, 0));
            assert_eq!(entry.attr.ctime, asyncfuse::Timestamp::new(0, 0));
        }
    }

    #[tokio::test]
    async fn cached_short_content_is_eio_and_true_eof_is_empty() {
        let fs = file_view(4);
        let req = Request::default();
        let inode = fs
            .lookup(req, ROOT_INODE, OsStr::new("file"))
            .await
            .unwrap()
            .attr
            .ino;
        fs.state
            .lock()
            .unwrap()
            .contents
            .insert(inode, Arc::new(b"dat".to_vec()));
        let error = fs.read(req, inode, inode, 0, 4).await.unwrap_err();
        assert_eq!(i32::from(error), -libc::EIO);
        for (offset, size) in [(4, 10), (u64::MAX, u32::MAX), (0, 0)] {
            assert!(fs
                .read(req, inode, inode, offset, size)
                .await
                .unwrap()
                .data
                .is_empty());
        }
        fs.state
            .lock()
            .unwrap()
            .contents
            .insert(inode, Arc::new(b"data".to_vec()));
        assert_eq!(
            fs.read(req, inode, inode, 3, u32::MAX)
                .await
                .unwrap()
                .data
                .as_ref(),
            b"a"
        );
    }

    #[tokio::test]
    async fn local_large_ranges_verify_cold_file_and_warm_covering_chunks() {
        // Invoke filesystem operations directly, without a mount or an
        // offline authorization claim.
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(DurableStore::open(temp.path()).unwrap());
        let body = vec![0x51; 2 * 1024 * 1024 + 7];
        let digest = crate::snapshot::durable::digest_of(&body);
        let path = store
            .content_dir()
            .join(digest.strip_prefix("sha256:").unwrap());
        std::fs::write(&path, &body).unwrap();
        let fs = Mst2Fuse::build(
            None,
            Some(store),
            vec![SnapshotFile {
                rel_path: "large".into(),
                fs_kind: "regular".into(),
                size: body.len() as u64,
                content_digest: digest,
            }],
        )
        .unwrap();
        let req = Request::default();
        let inode = fs
            .lookup(req, ROOT_INODE, OsStr::new("large"))
            .await
            .unwrap()
            .attr
            .ino;
        // Cold construction must reject an unread bad tail and expose no
        // fact. Restore the body and retry before exercising warm semantics.
        let mut corrupt = body.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        std::fs::write(&path, &corrupt).unwrap();
        assert_eq!(
            i32::from(fs.read(req, inode, inode, 0, 13).await.unwrap_err()),
            -libc::EIO
        );
        std::fs::write(&path, &body).unwrap();
        assert_eq!(
            fs.read(req, inode, inode, 0, 13)
                .await
                .unwrap()
                .data
                .as_ref(),
            &body[..13]
        );
        std::fs::write(&path, &corrupt).unwrap();
        assert_eq!(
            fs.read(req, inode, inode, 0, 13)
                .await
                .unwrap()
                .data
                .as_ref(),
            &body[..13]
        );
        assert_eq!(
            i32::from(
                fs.read(req, inode, inode, body.len() as u64 - 5, 13)
                    .await
                    .unwrap_err()
            ),
            -libc::EIO
        );
        for corrupt_at in [4, 1024 * 1024 - 1] {
            let mut corrupt = body.clone();
            corrupt[corrupt_at] ^= 1;
            std::fs::write(&path, corrupt).unwrap();
            let error = fs.read(req, inode, inode, 0, 13).await.unwrap_err();
            assert_eq!(i32::from(error), -libc::EIO);
        }
        std::fs::write(&path, &body).unwrap();
        assert_eq!(
            fs.read(req, inode, inode, body.len() as u64 - 5, 13)
                .await
                .unwrap()
                .data
                .as_ref(),
            &body[body.len() - 5..]
        );
    }

    #[test]
    fn unavailable_view_does_not_become_a_negative_path_entry() {
        use crate::snapshot::{SnapshotError, SnapshotErrorCode};
        for code in [
            SnapshotErrorCode::ViewNotFound,
            SnapshotErrorCode::SnapshotGone,
        ] {
            assert_eq!(
                i32::from(io_err(SnapshotError::new(code, "unavailable"))),
                -libc::EIO
            );
        }
        assert_eq!(
            i32::from(io_err(SnapshotError::new(
                SnapshotErrorCode::PathNotFound,
                "absent"
            ))),
            -libc::ENOENT
        );
    }
}
