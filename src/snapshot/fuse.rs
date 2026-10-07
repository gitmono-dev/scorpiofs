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
        cas_content::VerifiedCasContent,
        cas_range::VerifiedCasRange,
        cas_worker::{CasReadScope, LocalCasAccess, RequestMeters, WorkResult},
        closure::{verify_directory_pages, ValidatedSnapshotClosure},
        content::ContentBudget,
        durable::{DurableStore, LocalCasRangeMeters},
        fuse_owned::{
            ContentEntry, OnlineContentEntry, OnlineFuseCache, OnlineRangeEntry, OwnedFuseCache,
            RangeEntry, ReplyAdmission, StoreRangeCache,
        },
        fuse_store::{ContentKey, StoreContent, StoreSmallCache},
        online_file::OnlineSnapshotFile,
        ContentBudgetLimits, FileMembershipError, MetadataProofLimits, OwnedChunkedFile,
        ProvenSnapshotFile, ScopeCache, SnapshotDirectoryEntry, SnapshotError, SnapshotErrorCode,
        SnapshotFile, SnapshotNodeIdentity, SnapshotPathState, SnapshotReader,
    },
    util::{file_attr::make_file_attr, mutation_fence::MutationPause},
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
    online_file: Option<Arc<OnlineSnapshotFile>>,
}

#[derive(Clone)]
pub(crate) enum Node {
    Dir(DirNode),
    File(FileNode),
}

struct State {
    next_inode: u64,
    nodes: HashMap<u64, Node>,
    /// Modern online mounts retain paid payload/reply owners in fixed slots.
    owned: Option<OwnedFuseCache>,
    online: Option<OnlineFuseCache>,
    /// Stored online/local mounts retain paid content independently of page hints.
    store_small: Option<StoreSmallCache>,
    store_ranges: Option<StoreRangeCache>,
    /// Lazy mounts: directory pages are fetched on first readdir/lookup.
    lazy: bool,
    /// None for legacy file-only manifests, which cannot prove namespace absence.
    namespace_scope: Option<String>,
}

struct LocalCasScope {
    budget: Arc<ContentBudget>,
    workers: Arc<CasReadScope>,
    access: LocalCasAccess,
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

fn stored_caches(
    reader: Option<&SnapshotReader>,
    store: Option<&Arc<DurableStore>>,
) -> std::result::Result<
    (Option<StoreSmallCache>, Option<StoreRangeCache>),
    crate::snapshot::SnapshotError,
> {
    let Some(reader) =
        reader.filter(|reader| store.is_some() && reader.capabilities().features.metadata_pages)
    else {
        return Ok((None, None));
    };
    Ok((
        Some(StoreSmallCache::new(&reader.content_scope)?),
        Some(StoreRangeCache::new(reader)?),
    ))
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
    /// Hints only, in the real owned workspace's authorized scope directory.
    scope_pages: Option<ScopeCache>,
    /// Retained for all reads of one local mount; no reader or wire fallback.
    local_cas: Option<LocalCasScope>,
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
        Self::build(Some(reader), None, manifest)?.with_online_paths()
    }

    /// Build over a reader *and* a durable store: content is served from the
    /// verified local CAS when present (no network on the read path).
    pub async fn from_reader_with_store(
        reader: SnapshotReader,
        store: Arc<DurableStore>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        store.bind_reader(&reader)?;
        if reader.capabilities().features.metadata_pages {
            let closure = reader.snapshot_closure().await?;
            return Self::from_snapshot_manifest(reader, store, closure);
        }
        let manifest = reader.file_manifest().await?;
        Self::build(Some(reader), Some(store), manifest)?.with_online_paths()
    }

    /// Reopen a completed, pinned hydration with no server contact at all.
    /// The caller establishes local access policy. Cold reads verify against
    /// the fixed digest; paid immutable owners can serve repeated small reads.
    pub fn from_store(
        store: Arc<DurableStore>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        let manifest = store.manifest()?;
        Self::build(None, Some(store), manifest)?.with_local_cas(LocalCasAccess::CallerEstablished)
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
        Self::build_snapshot_closure(None, Some(store), &closure)?
            .with_local_cas(LocalCasAccess::CallerEstablished)
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
        Self::build_snapshot_closure(None, Some(store), &closure)?
            .with_local_cas(LocalCasAccess::GrantCheckedOnReopen)
    }

    // Only the validated public local constructors enable this lane. A raw
    // metadata fixture does not acquire local authority by containing a store.
    fn with_local_cas(
        mut self,
        access: LocalCasAccess,
    ) -> std::result::Result<Self, SnapshotError> {
        if self.reader.is_some() || self.store.is_none() {
            return Err(SnapshotError::new(
                SnapshotErrorCode::Internal,
                "local CAS scope requires a readerless stored mount",
            ));
        }
        let budget = ContentBudget::new(ContentBudgetLimits::default());
        let small = StoreSmallCache::new(&budget)?;
        let workers = budget.cas_workers();
        self.state.get_mut().unwrap().store_small = Some(small);
        self.local_cas = Some(LocalCasScope {
            budget,
            workers,
            access,
        });
        Ok(self)
    }

    // Called only after a public constructor's completed fixed-SID JSON
    // manifest walk. Raw metadata builders cannot mint this online binding.
    fn with_online_paths(mut self) -> std::result::Result<Self, SnapshotError> {
        let reader = self.reader.as_ref().ok_or_else(|| {
            SnapshotError::new(
                SnapshotErrorCode::Internal,
                "online manifest needs a reader",
            )
        })?;
        if reader.capabilities().features.metadata_pages {
            return Err(SnapshotError::new(
                SnapshotErrorCode::Internal,
                "metadata-pages readers require the real fixed-root owner path",
            ));
        }
        reader.local_lease_status()?;
        let cache = OnlineFuseCache::new(reader)?;
        let state = self.state.get_mut().unwrap();
        for node in state.nodes.values_mut() {
            if let Node::File(file) = node {
                file.online_file = Some(OnlineSnapshotFile::from_manifest_file(
                    reader,
                    SnapshotFile {
                        rel_path: file.path.clone(),
                        fs_kind: file.fs_kind.clone(),
                        size: file.size,
                        content_digest: file.digest.clone(),
                    },
                )?);
            }
        }
        reader.local_lease_status()?;
        state.online = Some(cache);
        Ok(self)
    }

    #[cfg(test)]
    pub(super) fn online_file_for_test(&self, inode: Inode) -> Option<Arc<OnlineSnapshotFile>> {
        match self.state.lock().unwrap().nodes.get(&inode) {
            Some(Node::File(file)) => file.online_file.clone(),
            _ => None,
        }
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
        let scope_pages = match &store {
            Some(store) if store.workspace_binding()?.is_some() => {
                let scope = store.content_dir().parent().ok_or_else(|| {
                    SnapshotError::new(
                        SnapshotErrorCode::Internal,
                        "workspace metadata cache has no scope",
                    )
                })?;
                reader.authorized_context().bind_scope_cache(scope)?;
                Some(ScopeCache::open(scope)?)
            }
            _ => None,
        };
        let root_page_id = reader.descriptor().metadata_root.clone();
        let (store_small, store_ranges) = stored_caches(Some(&reader), store.as_ref())?;
        let mut state = State {
            next_inode: ROOT_INODE,
            nodes: HashMap::new(),
            owned: owned_cache(Some(&reader), store.as_ref())?,
            online: None,
            store_small,
            store_ranges,
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
            scope_pages,
            local_cas: None,
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
        self.check_metadata_lease()?;
        let result = self.load_directory(inode).await;
        self.check_metadata_lease()?;
        result
    }

    // Recorded nodes remain usable by a paused local upper scan. Loading a
    // missing directory still requires the existing live lease at both ends.
    async fn load_directory(
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
            let mut pages = Vec::new();
            let mut missing = Vec::new();
            for item in items {
                let expected = item.expected_digest.as_ref().expect("fixed page digest");
                if let Some(cache) = &self.scope_pages {
                    if let Some(bytes) = cache.read_page_verified_bounded(expected)? {
                        pages.push((crate::snapshot::frames::parse_digest(expected)?, bytes));
                        continue;
                    }
                }
                missing.push(item);
            }
            if !missing.is_empty() {
                pages.extend(
                    reader
                        .client
                        .metadata_pages(&sid, &missing, reader.encoding_hint())
                        .await?,
                );
            }
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

        // Local hints retain the same live fixed-view authority as wire pages.
        reader.ensure_lease().await?;
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
                        online_file: None,
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
        self.check_metadata_lease().map_err(io_err)?;
        let need = {
            let state = self.state.lock().unwrap();
            state.lazy
                && match state.nodes.get(&inode) {
                    Some(Node::Dir(d)) => !d.loaded,
                    _ => false,
                }
        };
        let result = if need {
            self.ensure_dir_loaded(inode).await.map_err(|error| {
                if self.store_reader().is_some() {
                    io_err(error)
                } else {
                    Errno::from(libc::EIO)
                }
            })
        } else {
            Ok(())
        };
        self.check_metadata_lease().map_err(io_err)?;
        result
    }

    pub(crate) fn build(
        reader: Option<SnapshotReader>,
        store: Option<Arc<DurableStore>>,
        manifest: Vec<SnapshotFile>,
    ) -> std::result::Result<Self, crate::snapshot::SnapshotError> {
        let mut state = State {
            next_inode: ROOT_INODE,
            nodes: HashMap::new(),
            owned: owned_cache(reader.as_ref(), store.as_ref())?,
            online: None,
            store_small: None,
            store_ranges: None,
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
            scope_pages: None,
            local_cas: None,
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
        let (store_small, store_ranges) = stored_caches(reader.as_ref(), store.as_ref())?;
        let mut state = State {
            next_inode: ROOT_INODE,
            nodes: HashMap::new(),
            owned: owned_cache(reader.as_ref(), store.as_ref())?,
            online: None,
            store_small,
            store_ranges,
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
                    online_file: None,
                }),
            );
        }
        Ok(Mst2Fuse {
            reader,
            store,
            scope_pages: None,
            local_cas: None,
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

    async fn read_local(
        &self,
        scope: &LocalCasScope,
        node: &FileNode,
        offset: u64,
        requested: u64,
    ) -> Result<ReplyData> {
        if node.fs_kind == "symlink" && !(1..=4095).contains(&node.size) {
            return Err(Errno::from(libc::EIO));
        }
        if requested == 0 || offset >= node.size {
            return Ok(ReplyData { data: Bytes::new() });
        }
        let wanted = requested.min(node.size - offset);
        if node.size <= crate::snapshot::OBJECT_CAP {
            return self.read_local_small(scope, node, offset, wanted).await;
        }
        self.read_local_range(scope, node, offset, wanted).await
    }

    async fn read_local_small(
        &self,
        scope: &LocalCasScope,
        node: &FileNode,
        offset: u64,
        wanted: u64,
    ) -> Result<ReplyData> {
        let key = ContentKey::new(&node.digest, node.size).map_err(io_err)?;
        let start = usize::try_from(offset).map_err(|_| Errno::from(libc::EIO))?;
        let stop = usize::try_from(offset + wanted).map_err(|_| Errno::from(libc::EIO))?;
        let admission = ReplyAdmission::reserve(&scope.budget).map_err(io_err)?;
        let cached = self
            .state
            .lock()
            .unwrap()
            .store_small
            .as_mut()
            .ok_or_else(|| Errno::from(libc::EIO))?
            .get(key);
        let (content, admission) = match cached {
            Some(content) => (content, admission),
            None => {
                let store = self
                    .store
                    .as_ref()
                    .ok_or_else(|| Errno::from(libc::EIO))?
                    .clone();
                let digest = node.digest.clone();
                let size = node.size;
                let budget = scope.budget.clone();
                let completion = scope
                    .workers
                    .run_local(
                        scope.access,
                        admission,
                        RequestMeters {
                            kind: "small_whole",
                            wanted: size,
                        },
                        move || {
                            WorkResult::local(
                                VerifiedCasContent::read(&store, &digest, size, &budget),
                                None,
                            )
                        },
                    )
                    .await
                    .map_err(io_err)?;
                // Preserve the existing small/readlink missing-body errno.
                // A local miss is terminal, never an online fallback.
                let owner = completion
                    .result
                    .map_err(io_err)?
                    .ok_or_else(|| Errno::from(libc::ENOENT))?;
                (StoreContent::Cas(owner), completion.admission)
            }
        };
        if content.len() as u64 != node.size
            || (node.fs_kind == "symlink" && content.as_bytes().contains(&0))
        {
            return Err(Errno::from(libc::EIO));
        }
        let data = admission
            .store_content(content.clone(), start, stop)
            .map_err(io_err)?;
        self.state
            .lock()
            .unwrap()
            .store_small
            .as_mut()
            .ok_or_else(|| Errno::from(libc::EIO))?
            .insert(key, content)
            .map_err(io_err)?;
        Ok(ReplyData { data })
    }

    async fn read_local_range(
        &self,
        scope: &LocalCasScope,
        node: &FileNode,
        offset: u64,
        wanted: u64,
    ) -> Result<ReplyData> {
        let admission = ReplyAdmission::reserve(&scope.budget).map_err(io_err)?;
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Errno::from(libc::EIO))?
            .clone();
        let digest = node.digest.clone();
        let size = node.size;
        let budget = scope.budget.clone();
        let completion = scope
            .workers
            .run_local(
                scope.access,
                admission,
                RequestMeters {
                    kind: "large_range",
                    wanted,
                },
                move || {
                    let mut meters = LocalCasRangeMeters::default();
                    let result = VerifiedCasRange::read(
                        &store,
                        &digest,
                        size,
                        offset,
                        wanted,
                        &budget,
                        &mut meters,
                    );
                    WorkResult::local(result, Some(meters))
                },
            )
            .await
            .map_err(io_err)?;
        // A declared local large file with missing backing bytes stays EIO.
        let owner = completion
            .result
            .map_err(io_err)?
            .ok_or_else(|| Errno::from(libc::EIO))?;
        if owner.len() as u64 != wanted {
            return Err(Errno::from(libc::EIO));
        }
        Ok(ReplyData {
            data: completion.admission.cas_range(owner).map_err(io_err)?,
        })
    }

    fn owned_reader(&self) -> Option<&SnapshotReader> {
        self.reader
            .as_ref()
            .filter(|reader| self.store.is_none() && reader.capabilities().features.metadata_pages)
    }

    async fn online_node(
        reader: &SnapshotReader,
        node: &FileNode,
    ) -> Result<Arc<OnlineSnapshotFile>> {
        let file = node
            .online_file
            .as_ref()
            .ok_or_else(|| Errno::from(libc::EIO))?;
        let fixed = file.file();
        if fixed.rel_path != node.path
            || fixed.fs_kind != node.fs_kind
            || fixed.size != node.size
            || fixed.content_digest != node.digest
        {
            return Err(Errno::from(libc::EIO));
        }
        file.validate(reader).await.map_err(io_err)?;
        Ok(file.clone())
    }

    async fn read_online(
        &self,
        reader: &SnapshotReader,
        inode: Inode,
        node: &FileNode,
        offset: u64,
        requested: u64,
    ) -> Result<ReplyData> {
        let file = Self::online_node(reader, node).await?;
        if node.fs_kind == "symlink" && !(1..=4095).contains(&node.size) {
            return Err(Errno::from(libc::EIO));
        }
        if requested == 0 || offset >= node.size {
            file.validate(reader).await.map_err(io_err)?;
            return Ok(ReplyData { data: Bytes::new() });
        }
        let wanted = requested.min(node.size - offset);
        let admission = ReplyAdmission::new(reader).map_err(io_err)?;
        if node.size <= crate::snapshot::OBJECT_CAP {
            return self
                .read_online_small(reader, inode, node, file, offset, wanted, admission)
                .await;
        }
        self.read_online_range(reader, inode, node, file, offset, wanted, admission)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_online_small(
        &self,
        reader: &SnapshotReader,
        inode: Inode,
        node: &FileNode,
        file: Arc<OnlineSnapshotFile>,
        offset: u64,
        wanted: u64,
        admission: ReplyAdmission,
    ) -> Result<ReplyData> {
        let (cached, workers, coordinator) = {
            let mut state = self.state.lock().unwrap();
            let cache = state
                .online
                .as_mut()
                .ok_or_else(|| Errno::from(libc::EIO))?;
            (
                cache.contents.get(inode),
                cache.workers.clone(),
                cache.coordinator.clone(),
            )
        };
        let (content, admission) = match cached {
            Some(entry) => (entry.content, admission),
            None => {
                let (local, admission) = match &self.store {
                    Some(store) => {
                        let store = store.clone();
                        let digest = node.digest.clone();
                        let size = node.size;
                        let budget = reader.content_scope.clone();
                        let completion = workers
                            .run(
                                reader.clone(),
                                admission,
                                RequestMeters {
                                    kind: "small_whole",
                                    wanted: size,
                                },
                                move || {
                                    WorkResult::local(
                                        VerifiedCasContent::read(&store, &digest, size, &budget),
                                        None,
                                    )
                                },
                            )
                            .await
                            .map_err(io_err)?;
                        (completion.result.map_err(io_err)?, completion.admission)
                    }
                    None => (None, admission),
                };
                file.validate(reader).await.map_err(io_err)?;
                let content = match local {
                    Some(owner) => StoreContent::Cas(owner),
                    None => StoreContent::Wire(
                        coordinator
                            .fetch_owned(
                                file.file().clone(),
                                reader.capabilities().features.objects,
                            )
                            .await
                            .map_err(io_err)?,
                    ),
                };
                (content, admission)
            }
        };
        if content.len() as u64 != node.size
            || (node.fs_kind == "symlink" && content.as_bytes().contains(&0))
        {
            return Err(Errno::from(libc::EIO));
        }
        file.validate(reader).await.map_err(io_err)?;
        let start = usize::try_from(offset).map_err(|_| Errno::from(libc::EIO))?;
        let end = usize::try_from(offset + wanted).map_err(|_| Errno::from(libc::EIO))?;
        let data = admission
            .store_content(content.clone(), start, end)
            .map_err(io_err)?;
        let mut state = self.state.lock().unwrap();
        reader.local_lease_status().map_err(io_err)?;
        state
            .online
            .as_mut()
            .ok_or_else(|| Errno::from(libc::EIO))?
            .contents
            .insert(OnlineContentEntry { inode, content });
        Ok(ReplyData { data })
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_online_range(
        &self,
        reader: &SnapshotReader,
        inode: Inode,
        node: &FileNode,
        file: Arc<OnlineSnapshotFile>,
        offset: u64,
        wanted: u64,
        admission: ReplyAdmission,
    ) -> Result<ReplyData> {
        let (cached, workers) = {
            let mut state = self.state.lock().unwrap();
            let cache = state
                .online
                .as_mut()
                .ok_or_else(|| Errno::from(libc::EIO))?;
            (cache.ranges.get(inode), cache.workers.clone())
        };
        if cached
            .as_ref()
            .is_some_and(|entry| !Arc::ptr_eq(&entry.file, &file))
        {
            return Err(Errno::from(libc::EIO));
        }
        // Always try real stored CAS before the path-specific wire handle.
        // The primitive returns None only for a verified true missing object.
        let (local, admission) = match &self.store {
            Some(store) => {
                let store = store.clone();
                let digest = node.digest.clone();
                let size = node.size;
                let budget = reader.content_scope.clone();
                let completion = workers
                    .run(
                        reader.clone(),
                        admission,
                        RequestMeters {
                            kind: "large_range",
                            wanted,
                        },
                        move || {
                            let mut meters = LocalCasRangeMeters::default();
                            let result = VerifiedCasRange::read(
                                &store,
                                &digest,
                                size,
                                offset,
                                wanted,
                                &budget,
                                &mut meters,
                            );
                            WorkResult::local(result, Some(meters))
                        },
                    )
                    .await
                    .map_err(io_err)?;
                (completion.result.map_err(io_err)?, completion.admission)
            }
            None => (None, admission),
        };
        file.validate(reader).await.map_err(io_err)?;
        if let Some(owner) = local {
            if owner.len() as u64 != wanted {
                return Err(Errno::from(libc::EIO));
            }
            reader.local_lease_status().map_err(io_err)?;
            return Ok(ReplyData {
                data: admission.cas_range(owner).map_err(io_err)?,
            });
        }
        let range = match cached {
            Some(entry) => entry.range,
            None => Arc::new(
                OwnedChunkedFile::open_online_path(reader, file.clone())
                    .await
                    .map_err(io_err)?,
            ),
        };
        let owner = range
            .read_range_owned(offset, wanted)
            .await
            .map_err(io_err)?;
        if owner.len() as u64 != wanted {
            return Err(Errno::from(libc::EIO));
        }
        file.validate(reader).await.map_err(io_err)?;
        let data = admission.range(owner).map_err(io_err)?;
        let mut state = self.state.lock().unwrap();
        reader.local_lease_status().map_err(io_err)?;
        state
            .online
            .as_mut()
            .ok_or_else(|| Errno::from(libc::EIO))?
            .ranges
            .insert(OnlineRangeEntry { inode, file, range });
        Ok(ReplyData { data })
    }

    fn store_reader(&self) -> Option<&SnapshotReader> {
        if self.state.lock().unwrap().store_small.is_some() {
            self.reader.as_ref()
        } else {
            None
        }
    }

    fn store_workers(&self) -> Result<Arc<CasReadScope>> {
        self.state
            .lock()
            .unwrap()
            .store_ranges
            .as_ref()
            .map(|cache| cache.workers.clone())
            .ok_or_else(|| Errno::from(libc::EIO))
    }

    // This observes the current local grant and any latched terminal failure;
    // it is not a fresh remote authorization decision or a metadata fetch.
    fn check_metadata_lease(&self) -> std::result::Result<(), SnapshotError> {
        match self.reader.as_ref() {
            Some(reader) => reader.local_lease_status(),
            None => Ok(()),
        }
    }

    /// Each logical path proves its own fixed membership before sharing bytes.
    /// A CAS miss alone may reach the existing sized, accounted transport.
    async fn read_store_small(
        &self,
        reader: &SnapshotReader,
        node: &FileNode,
        offset: u64,
        requested: u64,
    ) -> Result<ReplyData> {
        let proven = Self::proven_node(reader, node, None).await?;
        if requested == 0 || offset >= node.size {
            proven.validate(reader).await.map_err(io_err)?;
            return Ok(ReplyData { data: Bytes::new() });
        }
        let key = ContentKey::new(&node.digest, node.size).map_err(io_err)?;
        let end = offset.saturating_add(requested).min(node.size);
        let start = usize::try_from(offset).map_err(|_| Errno::from(libc::EIO))?;
        let stop = usize::try_from(end).map_err(|_| Errno::from(libc::EIO))?;
        let admission = ReplyAdmission::new(reader).map_err(io_err)?;
        let cached = self
            .state
            .lock()
            .unwrap()
            .store_small
            .as_mut()
            .ok_or_else(|| Errno::from(libc::EIO))?
            .get(key);
        let (content, admission) = match cached {
            Some(content) => (content, admission),
            None => {
                let store = self
                    .store
                    .as_ref()
                    .ok_or_else(|| Errno::from(libc::EIO))?
                    .clone();
                let digest = node.digest.clone();
                let size = node.size;
                let budget = reader.content_scope.clone();
                let completion = self
                    .store_workers()?
                    .run(
                        reader.clone(),
                        admission,
                        RequestMeters {
                            kind: "small_whole",
                            wanted: size,
                        },
                        move || {
                            WorkResult::local(
                                VerifiedCasContent::read(&store, &digest, size, &budget),
                                None,
                            )
                        },
                    )
                    .await
                    .map_err(io_err)?;
                let local = completion.result.map_err(io_err)?;
                // The queue and local read may outlive the original lease.
                proven.validate(reader).await.map_err(io_err)?;
                let content = match local {
                    Some(content) => StoreContent::Cas(content),
                    None => StoreContent::Wire(
                        reader
                            .read_proven_content(&proven, reader.capabilities().features.objects)
                            .await
                            .map_err(io_err)?,
                    ),
                };
                (content, completion.admission)
            }
        };
        if content.len() as u64 != node.size
            || (node.fs_kind == "symlink"
                && (!(1..=4095).contains(&content.len()) || content.as_bytes().contains(&0)))
        {
            return Err(Errno::from(libc::EIO));
        }
        proven.validate(reader).await.map_err(io_err)?;
        let data = admission
            .store_content(content.clone(), start, stop)
            .map_err(io_err)?;
        let mut state = self.state.lock().unwrap();
        reader.local_lease_status().map_err(io_err)?;
        state
            .store_small
            .as_mut()
            .ok_or_else(|| Errno::from(libc::EIO))?
            .insert(key, content)
            .map_err(io_err)?;
        Ok(ReplyData { data })
    }

    /// Every metadata-pages stored read uses actual fixed-root membership.
    /// CAS errors are terminal; only a true missing object permits wire I/O.
    async fn read_store_large(
        &self,
        reader: &SnapshotReader,
        inode: Inode,
        node: &FileNode,
        offset: u64,
        requested: u64,
    ) -> Result<ReplyData> {
        let cached = self
            .state
            .lock()
            .unwrap()
            .store_ranges
            .as_mut()
            .ok_or_else(|| Errno::from(libc::EIO))?
            .ranges
            .get(inode);
        let proven = Self::proven_node(
            reader,
            node,
            cached.as_ref().map(|entry| entry.proven.clone()),
        )
        .await?;
        if requested == 0 || offset >= node.size {
            proven.validate(reader).await.map_err(io_err)?;
            return Ok(ReplyData { data: Bytes::new() });
        }
        let wanted = requested.min(node.size - offset);
        let admission = ReplyAdmission::new(reader).map_err(io_err)?;
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Errno::from(libc::EIO))?
            .clone();
        let digest = node.digest.clone();
        let size = node.size;
        let budget = reader.content_scope.clone();
        // Always prefer local CAS, including when a wire handle is cached.
        // Only the primitive's safe-open NotFound permits wire fallback.
        let completion = self
            .store_workers()?
            .run(
                reader.clone(),
                admission,
                RequestMeters {
                    kind: "large_range",
                    wanted,
                },
                move || {
                    let mut meters = LocalCasRangeMeters::default();
                    let result = VerifiedCasRange::read(
                        &store,
                        &digest,
                        size,
                        offset,
                        wanted,
                        &budget,
                        &mut meters,
                    );
                    WorkResult::local(result, Some(meters))
                },
            )
            .await
            .map_err(io_err)?;
        let local = completion.result.map_err(io_err)?;
        proven.validate(reader).await.map_err(io_err)?;
        let admission = completion.admission;
        if let Some(owner) = local {
            if owner.len() as u64 != wanted {
                return Err(Errno::from(libc::EIO));
            }
            reader.local_lease_status().map_err(io_err)?;
            return Ok(ReplyData {
                data: admission.cas_range(owner).map_err(io_err)?,
            });
        }
        let range = match cached {
            Some(entry) => entry.range,
            None => Arc::new(
                OwnedChunkedFile::open_proven(reader, proven.clone())
                    .await
                    .map_err(io_err)?,
            ),
        };
        let owner = range
            .read_range_owned(offset, wanted)
            .await
            .map_err(io_err)?;
        if owner.len() as u64 != wanted {
            return Err(Errno::from(libc::EIO));
        }
        proven.validate(reader).await.map_err(io_err)?;
        let data = admission.range(owner).map_err(io_err)?;
        let mut state = self.state.lock().unwrap();
        reader.local_lease_status().map_err(io_err)?;
        state
            .store_ranges
            .as_mut()
            .ok_or_else(|| Errno::from(libc::EIO))?
            .ranges
            .insert(RangeEntry {
                inode,
                proven,
                range,
            });
        Ok(ReplyData { data })
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

    /// Fixed-root authority gates cached content, empty reads and EOF.
    /// Proof, quota and body validation failures are terminal.
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
        if node.fs_kind == "symlink" && !(1..=4095).contains(&node.size) {
            return Err(Errno::from(libc::EIO));
        }
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
            if owner.len() as u64 != node.size
                || (node.fs_kind == "symlink" && owner.as_bytes().contains(&0))
            {
                return Err(Errno::from(libc::EIO));
            }
            proven.validate(reader).await.map_err(io_err)?;
            let data = admission
                .content(owner.clone(), offset as usize, end as usize)
                .map_err(io_err)?;
            let mut state = self.state.lock().unwrap();
            reader.local_lease_status().map_err(io_err)?;
            state.owned.as_mut().unwrap().contents.insert(ContentEntry {
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
            let mut state = self.state.lock().unwrap();
            reader.local_lease_status().map_err(io_err)?;
            state.owned.as_mut().unwrap().ranges.insert(RangeEntry {
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

    pub(crate) fn metadata_node(&self, inode: u64) -> Result<Node> {
        self.check_metadata_lease().map_err(io_err)?;
        let node = self.node(inode)?;
        self.check_metadata_lease().map_err(io_err)?;
        Ok(node)
    }

    /// Query fixed metadata without reading content or following a symlink.
    /// A missing child is proven only after its parent's entire canonical
    /// radix is verified. Legacy file-only manifests cannot prove namespace.
    pub async fn path_state(
        &self,
        rel_path: &str,
    ) -> std::result::Result<SnapshotPathState, SnapshotError> {
        self.check_metadata_lease()?;
        let result = self.recorded_path_state(rel_path).await;
        self.check_metadata_lease()?;
        result
    }

    // Only paused upper-diff inspection may reuse already recorded facts after
    // revocation. Unloaded directory pages still go through load_directory.
    pub(crate) async fn path_state_for_diff(
        &self,
        rel_path: &str,
        pause: &MutationPause,
    ) -> std::result::Result<SnapshotPathState, SnapshotError> {
        Self::check_diff_pause(pause)?;
        let result = self.recorded_path_state(rel_path).await;
        Self::check_diff_pause(pause)?;
        result
    }

    fn check_diff_pause(pause: &MutationPause) -> std::result::Result<(), SnapshotError> {
        pause.ensure_certain().map_err(|error| {
            SnapshotError::new(SnapshotErrorCode::IntegrityError, error.to_string())
        })
    }

    async fn recorded_path_state(
        &self,
        rel_path: &str,
    ) -> std::result::Result<SnapshotPathState, SnapshotError> {
        match self.recorded_path_inode(rel_path).await? {
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
        self.check_metadata_lease()?;
        let result = self.recorded_directory_entries(rel_path).await;
        self.check_metadata_lease()?;
        result
    }

    pub(crate) async fn directory_entries_for_diff(
        &self,
        rel_path: &str,
        pause: &MutationPause,
    ) -> std::result::Result<Vec<SnapshotDirectoryEntry>, SnapshotError> {
        Self::check_diff_pause(pause)?;
        let result = self.recorded_directory_entries(rel_path).await;
        Self::check_diff_pause(pause)?;
        result
    }

    async fn recorded_directory_entries(
        &self,
        rel_path: &str,
    ) -> std::result::Result<Vec<SnapshotDirectoryEntry>, SnapshotError> {
        let inode = self.recorded_path_inode(rel_path).await?.ok_or_else(|| {
            SnapshotError::new(SnapshotErrorCode::PathNotFound, "fixed directory is absent")
        })?;
        self.load_directory(inode).await?;
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
                ));
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

    async fn recorded_path_inode(
        &self,
        rel_path: &str,
    ) -> std::result::Result<Option<u64>, SnapshotError> {
        let relative = self.metadata_path(rel_path)?;
        let mut inode = ROOT_INODE;
        if relative == "/" {
            return Ok(Some(inode));
        }
        for name in relative.trim_start_matches('/').split('/') {
            self.load_directory(inode).await?;
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
                    ));
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
            ));
        }
        // A manifest where one path is a file and another uses that file as a
        // directory (`a` and `a/b`) is contradictory; that is a server/manifest
        // defect, and panicking the mount thread would take the whole
        // filesystem down. Typed error instead.
        Some(Node::File(_)) => {
            return Err(crate::snapshot::SnapshotError::new(
                crate::snapshot::SnapshotErrorCode::Internal,
                format!("manifest uses file {parent_inode} as a directory (entry {name:?})"),
            ));
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
            ));
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
            online_file: None,
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
        self.check_metadata_lease().map_err(io_err)?;
        let node = self.node(inode)?;
        let attr = match &node {
            Node::Dir(_) => dir_attr(inode),
            Node::File(f) => file_attr(inode, f),
        };
        self.check_metadata_lease().map_err(io_err)?;
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
        self.check_metadata_lease().map_err(io_err)?;
        let inode = child.ok_or_else(|| Errno::from(libc::ENOENT))?;
        let node = self.node(inode)?;
        let attr = match &node {
            Node::Dir(_) => dir_attr(inode),
            Node::File(f) => file_attr(inode, f),
        };
        self.check_metadata_lease().map_err(io_err)?;
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
        self.check_metadata_lease().map_err(io_err)?;
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
        self.check_metadata_lease().map_err(io_err)?;
        Ok(ReplyDirectoryPlus {
            entries: iter(entries),
        })
    }

    async fn opendir(&self, _req: Request, inode: Inode, _flags: u32) -> Result<ReplyOpen> {
        // Handle needed only so the kernel's directory-open round trip
        // succeeds; the read-only tree needs no per-handle state.
        self.ensure_loaded(inode).await?;
        self.check_metadata_lease().map_err(io_err)?;
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
        self.check_metadata_lease().map_err(io_err)?;
        let files = {
            let state = self.state.lock().unwrap();
            state
                .nodes
                .values()
                .filter(|n| matches!(n, Node::File(_)))
                .count() as u64
        };
        self.check_metadata_lease().map_err(io_err)?;
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
        if let Some(reader) = self.store_reader() {
            reader.ensure_lease().await.map_err(io_err)?;
        }
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
        if let Some(scope) = &self.local_cas {
            return self.read_local(scope, &f, offset, size as u64).await;
        }
        if let Some(reader) = self.owned_reader() {
            return self
                .read_owned(reader, inode, &f, offset, size as u64)
                .await;
        }
        let store_reader = self.store_reader();
        if let Some(reader) = store_reader {
            reader.ensure_lease().await.map_err(io_err)?;
            if f.size <= crate::snapshot::OBJECT_CAP {
                return self.read_store_small(reader, &f, offset, size as u64).await;
            }
            return self
                .read_store_large(reader, inode, &f, offset, size as u64)
                .await;
        }
        if let Some(reader) = self.reader.as_ref() {
            return self
                .read_online(reader, inode, &f, offset, size as u64)
                .await;
        }
        Err(Errno::from(libc::EIO))
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
        if let Some(scope) = &self.local_cas {
            return self.read_local(scope, &f, 0, f.size).await;
        }
        if let Some(reader) = self.owned_reader() {
            return self.read_owned(reader, inode, &f, 0, f.size).await;
        }
        if let Some(reader) = self.store_reader() {
            reader.ensure_lease().await.map_err(io_err)?;
            return self.read_store_small(reader, &f, 0, f.size).await;
        }
        if let Some(reader) = self.reader.as_ref() {
            return self.read_online(reader, inode, &f, 0, f.size).await;
        }
        Err(Errno::from(libc::EIO))
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

fn io_err(e: crate::snapshot::SnapshotError) -> Errno {
    use crate::snapshot::SnapshotErrorCode::*;
    let code = match e.code {
        PathNotFound => libc::ENOENT,
        Unauthenticated | ScopeForbidden | LeaseUnknown => libc::EACCES,
        LeaseExpired | SnapshotGone => libc::ESTALE,
        NotDirectory => libc::ENOTDIR,
        MetadataNotReady => libc::EAGAIN,
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

    async fn local_content_view(
        body: &[u8],
        kind: &str,
        full: bool,
    ) -> (tempfile::TempDir, Arc<DurableStore>, Mst2Fuse) {
        use crate::snapshot::durable::{digest_of, ViewMeta};

        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(DurableStore::open(temp.path()).unwrap());
        let digest = digest_of(body);
        let fs = if full {
            let mut pages = BTreeMap::new();
            let empty = insert_page(&mut pages, &[]);
            let kind = match kind {
                "symlink" => EntryKind::Symlink,
                "executable" => EntryKind::Executable,
                _ => EntryKind::Regular,
            };
            let entry = |name: &[u8]| {
                Entry::file(
                    kind,
                    name,
                    body.len() as u64,
                    crate::snapshot::frames::parse_digest(&digest).unwrap(),
                )
            };
            let root = insert_page(
                &mut pages,
                &[entry(b"alias"), Entry::dir(b"empty", empty), entry(b"file")],
            );
            let closure = closure_for_pages(root, pages);
            let view = ViewMeta {
                snapshot_id: closure.snapshot_id().into(),
                namespace_view_id: closure.descriptor().namespace_view_id.clone(),
                scope: closure.descriptor().scope.clone(),
                lease_id: "local-integrity-test".into(),
            };
            store
                .hydrate_snapshot_with(&view, &closure, |_| std::future::ready(Ok(body.to_vec())))
                .await
                .unwrap();
            Mst2Fuse::from_snapshot_store(store.clone()).unwrap()
        } else {
            let manifest = ["alias", "file"].map(|path| SnapshotFile {
                rel_path: path.into(),
                fs_kind: kind.into(),
                size: body.len() as u64,
                content_digest: digest.clone(),
            });
            store
                .hydrate_with(
                    &ViewMeta {
                        snapshot_id: "sha256:local-snapshot".into(),
                        namespace_view_id: "sha256:local-view".into(),
                        scope: "/project".into(),
                        lease_id: "local-integrity-test".into(),
                    },
                    &manifest,
                    |_| std::future::ready(Ok(body.to_vec())),
                )
                .await
                .unwrap();
            Mst2Fuse::from_store(store.clone()).unwrap()
        };
        (temp, store, fs)
    }

    async fn local_inode(fs: &Mst2Fuse, name: &str) -> u64 {
        fs.lookup(Request::default(), ROOT_INODE, OsStr::new(name))
            .await
            .unwrap()
            .attr
            .ino
    }

    fn local_object(store: &DurableStore, body: &[u8]) -> std::path::PathBuf {
        store.content_dir().join(
            crate::snapshot::durable::digest_of(body)
                .strip_prefix("sha256:")
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn local_constructors_share_actual_small_owners_and_preserve_namespace_contracts() {
        let body = vec![0x51; 8192];
        for full in [false, true] {
            let (_temp, store, fs) = local_content_view(&body, "regular", full).await;
            assert!(fs.reader.is_none());
            let scope = fs.local_cas.as_ref().unwrap();
            assert_eq!(scope.access, LocalCasAccess::CallerEstablished);
            let budget = scope.budget.clone();
            assert!(Arc::ptr_eq(&scope.workers, &budget.cas_workers()));
            let baseline = budget.usage();
            let twin = if full {
                Mst2Fuse::from_snapshot_store(store).unwrap()
            } else {
                Mst2Fuse::from_store(store).unwrap()
            };
            let twin_scope = twin.local_cas.as_ref().unwrap();
            let twin_budget = twin_scope.budget.clone();
            assert!(!Arc::ptr_eq(&budget, &twin_budget));
            assert!(!Arc::ptr_eq(&scope.workers, &twin_scope.workers));
            let twin_baseline = twin_budget.usage();
            let inode = local_inode(&fs, "file").await;
            let alias = local_inode(&fs, "alias").await;
            assert_ne!(inode, alias);
            if full {
                assert_eq!(
                    fs.path_state("empty/missing").await.unwrap(),
                    SnapshotPathState::AbsentProven
                );
            } else {
                assert_eq!(
                    fs.path_state("missing").await.unwrap_err().code,
                    SnapshotErrorCode::SnapshotNotReady
                );
            }
            let reply = fs
                .read(Request::default(), inode, inode, 0, body.len() as u32)
                .await
                .unwrap();
            let alias_reply = fs
                .read(Request::default(), alias, alias, 0, body.len() as u32)
                .await
                .unwrap();
            assert_eq!(reply.data.as_ptr(), alias_reply.data.as_ptr());
            assert_eq!(twin_budget.usage(), twin_baseline);
            let twin_inode = local_inode(&twin, "file").await;
            let twin_reply = twin
                .read(
                    Request::default(),
                    twin_inode,
                    twin_inode,
                    0,
                    body.len() as u32,
                )
                .await
                .unwrap();
            assert_ne!(reply.data.as_ptr(), twin_reply.data.as_ptr());
            assert_eq!(twin_reply.data.as_ref(), body.as_slice());
            drop(twin_reply);
            drop(twin);
            assert_eq!(twin_budget.usage().output_bytes, 0);
            let key = ContentKey::new(&crate::snapshot::durable::digest_of(&body), 8192).unwrap();
            let owner = fs
                .state
                .lock()
                .unwrap()
                .store_small
                .as_mut()
                .unwrap()
                .get(key)
                .unwrap();
            assert_eq!(reply.data.as_ptr(), owner.as_bytes().as_ptr());
            drop(owner);
            let state = fs.state.lock().unwrap();
            assert!(state.owned.is_none() && state.online.is_none());
            assert!(state.store_ranges.is_none());
            drop(state);
            let last = reply.data.clone().slice(17..31);
            drop(reply);
            drop(alias_reply);
            let paid = budget.usage();
            assert!(paid.output_bytes > baseline.output_bytes + body.len());
            assert_eq!(paid.construction_bytes, 0);
            drop(fs);
            assert_eq!(
                budget.usage().output_bytes,
                paid.output_bytes - baseline.output_bytes
            );
            assert_eq!(last.as_ref(), &[0x51; 14]);
            drop(last);
            assert_eq!(budget.usage().output_bytes, 0);
        }
    }

    #[tokio::test]
    async fn local_large_replies_retain_real_output_credit_through_clone_and_mount_drop() {
        let body = vec![0x76; crate::snapshot::OBJECT_CAP as usize + 7];
        for full in [false, true] {
            let (_temp, _store, fs) = local_content_view(&body, "regular", full).await;
            let inode = local_inode(&fs, "file").await;
            let budget = fs.local_cas.as_ref().unwrap().budget.clone();
            let baseline = budget.usage();
            let reply = fs
                .read(Request::default(), inode, inode, 4, 4096)
                .await
                .unwrap();
            let pointer = reply.data.as_ptr();
            let paid = budget.usage();
            assert!(paid.output_bytes > baseline.output_bytes + 4096);
            assert_eq!(paid.construction_bytes, 0);
            let clone = reply.data.clone();
            assert_eq!(clone.as_ptr(), pointer);
            assert_eq!(budget.usage(), paid);
            let last = clone.slice(17..31);
            drop(clone);
            drop(reply);
            drop(fs);
            assert_eq!(last.as_ptr(), pointer.wrapping_add(17));
            assert_eq!(last.as_ref(), &[0x76; 14]);
            assert_eq!(
                budget.usage().output_bytes,
                paid.output_bytes - baseline.output_bytes
            );
            assert_eq!(budget.usage().construction_bytes, 0);
            drop(last);
            assert_eq!(budget.usage().output_bytes, 0);
        }
    }

    #[tokio::test]
    async fn local_cold_damage_and_type_errors_are_terminal_and_refund_admission() {
        for full in [false, true] {
            for size in [17, crate::snapshot::OBJECT_CAP as usize + 7] {
                for fault in ["hash", "short", "long", "directory"] {
                    let body = vec![0x57; size];
                    let (_temp, store, fs) = local_content_view(&body, "regular", full).await;
                    let inode = local_inode(&fs, "file").await;
                    let path = local_object(&store, &body);
                    match fault {
                        "hash" => {
                            let mut damaged = body.clone();
                            *damaged.last_mut().unwrap() ^= 1;
                            std::fs::write(&path, damaged).unwrap();
                        }
                        "short" => std::fs::write(&path, &body[..size - 1]).unwrap(),
                        "long" => {
                            let mut damaged = body.clone();
                            damaged.push(1);
                            std::fs::write(&path, damaged).unwrap();
                        }
                        "directory" => {
                            std::fs::remove_file(&path).unwrap();
                            std::fs::create_dir(&path).unwrap();
                        }
                        _ => unreachable!(),
                    }
                    let budget = fs.local_cas.as_ref().unwrap().budget.clone();
                    let baseline = budget.usage();
                    for _ in 0..2 {
                        let error = fs
                            .read(Request::default(), inode, inode, 0, 13)
                            .await
                            .unwrap_err();
                        assert_eq!(i32::from(error), -libc::EIO, "{full} {size} {fault}");
                        assert_eq!(budget.usage(), baseline);
                    }
                    assert!(fs.reader.is_none());
                    assert!(fs.state.lock().unwrap().online.is_none());
                }
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_cas_symlink_open_errors_are_terminal_even_with_matching_targets() {
        use std::os::unix::fs::symlink;

        for full in [false, true] {
            for size in [17, crate::snapshot::OBJECT_CAP as usize + 7] {
                let body = vec![0x63; size];
                let (temp, store, fs) = local_content_view(&body, "regular", full).await;
                let inode = local_inode(&fs, "file").await;
                let path = local_object(&store, &body);
                let target = temp.path().join("matching-target");
                std::fs::write(&target, &body).unwrap();
                std::fs::remove_file(&path).unwrap();
                symlink(&target, &path).unwrap();
                let baseline = fs.local_cas.as_ref().unwrap().budget.usage();
                for _ in 0..2 {
                    assert_eq!(
                        i32::from(
                            fs.read(Request::default(), inode, inode, 0, 13)
                                .await
                                .unwrap_err()
                        ),
                        -libc::EIO
                    );
                    assert_eq!(fs.local_cas.as_ref().unwrap().budget.usage(), baseline);
                }
            }
        }
    }

    #[tokio::test]
    async fn local_misses_keep_small_and_readlink_enoent_large_eio_and_empty_eof() {
        for full in [false, true] {
            for size in [17, crate::snapshot::OBJECT_CAP as usize + 7] {
                let body = vec![0x61; size];
                let (_temp, store, fs) = local_content_view(&body, "regular", full).await;
                let inode = local_inode(&fs, "file").await;
                std::fs::remove_file(local_object(&store, &body)).unwrap();
                let budget = fs.local_cas.as_ref().unwrap().budget.clone();
                let baseline = budget.usage();
                for (offset, wanted) in [(size as u64, 13), (u64::MAX, u32::MAX), (0, 0)] {
                    assert!(fs
                        .read(Request::default(), inode, inode, offset, wanted)
                        .await
                        .unwrap()
                        .data
                        .is_empty());
                }
                for _ in 0..2 {
                    let error = fs
                        .read(Request::default(), inode, inode, 0, 13)
                        .await
                        .unwrap_err();
                    let errno = if size <= crate::snapshot::OBJECT_CAP as usize {
                        libc::ENOENT
                    } else {
                        libc::EIO
                    };
                    assert_eq!(i32::from(error), -errno);
                    assert_eq!(budget.usage(), baseline);
                }
                assert!(fs.reader.is_none());
            }
            let (_temp, store, fs) = local_content_view(b"target", "symlink", full).await;
            let link = local_inode(&fs, "file").await;
            std::fs::remove_file(local_object(&store, b"target")).unwrap();
            let baseline = fs.local_cas.as_ref().unwrap().budget.usage();
            for _ in 0..2 {
                assert_eq!(
                    i32::from(fs.readlink(Request::default(), link).await.unwrap_err()),
                    -libc::ENOENT
                );
                assert_eq!(fs.local_cas.as_ref().unwrap().budget.usage(), baseline);
            }
        }
    }

    #[tokio::test]
    async fn file_only_local_symlinks_use_the_modern_target_profile() {
        for body in [Vec::new(), b"bad\0target".to_vec(), vec![b'x'; 4096]] {
            let (_temp, _store, fs) = local_content_view(&body, "symlink", false).await;
            let inode = local_inode(&fs, "file").await;
            assert_eq!(
                i32::from(
                    fs.open(Request::default(), inode, libc::O_RDONLY as u32)
                        .await
                        .unwrap_err()
                ),
                -libc::ELOOP
            );
            let baseline = fs.local_cas.as_ref().unwrap().budget.usage();
            assert_eq!(
                i32::from(fs.readlink(Request::default(), inode).await.unwrap_err()),
                -libc::EIO
            );
            assert_eq!(fs.local_cas.as_ref().unwrap().budget.usage(), baseline);
        }
    }

    #[tokio::test]
    async fn local_persistent_budget_rejects_before_missing_body_open_then_refunds() {
        let (_temp, store, fs) = local_content_view(b"data", "regular", false).await;
        let inode = local_inode(&fs, "file").await;
        let budget = fs.local_cas.as_ref().unwrap().budget.clone();
        let baseline = budget.usage();
        std::fs::remove_file(local_object(&store, b"data")).unwrap();
        let held = budget
            .reserve(
                crate::snapshot::content::BudgetClass::Output,
                128 * 1024 * 1024 - baseline.output_bytes,
            )
            .unwrap();
        let occupied = budget.usage();
        assert_eq!(
            i32::from(
                fs.read(Request::default(), inode, inode, 0, 4)
                    .await
                    .unwrap_err()
            ),
            -libc::EIO
        );
        assert_eq!(budget.usage(), occupied);
        drop(held);
        assert_eq!(budget.usage(), baseline);
        assert_eq!(
            i32::from(
                fs.read(Request::default(), inode, inode, 0, 4)
                    .await
                    .unwrap_err()
            ),
            -libc::ENOENT
        );
        assert_eq!(budget.usage(), baseline);
    }

    #[tokio::test]
    async fn local_held_small_reply_survives_real_cache_eviction_and_mount_drop() {
        use crate::snapshot::durable::{digest_of, ViewMeta};

        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(DurableStore::open(temp.path()).unwrap());
        let bodies: Vec<_> = (0..66u8)
            .map(|byte| vec![byte; crate::snapshot::OBJECT_CAP as usize])
            .collect();
        let manifest: Vec<_> = bodies
            .iter()
            .enumerate()
            .map(|(index, body)| SnapshotFile {
                rel_path: format!("file{index:03}"),
                fs_kind: "regular".into(),
                size: body.len() as u64,
                content_digest: digest_of(body),
            })
            .collect();
        store
            .hydrate_with(
                &ViewMeta {
                    snapshot_id: "sha256:local-snapshot".into(),
                    namespace_view_id: "sha256:local-view".into(),
                    scope: "/project".into(),
                    lease_id: "local-integrity-test".into(),
                },
                &manifest,
                |file| {
                    let index = file.rel_path[4..].parse::<usize>().unwrap();
                    std::future::ready(Ok(bodies[index].clone()))
                },
            )
            .await
            .unwrap();
        let fs = Mst2Fuse::from_store(store).unwrap();
        let budget = fs.local_cas.as_ref().unwrap().budget.clone();
        let inode = local_inode(&fs, "file000").await;
        let reply = fs
            .read(Request::default(), inode, inode, 0, 32)
            .await
            .unwrap();
        let last = reply.data.clone().slice(17..31);
        drop(reply);
        for index in 1..66 {
            let inode = local_inode(&fs, &format!("file{index:03}")).await;
            drop(
                fs.read(Request::default(), inode, inode, 0, 32)
                    .await
                    .unwrap(),
            );
        }
        let key = ContentKey::new(&manifest[0].content_digest, manifest[0].size).unwrap();
        assert!(fs
            .state
            .lock()
            .unwrap()
            .store_small
            .as_mut()
            .unwrap()
            .get(key)
            .is_none());
        assert_eq!(last.as_ref(), &[0; 14]);
        drop(fs);
        assert!(budget.usage().output_bytes > crate::snapshot::OBJECT_CAP as usize);
        assert_eq!(budget.usage().construction_bytes, 0);
        drop(last);
        assert_eq!(budget.usage().output_bytes, 0);
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
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(DurableStore::open(temp.path()).unwrap());
        let view = crate::snapshot::ViewMeta {
            snapshot_id: closure.snapshot_id().into(),
            namespace_view_id: closure.descriptor().namespace_view_id.clone(),
            scope: closure.descriptor().scope.clone(),
            lease_id: "local-integrity-test".into(),
        };
        store
            .hydrate_snapshot_with(&view, &closure, |file| {
                let body = match file.fs_kind.as_str() {
                    "executable" => b"#!/bin/sh\n".as_slice(),
                    "symlink" => b"plain".as_slice(),
                    _ => b"data".as_slice(),
                };
                std::future::ready(Ok(body.to_vec()))
            })
            .await
            .unwrap();
        let fs = Mst2Fuse::from_snapshot_store(store).unwrap();
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
        let (_temp, store, fs) = local_content_view(b"data", "regular", false).await;
        let budget = fs.local_cas.as_ref().unwrap().budget.clone();
        let baseline = budget.usage();
        // Any accidental body open after construction would now fail.
        std::fs::remove_file(local_object(&store, b"data")).unwrap();
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
        assert_eq!(budget.usage(), baseline);
        assert!(fs
            .state
            .lock()
            .unwrap()
            .store_small
            .as_mut()
            .unwrap()
            .get(ContentKey::new(&crate::snapshot::durable::digest_of(b"data"), 4).unwrap())
            .is_none());
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
    async fn local_short_content_is_eio_and_true_eof_is_empty() {
        let (_temp, store, fs) = local_content_view(b"data", "regular", false).await;
        let req = Request::default();
        let inode = fs
            .lookup(req, ROOT_INODE, OsStr::new("file"))
            .await
            .unwrap()
            .attr
            .ino;
        let path = local_object(&store, b"data");
        std::fs::write(&path, b"dat").unwrap();
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
        std::fs::write(&path, b"data").unwrap();
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
        let body = vec![0x51; 2 * 1024 * 1024 + 7];
        let (_temp, store, fs) = local_content_view(&body, "regular", false).await;
        let path = local_object(&store, &body);
        let req = Request::default();
        let inode = fs
            .lookup(req, ROOT_INODE, OsStr::new("file"))
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
        for (code, errno) in [
            (SnapshotErrorCode::ViewNotFound, libc::EIO),
            (SnapshotErrorCode::SnapshotGone, libc::ESTALE),
            (SnapshotErrorCode::LeaseExpired, libc::ESTALE),
            (SnapshotErrorCode::ScopeForbidden, libc::EACCES),
            (SnapshotErrorCode::Unauthenticated, libc::EACCES),
            (SnapshotErrorCode::LeaseUnknown, libc::EACCES),
        ] {
            assert_eq!(
                i32::from(io_err(SnapshotError::new(code, "unavailable"))),
                -errno
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
