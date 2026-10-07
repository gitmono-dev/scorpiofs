//! Selective fixed-root file membership, independent of complete closure proof.

use std::{
    mem::size_of,
    sync::{Arc, OnceLock},
    time::Duration,
};

use mst2_codec::metapage::{page_id, Entry, EntryKind, Page, MAX_DEPTH, PAGE_MAX_BYTES};
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};

use super::{
    closure::decode_page,
    frames::{parse_digest, MetadataPageItem},
    CacheDomain, SnapshotError, SnapshotErrorCode, SnapshotFile, SnapshotReader, VerifiedContent,
};

const CELLS: usize = 64;
const MAX_STEPS: usize = 4096;
const MAX_VISITS: usize = 4096;
const MAX_PROOF_BYTES: usize = 64 * 1024 * 1024;
const PROOF_DEADLINE: Duration = Duration::from_secs(30);

#[cfg(test)]
#[path = "proven_file_tests.rs"]
mod tests;

fn integrity(message: &str) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::IntegrityError, message)
}
fn limit() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::LimitExceeded,
        "selective membership work admission limit reached",
    )
}

/// Errors from the new selective membership API. This separate type preserves
/// exhaustive matches over the established `SnapshotErrorCode` enum.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FileMembershipError {
    /// The scope root or final committed path is a directory, not a file.
    NotFile { message: String },
    /// Original path, authority, lease, transport, integrity or work error.
    Snapshot(SnapshotError),
}
impl FileMembershipError {
    pub fn snapshot_error(&self) -> Option<&SnapshotError> {
        match self {
            Self::NotFile { .. } => None,
            Self::Snapshot(error) => Some(error),
        }
    }
    /// Local directory classification has no fabricated HTTP error status.
    pub fn http_status(&self) -> u16 {
        self.snapshot_error().map_or(0, |error| error.http_status)
    }
}
impl From<SnapshotError> for FileMembershipError {
    fn from(error: SnapshotError) -> Self {
        Self::Snapshot(error)
    }
}
impl std::fmt::Display for FileMembershipError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFile { message } => write!(formatter, "NotFile: {message}"),
            Self::Snapshot(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}
impl std::error::Error for FileMembershipError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.snapshot_error()
            .map(|error| error as &(dyn std::error::Error + 'static))
    }
}
fn not_file(message: &str) -> FileMembershipError {
    FileMembershipError::NotFile {
        message: message.into(),
    }
}

/// Immutable facts derived from a specific descriptor-root MTP2 path. This is
/// membership evidence, not a lease, offline permission or complete closure.
/// No public constructor, deserializer or unchecked conversion is provided.
///
/// ```compile_fail
/// use scorpiofs::snapshot::ProvenSnapshotFile;
/// let _: ProvenSnapshotFile = serde_json::from_value(serde_json::json!({})).unwrap();
/// ```
#[derive(Debug)]
pub struct ProvenSnapshotFile {
    file: SnapshotFile,
    domain: CacheDomain,
    epoch: u64,
    scope: String,
    root: String,
    snapshot: String,
}
impl ProvenSnapshotFile {
    pub fn file(&self) -> &SnapshotFile {
        &self.file
    }
    fn mint(reader: &SnapshotReader, file: SnapshotFile) -> Arc<Self> {
        Arc::new(Self {
            file,
            domain: reader.authorized_context().cache_domain().clone(),
            epoch: reader.authorized_context().authorization_epoch(),
            scope: reader.descriptor().scope.clone(),
            root: reader.descriptor().metadata_root.clone(),
            snapshot: reader.snapshot_id().to_string(),
        })
    }
    pub(crate) async fn validate(&self, reader: &SnapshotReader) -> Result<(), SnapshotError> {
        let context = reader.authorized_context();
        if self.domain != *context.cache_domain()
            || self.epoch != context.authorization_epoch()
            || self.scope != reader.descriptor().scope
            || self.root != reader.descriptor().metadata_root
            || self.snapshot != reader.snapshot_id()
        {
            return Err(integrity(
                "proven file belongs to a different reader authority or fixed root",
            ));
        }
        context.validate_relative_path(&self.file.rel_path)?;
        reader.ensure_lease().await
    }
}

struct PathCell {
    path: String,
    token: OnceCell<Arc<ProvenSnapshotFile>>,
}
struct CellTable {
    entries: [Option<Arc<PathCell>>; CELLS],
    next: usize,
}
impl CellTable {
    fn get(&mut self, path: &str) -> Result<Arc<PathCell>, SnapshotError> {
        if let Some(cell) = self.entries.iter().flatten().find(|cell| cell.path == path) {
            return Ok(cell.clone());
        }
        for distance in 0..CELLS {
            let index = (self.next + distance) % CELLS;
            if self.entries[index]
                .as_ref()
                .is_none_or(|cell| Arc::strong_count(cell) == 1)
            {
                let cell = Arc::new(PathCell {
                    path: path.into(),
                    token: OnceCell::new(),
                });
                self.entries[index] = Some(cell.clone());
                self.next = (index + 1) % CELLS;
                return Ok(cell);
            }
        }
        Err(limit())
    }
}

pub(crate) struct PathMembership {
    table: std::sync::Mutex<CellTable>,
    callers: Arc<Semaphore>,
    initializers: Arc<Semaphore>,
    deadline: Duration,
    complete_directories: OnceLock<Box<[String]>>,
}
impl PathMembership {
    pub(crate) fn new() -> Self {
        Self {
            table: std::sync::Mutex::new(CellTable {
                entries: std::array::from_fn(|_| None),
                next: 0,
            }),
            callers: Arc::new(Semaphore::new(128)),
            initializers: Arc::new(Semaphore::new(4)),
            deadline: PROOF_DEADLINE,
            complete_directories: OnceLock::new(),
        }
    }

    pub(crate) fn seed_directories(&self, closure: &super::ValidatedSnapshotClosure) {
        self.complete_directories.get_or_init(|| {
            closure
                .directories()
                .iter()
                .map(|directory| directory.rel_path.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice()
        });
    }
}
struct ProcessAdmission {
    callers: Arc<Semaphore>,
    initializers: Arc<Semaphore>,
}
fn process() -> &'static ProcessAdmission {
    static PROCESS: OnceLock<ProcessAdmission> = OnceLock::new();
    PROCESS.get_or_init(|| ProcessAdmission {
        callers: Arc::new(Semaphore::new(512)),
        initializers: Arc::new(Semaphore::new(16)),
    })
}
struct Admission {
    _local: OwnedSemaphorePermit,
    _process: OwnedSemaphorePermit,
}
impl Admission {
    fn acquire(local: &Arc<Semaphore>, process: &Arc<Semaphore>) -> Result<Self, SnapshotError> {
        let local = local.clone().try_acquire_owned().map_err(|_| limit())?;
        let process = process.clone().try_acquire_owned().map_err(|_| limit())?;
        Ok(Self {
            _local: local,
            _process: process,
        })
    }
}

struct ProofWork {
    steps: usize,
    visits: usize,
    bytes: usize,
}
impl ProofWork {
    fn new() -> Self {
        Self {
            steps: 0,
            visits: 0,
            bytes: 0,
        }
    }
    fn admit(&mut self, route: usize) -> Result<(), SnapshotError> {
        let units = route.checked_add(1).ok_or_else(limit)?;
        let bytes = units.checked_mul(PAGE_MAX_BYTES).ok_or_else(limit)?;
        if self.steps >= MAX_STEPS
            || self
                .visits
                .checked_add(units)
                .is_none_or(|n| n > MAX_VISITS)
            || self
                .bytes
                .checked_add(bytes)
                .is_none_or(|n| n > MAX_PROOF_BYTES)
        {
            return Err(limit());
        }
        self.steps += 1;
        Ok(())
    }
    fn received(&mut self, pages: &[([u8; 32], Vec<u8>)]) -> Result<(), SnapshotError> {
        self.visits = self.visits.checked_add(pages.len()).ok_or_else(limit)?;
        for (_, bytes) in pages {
            self.bytes = self.bytes.checked_add(bytes.len()).ok_or_else(limit)?;
        }
        if self.visits > MAX_VISITS || self.bytes > MAX_PROOF_BYTES {
            return Err(limit());
        }
        Ok(())
    }
}

impl SnapshotReader {
    /// Prove only a target's ancestors and selected radix pages. A matching
    /// complete-closure seed yields the same opaque token without metadata I/O.
    /// Reader clones share bounded path cells and actual initializer flights.
    pub async fn prove_file(
        &self,
        path: &str,
    ) -> Result<Arc<ProvenSnapshotFile>, FileMembershipError> {
        self.prove_file_deadline(path, self.path_membership.deadline)
            .await
    }

    async fn prove_file_deadline(
        &self,
        path: &str,
        deadline: Duration,
    ) -> Result<Arc<ProvenSnapshotFile>, FileMembershipError> {
        self.authorized_context().validate_relative_path(path)?;
        let _caller = Admission::acquire(&self.path_membership.callers, &process().callers)?;
        // This caller's one deadline includes renewal, waiting on another
        // initializer, any retries it starts, and final current-lease validation.
        tokio::time::timeout(deadline, self.prove_file_admitted(path))
            .await
            .map_err(|_| limit())?
    }

    async fn prove_file_admitted(
        &self,
        path: &str,
    ) -> Result<Arc<ProvenSnapshotFile>, FileMembershipError> {
        self.ensure_lease().await?;
        let path = path.strip_prefix('/').unwrap_or(path);
        if path.is_empty() {
            return Err(not_file("scope root is not a file"));
        }
        if let Some(files) = self.content_membership.get() {
            for (separator, _) in path.match_indices('/') {
                if let Some(ancestor) = files.get(&path[..separator]) {
                    return Err(SnapshotError::new(
                        if ancestor.fs_kind == "symlink" {
                            SnapshotErrorCode::SymlinkTraversal
                        } else {
                            SnapshotErrorCode::NotDirectory
                        },
                        "seeded fixed-root ancestor is not a directory",
                    )
                    .into());
                }
            }
            let file = files.get(path).ok_or_else(|| {
                if self
                    .path_membership
                    .complete_directories
                    .get()
                    .is_some_and(|directories| {
                        directories
                            .binary_search_by(|directory| directory.as_str().cmp(path))
                            .is_ok()
                    })
                {
                    return not_file("seeded fixed-root path is a directory");
                }
                SnapshotError::new(
                    SnapshotErrorCode::PathNotFound,
                    "file absent from seeded fixed root",
                )
                .into()
            })?;
            return Ok(ProvenSnapshotFile::mint(self, file.clone()));
        }
        if !self.capabilities().features.metadata_pages {
            return Err(SnapshotError::new(
                SnapshotErrorCode::SnapshotNotReady,
                "selective membership requires metadata/pages",
            )
            .into());
        }
        let cell = self.path_membership.table.lock().unwrap().get(path)?;
        let token = cell
            .token
            .get_or_try_init(|| async {
                let _flight = Admission::acquire(
                    &self.path_membership.initializers,
                    &process().initializers,
                )?;
                tokio::time::timeout(self.path_membership.deadline, self.prove_path(path))
                    .await
                    .map_err(|_| limit())?
            })
            .await?
            .clone();
        token.validate(self).await?;
        Ok(token)
    }

    /// Current authority and lease still gate an immutable membership token.
    pub async fn read_proven_content(
        &self,
        token: &ProvenSnapshotFile,
        use_frames: bool,
    ) -> Result<Arc<VerifiedContent>, SnapshotError> {
        token.validate(self).await?;
        self.read_owned_file(token.file(), use_frames, &self.content_scope)
            .await
    }

    async fn prove_path(&self, path: &str) -> Result<Arc<ProvenSnapshotFile>, FileMembershipError> {
        let mut root = parse_digest(&self.descriptor().metadata_root)?;
        let mut directory = String::from("/");
        let mut active = std::collections::HashSet::new();
        let mut work = ProofWork::new();
        let mut components = path.split('/').peekable();
        while let Some(component) = components.next() {
            if !active.insert(root) {
                return Err(integrity("cycle in selected directory roots").into());
            }
            let entry = self
                .prove_component(&directory, root, component.as_bytes(), &mut work)
                .await?;
            if components.peek().is_some() {
                if entry.kind == EntryKind::Symlink {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::SymlinkTraversal,
                        "selected ancestor is a symlink; membership does not follow it",
                    )
                    .into());
                }
                if entry.kind != EntryKind::Directory {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::NotDirectory,
                        "selected ancestor is not a directory",
                    )
                    .into());
                }
                root = entry.child_root;
                if directory != "/" {
                    directory.push('/');
                }
                directory.push_str(component);
            } else {
                if entry.kind == EntryKind::Directory {
                    return Err(not_file("selected path is a directory"));
                }
                if entry.size > super::content_profile::MAX_FILE_SIZE
                    || entry.kind == EntryKind::Symlink && !(1..=4095).contains(&entry.size)
                {
                    return Err(limit().into());
                }
                self.ensure_lease().await?;
                return Ok(ProvenSnapshotFile::mint(
                    self,
                    SnapshotFile {
                        rel_path: path.into(),
                        fs_kind: match entry.kind {
                            EntryKind::Regular => "regular",
                            EntryKind::Executable => "executable",
                            EntryKind::Symlink => "symlink",
                            EntryKind::Directory => unreachable!(),
                        }
                        .into(),
                        size: entry.size,
                        content_digest: format!("sha256:{}", hex::encode(entry.content_id)),
                    },
                ));
            }
        }
        Err(integrity("selected file path has no final component").into())
    }

    async fn prove_component(
        &self,
        directory: &str,
        root: [u8; 32],
        name: &[u8],
        work: &mut ProofWork,
    ) -> Result<Entry, SnapshotError> {
        let mut expected = root;
        let mut route = Vec::new();
        let mut chain: Vec<([u8; 32], Vec<u8>)> = Vec::new();
        let mut selected: Option<(Vec<u8>, u8)> = None;
        loop {
            if route.len() > MAX_DEPTH || chain.iter().any(|(id, _)| *id == expected) {
                return Err(integrity("cycle or excessive selected radix depth"));
            }
            work.admit(route.len())?;
            self.ensure_lease().await?;
            let pages = self
                .client()
                .metadata_pages(
                    self.snapshot_id(),
                    &[MetadataPageItem {
                        directory_path: directory.into(),
                        route: route.clone(),
                        expected_digest: Some(format!("sha256:{}", hex::encode(expected))),
                    }],
                    self.encoding_hint(),
                )
                .await?;
            work.received(&pages)?;
            let mut target = None;
            for (id, bytes) in pages {
                if page_id(&bytes) != id {
                    return Err(integrity(
                        "selective witness bytes differ from advertised page id",
                    ));
                }
                if id == expected {
                    if target.replace(bytes).is_some() {
                        return Err(integrity("duplicate selective target page"));
                    }
                } else if let Some((_, prior)) = chain.iter().find(|(known, _)| *known == id) {
                    if *prior != bytes {
                        return Err(integrity(
                            "repeated witness changed committed ancestor bytes",
                        ));
                    }
                    decode_page(&bytes)?;
                } else {
                    return Err(integrity("selective witness returned an unrelated page"));
                }
            }
            let bytes =
                target.ok_or_else(|| integrity("selective witness omitted requested target"))?;
            let page = decode_page(&bytes)?;
            if let Some((parent, label)) = &selected {
                // Both pages have passed arithmetic preflight before this codec API.
                Page::verify_received_child(parent, *label, &bytes).map_err(|_| {
                    integrity("selected child violates committed count or prefix partition")
                })?;
            }
            match &page {
                Page::Leaf { entries } => {
                    if Page::build(entries)
                        .map_err(|_| integrity("invalid selected leaf partition"))?
                        != bytes
                    {
                        return Err(integrity(
                            "selected leaf is not its canonical entry partition",
                        ));
                    }
                    return entries
                        .iter()
                        .find(|entry| entry.name == name)
                        .cloned()
                        .ok_or_else(|| {
                            SnapshotError::new(
                                SnapshotErrorCode::PathNotFound,
                                "file name absent from selected leaf",
                            )
                        });
                }
                Page::Branch {
                    prefix,
                    terminal,
                    children,
                } => {
                    if name == prefix {
                        return terminal.clone().ok_or_else(|| {
                            SnapshotError::new(
                                SnapshotErrorCode::PathNotFound,
                                "file name absent from selected branch terminal",
                            )
                        });
                    }
                    if !name.starts_with(prefix) || name.len() <= prefix.len() {
                        return Err(SnapshotError::new(
                            SnapshotErrorCode::PathNotFound,
                            "file name outside selected branch prefix",
                        ));
                    }
                    let label = name[prefix.len()];
                    let child = children
                        .iter()
                        .find(|child| child.label == label)
                        .ok_or_else(|| {
                            SnapshotError::new(
                                SnapshotErrorCode::PathNotFound,
                                "file name has no selected child",
                            )
                        })?;
                    expected = child.child_page_id;
                    route.push(label);
                    selected = Some((bytes.clone(), label));
                }
            }
            chain.push((page_id(&bytes), bytes));
        }
    }
}

// The fixed table is metadata scope, independent of content payload credits.
const _: () = assert!(size_of::<CellTable>() < 2048);
