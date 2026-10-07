//! Bounded disk admission and fixed-root retention for managed v3 scopes.
//!
//! Retention proves which names must survive; it grants neither access nor
//! completeness. Existing COMPLETE, lease and whole-body SHA checks remain.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
    time::{Duration, Instant},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{
    durable::{self, DurableStore},
    frames::parse_digest,
    secure_fs,
    workspace_pins::WorkspaceBinding,
    SnapshotError, SnapshotErrorCode, ValidatedSnapshotClosure,
};

const POLICY: &str = "cache-capacity.json";
const LEDGER: &str = "cache-charges.json";
const FENCE: &str = "cache-lifecycle.lock";
const ADMISSION_LOCK: &str = "cache-admission.lock";
const OWNER_ADMISSION_LOCK: &str = "cache-owner-admission.lock";
const INTENTS: &str = "cache-writers";
const USE_RECORD: &str = "CACHE_USE.json";
const RETENTION_DIR: &str = "retention";
const ENTRY_CHARGE: u64 = 4096;
const CONTROL_BYTES: u64 = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheLimits {
    pub max_bytes: u64,
    pub proof_headroom_bytes: u64,
    pub max_entries: usize,
    pub max_owners: usize,
    pub max_inventory_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_root_nodes: usize,
    pub max_record_bytes: usize,
    pub max_scan_millis: u64,
    pub max_delete_entries: usize,
    pub max_delete_bytes: u64,
}

impl Default for CacheLimits {
    fn default() -> Self {
        Self {
            max_bytes: 64 * 1024 * 1024 * 1024,
            proof_headroom_bytes: 256 * 1024 * 1024,
            max_entries: 2_000_000,
            max_owners: 4096,
            max_inventory_bytes: 256 * 1024 * 1024,
            max_metadata_bytes: 128 * 1024 * 1024,
            max_root_nodes: 1_000_000,
            max_record_bytes: 16 * 1024 * 1024,
            max_scan_millis: 2000,
            max_delete_entries: 128,
            // A single admitted large object must remain collectable. Entry and
            // wall-clock limits bound work; unlink never reads body bytes.
            max_delete_bytes: 64 * 1024 * 1024 * 1024,
        }
    }
}

impl CacheLimits {
    pub(super) fn validate(&self) -> Result<(), SnapshotError> {
        if self.max_bytes <= self.proof_headroom_bytes
            || self.proof_headroom_bytes < CONTROL_BYTES + 8 * ENTRY_CHARGE
            || self.max_entries == 0
            || self.max_owners == 0
            || self.max_inventory_bytes == 0
            || self.max_metadata_bytes == 0
            || self.max_root_nodes == 0
            || self.max_record_bytes == 0
            || self.max_scan_millis == 0
            || Instant::now()
                .checked_add(Duration::from_millis(self.max_scan_millis))
                .is_none()
            || self.max_delete_entries == 0
            || self.max_delete_bytes == 0
        {
            return Err(limit("invalid managed cache capacity limits"));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapacityPolicy {
    revision: u8,
    limits: CacheLimits,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Charges {
    revision: u8,
    bytes: u64,
    entries: usize,
}

/// A stable shared lock must live in the actual IO job, including a blocking
/// job whose awaiting task was cancelled. Acquisitions never wait on an inner
/// owner/index lock; collector contention is a retryable, zero-delete result.
pub(super) struct ScopeIoGuard(File);
impl Drop for ScopeIoGuard {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

fn lock(scope: &Path, name: &str, shared: bool) -> Result<ScopeIoGuard, SnapshotError> {
    let file = secure_fs::open_rw_create(&scope.join(name)).map_err(io_error)?;
    lock_file(file, shared)
}

fn lock_file(file: File, shared: bool) -> Result<ScopeIoGuard, SnapshotError> {
    let result = if shared {
        file.try_lock_shared()
    } else {
        file.try_lock()
    };
    match result {
        Ok(()) => Ok(ScopeIoGuard(file)),
        Err(fs::TryLockError::WouldBlock) => Err(busy("managed cache lifecycle is busy")),
        Err(fs::TryLockError::Error(error)) => Err(io_error(error)),
    }
}

pub(super) fn managed_scope(path: &Path) -> Result<Option<PathBuf>, SnapshotError> {
    // Only repository-defined layouts are at most eight levels below scope.
    // A present malformed policy is an error, never an unmanaged fallback.
    for directory in path.ancestors().take(9) {
        match fs::symlink_metadata(directory.join(POLICY)) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                policy(directory)?;
                return Ok(Some(directory.to_path_buf()));
            }
            Ok(_) => return Err(integrity("managed cache policy is not a regular file")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
    }
    Ok(None)
}

pub(super) fn io_guard(path: &Path) -> Result<Option<ScopeIoGuard>, SnapshotError> {
    managed_scope(path)?
        .map(|scope| lock(&scope, FENCE, true))
        .transpose()
}

fn policy(scope: &Path) -> Result<CacheLimits, SnapshotError> {
    let policy: CapacityPolicy = read_record(&scope.join(POLICY), CONTROL_BYTES as usize)?;
    if policy.revision != 1 {
        return Err(integrity("unsupported managed cache policy"));
    }
    policy.limits.validate()?;
    Ok(policy.limits)
}

fn entry_capacity(limits: CacheLimits, emergency: bool, proof: bool) -> usize {
    let headroom = usize::try_from(limits.proof_headroom_bytes / ENTRY_CHARGE)
        .unwrap_or(usize::MAX)
        .min(limits.max_entries / 4)
        .max(1);
    if emergency {
        limits.max_entries
    } else if proof {
        limits.max_entries.saturating_sub((headroom / 4).max(1))
    } else {
        limits.max_entries.saturating_sub(headroom)
    }
}

/// Enable capacity after the reader has bound the scope authority. Reusing
/// the same policy requires only a shared fence; initialization is exclusive.
pub fn configure_scope(scope: &Path, limits: CacheLimits) -> Result<(), SnapshotError> {
    limits.validate()?;
    match fs::symlink_metadata(scope.join(POLICY)) {
        Ok(_) => {
            // A configured scope already owns its stable lifecycle lock. Do
            // not create records, scan inventory or reset charges on reopen.
            let metadata = fs::symlink_metadata(scope).map_err(io_error)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(integrity("managed cache scope is not a real directory"));
            }
            let fence =
                secure_fs::open_regular_nonblocking(&scope.join(FENCE)).map_err(io_error)?;
            let _fence = lock_file(fence, true)?;
            super::auth::validate_retention_scope(scope)?;
            if policy(scope)? == limits {
                return Ok(());
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(error)),
    }
    secure_fs::create_dir_all_no_symlink(scope).map_err(io_error)?;
    let _fence = lock(scope, FENCE, false)?;
    super::auth::validate_retention_scope(scope)?;
    let _admission = lock(scope, ADMISSION_LOCK, false)?;
    match fs::symlink_metadata(scope.join(POLICY)) {
        Ok(_) if policy(scope)? == limits => return Ok(()),
        Ok(_) => {
            return Err(busy(
                "managed cache capacity policy differs from its existing scope",
            ))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(error)),
    }
    let inventory = Inventory::scan(scope, limits)?;
    let bytes = inventory
        .bytes
        .checked_add(8 * ENTRY_CHARGE + CONTROL_BYTES)
        .ok_or_else(|| limit("managed cache byte count overflow"))?;
    if bytes > limits.max_bytes || inventory.entries.saturating_add(8) > limits.max_entries {
        return Err(limit("existing cache exceeds managed capacity"));
    }
    secure_fs::create_dir_all_no_symlink(&scope.join(INTENTS)).map_err(io_error)?;
    raw_atomic(
        scope,
        LEDGER,
        &Charges {
            revision: 1,
            bytes,
            entries: inventory.entries + 8,
        },
    )?;
    raw_atomic(
        scope,
        POLICY,
        &CapacityPolicy {
            revision: 1,
            limits,
        },
    )
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WriterIntent {
    revision: u8,
    operation_id: String,
    relative_directory: String,
    final_name: String,
    temporary_name: String,
    maximum_bytes: u64,
}

/// A distinct physical temporary write is charged even when another writer
/// has the same digest. Charges stay conservative after completion/cancellation;
/// only a fresh quiescent inventory resets them. Admission is O(1), not a cache
/// scan per object. Intent and quota become durable before temporary creation.
pub(super) struct WriteAdmission {
    _fence: ScopeIoGuard,
    scope: PathBuf,
    directory: PathBuf,
    intent_name: Option<String>,
    temporary_name: String,
    _budget: Option<Arc<HydrationBudget>>,
}

impl WriteAdmission {
    pub(super) fn begin(
        directory: &Path,
        final_name: &str,
        maximum_bytes: u64,
    ) -> Result<Option<Self>, SnapshotError> {
        Self::begin_with_budget(
            directory,
            final_name,
            maximum_bytes,
            directory_budget(directory),
        )
    }

    pub(super) fn begin_with_budget(
        directory: &Path,
        final_name: &str,
        maximum_bytes: u64,
        budget: Option<Arc<HydrationBudget>>,
    ) -> Result<Option<Self>, SnapshotError> {
        let Some(scope) = managed_scope(directory)? else {
            return Ok(None);
        };
        let fence = lock(&scope, FENCE, true)?;
        let limits = policy(&scope)?;
        validate_child(final_name)?;
        if let Some(budget) = budget {
            if budget.scope != scope {
                return Err(integrity("hydration reservation crosses managed scopes"));
            }
            budget.consume(
                maximum_bytes
                    .checked_add(CONTROL_BYTES + 3 * ENTRY_CHARGE)
                    .ok_or_else(|| limit("cache reservation overflow"))?,
            )?;
            // The peak has already been durably reserved once. Unique child
            // intent names need no shared quota transaction or per-file scan.
            return Self::declared(
                fence,
                scope,
                directory,
                final_name,
                maximum_bytes,
                Some(budget),
            )
            .map(Some);
        }
        let _admission = lock(&scope, ADMISSION_LOCK, false)?;
        let relative = directory
            .strip_prefix(&scope)
            .map_err(|_| integrity("writer escaped managed scope"))?;
        let emergency_control = final_name == USE_RECORD
            || final_name == "LOCAL_PIN_REVOKE"
            || relative
                .file_name()
                .is_some_and(|name| name == "workspace-pins");
        let proof_control = relative
            .components()
            .any(|component| component.as_os_str() == RETENTION_DIR);
        let mut charges: Charges = read_record(&scope.join(LEDGER), CONTROL_BYTES as usize)?;
        if charges.revision != 1 {
            return Err(integrity("unsupported cache charge ledger"));
        }
        let charge = maximum_bytes
            .checked_add(CONTROL_BYTES + 3 * ENTRY_CHARGE)
            .ok_or_else(|| limit("cache write reservation overflow"))?;
        charges.bytes = charges
            .bytes
            .checked_add(charge)
            .ok_or_else(|| limit("cache charge ledger overflow"))?;
        charges.entries = charges
            .entries
            .checked_add(3)
            .ok_or_else(|| limit("cache entry charge overflow"))?;
        let capacity = if emergency_control {
            limits.max_bytes
        } else if proof_control {
            limits.max_bytes - limits.proof_headroom_bytes / 4
        } else {
            limits.max_bytes - limits.proof_headroom_bytes
        };
        if charges.bytes > capacity
            || charges.entries > entry_capacity(limits, emergency_control, proof_control)
        {
            return Err(limit("managed cache capacity exhausted; collect and retry"));
        }
        // If this process dies between these two records it only overcharges.
        raw_atomic(&scope, LEDGER, &charges)?;
        Self::declared(
            fence,
            scope.clone(),
            directory,
            final_name,
            maximum_bytes,
            None,
        )
        .map(Some)
    }

    fn declared(
        fence: ScopeIoGuard,
        scope: PathBuf,
        directory: &Path,
        final_name: &str,
        maximum_bytes: u64,
        budget: Option<Arc<HydrationBudget>>,
    ) -> Result<Self, SnapshotError> {
        let relative_directory = directory
            .strip_prefix(&scope)
            .map_err(|_| integrity("writer escaped managed scope"))?
            .to_str()
            .ok_or_else(|| integrity("managed cache directory is not UTF-8"))?
            .replace('\\', "/");
        let operation_id = uuid::Uuid::new_v4().to_string();
        let intent_name = format!("{operation_id}.json");
        let temporary_name = format!(".{final_name}.tmp.{operation_id}");
        let intent = WriterIntent {
            revision: 1,
            operation_id,
            relative_directory,
            final_name: final_name.to_owned(),
            temporary_name: temporary_name.clone(),
            maximum_bytes,
        };
        raw_atomic(&scope.join(INTENTS), &intent_name, &intent)?;
        Ok(Self {
            _fence: fence,
            scope,
            directory: directory.to_path_buf(),
            intent_name: Some(intent_name),
            temporary_name,
            _budget: budget,
        })
    }

    pub(super) fn temporary_path(&self) -> PathBuf {
        self.directory.join(&self.temporary_name)
    }
}

impl Drop for WriteAdmission {
    fn drop(&mut self) {
        // Drop occurs only after the actual FD job has retired. On cleanup or
        // sync failure leave the durable intent for conservative recovery.
        let temporary = self.temporary_path();
        let removed = match fs::remove_file(&temporary) {
            Ok(()) => durable::sync_dir(&self.directory).is_ok(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => true,
            Err(_) => false,
        };
        if let (true, Some(intent_name)) = (removed, &self.intent_name) {
            let intents = self.scope.join(INTENTS);
            if fs::remove_file(intents.join(intent_name)).is_ok() {
                let _ = durable::sync_dir(&intents);
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HydrationIntent {
    revision: u8,
    operation_id: String,
    binding: WorkspaceBinding,
    maximum_bytes: u64,
}

/// One upfront reservation owns all owner-control and content temporary IO.
/// Subwriters keep this Arc inside their actual jobs. Owner metadata helpers
/// find only this same locked owner's weak entry; CAS writers pass it explicitly.
pub(super) struct HydrationBudget {
    _fence: ScopeIoGuard,
    scope: PathBuf,
    owner_root: PathBuf,
    operation_id: String,
    remaining: Mutex<u64>,
    proof: bool,
}

type OwnerBudgets = Mutex<BTreeMap<PathBuf, Weak<HydrationBudget>>>;
fn owner_budgets() -> &'static OwnerBudgets {
    static BUDGETS: OnceLock<OwnerBudgets> = OnceLock::new();
    BUDGETS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn directory_budget(directory: &Path) -> Option<Arc<HydrationBudget>> {
    let budgets = owner_budgets().lock().unwrap();
    directory
        .ancestors()
        .take(4)
        .find_map(|root| budgets.get(root).and_then(Weak::upgrade))
}

pub(super) fn owner_budget(directory: &Path) -> Option<Arc<HydrationBudget>> {
    directory_budget(directory)
}

pub(super) fn inventory_limits(scope: &Path) -> Result<CacheLimits, SnapshotError> {
    if managed_scope(scope)?.is_some() {
        policy(scope)
    } else {
        Ok(CacheLimits::default())
    }
}

impl HydrationBudget {
    pub(super) fn reserve(
        store: &DurableStore,
        manifest: &[super::SnapshotFile],
        closure: Option<&ValidatedSnapshotClosure>,
    ) -> Result<Option<Arc<Self>>, SnapshotError> {
        let Some(scope) = managed_scope(store.content_dir())? else {
            return Ok(None);
        };
        let fence = lock(&scope, FENCE, true)?;
        let binding = store
            .workspace_binding()?
            .ok_or_else(|| integrity("managed hydration requires a workspace owner"))?;
        binding.validate_retention_store(store)?;
        let limits = policy(&scope)?;
        if manifest.len() > limits.max_root_nodes {
            return Err(limit("hydration reservation entry budget exceeded"));
        }
        // Reserve all possible writes, including same-digest temporary peak in
        // other owners. Existing regular objects of the correct size are resume
        // candidates; if a whole-SHA audit rejects one, its repair must obtain an
        // additional reservation before creating a temp. A valid COMPLETE has
        // only hits, so control space is secured before revoking its marker.
        let mut unique = BTreeMap::new();
        let mut missing_bytes = 0u64;
        let mut missing_objects = 0u64;
        let manifest_bytes = serialized_size(manifest, limits.max_inventory_bytes as u64)?;
        for file in manifest {
            let digest = hex::encode(parse_digest(&file.content_digest)?);
            if unique.insert(digest.clone(), file.size).is_some() {
                continue;
            }
            let present =
                fs::symlink_metadata(store.content_dir().join(&digest)).is_ok_and(|metadata| {
                    metadata.is_file()
                        && !metadata.file_type().is_symlink()
                        && metadata.len() == file.size
                });
            if !present {
                missing_bytes = missing_bytes
                    .checked_add(file.size)
                    .ok_or_else(|| limit("hydration reservation overflow"))?;
                missing_objects += 1;
            }
        }
        let mut proof_bytes = 0u64;
        let mut proof_pages = 0u64;
        if let Some(closure) = closure {
            for bytes in closure.pages().values() {
                proof_bytes = proof_bytes
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| limit("metadata reservation overflow"))?;
            }
            proof_pages = closure.pages().len() as u64;
        }
        // Journal serialization, two manifests/pin/index and their temporary
        // copies are bounded by the validated input; journal appends are chunked.
        let journal_chunks = manifest_bytes / (128 * 1024) + manifest.len() as u64 / 64 + 2;
        let control_writes = proof_pages
            .checked_add(journal_chunks)
            .and_then(|n| n.checked_add(64))
            .ok_or_else(|| limit("hydration control reservation overflow"))?;
        let charge = missing_bytes
            .checked_add(
                manifest_bytes
                    .checked_mul(16)
                    .ok_or_else(|| limit("hydration metadata reservation overflow"))?,
            )
            .and_then(|n| n.checked_add(proof_bytes.checked_mul(3)?))
            .and_then(|n| {
                n.checked_add(
                    (missing_objects + control_writes)
                        .checked_mul(CONTROL_BYTES + 3 * ENTRY_CHARGE)?,
                )
            })
            .ok_or_else(|| limit("hydration reservation overflow"))?;
        let entries = (missing_objects + control_writes)
            .checked_mul(3)
            .and_then(|n| n.checked_add(8))
            .ok_or_else(|| limit("cache entry reservation overflow"))?;
        Self::reserve_charge(
            store,
            scope,
            fence,
            binding,
            limits,
            charge,
            usize::try_from(entries).map_err(|_| limit("cache entry reservation overflow"))?,
            false,
        )
        .map(Some)
    }

    fn reserve_root(
        store: &DurableStore,
        scope: PathBuf,
        limits: CacheLimits,
        binding: WorkspaceBinding,
        closure: &ValidatedSnapshotClosure,
        record_bytes: usize,
    ) -> Result<Arc<Self>, SnapshotError> {
        let fence = lock(&scope, FENCE, true)?;
        let writes = (closure.pages().len() as u64)
            .checked_add(2)
            .ok_or_else(|| limit("root reservation overflow"))?;
        let bytes = closure.pages().values().try_fold(0u64, |sum, bytes| {
            sum.checked_add(bytes.len() as u64)
                .ok_or_else(|| limit("root reservation overflow"))
        })?;
        let charge = bytes
            .checked_add(closure.descriptor_bytes().len() as u64)
            .and_then(|n| n.checked_add(record_bytes as u64))
            .and_then(|n| n.checked_add(writes.checked_mul(CONTROL_BYTES + 3 * ENTRY_CHARGE)?))
            .ok_or_else(|| limit("root reservation overflow"))?;
        let entries = writes
            .checked_mul(3)
            .and_then(|n| n.checked_add(8))
            .ok_or_else(|| limit("root entry reservation overflow"))?;
        Self::reserve_charge(
            store,
            scope,
            fence,
            binding,
            limits,
            charge,
            usize::try_from(entries).map_err(|_| limit("root entry reservation overflow"))?,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn reserve_charge(
        store: &DurableStore,
        scope: PathBuf,
        fence: ScopeIoGuard,
        binding: WorkspaceBinding,
        limits: CacheLimits,
        charge: u64,
        entries: usize,
        proof: bool,
    ) -> Result<Arc<Self>, SnapshotError> {
        let _admission = lock(&scope, ADMISSION_LOCK, false)?;
        let mut charges: Charges = read_record(&scope.join(LEDGER), CONTROL_BYTES as usize)?;
        if charges.revision != 1 {
            return Err(integrity("unsupported cache charge ledger"));
        }
        charges.bytes = charges
            .bytes
            .checked_add(charge)
            .ok_or_else(|| limit("cache charge ledger overflow"))?;
        charges.entries = charges
            .entries
            .checked_add(entries)
            .ok_or_else(|| limit("cache entry charge overflow"))?;
        let capacity = limits.max_bytes
            - if proof {
                limits.proof_headroom_bytes / 4
            } else {
                limits.proof_headroom_bytes
            };
        if charges.bytes > capacity || charges.entries > entry_capacity(limits, false, proof) {
            return Err(limit(
                "managed hydration capacity exhausted before publication",
            ));
        }
        raw_atomic(&scope, LEDGER, &charges)?;
        let operation_id = uuid::Uuid::new_v4().to_string();
        raw_atomic(
            &scope.join(INTENTS),
            &format!("{operation_id}.hydrate.json"),
            &HydrationIntent {
                revision: 1,
                operation_id: operation_id.clone(),
                binding,
                maximum_bytes: charge,
            },
        )?;
        let budget = Arc::new(Self {
            _fence: fence,
            scope,
            owner_root: store.root().to_path_buf(),
            operation_id,
            remaining: Mutex::new(charge),
            proof,
        });
        owner_budgets()
            .lock()
            .unwrap()
            .insert(store.root().to_path_buf(), Arc::downgrade(&budget));
        Ok(budget)
    }

    fn consume(&self, bytes: u64) -> Result<(), SnapshotError> {
        let mut remaining = self.remaining.lock().unwrap();
        if let Some(next) = remaining.checked_sub(bytes) {
            *remaining = next;
            return Ok(());
        }
        // A corrupt same-sized resume hint may require an unplanned repair.
        // Keep the original charge and obtain extra capacity before temp IO.
        let _admission = lock(&self.scope, ADMISSION_LOCK, false)?;
        let limits = policy(&self.scope)?;
        let mut charges: Charges = read_record(&self.scope.join(LEDGER), CONTROL_BYTES as usize)?;
        if charges.revision != 1 {
            return Err(integrity("unsupported cache charge ledger"));
        }
        charges.bytes = charges
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| limit("cache repair reservation overflow"))?;
        charges.entries = charges
            .entries
            .checked_add(3)
            .ok_or_else(|| limit("cache repair entry reservation overflow"))?;
        let capacity = limits.max_bytes
            - if self.proof {
                limits.proof_headroom_bytes / 4
            } else {
                limits.proof_headroom_bytes
            };
        if charges.bytes > capacity || charges.entries > entry_capacity(limits, false, self.proof) {
            return Err(limit("managed cache repair capacity exhausted"));
        }
        raw_atomic(&self.scope, LEDGER, &charges)?;
        let path = self
            .scope
            .join(INTENTS)
            .join(format!("{}.hydrate.json", self.operation_id));
        let mut intent: HydrationIntent = read_record(&path, CONTROL_BYTES as usize)?;
        intent.maximum_bytes = intent
            .maximum_bytes
            .checked_add(bytes)
            .ok_or_else(|| limit("hydration intent reservation overflow"))?;
        raw_atomic(
            path.parent().unwrap(),
            path.file_name().unwrap().to_str().unwrap(),
            &intent,
        )
    }
}

impl Drop for HydrationBudget {
    fn drop(&mut self) {
        let mut budgets = owner_budgets().lock().unwrap();
        if budgets
            .get(&self.owner_root)
            .is_some_and(|weak| std::ptr::eq(weak.as_ptr(), self))
        {
            budgets.remove(&self.owner_root);
        }
        drop(budgets);
        // Every child has its own durable exact-name intent, including a child
        // whose cleanup fails. No parent UUID grants deletion of other files.
        let directory = self.scope.join(INTENTS);
        if fs::remove_file(directory.join(format!("{}.hydrate.json", self.operation_id))).is_ok() {
            let _ = durable::sync_dir(&directory);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageDependency {
    page_id: String,
    size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceUse {
    revision: u8,
    binding: WorkspaceBinding,
    retired: bool,
    descriptor_digest: Option<String>,
    pages: Vec<PageDependency>,
}

impl DurableStore {
    pub fn open_for_workspace_with_cache_limits(
        cache_root: impl AsRef<Path>,
        workspace_id: &str,
        reader: &super::SnapshotReader,
        limits: CacheLimits,
    ) -> Result<Self, SnapshotError> {
        let scope = reader
            .authorized_context()
            .scope_cache_dir(cache_root.as_ref());
        reader.authorized_context().bind_scope_cache(&scope)?;
        configure_scope(&scope, limits)?;
        Self::open_for_workspace(cache_root, workspace_id, reader)
    }

    pub fn cache_retention_known(&self) -> Result<bool, SnapshotError> {
        let Some(scope) = managed_scope(self.content_dir())? else {
            return Ok(false);
        };
        let _transaction = self.transaction()?;
        let binding = self
            .workspace_binding()?
            .ok_or_else(|| integrity("managed use lacks owner binding"))?;
        let record: WorkspaceUse = read_record(
            &self.root().join(USE_RECORD),
            policy(&scope)?.max_record_bytes,
        )?;
        if record.revision != 1 || record.binding != binding {
            return Err(integrity("cache use binding changed"));
        }
        Ok(!record.retired && record.descriptor_digest.is_some())
    }
    pub(super) fn register_cache_use(
        &self,
        binding: &WorkspaceBinding,
    ) -> Result<(), SnapshotError> {
        if managed_scope(self.content_dir())?.is_none() {
            return Ok(());
        }
        let scope = managed_scope(self.content_dir())?.unwrap();
        let limits = policy(&scope)?;
        let path = self.root().join(USE_RECORD);
        match read_optional_record::<WorkspaceUse>(&path, limits.max_record_bytes)? {
            Some(record)
                if record.revision == 1 && record.binding == *binding && !record.retired =>
            {
                Ok(())
            }
            Some(record) if record.revision == 1 && record.binding == *binding => {
                // Re-exposure must establish fresh use before any lower/upper.
                write_use(
                    self,
                    &WorkspaceUse {
                        retired: false,
                        descriptor_digest: None,
                        pages: vec![],
                        ..record
                    },
                )
            }
            Some(_) => Err(integrity("workspace cache use binding changed")),
            None => write_use(
                self,
                &WorkspaceUse {
                    revision: 1,
                    binding: binding.clone(),
                    retired: false,
                    descriptor_digest: None,
                    pages: vec![],
                },
            ),
        }
    }

    /// Persist metadata-only retention before bodies are published. This is
    /// independent of COMPLETE and cannot certify a single body's integrity.
    pub fn retain_snapshot_root(
        &self,
        closure: &ValidatedSnapshotClosure,
    ) -> Result<(), SnapshotError> {
        let Some(scope) = managed_scope(self.content_dir())? else {
            return Ok(());
        };
        let _transaction = self.transaction()?;
        let binding = self
            .workspace_binding()?
            .ok_or_else(|| integrity("managed retention requires an owned workspace"))?;
        binding.validate_retention_store(self)?;
        if closure.snapshot_id() != binding.snapshot_id()
            || closure.descriptor().scope != binding.scope_name()
        {
            return Err(integrity(
                "retention root differs from workspace fixed view",
            ));
        }
        let limits = policy(&scope)?;
        let current: WorkspaceUse =
            read_record(&self.root().join(USE_RECORD), limits.max_record_bytes)?;
        if current.revision != 1 || current.binding != binding || current.retired {
            return Err(integrity(
                "metadata promotion requires the current unretired workspace use",
            ));
        }
        let mut bytes_total = 0usize;
        if closure.pages().len() > limits.max_entries
            || closure
                .files()
                .len()
                .saturating_add(closure.directories().len())
                > limits.max_root_nodes
        {
            return Err(limit("retention root entry budget exceeded"));
        }
        for bytes in closure.pages().values() {
            bytes_total = bytes_total
                .checked_add(bytes.len())
                .ok_or_else(|| limit("retention metadata size overflow"))?;
        }
        if bytes_total > limits.max_metadata_bytes {
            return Err(limit("retention metadata budget exceeded"));
        }
        if closure
            .pages()
            .len()
            .saturating_mul(128)
            .saturating_add(CONTROL_BYTES as usize)
            > limits.max_record_bytes
        {
            return Err(limit(
                "retention page-record budget exceeded before allocation",
            ));
        }
        let mut pages = Vec::with_capacity(closure.pages().len());
        for (page_id, bytes) in closure.pages() {
            pages.push(PageDependency {
                page_id: page_id.clone(),
                size: bytes.len() as u64,
            });
        }
        let record = WorkspaceUse {
            revision: 1,
            binding,
            retired: false,
            descriptor_digest: Some(durable::digest_of(closure.descriptor_bytes())),
            pages,
        };
        serialized_size(&record, limits.max_record_bytes as u64)?;
        let encoded = serde_json::to_vec(&record).map_err(json_error)?;
        let directory = self.root().join(RETENTION_DIR);
        let pages_dir = directory.join("pages");
        if current == record {
            // The caller supplies a freshly validated fixed-root closure. Read
            // every stored dependency and compare exact bytes to that proof;
            // a known marker alone never skips integrity validation.
            let descriptor_matches =
                read_optional_bytes(&directory.join("descriptor.msd2"), limits.max_record_bytes)?
                    .is_some_and(|bytes| bytes == closure.descriptor_bytes());
            let mut pages_match = descriptor_matches;
            for (page_id, bytes) in closure.pages() {
                let path = pages_dir.join(hex::encode(parse_digest(page_id)?));
                if !read_optional_bytes(&path, mst2_codec::metapage::PAGE_MAX_BYTES)?
                    .is_some_and(|stored| stored == *bytes)
                {
                    pages_match = false;
                }
            }
            if pages_match {
                return Ok(());
            }
        }
        // Reserve the complete metadata publication before the first temporary
        // write. A failed proof admission cannot consume partial proof headroom.
        let _budget = HydrationBudget::reserve_root(
            self,
            scope.clone(),
            limits,
            record.binding.clone(),
            closure,
            encoded.len(),
        )?;
        secure_fs::create_dir_all_no_symlink(&pages_dir).map_err(io_error)?;
        for (page_id, bytes) in closure.pages() {
            let name = hex::encode(parse_digest(page_id)?);
            durable::write_atomic(&pages_dir, &name, bytes)?;
        }
        durable::write_atomic(&directory, "descriptor.msd2", closure.descriptor_bytes())?;
        durable::write_atomic(self.root(), USE_RECORD, &encoded)
    }

    /// Only the service's completed native/dirty-upper destroy path calls this.
    /// Pin release, process exit, lease expiry and shutdown never retire use.
    pub(crate) fn retire_cache_use(&self) -> Result<(), SnapshotError> {
        let Some(scope) = managed_scope(self.content_dir())? else {
            return Ok(());
        };
        let _transaction = self.transaction()?;
        let binding = self
            .workspace_binding()?
            .ok_or_else(|| integrity("managed use lacks workspace binding"))?;
        let limits = policy(&scope)?;
        let mut record: WorkspaceUse =
            read_record(&self.root().join(USE_RECORD), limits.max_record_bytes)?;
        if record.revision != 1 || record.binding != binding {
            return Err(integrity("retired cache use binding changed"));
        }
        if self
            .root()
            .join("DURABLE_COMPLETE")
            .try_exists()
            .map_err(io_error)?
            || self
                .root()
                .join("pin.json")
                .try_exists()
                .map_err(io_error)?
        {
            return Err(integrity("cache use cannot retire a retained complete pin"));
        }
        let owners = super::workspace_pins::discover_retention_owners(
            &scope,
            limits,
            Instant::now() + Duration::from_millis(limits.max_scan_millis),
        )?;
        if !owners
            .iter()
            .any(|owner| owner.binding == binding && owner.released && owner.release_finished)
        {
            return Err(integrity(
                "cache use retirement requires completed owner revocation",
            ));
        }
        record.retired = true;
        write_use(self, &record)
    }
}

pub fn cache_pressure(scope: &Path) -> Result<bool, SnapshotError> {
    if managed_scope(scope)?.is_none() {
        return Ok(false);
    }
    let _fence = lock(scope, FENCE, true)?;
    let limits = policy(scope)?;
    let charges: Charges = read_record(&scope.join(LEDGER), CONTROL_BYTES as usize)?;
    if charges.revision != 1 {
        return Err(integrity("unsupported cache charge ledger"));
    }
    Ok(
        charges.bytes >= (limits.max_bytes - limits.proof_headroom_bytes) / 4 * 3
            || charges.entries >= limits.max_entries / 4 * 3,
    )
}

fn write_use(store: &DurableStore, record: &WorkspaceUse) -> Result<(), SnapshotError> {
    if let Some(scope) = managed_scope(store.content_dir())? {
        serialized_size(record, policy(&scope)?.max_record_bytes as u64)?;
    }
    let bytes = serde_json::to_vec(record).map_err(json_error)?;
    durable::write_atomic(store.root(), USE_RECORD, &bytes)
}

pub(super) fn admit_owner(
    scope: &Path,
    workspace_id: &str,
) -> Result<Option<ScopeIoGuard>, SnapshotError> {
    if managed_scope(scope)?.is_none() {
        return Ok(None);
    }
    let guard = lock(scope, OWNER_ADMISSION_LOCK, false)?;
    let limits = policy(scope)?;
    let owners = super::workspace_pins::discover_retention_owners(
        scope,
        limits,
        Instant::now() + Duration::from_millis(limits.max_scan_millis),
    )?;
    if owners.len() >= limits.max_owners
        && !owners
            .iter()
            .any(|owner| owner.binding.workspace_id() == workspace_id)
    {
        return Err(limit("managed workspace owner capacity exhausted"));
    }
    Ok(Some(guard))
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct CacheCollectionReport {
    pub scanned_entries: usize,
    pub deleted_entries: usize,
    pub deleted_bytes: u64,
    pub retained_bytes: u64,
}

struct CollectionBudget {
    limits: CacheLimits,
    deadline: Instant,
    allocation: usize,
    metadata: usize,
    nodes: usize,
}

impl CollectionBudget {
    fn admit(&mut self, bytes: usize) -> Result<(), SnapshotError> {
        if Instant::now() >= self.deadline {
            return Err(limit("cache collection deadline exceeded"));
        }
        self.allocation = self
            .allocation
            .checked_add(bytes)
            .ok_or_else(|| limit("collection allocation overflow"))?;
        if self.allocation > self.limits.max_inventory_bytes {
            return Err(limit("collection allocation budget exceeded"));
        }
        Ok(())
    }

    fn read(
        &mut self,
        inventory: &Inventory,
        path: &Path,
        maximum: usize,
    ) -> Result<Option<Vec<u8>>, SnapshotError> {
        let Some(expected) = inventory.files.get(path) else {
            return Ok(None);
        };
        let size =
            usize::try_from(expected.size).map_err(|_| limit("cache record size overflow"))?;
        if size > maximum {
            return Err(limit("cache record exceeds its read budget"));
        }
        self.admit(size.saturating_mul(16).saturating_add(512))?;
        let mut file = secure_fs::open_regular_nonblocking(path).map_err(io_error)?;
        if secure_fs::RegularIdentity::from_metadata(&file.metadata().map_err(io_error)?)
            .map_err(io_error)?
            != *expected
        {
            return Err(integrity(
                "cache inventory entry lifetime changed during proof",
            ));
        }
        let mut bytes = Vec::new();
        Read::by_ref(&mut file)
            .take(size as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
        if bytes.len() != size {
            return Err(integrity("cache record changed during proof"));
        }
        Ok(Some(bytes))
    }

    fn required(
        &mut self,
        inventory: &Inventory,
        path: &Path,
        maximum: usize,
    ) -> Result<Vec<u8>, SnapshotError> {
        self.read(inventory, path, maximum)?
            .ok_or_else(|| integrity("cache protection dependency is missing"))
    }
}

/// Collect only after a complete current inventory and an independent canonical
/// proof for every live use. Unknown, corrupt, busy or over-budget scopes return
/// before the first unlink. There is no persisted digest deletion plan.
pub fn collect_scope(scope: &Path) -> Result<CacheCollectionReport, SnapshotError> {
    let absolute_scope = if scope.is_absolute() {
        scope.to_path_buf()
    } else {
        std::env::current_dir().map_err(io_error)?.join(scope)
    };
    let scope = absolute_scope.as_path();
    if managed_scope(scope)?.is_none() {
        return Ok(CacheCollectionReport::default());
    }
    let _fence = lock(scope, FENCE, false)?;
    let _admission = lock(scope, ADMISSION_LOCK, false)?;
    #[cfg(test)]
    collection_hooks::pause(scope);
    super::auth::validate_retention_scope(scope)?;
    super::auth::validate_retention_content_scope(scope)?;
    let limits = policy(scope)?;
    let charges: Charges = read_record(&scope.join(LEDGER), CONTROL_BYTES as usize)?;
    if charges.revision != 1 {
        return Err(integrity("unsupported cache charge ledger"));
    }
    let inventory = Inventory::scan(scope, limits)?;
    let mut budget = CollectionBudget {
        limits,
        deadline: inventory.deadline,
        allocation: inventory.allocation_bytes,
        metadata: 0,
        nodes: 0,
    };
    let owners =
        super::workspace_pins::discover_retention_owners(scope, limits, inventory.deadline)?;
    budget.admit(owners.len().saturating_mul(2048))?;
    let owner_roots: BTreeSet<_> = owners.iter().map(|owner| owner.root.clone()).collect();
    let mut owner_locks = Vec::new();
    let mut retired_roots = BTreeSet::new();
    let mut blobs = BTreeSet::new();
    let mut pages = BTreeSet::new();
    for owner in &owners {
        budget.admit(512)?;
        let file = secure_fs::open_regular_nonblocking(&owner.root.join(".hydrate.lock"))
            .map_err(io_error)?;
        match file.try_lock() {
            Ok(()) => owner_locks.push(ScopeIoGuard(file)),
            Err(fs::TryLockError::WouldBlock) => {
                return Err(busy("cache protection owner is busy"))
            }
            Err(fs::TryLockError::Error(error)) => return Err(io_error(error)),
        }
        let record_bytes = budget.required(
            &inventory,
            &owner.root.join(USE_RECORD),
            limits.max_record_bytes,
        )?;
        let record: WorkspaceUse = serde_json::from_slice(&record_bytes).map_err(json_error)?;
        if record.revision != 1 || record.binding != owner.binding {
            return Err(integrity("cache use identity changed"));
        }
        let has_pin = inventory.files.contains_key(&owner.root.join("pin.json"));
        let has_complete = inventory
            .files
            .contains_key(&owner.root.join("DURABLE_COMPLETE"));
        if record.retired {
            if !owner.released || !owner.release_finished || has_pin || has_complete {
                return Err(integrity(
                    "retired cache use still carries an owner guarantee",
                ));
            }
            retired_roots.insert(owner.root.clone());
            continue;
        }
        let Some(descriptor_digest) = record.descriptor_digest else {
            return Err(busy(
                "live cache use has not acquired its complete metadata proof",
            ));
        };
        parse_digest(&descriptor_digest)?;
        if record.pages.len() > limits.max_entries {
            return Err(limit("protection page count exceeded"));
        }
        let directory = owner.root.join(RETENTION_DIR);
        let descriptor = budget.required(
            &inventory,
            &directory.join("descriptor.msd2"),
            limits.max_record_bytes,
        )?;
        if durable::digest_of(&descriptor) != descriptor_digest {
            return Err(integrity("protection descriptor digest differs"));
        }
        let mut canonical_pages = BTreeMap::new();
        for dependency in record.pages {
            let digest = parse_digest(&dependency.page_id)?;
            let bytes = budget.required(
                &inventory,
                &directory.join("pages").join(hex::encode(digest)),
                mst2_codec::metapage::PAGE_MAX_BYTES,
            )?;
            budget.metadata = budget
                .metadata
                .checked_add(bytes.len())
                .ok_or_else(|| limit("protection metadata overflow"))?;
            if budget.metadata > limits.max_metadata_bytes {
                return Err(limit("aggregate protection metadata exceeded"));
            }
            if bytes.len() as u64 != dependency.size
                || canonical_pages.insert(dependency.page_id, bytes).is_some()
            {
                return Err(integrity(
                    "protection page dependency differs or is repeated",
                ));
            }
        }
        let available = limits.max_inventory_bytes.saturating_sub(budget.allocation);
        let closure = ValidatedSnapshotClosure::from_canonical_pages_bounded(
            &descriptor,
            canonical_pages,
            limits.max_root_nodes.saturating_sub(budget.nodes),
            available,
            Some(budget.deadline),
        )?;
        if closure.snapshot_id() != owner.binding.snapshot_id()
            || closure.descriptor().scope != owner.binding.scope_name()
        {
            return Err(integrity("protection graph differs from fixed workspace"));
        }
        budget.nodes = budget
            .nodes
            .checked_add(closure.files().len())
            .and_then(|n| n.checked_add(closure.directories().len()))
            .ok_or_else(|| limit("protection logical node overflow"))?;
        if budget.nodes > limits.max_root_nodes {
            return Err(limit("aggregate protection logical node budget exceeded"));
        }
        // Stored COMPLETE remains governed by its original whole-body audit.
        // Here it is checked only for internally consistent metadata; no body
        // hash or cached manifest grants this collector root authority.
        validate_pin_metadata(
            &mut budget,
            &inventory,
            &owner.root,
            &closure,
            has_pin,
            has_complete,
        )?;
        for file in closure.files() {
            let name = hex::encode(parse_digest(&file.content_digest)?);
            if !blobs.contains(&name) {
                budget.admit(256)?;
                blobs.insert(name);
            }
        }
        for id in closure.pages().keys() {
            let name = hex::encode(parse_digest(id)?);
            if !pages.contains(&name) {
                budget.admit(256)?;
                pages.insert(name);
            }
        }
    }

    // Every directory must have a known fixed layout, including empty ones.
    for path in &inventory.directories {
        budget.admit(0)?;
        if !valid_directory(scope, path, &owner_roots)? {
            return Err(integrity("unknown cache directory blocks collection"));
        }
    }
    let mut control_temps = BTreeSet::new();
    for (path, identity) in &inventory.files {
        budget.admit(0)?;
        if raw_control_temporary(scope, path)? {
            if identity.size > CONTROL_BYTES {
                return Err(limit(
                    "orphan cache control temporary exceeds its write bound",
                ));
            }
            budget.admit(
                path.as_os_str()
                    .len()
                    .saturating_mul(4)
                    .saturating_add(1024),
            )?;
            control_temps.insert(path.clone());
        }
    }
    let mut orphan_temps = BTreeSet::new();
    let mut orphan_groups: Vec<Vec<PathBuf>> = Vec::new();
    for path in inventory.files.keys().filter(|path| {
        path.parent() == Some(scope.join(INTENTS).as_path()) && !control_temps.contains(*path)
    }) {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| integrity("invalid writer intent filename"))?;
        let bytes = budget.required(&inventory, path, CONTROL_BYTES as usize)?;
        if let Some(id) = name.strip_suffix(".hydrate.json") {
            canonical_uuid(id)?;
            let intent: HydrationIntent = serde_json::from_slice(&bytes).map_err(json_error)?;
            if intent.revision != 1
                || intent.operation_id != id
                || intent.maximum_bytes > limits.max_bytes
                || !owners.iter().any(|owner| owner.binding == intent.binding)
            {
                return Err(integrity("orphan hydration reservation binding differs"));
            }
            orphan_groups.push(vec![path.clone()]);
        } else {
            let id = name
                .strip_suffix(".json")
                .ok_or_else(|| integrity("unknown writer intent entry"))?;
            canonical_uuid(id)?;
            let intent: WriterIntent = serde_json::from_slice(&bytes).map_err(json_error)?;
            if intent.revision != 1
                || intent.operation_id != id
                || intent.maximum_bytes > limits.max_bytes
                || intent.temporary_name != format!(".{}.tmp.{id}", intent.final_name)
                || Path::new(&intent.relative_directory).is_absolute()
                || (!intent.relative_directory.is_empty()
                    && intent.relative_directory.split('/').any(|part| {
                        part.is_empty()
                            || part == "."
                            || part == ".."
                            || part.contains(['\\', '\0'])
                    }))
            {
                return Err(integrity("orphan writer declaration differs"));
            }
            validate_child(&intent.final_name)?;
            validate_child(&intent.temporary_name)?;
            let target = scope
                .join(&intent.relative_directory)
                .join(&intent.final_name);
            if !valid_final_file(scope, &target, &owner_roots)? {
                return Err(integrity(
                    "writer declaration targets an unknown cache location",
                ));
            }
            let temporary = target.parent().unwrap().join(&intent.temporary_name);
            let mut group = Vec::new();
            if let Some(identity) = inventory.files.get(&temporary) {
                if identity.size > intent.maximum_bytes || !orphan_temps.insert(temporary.clone()) {
                    return Err(integrity(
                        "orphan temporary exceeds or duplicates its declaration",
                    ));
                }
                group.push(temporary);
            }
            group.push(path.clone());
            orphan_groups.push(group);
        }
        budget.admit(
            path.as_os_str()
                .len()
                .saturating_mul(4)
                .saturating_add(1024),
        )?;
    }
    let mut candidates = Vec::new();
    for path in inventory.files.keys() {
        budget.admit(0)?;
        if control_temps.contains(path)
            || orphan_temps.contains(path)
            || path.parent() == Some(scope.join(INTENTS).as_path())
        {
            continue;
        }
        if !valid_final_file(scope, path, &owner_roots)? {
            return Err(integrity("unknown cache entry blocks collection"));
        }
        let name = path.file_name().and_then(|name| name.to_str()).unwrap();
        let stale_blob = path.parent() == Some(scope.join("blobs").as_path())
            && canonical_hex(name)
            && !blobs.contains(name);
        let stale_page = path.parent() == Some(scope.join("pages").as_path())
            && canonical_hex(name)
            && !pages.contains(name);
        let retired_metadata = path
            .ancestors()
            .take(8)
            .find(|root| retired_roots.contains(*root))
            .is_some_and(|root| {
                path.starts_with(root.join("metadata"))
                    || path.starts_with(root.join(RETENTION_DIR))
                    || (path.parent() == Some(root)
                        && matches!(
                            name,
                            "descriptor.bin"
                                | "metadata.json"
                                | "manifest.json"
                                | "journal.log"
                                | "offline_grant.json"
                                | "NEEDS_REPAIR"
                        ))
            });
        if control_temps.is_empty() && (stale_blob || stale_page || retired_metadata) {
            budget.admit(path.as_os_str().len().saturating_mul(2).saturating_add(512))?;
            candidates.push(vec![path.clone()]);
        }
    }
    if control_temps.is_empty() {
        candidates.extend(orphan_groups);
    } else {
        // A raw_atomic crash may leave any prefix of a bounded control record.
        // Its bytes are never decoded or adopted as policy, charges or intent.
        // This recovery pass deletes only those exact disposable temporaries;
        // all other entries and live roots were still independently validated.
        candidates.extend(control_temps.into_iter().map(|path| vec![path]));
    }
    let mut selected = Vec::new();
    let mut selected_bytes = 0u64;
    for group in candidates {
        budget.admit(group.len().saturating_mul(512))?;
        let bytes = group.iter().try_fold(0u64, |n, path| {
            n.checked_add(inventory.files[path].size)
                .ok_or_else(|| limit("collection delete size overflow"))
        })?;
        if selected.len().saturating_add(group.len()) > limits.max_delete_entries
            || selected_bytes
                .checked_add(bytes)
                .is_none_or(|n| n > limits.max_delete_bytes)
        {
            continue;
        }
        selected_bytes += bytes;
        selected.extend(group);
    }
    let mut prepared = Vec::new();
    for path in &selected {
        budget.admit(1024)?;
        let entry = secure_fs::prepare_current_regular(
            path.parent().unwrap(),
            path.file_name().unwrap().to_str().unwrap(),
            inventory.files[path],
        )
        .map_err(io_error)?
        .ok_or_else(|| integrity("collection candidate disappeared"))?;
        prepared.push((entry, inventory.files[path].size));
    }
    #[cfg(test)]
    collection_hooks::pause_before_unlink(scope);
    for (entry, _) in &prepared {
        budget.admit(0)?;
        entry.verify().map_err(io_error)?;
    }
    // No awaited tasks or retained stale plans intervene after this full proof.
    let mut report = CacheCollectionReport {
        scanned_entries: inventory.entries,
        retained_bytes: inventory.bytes,
        ..Default::default()
    };
    for (entry, bytes) in prepared {
        if entry.remove().map_err(io_error)? {
            report.deleted_entries += 1;
            report.deleted_bytes += bytes;
            report.retained_bytes -= bytes + ENTRY_CHARGE;
        }
    }
    // A crashed reset can only leave conservative charges. No partial or busy
    // inventory is ever allowed to reduce the ledger.
    raw_atomic(
        scope,
        LEDGER,
        &Charges {
            revision: 1,
            bytes: report
                .retained_bytes
                .checked_add(CONTROL_BYTES + 8 * ENTRY_CHARGE)
                .ok_or_else(|| limit("ledger reset overflow"))?,
            entries: inventory.entries - report.deleted_entries + 8,
        },
    )?;
    Ok(report)
}

fn canonical_hex(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn canonical_uuid(id: &str) -> Result<(), SnapshotError> {
    if uuid::Uuid::parse_str(id).is_ok_and(|value| !value.is_nil() && value.to_string() == id) {
        Ok(())
    } else {
        Err(integrity("cache intent UUID is not canonical"))
    }
}

fn raw_control_temporary(scope: &Path, path: &Path) -> Result<bool, SnapshotError> {
    let relative = parts(scope, path)?;
    let (name, in_intents) = match relative.as_slice() {
        [name] => (*name, false),
        [INTENTS, name] => (*name, true),
        _ => return Ok(false),
    };
    let Some((final_name, id)) = name
        .strip_prefix('.')
        .and_then(|name| name.rsplit_once(".tmp."))
    else {
        return Ok(false);
    };
    if canonical_uuid(id).is_err() {
        return Ok(false);
    }
    if !in_intents {
        return Ok(matches!(final_name, POLICY | LEDGER));
    }
    Ok(final_name
        .strip_suffix(".hydrate.json")
        .or_else(|| final_name.strip_suffix(".json"))
        .is_some_and(|id| canonical_uuid(id).is_ok()))
}

fn parts<'a>(scope: &Path, path: &'a Path) -> Result<Vec<&'a str>, SnapshotError> {
    path.strip_prefix(scope)
        .map_err(|_| integrity("cache path escaped scope"))?
        .iter()
        .map(|part| {
            part.to_str()
                .ok_or_else(|| integrity("cache name is not UTF-8"))
        })
        .collect()
}

fn valid_directory(
    scope: &Path,
    path: &Path,
    owners: &BTreeSet<PathBuf>,
) -> Result<bool, SnapshotError> {
    let parts = parts(scope, path)?;
    Ok(match parts.as_slice() {
        [name] => {
            matches!(*name, "blobs" | "pages" | "workspace-pins" | INTENTS) || canonical_hex(name)
        }
        [sid, "owners"] => canonical_hex(sid),
        [sid, "owners", id] => {
            canonical_hex(sid) && owners.contains(&scope.join(sid).join("owners").join(id))
        }
        [sid, "owners", id, name] => {
            owners.contains(&scope.join(sid).join("owners").join(id))
                && matches!(*name, "metadata" | RETENTION_DIR)
        }
        [sid, "owners", id, RETENTION_DIR, "pages"] => {
            owners.contains(&scope.join(sid).join("owners").join(id))
        }
        _ => false,
    })
}

fn valid_final_file(
    scope: &Path,
    path: &Path,
    owners: &BTreeSet<PathBuf>,
) -> Result<bool, SnapshotError> {
    let parts = parts(scope, path)?;
    let Some((name, parents)) = parts.split_last() else {
        return Ok(false);
    };
    Ok(match parents {
        [] => matches!(
            *name,
            "authority.json"
                | "authority.lock"
                | POLICY
                | LEDGER
                | FENCE
                | ADMISSION_LOCK
                | OWNER_ADMISSION_LOCK
                | "closures.json"
                | "closures.lock"
        ),
        ["blobs"] => canonical_hex(name) || matches!(*name, "authority.json" | "authority.lock"),
        ["pages"] => canonical_hex(name),
        ["workspace-pins"] => name
            .strip_suffix(".json")
            .or_else(|| name.strip_suffix(".lock"))
            .is_some_and(|id| canonical_uuid(id).is_ok()),
        [sid, "owners", id] if owners.contains(&scope.join(sid).join("owners").join(id)) => {
            matches!(
                *name,
                "authority.json"
                    | "authority.lock"
                    | "workspace.json"
                    | ".hydrate.lock"
                    | USE_RECORD
                    | "LOCAL_PIN_REVOKE"
                    | "view.json"
                    | "manifest.json"
                    | "journal.log"
                    | "DURABLE_COMPLETE"
                    | "pin.json"
                    | "offline_grant.json"
                    | "NEEDS_REPAIR"
                    | "descriptor.bin"
                    | "metadata.json"
            )
        }
        [sid, "owners", id, "metadata"]
            if owners.contains(&scope.join(sid).join("owners").join(id)) =>
        {
            canonical_hex(name)
        }
        [sid, "owners", id, RETENTION_DIR]
            if owners.contains(&scope.join(sid).join("owners").join(id)) =>
        {
            *name == "descriptor.msd2"
        }
        [sid, "owners", id, RETENTION_DIR, "pages"]
            if owners.contains(&scope.join(sid).join("owners").join(id)) =>
        {
            canonical_hex(name)
        }
        _ => false,
    })
}

fn validate_pin_metadata(
    budget: &mut CollectionBudget,
    inventory: &Inventory,
    root: &Path,
    closure: &ValidatedSnapshotClosure,
    has_pin: bool,
    has_complete: bool,
) -> Result<(), SnapshotError> {
    if has_complete && !has_pin {
        return Err(integrity("completion has lost its pin metadata"));
    }
    if !has_pin {
        return Ok(());
    }
    let mut records = BTreeMap::new();
    for name in ["view.json", "manifest.json", "pin.json"] {
        records.insert(
            name,
            budget.required(inventory, &root.join(name), budget.limits.max_record_bytes)?,
        );
    }
    if has_complete {
        records.insert(
            "DURABLE_COMPLETE",
            budget.required(
                inventory,
                &root.join("DURABLE_COMPLETE"),
                budget.limits.max_record_bytes,
            )?,
        );
    }
    for name in ["descriptor.bin", "metadata.json", "offline_grant.json"] {
        if let Some(bytes) =
            budget.read(inventory, &root.join(name), budget.limits.max_record_bytes)?
        {
            records.insert(name, bytes);
        }
    }
    if durable::completion_revision(&records["pin.json"])? == 3 {
        for (id, expected) in closure.pages() {
            let path = root.join("metadata").join(hex::encode(parse_digest(id)?));
            let bytes = budget.required(inventory, &path, mst2_codec::metapage::PAGE_MAX_BYTES)?;
            if bytes != *expected {
                return Err(integrity(
                    "complete metadata page differs from fixed canonical graph",
                ));
            }
        }
    }
    durable::audit_retention_metadata(closure, &records)
}

struct Inventory {
    bytes: u64,
    entries: usize,
    allocation_bytes: usize,
    files: BTreeMap<PathBuf, secure_fs::RegularIdentity>,
    directories: BTreeSet<PathBuf>,
    deadline: Instant,
    limits: CacheLimits,
}

impl Inventory {
    fn scan(scope: &Path, limits: CacheLimits) -> Result<Self, SnapshotError> {
        let mut inventory = Self {
            bytes: 0,
            entries: 0,
            allocation_bytes: 0,
            files: BTreeMap::new(),
            directories: BTreeSet::new(),
            deadline: Instant::now() + Duration::from_millis(limits.max_scan_millis),
            limits,
        };
        inventory.scan_directory(scope, 0)?;
        Ok(inventory)
    }

    fn check_work(&self) -> Result<(), SnapshotError> {
        if Instant::now() >= self.deadline {
            return Err(limit("cache inventory deadline exceeded"));
        }
        Ok(())
    }

    fn scan_directory(&mut self, directory: &Path, depth: usize) -> Result<(), SnapshotError> {
        self.check_work()?;
        if depth > 8 {
            return Err(integrity("unknown managed cache directory layout"));
        }
        for entry in fs::read_dir(directory).map_err(io_error)? {
            self.check_work()?;
            let entry = entry.map_err(io_error)?;
            self.entries += 1;
            if self.entries > self.limits.max_entries {
                return Err(limit("cache inventory entry budget exceeded"));
            }
            let metadata = fs::symlink_metadata(entry.path()).map_err(io_error)?;
            if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
                return Err(integrity(
                    "managed cache contains a symlink or special file",
                ));
            }
            self.bytes = self
                .bytes
                .checked_add(
                    ENTRY_CHARGE
                        + if metadata.is_file() {
                            metadata.len()
                        } else {
                            0
                        },
                )
                .ok_or_else(|| limit("cache inventory byte count overflow"))?;
            if self.bytes > self.limits.max_bytes {
                return Err(limit("cache physical inventory exceeds capacity"));
            }
            let path = entry.path();
            self.allocation_bytes = self
                .allocation_bytes
                .checked_add(
                    path.as_os_str().len().saturating_mul(2)
                        + std::mem::size_of::<(PathBuf, u64)>()
                        + 64,
                )
                .ok_or_else(|| limit("cache inventory allocation overflow"))?;
            if self.allocation_bytes > self.limits.max_inventory_bytes {
                return Err(limit("cache inventory allocation budget exceeded"));
            }
            if metadata.is_dir() {
                self.directories.insert(path.clone());
                self.scan_directory(&path, depth + 1)?;
            } else {
                self.files.insert(
                    path,
                    secure_fs::RegularIdentity::from_metadata(&metadata).map_err(io_error)?,
                );
            }
        }
        Ok(())
    }
}

fn read_record<T: DeserializeOwned>(path: &Path, maximum: usize) -> Result<T, SnapshotError> {
    read_optional_record(path, maximum)?.ok_or_else(|| integrity("managed cache record is missing"))
}

fn read_optional_record<T: DeserializeOwned>(
    path: &Path,
    maximum: usize,
) -> Result<Option<T>, SnapshotError> {
    read_optional_bytes(path, maximum)?
        .map(|bytes| serde_json::from_slice(&bytes).map_err(json_error))
        .transpose()
}

fn read_optional_bytes(path: &Path, maximum: usize) -> Result<Option<Vec<u8>>, SnapshotError> {
    let mut file = match secure_fs::open_regular_nonblocking(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
    };
    if file.metadata().map_err(io_error)?.len() > maximum as u64 {
        return Err(limit("managed cache record size budget exceeded"));
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() > maximum {
        return Err(limit("managed cache record grew beyond its budget"));
    }
    Ok(Some(bytes))
}

fn raw_atomic<T: Serialize>(directory: &Path, name: &str, value: &T) -> Result<(), SnapshotError> {
    validate_child(name)?;
    let bytes = serde_json::to_vec(value).map_err(json_error)?;
    if bytes.len() as u64 > CONTROL_BYTES {
        return Err(limit("cache control record exceeds budget"));
    }
    let temporary = directory.join(format!(".{name}.tmp.{}", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = secure_fs::open_create_new(&temporary).map_err(io_error)?;
        file.write_all(&bytes).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        fs::rename(&temporary, directory.join(name)).map_err(io_error)?;
        durable::sync_dir(directory)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn validate_child(name: &str) -> Result<(), SnapshotError> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', '\0']) {
        return Err(integrity("invalid managed cache child name"));
    }
    Ok(())
}

pub(super) fn serialized_size<T: Serialize + ?Sized>(
    value: &T,
    maximum: u64,
) -> Result<u64, SnapshotError> {
    struct Counter {
        bytes: u64,
        maximum: u64,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len() as u64)
                .filter(|bytes| *bytes <= self.maximum)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "cache serialization budget exceeded",
                    )
                })?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, maximum };
    serde_json::to_writer(&mut counter, value)
        .map_err(|_| limit("cache serialization budget exceeded"))?;
    Ok(counter.bytes)
}

fn integrity(message: impl Into<String>) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::IntegrityError, message)
}
fn limit(message: impl Into<String>) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::LimitExceeded, message)
}
fn busy(message: impl Into<String>) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::SnapshotNotReady, message)
}
fn io_error(error: io::Error) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::Internal, error.to_string())
}
fn json_error(error: serde_json::Error) -> SnapshotError {
    integrity(format!("invalid managed cache record: {error}"))
}

#[cfg(all(test, unix))]
#[path = "cache_retention_tests.rs"]
mod tests;

#[cfg(test)]
mod collection_hooks {
    use super::*;
    type Hook = (
        tokio::sync::oneshot::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    );
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum Stage {
        Locked,
        Prepared,
    }
    fn hooks() -> &'static Mutex<BTreeMap<(PathBuf, Stage), Hook>> {
        static HOOKS: OnceLock<Mutex<BTreeMap<(PathBuf, Stage), Hook>>> = OnceLock::new();
        HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
    }
    pub(super) struct Barrier {
        entered: Option<tokio::sync::oneshot::Receiver<()>>,
        release: Option<std::sync::mpsc::Sender<()>>,
    }
    impl Barrier {
        pub(super) async fn entered(&mut self) {
            self.entered.take().unwrap().await.unwrap();
        }
        pub(super) fn release(mut self) {
            let _ = self.release.take().unwrap().send(());
        }
    }
    impl Drop for Barrier {
        fn drop(&mut self) {
            if let Some(release) = self.release.take() {
                let _ = release.send(());
            }
        }
    }
    pub(super) fn install(scope: &Path) -> Barrier {
        install_stage(scope, Stage::Locked)
    }
    pub(super) fn install_before_unlink(scope: &Path) -> Barrier {
        install_stage(scope, Stage::Prepared)
    }
    fn install_stage(scope: &Path, stage: Stage) -> Barrier {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        assert!(hooks()
            .lock()
            .unwrap()
            .insert((scope.to_path_buf(), stage), (entered_tx, release_rx))
            .is_none());
        Barrier {
            entered: Some(entered_rx),
            release: Some(release_tx),
        }
    }
    pub(super) fn pause(scope: &Path) {
        pause_stage(scope, Stage::Locked)
    }
    pub(super) fn pause_before_unlink(scope: &Path) {
        pause_stage(scope, Stage::Prepared)
    }
    fn pause_stage(scope: &Path, stage: Stage) {
        let hook = hooks()
            .lock()
            .unwrap()
            .remove(&(scope.to_path_buf(), stage));
        if let Some((entered, release)) = hook {
            let _ = entered.send(());
            let _ = release.recv();
        }
    }
}
