//! Independent local retention owners for fixed workspace snapshots.
//!
//! The registry discovers retention candidates, never grants access. Every
//! candidate is bound to the containing authorization scope and audited through
//! its own DurableStore before it can support incremental reuse.

use std::{
    collections::BTreeSet,
    fs,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use super::{
    auth::AuthorizedSnapshotContext,
    durable::{self, DurableStore, ViewMeta},
    frames::parse_digest,
    CompletionKind, SnapshotError, SnapshotErrorCode, SnapshotReader,
};

const OWNER_FILE: &str = "workspace.json";
const REGISTRY_DIR: &str = "workspace-pins";
const REVOKE_FILE: &str = "LOCAL_PIN_REVOKE";
const REVISION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceBinding {
    revision: u8,
    workspace_id: String,
    snapshot_id: String,
    scope: String,
    auth_domain: String,
}

impl WorkspaceBinding {
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    fn scope_dir(&self, root: &Path) -> Result<PathBuf, SnapshotError> {
        canonical_uuid(&self.workspace_id)?;
        let sid = hex::encode(parse_digest(&self.snapshot_id)?);
        let owners = root
            .parent()
            .ok_or_else(|| integrity("workspace root has no parent"))?;
        let view = owners
            .parent()
            .ok_or_else(|| integrity("workspace root has no view"))?;
        let scope = view
            .parent()
            .ok_or_else(|| integrity("workspace root has no scope"))?;
        if self.revision != REVISION
            || root.file_name() != Some(self.workspace_id.as_ref())
            || owners.file_name() != Some("owners".as_ref())
            || view.file_name() != Some(sid.as_ref())
        {
            return Err(integrity(
                "workspace binding disagrees with its fixed cache location",
            ));
        }
        check_directory(owners)?;
        check_directory(view)?;
        check_directory(root)?;
        validate_authority(scope, &self.auth_domain, &self.scope, None)?;
        validate_authority(
            root,
            &self.auth_domain,
            &self.scope,
            Some(&self.snapshot_id),
        )?;
        Ok(scope.to_path_buf())
    }

    fn validate_store(&self, store: &DurableStore) -> Result<PathBuf, SnapshotError> {
        let scope = self.scope_dir(store.root())?;
        if store.content_dir() != scope.join("blobs") {
            return Err(integrity(
                "workspace content store is outside its authorization scope",
            ));
        }
        check_directory(store.content_dir())?;
        validate_authority(store.content_dir(), &self.auth_domain, &self.scope, None)?;
        Ok(scope)
    }

    pub(super) fn validate_view(&self, view: &ViewMeta) -> Result<(), SnapshotError> {
        if self.snapshot_id != view.snapshot_id || self.scope != view.scope {
            return Err(integrity(
                "workspace hydration differs from its fixed owner binding",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RegistrationState {
    Active,
    Released,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    binding: WorkspaceBinding,
    state: RegistrationState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReleasePhase {
    Revoking,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseRecord {
    binding: WorkspaceBinding,
    operation_id: String,
    phase: ReleasePhase,
}

/// A successful, durable revocation of this owner's complete-retention claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReleaseLocalPinReceipt {
    pub workspace_id: String,
    pub snapshot_id: String,
    pub operation_id: String,
}

/// Actual audited local guarantee. A busy store cannot certify completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalPinState {
    Incomplete,
    Complete(CompletionKind),
    Revoking { operation_id: String },
    Revoked { operation_id: String },
    Unknown,
}

pub(super) enum PinAudit {
    Active(String),
    Inactive,
    Unknown,
}

impl DurableStore {
    /// Open one workspace's private metadata with shared, authority-bound CAS.
    /// The UUID cannot be reused for another snapshot within the same scope.
    pub fn open_for_workspace(
        cache_root: impl AsRef<Path>,
        workspace_id: &str,
        reader: &SnapshotReader,
    ) -> Result<Self, SnapshotError> {
        Self::open_workspace_context(
            cache_root.as_ref(),
            workspace_id,
            reader.authorized_context(),
        )
    }

    fn open_workspace_context(
        root: &Path,
        workspace_id: &str,
        context: &AuthorizedSnapshotContext,
    ) -> Result<Self, SnapshotError> {
        canonical_uuid(workspace_id)?;
        let scope = context.scope_cache_dir(root);
        durable::create_dirs_durable(&scope)?;
        context.bind_scope_cache(&scope)?;
        let binding = WorkspaceBinding {
            revision: REVISION,
            workspace_id: workspace_id.into(),
            snapshot_id: context.descriptor().snapshot_id.clone(),
            scope: context.descriptor().scope.clone(),
            auth_domain: context.cache_domain().id().into(),
        };
        if read_registration(&scope, workspace_id)?
            .is_some_and(|previous| previous.binding != binding)
        {
            return Err(integrity(
                "workspace UUID is already bound to another fixed snapshot",
            ));
        }
        let sid = hex::encode(parse_digest(&context.descriptor().snapshot_id)?);
        create_child(&scope, &sid)?;
        let view = scope.join(sid);
        create_child(&view, "owners")?;
        create_child(&view.join("owners"), workspace_id)?;
        let owner_root = view.join("owners").join(workspace_id);
        let content = scope.join("blobs");
        create_child(&scope, "blobs")?;
        context.bind_view_cache(&owner_root)?;
        context.bind_scope_cache(&content)?;
        let store = Self {
            root: owner_root,
            content,
            verification_meters: None,
        };
        let _transaction = store.transaction()?;
        match read_record::<WorkspaceBinding>(&store.root.join(OWNER_FILE))? {
            Some(existing) if existing != binding => {
                return Err(integrity("workspace owner binding changed"))
            }
            Some(_) => {}
            None => {
                if has_guarantee_artifacts(&store.root)? {
                    return Err(integrity(
                        "workspace owner record is missing from existing retention state",
                    ));
                }
                durable::write_atomic(&store.root, OWNER_FILE, &encode(&binding)?)?;
            }
        }
        binding.validate_store(&store)?;
        // Opening an existing released owner must never make its registry active.
        if read_registration(&scope, workspace_id)?.is_none() {
            if has_guarantee_artifacts(&store.root)? {
                return Err(integrity(
                    "workspace registry is missing from existing retention state",
                ));
            }
            register(&store, &binding, RegistrationState::Active)?;
        } else {
            require_registration(&store, &binding)?;
        }
        store.completed_manifest_locked()?;
        Ok(store)
    }

    pub fn workspace_binding(&self) -> Result<Option<WorkspaceBinding>, SnapshotError> {
        owner_binding(self)
    }

    pub fn local_pin_state(&self) -> Result<LocalPinState, SnapshotError> {
        let Some(_transaction) = self.try_transaction()? else {
            return Ok(LocalPinState::Unknown);
        };
        let binding = require_owner(self)?;
        require_registration(self, &binding)?;
        if let Some(record) = release_record(self, &binding)? {
            return Ok(match record.phase {
                ReleasePhase::Revoking => LocalPinState::Revoking {
                    operation_id: record.operation_id,
                },
                ReleasePhase::Revoked => LocalPinState::Revoked {
                    operation_id: record.operation_id,
                },
            });
        }
        if self.completed_manifest_locked()?.is_none() {
            return Ok(LocalPinState::Incomplete);
        }
        let bytes = durable::required_dependency(&self.root.join("DURABLE_COMPLETE"))?;
        Ok(LocalPinState::Complete(
            if durable::completion_revision(&bytes)? == 3 {
                CompletionKind::FullSnapshot
            } else {
                CompletionKind::FileClosure
            },
        ))
    }

    /// Revoke only this workspace's local guarantee. Online reads and other
    /// owners' verified CAS bytes remain available; no content GC is performed.
    /// A retry or reopen continues the original durable release operation.
    pub fn release_local_pin(&self) -> Result<ReleaseLocalPinReceipt, SnapshotError> {
        let _transaction = self.transaction()?;
        let binding = require_owner(self)?;
        let scope = binding.validate_store(self)?;
        require_registration(self, &binding)?;
        let mut record = release_record(self, &binding)?.unwrap_or_else(|| ReleaseRecord {
            binding: binding.clone(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            phase: ReleasePhase::Revoking,
        });
        durable::write_atomic(&self.root, REVOKE_FILE, &encode(&record)?)?;
        durable::durability_checkpoint(&self.root, "release-intent-durable")?;
        self.invalidate_complete()?;
        durable::durability_checkpoint(&self.root, "release-complete-revoked")?;
        remove_durable(&self.root, "pin.json")?;
        durable::durability_checkpoint(&self.root, "release-pin-removed")?;
        register(self, &binding, RegistrationState::Released)?;
        durable::durability_checkpoint(&self.root, "release-registry-released")?;
        super::ScopeCache::open(scope)?
            .drop_records_for_released_owner(&binding, self.verification_meters.as_ref())?;
        durable::durability_checkpoint(&self.root, "release-index-pruned")?;
        record.phase = ReleasePhase::Revoked;
        durable::write_atomic(&self.root, REVOKE_FILE, &encode(&record)?)?;
        durable::durability_checkpoint(&self.root, "release-durable")?;
        Ok(ReleaseLocalPinReceipt {
            workspace_id: binding.workspace_id,
            snapshot_id: binding.snapshot_id,
            operation_id: record.operation_id,
        })
    }
}

pub(super) fn owner_binding(
    store: &DurableStore,
) -> Result<Option<WorkspaceBinding>, SnapshotError> {
    let binding = read_record::<WorkspaceBinding>(&store.root().join(OWNER_FILE))?;
    if binding.is_none()
        && store.root().parent().and_then(Path::file_name) == Some("owners".as_ref())
    {
        return Err(integrity("workspace owner record is missing"));
    }
    if let Some(binding) = &binding {
        binding.validate_store(store)?;
    }
    Ok(binding)
}

fn require_owner(store: &DurableStore) -> Result<WorkspaceBinding, SnapshotError> {
    owner_binding(store)?.ok_or_else(|| {
        SnapshotError::new(
            SnapshotErrorCode::InvalidRequest,
            "local pin release requires an owned workspace store",
        )
    })
}

fn require_registration(
    store: &DurableStore,
    binding: &WorkspaceBinding,
) -> Result<Registration, SnapshotError> {
    let scope = binding.validate_store(store)?;
    let registration = read_registration(&scope, &binding.workspace_id)?
        .ok_or_else(|| integrity("workspace pin registry entry is missing"))?;
    if registration.binding != *binding {
        return Err(integrity("workspace registry binding changed"));
    }
    if registration.state == RegistrationState::Released
        && release_record(store, binding)?.is_none()
    {
        return Err(integrity(
            "released workspace registry has lost its revocation record",
        ));
    }
    Ok(registration)
}

fn release_record(
    store: &DurableStore,
    binding: &WorkspaceBinding,
) -> Result<Option<ReleaseRecord>, SnapshotError> {
    let record = read_record::<ReleaseRecord>(&store.root().join(REVOKE_FILE))?;
    if let Some(record) = &record {
        canonical_uuid(&record.operation_id)?;
        if record.binding != *binding {
            return Err(integrity("local pin revocation binding changed"));
        }
    }
    Ok(record)
}

/// Every completion/reopen path calls this before interpreting a marker.
pub(super) fn complete_allowed(store: &DurableStore) -> Result<bool, SnapshotError> {
    let Some(binding) = owner_binding(store)? else {
        return Ok(true);
    };
    let registration = require_registration(store, &binding)?;
    match read_record::<ViewMeta>(&store.root().join("view.json"))? {
        Some(view) => binding.validate_view(&view)?,
        None => {
            for name in ["DURABLE_COMPLETE", "pin.json"] {
                match fs::symlink_metadata(store.root().join(name)) {
                    Ok(_) => {
                        return Err(integrity(
                            "workspace retention state has lost its fixed view",
                        ))
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(io_error(error)),
                }
            }
        }
    }
    if release_record(store, &binding)?.is_some() {
        return Ok(false);
    }
    Ok(registration.state == RegistrationState::Active)
}

pub(super) fn validate_hydration(
    store: &DurableStore,
    view: &ViewMeta,
) -> Result<(), SnapshotError> {
    if let Some(binding) = owner_binding(store)? {
        binding.validate_view(view)?;
        require_registration(store, &binding)?;
    }
    Ok(())
}

// Caller has durably removed COMPLETE while holding the hydration lock. A
// fresh explicit hydrate may restore a guarantee only by re-verifying bytes.
pub(super) fn begin_hydration(store: &DurableStore) -> Result<(), SnapshotError> {
    if let Some(binding) = owner_binding(store)? {
        release_record(store, &binding)?;
        register(store, &binding, RegistrationState::Active)?;
        remove_durable(store.root(), REVOKE_FILE)?;
    }
    Ok(())
}

pub(super) fn publish_pin(store: &DurableStore, view: &ViewMeta) -> Result<(), SnapshotError> {
    if let Some(binding) = owner_binding(store)? {
        binding.validate_view(view)?;
        if release_record(store, &binding)?.is_some() {
            return Err(integrity("hydration cannot publish a revoked epoch"));
        }
        register(store, &binding, RegistrationState::Active)?;
        durable::durability_checkpoint(store.root(), "workspace-registry-durable")?;
    }
    Ok(())
}

struct RegistryLock(File);
impl Drop for RegistryLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

fn register(
    store: &DurableStore,
    binding: &WorkspaceBinding,
    state: RegistrationState,
) -> Result<(), SnapshotError> {
    let scope = binding.validate_store(store)?;
    create_child(&scope, REGISTRY_DIR)?;
    let dir = scope.join(REGISTRY_DIR);
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join(format!("{}.lock", binding.workspace_id)))
        .map_err(io_error)?;
    let _lock = match file.try_lock() {
        Ok(()) => RegistryLock(file),
        Err(fs::TryLockError::WouldBlock) => {
            return Err(SnapshotError::new(
                SnapshotErrorCode::SnapshotNotReady,
                "workspace registry entry is busy",
            ))
        }
        Err(fs::TryLockError::Error(error)) => return Err(io_error(error)),
    };
    if read_registration(&scope, &binding.workspace_id)?
        .is_some_and(|previous| previous.binding != *binding)
    {
        return Err(integrity(
            "workspace UUID is already bound to another fixed snapshot",
        ));
    }
    durable::write_atomic(
        &dir,
        &format!("{}.json", binding.workspace_id),
        &encode(&Registration {
            binding: binding.clone(),
            state,
        })?,
    )
}

fn read_registration(scope: &Path, id: &str) -> Result<Option<Registration>, SnapshotError> {
    canonical_uuid(id)?;
    let directory = scope.join(REGISTRY_DIR);
    match fs::symlink_metadata(&directory) {
        Ok(_) => check_directory(&directory)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
    }
    read_record(&scope.join(REGISTRY_DIR).join(format!("{id}.json")))
}

/// Fixed registry paths only. A damaged or busy owner blocks cleanup rather
/// than authorizing deletion; it cannot prove a pin for incremental reuse.
pub(super) fn owner_inventory(
    scope: &Path,
    releasing: Option<&WorkspaceBinding>,
    meters: Option<&durable::CasVerificationMeters>,
) -> Result<Vec<(String, PinAudit)>, SnapshotError> {
    let directory = scope.join(REGISTRY_DIR);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            ensure_owners_registered(scope, &BTreeSet::new())?;
            return Ok(vec![]);
        }
        Err(error) => return Err(io_error(error)),
    };
    check_directory(&directory)?;
    let mut out = vec![];
    let mut registered = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(io_error)?;
        if entry.path().extension() != Some("json".as_ref()) {
            continue;
        }
        let name = entry.file_name();
        let id = name
            .to_str()
            .and_then(|name| name.strip_suffix(".json"))
            .ok_or_else(|| integrity("invalid workspace registry filename"))?;
        canonical_uuid(id)?;
        let registration = read_registration(scope, id)?
            .ok_or_else(|| integrity("workspace registry entry disappeared"))?;
        if registration.binding.workspace_id != id {
            return Err(integrity("workspace registry owner changed"));
        }
        let sid = hex::encode(parse_digest(&registration.binding.snapshot_id)?);
        let root = scope.join(&sid).join("owners").join(id);
        let store = DurableStore {
            root,
            content: scope.join("blobs"),
            verification_meters: meters.cloned(),
        };
        let binding = require_owner(&store)?;
        if binding != registration.binding {
            return Err(integrity("registry entry differs from its workspace owner"));
        }
        // The release caller already holds this exact owner's transaction.
        // Audit every other owner, even a Released registry row: a damaged or
        // busy owner remains conservative evidence against pruning.
        let audit = if releasing == Some(&binding) {
            if registration.state != RegistrationState::Released
                || release_record(&store, &binding)?.is_none()
            {
                return Err(integrity(
                    "index pruning requires a durable owner revocation",
                ));
            }
            PinAudit::Inactive
        } else {
            store.audit_pin()?
        };
        if matches!(&audit, PinAudit::Active(id) if id != &binding.snapshot_id) {
            return Err(integrity(
                "audited snapshot differs from its fixed workspace owner",
            ));
        }
        registered.insert((sid, id.to_owned()));
        out.push((binding.snapshot_id, audit));
    }
    ensure_owners_registered(scope, &registered)?;
    Ok(out)
}

// A missing registry row must not hide an existing owner from cleanup. This
// scan discovers fixed owner locations only; it never supplies read authority.
fn ensure_owners_registered(
    scope: &Path,
    registered: &BTreeSet<(String, String)>,
) -> Result<(), SnapshotError> {
    for view in fs::read_dir(scope).map_err(io_error)? {
        let view = view.map_err(io_error)?;
        let name = view.file_name();
        let Some(sid) = name.to_str().filter(|name| {
            name.len() == 64
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }) else {
            continue;
        };
        check_directory(&view.path())?;
        let owners = view.path().join("owners");
        let entries = match fs::read_dir(&owners) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_error(error)),
        };
        check_directory(&owners)?;
        for owner in entries {
            let owner = owner.map_err(io_error)?;
            let name = owner.file_name();
            let id = name
                .to_str()
                .ok_or_else(|| integrity("invalid workspace owner filename"))?;
            canonical_uuid(id)?;
            check_directory(&owner.path())?;
            if !registered.contains(&(sid.to_owned(), id.to_owned())) {
                return Err(integrity(
                    "workspace owner is missing from the retention registry",
                ));
            }
        }
    }
    Ok(())
}

fn validate_authority(
    dir: &Path,
    domain: &str,
    scope: &str,
    snapshot: Option<&str>,
) -> Result<(), SnapshotError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Authority {
        revision: u8,
        domain: String,
        scope: String,
        snapshot_id: Option<String>,
    }
    let authority: Authority = read_record(&dir.join("authority.json"))?
        .ok_or_else(|| integrity("workspace cache authority binding is missing"))?;
    if authority.revision != 1
        || authority.domain != domain
        || authority.scope != scope
        || authority.snapshot_id.as_deref() != snapshot
    {
        return Err(integrity(
            "workspace cache crosses its authorization domain or scope",
        ));
    }
    Ok(())
}

fn canonical_uuid(id: &str) -> Result<(), SnapshotError> {
    let parsed =
        uuid::Uuid::parse_str(id).map_err(|_| integrity("invalid workspace operation UUID"))?;
    if parsed.is_nil() || parsed.to_string() != id {
        return Err(integrity("noncanonical workspace operation UUID"));
    }
    Ok(())
}

fn check_directory(path: &Path) -> Result<(), SnapshotError> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(integrity(
            "workspace cache component is not a real directory",
        ));
    }
    Ok(())
}

fn create_child(parent: &Path, name: &str) -> Result<(), SnapshotError> {
    let path = parent.join(name);
    match fs::create_dir(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(io_error(error)),
    }
    check_directory(&path)?;
    durable::sync_dir(parent)?;
    durable::sync_dir(&path)
}

fn has_guarantee_artifacts(root: &Path) -> Result<bool, SnapshotError> {
    for name in ["DURABLE_COMPLETE", "pin.json", REVOKE_FILE] {
        match fs::symlink_metadata(root.join(name)) {
            Ok(_) => return Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
    }
    Ok(false)
}

fn read_record<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, SnapshotError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
            return Err(integrity("workspace record is not a regular file"))
        }
        Ok(metadata) if metadata.len() > 16 * 1024 => {
            return Err(integrity("workspace record exceeds its local size limit"))
        }
        Ok(_) => {}
    }
    let mut bytes = Vec::new();
    super::secure_fs::open_regular(path)
        .map_err(io_error)?
        .take(16 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() > 16 * 1024 {
        return Err(integrity("workspace record exceeds its local size limit"));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| integrity(format!("invalid workspace record: {error}")))
}

fn encode<T: Serialize>(record: &T) -> Result<Vec<u8>, SnapshotError> {
    serde_json::to_vec(record)
        .map_err(|error| SnapshotError::new(SnapshotErrorCode::Internal, error.to_string()))
}

fn remove_durable(dir: &Path, name: &str) -> Result<(), SnapshotError> {
    match fs::remove_file(dir.join(name)) {
        Ok(()) => durable::sync_dir(dir),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}
fn integrity(message: impl Into<String>) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::IntegrityError, message)
}
fn io_error(error: std::io::Error) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::Internal, error.to_string())
}

#[cfg(test)]
#[path = "workspace_pins_tests.rs"]
mod tests;
