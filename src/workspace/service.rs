use std::{
    collections::HashMap,
    fs,
    future::Future,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    },
};

use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};

use super::{mount::WorkspaceMount, types::*};
use crate::snapshot::{
    fuse::Mst2Fuse,
    upper_diff::{scan_upper, DiffLimits},
    CompletionKind, DurableStore, HydrateReport, LocalPinState, MetadataProofLimits, Mst2Client,
    SnapshotError, SnapshotErrorCode, SnapshotReader,
};

pub struct WorkspaceConfig {
    pub workspace_root: PathBuf,
    pub cache_root: PathBuf,
    pub max_workspaces: usize,
    pub max_operations: usize,
    pub max_hydrations: usize,
    pub lease_seconds: u64,
    pub metadata_limits: MetadataProofLimits,
    pub diff_limits: DiffLimits,
}

impl WorkspaceConfig {
    pub fn new(workspace_root: PathBuf, cache_root: PathBuf) -> Self {
        Self {
            workspace_root,
            cache_root,
            max_workspaces: 32,
            max_operations: 64,
            max_hydrations: 2,
            lease_seconds: 300,
            metadata_limits: MetadataProofLimits::default(),
            diff_limits: DiffLimits::default(),
        }
    }
}

struct Runtime {
    reader: Option<SnapshotReader>,
    store: Option<Arc<DurableStore>>,
    lower: Option<Arc<Mst2Fuse>>,
    mount: Option<WorkspaceMount>,
    mount_state: MountState,
    hydration_state: HydrationState,
    hydrate: Option<JoinHandle<Result<HydrateReport, SnapshotError>>>,
    directory_identity: Option<(u64, u64)>,
    upper_identity: Option<(u64, u64)>,
    mountpoint_identity: Option<(u64, u64)>,
    upper_removed: bool,
    mountpoint_removed: bool,
    last_error: Option<String>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            reader: None,
            store: None,
            lower: None,
            mount: None,
            mount_state: MountState::Creating,
            hydration_state: HydrationState::Idle,
            hydrate: None,
            directory_identity: None,
            upper_identity: None,
            mountpoint_identity: None,
            upper_removed: false,
            mountpoint_removed: false,
            last_error: None,
        }
    }
}

struct Workspace {
    id: String,
    generation: String,
    directory: PathBuf,
    upper: PathBuf,
    mountpoint: PathBuf,
    runtime: Mutex<Runtime>,
    _capacity: OwnedSemaphorePermit,
}

/// Independent v3 ownership; no dictionary, Antares service or in-place
/// refresh is constructed. Different entries have independent lifecycle locks.
pub struct WorkspaceService {
    client: Mst2Client,
    config: WorkspaceConfig,
    entries: StdMutex<HashMap<String, Arc<Workspace>>>,
    workspace_capacity: Arc<Semaphore>,
    operations: Arc<Semaphore>,
    hydrations: Arc<Semaphore>,
    shutting_down: AtomicBool,
}

impl WorkspaceService {
    pub fn new(
        client: Mst2Client,
        mut config: WorkspaceConfig,
    ) -> Result<Arc<Self>, WorkspaceError> {
        if config.max_workspaces == 0
            || config.max_operations == 0
            || config.max_hydrations == 0
            || config.max_workspaces > 4096
            || config.max_operations > 65536
            || config.max_hydrations > 4096
            || config.lease_seconds == 0
        {
            return Err(WorkspaceError::new(
                "INVALID_CONFIG",
                "workspace capacities must be positive",
            ));
        }
        config.workspace_root = prepare_root(&config.workspace_root)?;
        config.cache_root = prepare_root(&config.cache_root)?;
        Ok(Arc::new(Self {
            workspace_capacity: Arc::new(Semaphore::new(config.max_workspaces)),
            operations: Arc::new(Semaphore::new(config.max_operations)),
            hydrations: Arc::new(Semaphore::new(config.max_hydrations)),
            client,
            config,
            entries: StdMutex::new(HashMap::new()),
            shutting_down: AtomicBool::new(false),
        }))
    }

    fn find(&self, id: &str) -> Result<Arc<Workspace>, WorkspaceError> {
        self.entries
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| WorkspaceError::new("WORKSPACE_NOT_FOUND", "unknown workspace"))
    }

    async fn owned<T, F>(&self, future: F) -> Result<T, WorkspaceError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, WorkspaceError>> + Send + 'static,
    {
        let permit = self.operations.clone().try_acquire_owned().map_err(|_| {
            WorkspaceError::new("WORKSPACE_BUSY", "workspace operation capacity exhausted")
        })?;
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(WorkspaceError::new(
                "WORKSPACE_BUSY",
                "workspace service is shutting down",
            ));
        }
        // Control operations finish under an owned, bounded task when their
        // HTTP caller disappears. Native read/write hot paths remain inline.
        tokio::spawn(async move {
            let _permit = permit;
            future.await
        })
        .await
        .map_err(|_| {
            WorkspaceError::new(
                "WORKSPACE_UNKNOWN",
                "workspace operation terminated unexpectedly",
            )
        })?
    }

    pub async fn create(
        self: &Arc<Self>,
        request: CreateWorkspace,
    ) -> Result<WorkspaceStatus, WorkspaceError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(WorkspaceError::new(
                "WORKSPACE_BUSY",
                "workspace service is shutting down",
            ));
        }
        let capacity = self
            .workspace_capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| WorkspaceError::new("WORKSPACE_BUSY", "workspace capacity exhausted"))?;
        let service = self.clone();
        self.owned(async move {
            let id = uuid::Uuid::new_v4().to_string();
            let directory = service.config.workspace_root.join(&id);
            let workspace = Arc::new(Workspace {
                id: id.clone(),
                generation: uuid::Uuid::new_v4().to_string(),
                upper: directory.join("upper"),
                mountpoint: directory.join("mount"),
                directory,
                runtime: Mutex::new(Runtime::default()),
                _capacity: capacity,
            });
            service
                .entries
                .lock()
                .unwrap()
                .insert(id, workspace.clone());
            let mut runtime = workspace.runtime.lock().await;
            let result = service.prepare(&workspace, &mut runtime, &request).await;
            if let Err(error) = result {
                runtime.mount_state = MountState::Failed;
                runtime.last_error = Some(error.to_string());
                // Retain every partial native/cache owner for observable retry.
                return Err(WorkspaceError::new(
                    error.code,
                    format!("workspace {}: {}", workspace.id, error.message),
                ));
            }
            service.observe(&workspace, &mut runtime, false).await
        })
        .await
    }

    async fn prepare(
        &self,
        workspace: &Workspace,
        runtime: &mut Runtime,
        request: &CreateWorkspace,
    ) -> Result<(), WorkspaceError> {
        let full_permit = if matches!(request.delivery, WorkspaceDelivery::Full) {
            Some(self.hydrations.clone().try_acquire_owned().map_err(|_| {
                WorkspaceError::new("WORKSPACE_BUSY", "hydration capacity exhausted")
            })?)
        } else {
            None
        };
        let reader = SnapshotReader::resolve_request(
            self.client.clone(),
            &request.resolve_request(self.config.lease_seconds),
        )
        .await?;
        runtime.reader = Some(reader.clone());
        let cache_root = self.config.cache_root.clone();
        let id = workspace.id.clone();
        let fixed = reader.clone();
        let store = Arc::new(
            tokio::task::spawn_blocking(move || {
                DurableStore::open_for_workspace(cache_root, &id, &fixed)
            })
            .await
            .map_err(|_| WorkspaceError::new("WORKSPACE_UNKNOWN", "cache binding task failed"))??,
        );
        runtime.store = Some(store.clone());
        let lower = Arc::new(
            Mst2Fuse::from_reader_lazy_with_limits(
                reader,
                Some(store),
                self.config.metadata_limits,
            )
            .await?,
        );
        runtime.lower = Some(lower.clone());
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&workspace.directory)?;
        runtime.directory_identity = Some(directory_identity(&workspace.directory)?);
        initialize_workspace_directory(&workspace.directory, 0o700)?;
        // The scaffold has the fixed lower's synthesized root mode/owner.
        // Do not derive it from umask: a later root chmod/chown is a real edit.
        fs::DirBuilder::new().mode(0o700).create(&workspace.upper)?;
        runtime.upper_identity = Some(directory_identity(&workspace.upper)?);
        initialize_workspace_directory(&workspace.upper, 0o755)?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&workspace.mountpoint)?;
        runtime.mountpoint_identity = Some(directory_identity(&workspace.mountpoint)?);
        initialize_workspace_directory(&workspace.mountpoint, 0o755)?;
        runtime.mount =
            Some(WorkspaceMount::new(lower, &workspace.upper, workspace.mountpoint.clone()).await?);
        runtime.mount.as_mut().unwrap().mount().await?;
        runtime.mount_state = MountState::Mounted;
        if let Some(permit) = full_permit {
            spawn_hydrate(runtime, permit);
        }
        Ok(())
    }

    fn start_hydrate_locked(&self, runtime: &mut Runtime) -> Result<(), WorkspaceError> {
        if runtime.hydrate.is_some() {
            return Ok(());
        }
        if runtime.mount_state != MountState::Mounted {
            return Err(WorkspaceError::new(
                "WORKSPACE_NOT_READY",
                "workspace is not mounted",
            ));
        }
        let permit =
            self.hydrations.clone().try_acquire_owned().map_err(|_| {
                WorkspaceError::new("WORKSPACE_BUSY", "hydration capacity exhausted")
            })?;
        spawn_hydrate(runtime, permit);
        Ok(())
    }

    pub async fn start_hydrate(
        self: &Arc<Self>,
        id: &str,
    ) -> Result<WorkspaceStatus, WorkspaceError> {
        let workspace = self.find(id)?;
        let service = self.clone();
        self.owned(async move {
            let mut runtime = workspace.runtime.lock().await;
            reconcile_hydrate(&mut runtime, false).await;
            service.start_hydrate_locked(&mut runtime)?;
            service.observe(&workspace, &mut runtime, false).await
        })
        .await
    }

    pub async fn cancel_hydrate(
        self: &Arc<Self>,
        id: &str,
    ) -> Result<WorkspaceStatus, WorkspaceError> {
        let workspace = self.find(id)?;
        let service = self.clone();
        self.owned(async move {
            let mut runtime = workspace.runtime.lock().await;
            reconcile_hydrate(&mut runtime, true).await;
            service.observe(&workspace, &mut runtime, false).await
        })
        .await
    }

    pub async fn release_local_pin(
        self: &Arc<Self>,
        id: &str,
    ) -> Result<crate::snapshot::ReleaseLocalPinReceipt, WorkspaceError> {
        let workspace = self.find(id)?;
        self.owned(async move {
            let mut runtime = workspace.runtime.lock().await;
            reconcile_hydrate(&mut runtime, true).await;
            let store = runtime
                .store
                .as_ref()
                .ok_or_else(|| {
                    WorkspaceError::new("WORKSPACE_NOT_READY", "workspace has no fixed store")
                })?
                .clone();
            let result = tokio::task::spawn_blocking(move || store.release_local_pin())
                .await
                .map_err(|_| {
                    WorkspaceError::new("WORKSPACE_UNKNOWN", "pin revocation task failed")
                })??;
            runtime.hydration_state = HydrationState::Idle;
            Ok(result)
        })
        .await
    }

    pub async fn status(self: &Arc<Self>, id: &str) -> Result<WorkspaceStatus, WorkspaceError> {
        let workspace = self.find(id)?;
        let service = self.clone();
        self.owned(async move {
            let mut runtime = workspace.runtime.lock().await;
            service.observe(&workspace, &mut runtime, true).await
        })
        .await
    }

    pub async fn list(self: &Arc<Self>) -> Result<Vec<WorkspaceStatus>, WorkspaceError> {
        let entries: Vec<_> = self.entries.lock().unwrap().values().cloned().collect();
        let service = self.clone();
        self.owned(async move {
            let mut result = Vec::with_capacity(entries.len());
            for workspace in entries {
                let mut runtime = workspace.runtime.lock().await;
                // Listing does not block native writes for a full upper scan.
                result.push(service.observe(&workspace, &mut runtime, false).await?);
            }
            Ok(result)
        })
        .await
    }

    async fn observe(
        &self,
        workspace: &Workspace,
        runtime: &mut Runtime,
        scan: bool,
    ) -> Result<WorkspaceStatus, WorkspaceError> {
        reconcile_hydrate(runtime, false).await;
        if runtime.mount_state == MountState::Mounted {
            let ready = match &runtime.mount {
                Some(mount) => mount.is_ready().await,
                None => Ok(false),
            };
            if !matches!(ready, Ok(true)) {
                runtime.mount_state = MountState::Failed;
                runtime.last_error = Some(match ready {
                    Err(error) => error.to_string(),
                    _ => "workspace native mount is no longer ready".into(),
                });
            }
        }
        let local_pin_state = if let Some(store) = &runtime.store {
            let store = store.clone();
            match tokio::task::spawn_blocking(move || store.local_pin_state()).await {
                Ok(Ok(LocalPinState::Complete(CompletionKind::FullSnapshot))) => {
                    PinState::CompleteSnapshot
                }
                Ok(Ok(LocalPinState::Complete(CompletionKind::FileClosure))) => {
                    PinState::FileClosureOnly
                }
                Ok(Ok(LocalPinState::Incomplete)) => PinState::Incomplete,
                Ok(Ok(LocalPinState::Revoking { .. })) => PinState::Revoking,
                Ok(Ok(LocalPinState::Revoked { .. })) => PinState::Released,
                _ => PinState::Unknown,
            }
        } else {
            PinState::Unknown
        };
        // A task's success alone is not the current durable guarantee: release,
        // damaged markers or a busy audit can invalidate the local observation.
        if runtime.hydration_state == HydrationState::Complete
            && local_pin_state != PinState::CompleteSnapshot
        {
            runtime.hydration_state = HydrationState::Idle;
        }
        let dirty_state = if scan {
            match self.dirty(workspace, runtime).await {
                Ok(state) => state,
                Err(error) => {
                    runtime.last_error = Some(error.to_string());
                    DirtyState::Unknown
                }
            }
        } else {
            DirtyState::Unknown
        };
        let lease_state = match runtime
            .reader
            .as_ref()
            .map(SnapshotReader::local_lease_status)
        {
            None => LeaseState::NotResolved,
            Some(Ok(())) => LeaseState::GrantedLocally,
            Some(Err(error)) if error.code == SnapshotErrorCode::LeaseExpired => {
                LeaseState::Expired
            }
            Some(Err(_)) => LeaseState::Failed,
        };
        Ok(WorkspaceStatus {
            workspace_id: workspace.id.clone(),
            generation: workspace.generation.clone(),
            snapshot_id: runtime.reader.as_ref().map(|r| r.snapshot_id().into()),
            mountpoint: workspace.mountpoint.display().to_string(),
            mount_state: runtime.mount_state,
            metadata_ready: runtime.mount_state == MountState::Mounted,
            hydration_state: runtime.hydration_state,
            dirty_state,
            lease_state,
            local_pin_state,
            last_error: runtime.last_error.clone(),
        })
    }

    async fn dirty(
        &self,
        workspace: &Workspace,
        runtime: &Runtime,
    ) -> Result<DirtyState, WorkspaceError> {
        check_private_paths(workspace, runtime)?;
        let state = self.dirty_inner(workspace, runtime).await?;
        check_private_paths(workspace, runtime)?;
        Ok(state)
    }

    async fn dirty_inner(
        &self,
        workspace: &Workspace,
        runtime: &Runtime,
    ) -> Result<DirtyState, WorkspaceError> {
        if runtime.upper_removed {
            return match fs::symlink_metadata(&workspace.upper) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(DirtyState::Clean),
                _ => Err(WorkspaceError::new(
                    "WORKSPACE_UNKNOWN",
                    "removed private upper reappeared",
                )),
            };
        }
        if let (Some(lower), Some(mount)) = (&runtime.lower, &runtime.mount) {
            let pause = mount.fence().pause().await?;
            let diff = scan_upper(lower, &workspace.upper, &pause, self.config.diff_limits).await?;
            pause.ensure_certain()?;
            return Ok(if diff.is_clean() {
                DirtyState::Clean
            } else {
                DirtyState::Dirty
            });
        }
        // No native overlay was exposed on a failed preparation. A host-side
        // edit still counts: use the same bounded scan, including root owner,
        // mode and xattrs. The empty fence has no native writers to retire.
        match fs::symlink_metadata(&workspace.upper) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(DirtyState::Clean),
            Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {
                let Some(lower) = &runtime.lower else {
                    return Ok(DirtyState::Unknown);
                };
                let fence = crate::util::mutation_fence::MutationFence::new(1);
                let pause = fence.pause().await?;
                let diff =
                    scan_upper(lower, &workspace.upper, &pause, self.config.diff_limits).await?;
                pause.ensure_certain()?;
                Ok(if diff.is_clean() {
                    DirtyState::Clean
                } else {
                    DirtyState::Dirty
                })
            }
            _ => Ok(DirtyState::Unknown),
        }
    }

    pub async fn destroy(
        self: &Arc<Self>,
        id: &str,
        policy: DestroyWorkspace,
    ) -> Result<(), WorkspaceError> {
        let workspace = self.find(id)?;
        let service = self.clone();
        self.owned(async move {
            let mut runtime = workspace.runtime.lock().await;
            check_private_paths(&workspace, &runtime)?;
            if let (Some(lower), Some(mount)) = (&runtime.lower, &runtime.mount) {
                let mut pause = mount.fence().pause().await?;
                if runtime.upper_removed {
                    refuse_dirty(service.dirty_removed_upper(&workspace)?, policy)?;
                } else {
                    let diff =
                        scan_upper(lower, &workspace.upper, &pause, service.config.diff_limits)
                            .await?;
                    check_private_paths(&workspace, &runtime)?;
                    refuse_dirty(
                        if diff.is_clean() {
                            DirtyState::Clean
                        } else {
                            DirtyState::Dirty
                        },
                        policy,
                    )?;
                }
                pause.seal()?;
            } else {
                refuse_dirty(service.dirty(&workspace, &runtime).await?, policy)?;
            }
            reconcile_hydrate(&mut runtime, true).await;
            runtime.mount_state = MountState::Retiring;
            if let Some(mount) = &mut runtime.mount {
                if let Err(error) = mount.unmount().await {
                    runtime.last_error = Some(error.to_string());
                    return Err(error.into());
                }
            }
            runtime.mount_state = MountState::Unmounted;
            // Final check follows native owner retirement and explicit recovery.
            // A late dirty result preserves upper and owner for explicit discard.
            refuse_dirty(service.dirty(&workspace, &runtime).await?, policy)?;
            check_retired_mountpoint(&workspace, &runtime)?;
            if let Some(store) = &runtime.store {
                let store = store.clone();
                tokio::task::spawn_blocking(move || store.release_local_pin())
                    .await
                    .map_err(|_| {
                        WorkspaceError::new("WORKSPACE_UNKNOWN", "pin revocation task failed")
                    })??;
            }
            check_private_paths(&workspace, &runtime)?;
            check_retired_mountpoint(&workspace, &runtime)?;
            if let Some(identity) = runtime.directory_identity {
                if directory_identity(&workspace.directory)? != identity {
                    return Err(WorkspaceError::new(
                        "WORKSPACE_UNKNOWN",
                        "private workspace directory was replaced",
                    ));
                }
                // remove_dir_all never follows upper symlinks. The mount has
                // retired and the scan has accounted for every upper entry.
                remove_if_present(&workspace.upper, true)?;
                runtime.upper_removed = true;
                remove_if_present(&workspace.mountpoint, false)?;
                runtime.mountpoint_removed = true;
                fs::remove_dir(&workspace.directory)?;
            }
            service.entries.lock().unwrap().remove(&workspace.id);
            Ok(())
        })
        .await
    }

    /// Shutdown retires mounts and hydration but preserves dirty uppers and
    /// durable pins. It neither discards edits nor silently revokes completion.
    pub async fn shutdown_cleanup(&self) -> Result<(), WorkspaceError> {
        self.shutting_down.store(true, Ordering::Release);
        // Existing detached control operations retain their permits. Join that
        // ownership before taking the entry snapshot, including a canceled
        // create whose native handle has not yet been recorded.
        let _drain = self
            .operations
            .clone()
            .acquire_many_owned(self.config.max_operations as u32)
            .await
            .map_err(|_| {
                WorkspaceError::new("WORKSPACE_UNKNOWN", "control operation drain failed")
            })?;
        let entries: Vec<_> = self.entries.lock().unwrap().values().cloned().collect();
        use futures::StreamExt;
        let mut pending = futures::stream::FuturesUnordered::new();
        for workspace in entries {
            pending.push(async move {
                let mut runtime = workspace.runtime.lock().await;
                reconcile_hydrate(&mut runtime, true).await;
                check_private_paths(&workspace, &runtime)?;
                if let Some(mount) = &mut runtime.mount {
                    match mount.unmount().await {
                        Ok(()) => runtime.mount_state = MountState::Unmounted,
                        Err(error) => {
                            runtime.last_error = Some(error.to_string());
                            return Err(WorkspaceError::from(error));
                        }
                    }
                }
                Ok(())
            });
        }
        let mut failure = None;
        while let Some(result) = pending.next().await {
            if let Err(error) = result {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    fn dirty_removed_upper(&self, workspace: &Workspace) -> Result<DirtyState, WorkspaceError> {
        match fs::symlink_metadata(&workspace.upper) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(DirtyState::Clean),
            _ => Err(WorkspaceError::new(
                "WORKSPACE_UNKNOWN",
                "removed private upper reappeared",
            )),
        }
    }
}

fn spawn_hydrate(runtime: &mut Runtime, permit: OwnedSemaphorePermit) {
    let reader = runtime.reader.as_ref().unwrap().clone();
    let store = runtime.store.as_ref().unwrap().clone();
    runtime.hydration_state = HydrationState::Running;
    runtime.hydrate = Some(tokio::spawn(async move {
        let _permit = permit;
        store.hydrate_snapshot(&reader).await
    }));
}

async fn reconcile_hydrate(runtime: &mut Runtime, cancel: bool) {
    let Some(task) = &mut runtime.hydrate else {
        return;
    };
    if cancel {
        task.abort();
    }
    if !cancel && !task.is_finished() {
        return;
    }
    runtime.hydration_state = match task.await {
        Ok(Ok(report))
            if report.complete && report.completion_kind == CompletionKind::FullSnapshot =>
        {
            HydrationState::Complete
        }
        Ok(Ok(_)) => HydrationState::Idle,
        Ok(Err(error)) => {
            runtime.last_error = Some(error.to_string());
            HydrationState::Failed
        }
        Err(error) if error.is_cancelled() => HydrationState::Cancelled,
        Err(error) => {
            runtime.last_error = Some(error.to_string());
            HydrationState::Failed
        }
    };
    runtime.hydrate = None;
}

fn refuse_dirty(state: DirtyState, policy: DestroyWorkspace) -> Result<(), WorkspaceError> {
    match state {
        DirtyState::Clean => Ok(()),
        DirtyState::Dirty if policy.discard_dirty => Ok(()),
        DirtyState::Dirty => Err(WorkspaceError::new(
            "WORKSPACE_DIRTY",
            "explicit discard_dirty is required",
        )),
        DirtyState::Unknown => Err(WorkspaceError::new(
            "WORKSPACE_UNKNOWN",
            "upper or native ownership could not be proved",
        )),
    }
}

fn prepare_root(path: &Path) -> Result<PathBuf, WorkspaceError> {
    fs::create_dir_all(path)?;
    directory_identity(path)?;
    Ok(fs::canonicalize(path)?)
}

fn initialize_workspace_directory(path: &Path, mode: u32) -> Result<(), WorkspaceError> {
    use std::os::{fd::AsRawFd, unix::fs::OpenOptionsExt};

    // The directory is already privately created and its parent's identity
    // recorded, so even an initialization failure can be cleaned up on retry.
    let directory = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let owner = crate::util::mount_owner::mount_owner();
    let meta = directory.metadata()?;
    use std::os::unix::fs::MetadataExt;
    if (meta.uid(), meta.gid()) != (owner.uid, owner.gid)
        && unsafe { libc::fchown(directory.as_raw_fd(), owner.uid, owner.gid) } != 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    if unsafe { libc::fchmod(directory.as_raw_fd(), mode as libc::mode_t) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

fn check_private_paths(workspace: &Workspace, runtime: &Runtime) -> Result<(), WorkspaceError> {
    check_directory_binding(&workspace.directory, runtime.directory_identity, false)?;
    check_directory_binding(
        &workspace.upper,
        runtime.upper_identity,
        runtime.upper_removed,
    )
}

fn check_retired_mountpoint(
    workspace: &Workspace,
    runtime: &Runtime,
) -> Result<(), WorkspaceError> {
    // The mounted filesystem has its own root identity. Only compare the
    // original plain directory after all native capabilities have retired.
    check_directory_binding(
        &workspace.mountpoint,
        runtime.mountpoint_identity,
        runtime.mountpoint_removed,
    )
}

fn check_directory_binding(
    path: &Path,
    expected: Option<(u64, u64)>,
    removed: bool,
) -> Result<(), WorkspaceError> {
    match fs::symlink_metadata(path) {
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound && (removed || expected.is_none()) =>
        {
            Ok(())
        }
        Ok(meta)
            if !removed
                && expected.is_some()
                && meta.is_dir()
                && !meta.file_type().is_symlink() =>
        {
            use std::os::unix::fs::MetadataExt;
            if Some((meta.dev(), meta.ino())) == expected {
                return Ok(());
            }
            Err(WorkspaceError::new(
                "WORKSPACE_UNKNOWN",
                "private directory was replaced",
            ))
        }
        _ => Err(WorkspaceError::new(
            "WORKSPACE_UNKNOWN",
            "private directory ownership could not be proved",
        )),
    }
}

fn directory_identity(path: &Path) -> Result<(u64, u64), WorkspaceError> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Err(WorkspaceError::new(
            "WORKSPACE_UNKNOWN",
            "workspace directory is not a real directory",
        ));
    }
    Ok((meta.dev(), meta.ino()))
}

fn remove_if_present(path: &Path, recursive: bool) -> Result<(), WorkspaceError> {
    let result = if recursive {
        fs::remove_dir_all(path)
    } else {
        fs::remove_dir(path)
    };
    match result {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
