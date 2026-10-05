//! Single-flight, bounded-concurrency content fetching (spec 11 §5/§7).
//!
//! Waiters merge on content identity (`content_id + size`), never on path:
//! every caller proves membership against the fixed reader's committed
//! metadata root and checks its current lease before joining a download.
//! Deployments without metadata pages keep a separate online request for
//! every caller, preserving the server's per-path authorization check.
//! Queued/running jobs and active callers have process and coordinator count
//! limits. These limits do not account returned body bytes or process RSS.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

use tokio::sync::{oneshot, watch, OnceCell, OwnedSemaphorePermit, Semaphore};

use crate::snapshot::{
    SnapshotError, SnapshotErrorCode, SnapshotFile, SnapshotReader, ValidatedSnapshotClosure,
};

type FetchResult = Result<Arc<super::VerifiedContent>, Arc<SnapshotError>>;

/// Maximum queued/running coordinator content jobs in one process.
pub const MAX_PROCESS_PENDING_JOBS: usize = 256;
/// Maximum active coordinator fetch calls in one process.
pub const MAX_PROCESS_ACTIVE_CALLERS: usize = 1024;
/// Maximum queued/running content jobs in one coordinator.
pub const MAX_COORDINATOR_PENDING_JOBS: usize = 64;
/// Maximum active fetch calls in one coordinator.
pub const MAX_COORDINATOR_ACTIVE_CALLERS: usize = 256;

/// Validated count limits. A policy may lower, but never raise, the hard caps.
#[derive(Debug, Clone, Copy)]
pub struct FetchCoordinatorLimits {
    max_pending_jobs: usize,
    max_active_callers: usize,
}

impl FetchCoordinatorLimits {
    pub fn new(max_pending_jobs: usize, max_active_callers: usize) -> Result<Self, SnapshotError> {
        if !(1..=MAX_COORDINATOR_PENDING_JOBS).contains(&max_pending_jobs)
            || !(1..=MAX_COORDINATOR_ACTIVE_CALLERS).contains(&max_active_callers)
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::InvalidRequest,
                "fetch count limits must be positive and within coordinator hard caps",
            ));
        }
        Ok(Self {
            max_pending_jobs,
            max_active_callers,
        })
    }
}

impl Default for FetchCoordinatorLimits {
    fn default() -> Self {
        Self {
            max_pending_jobs: MAX_COORDINATOR_PENDING_JOBS,
            max_active_callers: MAX_COORDINATOR_ACTIVE_CALLERS,
        }
    }
}

/// Momentary count observations backed by permit ownership. Fields are sampled
/// separately; this is a diagnostic snapshot, not an admission decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchCoordinatorCounts {
    pub pending_jobs: usize,
    pub active_callers: usize,
}

struct ProcessAdmission {
    jobs: Arc<Semaphore>,
    callers: Arc<Semaphore>,
}

fn process_admission() -> &'static ProcessAdmission {
    static ADMISSION: OnceLock<ProcessAdmission> = OnceLock::new();
    ADMISSION.get_or_init(|| ProcessAdmission {
        jobs: Arc::new(Semaphore::new(MAX_PROCESS_PENDING_JOBS)),
        callers: Arc::new(Semaphore::new(MAX_PROCESS_ACTIVE_CALLERS)),
    })
}

struct CountAdmission {
    _local: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

impl CountAdmission {
    fn acquire(local: &Arc<Semaphore>, global: &Arc<Semaphore>) -> Result<Self, SnapshotError> {
        // A failed second acquisition drops the first permit. No admission
        // waiter is enqueued, and no live job is evicted to make room.
        let local = local
            .clone()
            .try_acquire_owned()
            .map_err(|_| count_limit())?;
        let global = global
            .clone()
            .try_acquire_owned()
            .map_err(|_| count_limit())?;
        Ok(Self {
            _local: local,
            _global: global,
        })
    }
}

fn count_limit() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::LimitExceeded,
        "fetch count admission limit reached",
    )
}

struct Flight {
    generation: u64,
    // The sender lives through the actual leader future, even after its map
    // entry is detached. Late subscriptions observe the latched cancellation.
    cancel: watch::Sender<bool>,
}

struct PendingFlight {
    flight: Arc<Flight>,
    waiters: HashMap<u64, oneshot::Sender<FetchResult>>,
}

#[derive(Default)]
struct Inflight {
    entries: HashMap<String, PendingFlight>,
    next_id: u64,
}

impl Inflight {
    fn id(&mut self) -> Result<u64, SnapshotError> {
        self.next_id = self.next_id.checked_add(1).ok_or_else(|| {
            SnapshotError::new(
                SnapshotErrorCode::Internal,
                "fetch identity space exhausted",
            )
        })?;
        Ok(self.next_id)
    }
}

fn same_flight(a: &Arc<Flight>, b: &Arc<Flight>) -> bool {
    a.generation == b.generation && Arc::ptr_eq(a, b)
}

struct WaiterRegistration {
    coordinator: Arc<FetchCoordinator>,
    key: String,
    flight: Arc<Flight>,
    id: u64,
    _caller: CountAdmission,
}

impl Drop for WaiterRegistration {
    fn drop(&mut self) {
        // All mutable flight state uses this one lock. Joining, last-waiter
        // detachment and completion therefore have a single atomic order.
        let mut map = self.coordinator.cleanup_lock();
        if let Some(pending) = map.entries.get_mut(&self.key) {
            if same_flight(&pending.flight, &self.flight) {
                pending.waiters.remove(&self.id);
                if pending.waiters.is_empty() {
                    map.entries.remove(&self.key);
                    self.flight.cancel.send_replace(true);
                }
            }
        }
    }
}

struct JobGuard {
    coordinator: Arc<FetchCoordinator>,
    key: String,
    flight: Arc<Flight>,
    _job: CountAdmission,
    completed: bool,
}

impl JobGuard {
    fn take_waiters(&self) -> HashMap<u64, oneshot::Sender<FetchResult>> {
        let mut map = self.coordinator.cleanup_lock();
        if map
            .entries
            .get(&self.key)
            .is_some_and(|p| same_flight(&p.flight, &self.flight))
        {
            map.entries
                .remove(&self.key)
                .map(|p| p.waiters)
                .unwrap_or_default()
        } else {
            HashMap::new()
        }
    }

    fn complete(mut self, result: Result<Arc<super::VerifiedContent>, SnapshotError>) {
        let waiters = self.take_waiters();
        self.completed = true;
        let shared = result.map_err(Arc::new);
        for waiter in waiters.into_values() {
            let _ = waiter.send(shared.clone());
        }
    }
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let waiters = self.take_waiters();
        let error = Arc::new(SnapshotError::new(
            crate::snapshot::SnapshotErrorCode::Internal,
            "fetch task ended without a result",
        ));
        for waiter in waiters.into_values() {
            let _ = waiter.send(Err(error.clone()));
        }
    }
}

/// Coordinates verified file fetches over one [`SnapshotReader`].
pub struct FetchCoordinator {
    reader: SnapshotReader,
    membership: OnceCell<HashMap<String, SnapshotFile>>,
    inflight: Mutex<Inflight>,
    semaphore: Arc<Semaphore>,
    jobs: Arc<Semaphore>,
    callers: Arc<Semaphore>,
    limits: FetchCoordinatorLimits,
    content: Arc<super::content::ContentBudget>,
}

impl FetchCoordinator {
    /// `max_concurrent` bounds simultaneous leader downloads; waiters do
    /// not hold permits.
    pub fn new(reader: SnapshotReader, max_concurrent: usize) -> Arc<Self> {
        Self::new_with_limits(reader, max_concurrent, FetchCoordinatorLimits::default())
    }

    /// Reject excess callers/jobs immediately with `LimitExceeded`.
    /// `max_concurrent` is clamped to `1..=limits.max_pending_jobs`.
    pub fn new_with_limits(
        reader: SnapshotReader,
        max_concurrent: usize,
        limits: FetchCoordinatorLimits,
    ) -> Arc<Self> {
        Self::with_membership(
            reader,
            max_concurrent,
            limits,
            super::ContentBudgetLimits::default(),
            None,
        )
    }

    /// Configure independent retained-output and managed-construction budgets.
    /// Count admission and byte admission both reject excess work immediately.
    pub fn new_with_budgets(
        reader: SnapshotReader,
        max_concurrent: usize,
        limits: FetchCoordinatorLimits,
        content_limits: super::ContentBudgetLimits,
    ) -> Arc<Self> {
        Self::with_membership(reader, max_concurrent, limits, content_limits, None)
    }

    /// Reuse a complete root proof already acquired for this fixed reader.
    /// Only file facts are retained; the closure cannot select a different
    /// descriptor or replace any caller's current lease/credential checks.
    pub fn with_verified_closure(
        reader: SnapshotReader,
        closure: &ValidatedSnapshotClosure,
        max_concurrent: usize,
    ) -> Result<Arc<Self>, SnapshotError> {
        Self::with_verified_closure_and_limits(
            reader,
            closure,
            max_concurrent,
            FetchCoordinatorLimits::default(),
        )
    }

    pub fn with_verified_closure_and_limits(
        reader: SnapshotReader,
        closure: &ValidatedSnapshotClosure,
        max_concurrent: usize,
        limits: FetchCoordinatorLimits,
    ) -> Result<Arc<Self>, SnapshotError> {
        Self::with_verified_closure_and_budgets(
            reader,
            closure,
            max_concurrent,
            limits,
            super::ContentBudgetLimits::default(),
        )
    }

    pub fn with_verified_closure_and_budgets(
        reader: SnapshotReader,
        closure: &ValidatedSnapshotClosure,
        max_concurrent: usize,
        limits: FetchCoordinatorLimits,
        content_limits: super::ContentBudgetLimits,
    ) -> Result<Arc<Self>, SnapshotError> {
        closure.matches_descriptor(reader.descriptor())?;
        Ok(Self::with_membership(
            reader,
            max_concurrent,
            limits,
            content_limits,
            Some(membership_index(closure)),
        ))
    }

    fn with_membership(
        reader: SnapshotReader,
        max_concurrent: usize,
        limits: FetchCoordinatorLimits,
        content_limits: super::ContentBudgetLimits,
        membership: Option<HashMap<String, SnapshotFile>>,
    ) -> Arc<Self> {
        Arc::new(FetchCoordinator {
            reader,
            membership: OnceCell::new_with(membership),
            inflight: Mutex::new(Inflight::default()),
            semaphore: Arc::new(Semaphore::new(
                max_concurrent.clamp(1, limits.max_pending_jobs),
            )),
            jobs: Arc::new(Semaphore::new(limits.max_pending_jobs)),
            callers: Arc::new(Semaphore::new(limits.max_active_callers)),
            limits,
            content: super::content::ContentBudget::new(content_limits),
        })
    }

    pub fn counts(&self) -> FetchCoordinatorCounts {
        FetchCoordinatorCounts {
            pending_jobs: self.limits.max_pending_jobs - self.jobs.available_permits(),
            active_callers: self.limits.max_active_callers - self.callers.available_permits(),
        }
    }

    pub fn content_usage(&self) -> super::ContentBudgetUsage {
        self.content.usage()
    }

    pub fn process_content_usage() -> super::ContentBudgetUsage {
        super::content::ContentBudget::process_usage()
    }

    pub fn process_counts() -> FetchCoordinatorCounts {
        let global = process_admission();
        FetchCoordinatorCounts {
            pending_jobs: MAX_PROCESS_PENDING_JOBS - global.jobs.available_permits(),
            active_callers: MAX_PROCESS_ACTIVE_CALLERS - global.callers.available_permits(),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inflight>, SnapshotError> {
        self.inflight
            .lock()
            .map_err(|_| SnapshotError::new(SnapshotErrorCode::Internal, "fetch state poisoned"))
    }

    fn cleanup_lock(&self) -> MutexGuard<'_, Inflight> {
        // Cleanup must not panic again while unwinding a failed leader.
        self.inflight.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Fetch one file's verified bytes, merging concurrent identical
    /// requests. Every returned success has passed whole-file verification
    /// in the reader (object/chunk rehash).
    pub async fn fetch(
        self: &Arc<Self>,
        file: SnapshotFile,
        use_frames: bool,
    ) -> Result<Arc<super::VerifiedContent>, SnapshotError> {
        // A canonical path is not a membership proof. A caller cannot bypass
        // its own fixed-view check by naming another leader's content id.
        self.reader
            .authorized_context()
            .validate_relative_path(&file.rel_path)?;
        if file.size > crate::snapshot::client::MAX_BUFFERED_FILE_BYTES
            || usize::try_from(file.size).is_err()
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "whole-file buffered read exceeds the local 64 MiB budget; use range reads",
            ));
        }
        let caller = CountAdmission::acquire(&self.callers, &process_admission().callers)?;
        if !self.reader.capabilities().features.metadata_pages {
            // Without a root-verified proof, retain each caller's online
            // fixed-SID/path request. No other caller may supply its bytes
            // or its authorization result; concurrency is still bounded.
            self.reader.ensure_lease().await?;
            let _job = CountAdmission::acquire(&self.jobs, &process_admission().jobs)?;
            return self.lead(&file, use_frames, None).await;
        }
        self.validate_membership(&file).await?;
        self.reader.ensure_lease().await?;
        let key = format!("{}:{}", file.content_digest, file.size);
        let (tx, rx) = oneshot::channel();
        let (registration, job) = {
            let mut map = self.lock()?;
            let id = map.id()?;
            let (flight, job) = match map.entries.get_mut(&key) {
                Some(pending) => {
                    pending.waiters.insert(id, tx);
                    (pending.flight.clone(), None)
                }
                None => {
                    let job = CountAdmission::acquire(&self.jobs, &process_admission().jobs)?;
                    let generation = map.id()?;
                    let (cancel, _) = watch::channel(false);
                    let flight = Arc::new(Flight { generation, cancel });
                    map.entries.insert(
                        key.clone(),
                        PendingFlight {
                            flight: flight.clone(),
                            waiters: HashMap::from([(id, tx)]),
                        },
                    );
                    let guard = JobGuard {
                        coordinator: self.clone(),
                        key: key.clone(),
                        flight: flight.clone(),
                        _job: job,
                        completed: false,
                    };
                    (flight, Some(guard))
                }
            };
            (
                WaiterRegistration {
                    coordinator: self.clone(),
                    key,
                    flight,
                    id,
                    _caller: caller,
                },
                job,
            )
        };
        if let Some(guard) = job {
            // The first caller is a waiter too: dropping its future must not
            // cancel a download that another caller still needs.
            tokio::spawn(async move {
                let mut cancel = guard.flight.cancel.subscribe();
                let result = tokio::select! {
                    biased;
                    _ = cancelled(&mut cancel) => Err(SnapshotError::new(SnapshotErrorCode::Internal, "fetch has no live waiters")),
                    result = guard.coordinator.lead(&file, use_frames, Some(guard.flight.cancel.subscribe())) => result,
                };
                guard.complete(result);
            });
        }
        let result = rx
            .await
            .map_err(|_| {
                SnapshotError::new(
                    crate::snapshot::SnapshotErrorCode::Internal,
                    "fetch task disappeared without a result",
                )
            })?
            .map_err(|e| (*e).clone());
        drop(registration);
        result
    }

    async fn validate_membership(&self, file: &SnapshotFile) -> Result<(), SnapshotError> {
        let files = self
            .membership
            .get_or_try_init(|| async {
                let closure = self.reader.snapshot_closure().await?;
                // Keep the fixed-root-derived path index, not duplicate page
                // bytes. A failed/cancelled initialization can be retried.
                Ok::<_, SnapshotError>(membership_index(&closure))
            })
            .await?;
        let path = file.rel_path.strip_prefix('/').unwrap_or(&file.rel_path);
        let expected = files.get(path).ok_or_else(|| {
            SnapshotError::new(
                SnapshotErrorCode::PathNotFound,
                format!("{} is not a file in the fixed snapshot", file.rel_path),
            )
        })?;
        let same_kind = file.fs_kind == expected.fs_kind
            || (file.fs_kind == "file" && expected.fs_kind == "regular");
        if file.content_digest != expected.content_digest
            || file.size != expected.size
            || !same_kind
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!("{} differs from its committed file metadata", file.rel_path),
            ));
        }
        Ok(())
    }

    async fn lead(
        &self,
        file: &SnapshotFile,
        use_frames: bool,
        cancellation: Option<watch::Receiver<bool>>,
    ) -> Result<Arc<super::VerifiedContent>, SnapshotError> {
        let _permit = self.semaphore.clone().acquire_owned().await.map_err(|e| {
            SnapshotError::new(
                crate::snapshot::SnapshotErrorCode::Internal,
                format!("fetch semaphore closed: {e}"),
            )
        })?;
        if cancellation
            .as_ref()
            .is_some_and(|receiver| *receiver.borrow())
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::Internal,
                "fetch has no live waiters",
            ));
        }
        let bytes = self
            .reader
            .read_owned_file(file, use_frames, &self.content)
            .await?;
        // The fetch paths verify content; the size must also match the view.
        if bytes.len() as u64 != file.size {
            return Err(SnapshotError::new(
                crate::snapshot::SnapshotErrorCode::DigestMismatch,
                format!(
                    "{}: fetched size {} != view size {}",
                    file.rel_path,
                    bytes.len(),
                    file.size
                ),
            ));
        }
        Ok(bytes)
    }
}

async fn cancelled(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

fn membership_index(closure: &ValidatedSnapshotClosure) -> HashMap<String, SnapshotFile> {
    closure
        .files()
        .iter()
        .cloned()
        .map(|file| (file.rel_path.clone(), file))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_identity_space_returns_error_without_wrapping() {
        let mut state = Inflight {
            next_id: u64::MAX - 1,
            ..Inflight::default()
        };
        assert_eq!(state.id().unwrap(), u64::MAX);
        for _ in 0..2 {
            assert_eq!(state.id().unwrap_err().code, SnapshotErrorCode::Internal);
            assert_eq!(state.next_id, u64::MAX);
        }
    }

    #[test]
    fn failed_global_admission_returns_partial_local_permit() {
        let local = Arc::new(Semaphore::new(1));
        let global = Arc::new(Semaphore::new(1));
        let occupied = global.clone().try_acquire_owned().unwrap();
        for _ in 0..2 {
            assert_eq!(
                CountAdmission::acquire(&local, &global).err().unwrap().code,
                SnapshotErrorCode::LimitExceeded
            );
            assert_eq!(local.available_permits(), 1);
            assert_eq!(global.available_permits(), 0);
        }
        drop(occupied);
        let admission = CountAdmission::acquire(&local, &global).unwrap();
        assert_eq!(
            (local.available_permits(), global.available_permits()),
            (0, 0)
        );
        drop(admission);
        assert_eq!(
            (local.available_permits(), global.available_permits()),
            (1, 1)
        );
    }
}
