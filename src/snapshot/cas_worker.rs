//! Bounded local CAS work. A cancelled waiter never refunds a running job's
//! actual buffers or permits, and only the primitive can report a CAS miss.

use std::{
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc, OnceLock,
    },
    time::Instant,
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use super::{
    cas_index::LocalCasRangeMeters, fuse_owned::ReplyAdmission, SnapshotError, SnapshotErrorCode,
    SnapshotReader,
};
use crate::util::read_profile::{Metric, ReadProfile};

const LOCAL_OUTSTANDING: usize = 16;
const LOCAL_RUNNING: usize = 2;
const PROCESS_OUTSTANDING: usize = 64;
const PROCESS_RUNNING: usize = 4;
const PENDING: u8 = 0;
const RUNNING: u8 = 1;
const CANCELLED: u8 = 2;
const FINISHED: u8 = 3;

struct ProcessAdmission {
    outstanding: Arc<Semaphore>,
    running: Arc<Semaphore>,
}

fn process_admission() -> Arc<ProcessAdmission> {
    static PROCESS: OnceLock<Arc<ProcessAdmission>> = OnceLock::new();
    PROCESS
        .get_or_init(|| {
            Arc::new(ProcessAdmission {
                outstanding: Arc::new(Semaphore::new(PROCESS_OUTSTANDING)),
                running: Arc::new(Semaphore::new(PROCESS_RUNNING)),
            })
        })
        .clone()
}

/// Shared by reader clones and mounts using their same content budget. The
/// process gate bounds this component's jobs, not every Tokio blocking task.
pub(crate) struct CasReadScope {
    outstanding: Arc<Semaphore>,
    running: Arc<Semaphore>,
    process: Arc<ProcessAdmission>,
}

/// Local constructors establish access before enabling byte reads. This is
/// not an online lease or a grant that expires again on each local read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LocalCasAccess {
    CallerEstablished,
    GrantCheckedOnReopen,
}

enum AccessCheck<F> {
    Online(F),
    Local(LocalCasAccess),
}

impl<F: Fn() -> Result<(), SnapshotError>> AccessCheck<F> {
    fn check(&self) -> Result<(), SnapshotError> {
        match self {
            Self::Online(lease) => lease(),
            Self::Local(
                LocalCasAccess::CallerEstablished | LocalCasAccess::GrantCheckedOnReopen,
            ) => Ok(()),
        }
    }
}

pub(crate) struct WorkResult<T> {
    pub(crate) result: Result<T, SnapshotError>,
    // Small-object work has no range meters; never report it as zero I/O.
    pub(crate) meters: Option<LocalCasRangeMeters>,
    outcome: &'static str,
}

impl<T> WorkResult<Option<T>> {
    pub(crate) fn local(
        result: Result<Option<T>, SnapshotError>,
        meters: Option<LocalCasRangeMeters>,
    ) -> Self {
        let outcome = match &result {
            Ok(Some(_)) => "hit",
            Ok(None) => "miss",
            Err(_) => "error",
        };
        Self {
            result,
            meters,
            outcome,
        }
    }
}

pub(crate) struct Completion<T> {
    pub(crate) result: Result<T, SnapshotError>,
    pub(crate) admission: ReplyAdmission,
}

#[derive(Clone, Copy)]
pub(crate) struct RequestMeters {
    pub(crate) kind: &'static str,
    pub(crate) wanted: u64,
}

struct JobPermits {
    _local_outstanding: OwnedSemaphorePermit,
    _process_outstanding: OwnedSemaphorePermit,
    _local_running: OwnedSemaphorePermit,
    _process_running: OwnedSemaphorePermit,
}

// Pending -> Running and Pending -> Cancelled have a single atomic winner.
// Once started, cancelling the waiter only detaches; it does not stop I/O.
struct CancelOnDrop {
    state: Arc<AtomicU8>,
    id: u64,
    kind: &'static str,
    profile: Option<Arc<ReadProfile>>,
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        let result =
            self.state
                .compare_exchange(PENDING, CANCELLED, Ordering::AcqRel, Ordering::Acquire);
        if result.is_ok() || result == Err(RUNNING) {
            if let Some(profile) = &self.profile {
                profile.add(
                    if result.is_ok() {
                        Metric::WorkerCancelledPending
                    } else {
                        Metric::WorkerDetached
                    },
                    1,
                );
            }
            tracing::debug!(
                target: "scorpiofs::workspace::performance",
                job_id = self.id, kind = self.kind,
                cancelled_before_start = result.is_ok(),
                detached_running = result == Err(RUNNING),
                "local CAS waiter dropped"
            );
        }
    }
}

struct JobTrace {
    state: Arc<AtomicU8>,
    request: RequestMeters,
    id: u64,
    queued: Option<Instant>,
    started: Option<Instant>,
    outcome: &'static str,
    meters: Option<LocalCasRangeMeters>,
    profile_work: ProfileWork,
}

// Moved into the actual blocking job. A detached waiter cannot finish it.
struct ProfileWork(Option<Arc<ReadProfile>>);
impl Drop for ProfileWork {
    fn drop(&mut self) {
        if let Some(profile) = &self.0 {
            profile.worker_leave();
        }
    }
}
impl Drop for JobTrace {
    fn drop(&mut self) {
        self.state.store(FINISHED, Ordering::Release);
        if let Some(profile) = &self.profile_work.0 {
            let outcome = if std::thread::panicking() {
                Some(Metric::WorkerPanic)
            } else {
                match self.outcome {
                    "hit" => Some(Metric::WorkerBackingHit),
                    "miss" => Some(Metric::WorkerBackingMiss),
                    "lease_error" => Some(Metric::WorkerLeaseRejected),
                    // The waiter owns the pending-cancellation counter.
                    "cancelled_before_start" => None,
                    _ => Some(Metric::WorkerError),
                }
            };
            if let Some(outcome) = outcome {
                profile.add(outcome, 1);
            }
            if let (Some(queued), Some(started)) = (self.queued, self.started) {
                profile.add_durations(&[
                    (Metric::WorkerQueueNs, started.duration_since(queued)),
                    (Metric::WorkerWallNs, started.elapsed()),
                ]);
            }
            if let Some(meters) = self.meters {
                profile.add_many(&[
                    (Metric::LargeCasReadBytes, meters.bytes_read),
                    (Metric::LargeWholeHashBytes, meters.whole_sha256_bytes),
                    (Metric::LargeChunkHashBytes, meters.chunk_sha256_bytes),
                    (Metric::LargeCasAppendBytes, meters.output_append_bytes),
                    (Metric::LargeIndexHit, u64::from(meters.index_hit)),
                    (Metric::LargeIndexBuilt, u64::from(meters.index_built)),
                    (
                        Metric::LargeStrictFallback,
                        u64::from(meters.strict_fallback),
                    ),
                ]);
            }
        }
        if let (Some(queued), Some(started)) = (self.queued, self.started) {
            let meters = self.meters.unwrap_or_default();
            tracing::debug!(
                target: "scorpiofs::workspace::performance",
                job_id = self.id,
                kind = self.request.kind,
                wanted = self.request.wanted,
                queue_wait_us = started.duration_since(queued).as_micros().min(u64::MAX as u128) as u64,
                worker_wall_us = started.elapsed().as_micros().min(u64::MAX as u128) as u64,
                outcome = if std::thread::panicking() { "panic" } else { self.outcome },
                range_meters_available = self.meters.is_some(),
                bytes_read = meters.bytes_read,
                whole_sha256_bytes = meters.whole_sha256_bytes,
                chunk_sha256_bytes = meters.chunk_sha256_bytes,
                index_hit = meters.index_hit,
                index_built = meters.index_built,
                strict_fallback = meters.strict_fallback,
                "local CAS worker finished"
            );
        }
    }
}

impl CasReadScope {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            outstanding: Arc::new(Semaphore::new(LOCAL_OUTSTANDING)),
            running: Arc::new(Semaphore::new(LOCAL_RUNNING)),
            process: process_admission(),
        })
    }

    #[cfg(test)]
    async fn run_checked<T: Send + 'static>(
        &self,
        lease: impl Fn() -> Result<(), SnapshotError> + Send + Sync + 'static,
        admission: ReplyAdmission,
        request: RequestMeters,
        work: impl FnOnce() -> WorkResult<T> + Send + 'static,
    ) -> Result<Completion<T>, SnapshotError> {
        self.run_admitted(AccessCheck::Online(lease), admission, request, None, work)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn run_local<T: Send + 'static>(
        &self,
        access: LocalCasAccess,
        admission: ReplyAdmission,
        request: RequestMeters,
        work: impl FnOnce() -> WorkResult<T> + Send + 'static,
    ) -> Result<Completion<T>, SnapshotError> {
        tracing::debug!(
            target: "scorpiofs::workspace::performance", mode = ?access,
            "local CAS uses constructor-established access"
        );
        self.run_admitted(
            AccessCheck::<fn() -> Result<(), SnapshotError>>::Local(access),
            admission,
            request,
            None,
            work,
        )
        .await
    }

    pub(crate) async fn run_profiled<T: Send + 'static>(
        &self,
        reader: SnapshotReader,
        admission: ReplyAdmission,
        request: RequestMeters,
        profile: Option<Arc<ReadProfile>>,
        work: impl FnOnce() -> WorkResult<T> + Send + 'static,
    ) -> Result<Completion<T>, SnapshotError> {
        self.run_admitted(
            AccessCheck::Online(move || reader.local_lease_status()),
            admission,
            request,
            profile,
            work,
        )
        .await
    }

    pub(crate) async fn run_local_profiled<T: Send + 'static>(
        &self,
        access: LocalCasAccess,
        admission: ReplyAdmission,
        request: RequestMeters,
        profile: Option<Arc<ReadProfile>>,
        work: impl FnOnce() -> WorkResult<T> + Send + 'static,
    ) -> Result<Completion<T>, SnapshotError> {
        tracing::debug!(target: "scorpiofs::workspace::performance", mode = ?access,
            "local CAS uses constructor-established access");
        self.run_admitted(
            AccessCheck::<fn() -> Result<(), SnapshotError>>::Local(access),
            admission,
            request,
            profile,
            work,
        )
        .await
    }

    async fn run_admitted<T: Send + 'static>(
        &self,
        access: AccessCheck<impl Fn() -> Result<(), SnapshotError> + Send + Sync + 'static>,
        admission: ReplyAdmission,
        request: RequestMeters,
        profile: Option<Arc<ReadProfile>>,
        work: impl FnOnce() -> WorkResult<T> + Send + 'static,
    ) -> Result<Completion<T>, SnapshotError> {
        let local_outstanding = try_admit(&self.outstanding).inspect_err(|_| {
            if let Some(profile) = &profile {
                profile.add(Metric::WorkerRejected, 1);
            }
        })?;
        let process_outstanding = try_admit(&self.process.outstanding).inspect_err(|_| {
            if let Some(profile) = &profile {
                profile.add(Metric::WorkerRejected, 1);
            }
        })?;
        if let Some(profile) = &profile {
            profile.worker_enter();
        }
        let profile_work = ProfileWork(profile.clone());
        let enabled =
            tracing::enabled!(target: "scorpiofs::workspace::performance", tracing::Level::DEBUG);
        let timed = enabled || profile.is_some();
        let queued = timed.then(Instant::now);
        let state = Arc::new(AtomicU8::new(PENDING));
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let _cancel = CancelOnDrop {
            state: state.clone(),
            id,
            kind: request.kind,
            profile: profile.clone(),
        };
        // No component can occupy global execution while awaiting its local
        // execution slot. Waiting futures are already count-admitted.
        let local_running = self.running.clone().acquire_owned().await.map_err(|_| {
            state.store(FINISHED, Ordering::Release);
            if let Some(profile) = &profile {
                profile.add(Metric::WorkerError, 1);
            }
            closed()
        })?;
        let process_running = self
            .process
            .running
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                state.store(FINISHED, Ordering::Release);
                if let Some(profile) = &profile {
                    profile.add(Metric::WorkerError, 1);
                }
                closed()
            })?;
        if let Err(error) = access.check() {
            if let Some(profile) = &profile {
                profile.add(Metric::WorkerLeaseRejected, 1);
            }
            state.store(FINISHED, Ordering::Release);
            tracing::debug!(
                target: "scorpiofs::workspace::performance",
                job_id = id, kind = request.kind, wanted = request.wanted,
                code = ?error.code, "local CAS dispatch lease rejected"
            );
            return Err(error);
        }
        let permits = JobPermits {
            _local_outstanding: local_outstanding,
            _process_outstanding: process_outstanding,
            _local_running: local_running,
            _process_running: process_running,
        };
        let parent = tracing::Span::current();
        let dispatcher = tracing::dispatcher::get_default(|current| current.clone());
        tokio::task::spawn_blocking(move || {
            tracing::dispatcher::with_default(&dispatcher, || {
                parent.in_scope(|| {
                    let _permits = permits;
                    let mut trace = JobTrace {
                        state: state.clone(),
                        request,
                        id,
                        queued,
                        started: timed.then(Instant::now),
                        outcome: "not_started",
                        meters: None,
                        profile_work,
                    };
                    let result = if state
                        .compare_exchange(PENDING, RUNNING, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        trace.outcome = "cancelled_before_start";
                        Err(SnapshotError::new(
                            SnapshotErrorCode::Internal,
                            "local CAS waiter cancelled before worker start",
                        ))
                    } else if let Err(error) = access.check() {
                        trace.outcome = "lease_error";
                        Err(error)
                    } else {
                        let output = work();
                        trace.meters = output.meters;
                        trace.outcome = output.outcome;
                        output.result
                    };
                    Completion { result, admission }
                })
            })
        })
        .await
        .map_err(|error| {
            SnapshotError::new(
                SnapshotErrorCode::Internal,
                format!("local CAS worker join failed: {error}"),
            )
        })
    }
}

fn try_admit(semaphore: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit, SnapshotError> {
    semaphore.clone().try_acquire_owned().map_err(|error| match error {
        TryAcquireError::NoPermits => {
            tracing::debug!(target: "scorpiofs::workspace::performance", "local CAS outstanding count rejected");
            SnapshotError::new(SnapshotErrorCode::LimitExceeded, "local CAS outstanding count limit reached")
        }
        TryAcquireError::Closed => closed(),
    })
}

fn closed() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::Internal,
        "local CAS execution gate closed",
    )
}

#[cfg(test)]
#[path = "cas_worker_tests.rs"]
mod tests;
