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
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        let result =
            self.state
                .compare_exchange(PENDING, CANCELLED, Ordering::AcqRel, Ordering::Acquire);
        if result.is_ok() || result == Err(RUNNING) {
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
}
impl Drop for JobTrace {
    fn drop(&mut self) {
        self.state.store(FINISHED, Ordering::Release);
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

    pub(crate) async fn run<T: Send + 'static>(
        &self,
        reader: SnapshotReader,
        admission: ReplyAdmission,
        request: RequestMeters,
        work: impl FnOnce() -> WorkResult<T> + Send + 'static,
    ) -> Result<Completion<T>, SnapshotError> {
        self.run_checked(
            move || reader.local_lease_status(),
            admission,
            request,
            work,
        )
        .await
    }

    async fn run_checked<T: Send + 'static>(
        &self,
        lease: impl Fn() -> Result<(), SnapshotError> + Send + Sync + 'static,
        admission: ReplyAdmission,
        request: RequestMeters,
        work: impl FnOnce() -> WorkResult<T> + Send + 'static,
    ) -> Result<Completion<T>, SnapshotError> {
        let local_outstanding = try_admit(&self.outstanding)?;
        let process_outstanding = try_admit(&self.process.outstanding)?;
        let enabled =
            tracing::enabled!(target: "scorpiofs::workspace::performance", tracing::Level::DEBUG);
        let queued = enabled.then(Instant::now);
        let state = Arc::new(AtomicU8::new(PENDING));
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let _cancel = CancelOnDrop {
            state: state.clone(),
            id,
            kind: request.kind,
        };
        // No component can occupy global execution while awaiting its local
        // execution slot. Waiting futures are already count-admitted.
        let local_running = self
            .running
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| closed())?;
        let process_running = self
            .process
            .running
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| closed())?;
        lease()?;
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
                        started: enabled.then(Instant::now),
                        outcome: "not_started",
                        meters: None,
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
                    } else if let Err(error) = lease() {
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
