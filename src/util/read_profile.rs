//! Explicitly opted-in, fixed-size counters. No content or identity is retained.

use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Instant,
};

use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Metric {
    OwnerCacheHit,
    OwnerCacheMiss,
    CacheGetProbes,
    CacheInsertProbes,
    CacheEvictions,
    NodeFileClones,
    NodeDirectoryClones,
    DirectoryLoadSkipped,
    DirectoryLoad,
    MetadataLocalPages,
    MetadataWirePages,
    SmallCasCalls,
    SmallCasReadAttempts,
    SmallCasReadBytes,
    SmallCasReadEof,
    SmallCasReadInterrupted,
    SmallCasWholeHashBytes,
    SmallCasAppendBytes,
    LargeCasCalls,
    LargeCasAppendBytes,
    LargeCasReadBytes,
    LargeWholeHashBytes,
    LargeChunkHashBytes,
    LargeIndexHit,
    LargeIndexBuilt,
    LargeStrictFallback,
    ReplyOwners,
    ReplyOwnerBytes,
    ReplyPayloadCopyBytes,
    WorkerAdmitted,
    WorkerBackingHit,
    WorkerBackingMiss,
    WorkerError,
    WorkerPanic,
    WorkerRejected,
    WorkerLeaseRejected,
    WorkerCancelledPending,
    WorkerDetached,
    WorkerQueueNs,
    WorkerWallNs,
}
const METRICS: usize = Metric::WorkerWallNs as usize + 1;
const ALL_METRICS: [Metric; METRICS] = [
    Metric::OwnerCacheHit,
    Metric::OwnerCacheMiss,
    Metric::CacheGetProbes,
    Metric::CacheInsertProbes,
    Metric::CacheEvictions,
    Metric::NodeFileClones,
    Metric::NodeDirectoryClones,
    Metric::DirectoryLoadSkipped,
    Metric::DirectoryLoad,
    Metric::MetadataLocalPages,
    Metric::MetadataWirePages,
    Metric::SmallCasCalls,
    Metric::SmallCasReadAttempts,
    Metric::SmallCasReadBytes,
    Metric::SmallCasReadEof,
    Metric::SmallCasReadInterrupted,
    Metric::SmallCasWholeHashBytes,
    Metric::SmallCasAppendBytes,
    Metric::LargeCasCalls,
    Metric::LargeCasAppendBytes,
    Metric::LargeCasReadBytes,
    Metric::LargeWholeHashBytes,
    Metric::LargeChunkHashBytes,
    Metric::LargeIndexHit,
    Metric::LargeIndexBuilt,
    Metric::LargeStrictFallback,
    Metric::ReplyOwners,
    Metric::ReplyOwnerBytes,
    Metric::ReplyPayloadCopyBytes,
    Metric::WorkerAdmitted,
    Metric::WorkerBackingHit,
    Metric::WorkerBackingMiss,
    Metric::WorkerError,
    Metric::WorkerPanic,
    Metric::WorkerRejected,
    Metric::WorkerLeaseRejected,
    Metric::WorkerCancelledPending,
    Metric::WorkerDetached,
    Metric::WorkerQueueNs,
    Metric::WorkerWallNs,
];

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    LowerGetattrMapping,
    LowerRead,
    DirectoryLoad,
    LeaseBeforeRead,
    MembershipAndLease,
    ValidateProvenFile,
    CacheGet,
    CacheInsert,
    ReplyOwner,
    SmallCas,
    SmallCasOpen,
    SmallCasFstat,
    SmallCasRead,
    SmallCasHash,
    LargeCas,
}
const PHASES: usize = Phase::LargeCas as usize + 1;
const ALL_PHASES: [Phase; PHASES] = [
    Phase::LowerGetattrMapping,
    Phase::LowerRead,
    Phase::DirectoryLoad,
    Phase::LeaseBeforeRead,
    Phase::MembershipAndLease,
    Phase::ValidateProvenFile,
    Phase::CacheGet,
    Phase::CacheInsert,
    Phase::ReplyOwner,
    Phase::SmallCas,
    Phase::SmallCasOpen,
    Phase::SmallCasFstat,
    Phase::SmallCasRead,
    Phase::SmallCasHash,
    Phase::LargeCas,
];

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Operation {
    Lookup,
    Getattr,
    Open,
    Read,
    Readlink,
    Opendir,
    Readdir,
    Readdirplus,
    Release,
    Releasedir,
}
const OPERATIONS: usize = Operation::Releasedir as usize + 1;
const ALL_OPERATIONS: [Operation; OPERATIONS] = [
    Operation::Lookup,
    Operation::Getattr,
    Operation::Open,
    Operation::Read,
    Operation::Readlink,
    Operation::Opendir,
    Operation::Readdir,
    Operation::Readdirplus,
    Operation::Release,
    Operation::Releasedir,
];

#[derive(Clone, Copy, Default, Serialize)]
pub(crate) struct Times {
    pub calls: u64,
    pub completed: u64,
    pub errors: u64,
    pub dropped: u64,
    pub active: u64,
    pub returned_bytes: u64,
    pub wall_ns: u64,
    pub max_wall_ns: u64,
    pub empty_read_replies: u64,
}
struct Data {
    metrics: [u64; METRICS],
    phases: [Times; PHASES],
    operations: [Times; OPERATIONS],
    workers_active: u64,
    sequence: u64,
    overflow: bool,
}
impl Default for Data {
    fn default() -> Self {
        Self {
            metrics: [0; METRICS],
            phases: [Times::default(); PHASES],
            operations: [Times::default(); OPERATIONS],
            workers_active: 0,
            sequence: 0,
            overflow: false,
        }
    }
}

pub(crate) struct ReadProfile {
    started: Instant,
    data: Mutex<Data>,
}
#[derive(Serialize)]
pub(crate) struct Counter {
    pub metric: Metric,
    pub value: u64,
}
#[derive(Serialize)]
pub(crate) struct PhaseTimes {
    pub phase: Phase,
    pub calls: u64,
    pub active: u64,
    pub wall_ns: u64,
    pub max_wall_ns: u64,
}
#[derive(Serialize)]
pub(crate) struct OperationTimes {
    pub operation: Operation,
    pub times: Times,
}
#[derive(Serialize)]
pub(crate) struct ProfileSnapshot {
    pub revision: u8,
    pub sequence: u64,
    pub sample_started_ns: u64,
    pub sample_finished_ns: u64,
    pub overflow: bool,
    pub workers_active: u64,
    pub native_kernel_copy_measured: bool,
    pub upper_reply_copy_measured: bool,
    pub directory_stream_delivery_measured: bool,
    pub metrics: Vec<Counter>,
    pub phases: Vec<PhaseTimes>,
    pub operations: Vec<OperationTimes>,
}
impl ProfileSnapshot {
    #[cfg(test)]
    pub(crate) fn metric(&self, metric: Metric) -> u64 {
        self.metrics[metric as usize].value
    }
}
fn plus(value: &mut u64, amount: u64, overflow: &mut bool) {
    match value.checked_add(amount) {
        Some(next) => *value = next,
        None => {
            *value = u64::MAX;
            *overflow = true;
        }
    }
}
fn nanos(duration: std::time::Duration, overflow: &mut bool) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or_else(|_| {
        *overflow = true;
        u64::MAX
    })
}
impl ReadProfile {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            data: Mutex::new(Data::default()),
        })
    }
    fn change(&self, work: impl FnOnce(&mut Data)) {
        let mut data = self.data.lock().unwrap_or_else(|poison| {
            let mut data = poison.into_inner();
            data.overflow = true;
            data
        });
        work(&mut data);
    }
    pub(crate) fn add(&self, metric: Metric, amount: u64) {
        self.add_many(&[(metric, amount)]);
    }
    pub(crate) fn add_many(&self, values: &[(Metric, u64)]) {
        self.change(|data| {
            for &(metric, amount) in values {
                plus(
                    &mut data.metrics[metric as usize],
                    amount,
                    &mut data.overflow,
                );
            }
        });
    }
    pub(crate) fn add_durations(&self, values: &[(Metric, std::time::Duration)]) {
        self.change(|data| {
            for &(metric, duration) in values {
                let amount = nanos(duration, &mut data.overflow);
                plus(
                    &mut data.metrics[metric as usize],
                    amount,
                    &mut data.overflow,
                );
            }
        });
    }
    pub(crate) fn mark_overflow(&self) {
        self.change(|data| data.overflow = true);
    }
    pub(crate) fn record_phase(&self, phase: Phase, wall_ns: u64) {
        self.change(|data| {
            let times = &mut data.phases[phase as usize];
            plus(&mut times.calls, 1, &mut data.overflow);
            plus(&mut times.wall_ns, wall_ns, &mut data.overflow);
            times.max_wall_ns = times.max_wall_ns.max(wall_ns);
        });
    }
    pub(crate) fn worker_enter(&self) {
        self.change(|data| {
            plus(&mut data.workers_active, 1, &mut data.overflow);
            plus(
                &mut data.metrics[Metric::WorkerAdmitted as usize],
                1,
                &mut data.overflow,
            );
        });
    }
    pub(crate) fn worker_leave(&self) {
        self.change(|data| match data.workers_active.checked_sub(1) {
            Some(value) => data.workers_active = value,
            None => data.overflow = true,
        });
    }
    pub(crate) fn phase(self: &Arc<Self>, phase: Phase) -> PhaseGuard {
        self.enter(false, phase as usize);
        PhaseGuard {
            profile: self.clone(),
            index: phase as usize,
            native: false,
            started: Instant::now(),
            completed: true,
            error: false,
            bytes: 0,
        }
    }
    fn enter(&self, native: bool, index: usize) {
        self.change(|data| {
            let times = if native {
                &mut data.operations[index]
            } else {
                &mut data.phases[index]
            };
            plus(&mut times.calls, 1, &mut data.overflow);
            plus(&mut times.active, 1, &mut data.overflow);
        });
    }
    pub(crate) fn snapshot(&self) -> ProfileSnapshot {
        let mut overflow = false;
        let start = nanos(self.started.elapsed(), &mut overflow);
        let mut result = None;
        self.change(|data| {
            data.overflow |= overflow;
            plus(&mut data.sequence, 1, &mut data.overflow);
            let finished = nanos(self.started.elapsed(), &mut data.overflow);
            result = Some(ProfileSnapshot {
                revision: 1,
                sequence: data.sequence,
                sample_started_ns: start,
                sample_finished_ns: finished,
                overflow: data.overflow,
                workers_active: data.workers_active,
                native_kernel_copy_measured: false,
                upper_reply_copy_measured: false,
                directory_stream_delivery_measured: false,
                metrics: ALL_METRICS
                    .iter()
                    .map(|&metric| Counter {
                        metric,
                        value: data.metrics[metric as usize],
                    })
                    .collect(),
                phases: ALL_PHASES
                    .iter()
                    .map(|&phase| PhaseTimes {
                        phase,
                        calls: data.phases[phase as usize].calls,
                        active: data.phases[phase as usize].active,
                        wall_ns: data.phases[phase as usize].wall_ns,
                        max_wall_ns: data.phases[phase as usize].max_wall_ns,
                    })
                    .collect(),
                operations: ALL_OPERATIONS
                    .iter()
                    .map(|&operation| OperationTimes {
                        operation,
                        times: data.operations[operation as usize],
                    })
                    .collect(),
            });
        });
        result.unwrap()
    }
}

pub(crate) struct PhaseGuard {
    profile: Arc<ReadProfile>,
    index: usize,
    native: bool,
    started: Instant,
    completed: bool,
    error: bool,
    bytes: u64,
}
impl Drop for PhaseGuard {
    fn drop(&mut self) {
        let mut overflow = false;
        let elapsed = nanos(self.started.elapsed(), &mut overflow);
        self.profile.change(|data| {
            data.overflow |= overflow;
            let times = if self.native {
                &mut data.operations[self.index]
            } else {
                &mut data.phases[self.index]
            };
            times.active = times.active.checked_sub(1).unwrap_or_else(|| {
                data.overflow = true;
                0
            });
            plus(
                if self.completed {
                    &mut times.completed
                } else {
                    &mut times.dropped
                },
                1,
                &mut data.overflow,
            );
            if self.error {
                plus(&mut times.errors, 1, &mut data.overflow);
            }
            plus(&mut times.returned_bytes, self.bytes, &mut data.overflow);
            if self.native
                && self.index == Operation::Read as usize
                && self.completed
                && !self.error
                && self.bytes == 0
            {
                plus(&mut times.empty_read_replies, 1, &mut data.overflow);
            }
            plus(&mut times.wall_ns, elapsed, &mut data.overflow);
            times.max_wall_ns = times.max_wall_ns.max(elapsed);
        });
    }
}

pub(crate) fn phase(profile: Option<&Arc<ReadProfile>>, kind: Phase) -> Option<PhaseGuard> {
    profile.map(|profile| profile.phase(kind))
}

/// Timer covers the actual wrapper future, including awaits; it ends before
/// asyncfuse serialization/kernel delivery. Directory streams are construction only.
pub(crate) async fn native<T>(
    profile: Option<&Arc<ReadProfile>>,
    operation: Operation,
    future: impl Future<Output = asyncfuse::Result<T>>,
    bytes: impl FnOnce(&T) -> u64,
) -> asyncfuse::Result<T> {
    let mut timer = profile.map(|profile| {
        profile.enter(true, operation as usize);
        PhaseGuard {
            profile: profile.clone(),
            index: operation as usize,
            native: true,
            started: Instant::now(),
            completed: false,
            error: false,
            bytes: 0,
        }
    });
    let result = future.await;
    if let Some(timer) = &mut timer {
        timer.completed = true;
        timer.error = result.is_err();
        timer.bytes = result.as_ref().map(bytes).unwrap_or(0);
    }
    result
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Waker};

    use super::*;

    #[tokio::test]
    async fn actual_future_errors_empty_replies_and_cancellation_are_distinct() {
        let profile = ReadProfile::new();
        let body = native(
            Some(&profile),
            Operation::Read,
            async { Ok(vec![1_u8, 2, 3]) },
            |body| body.len() as u64,
        )
        .await
        .unwrap();
        assert_eq!(body, [1, 2, 3]);
        native(
            Some(&profile),
            Operation::Read,
            async { Ok(Vec::<u8>::new()) },
            |body| body.len() as u64,
        )
        .await
        .unwrap();
        let error = native::<Vec<u8>>(
            Some(&profile),
            Operation::Read,
            async { Err(libc::EIO.into()) },
            |body| body.len() as u64,
        )
        .await
        .unwrap_err();
        assert_eq!(i32::from(error), -libc::EIO);
        let mut pending = Box::pin(native(
            Some(&profile),
            Operation::Read,
            std::future::pending::<asyncfuse::Result<Vec<u8>>>(),
            |body| body.len() as u64,
        ));
        assert!(pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        let running = profile.snapshot().operations[Operation::Read as usize].times;
        assert_eq!(running.calls, 4);
        assert_eq!(running.completed, 3);
        assert_eq!(running.active, 1);
        assert_eq!(running.errors, 1);
        assert_eq!(running.dropped, 0);
        assert_eq!(running.returned_bytes, 3);
        assert_eq!(running.empty_read_replies, 1);
        drop(pending);
        let finished = profile.snapshot().operations[Operation::Read as usize].times;
        assert_eq!(finished.active, 0);
        assert_eq!(finished.completed, 3);
        assert_eq!(finished.dropped, 1);
        assert_eq!(finished.errors, 1);
        assert_eq!(finished.empty_read_replies, 1);
        assert!(!profile.snapshot().overflow);

        // Disabled diagnostics execute the same future without sampling its reply.
        let body = native(None, Operation::Read, async { Ok(vec![4_u8]) }, |_| {
            panic!("disabled diagnostics inspected the result")
        })
        .await
        .unwrap();
        assert_eq!(body, [4]);
    }

    #[test]
    fn fixed_checkpoint_saturates_and_preserves_inflight_and_measurement_boundaries() {
        let profile = ReadProfile::new();
        profile.add_durations(&[(Metric::WorkerWallNs, std::time::Duration::MAX)]);
        profile.add(Metric::SmallCasReadBytes, u64::MAX);
        profile.add(Metric::SmallCasReadBytes, 1);
        profile.worker_enter();
        let phase = profile.phase(Phase::SmallCas);
        let first = profile.snapshot();
        assert!(first.overflow);
        assert_eq!(first.metric(Metric::WorkerWallNs), u64::MAX);
        assert_eq!(first.metric(Metric::SmallCasReadBytes), u64::MAX);
        assert_eq!(first.workers_active, 1);
        assert_eq!(first.phases[Phase::SmallCas as usize].active, 1);
        drop(phase);
        profile.worker_leave();
        let second = profile.snapshot();
        assert_eq!(second.sequence, first.sequence + 1);
        assert!(second.sample_started_ns >= first.sample_started_ns);
        assert!(second.sample_finished_ns >= second.sample_started_ns);
        assert!(second.overflow);
        assert_eq!(second.workers_active, 0);
        assert_eq!(second.phases[Phase::SmallCas as usize].active, 0);
        assert!(!second.native_kernel_copy_measured);
        assert!(!second.upper_reply_copy_measured);
        assert!(!second.directory_stream_delivery_measured);
        let value = serde_json::to_value(second).unwrap();
        assert_eq!(value["metrics"].as_array().unwrap().len(), METRICS);
        assert_eq!(value["phases"].as_array().unwrap().len(), PHASES);
        assert_eq!(value["operations"].as_array().unwrap().len(), OPERATIONS);
        assert!(value["phases"][0].get("completed").is_none());
        assert!(serde_json::to_vec(&value).unwrap().len() < 16 * 1024);
        // No caller-defined strings can become labels or retained identities.
        for record in value["metrics"].as_array().unwrap() {
            assert_eq!(record.as_object().unwrap().len(), 2);
            assert!(record["metric"].is_string());
            assert!(record["value"].as_u64().is_some());
        }
        profile.add(Metric::SmallCasReadBytes, 1);
        assert_eq!(
            profile.snapshot().metric(Metric::SmallCasReadBytes),
            u64::MAX
        );
    }
}
