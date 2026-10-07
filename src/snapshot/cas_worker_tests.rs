//! Isolated scheduler tests exercise real CAS owners with controlled barriers,
//! rather than depending on a large file being slow or a FIFO blocking.

use std::{
    sync::{atomic::AtomicBool, Condvar, Mutex},
    time::Duration,
};

use tokio::sync::Notify;

use super::*;
use crate::snapshot::{
    cas_range::VerifiedCasRange,
    content::{ContentBudget, ContentBudgetLimits, ContentBudgetUsage},
    durable::{digest_of, DurableStore},
    frames::parse_digest,
};

type Owner = Option<Arc<VerifiedCasRange>>;
const REQUEST: RequestMeters = RequestMeters {
    kind: "large_range",
    wanted: 4096,
};

#[derive(Clone)]
struct LogOutput(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for LogOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn capture_logs() -> (tracing::subscriber::DefaultGuard, Arc<Mutex<Vec<u8>>>) {
    let output = LogOutput(Arc::new(Mutex::new(Vec::new())));
    let sink = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || sink.clone())
        .finish();
    (tracing::subscriber::set_default(subscriber), output.0)
}

struct Gate {
    entered: Notify,
    released: Mutex<bool>,
    condition: Condvar,
    thread: Mutex<Option<std::thread::ThreadId>>,
}
impl Gate {
    fn block(&self) {
        *self.thread.lock().unwrap() = Some(std::thread::current().id());
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.condition.wait(released).unwrap();
        }
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.condition.notify_all();
    }
}

// Unwinding the controller always lets a blocked worker/runtime finish.
struct Controller(Arc<Gate>);
impl Controller {
    fn new() -> Self {
        Self(Arc::new(Gate {
            entered: Notify::new(),
            released: Mutex::new(false),
            condition: Condvar::new(),
            thread: Mutex::new(None),
        }))
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(2), self.0.entered.notified())
            .await
            .unwrap();
    }
}
impl Drop for Controller {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn scope(
    local_pending: usize,
    local_running: usize,
    process_pending: usize,
    process_running: usize,
) -> Arc<CasReadScope> {
    Arc::new(CasReadScope {
        outstanding: Arc::new(Semaphore::new(local_pending)),
        running: Arc::new(Semaphore::new(local_running)),
        process: Arc::new(ProcessAdmission {
            outstanding: Arc::new(Semaphore::new(process_pending)),
            running: Arc::new(Semaphore::new(process_running)),
        }),
    })
}

fn fixture() -> (
    tempfile::TempDir,
    Arc<DurableStore>,
    String,
    Arc<ContentBudget>,
    u64,
) {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(DurableStore::open(temp.path()).unwrap());
    let body = vec![0x71; 2 * 1024 * 1024 + 7];
    let digest = digest_of(&body);
    std::fs::write(
        store
            .content_dir()
            .join(hex::encode(parse_digest(&digest).unwrap())),
        &body,
    )
    .unwrap();
    (
        temp,
        store,
        digest,
        ContentBudget::new(ContentBudgetLimits::default()),
        body.len() as u64,
    )
}

fn operation(
    store: Arc<DurableStore>,
    digest: String,
    size: u64,
    budget: Arc<ContentBudget>,
    gate: Option<Arc<Gate>>,
) -> impl FnOnce() -> WorkResult<Owner> + Send + 'static {
    move || {
        let mut meters = LocalCasRangeMeters::default();
        let result = match gate {
            Some(gate) => VerifiedCasRange::read_paused(
                &store,
                &digest,
                size,
                0,
                REQUEST.wanted,
                &budget,
                &mut meters,
                move || gate.block(),
            ),
            None => VerifiedCasRange::read(
                &store,
                &digest,
                size,
                0,
                REQUEST.wanted,
                &budget,
                &mut meters,
            ),
        };
        WorkResult::local(result, Some(meters))
    }
}

async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

fn unused() -> ContentBudgetUsage {
    ContentBudgetUsage {
        output_bytes: 0,
        construction_bytes: 0,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn heartbeat_and_running_cancellation_preserve_real_admitted_buffers_until_work_finishes() {
    let (_temp, store, digest, budget, size) = fixture();
    let scope = scope(1, 1, 1, 1);
    let gate = Controller::new();
    let work = operation(store, digest, size, budget.clone(), Some(gate.0.clone()));
    let working = scope.clone();
    let admission = ReplyAdmission::reserve(&budget).unwrap();
    let task = tokio::spawn(async move {
        working
            .run_checked(|| Ok(()), admission, REQUEST, work)
            .await
    });
    gate.entered().await;
    assert_ne!(
        gate.0.thread.lock().unwrap().unwrap(),
        std::thread::current().id()
    );
    let heartbeat = Arc::new(AtomicBool::new(false));
    let tick = heartbeat.clone();
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        tick.store(true, Ordering::Release);
    })
    .await
    .unwrap();
    assert!(heartbeat.load(Ordering::Acquire));
    let held = budget.usage();
    assert!(held.output_bytes > 4096 && held.construction_bytes > 1024 * 1024);
    task.abort();
    assert!(task.await.err().unwrap().is_cancelled());
    assert_eq!(budget.usage(), held);
    assert_eq!(scope.outstanding.available_permits(), 0);
    assert_eq!(scope.running.available_permits(), 0);
    let called = Arc::new(AtomicBool::new(false));
    let flag = called.clone();
    let error = scope
        .run_checked(
            || Ok(()),
            ReplyAdmission::reserve(&budget).unwrap(),
            REQUEST,
            move || {
                flag.store(true, Ordering::Release);
                WorkResult::<Owner>::local(Ok(None), None)
            },
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, SnapshotErrorCode::LimitExceeded);
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(budget.usage(), held);
    gate.0.release();
    until(|| scope.outstanding.available_permits() == 1 && budget.usage() == unused()).await;
    assert_eq!(scope.running.available_permits(), 1);
    assert_eq!(scope.process.outstanding.available_permits(), 1);
    assert_eq!(scope.process.running.available_permits(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn execution_queue_is_bounded_cancel_refunds_only_the_waiter_and_admitted_queue_completes() {
    let (_temp, store, digest, budget, size) = fixture();
    let scope = scope(2, 1, 3, 2);
    let gate = Controller::new();
    let first_work = operation(
        store.clone(),
        digest.clone(),
        size,
        budget.clone(),
        Some(gate.0.clone()),
    );
    let first_scope = scope.clone();
    let admission = ReplyAdmission::reserve(&budget).unwrap();
    let first = tokio::spawn(async move {
        first_scope
            .run_checked(|| Ok(()), admission, REQUEST, first_work)
            .await
    });
    gate.entered().await;
    let held = budget.usage();
    let called = Arc::new(AtomicBool::new(false));
    let flag = called.clone();
    let waiting = scope.clone();
    let admission = ReplyAdmission::reserve(&budget).unwrap();
    let queued = tokio::spawn(async move {
        waiting
            .run_checked(
                || Ok(()),
                admission,
                REQUEST,
                move || {
                    flag.store(true, Ordering::Release);
                    WorkResult::<Owner>::local(Ok(None), None)
                },
            )
            .await
    });
    until(|| scope.outstanding.available_permits() == 0).await;
    assert_eq!(budget.usage().construction_bytes, held.construction_bytes);
    assert!(budget.usage().output_bytes > held.output_bytes);
    let rejected = scope
        .run_checked(
            || Ok(()),
            ReplyAdmission::reserve(&budget).unwrap(),
            REQUEST,
            || WorkResult::<Owner>::local(Ok(None), None),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(rejected.code, SnapshotErrorCode::LimitExceeded);
    queued.abort();
    assert!(queued.await.err().unwrap().is_cancelled());
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(budget.usage(), held);
    assert_eq!(scope.outstanding.available_permits(), 1);
    assert_eq!(scope.process.outstanding.available_permits(), 2);
    let next_scope = scope.clone();
    let admission = ReplyAdmission::reserve(&budget).unwrap();
    let next_work = operation(store, digest, size, budget.clone(), None);
    let next = tokio::spawn(async move {
        next_scope
            .run_checked(|| Ok(()), admission, REQUEST, next_work)
            .await
    });
    until(|| scope.outstanding.available_permits() == 0).await;
    gate.0.release();
    let first = first.await.unwrap().unwrap();
    let next = next.await.unwrap().unwrap();
    assert_eq!(
        first.result.as_ref().unwrap().as_ref().unwrap().as_bytes(),
        &[0x71; 4096]
    );
    assert_eq!(
        next.result.as_ref().unwrap().as_ref().unwrap().as_bytes(),
        &[0x71; 4096]
    );
    assert_eq!(scope.outstanding.available_permits(), 2);
    assert_eq!(scope.running.available_permits(), 1);
    drop(first);
    drop(next);
    assert_eq!(budget.usage(), unused());
}

#[tokio::test(flavor = "current_thread")]
async fn distinct_scopes_share_process_execution_and_cancel_only_the_waiting_scopes_owners() {
    let (_temp, store, digest, budget, size) = fixture();
    let first_scope = scope(2, 1, 3, 1);
    let second_scope = Arc::new(CasReadScope {
        outstanding: Arc::new(Semaphore::new(2)),
        running: Arc::new(Semaphore::new(1)),
        process: first_scope.process.clone(),
    });
    let gate = Controller::new();
    let first_work = operation(
        store.clone(),
        digest.clone(),
        size,
        budget.clone(),
        Some(gate.0.clone()),
    );
    let first = first_scope.clone();
    let admission = ReplyAdmission::reserve(&budget).unwrap();
    let task = tokio::spawn(async move {
        first
            .run_checked(|| Ok(()), admission, REQUEST, first_work)
            .await
    });
    gate.entered().await;
    let held = budget.usage();
    assert!(held.output_bytes > REQUEST.wanted as usize);
    assert!(held.construction_bytes > 1024 * 1024);
    let called = Arc::new(AtomicBool::new(false));
    let flag = called.clone();
    let second = second_scope.clone();
    let admission = ReplyAdmission::reserve(&budget).unwrap();
    let work = operation(store.clone(), digest.clone(), size, budget.clone(), None);
    let waiting = tokio::spawn(async move {
        second
            .run_checked(
                || Ok(()),
                admission,
                REQUEST,
                move || {
                    flag.store(true, Ordering::Release);
                    work()
                },
            )
            .await
    });
    until(|| {
        second_scope.running.available_permits() == 0
            && second_scope.outstanding.available_permits() == 1
            && first_scope.process.outstanding.available_permits() == 1
    })
    .await;
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(budget.usage().construction_bytes, held.construction_bytes);
    assert!(budget.usage().output_bytes > held.output_bytes);
    assert_eq!(first_scope.process.running.available_permits(), 0);
    waiting.abort();
    assert!(waiting.await.err().unwrap().is_cancelled());
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(second_scope.running.available_permits(), 1);
    assert_eq!(second_scope.outstanding.available_permits(), 2);
    assert_eq!(first_scope.process.outstanding.available_permits(), 2);
    assert_eq!(first_scope.process.running.available_permits(), 0);
    assert_eq!(first_scope.running.available_permits(), 0);
    assert_eq!(budget.usage(), held);

    let second = second_scope.clone();
    let admission = ReplyAdmission::reserve(&budget).unwrap();
    let work = operation(store, digest, size, budget.clone(), None);
    let retry = tokio::spawn(async move {
        second
            .run_checked(|| Ok(()), admission, REQUEST, work)
            .await
    });
    until(|| {
        second_scope.running.available_permits() == 0
            && first_scope.process.outstanding.available_permits() == 1
    })
    .await;
    assert_eq!(budget.usage().construction_bytes, held.construction_bytes);
    gate.0.release();
    let first = task.await.unwrap().unwrap();
    let second = retry.await.unwrap().unwrap();
    for completion in [&first, &second] {
        assert_eq!(
            completion
                .result
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .as_bytes(),
            &[0x71; 4096]
        );
    }
    assert_eq!(first_scope.running.available_permits(), 1);
    assert_eq!(second_scope.running.available_permits(), 1);
    assert_eq!(first_scope.process.running.available_permits(), 1);
    assert_eq!(first_scope.process.outstanding.available_permits(), 3);
    assert_eq!(budget.usage().construction_bytes, 0);
    drop(first);
    drop(second);
    assert_eq!(budget.usage(), unused());
}

#[test]
fn cancelled_spawned_pool_queue_holds_actual_count_and_reply_until_skipped_worker_is_destroyed() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (_temp, store, digest, budget, size) = fixture();
        let scope = scope(1, 1, 1, 1);
        let blocker = Controller::new();
        let blocking = blocker.0.clone();
        let thread = tokio::task::spawn_blocking(move || blocking.block());
        blocker.entered().await;
        let called = Arc::new(AtomicBool::new(false));
        let flag = called.clone();
        let work = operation(store, digest, size, budget.clone(), None);
        let waiting = scope.clone();
        let admission = ReplyAdmission::reserve(&budget).unwrap();
        let paid = budget.usage();
        let task = tokio::spawn(async move {
            waiting
                .run_checked(
                    || Ok(()),
                    admission,
                    REQUEST,
                    move || {
                        flag.store(true, Ordering::Release);
                        work()
                    },
                )
                .await
        });
        until(|| {
            scope.outstanding.available_permits() == 0 && scope.running.available_permits() == 0
        })
        .await;
        task.abort();
        assert!(task.await.err().unwrap().is_cancelled());
        assert_eq!(budget.usage(), paid);
        assert_eq!(paid.construction_bytes, 0);
        assert_eq!(scope.outstanding.available_permits(), 0);
        blocker.0.release();
        thread.await.unwrap();
        until(|| scope.outstanding.available_permits() == 1 && budget.usage() == unused()).await;
        assert!(
            !called.load(Ordering::Acquire),
            "cancelled pending job must skip allocation and CAS I/O"
        );
        assert_eq!(scope.process.running.available_permits(), 1);
    });
}

#[tokio::test(flavor = "current_thread")]
async fn partial_local_or_process_count_failure_refunds_admission_and_starts_no_work() {
    let budget = ContentBudget::new(ContentBudgetLimits::default());
    let scope = scope(1, 1, 1, 1);
    let process = scope
        .process
        .outstanding
        .clone()
        .try_acquire_owned()
        .unwrap();
    let error = scope
        .run_checked(
            || Ok(()),
            ReplyAdmission::reserve(&budget).unwrap(),
            REQUEST,
            || -> WorkResult<Owner> { panic!("count rejection ran body") },
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, SnapshotErrorCode::LimitExceeded);
    assert_eq!(scope.outstanding.available_permits(), 1);
    assert_eq!(budget.usage(), unused());
    drop(process);
    let local = scope.outstanding.clone().try_acquire_owned().unwrap();
    let error = scope
        .run_checked(
            || Ok(()),
            ReplyAdmission::reserve(&budget).unwrap(),
            REQUEST,
            || -> WorkResult<Owner> { panic!("count rejection ran body") },
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, SnapshotErrorCode::LimitExceeded);
    assert_eq!(scope.process.outstanding.available_permits(), 1);
    assert_eq!(budget.usage(), unused());
    drop(local);
}

#[tokio::test(flavor = "current_thread")]
async fn worker_panic_after_real_buffer_admission_is_terminal_and_refunds_actual_owners() {
    let (_temp, store, digest, budget, size) = fixture();
    let scope = scope(1, 1, 1, 1);
    let worker_budget = budget.clone();
    let error = scope
        .run_checked(
            || Ok(()),
            ReplyAdmission::reserve(&budget).unwrap(),
            REQUEST,
            move || {
                let mut meters = LocalCasRangeMeters::default();
                let result = VerifiedCasRange::read_paused(
                    &store,
                    &digest,
                    size,
                    0,
                    4096,
                    &worker_budget,
                    &mut meters,
                    || panic!("controlled worker panic after actual admission"),
                );
                WorkResult::local(result, Some(meters))
            },
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, SnapshotErrorCode::Internal);
    assert_eq!(budget.usage(), unused());
    assert_eq!(scope.outstanding.available_permits(), 1);
    assert_eq!(scope.running.available_permits(), 1);
    assert_eq!(scope.process.outstanding.available_permits(), 1);
    assert_eq!(scope.process.running.available_permits(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn completed_worker_returns_real_owner_and_reply_credit_independent_of_execution_slots() {
    let (_temp, store, digest, budget, size) = fixture();
    let scope = scope(1, 1, 1, 1);
    let completion = scope
        .run_checked(
            || Ok(()),
            ReplyAdmission::reserve(&budget).unwrap(),
            REQUEST,
            operation(store, digest, size, budget.clone(), None),
        )
        .await
        .unwrap();
    assert_eq!(scope.outstanding.available_permits(), 1);
    assert_eq!(scope.running.available_permits(), 1);
    assert_eq!(budget.usage().construction_bytes, 0);
    let owner = completion.result.unwrap().unwrap();
    let pointer = owner.as_bytes().as_ptr();
    let reply = completion.admission.cas_range(owner.clone()).unwrap();
    assert_eq!(reply.as_ptr(), pointer);
    let paid = budget.usage();
    drop(owner);
    let last = reply.slice(17..31);
    drop(reply);
    assert_eq!(last.as_ptr(), pointer.wrapping_add(17));
    assert_eq!(last.as_ref(), &[0x71; 14]);
    assert_eq!(budget.usage(), paid);
    drop(last);
    assert_eq!(budget.usage(), unused());
}

#[test]
fn pool_queued_worker_checks_current_lease_at_actual_start_before_primitive_admission() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let budget = ContentBudget::new(ContentBudgetLimits::default());
        let scope = scope(1, 1, 1, 1);
        let blocker = Controller::new();
        let blocking = blocker.0.clone();
        let thread = tokio::task::spawn_blocking(move || blocking.block());
        blocker.entered().await;
        let expired = Arc::new(AtomicBool::new(false));
        let lease = expired.clone();
        let waiting = scope.clone();
        let admission = ReplyAdmission::reserve(&budget).unwrap();
        let task = tokio::spawn(async move {
            waiting
                .run_checked(
                    move || {
                        if lease.load(Ordering::Acquire) {
                            Err(SnapshotError::new(
                                SnapshotErrorCode::LeaseExpired,
                                "controlled grant expired",
                            ))
                        } else {
                            Ok(())
                        }
                    },
                    admission,
                    REQUEST,
                    || -> WorkResult<Owner> { panic!("expired queued worker entered primitive") },
                )
                .await
        });
        until(|| scope.running.available_permits() == 0).await;
        expired.store(true, Ordering::Release);
        blocker.0.release();
        thread.await.unwrap();
        let completion = task.await.unwrap().unwrap();
        assert_eq!(
            completion.result.as_ref().err().unwrap().code,
            SnapshotErrorCode::LeaseExpired
        );
        assert_eq!(budget.usage().construction_bytes, 0);
        assert_eq!(scope.outstanding.available_permits(), 1);
        drop(completion);
        assert_eq!(budget.usage(), unused());
        let error = scope
            .run_checked(
                || {
                    Err(SnapshotError::new(
                        SnapshotErrorCode::SnapshotGone,
                        "controlled dispatch denial",
                    ))
                },
                ReplyAdmission::reserve(&budget).unwrap(),
                REQUEST,
                || -> WorkResult<Owner> { panic!("denied dispatch spawned worker") },
            )
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, SnapshotErrorCode::SnapshotGone);
        assert_eq!(scope.outstanding.available_permits(), 1);
        assert_eq!(scope.running.available_permits(), 1);
        assert_eq!(budget.usage(), unused());
    });
}

#[test]
fn reader_budget_clones_share_the_same_local_scope_and_distinct_readers_share_process_limits() {
    let budget = ContentBudget::new(ContentBudgetLimits::default());
    let clone = budget.clone();
    let first = budget.cas_workers();
    let second = clone.cas_workers();
    assert!(Arc::ptr_eq(&first, &second));
    let other = ContentBudget::new(ContentBudgetLimits::default()).cas_workers();
    assert!(!Arc::ptr_eq(&first, &other));
    assert!(Arc::ptr_eq(&first.process, &other.process));
    assert_eq!(first.outstanding.available_permits(), LOCAL_OUTSTANDING);
    assert_eq!(first.running.available_permits(), LOCAL_RUNNING);
}

#[tokio::test(flavor = "current_thread")]
async fn detached_worker_records_its_actual_finished_io_and_hash_work_in_the_captured_dispatcher() {
    let (_subscriber, output) = capture_logs();
    let (_temp, store, digest, budget, size) = fixture();
    let scope = scope(1, 1, 1, 1);
    let gate = Controller::new();
    let working = scope.clone();
    let work = operation(store, digest, size, budget.clone(), Some(gate.0.clone()));
    let admission = ReplyAdmission::reserve(&budget).unwrap();
    let task = tokio::spawn(async move {
        working
            .run_checked(|| Ok(()), admission, REQUEST, work)
            .await
    });
    gate.entered().await;
    task.abort();
    assert!(task.await.err().unwrap().is_cancelled());
    gate.0.release();
    until(|| scope.outstanding.available_permits() == 1 && budget.usage() == unused()).await;
    let log = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(log
        .lines()
        .any(|line| line.contains("local CAS waiter dropped")
            && line.contains("detached_running=true")));
    let finished = log
        .lines()
        .find(|line| line.contains("local CAS worker finished"))
        .unwrap();
    assert!(finished.contains("range_meters_available=true"));
    assert!(finished.contains(&format!("bytes_read={size}")));
    assert!(finished.contains(&format!("whole_sha256_bytes={size}")));
    assert!(finished.contains(&format!("chunk_sha256_bytes={size}")));
    assert!(finished.contains("queue_wait_us=") && finished.contains("worker_wall_us="));
    assert!(finished.contains("index_built=true"));
}

#[tokio::test(flavor = "current_thread")]
async fn awaited_dispatch_denial_and_closed_gates_do_not_report_waiter_cancellation() {
    let (_subscriber, output) = capture_logs();
    let budget = ContentBudget::new(ContentBudgetLimits::default());
    let denied = scope(1, 1, 1, 1);
    let error = denied
        .run_checked(
            || {
                Err(SnapshotError::new(
                    SnapshotErrorCode::LeaseExpired,
                    "dispatch expired",
                ))
            },
            ReplyAdmission::reserve(&budget).unwrap(),
            REQUEST,
            || -> WorkResult<Owner> { panic!("dispatch denial ran primitive") },
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, SnapshotErrorCode::LeaseExpired);
    assert_eq!(denied.outstanding.available_permits(), 1);
    assert_eq!(denied.process.outstanding.available_permits(), 1);
    assert_eq!(budget.usage(), unused());
    for process in [false, true] {
        let closed_scope = scope(1, 1, 1, 1);
        if process {
            closed_scope.process.running.close();
        } else {
            closed_scope.running.close();
        }
        let error = closed_scope
            .run_checked(
                || Ok(()),
                ReplyAdmission::reserve(&budget).unwrap(),
                REQUEST,
                || -> WorkResult<Owner> { panic!("closed gate ran primitive") },
            )
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, SnapshotErrorCode::Internal);
        assert_eq!(closed_scope.outstanding.available_permits(), 1);
        assert_eq!(closed_scope.process.outstanding.available_permits(), 1);
        assert_eq!(budget.usage(), unused());
    }
    let log = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(log.contains("local CAS dispatch lease rejected"));
    assert!(!log.contains("local CAS waiter dropped"));
    assert!(!log.contains("local CAS worker finished"));

    // A real pending future drop still records cancellation, and refunds only
    // its own admission without starting the synchronous primitive.
    output.lock().unwrap().clear();
    let pending = scope(1, 1, 1, 1);
    let running = pending.running.clone().try_acquire_owned().unwrap();
    let waiting = pending.clone();
    let admission = ReplyAdmission::reserve(&budget).unwrap();
    let task = tokio::spawn(async move {
        waiting
            .run_checked(
                || Ok(()),
                admission,
                REQUEST,
                || -> WorkResult<Owner> { panic!("cancelled execution waiter ran primitive") },
            )
            .await
    });
    until(|| pending.outstanding.available_permits() == 0).await;
    task.abort();
    assert!(task.await.err().unwrap().is_cancelled());
    assert_eq!(budget.usage(), unused());
    assert_eq!(pending.outstanding.available_permits(), 1);
    drop(running);
    let log = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(log
        .lines()
        .any(|line| line.contains("local CAS waiter dropped")
            && line.contains("cancelled_before_start=true")
            && line.contains("detached_running=false")));
    assert!(!log.contains("local CAS worker finished"));
}
