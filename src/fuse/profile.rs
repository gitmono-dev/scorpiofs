//! Low-overhead FUSE operation profiling.
//!
//! Events are pushed from FUSE workers to a bounded lock-free queue and written
//! by a dedicated task. The FUSE path never formats records, locks a file, or
//! performs disk I/O. If the queue is full, the event is dropped and counted.

use std::{
    fs::{File, OpenOptions},
    future::Future,
    io::{self, BufWriter, Write},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use asyncfuse::{raw::Request, Inode, Result as FuseResult};
use crossbeam::queue::ArrayQueue;
use tokio::{sync::Notify, task::JoinHandle, time::MissedTickBehavior};

pub const FUSE_PROFILE_COLUMNS: &[&str] = &[
    "timestamp_ns",
    "sequence",
    "op",
    "request_id",
    "pid",
    "uid",
    "gid",
    "inode",
    "fh",
    "parent",
    "offset",
    "requested_size",
    "bytes",
    "entries",
    "status",
    "duration_ns",
    "errno",
];

#[derive(Debug, Clone)]
pub struct FuseProfileOptions {
    pub path: String,
    pub mount_id: String,
    pub agent: String,
    pub task: String,
    pub capacity: usize,
    pub flush_interval: Duration,
}

impl Default for FuseProfileOptions {
    fn default() -> Self {
        Self {
            path: "/tmp/scorpiofs-fuse-profile.tsv".to_string(),
            mount_id: "unlabeled".to_string(),
            agent: "unlabeled".to_string(),
            task: "unlabeled".to_string(),
            capacity: 262_144,
            flush_interval: Duration::from_millis(10),
        }
    }
}

#[derive(Debug)]
pub(crate) struct FuseProfileEvent {
    pub sequence: u64,
    pub elapsed_ns: u128,
    pub op: &'static str,
    pub request: Request,
    pub inode: Option<Inode>,
    pub fh: Option<u64>,
    pub parent: Option<Inode>,
    pub offset: Option<u64>,
    pub requested_size: Option<u64>,
    pub bytes: Option<u64>,
    pub entries: Option<u64>,
    pub ok: bool,
    pub duration_ns: u128,
    pub errno: Option<i32>,
}

#[derive(Debug)]
pub struct FuseProfileContext {
    queue: Arc<ArrayQueue<FuseProfileEvent>>,
    shutdown: Arc<AtomicBool>,
    wake_writer: Arc<Notify>,
    dropped: Arc<AtomicU64>,
    started: Instant,
    next_sequence: AtomicU64,
}

impl FuseProfileContext {
    fn new(
        queue: Arc<ArrayQueue<FuseProfileEvent>>,
        shutdown: Arc<AtomicBool>,
        wake_writer: Arc<Notify>,
        dropped: Arc<AtomicU64>,
        started: Instant,
    ) -> Self {
        Self {
            queue,
            shutdown,
            wake_writer,
            dropped,
            started,
            next_sequence: AtomicU64::new(1),
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub(crate) fn record(&self, mut event: FuseProfileEvent) {
        event.sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        event.elapsed_ns = self.started.elapsed().as_nanos();
        let sequence = event.sequence;
        if self.queue.push(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        } else if sequence & 1023 == 0 {
            // Keep the producer path cheap while still waking the writer early
            // during sustained bursts instead of waiting for the next flush tick.
            self.wake_writer.notify_one();
        }
    }
}

#[derive(Debug)]
pub struct FuseProfileWriter {
    context: Arc<FuseProfileContext>,
    task: JoinHandle<io::Result<()>>,
}

impl FuseProfileWriter {
    pub fn dropped(&self) -> u64 {
        self.context.dropped()
    }

    pub async fn shutdown(self) -> io::Result<()> {
        self.context.shutdown.store(true, Ordering::Release);
        self.context.wake_writer.notify_one();
        self.task
            .await
            .map_err(|e| io::Error::other(format!("FUSE profile writer task failed: {e}")))?
    }
}

pub fn start_profile_writer(
    options: FuseProfileOptions,
) -> io::Result<(Arc<FuseProfileContext>, FuseProfileWriter)> {
    if options.capacity == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "profile capacity must be non-zero",
        ));
    }
    if options.flush_interval.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "profile flush interval must be non-zero",
        ));
    }
    if let Some(parent) = Path::new(&options.path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }

    // Open the sink and write its header before returning. Otherwise a bad
    // path or permission error would surface only when the daemon shuts down.
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&options.path)?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);
    let started = Instant::now();
    let started_unix_ns = unix_now_ns()?;
    write_header(&mut writer, &options, started_unix_ns)?;
    writer.flush()?;

    let queue = Arc::new(ArrayQueue::new(options.capacity));
    let shutdown = Arc::new(AtomicBool::new(false));
    let wake_writer = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicU64::new(0));
    let context = Arc::new(FuseProfileContext::new(
        queue.clone(),
        shutdown.clone(),
        wake_writer.clone(),
        dropped.clone(),
        started,
    ));
    let writer_context = context.clone();
    let task = tokio::spawn(async move {
        writer_loop(
            writer,
            started_unix_ns,
            options.flush_interval,
            queue,
            shutdown,
            wake_writer,
            dropped,
        )
        .await
    });

    let writer = FuseProfileWriter { context, task };
    Ok((writer_context, writer))
}

async fn writer_loop(
    mut writer: BufWriter<File>,
    started_unix_ns: u128,
    flush_interval: Duration,
    queue: Arc<ArrayQueue<FuseProfileEvent>>,
    shutdown: Arc<AtomicBool>,
    wake_writer: Arc<Notify>,
    dropped: Arc<AtomicU64>,
) -> io::Result<()> {
    let mut interval = tokio::time::interval(flush_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut events_written = 0_u64;
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = wake_writer.notified() => {}
        }
        let mut wrote_event = false;
        while let Some(event) = queue.pop() {
            write_event(&mut writer, started_unix_ns, &event)?;
            events_written = events_written.saturating_add(1);
            wrote_event = true;
        }
        if wrote_event {
            writer.flush()?;
        }
        if shutdown.load(Ordering::Acquire) && queue.is_empty() {
            break;
        }
    }
    let dropped = dropped.load(Ordering::Relaxed);
    writeln!(writer, "# completed_unix_ns\t{}", unix_now_ns()?)?;
    writeln!(writer, "# events_written\t{events_written}")?;
    writeln!(writer, "# dropped_events\t{dropped}")?;
    writer.flush()?;
    if dropped != 0 {
        tracing::warn!("FUSE profile dropped {dropped} events because its queue was full");
    }
    Ok(())
}

fn write_header<W: Write>(
    writer: &mut W,
    options: &FuseProfileOptions,
    started_unix_ns: u128,
) -> io::Result<()> {
    writeln!(writer, "# fuse-profile\t1")?;
    writeln!(writer, "# started_unix_ns\t{started_unix_ns}")?;
    writeln!(
        writer,
        "# mount_id\t{}",
        sanitize(options.mount_id.as_str())
    )?;
    writeln!(writer, "# agent\t{}", sanitize(options.agent.as_str()))?;
    writeln!(writer, "# task\t{}", sanitize(options.task.as_str()))?;
    writeln!(writer, "# columns\t{}", FUSE_PROFILE_COLUMNS.join("\t"))?;
    Ok(())
}

fn write_event<W: Write>(
    writer: &mut W,
    started_unix_ns: u128,
    event: &FuseProfileEvent,
) -> io::Result<()> {
    let timestamp_ns = started_unix_ns.saturating_add(event.elapsed_ns);
    writeln!(
        writer,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        timestamp_ns,
        event.sequence,
        event.op,
        event.request.unique,
        event.request.pid,
        event.request.uid,
        event.request.gid,
        option(event.inode),
        option(event.fh),
        option(event.parent),
        option(event.offset),
        option(event.requested_size),
        option(event.bytes),
        option(event.entries),
        if event.ok { "ok" } else { "error" },
        event.duration_ns,
        option(event.errno),
    )
}

fn option<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            '\0' | '\t' | '\n' | '\r' => ' ',
            _ => ch,
        })
        .collect()
}

fn unix_now_ns() -> io::Result<u128> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .map_err(io::Error::other)
}

pub(crate) async fn observe_result<T, F, I>(
    context: &FuseProfileContext,
    _op: &'static str,
    _request: Request,
    event: impl FnOnce() -> FuseProfileEvent,
    operation: F,
    inspect: I,
) -> FuseResult<T>
where
    F: Future<Output = FuseResult<T>> + Send,
    I: FnOnce(&T, &mut FuseProfileEvent) + Send,
    T: Send,
{
    let started = Instant::now();
    let result = operation.await;
    let mut profile_event = event();
    match &result {
        Ok(reply) => inspect(reply, &mut profile_event),
        Err(error) => {
            profile_event.ok = false;
            let raw: i32 = (*error).into();
            profile_event.errno = Some(raw.saturating_abs());
        }
    }
    profile_event.duration_ns = started.elapsed().as_nanos();
    context.record(profile_event);
    result
}

pub(crate) async fn observe_unit<F>(
    context: &FuseProfileContext,
    _op: &'static str,
    _request: Request,
    event: impl FnOnce() -> FuseProfileEvent,
    operation: F,
) where
    F: Future<Output = ()> + Send,
{
    let started = Instant::now();
    operation.await;
    let mut profile_event = event();
    profile_event.duration_ns = started.elapsed().as_nanos();
    context.record(profile_event);
}

pub(crate) fn base_event(
    op: &'static str,
    request: Request,
    inode: Option<Inode>,
) -> FuseProfileEvent {
    FuseProfileEvent {
        sequence: 0,
        elapsed_ns: 0,
        op,
        request,
        inode,
        fh: None,
        parent: None,
        offset: None,
        requested_size: None,
        bytes: None,
        entries: None,
        ok: true,
        duration_ns: 0,
        errno: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn writes_header_and_operation_events() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("profile.tsv");
        let (context, writer) = start_profile_writer(FuseProfileOptions {
            path: path.display().to_string(),
            mount_id: "mount-1".to_string(),
            agent: "claude".to_string(),
            task: "read-test".to_string(),
            capacity: 16,
            flush_interval: Duration::from_millis(1),
        })
        .unwrap();

        let mut event = base_event(
            "read",
            Request {
                unique: 42,
                uid: 1000,
                gid: 1000,
                pid: 1234,
            },
            Some(7),
        );
        event.offset = Some(128);
        event.requested_size = Some(16);
        event.bytes = Some(12);
        event.duration_ns = 345;
        context.record(event);

        writer.shutdown().await.unwrap();
        let output = std::fs::read_to_string(path).unwrap();
        assert!(output.contains("# fuse-profile\t1"));
        assert!(output.contains("# mount_id\tmount-1"));
        assert!(output.contains("# agent\tclaude"));
        assert!(output.contains("# task\tread-test"));
        let columns = output
            .lines()
            .find(|line| line.starts_with("# columns\t"))
            .unwrap();
        let header_fields: Vec<_> = columns.split('\t').skip(1).collect();
        assert_eq!(header_fields, FUSE_PROFILE_COLUMNS);
        assert!(output.contains("# events_written\t1"));
        assert!(output.contains("# dropped_events\t0"));

        let operation = output
            .lines()
            .find(|line| line.split('\t').nth(2) == Some("read"))
            .unwrap();
        let fields: Vec<_> = operation.split('\t').collect();
        assert_eq!(fields.len(), FUSE_PROFILE_COLUMNS.len());
        assert_eq!(fields[2], "read");
        assert_eq!(fields[3], "42");
        assert_eq!(fields[4], "1234");
        assert_eq!(fields[7], "7");
        assert_eq!(fields[8], "-");
        assert_eq!(fields[10], "128");
        assert_eq!(fields[11], "16");
        assert_eq!(fields[12], "12");
        assert_eq!(fields[14], "ok");
        assert_eq!(fields[15], "345");
    }

    #[tokio::test]
    async fn counts_events_dropped_when_queue_is_full() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("profile.tsv");
        let (context, writer) = start_profile_writer(FuseProfileOptions {
            path: path.display().to_string(),
            capacity: 1,
            flush_interval: Duration::from_secs(30),
            ..FuseProfileOptions::default()
        })
        .unwrap();

        let first = base_event("lookup", Request::default(), Some(1));
        let second = base_event("getattr", Request::default(), Some(2));
        context.record(first);
        context.record(second);
        assert!(context.dropped() >= 1);

        writer.shutdown().await.unwrap();
        assert!(context.dropped() >= 1);
        let output = std::fs::read_to_string(path).unwrap();
        assert!(output.contains("# dropped_events\t"));
    }

    #[tokio::test]
    async fn records_errno_and_error_status_without_formatting_on_the_hot_path() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("profile.tsv");
        let (context, writer) = start_profile_writer(FuseProfileOptions {
            path: path.display().to_string(),
            capacity: 16,
            flush_interval: Duration::from_millis(1),
            ..FuseProfileOptions::default()
        })
        .unwrap();

        let result: FuseResult<()> = observe_result(
            &context,
            "lookup",
            Request::default(),
            || base_event("lookup", Request::default(), Some(1)),
            async { Err(libc::ENOENT.into()) },
            |_, _| {},
        )
        .await;
        assert!(result.is_err());

        writer.shutdown().await.unwrap();
        let output = std::fs::read_to_string(path).unwrap();
        let fields: Vec<_> = output
            .lines()
            .find(|line| line.split('\t').nth(2) == Some("lookup"))
            .unwrap()
            .split('\t')
            .collect();
        assert_eq!(fields[14], "error");
        assert_eq!(fields[16], libc::ENOENT.to_string());
    }
}
