//! Explicit, bounded evidence from the daemon's actual workspace readers.
//! Acceptance requires both a complete footer and a successful daemon exit.

use std::{io, path::PathBuf, sync::Arc, time::Duration};

use serde::Serialize;
use tokio::{io::AsyncWriteExt, sync::oneshot, task::JoinHandle, time::Instant};

use crate::workspace::{
    WorkspaceObservationError, WorkspaceObservations, WorkspaceObserver, MAX_WORKSPACE_OBSERVATIONS,
};

const MAX_RECORDS: u64 = 256;
const MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
const FOOTER_RESERVE: u64 = 4096;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const JOIN_TIMEOUT: Duration = Duration::from_secs(10);

pub struct ObservationFileOptions {
    pub path: PathBuf,
    pub run_id: String,
}

pub(super) struct ObservationSink {
    failure: oneshot::Receiver<()>,
    finalize: oneshot::Sender<i32>,
    task: JoinHandle<bool>,
}

#[derive(Clone, Copy, Debug)]
enum SinkError {
    Observation(WorkspaceObservationError),
    RecordLimit,
    ByteLimit,
    Encode,
    Write,
    PayloadSync,
    FooterWrite,
    FooterSync,
    DirectorySync,
    FinalizeTimeout,
    MissingDaemonExit,
}

impl SinkError {
    fn code(self) -> &'static str {
        match self {
            Self::Observation(error) => match error {
                WorkspaceObservationError::InvalidRunId => "invalid_run_id",
                WorkspaceObservationError::InvalidCapacity => "invalid_capacity",
                WorkspaceObservationError::QueueFull => "queue_full",
                WorkspaceObservationError::ReceiverDropped => "receiver_dropped",
                WorkspaceObservationError::InvalidBinding => "invalid_binding",
                WorkspaceObservationError::NotDrained => "not_drained",
            },
            Self::RecordLimit => "record_limit",
            Self::ByteLimit => "byte_limit",
            Self::Encode => "encode",
            Self::Write => "write",
            Self::PayloadSync => "payload_sync",
            Self::FooterWrite => "footer_write",
            Self::FooterSync => "footer_sync",
            Self::DirectorySync => "directory_sync",
            Self::FinalizeTimeout => "finalize_timeout",
            Self::MissingDaemonExit => "missing_daemon_exit",
        }
    }
}

#[derive(Serialize)]
struct Footer<'a> {
    record: &'static str,
    revision: u8,
    run_id: &'a str,
    accepted_records: u64,
    received_records: u64,
    written_records: u64,
    written_bytes: u64,
    producers_closed: bool,
    drained: bool,
    daemon_exit_code: i32,
    complete: bool,
    first_error: Option<&'static str>,
}

impl ObservationSink {
    pub(super) fn start(
        options: ObservationFileOptions,
    ) -> io::Result<(Arc<WorkspaceObserver>, Self)> {
        // Validate the run before creating the exclusively owned output file.
        let (observer, observations) =
            WorkspaceObserver::channel(&options.run_id, MAX_WORKSPACE_OBSERVATIONS)
                .map_err(io::Error::other)?;
        let (file, parent) = create_file(&options.path)?;
        let (failure_tx, failure) = oneshot::channel();
        let (finalize, finalize_rx) = oneshot::channel();
        let task = tokio::spawn(write_observations(
            options.run_id,
            observations,
            file,
            parent,
            failure_tx,
            finalize_rx,
        ));
        Ok((
            observer,
            Self {
                failure,
                finalize,
                task,
            },
        ))
    }

    pub(super) async fn failed(&mut self) {
        // A writer panic also closes this channel and triggers shutdown.
        let _ = (&mut self.failure).await;
    }

    pub(super) async fn finish(self, daemon_exit_code: i32) -> bool {
        let Self {
            finalize, mut task, ..
        } = self;
        let _ = finalize.send(daemon_exit_code);
        match tokio::time::timeout(JOIN_TIMEOUT, &mut task).await {
            Ok(Ok(complete)) => complete,
            Ok(Err(error)) => {
                tracing::error!("workspace observation writer join failed: {error}");
                false
            }
            Err(_) => {
                tracing::error!("workspace observation writer finalization timed out");
                task.abort();
                let _ = task.await;
                false
            }
        }
    }
}

fn fail(
    first_error: &mut Option<SinkError>,
    notification: &mut Option<oneshot::Sender<()>>,
    error: SinkError,
) {
    if first_error.is_none() {
        tracing::error!(?error, "workspace observation evidence rejected");
        *first_error = Some(error);
    }
    if let Some(sender) = notification.take() {
        let _ = sender.send(());
    }
}

async fn drain_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn write_observations(
    run_id: String,
    mut observations: WorkspaceObservations,
    file: std::fs::File,
    parent: std::fs::File,
    failure_tx: oneshot::Sender<()>,
    mut finalize: oneshot::Receiver<i32>,
) -> bool {
    let mut file = tokio::fs::File::from_std(file);
    let mut notification = Some(failure_tx);
    let mut first_error = None;
    let mut written_records = 0;
    let mut written_bytes = 0;
    let mut daemon_exit_code = None;
    let mut deadline = None;
    let mut producers_closed = false;
    let mut check = tokio::time::interval(Duration::from_millis(100));
    check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            code = &mut finalize, if daemon_exit_code.is_none() => {
                daemon_exit_code = Some(match code {
                    Ok(code) => code,
                    Err(_) => {
                        fail(&mut first_error, &mut notification, SinkError::MissingDaemonExit);
                        super::exit::INTERNAL
                    }
                });
                deadline = Some(Instant::now() + DRAIN_TIMEOUT);
            }
            _ = drain_deadline(deadline) => {
                fail(&mut first_error, &mut notification, SinkError::FinalizeTimeout);
                break;
            }
            _ = check.tick() => {
                if let Some(error) = observations.status().first_error {
                    fail(&mut first_error, &mut notification, SinkError::Observation(error));
                }
            }
            record = observations.recv() => {
                let Some(record) = record else {
                    producers_closed = true;
                    break;
                };
                if let Some(error) = observations.status().first_error {
                    fail(&mut first_error, &mut notification, SinkError::Observation(error));
                }
                // Continue receiving after rejection so cleanup can retire all
                // producers. Rejected records never extend the output budget.
                if first_error.is_some() {
                    continue;
                }
                if written_records == MAX_RECORDS {
                    fail(&mut first_error, &mut notification, SinkError::RecordLimit);
                    continue;
                }
                let mut encoded = match serde_json::to_vec(&record) {
                    Ok(encoded) => encoded,
                    Err(_) => {
                        fail(&mut first_error, &mut notification, SinkError::Encode);
                        continue;
                    }
                };
                encoded.push(b'\n');
                if encoded.len() as u64 > MAX_FILE_BYTES - FOOTER_RESERVE - written_bytes {
                    fail(&mut first_error, &mut notification, SinkError::ByteLimit);
                    continue;
                }
                if file.write_all(&encoded).await.is_err() {
                    fail(&mut first_error, &mut notification, SinkError::Write);
                    continue;
                }
                written_bytes += encoded.len() as u64;
                written_records += 1;
            }
        }
    }

    // A closed channel alone cannot claim the daemon's exit outcome.
    let daemon_exit_code = match daemon_exit_code {
        Some(code) => code,
        None => match finalize.await {
            Ok(code) => code,
            Err(_) => {
                fail(
                    &mut first_error,
                    &mut notification,
                    SinkError::MissingDaemonExit,
                );
                super::exit::INTERNAL
            }
        },
    };
    let status = observations.status();
    let drained = observations.finish().is_ok();
    if let Some(error) = status.first_error {
        fail(
            &mut first_error,
            &mut notification,
            SinkError::Observation(error),
        );
    }
    if !drained {
        fail(
            &mut first_error,
            &mut notification,
            SinkError::Observation(WorkspaceObservationError::NotDrained),
        );
    }
    if file.flush().await.is_err() || file.sync_all().await.is_err() {
        fail(&mut first_error, &mut notification, SinkError::PayloadSync);
    }
    let complete = daemon_exit_code == super::exit::SUCCESS
        && first_error.is_none()
        && producers_closed
        && drained
        && status.accepted_records == written_records
        && status.received_records == written_records;
    let footer = Footer {
        record: "workspace_observation_footer",
        revision: 1,
        run_id: &run_id,
        accepted_records: status.accepted_records,
        received_records: status.received_records,
        written_records,
        written_bytes,
        producers_closed,
        drained,
        daemon_exit_code,
        complete,
        first_error: first_error.map(SinkError::code),
    };
    let mut encoded = match serde_json::to_vec(&footer) {
        Ok(encoded) if (encoded.len() as u64) < FOOTER_RESERVE => encoded,
        _ => return false,
    };
    encoded.push(b'\n');
    if file.write_all(&encoded).await.is_err() {
        fail(&mut first_error, &mut notification, SinkError::FooterWrite);
        return false;
    }
    if file.flush().await.is_err() || file.sync_all().await.is_err() {
        fail(&mut first_error, &mut notification, SinkError::FooterSync);
        return false;
    }
    if !matches!(
        tokio::task::spawn_blocking(move || parent.sync_all()).await,
        Ok(Ok(()))
    ) {
        fail(
            &mut first_error,
            &mut notification,
            SinkError::DirectorySync,
        );
        return false;
    }
    complete
}

#[cfg(unix)]
fn create_file(path: &std::path::Path) -> io::Result<(std::fs::File, std::fs::File)> {
    use std::{
        ffi::CString,
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::ffi::OsStrExt,
        },
        path::Component,
    };

    if !path.is_absolute() {
        return Err(io::Error::other(
            "observation output must be an absolute path",
        ));
    }
    let components: Vec<_> = path.components().collect();
    let Some(Component::Normal(name)) = components.last() else {
        return Err(io::Error::other("observation output requires a file name"));
    };
    let open = |parent: i32, name: &std::ffi::OsStr, flags: i32, mode: libc::mode_t| {
        let name = CString::new(name.as_bytes())
            .map_err(|_| io::Error::other("observation output contains NUL"))?;
        let fd = unsafe { libc::openat(parent, name.as_ptr(), flags, mode as libc::c_uint) };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(unsafe { std::fs::File::from_raw_fd(fd) })
        }
    };
    let directory_flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    let mut parent = open(
        libc::AT_FDCWD,
        std::ffi::OsStr::new("/"),
        directory_flags,
        0,
    )?;
    for component in &components[1..components.len() - 1] {
        let Component::Normal(name) = component else {
            return Err(io::Error::other(
                "observation output contains a non-normal component",
            ));
        };
        parent = open(parent.as_raw_fd(), name, directory_flags, 0)?;
    }
    let file = open(
        parent.as_raw_fd(),
        name,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        0o600,
    )?;
    // Only this newly created descriptor is changed; no parent is created or
    // rewritten. Force exact private permissions even under a restrictive umask.
    if unsafe { libc::fchmod(file.as_raw_fd(), 0o600) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((file, parent))
}

#[cfg(not(unix))]
fn create_file(_path: &std::path::Path) -> io::Result<(std::fs::File, std::fs::File)> {
    Err(io::Error::other(
        "workspace observations require Unix directory descriptors",
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::{symlink, PermissionsExt};

    use super::*;

    const RUN: &str = "11111111-2222-4333-8444-555555555555";

    #[test]
    fn observation_failure_codes_are_flat_closed_strings() {
        for (error, code) in [
            (WorkspaceObservationError::QueueFull, "queue_full"),
            (WorkspaceObservationError::InvalidBinding, "invalid_binding"),
            (
                WorkspaceObservationError::ReceiverDropped,
                "receiver_dropped",
            ),
            (WorkspaceObservationError::NotDrained, "not_drained"),
        ] {
            let footer = Footer {
                record: "workspace_observation_footer",
                revision: 1,
                run_id: RUN,
                accepted_records: 0,
                received_records: 0,
                written_records: 0,
                written_bytes: 0,
                producers_closed: false,
                drained: false,
                daemon_exit_code: 1,
                complete: false,
                first_error: Some(SinkError::Observation(error).code()),
            };
            assert_eq!(serde_json::to_value(footer).unwrap()["first_error"], code);
        }
    }

    #[test]
    fn output_is_exclusive_private_and_rejects_symlink_ancestors() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let path = root.join("observations.jsonl");
        let (file, _parent) = create_file(&path).unwrap();
        assert_eq!(file.metadata().unwrap().permissions().mode() & 0o777, 0o600);
        assert!(create_file(&path).is_err());
        assert!(create_file(std::path::Path::new("relative.jsonl")).is_err());
        let other = tempfile::tempdir().unwrap();
        symlink(other.path(), root.join("link")).unwrap();
        assert!(create_file(&root.join("link/foreign.jsonl")).is_err());
        assert!(!other.path().join("foreign.jsonl").exists());
        symlink(other.path().join("absent"), root.join("final-link")).unwrap();
        assert!(create_file(&root.join("final-link")).is_err());
        assert!(!other.path().join("absent").exists());
        assert!(create_file(&root.join("absent-parent/file")).is_err());
        assert!(!temp.path().join("absent-parent").exists());
    }

    #[tokio::test]
    async fn closed_empty_producers_require_daemon_success_and_a_complete_footer() {
        for code in [super::super::exit::SUCCESS, super::super::exit::CONFIG] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp
                .path()
                .canonicalize()
                .unwrap()
                .join("observations.jsonl");
            let (observer, sink) = ObservationSink::start(ObservationFileOptions {
                path: path.clone(),
                run_id: RUN.into(),
            })
            .unwrap();
            drop(observer);
            assert_eq!(sink.finish(code).await, code == 0);
            let footer: serde_json::Value =
                serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            assert_eq!(footer["record"], "workspace_observation_footer");
            assert_eq!(footer["daemon_exit_code"], code);
            assert_eq!(footer["complete"], code == 0);
            assert_eq!(footer["producers_closed"], true);
            assert_eq!(footer["drained"], true);
            assert_eq!(footer["written_records"], 0);
            assert_eq!(footer["written_bytes"], 0);
        }
    }

    #[tokio::test]
    async fn retained_producer_cannot_claim_a_complete_footer() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp
            .path()
            .canonicalize()
            .unwrap()
            .join("observations.jsonl");
        let (observer, sink) = ObservationSink::start(ObservationFileOptions {
            path: path.clone(),
            run_id: RUN.into(),
        })
        .unwrap();
        assert!(!sink.finish(0).await);
        let footer: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(footer["complete"], false);
        assert_eq!(footer["producers_closed"], false);
        assert_eq!(footer["drained"], false);
        assert_eq!(footer["first_error"], "finalize_timeout");
        drop(observer);
    }

    #[tokio::test]
    async fn unwritable_owned_file_rejects_acceptance_and_notifies_the_daemon() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let path = root.join("observations.jsonl");
        let (writable, parent) = create_file(&path).unwrap();
        drop(writable);
        let readonly = std::fs::File::open(&path).unwrap();
        let (observer, observations) = WorkspaceObserver::channel(RUN, 1).unwrap();
        let (notification, notified) = oneshot::channel();
        let (finalize, daemon_exit) = oneshot::channel();
        drop(observer);
        finalize.send(0).unwrap();
        assert!(
            !write_observations(
                RUN.into(),
                observations,
                readonly,
                parent,
                notification,
                daemon_exit,
            )
            .await
        );
        assert!(notified.await.is_ok());
        assert!(std::fs::read(path).unwrap().is_empty());
    }
}
