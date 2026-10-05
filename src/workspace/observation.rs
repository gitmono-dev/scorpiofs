//! Opt-in binding evidence from the reader actually owned by a workspace.
//! A binding record does not certify a native mount or durable completion.

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc,
    },
};

use mst2_codec::descriptor::ServingDescriptor;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::snapshot::{
    durable::digest_of, frames::parse_digest, DurableStore, ResolveTraceReceipt, SnapshotReader,
};

pub const MAX_WORKSPACE_OBSERVATIONS: usize = 64;
const MAX_RECORD_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceObservationError {
    #[error("observation run UUID must be canonical and non-nil")]
    InvalidRunId,
    #[error("observation capacity is outside its fixed limit")]
    InvalidCapacity,
    #[error("observation queue is full")]
    QueueFull,
    #[error("observation receiver was dropped")]
    ReceiverDropped,
    #[error("observation binding could not be recorded")]
    InvalidBinding,
    #[error("observation producers or records have not been drained")]
    NotDrained,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceResolveReceipt {
    pub logical_request_id: String,
    pub attempt_ids: Vec<String>,
    pub final_attempt_id: String,
    pub retry_count: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceObservationKind {
    WorkspaceResolveBinding,
}

/// Closed diagnostic data, without credentials or retention/access grants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceResolveBinding {
    pub record: WorkspaceObservationKind,
    pub revision: u8,
    pub run_id: String,
    pub workspace_id: String,
    pub generation: String,
    pub logical_request_id: String,
    pub resolve_trace_receipt: WorkspaceResolveReceipt,
    pub descriptor_bytes_hex: String,
    pub instance_id: String,
    pub namespace_view_id: String,
    pub snapshot_id: String,
    pub scope: String,
    pub publication_sequence: u64,
    pub store: PathBuf,
    pub content_store: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkspaceObservationStatus {
    pub accepted_records: u64,
    pub received_records: u64,
    pub first_error: Option<WorkspaceObservationError>,
}

#[derive(Default)]
struct State {
    accepted: AtomicU64,
    received: AtomicU64,
    error: AtomicU8,
}

impl State {
    fn fail(&self, error: WorkspaceObservationError) {
        let code = match error {
            WorkspaceObservationError::QueueFull => 1,
            WorkspaceObservationError::ReceiverDropped => 2,
            WorkspaceObservationError::InvalidBinding => 3,
            WorkspaceObservationError::NotDrained => 4,
            WorkspaceObservationError::InvalidRunId
            | WorkspaceObservationError::InvalidCapacity => {
                unreachable!("configuration errors do not enter an active observer")
            }
        };
        let _ = self
            .error
            .compare_exchange(0, code, Ordering::AcqRel, Ordering::Acquire);
    }

    fn status(&self) -> WorkspaceObservationStatus {
        WorkspaceObservationStatus {
            accepted_records: self.accepted.load(Ordering::Acquire),
            received_records: self.received.load(Ordering::Acquire),
            first_error: match self.error.load(Ordering::Acquire) {
                0 => None,
                1 => Some(WorkspaceObservationError::QueueFull),
                2 => Some(WorkspaceObservationError::ReceiverDropped),
                3 => Some(WorkspaceObservationError::InvalidBinding),
                _ => Some(WorkspaceObservationError::NotDrained),
            },
        }
    }
}

/// Library-owned, bounded, nonblocking producer. No caller callback runs in
/// the workspace's lifecycle path. Errors are sticky evidence failures; they
/// never abandon the actual workspace owner or change its cleanup policy.
pub struct WorkspaceObserver {
    run_id: String,
    sender: mpsc::Sender<WorkspaceResolveBinding>,
    state: Arc<State>,
}

pub struct WorkspaceObservations {
    receiver: mpsc::Receiver<WorkspaceResolveBinding>,
    state: Arc<State>,
    finished: bool,
}

impl WorkspaceObserver {
    pub fn channel(
        run_id: &str,
        capacity: usize,
    ) -> Result<(Arc<Self>, WorkspaceObservations), WorkspaceObservationError> {
        let uuid =
            uuid::Uuid::parse_str(run_id).map_err(|_| WorkspaceObservationError::InvalidRunId)?;
        if uuid.is_nil() || uuid.to_string() != run_id {
            return Err(WorkspaceObservationError::InvalidRunId);
        }
        if !(1..=MAX_WORKSPACE_OBSERVATIONS).contains(&capacity) {
            return Err(WorkspaceObservationError::InvalidCapacity);
        }
        let (sender, receiver) = mpsc::channel(capacity);
        let state = Arc::new(State::default());
        Ok((
            Arc::new(Self {
                run_id: run_id.into(),
                sender,
                state: state.clone(),
            }),
            WorkspaceObservations {
                receiver,
                state,
                finished: false,
            },
        ))
    }

    /// Counters are cumulative and nontransactional while producers run.
    /// A successful finish after producer retirement establishes final counts.
    pub fn status(&self) -> WorkspaceObservationStatus {
        self.state.status()
    }

    pub(crate) fn logical_id(&self, workspace_id: &str) -> String {
        // Both UUIDs are generated/validated locally. This is 76 ASCII bytes,
        // below the reader's 125-byte logical-id cap even with retry suffixes.
        format!("ws:{}:{workspace_id}", self.run_id)
    }

    pub(crate) fn record_binding(
        &self,
        workspace_id: &str,
        generation: &str,
        reader: &SnapshotReader,
        receipt: &ResolveTraceReceipt,
        store: &DurableStore,
        cache_root: &Path,
    ) {
        let record = (|| {
            for id in [workspace_id, generation] {
                let uuid = uuid::Uuid::parse_str(id).ok()?;
                if uuid.is_nil() || uuid.to_string() != id {
                    return None;
                }
            }
            let logical_id = self.logical_id(workspace_id);
            if receipt.logical_request_id() != logical_id {
                return None;
            }
            let binding = store.workspace_binding().ok()??;
            if binding.workspace_id() != workspace_id
                || binding.snapshot_id() != reader.snapshot_id()
            {
                return None;
            }
            let context = reader.authorized_context();
            if store.root()
                != context
                    .view_cache_dir(cache_root)
                    .ok()?
                    .join("owners")
                    .join(workspace_id)
                || store.content_dir() != context.scope_cache_dir(cache_root).join("blobs")
            {
                // Derive both paths from the actual reader authority, including
                // its instance/scope domain and fixed descriptor/SID binding.
                return None;
            }
            let descriptor = reader.descriptor();
            let encoded = ServingDescriptor {
                instance_uuid: *uuid::Uuid::parse_str(&descriptor.instance_id)
                    .ok()?
                    .as_bytes(),
                namespace_view_id: parse_digest(&descriptor.namespace_view_id).ok()?,
                scope: descriptor.scope.clone(),
                metadata_root: parse_digest(&descriptor.metadata_root).ok()?,
            }
            .encode()
            .ok()?;
            if digest_of(&encoded) != reader.snapshot_id() {
                return None;
            }
            let record = WorkspaceResolveBinding {
                record: WorkspaceObservationKind::WorkspaceResolveBinding,
                revision: 1,
                run_id: self.run_id.clone(),
                workspace_id: workspace_id.into(),
                generation: generation.into(),
                logical_request_id: logical_id,
                resolve_trace_receipt: WorkspaceResolveReceipt {
                    logical_request_id: receipt.logical_request_id().into(),
                    attempt_ids: receipt.attempt_ids().to_vec(),
                    final_attempt_id: receipt.final_attempt_id().into(),
                    retry_count: receipt.retry_count(),
                },
                descriptor_bytes_hex: hex::encode(encoded),
                instance_id: descriptor.instance_id.clone(),
                namespace_view_id: descriptor.namespace_view_id.clone(),
                snapshot_id: reader.snapshot_id().into(),
                scope: descriptor.scope.clone(),
                publication_sequence: context.publication_sequence(),
                store: store.root().into(),
                content_store: store.content_dir().into(),
            };
            if serde_json::to_vec(&record).ok()?.len() > MAX_RECORD_BYTES {
                return None;
            }
            Some(record)
        })();
        let Some(record) = record else {
            self.state.fail(WorkspaceObservationError::InvalidBinding);
            return;
        };
        match self.sender.try_send(record) {
            Ok(()) => {
                self.state.accepted.fetch_add(1, Ordering::AcqRel);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.state.fail(WorkspaceObservationError::QueueFull);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.state.fail(WorkspaceObservationError::ReceiverDropped);
            }
        }
    }
}

impl WorkspaceObservations {
    pub async fn recv(&mut self) -> Option<WorkspaceResolveBinding> {
        let record = self.receiver.recv().await?;
        self.state.received.fetch_add(1, Ordering::AcqRel);
        Some(record)
    }

    pub fn try_recv(&mut self) -> Result<WorkspaceResolveBinding, mpsc::error::TryRecvError> {
        let record = self.receiver.try_recv()?;
        self.state.received.fetch_add(1, Ordering::AcqRel);
        Ok(record)
    }

    pub fn status(&self) -> WorkspaceObservationStatus {
        self.state.status()
    }

    /// Retire all producer owners first, then drain to channel closure. A live
    /// producer, unread record or earlier evidence loss rejects final acceptance.
    pub fn finish(mut self) -> Result<WorkspaceObservationStatus, WorkspaceObservationError> {
        if !self.receiver.is_closed() || !self.receiver.is_empty() {
            self.state.fail(WorkspaceObservationError::NotDrained);
        }
        let counts = self.state.status();
        if counts.accepted_records != counts.received_records {
            self.state.fail(WorkspaceObservationError::NotDrained);
        }
        self.finished = true;
        let status = self.state.status();
        match status.first_error {
            Some(error) => Err(error),
            None => Ok(status),
        }
    }
}

impl Drop for WorkspaceObservations {
    fn drop(&mut self) {
        if !self.finished {
            self.state.fail(WorkspaceObservationError::ReceiverDropped);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_configuration_and_lost_receivers_cannot_certify_a_complete_observation() {
        let run = "11111111-2222-4333-8444-555555555555";
        for invalid in [
            "",
            "11111111222243338444555555555555",
            "00000000-0000-0000-0000-000000000000",
        ] {
            assert!(matches!(
                WorkspaceObserver::channel(invalid, 1),
                Err(WorkspaceObservationError::InvalidRunId)
            ));
        }
        for capacity in [0, MAX_WORKSPACE_OBSERVATIONS + 1] {
            assert!(matches!(
                WorkspaceObserver::channel(run, capacity),
                Err(WorkspaceObservationError::InvalidCapacity)
            ));
        }
        let (producer, receiver) = WorkspaceObserver::channel(run, 1).unwrap();
        drop(receiver);
        assert_eq!(
            producer.status().first_error,
            Some(WorkspaceObservationError::ReceiverDropped)
        );
        let (producer, receiver) = WorkspaceObserver::channel(run, 1).unwrap();
        assert_eq!(
            receiver.finish(),
            Err(WorkspaceObservationError::NotDrained)
        );
        drop(producer);
        let (producer, receiver) = WorkspaceObserver::channel(run, 1).unwrap();
        drop(producer);
        assert_eq!(receiver.finish().unwrap().accepted_records, 0);
    }
}
