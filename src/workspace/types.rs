use serde::{Deserialize, Serialize};

use crate::snapshot::{ResolveDelivery, ResolveRequest, ResolveTarget};

/// Versioning belongs to the /v3 route. Old revision and dictionary fields are
/// rejected rather than interpreted as a fixed snapshot.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateWorkspace {
    pub target: WorkspaceTarget,
    pub scope: String,
    pub delivery: WorkspaceDelivery,
    pub upper_policy: UpperPolicy,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkspaceTarget {
    Latest,
    View { view_id: String },
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceDelivery {
    Lazy,
    Full,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpperPolicy {
    Private,
}

impl CreateWorkspace {
    pub(crate) fn resolve_request(&self, lease_seconds: u64) -> ResolveRequest {
        ResolveRequest {
            target: match &self.target {
                WorkspaceTarget::Latest => ResolveTarget::Latest,
                WorkspaceTarget::View { view_id } => ResolveTarget::View {
                    view_id: view_id.clone(),
                },
            },
            scope: self.scope.clone(),
            delivery: match self.delivery {
                WorkspaceDelivery::Lazy => ResolveDelivery::Lazy,
                WorkspaceDelivery::Full => ResolveDelivery::Full,
            },
            lease_seconds,
        }
    }
}

/// Omission always rejects loss of dirty upper contents.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestroyWorkspace {
    #[serde(default)]
    pub discard_dirty: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MountState {
    Creating,
    Mounted,
    Retiring,
    Unmounted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HydrationState {
    Idle,
    Running,
    Complete,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DirtyState {
    Clean,
    Dirty,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PinState {
    Incomplete,
    CompleteSnapshot,
    FileClosureOnly,
    Revoking,
    Released,
    Unknown,
}

/// Observation of the reader's local retention grant, not an offline access
/// grant or a fresh remote authorization decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    NotResolved,
    GrantedLocally,
    Expired,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceStatus {
    pub workspace_id: String,
    pub generation: String,
    pub snapshot_id: Option<String>,
    pub mountpoint: String,
    pub mount_state: MountState,
    pub metadata_ready: bool,
    pub hydration_state: HydrationState,
    pub dirty_state: DirtyState,
    pub lease_state: LeaseState,
    pub local_pin_state: PinState,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{code}: {message}")]
pub struct WorkspaceError {
    pub code: &'static str,
    pub message: String,
}

impl WorkspaceError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl From<std::io::Error> for WorkspaceError {
    fn from(error: std::io::Error) -> Self {
        Self::new("WORKSPACE_IO", error.to_string())
    }
}

impl From<crate::snapshot::SnapshotError> for WorkspaceError {
    fn from(error: crate::snapshot::SnapshotError) -> Self {
        Self::new("SNAPSHOT_ERROR", error.to_string())
    }
}
