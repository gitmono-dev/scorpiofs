//! ScorpioFS / Worktree v3: fixed snapshots and private workspace ownership.

pub mod http;
mod hydrate;
mod mount;
mod observation;
mod service;
mod types;

pub use observation::{
    WorkspaceObservationError, WorkspaceObservationKind, WorkspaceObservationStatus,
    WorkspaceObservations, WorkspaceObserver, WorkspaceResolveBinding, WorkspaceResolveReceipt,
    MAX_WORKSPACE_OBSERVATIONS,
};
pub use service::{WorkspaceConfig, WorkspaceService};
pub use types::*;
