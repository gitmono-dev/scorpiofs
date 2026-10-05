//! ScorpioFS / Worktree v3: fixed snapshots and private workspace ownership.

pub mod http;
mod mount;
mod service;
mod types;

pub use service::{WorkspaceConfig, WorkspaceService};
pub use types::*;
