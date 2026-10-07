//! Fixed MST/2 snapshots and v3 workspaces.
//!
//! A workspace owns one verified, fixed snapshot reader, a private writable
//! upper and its native mutation fence. Commit updates create another workspace;
//! existing mounts and open handles retain their original snapshot.
//!
//! [`workspace::WorkspaceService`] owns creation, hydration, pin release and
//! safe destruction. [`snapshot`] contains the verified transport, namespace
//! proofs, shared durable CAS and FUSE lower layer.

pub mod cli;
pub mod doctor;
pub mod server;
pub mod snapshot;
pub mod util;
pub mod workspace;

pub mod prelude {
    pub use crate::{
        snapshot::{Mst2Client, SnapshotReader},
        workspace::{
            CreateWorkspace, DestroyWorkspace, WorkspaceConfig, WorkspaceService, WorkspaceStatus,
        },
    };
}
