//! Fixed MST/2 snapshots and v3 workspaces.
//!
//! A workspace owns one verified, fixed snapshot reader, a private writable
//! upper and its native mutation fence. Commit updates create another workspace;
//! existing mounts and open handles retain their original snapshot.
//!
//! [`workspace::WorkspaceService`] owns creation, hydration, pin release and
//! safe destruction. [`snapshot`] contains the verified transport, namespace
//! proofs, shared durable CAS and FUSE lower layer.

#[macro_use]
extern crate log;

pub mod antares;
pub mod cli;
pub mod daemon;
pub mod dicfuse;
pub mod doctor;
pub mod fuse;
pub mod manager;
pub mod server;
pub mod snapshot;
pub mod util;
pub mod workspace;

pub mod prelude {
    pub use crate::{
        antares::{fuse::AntaresFuse, AntaresConfig, AntaresManager, AntaresPaths},
        daemon::antares::{
            AntaresDaemon, AntaresService, AntaresServiceImpl, ApiError, BuildClRequest,
            CreateMountRequest, ErrorBody, HealthResponse, MountCollection, MountCreated,
            MountLayers, MountLifecycle, MountReadyResponse, MountStatus, PersistedMountState,
            PersistedState, ServiceError, StateOwnership,
        },
        dicfuse::DicfuseManager,
        snapshot::{Mst2Client, SnapshotReader},
        workspace::{
            CreateWorkspace, DestroyWorkspace, WorkspaceConfig, WorkspaceService, WorkspaceStatus,
        },
    };
}

pub use antares::{AntaresConfig, AntaresManager, AntaresPaths};

const READONLY_INODE: u64 = 0xffff_ffff;
