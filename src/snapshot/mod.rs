//! MST/2 fixed-view snapshot client (spec 03/04).
//!
//! - [`client::Mst2Client`] is the thin HTTP transport.
//! - [`reader::SnapshotReader`] resolves a view, walks verified directory
//!   pages and reads digest-verified file content.
//! - [`durable::DurableStore`] hydrates a view into a local verified CAS with
//!   resume, a completeness marker and a pin.

pub mod auth;
mod cas_index;
pub mod client;
pub mod closure;
pub mod coordinator;
mod directory;
pub mod durable;
pub mod frames;
pub mod fuse;
pub mod incremental;
pub mod layer;
mod lookup;
pub mod range;
pub mod reader;
pub mod types;

pub use auth::{AuthorizedSnapshotContext, CacheDomain};
pub use client::Mst2Client;
pub use closure::{SnapshotClosureMeters, SnapshotDirectory, ValidatedSnapshotClosure};
pub use coordinator::{FetchCoordinator, FetchCoordinatorCounts, FetchCoordinatorLimits};
pub use durable::{CompletionKind, DurableStore, HydrateReport, LocalCasRangeMeters, ViewMeta};
pub use frames::LeaseReleaseOutcome;
pub use incremental::{ClosureRecord, IncrementalSync, ScopeCache, SyncMeters};
pub use range::{ChunkedFile, OBJECT_CAP};
pub use reader::{SnapshotFile, SnapshotReader};
pub use types::{
    Capabilities, Descriptor, DirEntry, DirectoryResponse, LookupResult, SnapshotError,
    SnapshotErrorCode,
};
