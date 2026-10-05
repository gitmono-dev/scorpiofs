//! MST/2 fixed-view snapshot client (spec 03/04).
//!
//! - [`client::Mst2Client`] is the thin HTTP transport.
//! - [`reader::SnapshotReader`] resolves a view, walks verified directory
//!   pages and reads digest-verified file content.
//! - [`durable::DurableStore`] hydrates a view into a local verified CAS with
//!   resume, a completeness marker and a pin.

pub mod auth;
pub mod capabilities;
mod cas_index;
mod chunk_wire;
pub mod client;
pub mod closure;
mod content;
pub mod coordinator;
mod descriptor_wire;
mod directory;
pub mod durable;
pub mod error_wire;
pub mod frames;
pub mod fuse;
pub mod incremental;
pub mod layer;
mod lease_wire;
mod lookup;
mod owned_range;
mod owned_reader;
mod owned_transport;
mod proven_file;
pub mod range;
pub mod reader;
mod resolve_receipt;
mod resolve_wire;
pub mod types;

pub use auth::{AuthorizedSnapshotContext, CacheDomain};
pub use client::Mst2Client;
pub use closure::{SnapshotClosureMeters, SnapshotDirectory, ValidatedSnapshotClosure};
pub use content::{ContentBudgetLimits, ContentBudgetUsage, VerifiedContent, VerifiedContentBatch};
pub use coordinator::{FetchCoordinator, FetchCoordinatorCounts, FetchCoordinatorLimits};
pub use durable::{CompletionKind, DurableStore, HydrateReport, LocalCasRangeMeters, ViewMeta};
pub use frames::LeaseReleaseOutcome;
pub use incremental::{ClosureRecord, IncrementalSync, ScopeCache, SyncMeters};
pub use owned_range::{OwnedChunkedFile, VerifiedRange};
pub use proven_file::{FileMembershipError, ProvenSnapshotFile};
pub use range::{ChunkedFile, OBJECT_CAP};
pub use reader::{SnapshotFile, SnapshotReader};
pub use resolve_receipt::ResolveTraceReceipt;
pub use types::{
    Capabilities, Descriptor, DirEntry, DirectoryResponse, LookupResult, SnapshotError,
    SnapshotErrorCode,
};
