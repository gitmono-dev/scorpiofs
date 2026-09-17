//! MST/2 fixed-view snapshot client (spec 03/04).
//!
//! - [`client::Mst2Client`] is the thin HTTP transport.
//! - [`reader::SnapshotReader`] resolves a view, walks verified directory
//!   pages and reads digest-verified file content.
//! - [`durable::DurableStore`] hydrates a view into a local verified CAS with
//!   resume, a completeness marker and a pin.

pub mod client;
pub mod coordinator;
pub mod durable;
pub mod frames;
pub mod fuse;
pub mod reader;
pub mod types;

pub use client::Mst2Client;
pub use coordinator::FetchCoordinator;
pub use durable::{DurableStore, HydrateReport, ViewMeta};
pub use reader::{SnapshotFile, SnapshotReader};
pub use types::{
    Capabilities, Descriptor, DirEntry, DirectoryResponse, LookupResult, SnapshotError,
    SnapshotErrorCode,
};
