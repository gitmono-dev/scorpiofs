//! MST/2 fixed-view snapshot client (spec 03/04).
//!
//! - [`client::Mst2Client`] is the thin HTTP transport.
//! - [`reader::SnapshotReader`] resolves a view, walks verified directory
//!   pages and reads digest-verified file content.

pub mod client;
pub mod reader;
pub mod types;

pub use client::Mst2Client;
pub use reader::{SnapshotFile, SnapshotReader};
pub use types::{
    Capabilities, Descriptor, DirectoryResponse, DirEntry, LookupResult, SnapshotError,
    SnapshotErrorCode,
};
