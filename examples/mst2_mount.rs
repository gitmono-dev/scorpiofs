//! Mount a fixed MST/2 snapshot read-only.
//!
//! Online (default): resolve `latest`, hydrate the whole view into a verified
//! local store, pin it, then serve FUSE from that store.
//!
//! Offline reopen: `M2_STORE_DIR=<dir>` reopens a completed hydration with no
//! server contact, so a mount keeps serving the view it was pinned to even
//! after the branch moves (spec SYS-01).
//!
//! Usage:
//!   M2_BASE=http://127.0.0.1:19700 M2_SCOPE=/project \
//!     cargo run --example mst2_mount -- <mountpoint>
//!   M2_STORE_DIR=/var/lib/scorpio/mst2/snapshots/project/<hex> \
//!     cargo run --example mst2_mount -- <mountpoint>

use std::sync::Arc;

use scorpiofs::server;
use scorpiofs::snapshot::durable::DurableStore;
use scorpiofs::snapshot::fuse::Mst2Fuse;
use scorpiofs::snapshot::{Mst2Client, SnapshotReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mountpoint = std::env::args()
        .nth(1)
        .ok_or("usage: mst2_mount <mountpoint>")?;

    let fs = match std::env::var("M2_STORE_DIR") {
        Ok(dir) if !dir.is_empty() => {
            // Offline reopen: no network, no re-resolution.
            let store = Arc::new(DurableStore::open(&dir)?);
            let manifest = store.manifest()?;
            let verified = store.verify_all(&manifest)?;
            eprintln!("reopened {dir}: {verified} files re-verified, no server contact");
            Mst2Fuse::from_store(store)?
        }
        _ => {
            let base = std::env::var("M2_BASE").unwrap_or_else(|_| "http://127.0.0.1:19700".into());
            let scope = std::env::var("M2_SCOPE").unwrap_or_else(|_| "/project".into());
            let lease = std::env::var("M2_LEASE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3600);
            let store_root =
                std::env::var("M2_STORE_ROOT").unwrap_or_else(|_| "/var/lib/scorpio/mst2".into());

            let client = Mst2Client::new(base);
            eprintln!("resolving {scope} ...");
            let reader = SnapshotReader::resolve(client, &scope, lease).await?;
            let snapshot_id = reader.snapshot_id().to_string();
            eprintln!("snapshot {snapshot_id}");

            let dir =
                DurableStore::path_for(std::path::Path::new(&store_root), &scope, &snapshot_id);
            let store = Arc::new(DurableStore::open(&dir)?);
            let was_complete = store.is_complete()?;
            let report = store.hydrate(&reader).await?;
            let verified = store.verify_all(&store.manifest()?)?;
            if !store.is_complete()? {
                return Err("hydration finished without a completeness marker".into());
            }
            eprintln!(
                "store={} reopened={was_complete} files={} fetched={} resumed={} repaired={} bytes={} verified={verified}",
                dir.display(),
                report.total_files,
                report.fetched,
                report.resumed,
                report.repaired,
                report.bytes_total,
            );
            Mst2Fuse::from_reader_with_store(reader, store).await?
        }
    };

    let handle = server::mount_filesystem(fs, std::ffi::OsStr::new(&mountpoint)).await?;
    eprintln!("mounted at {mountpoint}; Ctrl-C to unmount");
    tokio::signal::ctrl_c().await.ok();
    drop(handle);
    Ok(())
}
