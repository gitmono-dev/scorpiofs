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

use scorpiofs::{
    server,
    snapshot::{durable::DurableStore, fuse::Mst2Fuse, Mst2Client, SnapshotReader},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mountpoint = std::env::args()
        .nth(1)
        .ok_or("usage: mst2_mount <mountpoint>")?;

    let fs = match std::env::var("M2_STORE_DIR") {
        Ok(dir) if !dir.is_empty() => {
            // Offline reopen: no network, no re-resolution. The content came
            // from the scope-level cache the online hydration wrote to, so
            // reopen the same way; a store that predates the shared cache
            // (view-local blobs) still opens if the scope cache is absent.
            let view = std::path::PathBuf::from(&dir);
            let scope_blobs = view
                .parent()
                .map(|scope| scope.join("blobs"))
                .filter(|p| p.exists());
            let store = Arc::new(match &scope_blobs {
                Some(content) => DurableStore::open_with_content(&view, content)?,
                None => DurableStore::open(&view)?,
            });
            let manifest = store.manifest()?;
            let verified = store.verify_all(&manifest)?;
            eprintln!(
                "reopened {dir} (content {}): {verified} files re-verified, no server contact",
                store.content_dir().display()
            );
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
            // Lazy by default: mount as soon as the root page arrives; content
            // materializes on open. M2_LAZY=0 restores full hydration before
            // the mount (offline-export semantics).
            let lazy = std::env::var("M2_LAZY").map(|v| v != "0").unwrap_or(true);

            let client = Mst2Client::with_token(
                base,
                std::env::var("M2_TOKEN")
                    .ok()
                    .or_else(|| std::env::var("MST2_TOKEN").ok()),
            );
            eprintln!("resolving {scope} ...");
            let reader = SnapshotReader::resolve(client, &scope, lease).await?;
            let snapshot_id = reader.snapshot_id().to_string();
            eprintln!("snapshot {snapshot_id}");

            let dir =
                DurableStore::path_for(std::path::Path::new(&store_root), &scope, &snapshot_id);
            // Content is shared by every view of the scope (spec 11 §3), and
            // verified subtrees are reused across versions (spec 11 §10).
            let scope_dir = dir.parent().expect("snapshot dir has a scope parent");
            let content_dir = scope_dir.join("blobs");
            let store = Arc::new(DurableStore::open_with_content(&dir, &content_dir)?);
            let was_complete = store.is_complete()?;

            if lazy {
                eprintln!("lazy mount: tree loads per directory on access");
                Mst2Fuse::from_reader_lazy(reader, Some(store)).await?
            } else {
                let cache = scorpiofs::snapshot::ScopeCache::open(scope_dir)?;
                let mut sync = scorpiofs::snapshot::IncrementalSync::new(&reader, &cache);
                let manifest = sync.sync().await?;
                let meters = sync.meters();
                eprintln!(
                "sync: traversal_nodes={} fetched_pages={} reused_pages={} reused_subtrees={} files={}",
                meters.traversal_nodes,
                meters.fetched_pages,
                meters.reused_pages,
                meters.reused_subtrees,
                manifest.len()
            );
                let view = scorpiofs::snapshot::ViewMeta {
                    snapshot_id: snapshot_id.clone(),
                    namespace_view_id: reader.descriptor.namespace_view_id.clone(),
                    scope: reader.descriptor.scope.clone(),
                    lease_id: reader.lease_id.clone(),
                };
                // Frame transport when advertised (OBJECT for small files,
                // chunk-map/CHUNK for >256 KiB); raw blob otherwise.
                let use_frames = reader.capabilities().features.objects
                    && reader.capabilities().features.chunk_reads;
                let concurrency = std::env::var("M2_CONCURRENCY")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(4usize);
                let report = if use_frames {
                    // Batched: small files ride OBJECT batches (128/request),
                    // large files use chunk-map + CHUNK frames per file.
                    let client = reader.client.clone();
                    let encoding = reader.encoding_hint().map(str::to_string);
                    let sid = reader.snapshot_id().to_string();
                    let reader_large = reader.clone();
                    store
                        .hydrate_batches(
                            &view,
                            &manifest,
                            concurrency,
                            concurrency,
                            move |batch| {
                                let client = client.clone();
                                let encoding = encoding.clone();
                                let sid = sid.clone();
                                Box::pin(async move {
                                    let items: Vec<(String, String)> = batch
                                        .iter()
                                        .map(|f| {
                                            (format!("/{}", f.rel_path), f.content_digest.clone())
                                        })
                                        .collect();
                                    let got =
                                        client.objects(&sid, &items, encoding.as_deref()).await?;
                                    let mut out = std::collections::HashMap::new();
                                    for f in &batch {
                                        let want = scorpiofs::snapshot::frames::parse_digest(
                                            &f.content_digest,
                                        )?;
                                        let data = got.get(&want).ok_or_else(|| {
                                            scorpiofs::snapshot::SnapshotError::new(
                                            scorpiofs::snapshot::SnapshotErrorCode::DigestMismatch,
                                            format!("batch missing {}", f.content_digest),
                                        )
                                        })?;
                                        out.insert(
                                            f.content_digest.clone(),
                                            std::sync::Arc::new(data.clone()),
                                        );
                                    }
                                    Ok(out)
                                })
                            },
                            move |f| {
                                let reader = reader_large.clone();
                                Box::pin(async move {
                                    let bytes = reader
                                        .read_file_frames(&f.rel_path, &f.content_digest, f.size)
                                        .await?;
                                    Ok(std::sync::Arc::new(bytes))
                                })
                            },
                        )
                        .await?
                } else {
                    let coordinator =
                        scorpiofs::snapshot::FetchCoordinator::new(reader.clone(), concurrency);
                    store
                        .hydrate_concurrent(&view, &manifest, concurrency, move |f| {
                            let coordinator = coordinator.clone();
                            Box::pin(async move { coordinator.fetch(f, use_frames).await })
                        })
                        .await?
                };
                store.pin(&view)?;
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
                Mst2Fuse::from_manifest(reader, store, manifest)?
            }
        }
    };

    let handle = server::mount_filesystem(fs, std::ffi::OsStr::new(&mountpoint)).await?;
    eprintln!("mounted at {mountpoint}; Ctrl-C to unmount");
    tokio::signal::ctrl_c().await.ok();
    drop(handle);
    Ok(())
}
