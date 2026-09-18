//! Run one incremental sync + hydration and report the meters
//! (spec 11 §10). Used by `mst2-impl/tmp/incr-e2e.sh` to drive the
//! cross-version scenarios.
//!
//! It mirrors the mount flow — resolve, incremental sync, hydrate into a
//! scope-shared content store, pin — so both halves of the incremental claim
//! are measurable and separate:
//!
//!  * "did not traverse everything" → `traversal_nodes` / `reused_subtrees`
//!  * "did not download again"      → hydrate `fetched` files vs `resumed`
//!
//! It also asserts that the incrementally produced manifest is *identical* to
//! a full walk of the same view: reuse must never lose, invent or mis-place a
//! file.
//!
//! Usage:
//!   M2_BASE=http://127.0.0.1:19700 M2_SCOPE=/project \
//!     M2_CACHE_DIR=/var/lib/scorpio/mst2/snapshots/project \
//!     cargo run --example mst2_sync

use std::{collections::BTreeMap, sync::Arc};

use scorpiofs::snapshot::{
    DurableStore, IncrementalSync, Mst2Client, ScopeCache, SnapshotReader, ViewMeta,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::var("M2_BASE").unwrap_or_else(|_| "http://127.0.0.1:19700".into());
    let scope = std::env::var("M2_SCOPE").unwrap_or_else(|_| "/project".into());
    let cache_dir = std::env::var("M2_CACHE_DIR").map_err(|_| "M2_CACHE_DIR is required")?;
    let lease = std::env::var("M2_LEASE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600u64);

    let reader = SnapshotReader::resolve(
        Mst2Client::with_token(
            base,
            std::env::var("M2_TOKEN")
                .ok()
                .or_else(|| std::env::var("MST2_TOKEN").ok()),
        ),
        &scope,
        lease,
    )
    .await?;
    let snapshot_id = reader.snapshot_id().to_string();
    let cache = ScopeCache::open(&cache_dir)?;
    let mut sync = IncrementalSync::new(&reader, &cache);
    let manifest = sync.sync().await?;
    let meters = sync.meters();

    // Independent correctness check: the same view walked plainly.
    let full = reader.file_manifest().await?;
    let index = |v: &[scorpiofs::snapshot::SnapshotFile]| {
        v.iter()
            .map(|f| {
                (
                    f.rel_path.clone(),
                    (f.content_digest.clone(), f.size, f.fs_kind.clone()),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    let a = index(&manifest);
    let b = index(&full);
    let consistent = a == b;
    if !consistent {
        let missing: Vec<_> = b.keys().filter(|k| !a.contains_key(*k)).take(5).collect();
        let extra: Vec<_> = a.keys().filter(|k| !b.contains_key(*k)).take(5).collect();
        eprintln!("MISMATCH missing={missing:?} extra={extra:?}");
    }

    // Hydrate into the scope-shared content store and pin, exactly like the
    // mount does — this is what makes content reuse observable and what
    // backs the closure records with a live pin.
    let cache_path = std::path::Path::new(&cache_dir);
    let view_dir = cache_path.join(snapshot_id.trim_start_matches("sha256:"));
    let store = Arc::new(DurableStore::open_with_content(
        &view_dir,
        cache_path.join("blobs"),
    )?);
    let view = ViewMeta {
        snapshot_id: snapshot_id.clone(),
        namespace_view_id: reader.descriptor.namespace_view_id.clone(),
        scope: reader.descriptor.scope.clone(),
        lease_id: reader.lease_id.clone(),
    };
    // Hydrate from the *incremental* manifest (never a second full walk),
    // through the single-flight coordinator.
    let coordinator = scorpiofs::snapshot::FetchCoordinator::new(reader.clone(), 4);
    let report = store
        .hydrate_concurrent(&view, &manifest, 4, move |f| {
            let coordinator = coordinator.clone();
            Box::pin(async move { coordinator.fetch(f, false).await })
        })
        .await?;
    store.pin(&view)?;

    println!(
        "{{\"traversal_nodes\":{},\"fetched_pages\":{},\"reused_pages\":{},\"reused_subtrees\":{},\"hydrate_fetched\":{},\"hydrate_resumed\":{},\"files\":{},\"consistent\":{}}}",
        meters.traversal_nodes,
        meters.fetched_pages,
        meters.reused_pages,
        meters.reused_subtrees,
        report.fetched,
        report.resumed,
        manifest.len(),
        consistent
    );
    if !consistent {
        std::process::exit(1);
    }
    Ok(())
}
