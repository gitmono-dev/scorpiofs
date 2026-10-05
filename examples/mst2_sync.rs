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
//! The complete root proof has separate meters from incremental acquisition;
//! the independent diagnostic walk below also runs outside those meters.
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
    let context = reader.authorized_context();
    let cache_path = context.scope_cache_dir(std::path::Path::new(&cache_dir));
    context.bind_scope_cache(&cache_path)?;
    let cache = ScopeCache::open(&cache_path)?;
    let mut sync = IncrementalSync::new(&reader, &cache);
    let closure = sync.sync_snapshot().await?;
    let manifest = closure.files();
    let meters = sync.meters();
    let proof = sync.closure_meters();

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
    let a = index(manifest);
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
    let view_dir = cache_path.join(snapshot_id.trim_start_matches("sha256:"));
    let store = Arc::new(DurableStore::open_for_reader(
        &view_dir,
        cache_path.join("blobs"),
        &reader,
    )?);
    let view = ViewMeta {
        snapshot_id: snapshot_id.clone(),
        namespace_view_id: reader.descriptor().namespace_view_id.clone(),
        scope: reader.descriptor().scope.clone(),
        lease_id: reader.lease_id().to_string(),
    };
    // Hydrate from the proved incremental closure, without another metadata
    // RPC in the hydration path. Small files ride OBJECT batches; large
    // files use chunk frames. Raw transport retains the same concurrency.
    let concurrency = std::env::var("M2_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4usize);
    let use_frames =
        reader.capabilities().features.objects && reader.capabilities().features.chunk_reads;
    let report = if use_frames {
        let reader_batch = reader.clone();
        store
            .hydrate_snapshot_content_batches(
                &reader,
                &closure,
                concurrency,
                concurrency,
                move |batch| {
                    let reader = reader_batch.clone();
                    Box::pin(async move { reader.read_content_batch(&batch).await })
                },
                {
                    let reader_large = reader.clone();
                    move |file| {
                        let reader = reader_large.clone();
                        Box::pin(async move { reader.read_content(&file, true).await })
                    }
                },
            )
            .await?
    } else {
        let coordinator = scorpiofs::snapshot::FetchCoordinator::with_verified_closure(
            reader.clone(),
            &closure,
            concurrency,
        )?;
        store
            .hydrate_snapshot_concurrent(&reader, &closure, concurrency, move |f| {
                let coordinator = coordinator.clone();
                Box::pin(async move { coordinator.fetch(f, use_frames).await })
            })
            .await?
    };
    store.pin(&view)?;
    if !store.is_snapshot_complete()? {
        return Err("hydration finished without a full snapshot marker".into());
    }

    println!(
        "{}",
        serde_json::json!({
            "traversal_nodes": meters.traversal_nodes,
            "fetched_pages": meters.fetched_pages,
            "reused_pages": meters.reused_pages,
            "reused_subtrees": meters.reused_subtrees,
            "closure_index_reads": meters.closure_index_reads,
            "closure_index_writes": meters.closure_index_writes,
            "pin_set_reads": meters.pin_set_reads,
            "page_rehashes": meters.page_rehashes,
            "unique_page_rehashes": meters.unique_page_rehashes,
            "page_rehash_bytes": meters.page_rehash_bytes,
            "closure_proof": proof,
            "hydrate_fetched": report.fetched,
            "hydrate_resumed": report.resumed,
            "hydrate_repaired": report.repaired,
            "hydrate_bytes": report.bytes_total,
            "files": manifest.len(),
            "directories": closure.directories().len(),
            "metadata_pages": closure.pages().len(),
            "completion_kind": report.completion_kind,
            "full_snapshot_complete": true,
            "consistent": consistent
        })
    );
    if !consistent {
        std::process::exit(1);
    }
    Ok(())
}
