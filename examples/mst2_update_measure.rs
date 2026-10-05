//! One measured real-service sync, driven by commit_update_bench.py.
//! The independent expected manifest comes from Git, outside measured work.
//! Local completion checks establish integrity, not offline authorization.

#![recursion_limit = "256"]

use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Instant};

use scorpiofs::snapshot::{
    DurableStore, FetchCoordinator, IncrementalSync, Mst2Client, ScopeCache, SnapshotFile,
    SnapshotReader, ViewMeta,
};

#[derive(serde::Deserialize)]
struct Expected {
    files: Vec<SnapshotFile>,
    directories: Vec<String>,
}

fn same_directories(
    actual: &[scorpiofs::snapshot::SnapshotDirectory],
    expected: &[String],
) -> bool {
    let mut actual: Vec<_> = actual.iter().map(|d| d.rel_path.as_str()).collect();
    let mut expected: Vec<_> = expected.iter().map(String::as_str).collect();
    actual.sort_unstable();
    expected.sort_unstable();
    actual == expected
}

fn index(files: &[SnapshotFile]) -> BTreeMap<String, (String, u64, String)> {
    files
        .iter()
        .map(|file| {
            (
                file.rel_path.clone(),
                (file.fs_kind.clone(), file.size, file.content_digest.clone()),
            )
        })
        .collect()
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().ok_or("sync or audit required")?;
    let expected_path = PathBuf::from(args.next().ok_or("expected Git manifest required")?);
    let expected: Expected = serde_json::from_slice(&std::fs::read(expected_path)?)?;
    if index(&expected.files).len() != expected.files.len() {
        return Err("duplicate paths in independent Git manifest".into());
    }
    if mode == "audit" {
        let root = PathBuf::from(args.next().ok_or("old store required")?);
        if args.next().is_some() {
            return Err("audit accepts exactly one expected manifest and store".into());
        }
        let store = DurableStore::open(root)?;
        let closure = store.snapshot_manifest()?;
        if !store.is_snapshot_complete()?
            || index(closure.files()) != index(&expected.files)
            || !same_directories(closure.directories(), &expected.directories)
        {
            return Err("old fixed view no longer matches its complete Git oracle".into());
        }
        println!("{}", serde_json::json!({"old_view_integrity": "PASS"}));
        return Ok(());
    }
    if mode != "sync" || args.next().is_some() {
        return Err("usage: mst2_update_measure sync EXPECTED | audit EXPECTED STORE".into());
    }

    let base = std::env::var("M2_BASE")?;
    let scope = std::env::var("M2_SCOPE")?;
    let storage = PathBuf::from(std::env::var("M2_STORE_ROOT")?);
    let expected_view = std::env::var("M2_EXPECTED_VIEW")?;
    let expected_instance = std::env::var("M2_EXPECTED_INSTANCE")?;
    let client = Mst2Client::with_token(base, std::env::var("M2_TOKEN").ok());
    let started = Instant::now();
    let reader = SnapshotReader::resolve(client, &scope, 3600).await?;
    let resolve_ms = elapsed_ms(started);
    if reader.descriptor().namespace_view_id != expected_view
        || reader.descriptor().instance_id != expected_instance
        || reader.descriptor().scope != scope
    {
        return Err("stale or wrong resolved snapshot after the publication fence".into());
    }
    let cache_start = Instant::now();
    let context = reader.authorized_context();
    if let Ok(expected_sequence) = std::env::var("M2_EXPECTED_SEQUENCE") {
        if context.publication_sequence() != expected_sequence.parse::<u64>()? {
            return Err("resolved publication sequence differs from the native head fence".into());
        }
    }
    let scope_dir = context.scope_cache_dir(&storage);
    context.bind_scope_cache(&scope_dir)?;
    let cache = ScopeCache::open(&scope_dir)?;
    let view_dir = context.view_cache_dir(&storage)?;
    let store = Arc::new(DurableStore::open_for_reader(
        &view_dir,
        scope_dir.join("blobs"),
        &reader,
    )?);
    let cache_setup_ms = elapsed_ms(cache_start);
    let metadata_start = Instant::now();
    let before_metadata_bytes = reader.client().received_bytes();
    let mut sync = IncrementalSync::new(&reader, &cache);
    let closure = sync.sync_snapshot().await?;
    let metadata_ms = elapsed_ms(metadata_start);
    let metadata_ready_ms = elapsed_ms(started);
    let metadata_bytes = reader.client().received_bytes() - before_metadata_bytes;
    let acquisition = sync.meters();
    let proof = sync.closure_meters();
    // Keep the independent oracle outside each reported timing segment, but
    // account for it explicitly in overall wall time.
    let oracle_start = Instant::now();
    if index(closure.files()) != index(&expected.files)
        || closure.files().len() != expected.files.len()
        || !same_directories(closure.directories(), &expected.directories)
    {
        return Err("incremental closure differs from the independent fixed Git tree".into());
    }
    let metadata_oracle_ms = elapsed_ms(oracle_start);
    let content_start = Instant::now();
    let before_content_bytes = reader.client().received_bytes();
    let frames =
        reader.capabilities().features.objects && reader.capabilities().features.chunk_reads;
    let client = reader.client().clone();
    let encoding = reader.encoding_hint().map(str::to_string);
    let sid = reader.snapshot_id().to_owned();
    let report = if frames {
        store
            .hydrate_snapshot_batches(
                &reader,
                &closure,
                4,
                4,
                move |batch| {
                    let client = client.clone();
                    let sid = sid.clone();
                    let encoding = encoding.clone();
                    Box::pin(async move {
                        let items: Vec<_> = batch
                            .iter()
                            .map(|f| (format!("/{}", f.rel_path), f.content_digest.clone()))
                            .collect();
                        let got = client.objects(&sid, &items, encoding.as_deref()).await?;
                        let mut out = std::collections::HashMap::new();
                        for f in &batch {
                            let want =
                                scorpiofs::snapshot::frames::parse_digest(&f.content_digest)?;
                            let data = got.get(&want).ok_or_else(|| {
                                scorpiofs::snapshot::SnapshotError::new(
                                    scorpiofs::snapshot::SnapshotErrorCode::DigestMismatch,
                                    "object batch omitted an expected Git content digest",
                                )
                            })?;
                            out.insert(f.content_digest.clone(), Arc::new(data.clone()));
                        }
                        Ok(out)
                    })
                },
                {
                    let reader = reader.clone();
                    move |file| {
                        let reader = reader.clone();
                        Box::pin(async move {
                            Ok(Arc::new(
                                reader
                                    .read_file_frames(
                                        &file.rel_path,
                                        &file.content_digest,
                                        file.size,
                                    )
                                    .await?,
                            ))
                        })
                    }
                },
            )
            .await?
    } else {
        let coordinator = FetchCoordinator::new(reader.clone(), 4);
        store
            .hydrate_snapshot_concurrent(&reader, &closure, 4, move |file| {
                let coordinator = coordinator.clone();
                Box::pin(async move { coordinator.fetch(file, false).await })
            })
            .await?
    };
    let hydration_ms = elapsed_ms(content_start);
    let content_bytes = reader.client().received_bytes() - before_content_bytes;
    let view = ViewMeta {
        snapshot_id: reader.snapshot_id().to_owned(),
        namespace_view_id: reader.descriptor().namespace_view_id.clone(),
        scope: scope.clone(),
        lease_id: reader.lease_id().to_owned(),
    };
    let audit_start = Instant::now();
    store.pin(&view)?;
    if !store.is_snapshot_complete()? {
        return Err("durable descriptor/page/content/pin completion audit failed".into());
    }
    let completion_audit_ms = elapsed_ms(audit_start);
    let durable_complete_ms = elapsed_ms(started);
    println!(
        "{}",
        serde_json::json!({
            "record": "mst2_real_update", "snapshot_id": reader.snapshot_id(),
            "driver_source_digest": scorpiofs::snapshot::durable::digest_of(include_bytes!("mst2_update_measure.rs")),
            "namespace_view_id": reader.descriptor().namespace_view_id,
            "publication_sequence": context.publication_sequence(),
            "store": view_dir, "resolve_ms": resolve_ms,
            "cache_setup_ms": cache_setup_ms, "metadata_ms": metadata_ms,
            "metadata_ready_ms": metadata_ready_ms, "metadata_bytes": metadata_bytes,
            "metadata_oracle_ms": metadata_oracle_ms,
            "hydration_ms": hydration_ms, "completion_audit_ms": completion_audit_ms,
            "durable_complete_ms": durable_complete_ms,
            "durable_complete_scope": "resolve through full hydration and completion audit, including the separately reported Git manifest comparison",
            "metadata_acquisition": {"traversal_nodes": acquisition.traversal_nodes,
                "fetched_pages": acquisition.fetched_pages, "reused_pages": acquisition.reused_pages,
                "reused_subtrees": acquisition.reused_subtrees,
                "closure_index_reads": acquisition.closure_index_reads,
                "closure_index_read_bytes": acquisition.closure_index_read_bytes,
                "closure_index_writes": acquisition.closure_index_writes,
                "closure_index_write_bytes": acquisition.closure_index_write_bytes,
                "pin_set_reads": acquisition.pin_set_reads,
                "page_rehashes": acquisition.page_rehashes,
                "unique_page_rehashes": acquisition.unique_page_rehashes,
                "page_rehash_bytes": acquisition.page_rehash_bytes},
            "full_closure_proof": proof, "content_bytes": content_bytes,
            "content_delivery": if frames {"object-batches-and-chunk-frames"} else {"raw-concurrent"},
            "fetched_content_units": report.fetched, "resumed_files": report.resumed,
            "logical_files": report.total_files, "logical_bytes": report.bytes_total,
            "full_snapshot_complete": true, "git_manifest_equal": true,
            "fuse_mount": "NOT_RUN", "offline_authorization": false
        })
    );
    Ok(())
}
