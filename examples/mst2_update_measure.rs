//! One measured real-service sync, driven by commit_update_bench.py.
//! The independent expected manifest comes from Git, outside measured work.
//! Local completion checks establish integrity, not offline authorization.

#![recursion_limit = "256"]

use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Instant};

use scorpiofs::snapshot::{
    DurableStore, FetchCoordinator, IncrementalSync, Mst2Client, ScopeCache, SnapshotFile,
    SnapshotReader, ViewMeta,
};

#[derive(serde::Deserialize, serde::Serialize)]
struct Expected {
    files: Vec<SnapshotFile>,
    directories: Vec<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AuditAuthority {
    revision: u8,
    domain: String,
    scope: String,
    snapshot_id: Option<String>,
}

fn audit_old_view(
    expected: &Expected,
    root: &std::path::Path,
    content: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check the explicit shared CAS binding before opening can recover markers.
    let view: AuditAuthority =
        serde_json::from_slice(&std::fs::read(root.join("authority.json"))?)?;
    let cas: AuditAuthority =
        serde_json::from_slice(&std::fs::read(content.join("authority.json"))?)?;
    let snapshot = view
        .snapshot_id
        .as_deref()
        .ok_or("view binding missing snapshot")?;
    if view.revision != 1
        || cas.revision != 1
        || view.domain.len() != 64
        || !view
            .domain
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        || view.domain != cas.domain
        || view.scope != cas.scope
        || cas.snapshot_id.is_some()
        || !snapshot.starts_with("sha256:")
        || snapshot.len() != 71
        || !snapshot[7..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err("old view and shared content authority differ".into());
    }
    mst2_codec::descriptor::validate_scope(&view.scope)?;
    let store = DurableStore::open_with_content(root, content)?;
    let closure = store.snapshot_manifest()?;
    if !store.is_snapshot_complete()?
        || closure.snapshot_id() != snapshot
        || closure.descriptor().scope != view.scope
        || index(closure.files()) != index(&expected.files)
        || !same_directories(closure.directories(), &expected.directories)
    {
        return Err("old fixed view no longer matches its complete Git oracle".into());
    }
    Ok(())
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
async fn main() -> std::process::ExitCode {
    let mut stage = "arguments";
    match measure(&mut stage).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            // Stages are fixed harness labels. Error messages and environment
            // values are private; only the actual typed client code is public.
            let mut failure = serde_json::json!({
                "record": "measurement_failure", "stage": stage,
            });
            if let Some(snapshot) = error.downcast_ref::<scorpiofs::snapshot::SnapshotError>() {
                failure["snapshot_error_code"] = serde_json::json!(format!("{:?}", snapshot.code));
            }
            eprintln!("{failure}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn measure(stage: &mut &'static str) -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().ok_or("sync or audit required")?;
    let expected_path = PathBuf::from(args.next().ok_or("expected Git manifest required")?);
    let expected: Expected = serde_json::from_slice(&std::fs::read(expected_path)?)?;
    if index(&expected.files).len() != expected.files.len() {
        return Err("duplicate paths in independent Git manifest".into());
    }
    if mode == "audit" {
        *stage = "old_complete_view_audit";
        let root = PathBuf::from(args.next().ok_or("old store required")?);
        let content = PathBuf::from(args.next().ok_or("old shared content store required")?);
        if args.next().is_some() {
            return Err(
                "audit accepts exactly one expected manifest, view store and content store".into(),
            );
        }
        audit_old_view(&expected, &root, &content)?;
        println!("{}", serde_json::json!({"old_view_integrity": "PASS"}));
        return Ok(());
    }
    if mode != "sync" || args.next().is_some() {
        return Err(
            "usage: mst2_update_measure sync EXPECTED | audit EXPECTED STORE CONTENT".into(),
        );
    }

    let base = std::env::var("M2_BASE")?;
    let scope = std::env::var("M2_SCOPE")?;
    let storage = PathBuf::from(std::env::var("M2_STORE_ROOT")?);
    let expected_view = std::env::var("M2_EXPECTED_VIEW")?;
    let expected_instance = std::env::var("M2_EXPECTED_INSTANCE")?;
    let client = Mst2Client::with_token(base, std::env::var("M2_TOKEN").ok());
    *stage = "resolve";
    let started = Instant::now();
    let reader = SnapshotReader::resolve(client, &scope, 3600).await?;
    let resolve_ms = elapsed_ms(started);
    if reader.descriptor().namespace_view_id != expected_view
        || reader.descriptor().instance_id != expected_instance
        || reader.descriptor().scope != scope
    {
        return Err("stale or wrong resolved snapshot after the publication fence".into());
    }
    *stage = "cache_setup";
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
    *stage = "metadata";
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
    *stage = "metadata_oracle";
    let oracle_start = Instant::now();
    if index(closure.files()) != index(&expected.files)
        || closure.files().len() != expected.files.len()
        || !same_directories(closure.directories(), &expected.directories)
    {
        return Err("incremental closure differs from the independent fixed Git tree".into());
    }
    let metadata_oracle_ms = elapsed_ms(oracle_start);
    *stage = "hydrate";
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
    *stage = "completion_audit";
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
            "store": view_dir, "content_store": store.content_dir(), "resolve_ms": resolve_ms,
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

#[cfg(all(test, unix))]
mod tests {
    use std::{path::Path, process::Command};

    use mst2_codec::{
        descriptor::ServingDescriptor,
        metapage::{page_id, Entry, EntryKind, Page},
    };
    use scorpiofs::snapshot::{durable::digest_of, ValidatedSnapshotClosure};

    use super::*;

    async fn fixture(root: &Path, content: &Path, version: u8, body: &[u8]) -> Expected {
        let digest = digest_of(body);
        let raw = Page::Leaf {
            entries: vec![Entry::file(
                EntryKind::Regular,
                b"old.txt",
                body.len() as u64,
                scorpiofs::snapshot::frames::parse_digest(&digest).unwrap(),
            )],
        }
        .encode()
        .unwrap();
        let pid = page_id(&raw);
        let descriptor = ServingDescriptor {
            instance_uuid: *uuid::Uuid::parse_str("11111111-2222-4333-8444-555555555555")
                .unwrap()
                .as_bytes(),
            namespace_view_id: [version; 32],
            scope: "/project".into(),
            metadata_root: pid,
        };
        let pages = BTreeMap::from([(format!("sha256:{}", hex::encode(pid)), raw)]);
        let closure =
            ValidatedSnapshotClosure::from_canonical_pages(&descriptor.encode().unwrap(), pages)
                .unwrap();
        let store = DurableStore::open_with_content(root, content).unwrap();
        let view = ViewMeta {
            snapshot_id: closure.snapshot_id().into(),
            namespace_view_id: closure.descriptor().namespace_view_id.clone(),
            scope: "/project".into(),
            lease_id: "local-integrity-fixture".into(),
        };
        store
            .hydrate_snapshot_with(&view, &closure, |_| std::future::ready(Ok(body.to_vec())))
            .await
            .unwrap();
        assert!(store.is_snapshot_complete().unwrap());
        for (dir, snapshot) in [(root, Some(closure.snapshot_id())), (content, None)] {
            std::fs::write(
                dir.join("authority.json"),
                serde_json::to_vec(&serde_json::json!({
                    "revision": 1, "domain": "a".repeat(64),
                    "scope": "/project", "snapshot_id": snapshot,
                }))
                .unwrap(),
            )
            .unwrap();
        }
        Expected {
            files: vec![SnapshotFile {
                rel_path: "old.txt".into(),
                fs_kind: "regular".into(),
                size: body.len() as u64,
                content_digest: digest,
            }],
            directories: vec![String::new()],
        }
    }

    #[tokio::test]
    async fn real_audit_process_preserves_old_shared_cas_and_rejects_wrong_or_corrupt_content() {
        let driver = std::env::var_os("MST2_MEASURE_DRIVER")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                std::env::current_exe()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .join(format!(
                        "mst2_update_measure{}",
                        std::env::consts::EXE_SUFFIX
                    ))
            });
        assert!(
            driver.is_file(),
            "build the driver or set MST2_MEASURE_DRIVER before example tests"
        );
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("old-view");
        let content = temp.path().join("scope-blobs");
        let original = b"original fixed snapshot bytes";
        let expected = fixture(&root, &content, 0x22, original).await;
        fixture(
            &temp.path().join("new-view"),
            &content,
            0x23,
            b"new commit bytes",
        )
        .await;
        let manifest = temp.path().join("old-expected.json");
        std::fs::write(&manifest, serde_json::to_vec(&expected).unwrap()).unwrap();
        let audit = |cas: &Path| {
            Command::new(&driver)
                .args(["audit"])
                .arg(&manifest)
                .arg(&root)
                .arg(cas)
                .output()
                .unwrap()
        };
        let good = audit(&content);
        assert!(good.status.success());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&good.stdout).unwrap(),
            serde_json::json!({"old_view_integrity": "PASS"})
        );
        let wrong = temp.path().join("other-domain");
        std::fs::create_dir(&wrong).unwrap();
        std::fs::write(
            wrong.join("authority.json"),
            serde_json::to_vec(&serde_json::json!({
                "revision": 1, "domain": "b".repeat(64),
                "scope": "/project", "snapshot_id": null,
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(!audit(&wrong).status.success());
        assert!(
            audit(&content).status.success(),
            "wrong CAS must not recover the real view"
        );
        std::fs::write(
            content.join(
                expected.files[0]
                    .content_digest
                    .strip_prefix("sha256:")
                    .unwrap(),
            ),
            vec![0u8; original.len()],
        )
        .unwrap();
        let corrupt = audit(&content);
        assert!(!corrupt.status.success());
        let failure: serde_json::Value = serde_json::from_slice(&corrupt.stderr).unwrap();
        assert_eq!(failure["record"], "measurement_failure");
        assert_eq!(failure["stage"], "old_complete_view_audit");
    }
}
