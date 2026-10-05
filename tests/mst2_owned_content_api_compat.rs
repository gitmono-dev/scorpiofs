//! External-consumer source compatibility. These functions compile without executing I/O.
#![allow(dead_code, unreachable_code)]

use std::{collections::HashMap, sync::Arc};

use scorpiofs::snapshot::{
    durable::{DurableStore, ViewMeta},
    FetchCoordinator, FileMembershipError, ProvenSnapshotFile, SnapshotError, SnapshotErrorCode,
    SnapshotFile, SnapshotReader, ValidatedSnapshotClosure,
};

async fn legacy_fetch(
    coordinator: &Arc<FetchCoordinator>,
    file: SnapshotFile,
) -> Result<(), SnapshotError> {
    let result: Arc<Vec<u8>> = coordinator.fetch(file, false).await?;
    let _: &Vec<u8> = &result;
    let _: &[u8] = &result;
    let mut copied: Vec<u8> = result.as_ref().clone();
    copied.push(0);
    Ok(())
}

async fn selective_membership(reader: &SnapshotReader) -> Result<(), FileMembershipError> {
    let proven: Arc<ProvenSnapshotFile> = reader.prove_file("file").await?;
    let _ = reader.read_proven_content(&proven, true).await?;
    Ok(())
}

fn membership_error(error: &FileMembershipError) -> Option<SnapshotErrorCode> {
    match error {
        FileMembershipError::NotFile { message: _ } => None,
        FileMembershipError::Snapshot(error) => Some(error.code),
        _ => None,
    }
}

async fn legacy_hydration(
    store: &DurableStore,
    view: &ViewMeta,
    manifest: &[SnapshotFile],
    reader: &SnapshotReader,
    closure: &ValidatedSnapshotClosure,
) {
    let _ = store
        .hydrate_concurrent::<_>(view, manifest, 1, |_| {
            Box::pin(async { panic!("compile-only fetcher") })
        })
        .await;
    let _ = store
        .hydrate_concurrent::<_>(view, manifest, 1, |_| {
            Box::pin(async {
                Err(SnapshotError::new(
                    SnapshotErrorCode::InvalidRequest,
                    "compile-only error",
                ))
            })
        })
        .await;
    let _ = store
        .hydrate_concurrent::<_>(view, manifest, 1, |_| {
            Box::pin(async { Ok(Arc::new(Vec::new())) })
        })
        .await;
    let _ = store
        .hydrate_snapshot_concurrent::<_>(reader, closure, 1, |_| {
            Box::pin(async { panic!("compile-only fetcher") })
        })
        .await;
    let _ = store
        .hydrate_snapshot_concurrent::<_>(reader, closure, 1, |_| {
            Box::pin(async {
                Err(SnapshotError::new(
                    SnapshotErrorCode::InvalidRequest,
                    "compile-only error",
                ))
            })
        })
        .await;
    let _ = store
        .hydrate_snapshot_concurrent::<_>(reader, closure, 1, |_| {
            Box::pin(async { Ok(Arc::new(Vec::new())) })
        })
        .await;
    let _ = store
        .hydrate_batches::<_, _>(
            view,
            manifest,
            1,
            1,
            |_| Box::pin(async { panic!("compile-only batch") }),
            |_| Box::pin(async { panic!("compile-only large file") }),
        )
        .await;
    let _ = store
        .hydrate_batches::<_, _>(
            view,
            manifest,
            1,
            1,
            |_| {
                Box::pin(async {
                    Err(SnapshotError::new(
                        SnapshotErrorCode::InvalidRequest,
                        "compile-only error",
                    ))
                })
            },
            |_| {
                Box::pin(async {
                    Err(SnapshotError::new(
                        SnapshotErrorCode::InvalidRequest,
                        "compile-only error",
                    ))
                })
            },
        )
        .await;
    let _ = store
        .hydrate_batches::<_, _>(
            view,
            manifest,
            1,
            1,
            |_| Box::pin(async { Ok(HashMap::new()) }),
            |_| Box::pin(async { Ok(Arc::new(Vec::new())) }),
        )
        .await;
    let _ = store
        .hydrate_snapshot_batches::<_, _>(
            reader,
            closure,
            1,
            1,
            |_| Box::pin(async { panic!("compile-only batch") }),
            |_| Box::pin(async { panic!("compile-only large file") }),
        )
        .await;
    let _ = store
        .hydrate_snapshot_batches::<_, _>(
            reader,
            closure,
            1,
            1,
            |_| {
                Box::pin(async {
                    Err(SnapshotError::new(
                        SnapshotErrorCode::InvalidRequest,
                        "compile-only error",
                    ))
                })
            },
            |_| {
                Box::pin(async {
                    Err(SnapshotError::new(
                        SnapshotErrorCode::InvalidRequest,
                        "compile-only error",
                    ))
                })
            },
        )
        .await;
    let _ = store
        .hydrate_snapshot_batches::<_, _>(
            reader,
            closure,
            1,
            1,
            |_| Box::pin(async { Ok(HashMap::new()) }),
            |_| Box::pin(async { Ok(Arc::new(Vec::new())) }),
        )
        .await;
}

#[test]
fn legacy_public_api_compiles_without_running_hydration() {
    let _ = legacy_fetch;
    let _ = legacy_hydration;
    let _ = selective_membership;
    let _ = membership_error;
}
