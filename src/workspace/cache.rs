//! Pressure work proves metadata roots without downloading their bodies.

use std::{path::PathBuf, sync::Arc, time::Duration};

use crate::snapshot::{
    cache_retention::{cache_pressure, collect_scope},
    stage::trace_blocking,
    CacheCollectionReport, CacheLimits, DurableStore, SnapshotError, SnapshotErrorCode,
    SnapshotReader,
};

pub(super) async fn pressure(scope: PathBuf) -> Result<bool, SnapshotError> {
    trace_blocking("cache_pressure", move || cache_pressure(&scope))
        .await
        .map_err(|_| worker_error())?
}

/// The caller holds the workspace runtime lock until the final blocking job
/// returns. Cancellation of metadata collection does not publish a root; a
/// promotion job itself is joined before native retirement can take that lock.
pub(super) async fn promote(
    store: Arc<DurableStore>,
    reader: &SnapshotReader,
    limits: CacheLimits,
) -> Result<(), SnapshotError> {
    reader.local_lease_status()?;
    store.bind_reader(reader)?;
    if store.cache_retention_known()? {
        return Ok(());
    }
    let closure = tokio::time::timeout(
        Duration::from_millis(limits.max_scan_millis),
        reader.snapshot_closure_for_cache(limits),
    )
    .await
    .map_err(|_| {
        SnapshotError::new(
            SnapshotErrorCode::LimitExceeded,
            "cache root promotion exceeded its metadata deadline",
        )
    })??;
    reader.local_lease_status()?;
    trace_blocking("cache_root_promote", move || {
        store.retain_snapshot_root(&closure)
    })
    .await
    .map_err(|_| worker_error())?
}

pub(super) async fn collect(scope: PathBuf) -> Result<CacheCollectionReport, SnapshotError> {
    // The collector owns the exclusive lifecycle fence inside this actual job.
    // A canceled waiter cannot release it while disk deletion is still active.
    trace_blocking("cache_collect", move || collect_scope(&scope))
        .await
        .map_err(|_| worker_error())?
}

fn worker_error() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::Internal,
        "cache maintenance worker failed",
    )
}
