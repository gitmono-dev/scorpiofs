//! Live-stack e2e for transport retries (link Phase A-3).
//!
//! Point `MST2_BASE_URL` at the fault-injecting proxy (see
//! `mst2-impl/tmp/fault-proxy.py`) and the client must ride out injected
//! 503s / dropped connections on its bounded retry path, completing the
//! hydrate while reporting the retries it performed.
//!
//! Ignored by default; requires the running stack *and* the proxy.

use std::sync::Arc;

use scorpiofs::snapshot::{DurableStore, Mst2Client, SnapshotReader};

#[tokio::test]
#[ignore]
async fn hydrate_survives_injected_transient_failures() {
    let base = std::env::var("MST2_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:19701".into());
    let client = Mst2Client::new(base);

    let reader = SnapshotReader::resolve(client.clone(), "/project", 600)
        .await
        .expect("resolve through the fault-injecting proxy");

    let tmp = tempfile::TempDir::new().unwrap();
    let store = Arc::new(DurableStore::open(tmp.path()).unwrap());
    let report = store
        .hydrate(&reader)
        .await
        .expect("hydrate despite injected failures");
    assert!(report.complete && report.total_files > 0);
    assert!(store.is_complete().unwrap());

    // Every file is still verified against the view's digests.
    let manifest = store.manifest().unwrap();
    assert_eq!(store.verify_all(&manifest).unwrap(), manifest.len() as u64);

    // The retries were real: the proxy injected failures and the client
    // absorbed them rather than surfacing an error.
    assert!(
        client.retry_count() > 0,
        "expected injected failures to have been retried"
    );
}
