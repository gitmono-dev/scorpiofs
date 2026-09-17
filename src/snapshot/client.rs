//! MST/2 snapshot HTTP client (spec 04).
//!
//! Minimal client for the fixed-view read surface implemented on the server:
//! `POST /api/v2/snapshots/resolve`, `GET .../directory`, `POST .../lookup`,
//! `GET .../blob`. Content is always verified against the digest the fixed
//! view advertises; a mismatch is an error, never silent corruption
//! (spec 00 SYS-04).
//!
//! Every request is idempotent, so transport-level failures are retried
//! with jittered backoff (spec 04 §10). Only connection/timeout errors and
//! retryable statuses (429/5xx) are re-attempted; a typed server error is
//! definitive and returned as-is.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use reqwest::StatusCode;
use serde::Deserialize;

#[allow(unused_imports)]
use crate::snapshot::types::Descriptor;
use crate::snapshot::types::{
    Capabilities, DirectoryResponse, LookupResponse, ResolveResponse, SnapshotError,
    SnapshotErrorCode,
};

/// Client for one deployment's MST/2 surface. Cheap to clone (shares the
/// underlying connection pool).
#[derive(Clone)]
pub struct Mst2Client {
    http: Arc<reqwest::Client>,
    base: String,
    /// Transport-level retries performed (metrics, spec 13 §6).
    retries: Arc<AtomicU64>,
    /// Payload bytes received (frame/blob bodies), for the "transfer is
    /// proportional to the requested range" property of range reads.
    recv_bytes: Arc<AtomicU64>,
    /// Content units (OBJECT entries and CHUNK chunks) actually fetched.
    /// Wire bytes cannot prove a range read stayed narrow when a frame is
    /// compressed, but the unit count can.
    units_fetched: Arc<AtomicU64>,
}

/// Retry policy for idempotent reads: bounded attempts, exponential
/// backoff with jitter so a fleet of clients does not resynchronise.
const MAX_ATTEMPTS: u32 = 4;
const BASE_BACKOFF_MS: u64 = 40;

impl Mst2Client {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            http: Arc::new(reqwest::Client::new()),
            base: base_url.into().trim_end_matches('/').to_string(),
            retries: Arc::new(AtomicU64::new(0)),
            recv_bytes: Arc::new(AtomicU64::new(0)),
            units_fetched: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Content units (objects/chunks) this client has fetched so far.
    pub fn units_fetched(&self) -> u64 {
        self.units_fetched.load(Ordering::Relaxed)
    }

    /// Record `n` fetched content units (called by the frame consumers).
    pub(crate) fn count_units(&self, n: u64) {
        self.units_fetched.fetch_add(n, Ordering::Relaxed);
    }

    /// Payload bytes this client has received so far.
    pub fn received_bytes(&self) -> u64 {
        self.recv_bytes.load(Ordering::Relaxed)
    }

    /// How many transport retries this client has performed.
    pub fn retry_count(&self) -> u64 {
        self.retries.load(Ordering::Relaxed)
    }

    /// Issue one logical request with bounded retries. The request must be
    /// replayable (our bodies are JSON/bytes), which `try_clone` proves.
    async fn send_retrying(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, SnapshotError> {
        let req = builder.build().map_err(|e| {
            SnapshotError::new(SnapshotErrorCode::Internal, format!("request build: {e}"))
        })?;
        let mut req = Some(req);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let this = if attempt < MAX_ATTEMPTS {
                req.as_ref().and_then(|r| r.try_clone())
            } else {
                req.take()
            };
            let Some(this) = this else {
                // Body was a one-shot stream; nothing safe to retry with.
                return Err(SnapshotError::new(
                    SnapshotErrorCode::Internal,
                    "request body is not replayable",
                ));
            };
            match self.http.execute(this).await {
                Ok(resp) if attempt < MAX_ATTEMPTS && retryable_status(resp.status()) => {
                    self.retries.fetch_add(1, Ordering::Relaxed);
                    sleep_backoff(attempt).await;
                }
                Ok(resp) => return Ok(resp),
                Err(e) if attempt < MAX_ATTEMPTS && retryable_transport(&e) => {
                    self.retries.fetch_add(1, Ordering::Relaxed);
                    sleep_backoff(attempt).await;
                }
                Err(e) => return Err(net_err(e)),
            }
        }
    }

    fn snapshots_url(&self, tail: &str) -> String {
        format!("{}/api/v2/snapshots{tail}", self.base)
    }

    /// Capability advertisement; clients gate features on this (spec 04 §3).
    pub async fn capabilities(&self) -> Result<Capabilities, SnapshotError> {
        let resp = self
            .send_retrying(self.http.get(self.snapshots_url("/capabilities")))
            .await?;
        ok_or_error(resp).await?.json().await.map_err(de_err)
    }

    /// Fix a view on `target` for `scope`; returns the descriptor + lease.
    pub async fn resolve(
        &self,
        scope: &str,
        lease_seconds: u64,
    ) -> Result<ResolveResponse, SnapshotError> {
        let body = serde_json::json!({
            "target": {"kind": "latest"},
            "scope": scope,
            "delivery": "full",
            "lease_seconds": lease_seconds,
            "supported_metadata_codecs": [1],
        });
        let resp = self
            .send_retrying(self.http.post(self.snapshots_url("/resolve")).json(&body))
            .await?;
        ok_or_error(resp).await?.json().await.map_err(de_err)
    }

    /// One directory page; `cursor` continues pagination (spec 04 §5).
    pub async fn directory(
        &self,
        snapshot_id: &str,
        path: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<DirectoryResponse, SnapshotError> {
        let mut url = self.snapshots_url(&format!(
            "/{snapshot_id}/directory?path={}&limit={limit}",
            urlencode(path)
        ));
        if let Some(c) = cursor {
            url.push_str("&cursor=");
            url.push_str(&urlencode(c));
        }
        let resp = self.send_retrying(self.http.get(url)).await?;
        ok_or_error(resp).await?.json().await.map_err(de_err)
    }

    /// Batch path resolution (spec 04 §7).
    pub async fn lookup(
        &self,
        snapshot_id: &str,
        paths: &[String],
    ) -> Result<LookupResponse, SnapshotError> {
        let body = serde_json::json!({"paths": paths});
        let resp = self
            .send_retrying(
                self.http
                    .post(self.snapshots_url(&format!("/{snapshot_id}/lookup")))
                    .json(&body),
            )
            .await?;
        ok_or_error(resp).await?.json().await.map_err(de_err)
    }

    /// Fetch a whole file, verifying SHA-256 against `expected_digest`.
    ///
    /// The expected digest comes from the fixed view (directory/lookup), so
    /// this is content verification rather than trust in transport.
    pub async fn blob_verified(
        &self,
        snapshot_id: &str,
        path: &str,
        expected_digest: &str,
    ) -> Result<Vec<u8>, SnapshotError> {
        let url = self.snapshots_url(&format!(
            "/{snapshot_id}/blob?path={}&expected_digest={}",
            urlencode(path),
            urlencode(expected_digest)
        ));
        let resp = self.send_retrying(self.http.get(url)).await?;
        let resp = ok_or_error(resp).await?;
        let bytes = resp.bytes().await.map_err(net_err)?;
        self.recv_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        // Defense in depth: verify locally even though the server enforces
        // expected_digest too. ring is already a dependency.
        use ring::digest::{Context, SHA256};
        let mut cx = Context::new(&SHA256);
        cx.update(&bytes);
        let got = format!("sha256:{}", hex_lower(cx.finish().as_ref()));
        if got != expected_digest {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!("blob {path}: expected {expected_digest}, got {got}"),
            ));
        }
        Ok(bytes.to_vec())
    }
}

#[derive(Deserialize)]
struct ErrorEnvelope {
    error: ServerError,
}

#[derive(Deserialize)]
struct ServerError {
    code: String,
    message: String,
}

async fn ok_or_error(resp: reqwest::Response) -> Result<reqwest::Response, SnapshotError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    // Map the typed server envelope; fall back to HTTP status otherwise.
    if let Ok(env) = resp.json::<ErrorEnvelope>().await {
        return Err(SnapshotError {
            code: SnapshotErrorCode::from_server(&env.error.code),
            message: env.error.message,
            http_status: status.as_u16(),
        });
    }
    Err(SnapshotError::new(
        SnapshotErrorCode::Internal,
        format!("HTTP {status} without error envelope"),
    ))
}

/// Statuses worth another attempt: throttling and transient server faults.
/// A 4xx typed error is definitive and never retried.
fn retryable_status(status: StatusCode) -> bool {
    matches!(
        status.as_u16(),
        429 | 500 | 502 | 503 | 504
    )
}

/// Transport failures that a retry can plausibly fix. A malformed-URL or
/// body-encoding error is not retryable.
fn retryable_transport(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect() || e.is_request()
}

/// Exponential backoff with jitter derived from the clock, so retries from
/// many clients do not line up.
async fn sleep_backoff(attempt: u32) {
    let base = BASE_BACKOFF_MS << (attempt.saturating_sub(1)).min(4);
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| (d.subsec_nanos() as u64) % (base + 1))
        .unwrap_or(0);
    tokio::time::sleep(Duration::from_millis(base + jitter)).await;
}

fn net_err(e: reqwest::Error) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::Internal, format!("network: {e}"))
}
fn de_err(e: reqwest::Error) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::Internal, format!("decode: {e}"))
}

fn hex_lower(b: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push(H[(x >> 4) as usize] as char);
        s.push(H[(x & 0xf) as usize] as char);
    }
    s
}

fn urlencode(s: &str) -> String {
    // Minimal RFC 3986 percent-encoding sufficient for snapshot paths
    // (printable ASCII; reserve the query/control set).
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[allow(dead_code)]
fn _statuscode_marker(_: StatusCode) {}

// ---------------------------------------------------------------------------
// Generic verbs for the frame/navigation module (frames.rs). Kept here so the
// HTTP client owns every transport-level decision; frames.rs owns verification.
// ---------------------------------------------------------------------------

impl Mst2Client {
    pub(crate) fn base(&self) -> &str {
        &self.base
    }

    pub(crate) async fn get_json(
        &self,
        url: impl AsRef<str>,
    ) -> Result<serde_json::Value, SnapshotError> {
        let url = url.as_ref();
        ok_or_error(self.send_retrying(self.http.get(url)).await?)
            .await?
            .json()
            .await
            .map_err(de_err)
    }

    pub(crate) async fn post_json(
        &self,
        url: impl AsRef<str>,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, SnapshotError> {
        let url = url.as_ref();
        ok_or_error(self.send_retrying(self.http.post(url).json(&body)).await?)
            .await?
            .json()
            .await
            .map_err(de_err)
    }

    pub(crate) async fn delete_json(
        &self,
        url: impl AsRef<str>,
    ) -> Result<serde_json::Value, SnapshotError> {
        let url = url.as_ref();
        ok_or_error(self.send_retrying(self.http.delete(url)).await?)
            .await?
            .json()
            .await
            .map_err(de_err)
    }

    pub(crate) async fn post_octets(
        &self,
        url: impl AsRef<str>,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, SnapshotError> {
        let url = url.as_ref();
        let resp = ok_or_error(
            self.send_retrying(
                self.http
                    .post(url)
                    .header("content-type", "application/json")
                    .body(body),
            )
            .await?,
        )
        .await?;
        let bytes = resp.bytes().await.map_err(de_err)?;
        self.recv_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes.to_vec())
    }

    pub(crate) async fn head_blob(
        &self,
        url: impl AsRef<str>,
    ) -> Result<(u64, String, String), SnapshotError> {
        let url = url.as_ref();
        let resp = ok_or_error(self.send_retrying(self.http.head(url)).await?).await?;
        let headers = resp.headers();
        let len = headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| {
                SnapshotError::new(SnapshotErrorCode::Internal, "HEAD missing length")
            })?;
        let etag = headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let kind = headers
            .get("x-mega-fs-kind")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        Ok((len, etag, kind))
    }
}
