//! MST/2 snapshot HTTP client (spec 04).
//!
//! Minimal client for the fixed-view read surface implemented on the server:
//! `POST /api/v2/snapshots/resolve`, `GET .../directory`, `POST .../lookup`,
//! `GET .../blob`. Content is always verified against the digest the fixed
//! view advertises; a mismatch is an error, never silent corruption
//! (spec 00 SYS-04).

use std::sync::Arc;

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
}

impl Mst2Client {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            http: Arc::new(reqwest::Client::new()),
            base: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    fn snapshots_url(&self, tail: &str) -> String {
        format!("{}/api/v2/snapshots{tail}", self.base)
    }

    /// Capability advertisement; clients gate features on this (spec 04 §3).
    pub async fn capabilities(&self) -> Result<Capabilities, SnapshotError> {
        let resp = self
            .http
            .get(self.snapshots_url("/capabilities"))
            .send()
            .await
            .map_err(net_err)?;
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
            .http
            .post(self.snapshots_url("/resolve"))
            .json(&body)
            .send()
            .await
            .map_err(net_err)?;
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
        let resp = self.http.get(url).send().await.map_err(net_err)?;
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
            .http
            .post(self.snapshots_url(&format!("/{snapshot_id}/lookup")))
            .json(&body)
            .send()
            .await
            .map_err(net_err)?;
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
        let resp = self.http.get(url).send().await.map_err(net_err)?;
        let resp = ok_or_error(resp).await?;
        let bytes = resp.bytes().await.map_err(net_err)?;
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
        ok_or_error(self.http.get(url).send().await.map_err(net_err)?)
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
        ok_or_error(
            self.http
                .post(url)
                .json(&body)
                .send()
                .await
                .map_err(net_err)?,
        )
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
        ok_or_error(self.http.delete(url).send().await.map_err(net_err)?)
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
            self.http
                .post(url)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .map_err(net_err)?,
        )
        .await?;
        let bytes = resp.bytes().await.map_err(de_err)?;
        Ok(bytes.to_vec())
    }

    pub(crate) async fn head_blob(
        &self,
        url: impl AsRef<str>,
    ) -> Result<(u64, String, String), SnapshotError> {
        let url = url.as_ref();
        let resp = ok_or_error(self.http.head(url).send().await.map_err(net_err)?).await?;
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
