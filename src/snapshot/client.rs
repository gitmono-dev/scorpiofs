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

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};

pub(crate) const TREEFRAME_REQUEST_MAX_BYTES: usize = 131_072;

use reqwest::StatusCode;
use serde::{de::DeserializeOwned, Deserialize};

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
    request_timeout: Duration,
    base: String,
    /// Bearer token for the snapshot surface (spec 04 §1). Sent as
    /// `Authorization` on every request; unset for lab-only deployments.
    token: Arc<StdMutex<Option<String>>>,
    /// The lease this client resolved, sent as `X-Mega-Snapshot-Lease` on
    /// snapshot-bound requests (spec 04 §1: identity in headers, not URLs).
    lease: Arc<StdMutex<Option<String>>>,
    /// An immutable credential snapshot used by a resolved reader. Pool and
    /// counters remain shared; configuration changes affect future resolves.
    bound_credentials: Option<Arc<BoundCredentials>>,
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

struct BoundCredentials {
    token: Option<String>,
    lease: Option<String>,
}

/// Retry policy for idempotent reads: bounded attempts, exponential
/// backoff with jitter so a fleet of clients does not resynchronise.
const MAX_ATTEMPTS: u32 = 4;
const BASE_BACKOFF_MS: u64 = 40;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_JSON_REQUEST_BYTES: usize = TREEFRAME_REQUEST_MAX_BYTES;
const MAX_JSON_RESPONSE_BYTES: usize = 1_048_576;

/// Local limit for APIs returning a whole file in memory. Larger files use
/// bounded range reads; this is independent of the protocol's file-size cap.
pub const MAX_BUFFERED_FILE_BYTES: u64 = 64 * 1024 * 1024;

impl Mst2Client {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_token(base_url, None)
    }

    /// Client with a bearer token for authenticated deployments (spec 04 §1).
    /// The token can also come from `M2_TOKEN`; it is never logged.
    pub fn with_token(base_url: impl Into<String>, token: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            // Spec 14 §3: refuse automatic redirects — a redirect must never
            // carry (or silently drop) credentials to another origin.
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client with sane defaults");
        Self {
            http: Arc::new(http),
            request_timeout: REQUEST_TIMEOUT,
            base: base_url.into().trim_end_matches('/').to_string(),
            token: Arc::new(StdMutex::new(token)),
            lease: Arc::new(StdMutex::new(None)),
            bound_credentials: None,
            retries: Arc::new(AtomicU64::new(0)),
            recv_bytes: Arc::new(AtomicU64::new(0)),
            units_fetched: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Configure the logical request deadline (retries, backoff and body included).
    /// The transport hard ceiling is 60 seconds; zero selects one millisecond.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout.clamp(Duration::from_millis(1), REQUEST_TIMEOUT);
        self
    }

    /// Set or clear the bearer credential for future requests/resolves.
    /// Resolved readers keep their immutable credential snapshot.
    pub fn set_token(&self, token: Option<String>) {
        *self.token.lock().unwrap() = token;
    }

    /// Bind the lease produced by [`Mst2Client::resolve`]; it rides every
    /// subsequent unbound request as `X-Mega-Snapshot-Lease`. Readers bind a
    /// private immutable lease instead, so this cannot change their identity.
    pub fn bind_lease(&self, lease_id: &str) {
        *self.lease.lock().unwrap() = Some(lease_id.to_string());
    }

    fn credentials(&self) -> (Option<String>, Option<String>) {
        match &self.bound_credentials {
            Some(bound) => (bound.token.clone(), bound.lease.clone()),
            None => (
                self.token.lock().unwrap().clone(),
                self.lease.lock().unwrap().clone(),
            ),
        }
    }

    /// Freeze the actor before starting resolve, and remove any previous
    /// lease. Rotation while resolve is in flight must not rebind its actor.
    pub(crate) fn for_resolve(&self) -> Self {
        let (token, _) = self.credentials();
        let mut client = self.clone();
        client.token = Arc::new(StdMutex::new(token.clone()));
        client.lease = Arc::new(StdMutex::new(None));
        client.bound_credentials = Some(Arc::new(BoundCredentials { token, lease: None }));
        client
    }

    /// Conservative cache partition for the frozen actor credential. Token
    /// rotation deliberately yields a new partition even if an issuer maps
    /// both credentials to the same actor. Never log the token itself.
    pub fn credential_partition(&self) -> String {
        use ring::digest::{Context, SHA256};
        let (token, _) = self.credentials();
        let mut digest = Context::new(&SHA256);
        digest.update(b"scorpio.mst2.actor-credential\0");
        match token {
            Some(token) => {
                digest.update(b"bearer\0");
                digest.update(token.as_bytes());
            }
            None => digest.update(b"unauthenticated-lab\0"),
        }
        hex::encode(digest.finish().as_ref())
    }

    pub(crate) fn with_snapshot_lease(&self, lease_id: &str) -> Self {
        let (token, _) = self.credentials();
        let mut client = self.clone();
        client.bound_credentials = Some(Arc::new(BoundCredentials {
            token,
            lease: Some(lease_id.to_string()),
        }));
        client
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
        // Spec 04 §1: identity travels in headers — bearer credential plus
        // the resolved lease — never in the URL.
        let builder = {
            let (token, lease) = self.credentials();
            let mut b = builder;
            if let Some(t) = token {
                b = b.header(reqwest::header::AUTHORIZATION, format!("Bearer {t}"));
            }
            if let Some(l) = lease {
                b = b.header("x-mega-snapshot-lease", l);
            }
            b
        };
        let req = builder.build().map_err(|e| {
            SnapshotError::new(SnapshotErrorCode::Internal, format!("request build: {e}"))
        })?;
        if req
            .body()
            .and_then(reqwest::Body::as_bytes)
            .is_some_and(|body| body.len() > MAX_JSON_REQUEST_BYTES)
        {
            return Err(json_limit("request"));
        }
        let deadline = tokio::time::Instant::now() + self.request_timeout;
        let mut req = Some(req);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let this = if attempt < MAX_ATTEMPTS {
                req.as_ref().and_then(|r| r.try_clone())
            } else {
                req.take()
            };
            let Some(mut this) = this else {
                // Body was a one-shot stream; nothing safe to retry with.
                return Err(SnapshotError::new(
                    SnapshotErrorCode::Internal,
                    "request body is not replayable",
                ));
            };
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(request_deadline());
            }
            // Reqwest retains this timer through response body consumption.
            // A retry receives only the original deadline's remaining budget.
            *this.timeout_mut() = Some(remaining);
            if attempt > 1 {
                self.retries.fetch_add(1, Ordering::Relaxed);
            }
            match self.http.execute(this).await {
                Ok(resp) if attempt < MAX_ATTEMPTS && retryable_status(resp.status()) => {
                    tokio::time::timeout_at(deadline, sleep_backoff(attempt))
                        .await
                        .map_err(|_| request_deadline())?;
                }
                Ok(resp) => return Ok(resp),
                Err(e) if attempt < MAX_ATTEMPTS && retryable_transport(&e) => {
                    tokio::time::timeout_at(deadline, sleep_backoff(attempt))
                        .await
                        .map_err(|_| request_deadline())?;
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
        read_json(ok_or_error(resp).await?).await
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
        read_json(ok_or_error(resp).await?).await
    }

    /// One directory page; `cursor` continues pagination (spec 04 §5).
    pub async fn directory(
        &self,
        snapshot_id: &str,
        path: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<DirectoryResponse, SnapshotError> {
        if !(1..=256).contains(&limit) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeInvalid,
                "directory limit must be 1..256",
            ));
        }
        let mut url = self.snapshots_url(&format!(
            "/{snapshot_id}/directory?path={}&limit={limit}",
            urlencode(path)
        ));
        if let Some(c) = cursor {
            url.push_str("&cursor=");
            url.push_str(&urlencode(c));
        }
        let resp = self.send_retrying(self.http.get(url)).await?;
        let page: DirectoryResponse = read_json(ok_or_error(resp).await?).await?;
        super::directory::validate_page(&page, snapshot_id, path, limit, cursor)?;
        Ok(page)
    }

    /// Batch path resolution (spec 04 §7).
    pub async fn lookup(
        &self,
        snapshot_id: &str,
        paths: &[String],
    ) -> Result<LookupResponse, SnapshotError> {
        if paths.len() > 128 {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "lookup accepts at most 128 paths",
            ));
        }
        let body = serde_json::json!({"paths": paths});
        let resp = self
            .send_retrying(
                self.http
                    .post(self.snapshots_url(&format!("/{snapshot_id}/lookup")))
                    .json(&body),
            )
            .await?;
        let response: LookupResponse = read_json(ok_or_error(resp).await?).await?;
        if response.snapshot_id != snapshot_id || response.results.len() != paths.len() {
            return Err(lookup_binding_error());
        }
        for (result, path) in response.results.iter().zip(paths) {
            if result.path != *path
                || match result.status.as_str() {
                    "found" => result.node.is_none(),
                    "absent" | "not_directory" | "symlink_traversal" => result.node.is_some(),
                    _ => true,
                }
            {
                return Err(lookup_binding_error());
            }
            if let Some(node) = &result.node {
                super::lookup::validate_node(node, path)?;
            }
        }
        Ok(response)
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
        let mut resp = ok_or_error(resp).await?;
        if resp
            .content_length()
            .is_some_and(|length| length > MAX_BUFFERED_FILE_BYTES)
        {
            return Err(buffered_limit());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = resp.chunk().await.map_err(net_err)? {
            self.recv_bytes
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
            if chunk.len() as u64 > MAX_BUFFERED_FILE_BYTES - bytes.len() as u64 {
                return Err(buffered_limit());
            }
            if chunk.len() > bytes.capacity() - bytes.len() {
                let target = (bytes.len() + chunk.len())
                    .max(bytes.capacity().saturating_mul(2))
                    .min(MAX_BUFFERED_FILE_BYTES as usize);
                bytes
                    .try_reserve_exact(target - bytes.len())
                    .map_err(|_| buffered_limit())?;
            }
            bytes.extend_from_slice(&chunk);
        }
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
        Ok(bytes)
    }
}

fn buffered_limit() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::LimitExceeded,
        "whole-file buffered read exceeds the local 64 MiB budget; use range reads",
    )
}

fn lookup_binding_error() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::IntegrityError,
        "lookup response does not bind every ordered result to its requested snapshot and path",
    )
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
    let bytes = read_json_bytes(resp).await.map_err(|mut error| {
        error.http_status = status.as_u16();
        error
    })?;
    Err(server_error(&bytes, status))
}

fn server_error(bytes: &[u8], status: StatusCode) -> SnapshotError {
    let Ok(env) = parse_json::<ErrorEnvelope>(bytes) else {
        return SnapshotError {
            code: SnapshotErrorCode::Internal,
            message: format!("HTTP {status} without valid error envelope"),
            http_status: status.as_u16(),
        };
    };
    SnapshotError {
        code: SnapshotErrorCode::from_server(&env.error.code),
        message: env.error.message,
        http_status: status.as_u16(),
    }
}

fn json_limit(direction: &str) -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::LimitExceeded,
        format!("JSON {direction} exceeds the protocol byte budget"),
    )
}

fn request_deadline() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::TemporaryUnavailable,
        "snapshot request deadline exceeded",
    )
}

async fn read_json<T: DeserializeOwned>(resp: reqwest::Response) -> Result<T, SnapshotError> {
    parse_json(&read_json_bytes(resp).await?)
}

async fn read_json_bytes(mut resp: reqwest::Response) -> Result<Vec<u8>, SnapshotError> {
    if resp
        .content_length()
        .is_some_and(|length| length > MAX_JSON_RESPONSE_BYTES as u64)
    {
        return Err(json_limit("response"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(net_err)? {
        if chunk.len() > MAX_JSON_RESPONSE_BYTES - bytes.len() {
            return Err(json_limit("response"));
        }
        if chunk.len() > bytes.capacity() - bytes.len() {
            let target = (bytes.len() + chunk.len())
                .max(bytes.capacity().saturating_mul(2))
                .min(MAX_JSON_RESPONSE_BYTES);
            bytes
                .try_reserve_exact(target - bytes.len())
                .map_err(|_| json_limit("response"))?;
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn parse_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, SnapshotError> {
    let invalid = |error| {
        SnapshotError::new(
            SnapshotErrorCode::IntegrityError,
            format!("JSON decode: {error}"),
        )
    };
    // Validate keys before DTO/Value deserialization can discard duplicates,
    // including duplicates nested in fields the current DTO does not consume.
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    UniqueJson::deserialize(&mut decoder).map_err(invalid)?;
    decoder.end().map_err(invalid)?;
    serde_json::from_slice(bytes).map_err(invalid)
}

struct UniqueJson;

impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueJson;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("JSON without duplicate object keys")
            }
            fn visit_bool<E>(self, _: bool) -> Result<UniqueJson, E> {
                Ok(UniqueJson)
            }
            fn visit_i64<E>(self, _: i64) -> Result<UniqueJson, E> {
                Ok(UniqueJson)
            }
            fn visit_u64<E>(self, _: u64) -> Result<UniqueJson, E> {
                Ok(UniqueJson)
            }
            fn visit_f64<E>(self, _: f64) -> Result<UniqueJson, E> {
                Ok(UniqueJson)
            }
            fn visit_str<E>(self, _: &str) -> Result<UniqueJson, E> {
                Ok(UniqueJson)
            }
            fn visit_unit<E>(self) -> Result<UniqueJson, E> {
                Ok(UniqueJson)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<UniqueJson, A::Error> {
                while sequence.next_element::<UniqueJson>()?.is_some() {}
                Ok(UniqueJson)
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<UniqueJson, A::Error> {
                let mut keys = HashSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !keys.insert(key) {
                        return Err(serde::de::Error::custom("duplicate JSON key"));
                    }
                    map.next_value::<UniqueJson>()?;
                }
                Ok(UniqueJson)
            }
        }
        decoder.deserialize_any(Visitor)
    }
}

/// Statuses worth another attempt: throttling and transient server faults.
/// A 4xx typed error is definitive and never retried.
fn retryable_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504)
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
    let code = if retryable_transport(&e) || e.is_body() {
        SnapshotErrorCode::TemporaryUnavailable
    } else {
        SnapshotErrorCode::Internal
    };
    SnapshotError::new(code, format!("network: {e}"))
}
fn de_err(e: reqwest::Error) -> SnapshotError {
    if !e.is_decode() && (retryable_transport(&e) || e.is_body()) {
        net_err(e)
    } else {
        SnapshotError::new(SnapshotErrorCode::IntegrityError, format!("decode: {e}"))
    }
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
        read_json(ok_or_error(self.send_retrying(self.http.get(url)).await?).await?).await
    }

    pub(crate) async fn post_json(
        &self,
        url: impl AsRef<str>,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, SnapshotError> {
        let url = url.as_ref();
        read_json(ok_or_error(self.send_retrying(self.http.post(url).json(&body)).await?).await?)
            .await
    }

    /// Status selects the release wire contract before decoding: canonical
    /// 204 has no JSON, while legacy 200 carries an identity-bound receipt.
    pub(crate) async fn delete_release_json(
        &self,
        url: impl AsRef<str>,
    ) -> Result<Option<serde_json::Value>, SnapshotError> {
        let url = url.as_ref();
        let mut response = ok_or_error(self.send_retrying(self.http.delete(url)).await?).await?;
        let status = response.status();
        let invalid = || SnapshotError {
            code: SnapshotErrorCode::IntegrityError,
            message: "lease release success has invalid HTTP status or body framing".into(),
            http_status: status.as_u16(),
        };
        match status {
            StatusCode::NO_CONTENT => {
                if response
                    .headers()
                    .contains_key(reqwest::header::TRANSFER_ENCODING)
                    || response
                        .headers()
                        .get_all(reqwest::header::CONTENT_LENGTH)
                        .iter()
                        .any(|value| {
                            value
                                .to_str()
                                .ok()
                                .and_then(|text| text.parse::<u64>().ok())
                                != Some(0)
                        })
                {
                    return Err(invalid());
                }
                if response.chunk().await.map_err(net_err)?.is_some() {
                    return Err(invalid());
                }
                Ok(None)
            }
            StatusCode::OK => read_json(response).await.map(Some),
            _ => Err(invalid()),
        }
    }

    /// POST a TreeFrame request and validate the protocol identity headers
    /// before exposing any frame bytes to the decoder (MST/2 spec 06 §1).
    /// The request digest covers the exact serialized bytes sent on the wire.
    pub(crate) async fn post_treeframe(
        &self,
        url: impl AsRef<str>,
        body: Vec<u8>,
        snapshot_id: &str,
        max_response_bytes: usize,
    ) -> Result<Vec<u8>, SnapshotError> {
        if body.len() > TREEFRAME_REQUEST_MAX_BYTES {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "TreeFrame request exceeds the JSON request byte limit",
            ));
        }
        let url = url.as_ref();
        let expected_request_digest = {
            use ring::digest::{Context, SHA256};
            let mut cx = Context::new(&SHA256);
            cx.update(&body);
            format!("sha256:{}", hex_lower(cx.finish().as_ref()))
        };
        let mut resp = self
            .send_retrying(
                self.http
                    .post(url)
                    .header("content-type", "application/json")
                    .body(body),
            )
            .await?;
        let status = resp.status();
        let max_response_bytes = if status.is_success() {
            validate_treeframe_headers(resp.headers(), snapshot_id, &expected_request_digest)?;
            max_response_bytes
        } else {
            // Typed HTTP errors are JSON, not TreeFrames, but their bodies
            // need the same bounded read before attempting envelope parsing.
            max_response_bytes.min(MAX_JSON_RESPONSE_BYTES)
        };
        if resp
            .content_length()
            .is_some_and(|length| length > max_response_bytes as u64)
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "TreeFrame response exceeds the request's byte budget",
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = resp.chunk().await.map_err(de_err)? {
            self.recv_bytes
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
            if chunk.len() > max_response_bytes.saturating_sub(bytes.len()) {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::LimitExceeded,
                    "TreeFrame response exceeds the request's byte budget",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            return Err(server_error(&bytes, status));
        }
        Ok(bytes)
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

fn validate_treeframe_headers(
    headers: &reqwest::header::HeaderMap,
    snapshot_id: &str,
    expected_request_digest: &str,
) -> Result<(), SnapshotError> {
    let content_type = one_treeframe_header(headers, "content-type", "Content-Type")?;
    let mut media_parts = content_type.split(';').map(str::trim);
    let media_type = media_parts.next().unwrap_or_default();
    let version = media_parts.next().unwrap_or_default();
    if !media_type.eq_ignore_ascii_case("application/vnd.mega.treeframe")
        || !version.split_once('=').is_some_and(|(key, value)| {
            key.trim().eq_ignore_ascii_case("version") && matches!(value.trim(), "2" | "\"2\"")
        })
        || media_parts.next().is_some()
    {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            format!("unexpected TreeFrame Content-Type: {content_type}"),
        ));
    }
    // TreeFrame bytes are already framed and authenticated by the codec.
    // Letting reqwest transparently decode an HTTP content encoding before
    // parsing would make the response representation ambiguous and could
    // turn a proxy transformation into an integrity failure much later.
    // Spec 06 therefore requires this header to be absent.
    if headers.contains_key(reqwest::header::CONTENT_ENCODING) {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            "TreeFrame response must not use Content-Encoding",
        ));
    }
    let returned_snapshot =
        one_treeframe_header(headers, "x-mega-snapshot-id", "X-Mega-Snapshot-Id")?;
    if returned_snapshot != snapshot_id {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            format!("TreeFrame response snapshot {returned_snapshot} does not match {snapshot_id}"),
        ));
    }
    let returned_request_digest =
        one_treeframe_header(headers, "x-mega-request-digest", "X-Mega-Request-Digest")?;
    if returned_request_digest != expected_request_digest {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            format!(
                "TreeFrame request digest {returned_request_digest} does not match {expected_request_digest}"
            ),
        ));
    }
    Ok(())
}

fn one_treeframe_header<'a>(
    headers: &'a reqwest::header::HeaderMap,
    name: &str,
    display_name: &str,
) -> Result<&'a str, SnapshotError> {
    let mut values = headers.get_all(name).iter();
    let value = values.next().ok_or_else(|| {
        SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            format!("TreeFrame response is missing {display_name}"),
        )
    })?;
    if values.next().is_some() {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            format!("TreeFrame response has duplicate {display_name}"),
        ));
    }
    value.to_str().map_err(|_| {
        SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            format!("TreeFrame response has invalid {display_name}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use reqwest::header::{HeaderMap, HeaderValue};

    use super::*;

    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.mega.treeframe;version=2"),
        );
        headers.insert(
            "x-mega-snapshot-id",
            HeaderValue::from_static(
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
        );
        headers.insert(
            "x-mega-request-digest",
            HeaderValue::from_static(
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ),
        );
        headers
    }

    #[test]
    fn treeframe_identity_headers_are_required_and_bound() {
        let expected_snapshot =
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let expected_digest =
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        validate_treeframe_headers(&headers(), expected_snapshot, expected_digest).unwrap();

        let mut spaced = headers();
        spaced.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("Application/Vnd.Mega.TreeFrame; version = 2"),
        );
        validate_treeframe_headers(&spaced, expected_snapshot, expected_digest).unwrap();

        let mut quoted = headers();
        quoted.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.mega.treeframe; version=\"2\""),
        );
        validate_treeframe_headers(&quoted, expected_snapshot, expected_digest).unwrap();

        let mut missing = headers();
        missing.remove("x-mega-request-digest");
        assert!(validate_treeframe_headers(&missing, expected_snapshot, expected_digest).is_err());

        let mut wrong_digest = headers();
        wrong_digest.insert(
            "x-mega-request-digest",
            HeaderValue::from_static(
                "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            ),
        );
        assert!(
            validate_treeframe_headers(&wrong_digest, expected_snapshot, expected_digest).is_err()
        );

        let mut wrong_media_type = headers();
        wrong_media_type.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        assert!(
            validate_treeframe_headers(&wrong_media_type, expected_snapshot, expected_digest)
                .is_err()
        );

        let mut encoded = headers();
        encoded.insert(
            reqwest::header::CONTENT_ENCODING,
            HeaderValue::from_static("gzip"),
        );
        assert!(validate_treeframe_headers(&encoded, expected_snapshot, expected_digest).is_err());

        let mut duplicate = headers();
        duplicate.append(
            "x-mega-snapshot-id",
            HeaderValue::from_static(
                "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            ),
        );
        assert!(
            validate_treeframe_headers(&duplicate, expected_snapshot, expected_digest).is_err()
        );

        let mut wrong = headers();
        wrong.insert(
            "x-mega-snapshot-id",
            HeaderValue::from_static(
                "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            ),
        );
        assert!(validate_treeframe_headers(&wrong, expected_snapshot, expected_digest).is_err());
    }
}
