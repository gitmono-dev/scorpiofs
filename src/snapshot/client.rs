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
//! typed server errors whose canonical envelope says `retryable: true` are
//! re-attempted. Deterministic projection/integrity failures finish one
//! request and are surfaced without multiplying commit-update latency.

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
    /// Discovery is frozen independently for each resolved reader.
    profile: Option<Arc<super::capabilities::CanonicalCapabilities>>,
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
const MAX_ATTEMPTS: u32 = super::resolve_receipt::ATTEMPT_LIMIT as u32;
const BASE_BACKOFF_MS: u64 = 40;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_JSON_REQUEST_BYTES: usize = TREEFRAME_REQUEST_MAX_BYTES;
pub(crate) const MAX_JSON_RESPONSE_BYTES: usize = 1_048_576;

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
            profile: None,
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
        client.profile = None;
        client
    }

    pub(crate) fn with_advertisement(
        mut self,
        advertisement: &super::capabilities::CapabilityAdvertisement,
    ) -> Self {
        self.profile = match advertisement {
            super::capabilities::CapabilityAdvertisement::Canonical(caps) => {
                Some(Arc::new(caps.clone()))
            }
            super::capabilities::CapabilityAdvertisement::Legacy(_) => None,
        };
        self
    }

    pub(crate) fn is_canonical(&self) -> bool {
        self.profile.is_some()
    }

    pub(crate) fn request_byte_limit(&self) -> usize {
        self.profile
            .as_ref()
            .map_or(MAX_JSON_REQUEST_BYTES, |caps| {
                caps.limits().max_json_request_bytes as usize
            })
    }

    pub(crate) fn response_byte_limit(&self) -> usize {
        self.profile
            .as_ref()
            .map_or(MAX_JSON_RESPONSE_BYTES, |caps| {
                caps.limits().max_json_response_bytes as usize
            })
    }

    pub(crate) fn request_item_limit(&self) -> usize {
        self.profile
            .as_ref()
            .map_or(128, |caps| caps.limits().max_request_items as usize)
    }

    pub(crate) fn metadata_item_limit(&self) -> usize {
        self.profile
            .as_ref()
            .map_or(64, |caps| caps.limits().max_metadata_items as usize)
    }

    pub(crate) fn directory_limit(&self) -> u32 {
        self.profile
            .as_ref()
            .map_or(256, |caps| caps.limits().max_directory_entries)
    }

    pub(crate) fn object_byte_limit(&self) -> usize {
        self.profile.as_ref().map_or(8 * 1024 * 1024, |caps| {
            caps.limits().small_batch_bytes as usize
        })
    }

    pub(crate) fn chunk_byte_limit(&self) -> usize {
        self.profile.as_ref().map_or(128 * 1024 * 1024, |caps| {
            caps.limits().chunk_batch_bytes as usize
        })
    }

    pub(crate) fn frame_wire_limit(&self) -> usize {
        self.profile.as_ref().map_or(2 * 1024 * 1024, |caps| {
            caps.limits().frame_wire_bytes as usize
        })
    }

    pub(crate) fn validate_file_size(&self, size: u64) -> Result<(), SnapshotError> {
        let limit = self
            .profile
            .as_ref()
            .map_or(super::range::MAX_FILE_SIZE, |caps| {
                caps.limits().max_file_bytes
            });
        if size > limit {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "file exceeds the discovered serving limit",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_path(&self, path: &str) -> Result<(), SnapshotError> {
        if let Some(profile) = &self.profile {
            let limits = profile.limits();
            let path = path.strip_prefix('/').unwrap_or(path);
            if path.len() + 1 > limits.max_path_bytes as usize
                || (!path.is_empty()
                    && path.split('/').count() > limits.max_path_components as usize)
            {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::LimitExceeded,
                    "path exceeds the discovered serving limit",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn require_feature(&self, endpoint: &str) -> Result<(), SnapshotError> {
        if let Some(profile) = &self.profile {
            let features = profile.features();
            let enabled = match endpoint {
                "resolve" => features.strict_publication,
                "directory" => features.directory,
                "lookup" => features.lookup,
                "metadata/pages" => features.metadata_pages,
                "blob" => features.raw_blob,
                "objects" => features.small_objects,
                "chunks" | "chunk-map" => features.chunk_reads,
                _ => false,
            };
            if !enabled {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::SnapshotNotReady,
                    "endpoint is disabled by discovery",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_encoding(&self, encoding: Option<&str>) -> Result<(), SnapshotError> {
        if self.is_canonical() && encoding.is_some_and(|encoding| encoding != "identity") {
            return Err(SnapshotError::new(
                SnapshotErrorCode::InvalidRequest,
                "canonical reader selects identity frames",
            ));
        }
        Ok(())
    }

    async fn json_response<T: DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> Result<T, SnapshotError> {
        let response = self.checked_response(response).await?;
        parse_json(&read_json_bytes(response, self.response_byte_limit()).await?)
    }

    async fn checked_response(
        &self,
        response: reqwest::Response,
    ) -> Result<reqwest::Response, SnapshotError> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let bytes = read_json_bytes(response, self.response_byte_limit())
            .await
            .map_err(|mut error| {
                error.http_status = status.as_u16();
                error
            })?;
        Err(self.response_error(&bytes, status))
    }

    pub(crate) fn response_error(&self, bytes: &[u8], status: StatusCode) -> SnapshotError {
        if self.is_canonical() {
            if let Err(error) =
                super::error_wire::CanonicalSnapshotError::parse_response(bytes, status.as_u16())
            {
                return error;
            }
        }
        server_error(bytes, status)
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

    pub(crate) fn count_received_bytes(&self, bytes: usize) {
        self.recv_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) async fn owned_blob_response(
        &self,
        snapshot_id: &str,
        path: &str,
        expected_digest: &str,
    ) -> Result<reqwest::Response, SnapshotError> {
        self.require_feature("blob")?;
        self.validate_path(path)?;
        let url = self.snapshots_url(&format!(
            "/{snapshot_id}/blob?path={}&expected_digest={}",
            urlencode(path),
            urlencode(expected_digest),
        ));
        self.send_retrying(self.http.get(url)).await
    }

    pub(crate) async fn owned_frame_response(
        &self,
        snapshot_id: &str,
        endpoint: &str,
        body: bytes::Bytes,
    ) -> Result<reqwest::Response, SnapshotError> {
        self.require_feature(endpoint)?;
        let request_digest = format!(
            "sha256:{}",
            hex::encode(ring::digest::digest(&ring::digest::SHA256, &body))
        );
        let response = self
            .send_retrying(
                self.http
                    .post(self.snapshots_url(&format!("/{snapshot_id}/{endpoint}")))
                    .header("content-type", "application/json")
                    .body(body),
            )
            .await?;
        if response.status().is_success() {
            validate_treeframe_headers(response.headers(), snapshot_id, &request_digest)?;
        }
        Ok(response)
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
        self.send_retrying_with_receipt(builder, None).await
    }

    async fn send_retrying_with_receipt(
        &self,
        builder: reqwest::RequestBuilder,
        mut receipt: Option<&mut super::ResolveTraceReceipt>,
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
            .is_some_and(|body| body.len() > self.request_byte_limit())
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
            let request_id = receipt
                .as_deref_mut()
                .map(|receipt| receipt.attempt(attempt))
                .transpose()?;
            if let Some(id) = &request_id {
                this.headers_mut().insert(
                    "x-request-id",
                    reqwest::header::HeaderValue::from_str(id).map_err(|_| {
                        SnapshotError::new(
                            SnapshotErrorCode::InvalidRequest,
                            "invalid resolve attempt id",
                        )
                    })?,
                );
            }
            if attempt > 1 {
                self.retries.fetch_add(1, Ordering::Relaxed);
            }
            let response = self.http.execute(this).await;
            if let (Ok(response), Some(id)) = (&response, &request_id) {
                super::resolve_receipt::validate_echo(response.headers(), id)?;
            }
            match response {
                Ok(resp) if !resp.status().is_success() => {
                    // A canonical error owns its retry classification. Read
                    // the bounded body once so deterministic projection or
                    // integrity failures do not spend the full retry budget.
                    // A malformed canonical envelope has no retry authority;
                    // fail closed instead of repeating a deterministic 5xx.
                    // Older/plain deployments do not carry the canonical
                    // hints, so retain their bounded status retry policy.
                    let status = resp.status();
                    let bytes = read_json_bytes(resp, self.response_byte_limit())
                        .await
                        .map_err(|mut error| {
                            error.http_status = status.as_u16();
                            error
                        })?;
                    let retryable = super::error_wire::CanonicalSnapshotError::parse_response(
                        &bytes,
                        status.as_u16(),
                    )
                    .map(|error| error.retryable)
                    .unwrap_or_else(|_| {
                        let has_canonical_hint =
                            serde_json::from_slice::<serde_json::Value>(&bytes)
                                .ok()
                                .and_then(|value| value.get("error").cloned())
                                .is_some_and(|error| {
                                    error.get("request_id").is_some()
                                        || error.get("retryable").is_some()
                                });
                        !has_canonical_hint && retryable_status(status)
                    });
                    if attempt < MAX_ATTEMPTS && retryable {
                        tokio::time::timeout_at(deadline, sleep_backoff(attempt))
                            .await
                            .map_err(|_| request_deadline())?;
                    } else {
                        return Err(self.response_error(&bytes, status));
                    }
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
        self.json_response(resp).await
    }

    /// Discover the canonical profile or explicit legacy capabilities. This
    /// does not grant authorization. Readers freeze its limits in their client.
    pub async fn capability_advertisement(
        &self,
    ) -> Result<super::capabilities::CapabilityAdvertisement, SnapshotError> {
        let resp = self
            .send_retrying(self.http.get(self.snapshots_url("/capabilities")))
            .await?;
        super::capabilities::parse(self.json_response(resp).await?)
    }

    /// Fix a view on `target` for `scope`; returns the descriptor + lease.
    pub async fn resolve(
        &self,
        scope: &str,
        lease_seconds: u64,
    ) -> Result<ResolveResponse, SnapshotError> {
        self.resolve_with_receipt(scope, lease_seconds, None).await
    }

    /// Opt in to exact response-id checks and a receipt from this resolve's
    /// actual attempts. Other requests keep their existing headers and policy.
    pub async fn resolve_observed(
        &self,
        scope: &str,
        lease_seconds: u64,
        logical_request_id: &str,
    ) -> Result<(ResolveResponse, super::ResolveTraceReceipt), SnapshotError> {
        let mut receipt = super::ResolveTraceReceipt::new(logical_request_id)?;
        let response = self
            .resolve_with_receipt(scope, lease_seconds, Some(&mut receipt))
            .await?;
        Ok((response, receipt.finish()?))
    }

    async fn resolve_with_receipt(
        &self,
        scope: &str,
        lease_seconds: u64,
        receipt: Option<&mut super::ResolveTraceReceipt>,
    ) -> Result<ResolveResponse, SnapshotError> {
        self.resolve_request_internal(
            &super::ResolveRequest::latest(scope, lease_seconds),
            receipt,
            false,
        )
        .await
    }

    /// Typed canonical resolve. Discovery is required by SnapshotReader before
    /// this call; no legacy envelope can satisfy this explicit contract.
    pub async fn resolve_request(
        &self,
        request: &super::ResolveRequest,
    ) -> Result<ResolveResponse, SnapshotError> {
        self.resolve_request_internal(request, None, true).await
    }

    pub(crate) async fn resolve_request_observed(
        &self,
        request: &super::ResolveRequest,
        logical_request_id: &str,
    ) -> Result<(ResolveResponse, super::ResolveTraceReceipt), SnapshotError> {
        let mut receipt = super::ResolveTraceReceipt::new(logical_request_id)?;
        let response = self
            .resolve_request_internal(request, Some(&mut receipt), true)
            .await?;
        Ok((response, receipt.finish()?))
    }

    async fn resolve_request_internal(
        &self,
        request: &super::ResolveRequest,
        receipt: Option<&mut super::ResolveTraceReceipt>,
        require_canonical: bool,
    ) -> Result<ResolveResponse, SnapshotError> {
        super::auth::validate_scope(&request.scope)?;
        self.validate_path(&request.scope)?;
        self.require_feature("resolve")?;
        if let super::ResolveTarget::View { view_id } = &request.target {
            super::frames::parse_digest(view_id).map_err(|_| {
                SnapshotError::new(
                    SnapshotErrorCode::InvalidRequest,
                    "resolve view ID is not canonical",
                )
            })?;
        }
        if !(60..=3600).contains(&request.lease_seconds) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::InvalidRequest,
                "resolve lease suggestion must be between 60 and 3600 seconds",
            ));
        }
        let mut body = serde_json::to_value(request)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::InvalidRequest, e.to_string()))?;
        body["supported_metadata_codecs"] = serde_json::json!([1]);
        let resp = self
            .send_retrying_with_receipt(
                self.http.post(self.snapshots_url("/resolve")).json(&body),
                receipt,
            )
            .await?;
        let value = self.json_response(resp).await?;
        super::resolve_wire::parse_request(value, request, require_canonical || self.is_canonical())
    }

    /// One directory page; `cursor` continues pagination (spec 04 §5).
    pub async fn directory(
        &self,
        snapshot_id: &str,
        path: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<DirectoryResponse, SnapshotError> {
        self.require_feature("directory")?;
        self.validate_path(path)?;
        if !(1..=256).contains(&limit) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeInvalid,
                "directory limit must be 1..256",
            ));
        }
        let limit = limit.min(self.directory_limit());
        let mut url = self.snapshots_url(&format!(
            "/{snapshot_id}/directory?path={}&limit={limit}",
            urlencode(path)
        ));
        if let Some(c) = cursor {
            url.push_str("&cursor=");
            url.push_str(&urlencode(c));
        }
        let resp = self.send_retrying(self.http.get(url)).await?;
        let page: DirectoryResponse = self.json_response(resp).await?;
        super::directory::validate_page(&page, snapshot_id, path, limit, cursor)?;
        for entry in &page.entries {
            if let Some(size) = &entry.size {
                self.validate_file_size(super::frames::parse_count(size, "directory size")?)?;
            }
        }
        Ok(page)
    }

    /// Batch path resolution (spec 04 §7).
    pub async fn lookup(
        &self,
        snapshot_id: &str,
        paths: &[String],
    ) -> Result<LookupResponse, SnapshotError> {
        self.require_feature("lookup")?;
        for path in paths {
            self.validate_path(path)?;
        }
        if paths.len() > self.request_item_limit() && self.is_canonical() {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "lookup exceeds the discovered item limit",
            ));
        }
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
        let response: LookupResponse = self.json_response(resp).await?;
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
                if let Some(size) = &node.size {
                    self.validate_file_size(super::frames::parse_count(size, "lookup size")?)?;
                }
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
        self.require_feature("blob")?;
        self.validate_path(path)?;
        let url = self.snapshots_url(&format!(
            "/{snapshot_id}/blob?path={}&expected_digest={}",
            urlencode(path),
            urlencode(expected_digest)
        ));
        let resp = self.send_retrying(self.http.get(url)).await?;
        let mut resp = self.checked_response(resp).await?;
        let byte_limit = self
            .profile
            .as_ref()
            .map_or(MAX_BUFFERED_FILE_BYTES, |caps| {
                caps.limits().max_file_bytes.min(MAX_BUFFERED_FILE_BYTES)
            });
        if resp
            .content_length()
            .is_some_and(|length| length > byte_limit)
        {
            return Err(buffered_limit());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = resp.chunk().await.map_err(net_err)? {
            self.recv_bytes
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
            if chunk.len() as u64 > byte_limit - bytes.len() as u64 {
                return Err(buffered_limit());
            }
            if chunk.len() > bytes.capacity() - bytes.len() {
                let target = (bytes.len() + chunk.len())
                    .max(bytes.capacity().saturating_mul(2))
                    .min(byte_limit as usize);
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

pub(crate) fn server_error(bytes: &[u8], status: StatusCode) -> SnapshotError {
    let Ok(value) = parse_json::<serde_json::Value>(bytes) else {
        return SnapshotError {
            code: SnapshotErrorCode::Internal,
            message: format!("HTTP {status} without valid error envelope"),
            http_status: status.as_u16(),
        };
    };
    super::error_wire::parse(value, status.as_u16()).unwrap_or_else(|()| SnapshotError {
        code: SnapshotErrorCode::IntegrityError,
        message: "HTTP error envelope violates its selected contract or status binding".into(),
        http_status: status.as_u16(),
    })
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

async fn read_json_bytes(
    mut resp: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, SnapshotError> {
    if resp
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(json_limit("response"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(net_err)? {
        if chunk.len() > limit - bytes.len() {
            return Err(json_limit("response"));
        }
        if chunk.len() > bytes.capacity() - bytes.len() {
            let target = (bytes.len() + chunk.len())
                .max(bytes.capacity().saturating_mul(2))
                .min(limit);
            bytes
                .try_reserve_exact(target - bytes.len())
                .map_err(|_| json_limit("response"))?;
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub(crate) fn parse_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, SnapshotError> {
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

/// Statuses worth another attempt for legacy/plain responses. Canonical
/// envelopes are handled by their explicit `retryable` field above.
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

pub(crate) fn net_err(e: reqwest::Error) -> SnapshotError {
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

#[cfg(test)]
mod retry_classification_tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use axum::{
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::get,
        Json, Router,
    };

    use super::Mst2Client;

    async fn server(
        requests: Arc<AtomicUsize>,
        response: impl Fn(usize) -> Response + Send + Sync + 'static,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let response = Arc::new(response);
        let app = Router::new().route(
            "/probe",
            get({
                let requests = Arc::clone(&requests);
                move || {
                    let requests = Arc::clone(&requests);
                    let response = Arc::clone(&response);
                    async move {
                        let number = requests.fetch_add(1, Ordering::SeqCst) + 1;
                        response(number)
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("test server");
        });
        (format!("http://{address}"), task)
    }

    #[tokio::test]
    async fn canonical_non_retryable_integrity_error_is_not_retried() {
        let requests = Arc::new(AtomicUsize::new(0));
        let (base, task) = server(Arc::clone(&requests), |_| {
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "error": {
                        "code": "INTEGRITY_ERROR",
                        "message": "projection digest mismatch",
                        "request_id": "test-request",
                        "retryable": false
                    }
                })),
            )
                .into_response()
        })
        .await;
        let client = Mst2Client::new(base.clone());
        let request = client.http.get(format!("{base}/probe"));
        let error = client.send_retrying(request).await.unwrap_err();
        assert_eq!(error.code, super::SnapshotErrorCode::IntegrityError);
        assert_eq!(error.http_status, 502);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(client.retry_count(), 0);
        task.abort();
    }

    #[tokio::test]
    async fn canonical_retryable_unavailable_error_retries() {
        let requests = Arc::new(AtomicUsize::new(0));
        let (base, task) = server(Arc::clone(&requests), |number| {
            if number == 1 {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "error": {
                            "code": "SNAPSHOT_NOT_READY",
                            "message": "publication still being indexed",
                            "request_id": "test-request",
                            "retryable": true
                        }
                    })),
                )
                    .into_response()
            } else {
                StatusCode::NO_CONTENT.into_response()
            }
        })
        .await;
        let client = Mst2Client::new(base.clone());
        let request = client.http.get(format!("{base}/probe"));
        let response = client.send_retrying(request).await.expect("retry succeeds");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        assert_eq!(client.retry_count(), 1);
        task.abort();
    }

    #[tokio::test]
    async fn legacy_503_without_retry_hint_keeps_bounded_status_retry() {
        let requests = Arc::new(AtomicUsize::new(0));
        let (base, task) = server(Arc::clone(&requests), |number| {
            if number == 1 {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "error": {
                            "code": "TEMPORARY_UNAVAILABLE",
                            "message": "legacy transient"
                        }
                    })),
                )
                    .into_response()
            } else {
                StatusCode::NO_CONTENT.into_response()
            }
        })
        .await;
        let client = Mst2Client::new(base.clone());
        let response = client
            .send_retrying(client.http.get(format!("{base}/probe")))
            .await
            .expect("legacy status retry succeeds");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        assert_eq!(client.retry_count(), 1);
        task.abort();
    }

    #[tokio::test]
    async fn malformed_canonical_503_with_hint_is_not_retried() {
        let requests = Arc::new(AtomicUsize::new(0));
        let (base, task) = server(Arc::clone(&requests), |_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": {
                        "code": "SNAPSHOT_NOT_READY",
                        "message": "missing retryable",
                        "request_id": "test-request"
                    }
                })),
            )
                .into_response()
        })
        .await;
        let client = Mst2Client::new(base.clone());
        let error = client
            .send_retrying(client.http.get(format!("{base}/probe")))
            .await
            .unwrap_err();
        assert_eq!(error.http_status, 503);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(client.retry_count(), 0);
        task.abort();
    }
}

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
        self.json_response(self.send_retrying(self.http.get(url)).await?)
            .await
    }

    pub(crate) async fn post_json(
        &self,
        url: impl AsRef<str>,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, SnapshotError> {
        let url = url.as_ref();
        self.json_response(self.send_retrying(self.http.post(url).json(&body)).await?)
            .await
    }

    /// Status selects the release wire contract before decoding: canonical
    /// 204 has no JSON, while legacy 200 carries an identity-bound receipt.
    pub(crate) async fn delete_release_json(
        &self,
        url: impl AsRef<str>,
    ) -> Result<Option<serde_json::Value>, SnapshotError> {
        let url = url.as_ref();
        let mut response = self
            .checked_response(self.send_retrying(self.http.delete(url)).await?)
            .await?;
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
            StatusCode::OK if !self.is_canonical() => self.json_response(response).await.map(Some),
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
        if body.len() > self.request_byte_limit() {
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
            max_response_bytes.min(self.response_byte_limit())
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
            return Err(self.response_error(&bytes, status));
        }
        Ok(bytes)
    }

    pub(crate) async fn head_blob(
        &self,
        url: impl AsRef<str>,
    ) -> Result<(u64, String, String), SnapshotError> {
        let url = url.as_ref();
        let resp = self
            .checked_response(self.send_retrying(self.http.head(url)).await?)
            .await?;
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
        self.validate_file_size(len)?;
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
