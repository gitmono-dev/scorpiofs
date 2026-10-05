//! High-level fixed-view reader built on [`Mst2Client`].
//!
//! Resolves once, then walks the directory graph to produce a verified file
//! manifest of the fixed view. The view never moves (spec 03 §6); callers
//! bind paths, routes and handles to the snapshot id/generation.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::snapshot::{
    auth::AuthorizedSnapshotContext,
    client::Mst2Client,
    closure::{decode_page, ValidatedSnapshotClosure},
    frames::MetadataPageItem,
    types::{Capabilities, Descriptor, DirEntry, LookupResult, SnapshotError, SnapshotErrorCode},
};

/// Batch cap for one `metadata/pages` request: the server accepts 1..64.
const PAGES_BATCH: usize = 64;

/// Internal cache source. Bytes remain hints until the complete root proof.
pub(crate) trait SnapshotPageSource {
    fn cached_page(&mut self, id: &str) -> Result<Option<Vec<u8>>, SnapshotError>;
    fn received_page(&mut self, id: &str, bytes: &[u8]) -> Result<(), SnapshotError>;
}

struct NetworkPages;
impl SnapshotPageSource for NetworkPages {
    fn cached_page(&mut self, _: &str) -> Result<Option<Vec<u8>>, SnapshotError> {
        Ok(None)
    }
    fn received_page(&mut self, _: &str, _: &[u8]) -> Result<(), SnapshotError> {
        Ok(())
    }
}

/// One pending page fetch: a directory plus the label route from that
/// directory's MTP2 root, and the page id the parent page committed to (the
/// descriptor's `metadata_root` for the scope root).
#[derive(Debug, Clone)]
struct PageFrontier {
    dir: String,
    route: Vec<u8>,
    expected: String,
}

/// One resolved file in the fixed view.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotFile {
    /// Scope-relative path, leading `/` stripped.
    pub rel_path: String,
    pub fs_kind: String,
    pub size: u64,
    pub content_digest: String,
}

/// Keeps the fixed view's retention lease alive across every operation that
/// reaches the server (spec 04 §4). The view itself never changes — only the
/// server-side retention claim does — so renewal is invisible to callers.
///
/// The background task and operations about to reach the server renew when
/// less than a third of the actual server grant remains. Reads served from
/// the local CAS never touch this path, so a completed mount keeps working
/// even if the lease lapses (regression-protected).
struct LeaseKeeper {
    state: Arc<LeaseState>,
    /// Background renewer; aborted when the last reader clone is dropped.
    task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

struct LeaseWindow {
    deadline: Instant,
    renew_at: Instant,
    failure: Option<SnapshotError>,
    transient_error: Option<SnapshotError>,
    renewal_failures: u32,
}

/// The task owns only this state, not LeaseKeeper. Dropping the last reader
/// therefore drops the keeper and aborts even a renewal blocked on HTTP.
struct LeaseState {
    lease_seconds: u64,
    lease_id: String,
    snapshot_id: String,
    authorization_epoch: String,
    authorization_epoch_reported: AtomicBool,
    window: StdMutex<LeaseWindow>,
    renewing: tokio::sync::Mutex<()>,
}

impl LeaseState {
    fn fail(&self, error: SnapshotError) -> SnapshotError {
        let mut window = self.window.lock().unwrap();
        if let Some(failure) = &window.failure {
            return failure.clone();
        }
        let now = Instant::now();
        if recoverable_renewal_error(&error) && now < window.deadline {
            window.renewal_failures = window.renewal_failures.saturating_add(1);
            let delay = Duration::from_millis(100 << (window.renewal_failures - 1).min(3));
            window.renew_at = (now + delay).min(window.deadline);
            window.transient_error = Some(error.clone());
            error
        } else {
            let failure = if recoverable_renewal_error(&error) {
                SnapshotError::new(
                    SnapshotErrorCode::LeaseExpired,
                    "snapshot retention lease expired during renewal",
                )
            } else {
                error
            };
            window.failure = Some(failure.clone());
            failure
        }
    }

    fn needs_renewal(&self) -> Result<bool, SnapshotError> {
        let window = self.window.lock().unwrap();
        if let Some(error) = &window.failure {
            return Err(error.clone());
        }
        let now = Instant::now();
        if now >= window.deadline {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LeaseExpired,
                "snapshot retention lease expired",
            ));
        }
        if now < window.renew_at {
            if let Some(error) = &window.transient_error {
                return Err(error.clone());
            }
        }
        Ok(now >= window.renew_at)
    }

    async fn ensure(&self, client: &Mst2Client) -> Result<(), SnapshotError> {
        if !self.needs_renewal()? {
            return Ok(());
        }
        let _guard = self.renewing.lock().await;
        if !self.needs_renewal()? {
            return Ok(());
        }
        let result = async {
            let deadline = self.window.lock().unwrap().deadline;
            let renewed = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                client.renew_lease(&self.lease_id, self.lease_seconds),
            )
            .await
            .map_err(|_| {
                SnapshotError::new(
                    SnapshotErrorCode::LeaseExpired,
                    "renewal did not finish before the granted deadline",
                )
            })??;
            if Instant::now() >= deadline {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::LeaseExpired,
                    "renewal arrived after the granted deadline",
                ));
            }
            if renewed.get("lease_id").and_then(|v| v.as_str()) != Some(self.lease_id.as_str())
                || renewed.get("snapshot_id").and_then(|v| v.as_str())
                    != Some(self.snapshot_id.as_str())
            {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::IntegrityError,
                    "lease renewal returned a different lease or snapshot",
                ));
            }
            // Legacy renewals omit this field. Once the server reports it,
            // later renewals cannot remove it or change the resolved authority.
            match renewed.get("authorization_epoch") {
                Some(value) if value.as_str() == Some(self.authorization_epoch.as_str()) => {
                    self.authorization_epoch_reported
                        .store(true, Ordering::Relaxed);
                }
                None if !self.authorization_epoch_reported.load(Ordering::Relaxed) => {}
                _ => {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::IntegrityError,
                        "lease renewal changed or invalidated its authorization epoch",
                    ));
                }
            }
            let expiry = renewed
                .get("lease_expires_at")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    SnapshotError::new(
                        SnapshotErrorCode::IntegrityError,
                        "lease renewal omitted its expiry",
                    )
                })?;
            let window = lease_window(expiry)?;
            if Instant::now() >= deadline {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::LeaseExpired,
                    "renewal validation exceeded the granted deadline",
                ));
            }
            *self.window.lock().unwrap() = window;
            Ok(())
        }
        .await;
        result.map_err(|error| self.fail(error))
    }
}

impl LeaseKeeper {
    fn new(
        lease_seconds: u64,
        lease_id: &str,
        snapshot_id: &str,
        expiry: &str,
        authorization_epoch: u64,
    ) -> Result<Self, SnapshotError> {
        if lease_id.is_empty() {
            return Err(SnapshotError::new(
                SnapshotErrorCode::IntegrityError,
                "resolve omitted its lease identity",
            ));
        }
        Ok(LeaseKeeper {
            state: Arc::new(LeaseState {
                lease_seconds: lease_seconds.clamp(1, 3600),
                lease_id: lease_id.to_string(),
                snapshot_id: snapshot_id.to_string(),
                authorization_epoch: authorization_epoch.to_string(),
                authorization_epoch_reported: AtomicBool::new(false),
                window: StdMutex::new(lease_window(expiry)?),
                renewing: tokio::sync::Mutex::new(()),
            }),
            task: StdMutex::new(None),
        })
    }

    fn spawn(&self, client: Mst2Client) {
        let state = self.state.clone();
        let handle = tokio::spawn(async move {
            loop {
                let renew_at = state.window.lock().unwrap().renew_at;
                tokio::time::sleep_until(tokio::time::Instant::from_std(renew_at)).await;
                if let Err(error) = state.ensure(&client).await {
                    if recoverable_renewal_error(&error) {
                        tracing::warn!(code = ?error.code, "snapshot lease renewal temporarily unavailable; retrying within the grant");
                    } else {
                        tracing::warn!(code = ?error.code, "snapshot lease renewal failed; stopping renewer");
                        return;
                    }
                }
            }
        });
        *self.task.lock().unwrap() = Some(handle);
    }
}

fn lease_window(expiry: &str) -> Result<LeaseWindow, SnapshotError> {
    let expires = parse_rfc3339_timestamp(expiry).ok_or_else(|| {
        SnapshotError::new(
            SnapshotErrorCode::IntegrityError,
            "lease expiry is not a valid UTC RFC3339 timestamp",
        )
    })?;
    // Sample the monotonic clock first: scheduling delay between clock reads
    // can shorten this local window, but must never extend the server grant.
    let instant = Instant::now();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        SnapshotError::new(
            SnapshotErrorCode::Internal,
            "system clock precedes the Unix epoch",
        )
    })?;
    let remaining = expires
        .checked_sub(now)
        .filter(|d| !d.is_zero())
        .ok_or_else(|| {
            SnapshotError::new(
                SnapshotErrorCode::LeaseExpired,
                "server returned an expired snapshot lease",
            )
        })?;
    let deadline = instant.checked_add(remaining).ok_or_else(|| {
        SnapshotError::new(
            SnapshotErrorCode::IntegrityError,
            "lease expiry exceeds the clock range",
        )
    })?;
    Ok(LeaseWindow {
        deadline,
        renew_at: instant + remaining.mul_f64(2.0 / 3.0),
        failure: None,
        transient_error: None,
        renewal_failures: 0,
    })
}

fn recoverable_renewal_error(error: &SnapshotError) -> bool {
    match error.code {
        SnapshotErrorCode::TemporaryUnavailable if error.http_status == 0 => true,
        SnapshotErrorCode::TemporaryUnavailable
        | SnapshotErrorCode::Internal
        | SnapshotErrorCode::SnapshotNotReady => {
            matches!(error.http_status, 429 | 500 | 502 | 503 | 504)
        }
        _ => false,
    }
}

impl Drop for LeaseKeeper {
    fn drop(&mut self) {
        if let Some(h) = self.task.lock().unwrap().take() {
            h.abort();
        }
    }
}

/// Minimal RFC3339 (`YYYY-MM-DDTHH:MM:SSZ`) → unix seconds. Only the exact
/// shape this deployment emits is accepted. Calendar validation rejects
/// impossible dates instead of granting a fictitious extra lease window.
fn parse_rfc3339_unix(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    if b.len() != 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || b[19] != b'Z'
    {
        return None;
    }
    let num = |from: usize, to: usize| -> Option<u64> {
        if !b[from..to].iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(&b[from..to]).ok()?.parse::<u64>().ok()
    };
    let (y, mo, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (h, mi, sec) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let month_days = match mo {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if !(1..=month_days).contains(&d) || h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    // days_from_civil (Howard Hinnant), matching runtime.rs' inverse.
    let y_adj = if mo <= 2 { y as i64 - 1 } else { y as i64 };
    let era = y_adj.div_euclid(400);
    let yoe = y_adj - era * 400;
    let mp = (mo as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    if days < 0 {
        return None;
    }
    Some(days as u64 * 86_400 + h * 3600 + mi * 60 + sec)
}

fn parse_rfc3339_timestamp(value: &str) -> Option<Duration> {
    if value.len() == 20 {
        return parse_rfc3339_unix(value).map(Duration::from_secs);
    }
    let bytes = value.as_bytes();
    if !(22..=30).contains(&bytes.len()) || bytes[19] != b'.' || bytes.last() != Some(&b'Z') {
        return None;
    }
    let fraction = &bytes[20..bytes.len() - 1];
    if !fraction.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut canonical = bytes[..19].to_vec();
    canonical.push(b'Z');
    let seconds = parse_rfc3339_unix(std::str::from_utf8(&canonical).ok()?)?;
    let nanos = std::str::from_utf8(fraction).ok()?.parse::<u32>().ok()?
        * 10u32.pow(9 - fraction.len() as u32);
    Some(Duration::new(seconds, nanos))
}

/// A fixed view plus everything needed to read its content.
#[derive(Clone)]
pub struct SnapshotReader {
    pub(crate) client: Mst2Client,
    lease_id: String,
    context: AuthorizedSnapshotContext,
    caps: Capabilities,
    lease: Arc<LeaseKeeper>,
}

impl SnapshotReader {
    /// Resolve `latest` for `scope` and return a bound reader.
    pub async fn resolve(
        client: Mst2Client,
        scope: &str,
        lease_seconds: u64,
    ) -> Result<Self, SnapshotError> {
        let client = client.for_resolve();
        let caps = client.capabilities().await?;
        if !caps.features.resolve || !caps.features.directory {
            return Err(SnapshotError::new(
                SnapshotErrorCode::SnapshotNotReady,
                "deployment does not serve resolve/directory",
            ));
        }
        if !caps.metadata_codecs.contains(&1) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeInvalid,
                "server does not support metadata codec 1",
            ));
        }
        let res = client.resolve(scope, lease_seconds).await?;
        let context = AuthorizedSnapshotContext::new(
            client.base(),
            &client.credential_partition(),
            scope,
            res.descriptor.clone(),
            &res.authorization_epoch,
            &res.publication_sequence,
        )?;
        // Retention is bound independently of the actor's bearer credential.
        // Cloned clients resolving another view cannot overwrite this pair.
        let client = client.with_snapshot_lease(&res.lease_id);
        let lease = Arc::new(LeaseKeeper::new(
            lease_seconds,
            &res.lease_id,
            &res.descriptor.snapshot_id,
            &res.lease_expires_at,
            context.authorization_epoch(),
        )?);
        // Keep the retention claim alive for as long as this reader lives
        // (a hydrate or mount may outlast the initial window). Outside a
        // runtime there is nothing to spawn onto; the lazy path in
        // `ensure_lease` still covers operations that start near expiry.
        if tokio::runtime::Handle::try_current().is_ok() {
            lease.spawn(client.clone());
        }
        Ok(Self {
            client,
            context,
            lease_id: res.lease_id,
            caps,
            lease,
        })
    }

    /// Renew the view's retention lease if it is close to expiry. Called
    /// automatically before every operation that reaches the server; exposed
    /// so a mount can also keep it warm while idle.
    pub async fn ensure_lease(&self) -> Result<(), SnapshotError> {
        self.lease.state.ensure(&self.client).await
    }

    pub fn client(&self) -> &Mst2Client {
        &self.client
    }

    pub fn descriptor(&self) -> &Descriptor {
        self.context.descriptor()
    }

    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }

    pub fn authorized_context(&self) -> &AuthorizedSnapshotContext {
        &self.context
    }

    /// Capabilities captured at resolve time; callers gate frame
    /// transports on these rather than assuming the server profile.
    pub fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    /// Negotiated content encoding for frame responses, exposed for the
    /// range reader (which drives the client directly).
    pub fn encoding_hint(&self) -> Option<&'static str> {
        self.content_encoding()
    }

    /// Identity is byte-compatible with both codec digest contracts. A
    /// legacy `zstd` capability does not establish raw-payload digests.
    fn content_encoding(&self) -> Option<&'static str> {
        None
    }

    pub fn snapshot_id(&self) -> &str {
        &self.descriptor().snapshot_id
    }

    /// Batch lookup of scope-relative paths.
    pub async fn lookup(&self, paths: &[String]) -> Result<Vec<LookupResult>, SnapshotError> {
        for path in paths {
            self.context.validate_relative_path(path)?;
        }
        self.ensure_lease().await?;
        Ok(self.client.lookup(self.snapshot_id(), paths).await?.results)
    }

    /// Fetch one file's verified bytes (digest checked on both server and
    /// client sides).
    pub async fn read_file(&self, rel_path: &str, digest: &str) -> Result<Vec<u8>, SnapshotError> {
        self.context.validate_relative_path(rel_path)?;
        let request_path = if rel_path.is_empty() || rel_path == "/" {
            "/".to_string()
        } else if rel_path.starts_with('/') {
            rel_path.to_string()
        } else {
            format!("/{rel_path}")
        };
        self.ensure_lease().await?;
        self.client
            .blob_verified(self.snapshot_id(), &request_path, digest)
            .await
    }

    /// One directory, following the cursor to the end, so callers see every
    /// entry with the directory's own `directory_root` (spec 04 §6: the
    /// cursor chain is the complete enumeration, never a silent first page).
    pub async fn directory_page(
        &self,
        dir: &str,
        limit: u32,
    ) -> Result<crate::snapshot::types::DirectoryResponse, SnapshotError> {
        self.context.validate_relative_path(dir)?;
        self.ensure_lease().await?;
        let mut cursor: Option<String> = None;
        let mut merged: Option<crate::snapshot::types::DirectoryResponse> = None;
        let mut progress = super::directory::Progress::default();
        loop {
            let page = self
                .client
                .directory(self.snapshot_id(), dir, limit, cursor.as_deref())
                .await?;
            progress.accept(&page, &self.descriptor().metadata_root)?;
            match &mut merged {
                None => merged = Some(page.clone()),
                Some(acc) => {
                    if acc.directory_root != page.directory_root {
                        return Err(SnapshotError::new(
                            SnapshotErrorCode::CursorStale,
                            "directory_root changed mid-enumeration",
                        ));
                    }
                    acc.entries.extend(page.entries.clone());
                    acc.next_cursor = page.next_cursor.clone();
                }
            }
            match page.next_cursor {
                None => return Ok(merged.expect("first page always merged")),
                Some(c) => cursor = Some(c),
            }
        }
    }

    /// Walk the whole scope, collecting files. The MTP2 page surface
    /// (`metadata/pages`, spec 04 §8 / 11 §6) is preferred — one batched
    /// request per 64 pending pages instead of one paginated JSON request
    /// per directory page — and the JSON `directory` transport is the
    /// fallback for deployments without the capability.
    ///
    /// Missing a page after a server-advertised cursor or a parent-committed
    /// child id is an error; empty results are only accepted at real EOF.
    pub async fn file_manifest(&self) -> Result<Vec<SnapshotFile>, SnapshotError> {
        if self.caps.features.metadata_pages {
            return self.file_manifest_pages().await;
        }
        self.ensure_lease().await?;
        let mut out = Vec::new();
        self.walk_dir("/", &self.descriptor().metadata_root, &mut out)
            .await?;
        Ok(out)
    }

    /// Walk the whole scope through the binary page surface. Every page
    /// fetched is bound to the id its parent committed to (the descriptor's
    /// `metadata_root` at the root) and re-hashed client-side, so a page the
    /// fixed view does not contain can neither be accepted nor pass as one.
    pub async fn file_manifest_pages(&self) -> Result<Vec<SnapshotFile>, SnapshotError> {
        Ok(self.snapshot_closure().await?.files().to_vec())
    }

    /// Fetch every metadata dependency of the fixed view and derive its
    /// complete logical namespace, including empty directories and aliases.
    /// A deployment without the page surface cannot provide a full closure.
    pub async fn snapshot_closure(&self) -> Result<ValidatedSnapshotClosure, SnapshotError> {
        let (pages, _, _) = self.snapshot_pages_with(&mut NetworkPages).await?;
        ValidatedSnapshotClosure::from_pages(self.descriptor(), pages)
    }

    /// Collect only dependencies reached from this reader's fixed root.
    /// Both cached and wire bytes pass the same safe decoder and final proof.
    pub(crate) async fn snapshot_pages_with(
        &self,
        source: &mut impl SnapshotPageSource,
    ) -> Result<(BTreeMap<String, Vec<u8>>, u64, u64), SnapshotError> {
        if !self.caps.features.metadata_pages {
            return Err(SnapshotError::new(
                SnapshotErrorCode::SnapshotNotReady,
                "complete snapshot closure requires metadata/pages",
            ));
        }
        self.ensure_lease().await?;
        let mut frontier = vec![PageFrontier {
            dir: "/".to_string(),
            route: Vec::new(),
            expected: self.descriptor().metadata_root.clone(),
        }];
        // Immutable page bytes may be shared, but each logical directory
        // must still be expanded. Identical directories have identical page
        // ids; deduplicating the walk by id would omit one of their paths.
        let mut decoded = HashMap::new();
        let mut page_bytes = BTreeMap::new();
        let mut route_ids = HashMap::new();
        let mut expanded = HashSet::new();
        let mut route_visits = 0;
        let mut page_decodes = 0;
        while !frontier.is_empty() {
            let take = frontier.len().min(PAGES_BATCH);
            let batch: Vec<PageFrontier> = frontier.drain(..take).collect();
            let mut items = Vec::with_capacity(batch.len());
            let mut requested = HashSet::new();
            for f in &batch {
                self.context.validate_relative_path(&f.dir)?;
                if f.route.len() > mst2_codec::metapage::MAX_DEPTH {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::LimitExceeded,
                        "metadata radix depth exceeds 255",
                    ));
                }
                route_ids.insert((f.dir.clone(), f.route.clone()), f.expected.clone());
                if !decoded.contains_key(&f.expected) {
                    if let Some(bytes) = source.cached_page(&f.expected)? {
                        decoded.insert(f.expected.clone(), decode_page(&bytes)?);
                        page_decodes += 1;
                        page_bytes.insert(f.expected.clone(), bytes);
                    }
                }
                if !decoded.contains_key(&f.expected) && requested.insert(f.expected.clone()) {
                    items.push(MetadataPageItem {
                        directory_path: f.dir.clone(),
                        route: f.route.clone(),
                        expected_digest: Some(f.expected.clone()),
                    });
                }
            }
            let pages = if items.is_empty() {
                Vec::new()
            } else {
                self.ensure_lease().await?;
                self.client
                    .metadata_pages(self.snapshot_id(), &items, self.content_encoding())
                    .await?
            };
            // The server returns root-to-terminal witness chains. Their
            // ancestor ids are already committed by the pages that led us
            // to each requested route; unrelated cached pages are excluded.
            let mut allowed = std::collections::HashSet::new();
            for item in &items {
                for depth in 0..=item.route.len() {
                    if let Some(id) =
                        route_ids.get(&(item.directory_path.clone(), item.route[..depth].to_vec()))
                    {
                        allowed.insert(id.clone());
                    }
                }
            }
            for (pid, bytes) in pages {
                let id = format!("sha256:{}", crate::snapshot::frames::hex32(&pid));
                if !allowed.contains(&id) {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        format!("metadata/pages returned an unrequested page {id}"),
                    ));
                }
                // Re-hash the wire bytes ourselves: a (id, bytes) pair is a
                // claim, not evidence.
                if mst2_codec::metapage::page_id(&bytes) != pid {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        format!("metadata/pages payload does not hash to {id}"),
                    ));
                }
                let page = decode_page(&bytes)?;
                page_decodes += 1;
                source.received_page(&id, &bytes)?;
                decoded.insert(id.clone(), page);
                page_bytes.insert(id, bytes);
            }
            // Proven completeness: every page the parent committed to must be
            // in the response — never silently enumerated as absent.
            for f in &batch {
                if !decoded.contains_key(&f.expected) {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        format!(
                            "metadata/pages did not return the requested page {} for {}",
                            f.expected, f.dir
                        ),
                    ));
                }
            }
            for f in batch {
                route_visits += 1;
                if !expanded.insert((f.dir.clone(), f.route.clone())) {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::IntegrityError,
                        "duplicate logical metadata route",
                    ));
                }
                let page = &decoded[&f.expected];
                match page {
                    mst2_codec::metapage::Page::Leaf { entries } => {
                        for e in entries {
                            collect_directory(&f.dir, e, &mut frontier)?;
                        }
                    }
                    mst2_codec::metapage::Page::Branch {
                        terminal, children, ..
                    } => {
                        if let Some(e) = terminal {
                            collect_directory(&f.dir, e, &mut frontier)?;
                        }
                        for c in children {
                            let mut route = f.route.clone();
                            route.push(c.label);
                            frontier.push(PageFrontier {
                                dir: f.dir.clone(),
                                route,
                                expected: format!(
                                    "sha256:{}",
                                    crate::snapshot::frames::hex32(&c.child_page_id)
                                ),
                            });
                        }
                    }
                }
            }
        }
        Ok((page_bytes, route_visits, page_decodes))
    }

    /// Manifest through the JSON `directory` transport only — the equivalence
    /// baseline for the page walk (spec 11 §6: both transports must produce
    /// the same entry set for the same view).
    pub async fn file_manifest_directory(&self) -> Result<Vec<SnapshotFile>, SnapshotError> {
        self.ensure_lease().await?;
        let mut out = Vec::new();
        self.walk_dir("/", &self.descriptor().metadata_root, &mut out)
            .await?;
        Ok(out)
    }

    async fn walk_dir(
        &self,
        dir: &str,
        expected_root: &str,
        out: &mut Vec<SnapshotFile>,
    ) -> Result<(), SnapshotError> {
        self.context.validate_relative_path(dir)?;
        let mut cursor: Option<String> = None;
        let mut progress = super::directory::Progress::for_root(expected_root);
        loop {
            let page = self
                .client
                .directory(self.snapshot_id(), dir, 256, cursor.as_deref())
                .await?;
            progress.accept(&page, &self.descriptor().metadata_root)?;
            for e in page.entries {
                let rel = if dir == "/" {
                    e.name.clone()
                } else {
                    format!("{}/{}", dir.trim_start_matches('/'), e.name)
                };
                self.context.validate_relative_path(&rel)?;
                if let Some(child_root) = e.directory_root {
                    Box::pin(self.walk_dir(&format!("/{rel}"), &child_root, out)).await?;
                } else if let Some(digest) = e.content_digest {
                    let size = crate::snapshot::frames::parse_count(
                        e.size.as_deref().ok_or_else(super::directory::integrity)?,
                        "file size",
                    )?;
                    out.push(SnapshotFile {
                        rel_path: rel,
                        fs_kind: e.fs_kind,
                        size,
                        content_digest: digest,
                    });
                } else {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::Internal,
                        format!("file entry {rel} missing content_digest"),
                    ));
                }
            }
            match page.next_cursor {
                None => return Ok(()),
                Some(c) => cursor = Some(c),
            }
        }
    }

    /// Fetch one file through the frame surface (spec 04 §9 / spec 07):
    /// OBJECT batch for files ≤256 KiB, Chunk Map + CHUNK frames above that.
    ///
    /// Every chunk is hash-checked against the map's leaf and the assembled
    /// file is re-hashed before it is returned — verified chunks alone never
    /// make a verified file (spec 07 §7).
    pub async fn read_file_frames(
        &self,
        rel_path: &str,
        digest: &str,
        size: u64,
    ) -> Result<Vec<u8>, SnapshotError> {
        if size > crate::snapshot::client::MAX_BUFFERED_FILE_BYTES || usize::try_from(size).is_err()
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "whole-file buffered read exceeds the local 64 MiB budget; use range reads",
            ));
        }
        self.context.validate_relative_path(rel_path)?;
        self.ensure_lease().await?;
        let sid = self.snapshot_id();
        let request_path = if rel_path.starts_with('/') {
            rel_path.to_string()
        } else {
            format!("/{rel_path}")
        };
        if size <= 256 * 1024 {
            let map = self
                .client
                .objects(
                    sid,
                    &[(request_path, digest.to_string())],
                    self.content_encoding(),
                )
                .await?;
            let want = crate::snapshot::frames::parse_digest(digest)?;
            let bytes = map.get(&want).cloned().ok_or_else(|| {
                SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    "objects response missing the requested unit",
                )
            })?;
            if bytes.len() as u64 != size {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    format!("{rel_path}: size {} != advertised {size}", bytes.len()),
                ));
            }
            if crate::snapshot::durable::digest_of(&bytes) != digest {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    format!("{rel_path}: whole-object rehash mismatch"),
                ));
            }
            return Ok(bytes);
        }

        // Large file: verify the map binding, every leaf proof, every chunk
        // hash, then the whole-file hash.
        let map = self.client.chunk_map(sid, &request_path, digest).await?;
        if map.file_size != size {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "chunk map file size differs from the fixed view's advertised size",
            ));
        }
        let mut chunk_hashes: Vec<[u8; 32]> = Vec::with_capacity(map.chunk_count as usize);
        for page_index in 0..map.page_count {
            let leaf = self
                .client
                .chunk_map_page(sid, &request_path, digest, &map, page_index)
                .await?;
            chunk_hashes.extend(leaf.chunk_sha256);
        }
        if chunk_hashes.len() as u64 != map.chunk_count {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "chunk map pages did not cover chunk_count",
            ));
        }
        let file_id = crate::snapshot::frames::parse_digest(digest)?;
        let length_map = mst2_codec::chunkmap::ChunkMap::new(file_id, size, map.pages_root)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;

        let map_id = map.map_id.clone();
        let mut out: Vec<Option<Vec<u8>>> = (0..map.chunk_count).map(|_| None).collect();
        for start in (0..map.chunk_count).step_by(128) {
            let items: Vec<crate::snapshot::frames::ChunkRequest> = (start
                ..(start + 128).min(map.chunk_count))
                .map(|i| crate::snapshot::frames::ChunkRequest {
                    path: request_path.clone(),
                    expected_digest: digest.to_string(),
                    map_id: map_id.clone(),
                    chunk_index: i,
                })
                .collect();
            for unit in self
                .client
                .chunks(sid, &items, self.content_encoding())
                .await?
            {
                if unit.chunk_index >= map.chunk_count {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        "chunk index outside the map",
                    ));
                }
                let idx = unit.chunk_index as usize;
                let want_len = length_map.chunk_len(unit.chunk_index).map_err(|e| {
                    SnapshotError::new(SnapshotErrorCode::DigestMismatch, e.to_string())
                })?;
                if unit.bytes.len() as u64 != want_len {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        format!("chunk {idx} length {}", unit.bytes.len()),
                    ));
                }
                if crate::snapshot::durable::digest_of(&unit.bytes).as_str()
                    != format!(
                        "sha256:{}",
                        crate::snapshot::frames::hex32(&chunk_hashes[idx])
                    )
                {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        format!("chunk {idx} hash mismatch"),
                    ));
                }
                out[idx] = Some(unit.bytes);
            }
        }
        let total: usize = out
            .iter()
            .map(|c| c.as_ref().map(|v| v.len()).unwrap_or(0))
            .sum();
        if total as u64 != size {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "assembled size disagrees with the map",
            ));
        }
        let mut assembled = Vec::with_capacity(total);
        for c in out {
            assembled.extend_from_slice(&c.ok_or_else(|| {
                SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    "missing chunk after the response completed",
                )
            })?);
        }
        if crate::snapshot::durable::digest_of(&assembled) != digest {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!("{rel_path}: assembled file rehash mismatch"),
            ));
        }
        Ok(assembled)
    }

    /// Convenience: manifest keyed by scope-relative path.
    pub async fn file_map(&self) -> Result<HashMap<String, SnapshotFile>, SnapshotError> {
        Ok(self
            .file_manifest()
            .await?
            .into_iter()
            .map(|f| (f.rel_path.clone(), f))
            .collect())
    }
}

/// Kept for potential future use of directory entry inspection.
#[allow(dead_code)]
fn _entry_marker(_e: &DirEntry) {}

/// Discover logical child directories; files are derived only after the
/// complete page graph has passed the closure validator.
fn collect_directory(
    dir: &str,
    e: &mst2_codec::metapage::Entry,
    frontier: &mut Vec<PageFrontier>,
) -> Result<(), SnapshotError> {
    use mst2_codec::metapage::EntryKind as MetaEntryKind;
    if e.kind != MetaEntryKind::Directory {
        return Ok(());
    }
    let name = std::str::from_utf8(&e.name)
        .map_err(|_| {
            SnapshotError::new(
                SnapshotErrorCode::Internal,
                "non-utf8 entry name in MTP2 page",
            )
        })?
        .to_string();
    let rel = if dir == "/" {
        name
    } else {
        format!("{}/{}", dir.trim_start_matches('/'), name)
    };
    frontier.push(PageFrontier {
        dir: format!("/{rel}"),
        route: Vec::new(),
        expected: format!("sha256:{}", crate::snapshot::frames::hex32(&e.child_root)),
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ClosureHttpFixture {
        frame_encodings: Vec<&'static str>,
        descriptor: mst2_codec::descriptor::ServingDescriptor,
        pages: BTreeMap<String, Vec<u8>>,
        routes: BTreeMap<(String, Vec<u8>), String>,
        requested: StdMutex<Vec<Vec<MetadataPageItem>>>,
        omit: Option<String>,
        extra: Option<(String, Vec<u8>)>,
    }

    fn closure_http_fixture() -> ClosureHttpFixture {
        use mst2_codec::metapage::{page_id, Entry, EntryKind, Page};
        fn add_directory(
            fixture: &mut ClosureHttpFixture,
            path: &str,
            entries: &[Entry],
        ) -> [u8; 32] {
            let root = page_id(&Page::build(entries).unwrap());
            let mut pending = vec![Vec::new()];
            while let Some(route) = pending.pop() {
                let witnesses = Page::pages_along_route(entries, &route).unwrap();
                let current = witnesses.last().unwrap();
                let current_id = format!("sha256:{}", hex::encode(page_id(current)));
                fixture
                    .routes
                    .insert((path.to_string(), route.clone()), current_id);
                if let Page::Branch { children, .. } = decode_page(current).unwrap() {
                    for child in children {
                        let mut next = route.clone();
                        next.push(child.label);
                        pending.push(next);
                    }
                }
                for bytes in witnesses {
                    fixture
                        .pages
                        .insert(format!("sha256:{}", hex::encode(page_id(&bytes))), bytes);
                }
            }
            root
        }
        let mut fixture = ClosureHttpFixture {
            frame_encodings: vec!["identity"],
            descriptor: mst2_codec::descriptor::ServingDescriptor {
                instance_uuid: *uuid::Uuid::parse_str("11111111-2222-4333-8444-555555555555")
                    .unwrap()
                    .as_bytes(),
                namespace_view_id: [0x22; 32],
                scope: "/project".into(),
                metadata_root: [0; 32],
            },
            pages: BTreeMap::new(),
            routes: BTreeMap::new(),
            requested: StdMutex::new(Vec::new()),
            omit: None,
            extra: None,
        };
        let empty = add_directory(&mut fixture, "/empty", &[]);
        let entries: Vec<_> = (0..192u16)
            .map(|i| {
                Entry::file(
                    EntryKind::Regular,
                    format!("{}{:03}", (b'a' + (i / 64) as u8) as char, i).as_bytes(),
                    1,
                    [0x44; 32],
                )
            })
            .collect();
        let shared = add_directory(&mut fixture, "/left", &entries);
        assert_eq!(add_directory(&mut fixture, "/right", &entries), shared);
        fixture.descriptor.metadata_root = add_directory(
            &mut fixture,
            "/",
            &[
                Entry::dir(b"empty", empty),
                Entry::dir(b"left", shared),
                Entry::dir(b"right", shared),
            ],
        );
        fixture
    }

    async fn serve_closure_fixture(
        fixture: ClosureHttpFixture,
    ) -> (String, Arc<ClosureHttpFixture>, tokio::task::JoinHandle<()>) {
        use axum::{
            body::Bytes,
            extract::State,
            routing::{get, post},
            Json, Router,
        };
        use mst2_codec::treeframe::{EndPayload, MetaPayload};
        use serde_json::{json, Value};
        async fn capabilities(State(fixture): State<Arc<ClosureHttpFixture>>) -> Json<Value> {
            Json(json!({
                "protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": fixture.frame_encodings,
                "features": {"resolve": true, "directory": true, "leases": true, "metadata_pages": true}
            }))
        }
        async fn resolve(State(fixture): State<Arc<ClosureHttpFixture>>) -> Json<Value> {
            let descriptor = &fixture.descriptor;
            Json(json!({
                "descriptor": {
                    "schema_version": 2, "metadata_codec": 1,
                    "instance_id": uuid::Uuid::from_bytes(descriptor.instance_uuid).to_string(),
                    "namespace_view_id": format!("sha256:{}", hex::encode(descriptor.namespace_view_id)),
                    "scope": descriptor.scope, "materialization_policy": 1, "fs_semantics": 1, "access_projection": 0,
                    "metadata_root": format!("sha256:{}", hex::encode(descriptor.metadata_root)),
                    "snapshot_id": format!("sha256:{}", hex::encode(descriptor.snapshot_id().unwrap())),
                },
                "lease_id": "closure-lease", "lease_expires_at": "2099-01-01T00:00:00Z",
                "publication_sequence": "1", "authorization_epoch": "1",
            }))
        }
        async fn metadata(
            State(fixture): State<Arc<ClosureHttpFixture>>,
            body: Bytes,
        ) -> axum::response::Response {
            let request: Value = serde_json::from_slice(&body).unwrap();
            assert!(request.get("encoding").is_none_or(Value::is_null));
            let items: Vec<MetadataPageItem> = request["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| MetadataPageItem {
                    directory_path: item["directory_path"].as_str().unwrap().to_string(),
                    route: serde_json::from_value(item["route"].clone()).unwrap(),
                    expected_digest: Some(item["expected_digest"].as_str().unwrap().to_string()),
                })
                .collect();
            let mut response = BTreeMap::new();
            for item in &items {
                assert_eq!(
                    fixture
                        .routes
                        .get(&(item.directory_path.clone(), item.route.clone())),
                    item.expected_digest.as_ref()
                );
                for depth in 0..=item.route.len() {
                    let id = &fixture.routes
                        [&(item.directory_path.clone(), item.route[..depth].to_vec())];
                    if fixture.omit.as_ref() != Some(id) {
                        response.insert(id.clone(), fixture.pages[id].clone());
                    }
                }
            }
            if let Some((id, bytes)) = &fixture.extra {
                response.insert(id.clone(), bytes.clone());
            }
            fixture.requested.lock().unwrap().push(items.clone());
            let pages: Vec<_> = response
                .into_iter()
                .rev()
                .map(|(id, bytes)| (crate::snapshot::frames::parse_digest(&id).unwrap(), bytes))
                .collect();
            let logical_bytes = pages.iter().map(|(_, bytes)| bytes.len() as u64).sum();
            let mut wire = MetaPayload {
                pages: pages.clone(),
            }
            .encode(7, 0)
            .unwrap();
            wire.extend(
                EndPayload {
                    request_item_count: items.len() as u32,
                    unique_unit_count: pages.len() as u32,
                    logical_bytes,
                    request_body_sha256: crate::snapshot::frames::parse_digest(
                        &crate::snapshot::durable::digest_of(&body),
                    )
                    .unwrap(),
                }
                .encode(7, 1),
            );
            axum::response::Response::builder()
                .header("content-type", "application/vnd.mega.treeframe;version=2")
                .header(
                    "x-mega-snapshot-id",
                    format!(
                        "sha256:{}",
                        hex::encode(fixture.descriptor.snapshot_id().unwrap())
                    ),
                )
                .header(
                    "x-mega-request-digest",
                    crate::snapshot::durable::digest_of(&body),
                )
                .body(axum::body::Body::from(wire))
                .unwrap()
        }
        let fixture = Arc::new(fixture);
        let app = Router::new()
            .route("/api/v2/snapshots/capabilities", get(capabilities))
            .route("/api/v2/snapshots/resolve", post(resolve))
            .route("/api/v2/snapshots/{sid}/metadata/pages", post(metadata))
            .with_state(fixture.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, fixture, task)
    }

    #[tokio::test]
    async fn snapshot_closure_fetches_unique_pages_and_preserves_empty_and_aliased_directories() {
        let (url, fixture, server) = serve_closure_fixture(closure_http_fixture()).await;
        let reader = SnapshotReader::resolve(Mst2Client::new(url), "/project", 600)
            .await
            .unwrap();
        let closure = reader.snapshot_closure().await.unwrap();
        assert_eq!(closure.pages(), &fixture.pages);
        assert_eq!(
            closure
                .directories()
                .iter()
                .map(|d| d.rel_path.as_str())
                .collect::<Vec<_>>(),
            ["", "empty", "left", "right"]
        );
        assert_eq!(closure.files().len(), 384);
        for prefix in ["left/", "right/"] {
            assert_eq!(
                closure
                    .files()
                    .iter()
                    .filter(|f| f.rel_path.starts_with(prefix))
                    .count(),
                192
            );
        }
        let requests = fixture.requested.lock().unwrap();
        assert!(requests.iter().all(|batch| batch.len() <= PAGES_BATCH));
        assert_eq!(
            requests.iter().map(Vec::len).sum::<usize>(),
            fixture.pages.len(),
            "aliases and ancestor witnesses must not cause duplicate physical page requests"
        );
        drop(requests);
        server.abort();
    }

    #[tokio::test]
    async fn legacy_zstd_capability_keeps_reader_on_identity_frames() {
        let mut fixture = closure_http_fixture();
        fixture.frame_encodings.push("zstd");
        let (url, fixture, server) = serve_closure_fixture(fixture).await;
        let reader = SnapshotReader::resolve(Mst2Client::new(url), "/project", 600)
            .await
            .unwrap();
        assert!(reader
            .capabilities()
            .frame_encodings
            .iter()
            .any(|e| e == "zstd"));
        assert_eq!(reader.encoding_hint(), None);
        let closure = reader.snapshot_closure().await.unwrap();
        assert_eq!(closure.pages(), &fixture.pages);
        assert!(!fixture.requested.lock().unwrap().is_empty());
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn snapshot_closure_rejects_missing_children_and_unrequested_pages() {
        for omit in [true, false] {
            let mut fixture = closure_http_fixture();
            if omit {
                fixture.omit = Some(fixture.routes[&("/left".into(), vec![b'a'])].clone());
            } else {
                let bytes =
                    mst2_codec::metapage::Page::build(&[mst2_codec::metapage::Entry::file(
                        mst2_codec::metapage::EntryKind::Regular,
                        b"foreign",
                        1,
                        [8; 32],
                    )])
                    .unwrap();
                fixture.extra = Some((
                    format!(
                        "sha256:{}",
                        hex::encode(mst2_codec::metapage::page_id(&bytes))
                    ),
                    bytes,
                ));
            }
            let (url, _, server) = serve_closure_fixture(fixture).await;
            let reader = SnapshotReader::resolve(Mst2Client::new(url), "/project", 600)
                .await
                .unwrap();
            let error = reader.snapshot_closure().await.unwrap_err();
            assert_eq!(error.code, SnapshotErrorCode::DigestMismatch);
            server.abort();
        }
    }

    #[test]
    fn renewal_errors_use_typed_transport_status_and_keep_terminal_failures_terminal() {
        for (code, status, recoverable) in [
            (SnapshotErrorCode::TemporaryUnavailable, 0, true),
            (SnapshotErrorCode::TemporaryUnavailable, 503, true),
            (SnapshotErrorCode::Internal, 503, true),
            (SnapshotErrorCode::Internal, 429, true),
            (SnapshotErrorCode::SnapshotNotReady, 503, true),
            (SnapshotErrorCode::Internal, 0, false),
            (SnapshotErrorCode::TemporaryUnavailable, 403, false),
            (SnapshotErrorCode::IntegrityError, 503, false),
            (SnapshotErrorCode::ScopeForbidden, 503, false),
            (SnapshotErrorCode::LeaseExpired, 503, false),
        ] {
            let error = SnapshotError {
                code,
                http_status: status,
                message: "same message".into(),
            };
            assert_eq!(
                recoverable_renewal_error(&error),
                recoverable,
                "{code:?}/{status}"
            );
        }
    }

    #[test]
    fn rfc3339_parser_matches_known_values() {
        assert_eq!(parse_rfc3339_unix("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_rfc3339_unix("2026-09-16T02:28:42Z"),
            Some(1_789_525_722)
        );
        assert_eq!(
            parse_rfc3339_unix("2024-02-29T23:59:59Z"),
            Some(1_709_251_199)
        );
    }

    #[test]
    fn rfc3339_parser_rejects_non_canonical_shapes() {
        for bad in [
            "",
            "2026-09-16T02:28:42",       // missing Z
            "2026-09-16 02:28:42Z",      // space separator
            "2026-13-01T00:00:00Z",      // month 13
            "2026-09-16T24:00:00Z",      // hour 24
            "2026-09-16T02:28:42+08:00", // offset form not emitted here
            "2026-02-29T00:00:00Z",      // non-leap year
            "2026-04-31T00:00:00Z",      // April has 30 days
            "2026-09-16T02:28:60Z",      // invalid second
            "+026-09-16T02:28:42Z",      // numeric fields are ASCII digits
            "2026-+9-16T02:28:42Z",
        ] {
            assert_eq!(parse_rfc3339_unix(bad), None, "{bad} must be rejected");
        }
    }

    #[test]
    fn rfc3339_fraction_is_exact_and_strict() {
        assert_eq!(
            parse_rfc3339_timestamp("1970-01-01T00:00:01.123456789Z"),
            Some(Duration::new(1, 123_456_789))
        );
        assert_eq!(
            parse_rfc3339_timestamp("1970-01-01T00:00:01.1Z"),
            Some(Duration::new(1, 100_000_000))
        );
        for bad in [
            "1970-01-01T00:00:01.Z",
            "1970-01-01T00:00:01.1234567890Z",
            "1970-01-01T00:00:01.+1Z",
            "1970-01-01T00:00:01.1+00:00",
            "1970-01-01T00:00:01.１Z",
            "2026-02-29T00:00:01.1Z",
        ] {
            assert_eq!(parse_rfc3339_timestamp(bad), None, "{bad}");
        }
    }

    #[test]
    fn lease_keeper_checks_expiry_and_preserves_failure() {
        let keeper = LeaseKeeper::new(60, "lease", "snapshot", "2099-01-01T00:00:00Z", 1).unwrap();
        assert!(!keeper.state.needs_renewal().unwrap());
        {
            let mut window = keeper.state.window.lock().unwrap();
            window.renew_at = Instant::now() - Duration::from_secs(1);
        }
        assert!(keeper.state.needs_renewal().unwrap());
        let failure = SnapshotError::new(SnapshotErrorCode::IntegrityError, "invalid renewal");
        keeper.state.fail(failure.clone());
        assert_eq!(keeper.state.needs_renewal(), Err(failure));

        for (expiry, expected) in [
            ("", SnapshotErrorCode::IntegrityError),
            ("2026-02-29T00:00:00Z", SnapshotErrorCode::IntegrityError),
            ("1970-01-01T00:00:00Z", SnapshotErrorCode::LeaseExpired),
        ] {
            let error = match LeaseKeeper::new(60, "lease", "snapshot", expiry, 1) {
                Ok(_) => panic!("invalid initial expiry was accepted: {expiry}"),
                Err(error) => error,
            };
            assert_eq!(error.code, expected);
        }
        assert!(LeaseKeeper::new(60, "", "snapshot", "2099-01-01T00:00:00Z", 1).is_err());
        assert_eq!(
            LeaseKeeper::new(99_999, "l", "s", "2099-01-01T00:00:00Z", 1)
                .unwrap()
                .state
                .lease_seconds,
            3600
        );
        assert_eq!(
            LeaseKeeper::new(0, "l", "s", "2099-01-01T00:00:00Z", 1)
                .unwrap()
                .state
                .lease_seconds,
            1
        );
    }

    #[tokio::test]
    async fn dropping_keeper_aborts_an_in_flight_http_renewal() {
        use axum::{routing::post, Json, Router};
        let started = Arc::new(tokio::sync::Notify::new());
        let route_started = started.clone();
        let app = Router::new().route(
            "/api/v2/snapshots/leases/lease/renew",
            post(move || {
                let started = route_started.clone();
                async move {
                    started.notify_one();
                    std::future::pending::<Json<serde_json::Value>>().await
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let keeper = LeaseKeeper::new(60, "lease", "snapshot", "2099-01-01T00:00:00Z", 1).unwrap();
        keeper.state.window.lock().unwrap().renew_at = Instant::now();
        let weak = Arc::downgrade(&keeper.state);
        keeper.spawn(Mst2Client::new(format!("http://{address}")));
        tokio::time::timeout(Duration::from_secs(5), started.notified())
            .await
            .unwrap();
        drop(keeper);
        tokio::time::timeout(Duration::from_secs(5), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("renewal task retained state after the last reader dropped");
        server.abort();
    }
}
