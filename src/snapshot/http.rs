//! Source-aware HTTP transport. Not connected to legacy mounts: callers must
//! negotiate the source capability and acquire an attested descriptor + lease.

use std::{fmt, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::{
    header::{HeaderValue, ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_TYPE},
    Client, StatusCode,
};
use thiserror::Error;
use url::{Host, Url};

use super::{
    backend::{ObjectBackend, ObjectKind, SnapshotReadError},
    identity::{ObjectId, RelativePath, SourceSnapshot},
};

const LEASE_HEADER: &str = "x-mega-snapshot-lease";

#[derive(Debug, Error)]
pub enum HttpBackendConfigError {
    #[error("snapshot base URL must use HTTPS (HTTP is allowed only on loopback) and have no userinfo, query or fragment")]
    InvalidBaseUrl,
    #[error("snapshot credentials must be nonempty, bounded HTTP header values")]
    InvalidCredentials,
    #[error("snapshot HTTP timeouts must be nonzero")]
    InvalidTimeout,
    #[error("failed to initialize snapshot HTTP client")]
    ClientInitialization,
}

#[derive(Debug, Clone, Copy)]
pub struct HttpTimeouts {
    pub connect: Duration,
    pub request: Duration,
}

impl Default for HttpTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(5),
            request: Duration::from_secs(30),
        }
    }
}

/// Tokens are sensitive headers, never URL parameters or Debug fields. A lease
/// retains a snapshot but grants no read permission; the server checks both.
pub struct HttpObjectBackend {
    base: Url,
    client: Client,
    authorization: HeaderValue,
    lease: HeaderValue,
}

impl fmt::Debug for HttpObjectBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpObjectBackend")
            .field("origin", &self.base.origin().ascii_serialization())
            .finish_non_exhaustive()
    }
}

impl HttpObjectBackend {
    pub fn new(
        mut base: Url,
        bearer_token: &str,
        lease: &str,
        timeouts: HttpTimeouts,
    ) -> Result<Self, HttpBackendConfigError> {
        let loopback = match base.host() {
            Some(Host::Domain(domain)) => domain == "localhost",
            Some(Host::Ipv4(address)) => address.is_loopback(),
            Some(Host::Ipv6(address)) => address.is_loopback(),
            None => false,
        };
        if base.cannot_be_a_base()
            || base.host().is_none()
            || !(base.scheme() == "https" || (base.scheme() == "http" && loopback))
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(HttpBackendConfigError::InvalidBaseUrl);
        }
        if timeouts.connect.is_zero() || timeouts.request.is_zero() {
            return Err(HttpBackendConfigError::InvalidTimeout);
        }
        if bearer_token.is_empty()
            || bearer_token.len() > 8192
            || bearer_token.bytes().any(|b| b.is_ascii_whitespace())
            || lease.is_empty()
            || lease.len() > 512
            || lease.bytes().any(|b| b.is_ascii_whitespace())
        {
            return Err(HttpBackendConfigError::InvalidCredentials);
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {bearer_token}"))
            .map_err(|_| HttpBackendConfigError::InvalidCredentials)?;
        let mut lease =
            HeaderValue::from_str(lease).map_err(|_| HttpBackendConfigError::InvalidCredentials)?;
        authorization.set_sensitive(true);
        lease.set_sensitive(true);
        // Preserve an explicitly configured reverse-proxy prefix.
        if !base.path().ends_with('/') {
            base.path_segments_mut()
                .map_err(|_| HttpBackendConfigError::InvalidBaseUrl)?
                .push("");
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(timeouts.connect)
            .timeout(timeouts.request)
            .build()
            .map_err(|_| HttpBackendConfigError::ClientInitialization)?;
        Ok(Self {
            base,
            client,
            authorization,
            lease,
        })
    }
}

#[async_trait]
impl ObjectBackend for HttpObjectBackend {
    async fn fetch(
        &self,
        source: &SourceSnapshot,
        kind: ObjectKind,
        oid: &ObjectId,
        source_path: &RelativePath,
        max_bytes: usize,
    ) -> Result<Bytes, SnapshotReadError> {
        if max_bytes == 0 {
            return Err(SnapshotReadError::InvalidLimits);
        }
        let collection = match kind {
            ObjectKind::Tree => "trees",
            ObjectKind::Blob => "blobs",
        };
        let mut url = self
            .base
            .join(&format!(
                "api/v1/sources/{}/{collection}/{oid}",
                source.source_id
            ))
            .map_err(|_| SnapshotReadError::Unavailable("invalid snapshot endpoint".into()))?;
        url.query_pairs_mut().extend_pairs([
            ("object_format", source.object_format.as_str()),
            ("scope_path", source.scope_path.as_str()),
            ("commit_oid", source.commit_oid.as_str()),
            ("root_tree_oid", source.root_tree_oid.as_str()),
            ("source_path", source_path.as_str()),
        ]);
        let mut response = self
            .client
            .get(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .header(LEASE_HEADER, self.lease.clone())
            .header(ACCEPT, "application/octet-stream")
            .header(ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(transport_error)?;
        match response.status() {
            StatusCode::OK => {}
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                return Err(SnapshotReadError::Forbidden)
            }
            StatusCode::GONE => return Err(SnapshotReadError::Expired),
            status => {
                return Err(SnapshotReadError::Unavailable(format!(
                    "snapshot object HTTP {}",
                    status.as_u16()
                )))
            }
        }
        // Reject successful HTML/login/JSON error pages, partial content and
        // redirects. Only a 200 full raw object is the v1 representation.
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next());
        if content_type != Some("application/octet-stream") {
            return Err(SnapshotReadError::Unavailable(
                "snapshot response is not a raw object".into(),
            ));
        }
        if response
            .content_length()
            .is_some_and(|size| size > max_bytes as u64)
        {
            return Err(SnapshotReadError::ObjectTooLarge { limit: max_bytes });
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transport_error)? {
            if chunk.len() > max_bytes - bytes.len() {
                return Err(SnapshotReadError::ObjectTooLarge { limit: max_bytes });
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(Bytes::from(bytes))
    }
}

fn transport_error(error: reqwest::Error) -> SnapshotReadError {
    // reqwest's Display may include private source paths in the URL. Do not
    // retain it (or headers/tokens) in user-visible errors or diagnostics.
    SnapshotReadError::Unavailable(if error.is_timeout() {
        "snapshot transport timed out".into()
    } else {
        "snapshot transport failed".into()
    })
}

#[cfg(test)]
mod tests;
