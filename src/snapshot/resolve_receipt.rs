//! Optional per-attempt correlation, independent of read authority or leases.

use serde::Serialize;

use super::{SnapshotError, SnapshotErrorCode};

pub(crate) const ATTEMPT_LIMIT: usize = 4;

/// A successful resolve's actual request attempts. This contains no credential
/// or lease and grants no authorization. The caller supplies a unique logical
/// id for each logical resolve; retries append distinct attempt suffixes.
#[derive(Debug, Clone, Serialize)]
pub struct ResolveTraceReceipt {
    logical_request_id: String,
    attempt_ids: Vec<String>,
    final_attempt_id: String,
    retry_count: u32,
}

impl ResolveTraceReceipt {
    pub fn logical_request_id(&self) -> &str {
        &self.logical_request_id
    }
    pub fn attempt_ids(&self) -> &[String] {
        &self.attempt_ids
    }
    pub fn final_attempt_id(&self) -> &str {
        &self.final_attempt_id
    }
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }
    pub(crate) fn new(id: &str) -> Result<Self, SnapshotError> {
        validate_logical_id(id)?;
        Ok(Self {
            logical_request_id: id.into(),
            attempt_ids: Vec::with_capacity(ATTEMPT_LIMIT),
            final_attempt_id: String::new(),
            retry_count: 0,
        })
    }
    pub(crate) fn attempt(&mut self, attempt: u32) -> Result<String, SnapshotError> {
        if !(1..=ATTEMPT_LIMIT as u32).contains(&attempt)
            || self.attempt_ids.len() != attempt as usize - 1
        {
            return Err(echo_error());
        }
        let id = format!("{}:a{attempt}", self.logical_request_id);
        self.attempt_ids.push(id.clone());
        Ok(id)
    }
    pub(crate) fn finish(mut self) -> Result<Self, SnapshotError> {
        self.final_attempt_id = self.attempt_ids.last().cloned().ok_or_else(echo_error)?;
        self.retry_count = self.attempt_ids.len() as u32 - 1;
        Ok(self)
    }
}

pub(crate) fn validate_logical_id(id: &str) -> Result<(), SnapshotError> {
    if id.is_empty()
        || id.len() > 125
        || !id.bytes().all(|byte| {
            matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b':' | b'/')
        })
    {
        return Err(SnapshotError::new(
            SnapshotErrorCode::InvalidRequest,
            "resolve observation id must be a bounded ASCII trace token",
        ));
    }
    Ok(())
}

pub(crate) fn validate_echo(
    headers: &reqwest::header::HeaderMap,
    expected: &str,
) -> Result<(), SnapshotError> {
    let mut values = headers.get_all("x-request-id").iter();
    if values.next().and_then(|value| value.to_str().ok()) != Some(expected)
        || values.next().is_some()
    {
        return Err(echo_error());
    }
    Ok(())
}

fn echo_error() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::IntegrityError,
        "resolve observation response must echo exactly one matching request id",
    )
}
