//! Private, accounted coordinator transport. No unverified body escapes.

use std::{io, mem::size_of};

use bytes::Bytes;
use mst2_codec::treeframe::{self, Frame};
use serde::Serialize;

use super::{
    client::{
        net_err, server_error, Mst2Client, MAX_JSON_RESPONSE_BYTES, TREEFRAME_REQUEST_MAX_BYTES,
    },
    content::{AccountedBuffer, BudgetClass, ContentBudget},
    SnapshotError, SnapshotErrorCode,
};

/// Only an actual successful END followed by transport EOF produces this.
pub(crate) struct SuccessfulEndReceipt(());
/// Publication also accepts the sized raw collector's actual EOF.
pub(crate) struct ContentEofReceipt(());

impl SuccessfulEndReceipt {
    pub(crate) fn into_content(self) -> ContentEofReceipt {
        ContentEofReceipt(())
    }

    pub(crate) fn content_receipt(&self) -> ContentEofReceipt {
        ContentEofReceipt(())
    }
}

fn invalid(message: impl Into<String>) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::DigestMismatch, message)
}

fn limit(message: &str) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::LimitExceeded, message)
}

struct OwnedRequest {
    buffer: AccountedBuffer,
}
impl AsRef<[u8]> for OwnedRequest {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_bytes()
    }
}

impl io::Write for AccountedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.capacity() - self.len() {
            return Err(io::Error::other(
                "request exceeds fixed serialization capacity",
            ));
        }
        self.append(bytes)
            .map_err(|error| io::Error::other(error.message))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The actual HTTP/retry Bytes owner retains the serialization reservation.
pub(crate) fn request_body<T: Serialize>(
    budget: &ContentBudget,
    value: &T,
) -> Result<Bytes, SnapshotError> {
    let mut buffer = AccountedBuffer::new(
        budget,
        BudgetClass::Construction,
        TREEFRAME_REQUEST_MAX_BYTES,
        size_of::<OwnedRequest>() + size_of::<Bytes>(),
    )?;
    serde_json::to_writer(&mut buffer, value)
        .map_err(|_| limit("TreeFrame request exceeds the fixed JSON serialization capacity"))?;
    Ok(Bytes::from_owner(OwnedRequest { buffer }))
}

async fn typed_error(
    client: &Mst2Client,
    mut response: reqwest::Response,
    budget: &ContentBudget,
) -> SnapshotError {
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|length| length > MAX_JSON_RESPONSE_BYTES as u64)
    {
        return limit("JSON error response exceeds the protocol byte budget");
    }
    let mut buffer = match AccountedBuffer::new(
        budget,
        BudgetClass::Construction,
        MAX_JSON_RESPONSE_BYTES,
        0,
    ) {
        Ok(buffer) => buffer,
        Err(error) => return error,
    };
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                client.count_received_bytes(chunk.len());
                if chunk.len() > buffer.capacity() - buffer.len() {
                    return limit("JSON error response exceeds the protocol byte budget");
                }
                if let Err(error) = buffer.append(&chunk) {
                    return error;
                }
            }
            Ok(None) => return server_error(buffer.as_bytes(), status),
            Err(error) => return net_err(error),
        }
    }
}

pub(crate) async fn raw_content(
    client: &Mst2Client,
    snapshot: &str,
    path: &str,
    digest: &str,
    output: &mut AccountedBuffer,
    budget: &ContentBudget,
) -> Result<ContentEofReceipt, SnapshotError> {
    let mut response = client.owned_blob_response(snapshot, path, digest).await?;
    if !response.status().is_success() {
        return Err(typed_error(client, response, budget).await);
    }
    if response
        .content_length()
        .is_some_and(|length| length != output.capacity() as u64)
    {
        return Err(invalid(
            "raw body Content-Length differs from fixed expected size",
        ));
    }
    while let Some(chunk) = response.chunk().await.map_err(net_err)? {
        client.count_received_bytes(chunk.len());
        output.append(&chunk)?;
    }
    if output.len() != output.capacity() {
        return Err(invalid("raw body is shorter than fixed expected size"));
    }
    Ok(ContentEofReceipt(()))
}

struct WireCursor<'a> {
    client: &'a Mst2Client,
    response: reqwest::Response,
    current: Bytes,
    offset: usize,
}

impl WireCursor<'_> {
    async fn refill(&mut self) -> Result<bool, SnapshotError> {
        while self.offset == self.current.len() {
            match self.response.chunk().await.map_err(net_err)? {
                Some(chunk) => {
                    self.client.count_received_bytes(chunk.len());
                    self.current = chunk;
                    self.offset = 0;
                }
                None => return Ok(false),
            }
        }
        Ok(true)
    }

    async fn header(&mut self) -> Result<Option<[u8; treeframe::HEADER_LEN]>, SnapshotError> {
        if !self.refill().await? {
            return Ok(None);
        }
        let mut header = [0; treeframe::HEADER_LEN];
        let mut written = 0;
        while written < header.len() {
            if !self.refill().await? {
                return Err(invalid("truncated TreeFrame header"));
            }
            let len = (header.len() - written).min(self.current.len() - self.offset);
            header[written..written + len]
                .copy_from_slice(&self.current[self.offset..self.offset + len]);
            self.offset += len;
            written += len;
        }
        Ok(Some(header))
    }

    async fn payload(&mut self, output: &mut AccountedBuffer) -> Result<(), SnapshotError> {
        while output.len() < output.capacity() {
            if !self.refill().await? {
                return Err(invalid("truncated TreeFrame payload"));
            }
            let len = (output.capacity() - output.len()).min(self.current.len() - self.offset);
            output.append(&self.current[self.offset..self.offset + len])?;
            self.offset += len;
        }
        Ok(())
    }
}

// Frame drops first, then wire, then the wire owner's reservation. The codec's
// temporary decompressed Vec is covered before parse_frame and drops inside it.
struct ScratchFrame {
    frame: Frame,
    _wire: AccountedBuffer,
}

pub(crate) struct FrameRequest<'a> {
    pub snapshot: &'a str,
    pub endpoint: &'a str,
    pub body: Bytes,
    pub data_kind: u8,
    pub item_count: u32,
    pub logical_max: usize,
    pub allow_zstd: bool,
}

fn decode_capacity(kind: u8, raw: usize, compressed: bool) -> Result<usize, SnapshotError> {
    let compressed_raw = if compressed { raw } else { 0 };
    let (data, tables) = match kind {
        treeframe::KIND_OBJECT => (
            raw,
            treeframe::OBJECT_MAX_COUNT
                * (size_of::<([u8; 32], Vec<u8>)>() + size_of::<[u8; 32]>()),
        ),
        treeframe::KIND_CHUNK => (raw.saturating_sub(76), 0),
        // JsonScan has three closed fields. Geometric String growth can keep
        // old/new allocations during one realloc: 4x input covers that peak,
        // the held strings and one active key; small boolean/control is fixed.
        treeframe::KIND_ERROR => (4 * treeframe::ERROR_MAX_BYTES, 256),
        _ => (0, 0),
    };
    compressed_raw
        .checked_add(data)
        .and_then(|n| n.checked_add(tables))
        .and_then(|n| n.checked_add(size_of::<ScratchFrame>() + size_of::<Frame>()))
        .ok_or_else(|| limit("frame construction capacity overflow"))
}

/// The sink only borrows one verified data frame while its reservation lives.
/// It returns (unique units, logical bytes), after validating requested identity
/// and uniqueness using its bounded stack state.
pub(crate) async fn consume_frames(
    client: &Mst2Client,
    request: FrameRequest<'_>,
    budget: &ContentBudget,
    mut sink: impl FnMut(&Frame) -> Result<(u32, u64), SnapshotError>,
) -> Result<SuccessfulEndReceipt, SnapshotError> {
    let FrameRequest {
        snapshot,
        endpoint,
        body,
        data_kind,
        item_count,
        logical_max,
        allow_zstd,
    } = request;
    let request_sha: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, &body)
        .as_ref()
        .try_into()
        .unwrap();
    let response = client
        .owned_frame_response(snapshot, endpoint, body)
        .await?;
    if !response.status().is_success() {
        return Err(typed_error(client, response, budget).await);
    }
    let unit_overhead = if data_kind == treeframe::KIND_CHUNK {
        76
    } else {
        44
    };
    let raw_max = (item_count as usize)
        .checked_mul(unit_overhead)
        .and_then(|n| n.checked_add(logical_max))
        .and_then(|n| n.checked_add(treeframe::ERROR_MAX_BYTES))
        .ok_or_else(|| limit("frame response budget overflow"))?;
    let wire_max = raw_max
        .checked_mul(2)
        .and_then(|n| n.checked_add((item_count as usize + 1) * treeframe::HEADER_LEN))
        .ok_or_else(|| limit("frame response budget overflow"))?;
    if response
        .content_length()
        .is_some_and(|length| length > wire_max as u64)
    {
        return Err(limit("frame response exceeds its request byte budget"));
    }
    let mut cursor = WireCursor {
        client,
        response,
        current: Bytes::new(),
        offset: 0,
    };
    let (mut stream, mut sequence, mut frames, mut raw_total, mut wire_total) =
        (None, 0u64, 0usize, 0usize, 0usize);
    let (mut units, mut logical) = (0u32, 0u64);
    loop {
        let header = cursor
            .header()
            .await?
            .ok_or_else(|| invalid("stream missing terminal END"))?;
        let kind = header[6];
        let flags = header[7];
        let wire_len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
        let raw_len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let sid = u32::from_le_bytes(header[20..24].try_into().unwrap());
        let seq = u64::from_le_bytes(header[24..32].try_into().unwrap());
        if &header[..4] != b"MST2"
            || u16::from_le_bytes(header[4..6].try_into().unwrap()) != 2
            || u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize
                != treeframe::HEADER_LEN
            || flags & !treeframe::FLAG_ZSTD != 0
            || (flags != 0 && !allow_zstd)
            || sid == 0
            || stream.is_some_and(|first| first != sid)
            || seq != sequence
            || (kind != data_kind && !matches!(kind, treeframe::KIND_END | treeframe::KIND_ERROR))
            || (matches!(kind, treeframe::KIND_END | treeframe::KIND_ERROR) && flags != 0)
            || (flags == 0 && wire_len != raw_len)
        {
            return Err(invalid("invalid endpoint frame header or stream ordering"));
        }
        let frame_raw_max = match kind {
            treeframe::KIND_OBJECT => treeframe::OBJECT_MAX_RAW,
            treeframe::KIND_CHUNK => 76 + treeframe::CHUNK_MAX_LEN as usize,
            treeframe::KIND_END => 48,
            treeframe::KIND_ERROR => treeframe::ERROR_MAX_BYTES,
            _ => unreachable!("endpoint kind checked"),
        };
        if raw_len > frame_raw_max || wire_len > 2 * 1024 * 1024 {
            return Err(limit("frame exceeds its payload byte limit"));
        }
        frames += 1;
        raw_total = raw_total
            .checked_add(raw_len)
            .ok_or_else(|| limit("raw count overflow"))?;
        wire_total = wire_total
            .checked_add(treeframe::HEADER_LEN + wire_len)
            .ok_or_else(|| limit("wire count overflow"))?;
        if frames > item_count as usize + 2 || raw_total > raw_max || wire_total > wire_max {
            return Err(limit("frame response exceeds its request aggregate budget"));
        }
        stream = Some(sid);
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("sequence overflow"))?;
        let extra = decode_capacity(kind, raw_len, flags != 0)?;
        let mut wire = AccountedBuffer::new(
            budget,
            BudgetClass::Construction,
            treeframe::HEADER_LEN + wire_len,
            extra,
        )?;
        wire.append(&header)?;
        cursor.payload(&mut wire).await?;
        let (frame, used) = treeframe::parse_frame(wire.as_bytes())
            .map_err(|error| invalid(format!("frame: {error}")))?;
        if used != wire.len() {
            return Err(invalid(
                "frame parser did not consume its fixed wire buffer",
            ));
        }
        let scratch = ScratchFrame { frame, _wire: wire };
        match &scratch.frame {
            Frame::End(end) => {
                if end.request_item_count != item_count
                    || end.unique_unit_count != units
                    || units != item_count
                    || end.logical_bytes != logical
                    || end.request_body_sha256 != request_sha
                {
                    return Err(invalid(
                        "END unit set, logical bytes or request digest differs",
                    ));
                }
                drop(scratch);
                if cursor.refill().await? {
                    return Err(invalid("bytes after terminal END"));
                }
                client.count_units(units as u64);
                return Ok(SuccessfulEndReceipt(()));
            }
            Frame::Error(error) => {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::from_server(&error.code),
                    format!(
                        "server rejected TreeFrame request: {} (request_id {})",
                        error.code, error.request_id
                    ),
                ));
            }
            frame => {
                let (added_units, added_bytes) = sink(frame)?;
                units = units
                    .checked_add(added_units)
                    .ok_or_else(|| invalid("unit count overflow"))?;
                logical = logical
                    .checked_add(added_bytes)
                    .ok_or_else(|| invalid("logical count overflow"))?;
                if units > item_count || logical > logical_max as u64 {
                    return Err(invalid("unrequested units or excessive logical bytes"));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use axum::{
        body::Body,
        extract::State,
        http::Response,
        routing::{get, post},
        Router,
    };
    use futures::StreamExt;
    use mst2_codec::treeframe::{EndPayload, ErrorPayload, ObjectPayload};

    use super::*;
    use crate::snapshot::{content::VerifiedContent, ContentBudgetLimits};

    #[test]
    fn serialized_request_bytes_retain_credits_through_last_actual_clone() {
        let budget = budget();
        let body = request_body(&budget, &serde_json::json!({"items": ["a"]})).unwrap();
        let charged = budget.usage().construction_bytes;
        assert!(charged >= TREEFRAME_REQUEST_MAX_BYTES);
        let clone = body.clone();
        drop(body);
        assert_eq!(budget.usage().construction_bytes, charged);
        assert_eq!(clone.as_ref(), b"{\"items\":[\"a\"]}");
        drop(clone);
        assert_eq!(budget.usage().construction_bytes, 0);
        let too_large = "x".repeat(TREEFRAME_REQUEST_MAX_BYTES);
        assert_eq!(
            request_body(&budget, &too_large).err().unwrap().code,
            SnapshotErrorCode::LimitExceeded
        );
        assert_eq!(budget.usage().construction_bytes, 0);
    }

    const SNAPSHOT: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const CONTENT: &[u8] = b"bounded verified frame";

    fn hash(bytes: &[u8]) -> [u8; 32] {
        ring::digest::digest(&ring::digest::SHA256, bytes)
            .as_ref()
            .try_into()
            .unwrap()
    }

    struct Fixture {
        mode: AtomicUsize,
        data_requests: AtomicUsize,
    }
    struct Server {
        client: Mst2Client,
        state: Arc<Fixture>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn response(State(state): State<Arc<Fixture>>, body: Bytes) -> Response<Body> {
        state.data_requests.fetch_add(1, Ordering::SeqCst);
        let mode = state.mode.load(Ordering::SeqCst);
        let payload = ObjectPayload {
            objects: vec![(hash(CONTENT), CONTENT.to_vec())],
        };
        let mut wire = if mode == 14 || mode == 18 {
            payload.encode_zstd(27, 0).unwrap()
        } else {
            payload.encode(27, u64::from(mode == 3)).unwrap()
        };
        let mut end = EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: CONTENT.len() as u64,
            request_body_sha256: hash(&body),
        };
        if mode == 4 {
            end.request_item_count = 2;
        }
        if mode == 5 {
            end.logical_bytes += 1;
        }
        if mode == 6 {
            end.request_body_sha256[0] ^= 1;
        }
        if mode == 7 {
            wire.extend(payload.encode(27, 1).unwrap());
        }
        if mode == 8 {
            wire = ObjectPayload {
                objects: vec![(hash(b"foreign"), b"foreign".to_vec())],
            }
            .encode(27, 0)
            .unwrap();
        }
        if mode == 12 || mode == 13 {
            wire.extend(
                ErrorPayload {
                    code: "INTEGRITY_ERROR".into(),
                    retryable: false,
                    request_id: "late-error".into(),
                }
                .encode(27, 1)
                .unwrap(),
            );
        } else if mode != 10 {
            wire.extend(end.encode(
                if mode == 2 { 28 } else { 27 },
                if mode == 7 { 2 } else { 1 },
            ));
        }
        if mode == 9 {
            wire.pop();
        }
        if mode == 11 || mode == 13 {
            wire.push(0);
        }
        if mode == 16 {
            wire[7] = 0x80;
        }
        let chunks: Vec<Bytes> = if mode == 1 {
            wire.into_iter()
                .map(|byte| Bytes::from(vec![byte]))
                .collect()
        } else {
            vec![Bytes::from(wire)]
        };
        let stream = futures::stream::iter(chunks.into_iter().map(Ok::<_, Infallible>));
        let response_body = if mode == 15 {
            Body::from_stream(stream.chain(futures::stream::pending()))
        } else {
            Body::from_stream(stream)
        };
        Response::builder()
            .header("content-type", "application/vnd.mega.treeframe;version=2")
            .header("x-mega-snapshot-id", SNAPSHOT)
            .header(
                "x-mega-request-digest",
                format!("sha256:{}", hex::encode(hash(&body))),
            )
            .body(response_body)
            .unwrap()
    }

    impl Server {
        async fn start() -> Self {
            let state = Arc::new(Fixture {
                mode: AtomicUsize::new(0),
                data_requests: AtomicUsize::new(0),
            });
            let app = Router::new().route("/api/v2/snapshots/{snapshot}/objects", post(response))
                .route("/api/v2/snapshots/{snapshot}/blob", get(|| async {
                    (axum::http::StatusCode::NOT_FOUND,
                     axum::Json(serde_json::json!({"error":{"code":"PATH_NOT_FOUND","message":"missing"}})))
                })).with_state(state.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()))
                .with_request_timeout(Duration::from_secs(2));
            Self {
                client,
                state,
                task: tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
            }
        }
    }

    fn budget() -> Arc<ContentBudget> {
        ContentBudget::new(ContentBudgetLimits::new(1024, 8 * 1024 * 1024).unwrap())
    }

    async fn collect(
        server: &Server,
        budget: &ContentBudget,
        allow_zstd: bool,
    ) -> Result<Arc<VerifiedContent>, SnapshotError> {
        let body = request_body(
            budget,
            &serde_json::json!({"items":[{"path":"/a", "expected_digest":format!("sha256:{}",hex::encode(hash(CONTENT)))}]}),
        )?;
        let mut output = AccountedBuffer::new(
            budget,
            BudgetClass::Output,
            CONTENT.len(),
            size_of::<VerifiedContent>(),
        )?;
        let mut seen = false;
        let receipt = consume_frames(
            &server.client,
            FrameRequest {
                snapshot: SNAPSHOT,
                endpoint: "objects",
                body,
                data_kind: treeframe::KIND_OBJECT,
                item_count: 1,
                logical_max: CONTENT.len(),
                allow_zstd,
            },
            budget,
            |frame| {
                let Frame::Object(payload) = frame else {
                    return Err(invalid("unexpected data kind"));
                };
                if seen || payload.objects.len() != 1 || payload.objects[0].0 != hash(CONTENT) {
                    return Err(invalid("unrequested or duplicate object"));
                }
                assert_eq!(payload.objects.capacity(), payload.objects.len());
                assert_eq!(payload.objects[0].1.capacity(), payload.objects[0].1.len());
                assert!(
                    budget.usage().construction_bytes > 0,
                    "decoded frame remains charged inside sink"
                );
                output.append(&payload.objects[0].1)?;
                seen = true;
                Ok((1, CONTENT.len() as u64))
            },
        )
        .await?;
        VerifiedContent::publish(
            output,
            CONTENT.len(),
            &hash(CONTENT),
            receipt.into_content(),
        )
    }

    async fn empty_construction(budget: &ContentBudget) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while budget.usage().construction_bytes != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn real_http_single_byte_split_packed_frames_and_negotiated_compression() {
        let server = Server::start().await;
        for mode in [0, 1, 18] {
            server.state.mode.store(mode, Ordering::SeqCst);
            let budget = budget();
            let owner = collect(&server, &budget, mode == 18).await.unwrap();
            assert_eq!(owner.as_bytes(), CONTENT);
            empty_construction(&budget).await;
            assert_eq!(budget.usage().output_bytes, 1024);
            drop(owner);
            assert_eq!(budget.usage().output_bytes, 0);
        }
    }

    #[tokio::test]
    async fn real_http_stream_identity_sequence_units_end_and_late_failures_publish_nothing() {
        let server = Server::start().await;
        for mode in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16] {
            server.state.mode.store(mode, Ordering::SeqCst);
            let budget = budget();
            assert!(
                collect(&server, &budget, false).await.is_err(),
                "mode {mode} unexpectedly published"
            );
            empty_construction(&budget).await;
            assert_eq!(
                budget.usage().output_bytes,
                0,
                "mode {mode} retained unpublished output"
            );
        }
    }

    #[tokio::test]
    async fn terminal_end_without_real_eof_keeps_builder_until_actual_future_drop() {
        let server = Arc::new(Server::start().await);
        server.state.mode.store(15, Ordering::SeqCst);
        let budget = budget();
        let task = {
            let server = server.clone();
            let budget = budget.clone();
            tokio::spawn(async move { collect(&server, &budget, false).await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while server.state.data_requests.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(budget.usage().output_bytes, 1024);
        assert!(!task.is_finished(), "END alone published before EOF");
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        empty_construction(&budget).await;
        assert_eq!(budget.usage().output_bytes, 0);
    }

    #[tokio::test]
    async fn typed_http_error_uses_admitted_body_and_returns_its_capacity() {
        let server = Server::start().await;
        let budget = budget();
        let mut output = AccountedBuffer::new(&budget, BudgetClass::Output, 1, 0).unwrap();
        let result = raw_content(
            &server.client,
            SNAPSHOT,
            "/absent",
            "ignored",
            &mut output,
            &budget,
        )
        .await;
        assert_eq!(result.err().unwrap().code, SnapshotErrorCode::PathNotFound);
        assert_eq!(budget.usage().construction_bytes, 0);
        drop(output);
        assert_eq!(budget.usage().output_bytes, 0);
    }
}
