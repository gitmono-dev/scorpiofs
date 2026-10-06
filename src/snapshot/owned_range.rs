//! Owned ranges prove their authenticated covering chunks, not whole-file SHA.

use std::{fmt, mem::size_of, sync::Arc};

use mst2_codec::{
    chunkmap::{CHUNKS_PER_PAGE, CHUNK_SIZE},
    treeframe::{self, Frame},
};
use serde::Serialize;

use super::{
    content::{AccountedBuffer, BudgetClass, Reservation},
    frames::{parse_digest, VerifiedChunkMap},
    owned_transport::{consume_frames, request_body, FrameRequest, SuccessfulEndReceipt},
    SnapshotError, SnapshotErrorCode, SnapshotFile, SnapshotReader, OBJECT_CAP,
};

const CACHED_CHUNKS: usize = 16;
const CACHED_LEAVES: usize = 16;

fn invalid(message: &str) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::DigestMismatch, message)
}

#[cfg(test)]
#[path = "owned_range_tests.rs"]
mod tests;

/// Immutable bytes assembled from authenticated covering chunks. This does
/// not assert the complete file's SHA. Last Arc keeps the output reservation.
pub struct VerifiedRange {
    buffer: AccountedBuffer,
}

impl VerifiedRange {
    pub fn as_bytes(&self) -> &[u8] {
        self.buffer.as_bytes()
    }
    pub fn len(&self) -> usize {
        self.buffer.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn publish(buffer: AccountedBuffer, expected: usize) -> Result<Arc<Self>, SnapshotError> {
        if buffer.len() != expected {
            return Err(invalid("incomplete verified range coverage"));
        }
        Ok(Arc::new(Self { buffer }))
    }
}
impl AsRef<[u8]> for VerifiedRange {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
impl fmt::Debug for VerifiedRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedRange")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

struct VerifiedChunk {
    buffer: AccountedBuffer,
    map: [u8; 32],
    index: u64,
    digest: [u8; 32],
}
impl VerifiedChunk {
    fn publish(
        buffer: AccountedBuffer,
        map: [u8; 32],
        index: u64,
        digest: [u8; 32],
        expected: usize,
        _receipt: SuccessfulEndReceipt,
    ) -> Result<Arc<Self>, SnapshotError> {
        if buffer.len() != expected
            || ring::digest::digest(&ring::digest::SHA256, buffer.as_bytes()).as_ref() != digest
        {
            return Err(invalid(
                "chunk differs from its authenticated digest and map length",
            ));
        }
        Ok(Arc::new(Self {
            buffer,
            map,
            index,
            digest,
        }))
    }
}

// Inline fixed table: no geometric heap capacity, and eviction only drops the
// cache's Arc. A concurrent reader keeps its own output reservation alive.
struct ChunkCache {
    entries: [Option<Arc<VerifiedChunk>>; CACHED_CHUNKS],
    len: usize,
    _reservation: Reservation,
}
impl ChunkCache {
    fn new(reader: &SnapshotReader) -> Result<Self, SnapshotError> {
        let reservation = reader
            .content_scope
            .reserve(BudgetClass::Output, size_of::<Self>())?;
        Ok(Self {
            entries: std::array::from_fn(|_| None),
            len: 0,
            _reservation: reservation,
        })
    }
    fn remove(&mut self, index: usize) -> Arc<VerifiedChunk> {
        let owner = self.entries[index].take().unwrap();
        for slot in index..self.len - 1 {
            self.entries[slot] = self.entries[slot + 1].take();
        }
        self.len -= 1;
        owner
    }
    fn get(&mut self, index: u64) -> Option<Arc<VerifiedChunk>> {
        let slot = self.entries[..self.len]
            .iter()
            .position(|entry| entry.as_ref().unwrap().index == index)?;
        let owner = self.remove(slot);
        self.entries[self.len] = Some(owner.clone());
        self.len += 1;
        Some(owner)
    }
    fn insert(&mut self, owner: Arc<VerifiedChunk>) {
        if let Some(slot) = self.entries[..self.len]
            .iter()
            .position(|entry| entry.as_ref().unwrap().index == owner.index)
        {
            drop(self.remove(slot));
        }
        if self.len == CACHED_CHUNKS {
            drop(self.remove(0));
        }
        self.entries[self.len] = Some(owner);
        self.len += 1;
    }
    fn clear(&mut self) {
        for entry in &mut self.entries {
            *entry = None;
        }
        self.len = 0;
    }
}

// Leaf digests and proof metadata are a separate declared metadata scope.
type LeafEntry = (u64, Arc<Vec<[u8; 32]>>);
struct LeafCache {
    entries: [Option<LeafEntry>; CACHED_LEAVES],
    next: usize,
}
impl LeafCache {
    fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| None),
            next: 0,
        }
    }
    fn get(&self, index: u64) -> Option<Arc<Vec<[u8; 32]>>> {
        self.entries
            .iter()
            .flatten()
            .find(|entry| entry.0 == index)
            .map(|entry| entry.1.clone())
    }
    fn insert(&mut self, index: u64, digests: Arc<Vec<[u8; 32]>>) {
        self.entries[self.next] = Some((index, digests));
        self.next = (self.next + 1) % CACHED_LEAVES;
    }
}

/// Strict fixed-root range reader. Its payload cache and returned range owners
/// share the reader's retained-output scope. Metadata/proof heaps are separate.
pub struct OwnedChunkedFile {
    reader: SnapshotReader,
    file: SnapshotFile,
    proven: Option<Arc<super::ProvenSnapshotFile>>,
    map: VerifiedChunkMap,
    leaves: tokio::sync::Mutex<LeafCache>,
    chunks: tokio::sync::Mutex<ChunkCache>,
}

impl OwnedChunkedFile {
    pub async fn open(
        reader: &SnapshotReader,
        path: &str,
        digest: &str,
        size: u64,
    ) -> Result<Self, SnapshotError> {
        reader.authorized_context().validate_relative_path(path)?;
        reader.client().validate_file_size(size)?;
        if !(OBJECT_CAP + 1..=super::range::MAX_FILE_SIZE).contains(&size) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "owned chunk range requires a large file within 8 TiB",
            ));
        }
        let mut file = reader.content_member(path).await?.clone();
        if file.content_digest != digest || file.size != size {
            return Err(invalid("range tuple differs from the fixed-root file"));
        }
        file.rel_path = path.into();
        Self::open_file(reader, file, None).await
    }

    /// Open using selective fixed-root membership without a full closure walk.
    pub async fn open_proven(
        reader: &SnapshotReader,
        proven: Arc<super::ProvenSnapshotFile>,
    ) -> Result<Self, SnapshotError> {
        proven.validate(reader).await?;
        Self::open_file(reader, proven.file().clone(), Some(proven)).await
    }

    async fn open_file(
        reader: &SnapshotReader,
        file: SnapshotFile,
        proven: Option<Arc<super::ProvenSnapshotFile>>,
    ) -> Result<Self, SnapshotError> {
        if !(OBJECT_CAP + 1..=super::range::MAX_FILE_SIZE).contains(&file.size) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "owned chunk range requires a large file within 8 TiB",
            ));
        }
        let chunks = ChunkCache::new(reader)?;
        let request_path = super::reader::ScopeRequestPath(&file.rel_path).to_string();
        let map = reader
            .client()
            .chunk_map(reader.snapshot_id(), &request_path, &file.content_digest)
            .await?;
        if map.file_size != file.size {
            return Err(invalid("chunk map differs from fixed-root size"));
        }
        Ok(Self {
            reader: reader.clone(),
            file,
            proven,
            map,
            leaves: tokio::sync::Mutex::new(LeafCache::new()),
            chunks: tokio::sync::Mutex::new(chunks),
        })
    }
    async fn validate_file(&self) -> Result<(), SnapshotError> {
        match &self.proven {
            Some(proven) => proven.validate(&self.reader).await,
            None => self.reader.validate_content_member(&self.file).await,
        }
    }
    pub fn size(&self) -> u64 {
        self.file.size
    }
    pub fn map_id(&self) -> &str {
        &self.map.map_id
    }
    pub async fn fully_cached(&self) -> bool {
        self.chunks.lock().await.len as u64 == self.map.chunk_count
    }

    /// Caller-owned compatibility copy. Use read_range_owned to retain credits
    /// through the returned bytes' actual lifetime.
    pub async fn read_range(&self, offset: u64, length: u64) -> Result<Vec<u8>, SnapshotError> {
        let owner = self.read_range_owned(offset, length).await?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(owner.len()).map_err(|_| {
            SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "caller range allocation failed",
            )
        })?;
        bytes.extend_from_slice(owner.as_bytes());
        Ok(bytes)
    }

    pub async fn read_range_owned(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Arc<VerifiedRange>, SnapshotError> {
        self.validate_file().await?;
        let returned = if offset >= self.file.size {
            0
        } else {
            length.min(self.file.size - offset)
        };
        if returned > super::client::MAX_BUFFERED_FILE_BYTES {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "owned range output exceeds 64 MiB",
            ));
        }
        let mut output = self
            .admit_output(returned as usize, size_of::<VerifiedRange>())
            .await?;
        if returned == 0 {
            return VerifiedRange::publish(output, 0);
        }
        let end = offset + returned; // returned <= file.size-offset, hence no overflow.
        let first = offset / CHUNK_SIZE as u64;
        let last = (end - 1) / CHUNK_SIZE as u64;
        for index in first..=last {
            let chunk = self.ensure_chunk(index).await?;
            let start = index * CHUNK_SIZE as u64;
            let from = offset.saturating_sub(start) as usize;
            let to = (end - start).min(chunk.buffer.len() as u64) as usize;
            let bytes = chunk
                .buffer
                .as_bytes()
                .get(from..to)
                .ok_or_else(|| invalid("verified chunk does not cover requested range"))?;
            output.append(bytes)?;
        }
        VerifiedRange::publish(output, returned as usize)
    }

    async fn admit_output(
        &self,
        capacity: usize,
        extra: usize,
    ) -> Result<AccountedBuffer, SnapshotError> {
        let allocate = || {
            AccountedBuffer::new(
                &self.reader.content_scope,
                BudgetClass::Output,
                capacity,
                extra,
            )
        };
        match allocate() {
            Err(error) if error.code == SnapshotErrorCode::LimitExceeded => {
                self.chunks.lock().await.clear();
                allocate()
            }
            result => result,
        }
    }

    async fn ensure_chunk(&self, index: u64) -> Result<Arc<VerifiedChunk>, SnapshotError> {
        self.validate_file().await?;
        if let Some(owner) = self.chunks.lock().await.get(index) {
            return Ok(owner);
        }
        let page = index / CHUNKS_PER_PAGE as u64;
        let digests = self.ensure_leaf(page).await?;
        let want = digests
            .get((index % CHUNKS_PER_PAGE as u64) as usize)
            .copied()
            .ok_or_else(|| invalid("authenticated leaf omits requested chunk"))?;
        let map = parse_digest(&self.map.map_id)?;
        let file = parse_digest(&self.file.content_digest)?;
        let expected = self.map.chunk_len(index)? as usize;
        let mut output = self
            .admit_output(expected, size_of::<VerifiedChunk>())
            .await?;
        #[derive(Serialize)]
        struct Item<'a> {
            #[serde(serialize_with = "scope_path")]
            path: &'a str,
            expected_digest: &'a str,
            map_id: &'a str,
            #[serde(serialize_with = "decimal")]
            chunk_index: u64,
        }
        fn scope_path<S: serde::Serializer>(path: &&str, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_str(&super::reader::ScopeRequestPath(path))
        }
        fn decimal<S: serde::Serializer>(index: &u64, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_str(index)
        }
        #[derive(Serialize)]
        struct Request<'a> {
            items: [Item<'a>; 1],
            #[serde(skip_serializing_if = "Option::is_none")]
            encoding: Option<&'static str>,
        }
        let body = request_body(
            self.reader.client(),
            &self.reader.content_scope,
            &Request {
                items: [Item {
                    path: &self.file.rel_path,
                    expected_digest: &self.file.content_digest,
                    map_id: &self.map.map_id,
                    chunk_index: index,
                }],
                encoding: self.reader.encoding_hint(),
            },
        )?;
        let mut seen = false;
        let receipt = consume_frames(
            self.reader.client(),
            FrameRequest {
                snapshot: self.reader.snapshot_id(),
                endpoint: "chunks",
                body,
                data_kind: treeframe::KIND_CHUNK,
                item_count: 1,
                logical_max: expected,
                allow_zstd: self.reader.encoding_hint() == Some("zstd"),
            },
            &self.reader.content_scope,
            |frame| {
                let Frame::Chunk(chunk) = frame else {
                    return Err(invalid("range data is not a chunk"));
                };
                if seen
                    || chunk.map_id != map
                    || chunk.file_content_id != file
                    || chunk.chunk_index != index
                    || chunk.chunk_bytes.len() != expected
                {
                    return Err(invalid("unrequested duplicate or short range chunk"));
                }
                output.append(&chunk.chunk_bytes)?;
                seen = true;
                Ok((1, chunk.chunk_bytes.len() as u64))
            },
        )
        .await?;
        if !seen {
            return Err(invalid("range response omitted requested chunk"));
        }
        let owner = VerifiedChunk::publish(output, map, index, want, expected, receipt)?;
        // Recheck the immutable identity used for the cache key before insert.
        if owner.map != map || owner.digest != want {
            return Err(invalid("verified chunk cache identity changed"));
        }
        self.chunks.lock().await.insert(owner.clone());
        Ok(owner)
    }

    async fn ensure_leaf(&self, page: u64) -> Result<Arc<Vec<[u8; 32]>>, SnapshotError> {
        self.validate_file().await?;
        if let Some(digests) = self.leaves.lock().await.get(page) {
            return Ok(digests);
        }
        let request_path = super::reader::ScopeRequestPath(&self.file.rel_path).to_string();
        let leaf = self
            .reader
            .client()
            .chunk_map_page(
                self.reader.snapshot_id(),
                &request_path,
                &self.file.content_digest,
                &self.map,
                page,
            )
            .await?;
        let digests = Arc::new(leaf.chunk_sha256);
        self.leaves.lock().await.insert(page, digests.clone());
        Ok(digests)
    }
}
