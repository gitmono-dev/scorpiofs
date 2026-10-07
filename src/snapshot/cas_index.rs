//! Disposable chunk facts derived only from a successful whole-file scan.
//! Facts prove future covering chunks, not the current integrity of unread
//! bytes, completion, or authorization. No on-disk index is trusted.

use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
};

use ring::digest::{Context, SHA256};

use super::{content::AccountedBuffer, secure_fs, SnapshotError, SnapshotErrorCode};

pub(super) const CHUNK_SIZE: u64 = 1024 * 1024;
const INDEX_FORMAT: u32 = 1;
const MAX_DIGEST_BYTES: usize = 8 * 1024 * 1024;
const PROCESS_INDEX_BYTES: usize = 64 * 1024 * 1024;
const CACHE_ENTRIES: usize = 128;
// Covers the fixed Index/Arc/entry allocation and allocator bookkeeping.
// The digest vector and conservatively doubled PathBuf capacity are charged
// separately. This is index ownership accounting, not a process RSS bound.
const INDEX_OVERHEAD: usize = 512;

/// Actual synchronous local CAS read/hash work for one range request.
/// Counters include work before an integrity error, and are reset on entry.
/// They do not measure network traffic or the caller's output allocation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LocalCasRangeMeters {
    pub bytes_read: u64,
    pub whole_sha256_bytes: u64,
    pub chunk_sha256_bytes: u64,
    /// Actual successful copies into the requested output, not kernel transfer.
    pub output_append_bytes: u64,
    pub index_hit: bool,
    pub index_built: bool,
    pub strict_fallback: bool,
    /// Accounting charge of the fact used/built by this call, including
    /// digest capacity, domain path and conservative fixed overhead.
    pub index_fact_charge_bytes: usize,
}

struct Budget {
    limit: usize,
    owned: AtomicUsize,
}

impl Budget {
    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<Reservation> {
        self.owned
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |owned| {
                owned.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .ok()?;
        Some(Reservation {
            budget: Arc::clone(self),
            bytes,
        })
    }
}

// Ownership, including a builder or an evicted fact still held by a reader,
// keeps the charge until the last Arc actually releases its index.
struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.owned.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct Index {
    domain: PathBuf,
    digest: [u8; 32],
    size: u64,
    format: u32,
    chunks: Vec<[u8; 32]>,
    _reservation: Reservation,
}

impl Index {
    fn matches(&self, domain: &Path, digest: &[u8; 32], size: u64) -> bool {
        self.domain == domain
            && self.digest == *digest
            && self.size == size
            && self.format == INDEX_FORMAT
    }
}

struct Entry {
    fact: Arc<Index>,
    used: u64,
}

struct CacheState {
    entries: [Option<Entry>; CACHE_ENTRIES],
    clock: u64,
}

impl CacheState {
    fn evict_oldest(&mut self) -> bool {
        let slot = self
            .entries
            .iter()
            .enumerate()
            .filter_map(|(slot, entry)| entry.as_ref().map(|entry| (slot, entry.used)))
            .min_by_key(|(_, used)| *used)
            .map(|(slot, _)| slot);
        if let Some(slot) = slot {
            self.entries[slot] = None;
            true
        } else {
            false
        }
    }

    fn tick(&mut self) -> u64 {
        self.clock = self.clock.saturating_add(1);
        self.clock
    }
}

struct Cache {
    budget: Arc<Budget>,
    state: Mutex<CacheState>,
}

impl Cache {
    fn new(limit: usize) -> Self {
        Self {
            budget: Arc::new(Budget {
                limit,
                // The fixed slots exist even when there are no facts.
                owned: AtomicUsize::new(
                    std::mem::size_of::<Cache>() + std::mem::size_of::<CacheState>(),
                ),
            }),
            state: Mutex::new(CacheState {
                entries: std::array::from_fn(|_| None),
                clock: 0,
            }),
        }
    }

    fn get(&self, domain: &Path, digest: &[u8; 32], size: u64) -> Option<Arc<Index>> {
        let mut state = self.state.lock().ok()?;
        let used = state.tick();
        let entry = state
            .entries
            .iter_mut()
            .flatten()
            .find(|entry| entry.fact.matches(domain, digest, size))?;
        entry.used = used;
        Some(Arc::clone(&entry.fact))
    }

    fn reserve(&self, bytes: usize) -> Option<Reservation> {
        let mut state = self.state.lock().ok()?;
        loop {
            if let Some(reservation) = self.budget.reserve(bytes) {
                return Some(reservation);
            }
            // Dropping the cache's Arc does not release a reader's charge.
            if !state.evict_oldest() {
                return None;
            }
        }
    }

    fn publish(&self, fact: Arc<Index>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state
            .entries
            .iter()
            .flatten()
            .any(|entry| entry.fact.matches(&fact.domain, &fact.digest, fact.size))
        {
            // Concurrent builders remain independently budgeted until this
            // unused, fully verified fact and its last Arc are dropped.
            return;
        }
        if state.entries.iter().all(Option::is_some) {
            state.evict_oldest();
        }
        let used = state.tick();
        if let Some(slot) = state.entries.iter_mut().find(|entry| entry.is_none()) {
            *slot = Some(Entry { fact, used });
        }
    }
}

fn process_cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(|| Cache::new(PROCESS_INDEX_BYTES))
}

fn mismatch(message: &str) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::DigestMismatch, message)
}

fn io_error(error: io::Error) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::Internal, error.to_string())
}

fn open(path: &Path, size: u64) -> Result<Option<File>, SnapshotError> {
    let input = match secure_fs::open_regular(path) {
        Ok(input) => input,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
    };
    check_metadata(&input, size)?;
    Ok(Some(input))
}

fn check_metadata(input: &File, size: u64) -> Result<(), SnapshotError> {
    let metadata = input.metadata().map_err(io_error)?;
    if !metadata.is_file() || metadata.len() != size {
        return Err(mismatch(
            "local CAS object size/type differs from the fixed view",
        ));
    }
    Ok(())
}

fn output(size: u64, offset: u64, len: usize) -> Result<(Vec<u8>, u64), SnapshotError> {
    let want = size.saturating_sub(offset).min(len as u64);
    if want > super::client::MAX_BUFFERED_FILE_BYTES {
        return Err(SnapshotError::new(
            SnapshotErrorCode::LimitExceeded,
            "local CAS read exceeds the 64 MiB output budget; request a smaller range",
        ));
    }
    let want = usize::try_from(want).map_err(|_| allocation_error())?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(want)
        .map_err(|_| allocation_error())?;
    Ok((bytes, offset.saturating_add(want as u64)))
}

fn allocation_error() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::LimitExceeded,
        "local CAS range allocation failed",
    )
}

/// Both legacy Vec results and admitted owners use the same integrity core.
/// The core can only append within the one allocation supplied by its caller.
trait RangeSink {
    fn len(&self) -> usize;
    fn append(&mut self, bytes: &[u8]) -> Result<(), SnapshotError>;
}

impl RangeSink for Vec<u8> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn append(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        if bytes.len() > self.capacity() - self.len() {
            return Err(mismatch(
                "local CAS range exceeds its fixed output capacity",
            ));
        }
        self.extend_from_slice(bytes);
        Ok(())
    }
}

impl RangeSink for AccountedBuffer {
    fn len(&self) -> usize {
        AccountedBuffer::len(self)
    }
    fn append(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        AccountedBuffer::append(self, bytes)
    }
}

fn copy_intersection(
    output: &mut impl RangeSink,
    buffer: &[u8],
    start: u64,
    offset: u64,
    end: u64,
    meters: &mut LocalCasRangeMeters,
) -> Result<(), SnapshotError> {
    let from = start.max(offset);
    let to = (start + buffer.len() as u64).min(end);
    if from < to {
        output.append(&buffer[(from - start) as usize..(to - start) as usize])?;
        meters.output_append_bytes += to - from;
    }
    Ok(())
}

// The only constructor of trusted facts is this full successful scan. Whole
// and chunk digests consume the exact buffers that supply returned bytes.
#[allow(clippy::too_many_arguments)]
fn scan(
    mut input: File,
    digest: &str,
    size: u64,
    offset: u64,
    end: u64,
    output: &mut impl RangeSink,
    scratch: &mut [u8],
    chunks: Option<&mut Vec<[u8; 32]>>,
    meters: &mut LocalCasRangeMeters,
) -> Result<(), SnapshotError> {
    scan_body(
        &mut input, digest, size, offset, end, output, scratch, chunks, meters,
    )?;
    check_metadata(&input, size)
}

#[allow(clippy::too_many_arguments)]
fn scan_body(
    input: &mut impl Read,
    digest: &str,
    size: u64,
    offset: u64,
    end: u64,
    output: &mut impl RangeSink,
    scratch: &mut [u8],
    mut chunks: Option<&mut Vec<[u8; 32]>>,
    meters: &mut LocalCasRangeMeters,
) -> Result<(), SnapshotError> {
    let mut whole = Context::new(&SHA256);
    let mut chunk = Context::new(&SHA256);
    let mut chunk_bytes = 0u64;
    let scan_capacity = scratch.len().min(64 * 1024);
    if scan_capacity == 0 {
        return Err(allocation_error());
    }
    let buffer = &mut scratch[..scan_capacity];
    let mut read = 0u64;
    {
        let mut bounded = input.take(size.saturating_add(1));
        loop {
            let count = read_retry(&mut bounded, buffer)?;
            meters.bytes_read += count as u64;
            if count == 0 {
                break;
            }
            let next = read + count as u64;
            if next > size {
                return Err(mismatch("local CAS object grew past its fixed size"));
            }
            whole.update(&buffer[..count]);
            meters.whole_sha256_bytes += count as u64;
            if let Some(chunks) = chunks.as_mut() {
                // read() can return any length, including a short read that
                // crosses a chunk boundary on the next iteration.
                let mut consumed = 0usize;
                while consumed < count {
                    let take = (CHUNK_SIZE - chunk_bytes).min((count - consumed) as u64) as usize;
                    chunk.update(&buffer[consumed..consumed + take]);
                    meters.chunk_sha256_bytes += take as u64;
                    consumed += take;
                    chunk_bytes += take as u64;
                    if chunk_bytes == CHUNK_SIZE {
                        chunks.push(chunk.finish().as_ref().try_into().expect("SHA256 length"));
                        chunk = Context::new(&SHA256);
                        chunk_bytes = 0;
                    }
                }
            }
            copy_intersection(output, &buffer[..count], read, offset, end, meters)?;
            read = next;
        }
    }
    if let Some(chunks) = chunks.as_mut() {
        if chunk_bytes != 0 {
            chunks.push(chunk.finish().as_ref().try_into().expect("SHA256 length"));
        }
    }
    if read != size
        || output.len() as u64 != end.saturating_sub(offset)
        || format!("sha256:{}", hex::encode(whole.finish().as_ref())) != digest
    {
        return Err(mismatch(
            "local CAS object does not match the fixed whole-file identity",
        ));
    }
    Ok(())
}

fn read_retry(input: &mut impl Read, buffer: &mut [u8]) -> Result<usize, SnapshotError> {
    loop {
        match input.read(buffer) {
            Ok(count) => return Ok(count),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(io_error(error)),
        }
    }
}

pub(super) fn read_strict(
    path: &Path,
    digest: &str,
    size: u64,
    offset: u64,
    len: usize,
    meters: &mut LocalCasRangeMeters,
) -> Result<Option<Vec<u8>>, SnapshotError> {
    let Some(input) = open(path, size)? else {
        return Ok(None);
    };
    let (mut output, end) = output(size, offset, len)?;
    scan_legacy(input, digest, size, offset, end, &mut output, None, meters)?;
    Ok(Some(output))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn read_indexed(
    path: &Path,
    domain: &Path,
    digest: &str,
    size: u64,
    offset: u64,
    len: usize,
    meters: &mut LocalCasRangeMeters,
) -> Result<Option<Vec<u8>>, SnapshotError> {
    read_with_cache(
        process_cache(),
        path,
        domain,
        digest,
        size,
        offset,
        len,
        meters,
    )
}

#[allow(clippy::too_many_arguments)]
fn read_with_cache(
    cache: &Cache,
    path: &Path,
    domain: &Path,
    digest: &str,
    size: u64,
    offset: u64,
    len: usize,
    meters: &mut LocalCasRangeMeters,
) -> Result<Option<Vec<u8>>, SnapshotError> {
    let Some(input) = open(path, size)? else {
        return Ok(None);
    };
    let (mut output, end) = output(size, offset, len)?;
    read_input_with_cache(
        cache,
        input,
        domain,
        digest,
        size,
        offset,
        end,
        &mut output,
        None,
        meters,
    )?;
    Ok(Some(output))
}

/// Fill the caller's already admitted output and construction allocations.
/// Only a safe CAS open returning NotFound is a miss; all other errors are
/// terminal. No local payload or scratch allocation is made in this entry.
#[allow(clippy::too_many_arguments)]
pub(super) fn read_indexed_into(
    path: &Path,
    domain: &Path,
    digest: &str,
    size: u64,
    offset: u64,
    wanted: usize,
    output: &mut AccountedBuffer,
    scratch: &mut [u8],
    meters: &mut LocalCasRangeMeters,
) -> Result<bool, SnapshotError> {
    let input = match secure_fs::open_regular_nonblocking(path) {
        Ok(input) => input,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(io_error(error)),
    };
    check_metadata(&input, size)?;
    read_input_with_cache(
        process_cache(),
        input,
        domain,
        digest,
        size,
        offset,
        offset.saturating_add(wanted as u64),
        output,
        Some(scratch),
        meters,
    )?;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn read_input_with_cache(
    cache: &Cache,
    input: File,
    domain: &Path,
    digest: &str,
    size: u64,
    offset: u64,
    end: u64,
    output: &mut impl RangeSink,
    scratch: Option<&mut [u8]>,
    meters: &mut LocalCasRangeMeters,
) -> Result<(), SnapshotError> {
    let count = size.div_ceil(CHUNK_SIZE);
    let fixed_digest: Option<[u8; 32]> = digest
        .strip_prefix("sha256:")
        .filter(|hex| {
            hex.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .and_then(|hex| hex::decode(hex).ok()?.try_into().ok());
    let domain = std::fs::canonicalize(domain);
    // More than 256 GiB, a noncanonical identity, or an unavailable domain
    // retains the independent whole-file contract. No index is fabricated.
    let (Ok(domain), Some(fixed_digest)) = (domain, fixed_digest) else {
        meters.strict_fallback = true;
        return scan_with_scratch(
            input, digest, size, offset, end, output, scratch, None, meters,
        );
    };
    if count > (MAX_DIGEST_BYTES / 32) as u64 {
        meters.strict_fallback = true;
        return scan_with_scratch(
            input, digest, size, offset, end, output, scratch, None, meters,
        );
    }
    if let Some(fact) = cache.get(&domain, &fixed_digest, size) {
        meters.index_hit = true;
        meters.index_fact_charge_bytes = fact._reservation.bytes;
        return read_chunks_with_scratch(input, &fact, offset, end, output, scratch, meters);
    }
    let count = count as usize;
    // Reserve a conservative payload allowance before allocating. Global
    // budget includes simultaneous builders and actively held evicted facts.
    let payload = (count * 32)
        .checked_next_power_of_two()
        .unwrap_or(MAX_DIGEST_BYTES)
        .min(MAX_DIGEST_BYTES);
    // Compact the path and charge its actual platform storage capacity
    // conservatively for either bytes or Windows wide character units.
    let domain = domain.into_boxed_path().into_path_buf();
    let path_bytes = domain.capacity().saturating_mul(2);
    let charge = payload
        .saturating_add(path_bytes)
        .saturating_add(INDEX_OVERHEAD);
    let Some(reservation) = cache.reserve(charge) else {
        meters.strict_fallback = true;
        return scan_with_scratch(
            input, digest, size, offset, end, output, scratch, None, meters,
        );
    };
    let mut chunks = Vec::new();
    if chunks.try_reserve_exact(count).is_err() || chunks.capacity().saturating_mul(32) > payload {
        drop(chunks);
        drop(reservation);
        meters.strict_fallback = true;
        return scan_with_scratch(
            input, digest, size, offset, end, output, scratch, None, meters,
        );
    }
    scan_with_scratch(
        input,
        digest,
        size,
        offset,
        end,
        output,
        scratch,
        Some(&mut chunks),
        meters,
    )?;
    if chunks.len() != count {
        return Err(mismatch(
            "local CAS chunk geometry differs from the fixed file",
        ));
    }
    meters.index_fact_charge_bytes = reservation.bytes;
    cache.publish(Arc::new(Index {
        domain,
        digest: fixed_digest,
        size,
        format: INDEX_FORMAT,
        chunks,
        _reservation: reservation,
    }));
    meters.index_built = true;
    Ok(())
}

// Legacy public Vec calls keep their existing cold stack scratch and warm
// heap scratch. Owned calls always provide their already admitted allocation.
#[allow(clippy::too_many_arguments)]
fn scan_with_scratch(
    input: File,
    digest: &str,
    size: u64,
    offset: u64,
    end: u64,
    output: &mut impl RangeSink,
    scratch: Option<&mut [u8]>,
    chunks: Option<&mut Vec<[u8; 32]>>,
    meters: &mut LocalCasRangeMeters,
) -> Result<(), SnapshotError> {
    match scratch {
        Some(scratch) => scan(
            input, digest, size, offset, end, output, scratch, chunks, meters,
        ),
        None => scan_legacy(input, digest, size, offset, end, output, chunks, meters),
    }
}

// Keep the legacy stack scratch out of the admitted caller's stack frame.
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn scan_legacy(
    input: File,
    digest: &str,
    size: u64,
    offset: u64,
    end: u64,
    output: &mut impl RangeSink,
    chunks: Option<&mut Vec<[u8; 32]>>,
    meters: &mut LocalCasRangeMeters,
) -> Result<(), SnapshotError> {
    let mut scratch = [0u8; 64 * 1024];
    scan(
        input,
        digest,
        size,
        offset,
        end,
        output,
        &mut scratch,
        chunks,
        meters,
    )
}

#[allow(clippy::too_many_arguments)]
fn read_chunks_with_scratch(
    input: File,
    fact: &Index,
    offset: u64,
    end: u64,
    output: &mut impl RangeSink,
    scratch: Option<&mut [u8]>,
    meters: &mut LocalCasRangeMeters,
) -> Result<(), SnapshotError> {
    match scratch {
        Some(scratch) => read_chunks(input, fact, offset, end, output, scratch, meters),
        None => {
            let mut scratch = Vec::new();
            if end > offset {
                scratch
                    .try_reserve_exact(CHUNK_SIZE as usize)
                    .map_err(|_| allocation_error())?;
                scratch.resize(CHUNK_SIZE as usize, 0);
            }
            read_chunks(input, fact, offset, end, output, &mut scratch, meters)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn read_chunks(
    mut input: File,
    fact: &Index,
    offset: u64,
    end: u64,
    output: &mut impl RangeSink,
    scratch: &mut [u8],
    meters: &mut LocalCasRangeMeters,
) -> Result<(), SnapshotError> {
    if end > offset {
        for index in offset / CHUNK_SIZE..=(end - 1) / CHUNK_SIZE {
            let start = index * CHUNK_SIZE;
            let chunk_len = (fact.size - start).min(CHUNK_SIZE) as usize;
            let buffer = scratch.get_mut(..chunk_len).ok_or_else(allocation_error)?;
            input.seek(SeekFrom::Start(start)).map_err(io_error)?;
            let mut filled = 0;
            while filled < chunk_len {
                let count = read_retry(&mut input, &mut buffer[filled..])?;
                meters.bytes_read += count as u64;
                if count == 0 {
                    return Err(mismatch("local CAS covering chunk is truncated"));
                }
                filled += count;
            }
            let hash = ring::digest::digest(&SHA256, buffer);
            meters.chunk_sha256_bytes += chunk_len as u64;
            if hash.as_ref() != fact.chunks[index as usize] {
                return Err(mismatch(
                    "local CAS covering chunk does not match its verified digest",
                ));
            }
            copy_intersection(output, buffer, start, offset, end, meters)?;
        }
    }
    if output.len() as u64 != end.saturating_sub(offset) {
        return Err(mismatch(
            "local CAS range differs from its fixed output length",
        ));
    }
    check_metadata(&input, fact.size)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (tempfile::TempDir, PathBuf, String, Vec<u8>) {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("blob");
        let body = vec![0x51; 2 * CHUNK_SIZE as usize + 7];
        let digest = super::super::durable::digest_of(&body);
        std::fs::write(&path, &body).unwrap();
        (temp, path, digest, body)
    }

    fn read(
        cache: &Cache,
        path: &Path,
        digest: &str,
        size: u64,
        meters: &mut LocalCasRangeMeters,
    ) -> Result<Option<Vec<u8>>, SnapshotError> {
        *meters = LocalCasRangeMeters::default();
        read_with_cache(
            cache,
            path,
            path.parent().unwrap(),
            digest,
            size,
            0,
            13,
            meters,
        )
    }

    #[test]
    fn evicted_active_fact_keeps_its_budget_until_last_arc_drops() {
        let (temp, path, digest, body) = fixture();
        let probe = Cache::new(PROCESS_INDEX_BYTES);
        let base = probe.budget.owned.load(Ordering::Acquire);
        let mut meters = LocalCasRangeMeters::default();
        read(&probe, &path, &digest, body.len() as u64, &mut meters).unwrap();
        let charge = probe.budget.owned.load(Ordering::Acquire) - base;
        drop(probe);
        let cache = Cache::new(base + charge);
        read(&cache, &path, &digest, body.len() as u64, &mut meters).unwrap();
        let domain = std::fs::canonicalize(temp.path()).unwrap();
        let fixed: [u8; 32] = hex::decode(digest.strip_prefix("sha256:").unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let active = cache.get(&domain, &fixed, body.len() as u64).unwrap();
        assert!(cache.state.lock().unwrap().evict_oldest());
        assert_eq!(cache.budget.owned.load(Ordering::Acquire), base + charge);
        let second = temp.path().join("second");
        let second_body = b"a second valid file";
        let second_digest = super::super::durable::digest_of(second_body);
        std::fs::write(&second, second_body).unwrap();
        assert_eq!(
            read(
                &cache,
                &second,
                &second_digest,
                second_body.len() as u64,
                &mut meters
            )
            .unwrap()
            .unwrap(),
            second_body[..13]
        );
        assert!(meters.strict_fallback && !meters.index_built);
        assert_eq!(meters.bytes_read, second_body.len() as u64);
        assert_eq!(meters.whole_sha256_bytes, second_body.len() as u64);
        assert_eq!(meters.chunk_sha256_bytes, 0);
        assert_eq!(cache.budget.owned.load(Ordering::Acquire), base + charge);
        drop(active);
        assert_eq!(cache.budget.owned.load(Ordering::Acquire), base);
        read(
            &cache,
            &second,
            &second_digest,
            second_body.len() as u64,
            &mut meters,
        )
        .unwrap();
        assert!(meters.index_built && !meters.strict_fallback);
        assert!(cache.budget.owned.load(Ordering::Acquire) <= cache.budget.limit);
    }

    #[test]
    fn simultaneous_build_reservations_are_bounded_and_abandoned_work_releases_them() {
        use std::sync::Barrier;
        let (temp, path, digest, body) = fixture();
        let probe = Cache::new(PROCESS_INDEX_BYTES);
        let base = probe.budget.owned.load(Ordering::Acquire);
        let mut meters = LocalCasRangeMeters::default();
        read(&probe, &path, &digest, body.len() as u64, &mut meters).unwrap();
        let charge = probe.budget.owned.load(Ordering::Acquire) - base;
        drop(probe);
        let cache = Arc::new(Cache::new(base + 2 * charge));
        let admitted = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(Barrier::new(7));
        let release = Arc::new(Barrier::new(7));
        std::thread::scope(|scope| {
            for _ in 0..6 {
                let cache = Arc::clone(&cache);
                let admitted = Arc::clone(&admitted);
                let ready = Arc::clone(&ready);
                let release = Arc::clone(&release);
                scope.spawn(move || {
                    let reservation = cache.reserve(charge);
                    if reservation.is_some() {
                        admitted.fetch_add(1, Ordering::AcqRel);
                    }
                    ready.wait();
                    release.wait();
                    // Abandoned builders publish no fact and release only
                    // when their actual ownership ends.
                    drop(reservation);
                });
            }
            ready.wait();
            assert_eq!(admitted.load(Ordering::Acquire), 2);
            assert_eq!(
                cache.budget.owned.load(Ordering::Acquire),
                base + 2 * charge
            );
            assert_eq!(
                read(&cache, &path, &digest, body.len() as u64, &mut meters)
                    .unwrap()
                    .unwrap(),
                body[..13]
            );
            assert!(meters.strict_fallback);
            assert_eq!(meters.whole_sha256_bytes, body.len() as u64);
            release.wait();
        });
        assert_eq!(cache.budget.owned.load(Ordering::Acquire), base);
        assert!(cache
            .state
            .lock()
            .unwrap()
            .entries
            .iter()
            .all(Option::is_none));
        read(&cache, &path, &digest, body.len() as u64, &mut meters).unwrap();
        assert!(meters.index_built);
        drop(temp);
    }

    #[test]
    fn failed_whole_proof_releases_build_ownership_and_publishes_nothing() {
        let (_temp, path, digest, body) = fixture();
        let cache = Cache::new(PROCESS_INDEX_BYTES);
        let base = cache.budget.owned.load(Ordering::Acquire);
        let mut corrupt = body.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        std::fs::write(&path, corrupt).unwrap();
        let mut meters = LocalCasRangeMeters::default();
        assert_eq!(
            read(&cache, &path, &digest, body.len() as u64, &mut meters)
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch
        );
        assert_eq!(cache.budget.owned.load(Ordering::Acquire), base);
        assert!(cache
            .state
            .lock()
            .unwrap()
            .entries
            .iter()
            .all(Option::is_none));
        std::fs::write(path.as_path(), &body).unwrap();
        read(&cache, &path, &digest, body.len() as u64, &mut meters).unwrap();
        assert!(meters.index_built);
    }

    #[test]
    fn entry_lru_has_a_fixed_bound_and_eviction_can_release_inactive_facts() {
        let temp = tempfile::tempdir().unwrap();
        let cache = Cache::new(PROCESS_INDEX_BYTES);
        let base = cache.budget.owned.load(Ordering::Acquire);
        let mut meters = LocalCasRangeMeters::default();
        for index in 0..CACHE_ENTRIES + 2 {
            let path = temp.path().join(index.to_string());
            let body = index.to_le_bytes();
            let digest = super::super::durable::digest_of(&body);
            std::fs::write(&path, body).unwrap();
            read(&cache, &path, &digest, body.len() as u64, &mut meters).unwrap();
            assert!(cache.budget.owned.load(Ordering::Acquire) <= cache.budget.limit);
        }
        let mut state = cache.state.lock().unwrap();
        assert_eq!(state.entries.iter().flatten().count(), CACHE_ENTRIES);
        while state.evict_oldest() {}
        assert_eq!(cache.budget.owned.load(Ordering::Acquire), base);
    }

    #[test]
    fn unavailable_canonical_domain_retains_strict_proof_and_publishes_no_fact() {
        let (temp, path, digest, body) = fixture();
        let cache = Cache::new(PROCESS_INDEX_BYTES);
        let base = cache.budget.owned.load(Ordering::Acquire);
        let unavailable = temp.path().join("missing-domain");
        let mut meters = LocalCasRangeMeters::default();
        assert_eq!(
            read_with_cache(
                &cache,
                &path,
                &unavailable,
                &digest,
                body.len() as u64,
                0,
                13,
                &mut meters
            )
            .unwrap()
            .unwrap(),
            body[..13]
        );
        assert!(meters.strict_fallback && !meters.index_built && !meters.index_hit);
        assert_eq!(meters.whole_sha256_bytes, body.len() as u64);
        assert_eq!(cache.budget.owned.load(Ordering::Acquire), base);
        let mut corrupt = body.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        std::fs::write(&path, corrupt).unwrap();
        assert_eq!(
            read_with_cache(
                &cache,
                &path,
                &unavailable,
                &digest,
                body.len() as u64,
                0,
                13,
                &mut LocalCasRangeMeters::default()
            )
            .unwrap_err()
            .code,
            SnapshotErrorCode::DigestMismatch
        );
        assert!(cache
            .state
            .lock()
            .unwrap()
            .entries
            .iter()
            .all(Option::is_none));
    }

    #[test]
    fn strict_fallback_fills_the_same_admitted_output_without_a_payload_vec_or_fact() {
        use super::super::content::{BudgetClass, ContentBudget, ContentBudgetLimits};

        let (temp, path, digest, body) = fixture();
        for missing_domain in [false, true] {
            let cache = Cache::new(if missing_domain {
                PROCESS_INDEX_BYTES
            } else {
                0
            });
            let domain = if missing_domain {
                temp.path().join("not-a-domain")
            } else {
                temp.path().to_path_buf()
            };
            let budget = ContentBudget::new(ContentBudgetLimits::default());
            let baseline = budget.usage();
            let mut meters = LocalCasRangeMeters::default();
            let mut output = AccountedBuffer::new(&budget, BudgetClass::Output, 13, 0).unwrap();
            let pointer = output.as_bytes().as_ptr();
            // The core sees caller-owned, fixed construction storage.
            let scratch_credit = budget
                .reserve(BudgetClass::Construction, CHUNK_SIZE as usize + 1024)
                .unwrap();
            let mut scratch = vec![0u8; CHUNK_SIZE as usize];
            read_input_with_cache(
                &cache,
                open(&path, body.len() as u64).unwrap().unwrap(),
                &domain,
                &digest,
                body.len() as u64,
                0,
                13,
                &mut output,
                Some(&mut scratch),
                &mut meters,
            )
            .unwrap();
            assert_eq!(output.as_bytes().as_ptr(), pointer);
            assert_eq!(output.as_bytes(), &body[..13]);
            assert!(meters.strict_fallback && !meters.index_built && !meters.index_hit);
            assert_eq!(meters.bytes_read, body.len() as u64);
            assert_eq!(meters.whole_sha256_bytes, body.len() as u64);
            assert_eq!(meters.chunk_sha256_bytes, 0);
            assert!(cache
                .state
                .lock()
                .unwrap()
                .entries
                .iter()
                .all(Option::is_none));
            drop(output);
            let mut corrupt = body.clone();
            *corrupt.last_mut().unwrap() ^= 1;
            std::fs::write(&path, &corrupt).unwrap();
            let mut output = AccountedBuffer::new(&budget, BudgetClass::Output, 13, 0).unwrap();
            assert_eq!(
                read_input_with_cache(
                    &cache,
                    open(&path, body.len() as u64).unwrap().unwrap(),
                    &domain,
                    &digest,
                    body.len() as u64,
                    0,
                    13,
                    &mut output,
                    Some(&mut scratch),
                    &mut meters,
                )
                .unwrap_err()
                .code,
                SnapshotErrorCode::DigestMismatch
            );
            drop(output);
            drop(scratch);
            drop(scratch_credit);
            assert_eq!(budget.usage(), baseline);
            assert!(cache
                .state
                .lock()
                .unwrap()
                .entries
                .iter()
                .all(Option::is_none));
            std::fs::write(&path, &body).unwrap();
        }
    }

    #[test]
    fn shared_scan_retries_interrupted_short_reads_across_chunk_boundaries() {
        struct ShortReads {
            interrupted: bool,
            input: io::Cursor<Vec<u8>>,
        }
        impl Read for ShortReads {
            fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(io::ErrorKind::Interrupted.into());
                }
                let capacity = output.len().min(63 * 1024);
                self.input.read(&mut output[..capacity])
            }
        }
        let body = vec![0x35; CHUNK_SIZE as usize + 7];
        let digest = super::super::durable::digest_of(&body);
        let mut input = ShortReads {
            interrupted: false,
            input: io::Cursor::new(body.clone()),
        };
        let mut scratch = [0u8; 64 * 1024];
        let (mut output, end) = output(body.len() as u64, CHUNK_SIZE - 2, 9).unwrap();
        let mut chunks = Vec::with_capacity(2);
        let mut meters = LocalCasRangeMeters::default();
        scan_body(
            &mut input,
            &digest,
            body.len() as u64,
            CHUNK_SIZE - 2,
            end,
            &mut output,
            &mut scratch,
            Some(&mut chunks),
            &mut meters,
        )
        .unwrap();
        assert!(input.interrupted);
        assert_eq!(output, vec![0x35; 9]);
        assert_eq!(meters.bytes_read, body.len() as u64);
        assert_eq!(meters.whole_sha256_bytes, body.len() as u64);
        assert_eq!(meters.chunk_sha256_bytes, body.len() as u64);
        assert_eq!(chunks.len(), 2);
        for (chunk, bytes) in chunks.iter().zip(body.chunks(CHUNK_SIZE as usize)) {
            assert_eq!(
                chunk.as_slice(),
                ring::digest::digest(&SHA256, bytes).as_ref()
            );
        }
    }

    #[test]
    fn shared_scan_detects_growth_with_one_sentinel_and_truncation_before_publication() {
        use super::super::content::{BudgetClass, ContentBudget, ContentBudgetLimits};

        let budget = ContentBudget::new(ContentBudgetLimits::default());
        let baseline = budget.usage();
        let mut scratch = [0u8; 64 * 1024];
        let mut meters = LocalCasRangeMeters::default();
        let mut input = io::Cursor::new(b"grew well beyond the fixed view".as_slice());
        let mut output = AccountedBuffer::new(&budget, BudgetClass::Output, 1, 0).unwrap();
        assert_eq!(
            scan_body(
                &mut input,
                &super::super::durable::digest_of(b"gre"),
                3,
                0,
                1,
                &mut output,
                &mut scratch,
                None,
                &mut meters,
            )
            .unwrap_err()
            .code,
            SnapshotErrorCode::DigestMismatch
        );
        assert_eq!(input.position(), 4);
        assert_eq!(meters.bytes_read, 4);
        assert_eq!(output.len(), 0);
        drop(output);
        let mut input = io::Cursor::new(b"short".as_slice());
        let mut output = AccountedBuffer::new(&budget, BudgetClass::Output, 1, 0).unwrap();
        assert_eq!(
            scan_body(
                &mut input,
                &super::super::durable::digest_of(b"trusted"),
                7,
                0,
                1,
                &mut output,
                &mut scratch,
                None,
                &mut meters,
            )
            .unwrap_err()
            .code,
            SnapshotErrorCode::DigestMismatch
        );
        drop(output);
        assert_eq!(budget.usage(), baseline);
    }
}
