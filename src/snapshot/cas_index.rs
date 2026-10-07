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

use super::{secure_fs, SnapshotError, SnapshotErrorCode};

const CHUNK_SIZE: u64 = 1024 * 1024;
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

fn copy_intersection(output: &mut Vec<u8>, buffer: &[u8], start: u64, offset: u64, end: u64) {
    let from = start.max(offset);
    let to = (start + buffer.len() as u64).min(end);
    if from < to {
        output.extend_from_slice(&buffer[(from - start) as usize..(to - start) as usize]);
    }
}

// The only constructor of trusted facts is this full successful scan. Whole
// and chunk digests consume the exact buffers that supply returned bytes.
fn scan(
    mut input: File,
    digest: &str,
    size: u64,
    offset: u64,
    len: usize,
    mut chunks: Option<&mut Vec<[u8; 32]>>,
    meters: &mut LocalCasRangeMeters,
) -> Result<Vec<u8>, SnapshotError> {
    let (mut output, end) = output(size, offset, len)?;
    let mut whole = Context::new(&SHA256);
    let mut chunk = Context::new(&SHA256);
    let mut chunk_bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    let mut read = 0u64;
    {
        let mut bounded = (&mut input).take(size.saturating_add(1));
        loop {
            let count = bounded.read(&mut buffer).map_err(io_error)?;
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
            copy_intersection(&mut output, &buffer[..count], read, offset, end);
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
    check_metadata(&input, size)?;
    Ok(output)
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
    scan(input, digest, size, offset, len, None, meters).map(Some)
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
        return scan(input, digest, size, offset, len, None, meters).map(Some);
    };
    if count > (MAX_DIGEST_BYTES / 32) as u64 {
        meters.strict_fallback = true;
        return scan(input, digest, size, offset, len, None, meters).map(Some);
    }
    if let Some(fact) = cache.get(&domain, &fixed_digest, size) {
        meters.index_hit = true;
        meters.index_fact_charge_bytes = fact._reservation.bytes;
        return read_chunks(input, &fact, offset, len, meters).map(Some);
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
        return scan(input, digest, size, offset, len, None, meters).map(Some);
    };
    let mut chunks = Vec::new();
    if chunks.try_reserve_exact(count).is_err() || chunks.capacity().saturating_mul(32) > payload {
        drop(chunks);
        drop(reservation);
        meters.strict_fallback = true;
        return scan(input, digest, size, offset, len, None, meters).map(Some);
    }
    let bytes = scan(input, digest, size, offset, len, Some(&mut chunks), meters)?;
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
    Ok(Some(bytes))
}

fn read_chunks(
    mut input: File,
    fact: &Index,
    offset: u64,
    len: usize,
    meters: &mut LocalCasRangeMeters,
) -> Result<Vec<u8>, SnapshotError> {
    let (mut output, end) = output(fact.size, offset, len)?;
    if end > offset {
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(CHUNK_SIZE as usize)
            .map_err(|_| allocation_error())?;
        for index in offset / CHUNK_SIZE..=(end - 1) / CHUNK_SIZE {
            let start = index * CHUNK_SIZE;
            let chunk_len = (fact.size - start).min(CHUNK_SIZE) as usize;
            buffer.resize(chunk_len, 0);
            input.seek(SeekFrom::Start(start)).map_err(io_error)?;
            let mut filled = 0;
            while filled < chunk_len {
                let count = input.read(&mut buffer[filled..]).map_err(io_error)?;
                meters.bytes_read += count as u64;
                if count == 0 {
                    return Err(mismatch("local CAS covering chunk is truncated"));
                }
                filled += count;
            }
            let hash = ring::digest::digest(&SHA256, &buffer);
            meters.chunk_sha256_bytes += chunk_len as u64;
            if hash.as_ref() != fact.chunks[index as usize] {
                return Err(mismatch(
                    "local CAS covering chunk does not match its verified digest",
                ));
            }
            copy_intersection(&mut output, &buffer, start, offset, end);
        }
    }
    check_metadata(&input, fact.size)?;
    Ok(output)
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
}
