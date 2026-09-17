//! Live-stack e2e for range reads (link Phase B-1, spec 07 §6).
//!
//! Reading a small slice of a large file must transfer only the chunks that
//! cover it — never the whole file — and every returned byte must be the
//! file's byte at that offset, verified against the chunk's authenticated
//! leaf digest.
//!
//! Ignored by default; requires the running stack with the t08 fixture
//! (`t08/chunked.bin`, 2 MiB + 7 bytes, byte i = (73i+11) % 256).

use scorpiofs::snapshot::{ChunkedFile, Mst2Client, SnapshotReader};

const CHUNK: u64 = 1 << 20;

fn pattern_at(i: usize) -> u8 {
    ((73u64 * i as u64 + 11) % 256) as u8
}

#[tokio::test]
#[ignore]
async fn range_read_transfers_only_the_covering_chunks() {
    let base =
        std::env::var("MST2_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:19700".to_string());
    let client = Mst2Client::new(base);
    let reader = SnapshotReader::resolve(client.clone(), "/project", 600)
        .await
        .expect("resolve");
    let manifest = reader.file_manifest().await.expect("manifest");
    let large = manifest
        .iter()
        .find(|f| f.rel_path == "t08/chunked.bin")
        .expect("t08/chunked.bin seeded by the t08 oracle")
        .clone();
    assert!(large.size > 2 * CHUNK, "fixture must exceed two chunks");

    let file = ChunkedFile::open(
        &reader,
        "/t08/chunked.bin",
        &large.content_digest,
        large.size,
    )
    .await
    .expect("chunk map");

    // A 4 KiB read in the middle of chunk 1 fetches exactly one chunk —
    // counted in units, because the fixture's periodic byte pattern makes
    // wire bytes a poor proxy once zstd is negotiated.
    let offset = CHUNK + 4096;
    let before_units = client.units_fetched();
    let before_bytes = client.received_bytes();
    let bytes = file.read_range(offset, 4096).await.expect("range read");
    assert_eq!(bytes.len(), 4096);
    for (i, b) in bytes.iter().enumerate() {
        assert_eq!(*b, pattern_at(offset as usize + i), "byte {i} mismatch");
    }
    assert_eq!(
        client.units_fetched() - before_units,
        1,
        "a 4 KiB read must fetch one chunk, not the file"
    );
    assert!(
        client.received_bytes() - before_bytes < large.size,
        "a 4 KiB read must not transfer the whole {} byte file",
        large.size
    );

    // The chunk is cached: the same range again costs nothing.
    let before = client.units_fetched();
    let again = file.read_range(offset, 4096).await.expect("cached read");
    assert_eq!(again, bytes);
    assert_eq!(client.units_fetched() - before, 0, "cached chunk refetched");

    // Crossing a boundary fetches exactly the one chunk that was still
    // missing (chunk 1 is cached by the read above).
    let offset = CHUNK - 2048;
    let before = client.units_fetched();
    let bytes = file.read_range(offset, 4096).await.expect("boundary read");
    assert_eq!(bytes.len(), 4096);
    for (i, b) in bytes.iter().enumerate() {
        assert_eq!(*b, pattern_at(offset as usize + i));
    }
    assert_eq!(
        client.units_fetched() - before,
        1,
        "a boundary read must fetch exactly the missing chunk"
    );

    // The last (short) chunk is served exactly; a read past EOF clamps.
    let tail_off = large.size - 7;
    let tail = file.read_range(tail_off, 4096).await.expect("tail read");
    assert_eq!(tail.len(), 7);
    let past = file.read_range(large.size, 4096).await.expect("past EOF");
    assert!(past.is_empty());
    assert!(file.read_range(0, 0).await.unwrap().is_empty());
}
