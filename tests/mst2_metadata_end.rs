//! Request-bound META acceptance through the real HTTP client, using only
//! ephemeral loopback fixtures and the existing registry codec.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{body::Bytes, extract::State, response::Response, routing::post, Router};
use mst2_codec::{
    metapage::{page_id, Entry, EntryKind, Page},
    treeframe::{ChunkPayload, EndPayload, ErrorPayload, MetaPayload, ObjectPayload},
};
use scorpiofs::snapshot::{
    frames::{hex32, MetadataPageItem},
    Mst2Client, SnapshotError, SnapshotErrorCode,
};
use serde_json::{json, Value};

type Pages = Vec<([u8; 32], Vec<u8>)>;

#[derive(Clone, Copy, Debug)]
enum ResponseCase {
    Valid,
    WrongItems,
    WrongBodyHash,
    WrongUnits,
    WrongBytes,
    DuplicatePage,
    MissingPage,
    MissingWitness,
    ExtraPage,
    ObjectFrame,
    ChunkFrame,
    ErrorBeforePages,
    ErrorAfterPages,
}

struct ResponseFixture {
    case: ResponseCase,
    expected_request: Value,
    pages: Pages,
    requests: Arc<AtomicUsize>,
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}

async fn metadata_response(State(fixture): State<Arc<ResponseFixture>>, body: Bytes) -> Response {
    fixture.requests.fetch_add(1, Ordering::SeqCst);
    let request: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(request, fixture.expected_request);
    let stream_id = 19;
    let mut sequence = 0;
    let mut wire = Vec::new();
    let mut response_pages = fixture.pages.clone();
    match fixture.case {
        ResponseCase::MissingPage => {
            response_pages.pop();
        }
        ResponseCase::MissingWitness => {
            response_pages.remove(0);
        }
        ResponseCase::ExtraPage => {
            let page = Page::build(&[Entry::file(
                EntryKind::Regular,
                b"unrequested",
                1,
                [0x55; 32],
            )])
            .unwrap();
            response_pages.push((page_id(&page), page));
        }
        _ => {}
    }
    if !matches!(fixture.case, ResponseCase::ErrorBeforePages) {
        // One page per frame exercises stream-wide rather than per-frame checks.
        for page in &response_pages {
            wire.extend(
                MetaPayload {
                    pages: vec![page.clone()],
                }
                .encode(stream_id, sequence)
                .unwrap(),
            );
            sequence += 1;
        }
    }
    match fixture.case {
        ResponseCase::DuplicatePage => {
            wire.extend(
                MetaPayload {
                    pages: vec![fixture.pages[0].clone()],
                }
                .encode(stream_id, sequence)
                .unwrap(),
            );
            sequence += 1;
        }
        ResponseCase::ObjectFrame => {
            let data = b"unrequested object".to_vec();
            wire.extend(
                ObjectPayload {
                    objects: vec![(sha256(&data), data)],
                }
                .encode(stream_id, sequence)
                .unwrap(),
            );
            sequence += 1;
        }
        ResponseCase::ChunkFrame => {
            wire.extend(
                ChunkPayload {
                    map_id: [0x88; 32],
                    file_content_id: [0x99; 32],
                    chunk_index: 0,
                    chunk_bytes: b"unrequested chunk".to_vec(),
                }
                .encode(stream_id, sequence)
                .unwrap(),
            );
            sequence += 1;
        }
        _ => {}
    }
    if matches!(
        fixture.case,
        ResponseCase::ErrorBeforePages | ResponseCase::ErrorAfterPages
    ) {
        wire.extend(
            ErrorPayload {
                code: "TEMPORARY_UNAVAILABLE".into(),
                retryable: true,
                request_id: "metadata-error-fixture".into(),
            }
            .encode(stream_id, sequence)
            .unwrap(),
        );
    } else {
        // Compute the binding from the bytes the server actually received,
        // independently of the client's local serialization and checker.
        let mut end = EndPayload {
            request_item_count: request["items"]
                .as_array()
                .unwrap()
                .len()
                .try_into()
                .unwrap(),
            unique_unit_count: response_pages.len().try_into().unwrap(),
            logical_bytes: response_pages
                .iter()
                .map(|(_, page)| page.len() as u64)
                .sum(),
            request_body_sha256: sha256(&body),
        };
        match fixture.case {
            ResponseCase::WrongItems => end.request_item_count += 1,
            ResponseCase::WrongBodyHash => {
                end.request_body_sha256[0] ^= 1;
            }
            ResponseCase::WrongUnits => end.unique_unit_count += 1,
            ResponseCase::WrongBytes => end.logical_bytes += 1,
            _ => {}
        }
        wire.extend(end.encode(stream_id, sequence));
    }
    // All frames, including the deliberately wrong request-level responses,
    // retain valid codec hashes, sequences and termination.
    mst2_codec::treeframe::parse_stream(&wire).expect("fixture must be codec-valid");
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header("x-mega-snapshot-id", "fixed-snapshot")
        .header(
            "x-mega-request-digest",
            format!("sha256:{}", hex32(&sha256(&body))),
        )
        .body(axum::body::Body::from(wire))
        .unwrap()
}

struct FixtureServer {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FixtureServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fetch(
    case: ResponseCase,
    items: &[MetadataPageItem],
    pages: &Pages,
) -> Result<Pages, SnapshotError> {
    let requests = Arc::new(AtomicUsize::new(0));
    let fixture = Arc::new(ResponseFixture {
        case,
        expected_request: json!({"items": items}),
        pages: pages.clone(),
        requests: requests.clone(),
    });
    let app = Router::new()
        .route(
            "/api/v2/snapshots/fixed-snapshot/metadata/pages",
            post(metadata_response),
        )
        .with_state(fixture);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let _server = FixtureServer {
        task: tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
    };
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        Mst2Client::new(base).metadata_pages("fixed-snapshot", items, None),
    )
    .await
    .expect("fixture HTTP request must finish");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    result
}

fn witness_case() -> (Vec<MetadataPageItem>, Pages) {
    let entries: Vec<_> = (0..192)
        .map(|i| {
            Entry::file(
                EntryKind::Regular,
                format!("{}{:03}", if i < 96 { 'a' } else { 'b' }, i).as_bytes(),
                1,
                [0x44; 32],
            )
        })
        .collect();
    let mut pages: Pages = Page::pages_along_route(&entries, b"a")
        .unwrap()
        .into_iter()
        .map(|bytes| (page_id(&bytes), bytes))
        .collect();
    assert_eq!(
        pages.len(),
        2,
        "root and route terminal are separate witness pages"
    );
    let terminal = format!("sha256:{}", hex32(&pages.last().unwrap().0));
    let empty = Page::build(&[]).unwrap();
    let empty_id = page_id(&empty);
    pages.push((empty_id, empty));
    let items = vec![
        MetadataPageItem {
            directory_path: "/wide".into(),
            route: vec![b'a'],
            expected_digest: Some(terminal),
        },
        MetadataPageItem {
            directory_path: "/empty".into(),
            route: Vec::new(),
            expected_digest: Some(format!("sha256:{}", hex32(&empty_id))),
        },
    ];
    (items, pages)
}

async fn assert_rejected(case: ResponseCase, message: &str) {
    let (items, pages) = witness_case();
    assert_eq!(
        fetch(ResponseCase::Valid, &items, &pages).await.unwrap(),
        pages
    );
    let error = fetch(case, &items, &pages).await.unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::DigestMismatch, "{case:?}");
    assert!(
        error.message.contains(message),
        "{case:?}: {}",
        error.message
    );
}

#[tokio::test]
async fn metadata_end_accepts_multiple_frames_ancestor_witnesses_and_empty_page() {
    let (items, pages) = witness_case();
    assert_eq!(items.len(), 2);
    assert_eq!(pages.len(), 3);
    assert_eq!(
        fetch(ResponseCase::Valid, &items, &pages).await.unwrap(),
        pages
    );
}

#[tokio::test]
async fn metadata_end_accepts_alias_items_with_one_unique_empty_page() {
    let empty = Page::build(&[]).unwrap();
    let id = page_id(&empty);
    let items: Vec<_> = ["/empty", "/alias"]
        .into_iter()
        .map(|path| MetadataPageItem {
            directory_path: path.into(),
            route: Vec::new(),
            expected_digest: Some(format!("sha256:{}", hex32(&id))),
        })
        .collect();
    let pages = vec![(id, empty)];
    assert_eq!(
        fetch(ResponseCase::Valid, &items, &pages).await.unwrap(),
        pages
    );
}

#[tokio::test]
async fn metadata_end_rejects_wrong_request_item_count() {
    assert_rejected(ResponseCase::WrongItems, "END item count").await;
}

#[tokio::test]
async fn metadata_end_rejects_wrong_exact_request_body_digest() {
    assert_rejected(ResponseCase::WrongBodyHash, "END request_body_sha256").await;
}

#[tokio::test]
async fn metadata_end_rejects_wrong_unique_page_count() {
    assert_rejected(ResponseCase::WrongUnits, "END page count or logical bytes").await;
}

#[tokio::test]
async fn metadata_end_rejects_wrong_logical_page_bytes() {
    assert_rejected(ResponseCase::WrongBytes, "END page count or logical bytes").await;
}

#[tokio::test]
async fn metadata_end_rejects_duplicate_page_in_separate_frames() {
    assert_rejected(ResponseCase::DuplicatePage, "repeated a page across frames").await;
}

#[tokio::test]
async fn metadata_end_rejects_missing_terminal_witness_and_extra_pages() {
    assert_rejected(
        ResponseCase::MissingPage,
        "missing a requested terminal page",
    )
    .await;
    assert_rejected(
        ResponseCase::MissingWitness,
        "missing a requested route witness",
    )
    .await;
    assert_rejected(ResponseCase::ExtraPage, "contains an unrequested page").await;
}

#[tokio::test]
async fn metadata_end_rejects_object_data_frame() {
    assert_rejected(ResponseCase::ObjectFrame, "non-META data frame").await;
}

#[tokio::test]
async fn metadata_end_rejects_chunk_data_frame() {
    assert_rejected(ResponseCase::ChunkFrame, "non-META data frame").await;
}

#[tokio::test]
async fn metadata_end_preserves_error_failure_before_and_after_valid_pages() {
    let (items, pages) = witness_case();
    assert_eq!(
        fetch(ResponseCase::Valid, &items, &pages).await.unwrap(),
        pages
    );
    for case in [
        ResponseCase::ErrorBeforePages,
        ResponseCase::ErrorAfterPages,
    ] {
        let error = fetch(case, &items, &pages).await.unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::Internal);
        assert!(error.message.contains("TEMPORARY_UNAVAILABLE"));
        assert!(error.message.contains("metadata-error-fixture"));
    }
}
