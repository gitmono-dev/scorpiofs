use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    response::Response,
    routing::post,
    Router,
};
use mst2_codec::{
    metapage::{page_id, Page},
    treeframe::{ChunkPayload, EndPayload, MetaPayload, ObjectPayload},
};
use scorpiofs::snapshot::{
    frames::{hex32, ChunkRequest, MetadataPageItem},
    Mst2Client, SnapshotErrorCode,
};

fn digest(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}

fn object_bytes(index: usize) -> Vec<u8> {
    format!("item-{index}").into_bytes()
}

async fn response(
    State((calls, object_size)): State<(Arc<AtomicUsize>, usize)>,
    Path(endpoint): Path<String>,
    body: Bytes,
) -> Response {
    assert!(
        body.len() <= 131_072,
        "oversized request reached HTTP server"
    );
    calls.fetch_add(1, Ordering::Relaxed);
    let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let items = request["items"].as_array().unwrap();
    let mut wire = Vec::new();
    let mut units = 0;
    let mut logical = 0;
    if endpoint == "metadata" {
        let page = Page::build(&[]).unwrap();
        logical = page.len() as u64;
        wire.extend(
            MetaPayload {
                pages: vec![(page_id(&page), page)],
            }
            .encode(23, 0)
            .unwrap(),
        );
        units = 1;
    } else {
        for item in items {
            let index: usize = item["path"]
                .as_str()
                .unwrap()
                .rsplit('/')
                .next()
                .unwrap()
                .parse()
                .unwrap();
            let mut data = object_bytes(index);
            data.resize(data.len().max(object_size), b'x');
            logical += data.len() as u64;
            if endpoint == "objects" {
                wire.extend(
                    ObjectPayload {
                        objects: vec![(digest(&data), data)],
                    }
                    .encode(23, units)
                    .unwrap(),
                );
            } else {
                wire.extend(
                    ChunkPayload {
                        map_id: [0x11; 32],
                        file_content_id: [0x22; 32],
                        chunk_index: index as u64,
                        chunk_bytes: data,
                    }
                    .encode(23, units)
                    .unwrap(),
                );
            }
            units += 1;
        }
    }
    wire.extend(
        EndPayload {
            request_item_count: items.len() as u32,
            unique_unit_count: units as u32,
            logical_bytes: logical,
            request_body_sha256: digest(&body),
        }
        .encode(23, units),
    );
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe; version=2")
        .header("x-mega-snapshot-id", "fixed-snapshot")
        .header(
            "x-mega-request-digest",
            format!("sha256:{}", hex32(&digest(&body))),
        )
        .body(Body::from(wire))
        .unwrap()
}

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn client() -> (Mst2Client, Arc<AtomicUsize>, Server) {
    client_with_object_size(0).await
}

async fn client_with_object_size(object_size: usize) -> (Mst2Client, Arc<AtomicUsize>, Server) {
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/api/v2/snapshots/fixed-snapshot/{endpoint}",
            post(response),
        )
        .route(
            "/api/v2/snapshots/fixed-snapshot/metadata/pages",
            post(|state, body| response(state, Path("metadata".into()), body)),
        )
        .with_state((calls.clone(), object_size));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Mst2Client::with_token(
        format!("http://{}", listener.local_addr().unwrap()),
        Some("fixture-token".into()),
    );
    client.bind_lease("fixture-lease");
    let server = Server(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    }));
    (client, calls, server)
}

#[tokio::test]
async fn long_path_object_and_chunk_batches_are_split_before_http() {
    let (client, calls, _server) = client().await;
    let prefix = format!("/{}", "a".repeat(4080));
    let objects: Vec<_> = (0..128)
        .map(|i| {
            (
                format!("{prefix}/{i}"),
                format!("sha256:{}", hex32(&digest(&object_bytes(i)))),
            )
        })
        .collect();
    let received = client
        .objects("fixed-snapshot", &objects, Some("identity"))
        .await
        .unwrap();
    assert_eq!(received.len(), 128);
    assert!(calls.load(Ordering::Relaxed) > 1);
    for i in 0..128 {
        assert_eq!(received[&digest(&object_bytes(i))], object_bytes(i));
    }
    calls.store(0, Ordering::Relaxed);
    let chunks: Vec<_> = (0..128)
        .map(|i| ChunkRequest {
            path: format!("{prefix}/{i}"),
            expected_digest: format!("sha256:{}", hex32(&[0x22; 32])),
            map_id: format!("sha256:{}", hex32(&[0x11; 32])),
            chunk_index: i as u64,
        })
        .collect();
    let received = client
        .chunks("fixed-snapshot", &chunks, Some("identity"))
        .await
        .unwrap();
    assert_eq!(received.len(), 128);
    assert!(calls.load(Ordering::Relaxed) > 1);
    for chunk in received {
        assert_eq!(chunk.bytes, object_bytes(chunk.chunk_index as usize));
    }
}

#[tokio::test]
async fn escaped_metadata_paths_split_and_shared_witnesses_are_deduplicated() {
    let (client, calls, _server) = client().await;
    let page = Page::build(&[]).unwrap();
    let items: Vec<_> = (0..64)
        .map(|_| MetadataPageItem {
            directory_path: format!("/{}", "\"".repeat(4000)),
            route: vec![],
            expected_digest: Some(format!("sha256:{}", hex32(&page_id(&page)))),
        })
        .collect();
    let received = client
        .metadata_pages("fixed-snapshot", &items, Some("identity"))
        .await
        .unwrap();
    assert!(calls.load(Ordering::Relaxed) > 1);
    assert_eq!(received, vec![(page_id(&page), page)]);
}

#[tokio::test]
async fn a_single_oversized_item_is_rejected_without_http() {
    let (client, calls, _server) = client().await;
    let items = vec![(
        format!("/{}", "x".repeat(131_072)),
        format!("sha256:{}", hex32(&digest(b"data"))),
    )];
    assert_eq!(
        client
            .objects("fixed-snapshot", &items, None)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn split_object_batches_keep_the_total_output_limit() {
    let (client, calls, _server) = client_with_object_size(100 * 1024).await;
    let prefix = format!("/{}", "a".repeat(4080));
    let items: Vec<_> = (0..128)
        .map(|index| {
            let mut bytes = object_bytes(index);
            bytes.resize(100 * 1024, b'x');
            (
                format!("{prefix}/{index}"),
                format!("sha256:{}", hex32(&digest(&bytes))),
            )
        })
        .collect();
    assert_eq!(
        client
            .objects("fixed-snapshot", &items, None)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert!(calls.load(Ordering::Relaxed) > 1);
}
