//! Exercise the adopted codec's raw digest contract through the HTTP client.

use axum::{body::Bytes, extract::State, response::Response, routing::post, Router};
use mst2_codec::treeframe::{EndPayload, ObjectPayload, HEADER_LEN};
use scorpiofs::snapshot::{Mst2Client, SnapshotErrorCode};

fn digest(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}

fn content() -> Vec<u8> {
    vec![b'x'; 4096]
}

async fn object_response(State(legacy): State<bool>, body: Bytes) -> Response {
    let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(request["encoding"], "zstd");
    let payload = ObjectPayload {
        objects: vec![(digest(&content()), content())],
    };
    let identity = payload.encode(31, 0).unwrap();
    let mut compressed = payload.encode_zstd(31, 0).unwrap();
    assert_eq!(compressed[7], 1, "fixture must use compressed wire bytes");
    assert_eq!(compressed[32..64], digest(&identity[HEADER_LEN..]));
    let wire_digest = digest(&compressed[HEADER_LEN..]);
    assert_ne!(compressed[32..64], wire_digest);
    if legacy {
        compressed[32..64].copy_from_slice(&wire_digest);
    }
    compressed.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: content().len() as u64,
            request_body_sha256: digest(&body),
        }
        .encode(31, 1),
    );
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header("x-mega-snapshot-id", "codec-v4")
        .header(
            "x-mega-request-digest",
            format!("sha256:{}", hex::encode(digest(&body))),
        )
        .body(compressed.into())
        .unwrap()
}

#[tokio::test]
async fn compressed_raw_digest_is_accepted_and_legacy_wire_digest_is_rejected() {
    for legacy in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/api/v2/snapshots/codec-v4/objects", post(object_response))
            .with_state(legacy);
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = Mst2Client::new(format!("http://{address}"));
        let result = client
            .objects(
                "codec-v4",
                &[(
                    "/file".into(),
                    format!("sha256:{}", hex::encode(digest(&content()))),
                )],
                Some("zstd"),
            )
            .await;
        if legacy {
            assert_eq!(result.unwrap_err().code, SnapshotErrorCode::DigestMismatch);
        } else {
            assert_eq!(result.unwrap()[&digest(&content())], content());
        }
        task.abort();
        let _ = task.await;
    }
}
