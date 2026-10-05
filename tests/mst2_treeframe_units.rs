//! Endpoint unit-set and bounded-body acceptance through the real HTTP client.

use std::{collections::HashSet, sync::Arc, time::Duration};

use axum::{
    body::{Body, Bytes},
    extract::State,
    http::HeaderMap,
    response::Response,
    routing::post,
    Router,
};
use mst2_codec::{
    metapage::{page_id, Page},
    treeframe::{ChunkPayload, EndPayload, ErrorPayload, MetaPayload, ObjectPayload},
};
use scorpiofs::snapshot::{
    frames::{hex32, ChunkRequest},
    Mst2Client, SnapshotErrorCode,
};
use serde_json::Value;

#[derive(Clone, Copy, Debug)]
enum Endpoint {
    Objects,
    Chunks,
}

#[derive(Clone, Copy, Debug)]
enum Case {
    Valid,
    Alias,
    Duplicate,
    Missing,
    Extra,
    WrongKind,
    WrongFile,
    WrongMap,
    WrongIndex,
    WrongItems,
    WrongUnits,
    WrongBytes,
    WrongRequest,
    Error,
    HttpError,
    DeclaredTooLarge,
    StreamTooLarge,
    DeclaredHttpErrorTooLarge,
    StreamHttpErrorTooLarge,
    RawTooLarge,
    WireTooLarge,
}

struct Fixture {
    endpoint: Endpoint,
    case: Case,
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}

fn id(bytes: &[u8]) -> String {
    format!("sha256:{}", hex32(&digest(bytes)))
}

async fn response(
    State(fixture): State<Arc<Fixture>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    assert_eq!(headers["authorization"], "Bearer fixture-token");
    assert_eq!(headers["x-mega-snapshot-lease"], "fixture-lease");
    let request: Value = serde_json::from_slice(&body).unwrap();
    let request_count = request["items"].as_array().unwrap().len() as u32;
    let aliased = matches!(fixture.case, Case::Alias);
    let mut wire = Vec::new();
    let mut sequence = 0;
    let mut unit_count = 0;
    let mut logical_bytes = 0;
    if matches!(fixture.case, Case::WrongKind) {
        let page = Page::build(&[]).unwrap();
        logical_bytes = page.len() as u64;
        unit_count = 1;
        wire.extend(
            MetaPayload {
                pages: vec![(page_id(&page), page)],
            }
            .encode(23, sequence)
            .unwrap(),
        );
        sequence += 1;
    } else if !matches!(fixture.case, Case::Error) {
        let indices = match fixture.case {
            Case::Alias | Case::Missing => vec![0],
            Case::Duplicate => vec![0, 1, 0],
            Case::Extra => vec![0, 1, 2],
            _ => vec![1, 0],
        };
        for index in indices {
            let data = format!("object-{index}").into_bytes();
            match fixture.endpoint {
                Endpoint::Objects => {
                    wire.extend(
                        ObjectPayload {
                            objects: vec![(digest(&data), data.clone())],
                        }
                        .encode(23, sequence)
                        .unwrap(),
                    );
                }
                Endpoint::Chunks => {
                    wire.extend(
                        ChunkPayload {
                            map_id: if matches!(fixture.case, Case::WrongMap) {
                                [0x33; 32]
                            } else {
                                [0x11; 32]
                            },
                            file_content_id: if matches!(fixture.case, Case::WrongFile) {
                                [0x44; 32]
                            } else {
                                [0x22; 32]
                            },
                            chunk_index: if matches!(fixture.case, Case::WrongIndex) {
                                index + 9
                            } else {
                                index
                            },
                            chunk_bytes: data.clone(),
                        }
                        .encode(23, sequence)
                        .unwrap(),
                    );
                }
            }
            unit_count += 1;
            logical_bytes += data.len() as u64;
            sequence += 1;
        }
    }
    if matches!(fixture.case, Case::Error) {
        wire.extend(
            ErrorPayload {
                code: "TEMPORARY_UNAVAILABLE".into(),
                retryable: true,
                request_id: "unit-fixture".into(),
            }
            .encode(23, sequence)
            .unwrap(),
        );
    } else {
        let mut end = EndPayload {
            request_item_count: request_count,
            unique_unit_count: unit_count,
            logical_bytes,
            request_body_sha256: digest(&body),
        };
        match fixture.case {
            Case::WrongItems => end.request_item_count += 1,
            Case::WrongUnits => end.unique_unit_count += 1,
            Case::WrongBytes => end.logical_bytes += 1,
            Case::WrongRequest => end.request_body_sha256[0] ^= 1,
            _ => {}
        }
        wire.extend(end.encode(23, sequence));
    }
    if matches!(fixture.case, Case::RawTooLarge) {
        wire[16..20].copy_from_slice(&(2_097_153u32).to_le_bytes());
    }
    if matches!(fixture.case, Case::WireTooLarge) {
        wire[12..16].copy_from_slice(&(2_097_153u32).to_le_bytes());
    }
    let mut response = Response::builder()
        .header(
            "content-type",
            "application/vnd.mega.treeframe; version=\"2\"",
        )
        .header("x-mega-snapshot-id", "fixed-snapshot")
        .header("x-mega-request-digest", id(&body));
    if matches!(
        fixture.case,
        Case::HttpError | Case::DeclaredHttpErrorTooLarge | Case::StreamHttpErrorTooLarge
    ) {
        response = response.status(axum::http::StatusCode::BAD_REQUEST);
    }
    if matches!(
        fixture.case,
        Case::DeclaredTooLarge | Case::DeclaredHttpErrorTooLarge
    ) {
        response = response.header("content-length", "536870912");
    }
    let response_body = if matches!(
        fixture.case,
        Case::StreamTooLarge
            | Case::DeclaredTooLarge
            | Case::DeclaredHttpErrorTooLarge
            | Case::StreamHttpErrorTooLarge
    ) {
        // No Content-Length: the client must enforce the budget while reading.
        Body::from_stream(futures::stream::iter(
            (0..8192).map(|_| Ok::<_, std::io::Error>(Bytes::from(vec![0; 65_536]))),
        ))
    } else if matches!(fixture.case, Case::HttpError) {
        Body::from(r#"{"error":{"code":"SCOPE_FORBIDDEN","message":"fixture scope denied"}}"#)
    } else {
        Body::from(wire)
    };
    assert_eq!(request_count, if aliased { 3 } else { 2 });
    response.body(response_body).unwrap()
}

struct Server(tokio::task::JoinHandle<()>);

impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn fetch(endpoint: Endpoint, case: Case) -> Result<usize, SnapshotErrorCode> {
    let tail = match endpoint {
        Endpoint::Objects => "objects",
        Endpoint::Chunks => "chunks",
    };
    let app = Router::new()
        .route(
            &format!("/api/v2/snapshots/fixed-snapshot/{tail}"),
            post(response),
        )
        .with_state(Arc::new(Fixture { endpoint, case }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Mst2Client::with_token(
        format!("http://{}", listener.local_addr().unwrap()),
        Some("fixture-token".into()),
    );
    client.bind_lease("fixture-lease");
    let _server = Server(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap()
    }));
    tokio::time::timeout(Duration::from_secs(5), async {
        match endpoint {
            Endpoint::Objects => {
                let mut items = vec![
                    ("/zero".into(), id(b"object-0")),
                    ("/one".into(), id(b"object-1")),
                ];
                if matches!(case, Case::Alias) {
                    items[1].1 = items[0].1.clone();
                    items.push(("/alias".into(), items[0].1.clone()));
                }
                client
                    .objects("fixed-snapshot", &items, None)
                    .await
                    .map(|objects| {
                        assert_eq!(
                            objects[&digest(b"object-0")].as_slice(),
                            b"object-0".as_slice()
                        );
                        objects.len()
                    })
                    .map_err(|error| error.code)
            }
            Endpoint::Chunks => {
                let mut items: Vec<_> = (0..2)
                    .map(|index| ChunkRequest {
                        path: "/large".into(),
                        expected_digest: format!("sha256:{}", hex32(&[0x22; 32])),
                        map_id: format!("sha256:{}", hex32(&[0x11; 32])),
                        chunk_index: index,
                    })
                    .collect();
                if matches!(case, Case::Alias) {
                    items[1].chunk_index = 0;
                    items.push(items[0].clone());
                }
                client
                    .chunks("fixed-snapshot", &items, None)
                    .await
                    .map(|chunks| {
                        let received: HashSet<_> =
                            chunks.iter().map(|chunk| chunk.chunk_index).collect();
                        assert!(received.contains(&0));
                        chunks.len()
                    })
                    .map_err(|error| error.code)
            }
        }
    })
    .await
    .expect("bounded fixture request must finish")
}

#[tokio::test]
async fn endpoint_accepts_exact_units_and_deduplicated_alias_items() {
    for endpoint in [Endpoint::Objects, Endpoint::Chunks] {
        assert_eq!(fetch(endpoint, Case::Valid).await.unwrap(), 2);
        assert_eq!(fetch(endpoint, Case::Alias).await.unwrap(), 1);
    }
}

#[tokio::test]
async fn endpoint_rejects_wrong_kind_duplicate_missing_and_extra_units() {
    for endpoint in [Endpoint::Objects, Endpoint::Chunks] {
        for case in [Case::WrongKind, Case::Duplicate, Case::Missing, Case::Extra] {
            assert_eq!(
                fetch(endpoint, case).await.unwrap_err(),
                SnapshotErrorCode::DigestMismatch,
                "{endpoint:?}/{case:?}"
            );
        }
    }
    for case in [Case::WrongFile, Case::WrongMap, Case::WrongIndex] {
        assert_eq!(
            fetch(Endpoint::Chunks, case).await.unwrap_err(),
            SnapshotErrorCode::DigestMismatch,
            "{case:?}"
        );
    }
}

#[tokio::test]
async fn endpoint_requires_end_binding_to_actual_units_and_exact_request() {
    for endpoint in [Endpoint::Objects, Endpoint::Chunks] {
        for case in [
            Case::WrongItems,
            Case::WrongUnits,
            Case::WrongBytes,
            Case::WrongRequest,
        ] {
            assert_eq!(
                fetch(endpoint, case).await.unwrap_err(),
                SnapshotErrorCode::DigestMismatch,
                "{endpoint:?}/{case:?}"
            );
        }
        assert_eq!(
            fetch(endpoint, Case::Error).await.unwrap_err(),
            SnapshotErrorCode::Internal
        );
        assert_eq!(
            fetch(endpoint, Case::HttpError).await.unwrap_err(),
            SnapshotErrorCode::ScopeForbidden
        );
    }
}

#[tokio::test]
async fn object_response_read_is_bounded_with_or_without_content_length() {
    for case in [
        Case::DeclaredTooLarge,
        Case::StreamTooLarge,
        Case::DeclaredHttpErrorTooLarge,
        Case::StreamHttpErrorTooLarge,
        Case::RawTooLarge,
        Case::WireTooLarge,
    ] {
        assert_eq!(
            fetch(Endpoint::Objects, case).await.unwrap_err(),
            SnapshotErrorCode::LimitExceeded,
            "{case:?}"
        );
    }
}
