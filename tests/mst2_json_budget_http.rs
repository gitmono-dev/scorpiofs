//! Exercise production JSON limits, strict parsing and deadlines over HTTP.

use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use axum::{
    body::{Body, Bytes},
    http::StatusCode,
    response::Response,
    routing::any,
    Router,
};
use futures::{stream, StreamExt};
use scorpiofs::snapshot::{frames::MetadataPageItem, Mst2Client, SnapshotErrorCode};

const CAP: usize = 1_048_576;
const CAPS: &str = r#"{"protocol_versions":[2],"metadata_codecs":[1],"frame_encodings":["identity"],"features":{"resolve":true,"directory":true,"leases":true}}"#;

struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(app: Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, task }
    }

    async fn json(status: StatusCode, bytes: Vec<u8>, chunked: bool) -> Self {
        let app = Router::new().route(
            "/{*path}",
            any(move || {
                let bytes = bytes.clone();
                async move {
                    let body = if chunked {
                        Body::from_stream(stream::iter(
                            bytes
                                .chunks(8192)
                                .map(|chunk| Ok::<_, Infallible>(Bytes::copy_from_slice(chunk)))
                                .collect::<Vec<_>>(),
                        ))
                    } else {
                        Body::from(bytes)
                    };
                    Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(body)
                        .unwrap()
                }
            }),
        );
        Self::start(app).await
    }

    fn client(&self) -> Mst2Client {
        Mst2Client::new(&self.url)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn exact_json_budget_succeeds_and_streamed_excess_fails() {
    let mut body = CAPS.as_bytes().to_vec();
    body.resize(CAP, b' ');
    let server = Server::json(StatusCode::OK, body.clone(), true).await;
    assert!(
        server
            .client()
            .capabilities()
            .await
            .unwrap()
            .features
            .leases
    );
    body.push(b' ');
    let server = Server::json(StatusCode::OK, body, true).await;
    assert_eq!(
        server.client().capabilities().await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    // Generic Value responses and DELETE have the same bound as typed DTOs.
    assert_eq!(
        server.client().descriptor("sid").await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(
        server
            .client()
            .release_lease("lease")
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
}

#[tokio::test]
async fn declared_oversize_is_rejected_without_waiting_for_body() {
    let server = Server::start(Router::new().route(
        "/{*path}",
        any(|| async {
            Response::builder()
                .header("content-length", (CAP + 1).to_string())
                .body(Body::from_stream(stream::pending::<
                    Result<Bytes, Infallible>,
                >()))
                .unwrap()
        }),
    ))
    .await;
    let started = Instant::now();
    let error = server
        .client()
        .with_request_timeout(Duration::from_secs(1))
        .capabilities()
        .await
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::LimitExceeded);
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn errors_are_bounded_and_invalid_envelopes_never_become_absence() {
    for chunked in [false, true] {
        let server = Server::json(StatusCode::FORBIDDEN, vec![b' '; CAP + 1], chunked).await;
        let error = server.client().capabilities().await.unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::LimitExceeded);
        assert_eq!(error.http_status, 403);
        assert_eq!(
            server
                .client()
                .blob_verified("sid", "/file", "digest")
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::LimitExceeded
        );
    }
    let server = Server::json(
        StatusCode::NOT_FOUND,
        br#"{"error":{"code":"PATH_NOT_FOUND","message":"absent"},"extra":{"a":1,"\u0061":2}}"#
            .to_vec(),
        false,
    )
    .await;
    let error = server.client().capabilities().await.unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::Internal);
    assert_eq!(error.http_status, 404);
    let error = server
        .client()
        .metadata_pages(
            "sid",
            &[MetadataPageItem {
                directory_path: "/".into(),
                route: vec![],
                expected_digest: None,
            }],
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::Internal);
    assert_eq!(error.http_status, 404);
    let server = Server::json(
        StatusCode::NOT_FOUND,
        br#"{"error":{"code":"PATH_NOT_FOUND","message":"absent"}}"#.to_vec(),
        false,
    )
    .await;
    assert_eq!(
        server.client().capabilities().await.unwrap_err().code,
        SnapshotErrorCode::PathNotFound
    );
}

#[tokio::test]
async fn duplicate_keys_nested_extensions_and_trailing_json_are_rejected() {
    let cases = [
        r#"{"a":1,"a":2}"#.to_string(),
        r#"{"a":1,"\u0061":2}"#.to_string(),
        r#"{"a":[{"nested":1,"nested":2}]}"#.to_string(),
        format!(
            "{},\"extra\":{{\"unknown\":1,\"unknown\":2}}}}",
            &CAPS[..CAPS.len() - 1]
        ),
        format!("{CAPS} {{}}"),
    ];
    for body in cases {
        let server = Server::json(StatusCode::OK, body.into_bytes(), false).await;
        assert_eq!(
            server.client().capabilities().await.unwrap_err().code,
            SnapshotErrorCode::IntegrityError
        );
        assert_eq!(
            server.client().descriptor("sid").await.unwrap_err().code,
            SnapshotErrorCode::IntegrityError
        );
        assert_eq!(
            server
                .client()
                .renew_lease("lease", 600)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::IntegrityError
        );
        assert_eq!(
            server
                .client()
                .release_lease("lease")
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::IntegrityError
        );
    }
    // Equal names in different objects remain legal, as do legitimate extensions.
    let body = format!(
        "{},\"extra\":[{{\"a\":1}},{{\"a\":2}}]}}",
        &CAPS[..CAPS.len() - 1]
    );
    let server = Server::json(StatusCode::OK, body.into_bytes(), false).await;
    assert!(server.client().capabilities().await.is_ok());
    for body in [
        vec![b'"', 0xff, b'"'],
        format!("{}0{}", "[".repeat(150), "]".repeat(150)).into_bytes(),
    ] {
        let server = Server::json(StatusCode::OK, body, false).await;
        assert_eq!(
            server.client().descriptor("sid").await.unwrap_err().code,
            SnapshotErrorCode::IntegrityError
        );
    }
}

#[tokio::test]
async fn stalled_headers_body_and_drip_stream_share_a_total_deadline() {
    for mode in 0..3 {
        let app = Router::new().route(
            "/{*path}",
            any(move || async move {
                if mode == 0 {
                    return std::future::pending::<Response>().await;
                }
                let body = if mode == 1 {
                    Body::from_stream(
                        stream::once(async { Ok::<_, Infallible>(Bytes::from_static(b"{")) })
                            .chain(stream::pending::<Result<Bytes, Infallible>>()),
                    )
                } else {
                    Body::from_stream(stream::unfold((), |_| async {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        Some((Ok::<_, Infallible>(Bytes::from_static(b" ")), ()))
                    }))
                };
                Response::new(body)
            }),
        );
        let server = Server::start(app).await;
        let client = server
            .client()
            .with_request_timeout(Duration::from_millis(150));
        let started = Instant::now();
        let error = tokio::time::timeout(Duration::from_secs(2), client.capabilities())
            .await
            .expect("production request deadline did not fire")
            .unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::TemporaryUnavailable);
        assert!(started.elapsed() < Duration::from_secs(1));
        if mode == 1 {
            assert_eq!(
                client
                    .blob_verified("sid", "/file", "digest")
                    .await
                    .unwrap_err()
                    .code,
                SnapshotErrorCode::TemporaryUnavailable
            );
        }
    }
}

#[tokio::test]
async fn retries_and_backoff_share_the_original_deadline_and_next_request_recovers() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let app = Router::new().route(
        "/{*path}",
        any(move || {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(80)).await;
                Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .body(Body::empty())
                    .unwrap()
            }
        }),
    );
    let server = Server::start(app).await;
    let client = server
        .client()
        .with_request_timeout(Duration::from_millis(300));
    let started = Instant::now();
    let error = client.capabilities().await.unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::TemporaryUnavailable);
    assert!(started.elapsed() < Duration::from_millis(550));
    assert!((2..=3).contains(&calls.load(Ordering::SeqCst)));
    assert!(client.retry_count() >= 1);
    // A deadline is local to its logical operation, never stored on the pool.
    let recovery_calls = Arc::new(AtomicUsize::new(0));
    let seen = recovery_calls.clone();
    let server = Server::start(Router::new().route(
        "/{*path}",
        any(move || {
            let seen = seen.clone();
            async move {
                if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                    std::future::pending::<Response>().await
                } else {
                    Response::new(Body::from(CAPS))
                }
            }
        }),
    ))
    .await;
    let client = server
        .client()
        .with_request_timeout(Duration::from_millis(150));
    assert_eq!(
        client.capabilities().await.unwrap_err().code,
        SnapshotErrorCode::TemporaryUnavailable
    );
    assert!(client.capabilities().await.unwrap().features.resolve);
}

#[tokio::test]
async fn oversized_json_request_is_rejected_before_network() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let server = Server::start(Router::new().route(
        "/{*path}",
        any(move || {
            seen.fetch_add(1, Ordering::SeqCst);
            async { Response::new(Body::empty()) }
        }),
    ))
    .await;
    let error = server
        .client()
        .lookup("sid", &["x".repeat(131_072)])
        .await
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::LimitExceeded);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
