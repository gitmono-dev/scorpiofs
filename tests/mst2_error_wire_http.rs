//! Typed errors must bind their HTTP status and never fabricate absence.

use axum::{body::Body, http::StatusCode, response::Response, routing::any, Router};
use scorpiofs::snapshot::{Mst2Client, SnapshotError, SnapshotErrorCode};
use serde_json::{json, Value};

struct Server {
    client: Mst2Client,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn start(status: u16, body: String) -> Self {
        let app = Router::new().route(
            "/{*path}",
            any(move || {
                let body = body.clone();
                async move {
                    Response::builder()
                        .status(StatusCode::from_u16(status).unwrap())
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { client, task }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn envelope(code: &str) -> Value {
    json!({"error": {"code": code, "message": "public error",
        "request_id": "request-fixture", "retryable": false}})
}
async fn response(status: u16, body: Value) -> SnapshotError {
    let server = Server::start(status, body.to_string()).await;
    server.client.capabilities().await.unwrap_err()
}

#[tokio::test]
async fn every_spec_error_preserves_legacy_fallback_and_actual_http_status() {
    use SnapshotErrorCode::*;
    for (code, status, expected) in [
        ("PATH_NOT_FOUND", 404, PathNotFound),
        ("NOT_DIRECTORY", 409, NotDirectory),
        ("NOT_FILE", 409, Internal),
        ("SYMLINK_TRAVERSAL", 409, SymlinkTraversal),
        ("UNAUTHENTICATED", 401, Unauthenticated),
        ("SCOPE_FORBIDDEN", 403, ScopeForbidden),
        ("LEASE_EXPIRED", 410, LeaseExpired),
        ("SNAPSHOT_GONE", 410, SnapshotGone),
        ("SNAPSHOT_NOT_READY", 503, SnapshotNotReady),
        ("METADATA_NOT_READY", 503, MetadataNotReady),
        ("OBJECT_UNAVAILABLE", 503, ObjectUnavailable),
        ("INTEGRITY_ERROR", 502, IntegrityError),
        ("CURSOR_STALE", 409, CursorStale),
        ("EXPECTED_DIGEST_MISMATCH", 409, DigestMismatch),
        ("MIXED_SOURCE_BATCH", 422, Internal),
        ("UNSUPPORTED_ENTRY", 422, UnsupportedEntry),
        ("UNSUPPORTED_CODEC", 422, Internal),
        ("OBJECT_TOO_LARGE", 413, Internal),
        ("LIMIT_EXCEEDED", 413, LimitExceeded),
        ("PROOF_BUDGET_EXCEEDED", 413, ProofBudgetExceeded),
        ("INVALID_REQUEST", 400, InvalidRequest),
        ("RANGE_NOT_SUPPORTED", 400, RangeNotSupported),
        ("NAMESPACE_CONFLICT", 409, Internal),
        ("PUBLICATION_CONFLICT", 409, Internal),
        ("RELEASE_IMMUTABLE", 409, Internal),
        ("RATE_LIMITED", 429, Internal),
        ("TEMPORARY_UNAVAILABLE", 503, TemporaryUnavailable),
    ] {
        let error = response(status, envelope(code)).await;
        assert_eq!(error.code, expected, "{code}");
        assert_eq!(error.http_status, status);
        assert_eq!(error.message, "public error");
        let bad_status = if status == 404 { 403 } else { 404 };
        let error = response(bad_status, envelope(code)).await;
        assert_eq!(
            error.code, IntegrityError,
            "{code} cannot bind status {bad_status}"
        );
        assert_eq!(error.http_status, bad_status);
    }
}

#[tokio::test]
async fn frozen_error_fixture_preserves_the_typed_unavailable_result() {
    let body = include_str!("fixtures/mst2_error_0_2_1.json");
    let server = Server::start(503, body.into()).await;
    let error = server
        .client
        .verified_descriptor("fixture-sid")
        .await
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::ObjectUnavailable);
    assert_eq!(error.http_status, 503);
    assert_eq!(
        error.message,
        "The fixed object is temporarily unavailable."
    );
}

#[tokio::test]
async fn malformed_canonical_fields_are_closed_and_cannot_become_legacy_absence() {
    let fixture = envelope("PATH_NOT_FOUND");
    for key in ["code", "message", "request_id", "retryable"] {
        for null in [false, true] {
            let mut value = fixture.clone();
            if null {
                value["error"][key] = Value::Null;
            } else {
                value["error"].as_object_mut().unwrap().remove(key);
            }
            assert_eq!(
                response(404, value).await.code,
                SnapshotErrorCode::IntegrityError,
                "{key}"
            );
        }
    }
    for (field, bad) in [
        ("code", json!("FUTURE_UNKNOWN")),
        ("code", json!(404)),
        ("message", json!(true)),
        ("message", json!("a".repeat(1025))),
        ("request_id", json!("")),
        ("request_id", json!("a".repeat(513))),
        ("request_id", json!(5)),
        ("retryable", json!("false")),
        ("retryable", json!(0)),
    ] {
        let mut value = fixture.clone();
        value["error"][field] = bad;
        assert_eq!(
            response(404, value).await.code,
            SnapshotErrorCode::IntegrityError,
            "{field}"
        );
    }
    for outer in [false, true] {
        let mut value = fixture.clone();
        if outer {
            value["extra"] = json!(true);
        } else {
            value["error"]["extra"] = json!(true);
        }
        assert_eq!(
            response(404, value).await.code,
            SnapshotErrorCode::IntegrityError
        );
    }
    for length in [1, 512] {
        let mut value = fixture.clone();
        value["error"]["request_id"] = json!("界".repeat(length));
        value["error"]["message"] = json!("界".repeat(1024));
        assert_eq!(
            response(404, value).await.code,
            SnapshotErrorCode::PathNotFound
        );
    }
}

#[tokio::test]
async fn deployed_legacy_envelopes_keep_explicit_codes_without_status_spoofing() {
    use SnapshotErrorCode::*;
    for (code, status, expected) in [
        ("SCOPE_INVALID", 400, ScopeInvalid),
        ("CURSOR_INVALID", 400, CursorInvalid),
        ("VIEW_NOT_FOUND", 404, ViewNotFound),
        ("LEASE_UNKNOWN", 404, LeaseUnknown),
        ("OBJECT_DIGEST_MISMATCH", 409, DigestMismatch),
        ("INTERNAL", 500, Internal),
        ("CONFLICT", 409, Internal),
    ] {
        // Current deployed legacy codes also carry request/retry hints.
        assert_eq!(
            response(status, envelope(code)).await.code,
            expected,
            "{code}"
        );
        let value =
            json!({"error": {"code": code, "message": "legacy", "extension": true}, "extra": true});
        assert_eq!(response(status, value).await.code, expected, "{code}");
    }
    for value in [
        envelope("CONFLICT"),
        json!({"error": {"code": "CONFLICT", "message": "legacy"}}),
    ] {
        assert_eq!(response(403, value).await.code, IntegrityError);
    }
    let value = json!({"error": {"code": "FUTURE_UNKNOWN", "message": "legacy"}});
    assert_eq!(response(403, value).await.code, Internal);
    for canonical in [false, true] {
        let value = if canonical {
            envelope("PATH_NOT_FOUND")
        } else {
            json!({"error": {"code": "PATH_NOT_FOUND", "message": "legacy absent"}})
        };
        let server = Server::start(403, value.to_string()).await;
        for error in [
            server
                .client
                .blob_verified("sid", "/file", "digest")
                .await
                .unwrap_err(),
            server.client.descriptor("sid").await.unwrap_err(),
            server.client.release_lease("lease").await.unwrap_err(),
        ] {
            assert_eq!(
                error.code, IntegrityError,
                "wrong HTTP status cannot mean absence"
            );
            assert_eq!(error.http_status, 403);
        }
    }
}

#[tokio::test]
async fn error_wire_reuses_recursive_duplicate_and_body_byte_bounds() {
    for body in [
        r#"{"error":{"code":"PATH_NOT_FOUND","message":"absent","request_id":"a","retryable":false,"\u0072etryable":true}}"#.into(),
        format!("{} {{}}", envelope("PATH_NOT_FOUND")),
    ] {
        let server = Server::start(404, body).await;
        let error = server.client.capabilities().await.unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::Internal);
        assert_eq!(error.http_status, 404);
    }
    let server = Server::start(404, " ".repeat(1_048_577)).await;
    let error = server.client.capabilities().await.unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::LimitExceeded);
    assert_eq!(error.http_status, 404);
}

#[test]
fn explicit_canonical_parser_retains_all_spec_error_types() {
    use scorpiofs::snapshot::error_wire::{CanonicalSnapshotError, CanonicalSnapshotErrorCode::*};
    for (code, status, expected) in [
        ("PATH_NOT_FOUND", 404, PathNotFound),
        ("NOT_DIRECTORY", 409, NotDirectory),
        ("NOT_FILE", 409, NotFile),
        ("SYMLINK_TRAVERSAL", 409, SymlinkTraversal),
        ("UNAUTHENTICATED", 401, Unauthenticated),
        ("SCOPE_FORBIDDEN", 403, ScopeForbidden),
        ("LEASE_EXPIRED", 410, LeaseExpired),
        ("SNAPSHOT_GONE", 410, SnapshotGone),
        ("SNAPSHOT_NOT_READY", 503, SnapshotNotReady),
        ("METADATA_NOT_READY", 503, MetadataNotReady),
        ("OBJECT_UNAVAILABLE", 503, ObjectUnavailable),
        ("INTEGRITY_ERROR", 502, IntegrityError),
        ("CURSOR_STALE", 409, CursorStale),
        ("EXPECTED_DIGEST_MISMATCH", 409, ExpectedDigestMismatch),
        ("MIXED_SOURCE_BATCH", 422, MixedSourceBatch),
        ("UNSUPPORTED_ENTRY", 422, UnsupportedEntry),
        ("UNSUPPORTED_CODEC", 422, UnsupportedCodec),
        ("OBJECT_TOO_LARGE", 413, ObjectTooLarge),
        ("LIMIT_EXCEEDED", 413, LimitExceeded),
        ("PROOF_BUDGET_EXCEEDED", 413, ProofBudgetExceeded),
        ("INVALID_REQUEST", 400, InvalidRequest),
        ("RANGE_NOT_SUPPORTED", 400, RangeNotSupported),
        ("NAMESPACE_CONFLICT", 409, NamespaceConflict),
        ("PUBLICATION_CONFLICT", 409, PublicationConflict),
        ("RELEASE_IMMUTABLE", 409, ReleaseImmutable),
        ("RATE_LIMITED", 429, RateLimited),
        ("TEMPORARY_UNAVAILABLE", 503, TemporaryUnavailable),
        // Deployed compatibility code accepted by the legacy parser also
        // needs canonical retry classification when it carries full hints.
        ("INTERNAL", 500, Internal),
    ] {
        let bytes = serde_json::to_vec(&envelope(code)).unwrap();
        let error = CanonicalSnapshotError::parse_response(&bytes, status).unwrap();
        assert_eq!(error.code, expected);
        assert_eq!(error.http_status, status);
        assert_eq!(error.message, "public error");
        assert_eq!(error.request_id, "request-fixture");
        assert!(!error.retryable);
        assert!(CanonicalSnapshotError::parse_response(
            &bytes,
            if status == 404 { 403 } else { 404 }
        )
        .is_err());
    }
    // Deployed aliases stay available through old APIs, but are not SPEC codes.
    assert!(CanonicalSnapshotError::parse_response(
        &serde_json::to_vec(&envelope("CONFLICT")).unwrap(),
        409
    )
    .is_err());
    assert!(CanonicalSnapshotError::parse_response(
        br#"{"error":{"code":"PATH_NOT_FOUND","message":"legacy"}}"#,
        404
    )
    .is_err());
    assert!(CanonicalSnapshotError::parse_response(br#"{"error":{"code":"PATH_NOT_FOUND","message":"legacy","request_id":"a","retryable":false,"retryable":true}}"#, 404).is_err());
    assert_eq!(
        CanonicalSnapshotError::parse_response(&vec![b' '; 1_048_577], 404)
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
}

#[test]
fn public_error_enum_supports_exhaustive_external_matches() {
    fn classify(code: SnapshotErrorCode) -> bool {
        use SnapshotErrorCode::*;
        match code {
            ScopeInvalid | InvalidRequest | LimitExceeded | Unauthenticated | ScopeForbidden
            | ViewNotFound | SnapshotNotReady | MetadataNotReady | SnapshotGone | PathNotFound
            | NotDirectory | UnsupportedEntry | LeaseUnknown | LeaseExpired | CursorInvalid
            | CursorStale | ProofBudgetExceeded | DigestMismatch | IntegrityError
            | ObjectUnavailable | RangeNotSupported | SymlinkTraversal | DurableViewConflict
            | TemporaryUnavailable | Internal => true,
        }
    }
    assert!(classify(SnapshotErrorCode::Internal));
    assert!(classify(SnapshotErrorCode::MetadataNotReady));
}
