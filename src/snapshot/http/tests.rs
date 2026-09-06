use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use axum::{
    body::Body,
    extract::{Request, State},
    response::Response,
    Router,
};
use tokio::{net::TcpListener, task::JoinHandle};

use super::*;
use crate::snapshot::{
    backend::{ReadLimits, SourceReader},
    identity::{ObjectFormat, RepoPath, SourceId},
};

#[derive(Clone)]
enum Reply {
    Full(StatusCode, &'static str, Bytes),
    Chunked,
    Redirect,
    Wait,
    Objects(HashMap<String, Bytes>),
}

#[derive(Clone, Debug)]
struct Observed {
    path: String,
    query: HashMap<String, String>,
    authorized: bool,
    leased: bool,
    method: String,
}

#[derive(Clone)]
struct ServerState {
    reply: Reply,
    seen: Arc<Mutex<Vec<Observed>>>,
}

struct TestServer {
    base: Url,
    seen: Arc<Mutex<Vec<Observed>>>,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TestServer {
    async fn start(reply: Reply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = Url::parse(&format!("http://{}/proxy", listener.local_addr().unwrap())).unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = ServerState {
            reply,
            seen: seen.clone(),
        };
        let router = Router::new().fallback(handle).with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { base, seen, task }
    }

    fn backend(&self) -> HttpObjectBackend {
        HttpObjectBackend::new(
            self.base.clone(),
            "fixture-access-token",
            "fixture-lease",
            HttpTimeouts::default(),
        )
        .unwrap()
    }
}

async fn handle(State(state): State<ServerState>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    state.seen.lock().unwrap().push(Observed {
        path: path.clone(),
        query: url::form_urlencoded::parse(request.uri().query().unwrap_or_default().as_bytes())
            .into_owned()
            .collect(),
        authorized: request
            .headers()
            .get(AUTHORIZATION)
            .is_some_and(|v| v == "Bearer fixture-access-token"),
        leased: request
            .headers()
            .get(LEASE_HEADER)
            .is_some_and(|v| v == "fixture-lease"),
        method: request.method().to_string(),
    });
    match &state.reply {
        Reply::Full(status, content_type, bytes) => Response::builder()
            .status(*status)
            .header(CONTENT_TYPE, *content_type)
            .body(Body::from(bytes.clone()))
            .unwrap(),
        Reply::Chunked => {
            let chunks = futures::stream::iter([
                Ok::<_, std::io::Error>(Bytes::from_static(b"abc")),
                Ok(Bytes::from_static(b"def")),
            ]);
            Response::builder()
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(Body::from_stream(chunks))
                .unwrap()
        }
        Reply::Redirect => Response::builder()
            .status(302)
            .header("location", "/redirected")
            .body(Body::empty())
            .unwrap(),
        Reply::Wait => std::future::pending().await,
        Reply::Objects(objects) => {
            let oid = path.rsplit('/').next().unwrap();
            match objects.get(oid) {
                Some(bytes) => Response::builder()
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::from(bytes.clone()))
                    .unwrap(),
                None => Response::builder().status(404).body(Body::empty()).unwrap(),
            }
        }
    }
}

fn source() -> SourceSnapshot {
    SourceSnapshot {
        source_id: SourceId::new("11111111-1111-4111-8111-111111111111").unwrap(),
        scope_path: RepoPath::new("/third-party/库+1\\literal").unwrap(),
        object_format: ObjectFormat::Sha1,
        commit_oid: ObjectId::new("1".repeat(40)).unwrap(),
        root_tree_oid: ObjectId::new("2".repeat(40)).unwrap(),
    }
}

async fn fetch(server: &TestServer, limit: usize) -> Result<Bytes, SnapshotReadError> {
    let source = source();
    server
        .backend()
        .fetch(
            &source,
            ObjectKind::Blob,
            &source.commit_oid,
            &RelativePath::new("src/a+b.rs").unwrap(),
            limit,
        )
        .await
}

#[tokio::test]
async fn request_binds_fixed_descriptor_path_and_sensitive_headers() {
    let server = TestServer::start(Reply::Full(
        StatusCode::OK,
        "application/octet-stream",
        Bytes::from_static(b"blob 4\0data"),
    ))
    .await;
    let result = fetch(&server, 1024).await.unwrap();
    assert_eq!(result.as_ref(), b"blob 4\0data");
    let source = source();
    let seen = server.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(
        seen[0].path,
        format!(
            "/proxy/api/v1/sources/{}/blobs/{}",
            source.source_id, source.commit_oid
        )
    );
    assert_eq!(seen[0].query["scope_path"], source.scope_path.as_str());
    assert_eq!(seen[0].query["source_path"], "src/a+b.rs");
    assert_eq!(
        seen[0].query["root_tree_oid"],
        source.root_tree_oid.as_str()
    );
    assert_eq!(seen[0].query["commit_oid"], source.commit_oid.as_str());
    assert_eq!(seen[0].query["object_format"], "sha1");
    assert_eq!(seen[0].query.len(), 5);
    assert!(seen[0].authorized && seen[0].leased);
    assert_eq!(seen[0].method, "GET");
    let backend = server.backend();
    assert!(backend.authorization.is_sensitive() && backend.lease.is_sensitive());
    assert!(!format!("{backend:?}").contains("fixture-access-token"));
    assert!(!format!("{backend:?}").contains("fixture-lease"));
}

#[tokio::test]
async fn error_statuses_do_not_become_empty_successes_or_latest_fallbacks() {
    for status in [401, 403, 404, 410, 429, 500, 503, 204, 206] {
        let server = TestServer::start(Reply::Full(
            StatusCode::from_u16(status).unwrap(),
            "application/octet-stream",
            Bytes::new(),
        ))
        .await;
        let result = fetch(&server, 1024).await;
        match status {
            401 | 403 => assert!(matches!(result, Err(SnapshotReadError::Forbidden))),
            410 => assert!(matches!(result, Err(SnapshotReadError::Expired))),
            _ => assert!(matches!(result, Err(SnapshotReadError::Unavailable(_)))),
        }
        assert_eq!(server.seen.lock().unwrap().len(), 1);
    }
    let redirect = TestServer::start(Reply::Redirect).await;
    assert!(matches!(
        fetch(&redirect, 1024).await,
        Err(SnapshotReadError::Unavailable(_))
    ));
    assert_eq!(redirect.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn full_and_chunked_body_limits_and_content_type_are_checked() {
    for reply in [
        Reply::Full(
            StatusCode::OK,
            "application/octet-stream",
            Bytes::from_static(b"abcdef"),
        ),
        Reply::Chunked,
    ] {
        let server = TestServer::start(reply).await;
        assert!(matches!(
            fetch(&server, 3).await,
            Err(SnapshotReadError::ObjectTooLarge { limit: 3 })
        ));
    }
    let server = TestServer::start(Reply::Full(
        StatusCode::OK,
        "application/json",
        Bytes::from_static(b"{}"),
    ))
    .await;
    assert!(matches!(
        fetch(&server, 1024).await,
        Err(SnapshotReadError::Unavailable(_))
    ));
    let empty = TestServer::start(Reply::Full(
        StatusCode::OK,
        "application/octet-stream",
        Bytes::new(),
    ))
    .await;
    assert!(fetch(&empty, 1024).await.unwrap().is_empty());
    assert!(matches!(
        fetch(&empty, 0).await,
        Err(SnapshotReadError::InvalidLimits)
    ));
    assert_eq!(empty.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn transport_deadline_has_a_redacted_error() {
    let server = TestServer::start(Reply::Wait).await;
    let backend = HttpObjectBackend::new(
        server.base.clone(),
        "fixture-access-token",
        "fixture-lease",
        HttpTimeouts {
            connect: Duration::from_secs(1),
            request: Duration::from_millis(100),
        },
    )
    .unwrap();
    let source = source();
    let error = backend
        .fetch(
            &source,
            ObjectKind::Tree,
            &source.root_tree_oid,
            &RelativePath::new("").unwrap(),
            1024,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "object is unavailable: snapshot transport timed out"
    );
}

#[test]
fn insecure_origins_ambiguous_urls_credentials_and_zero_timeouts_are_rejected() {
    for url in [
        "http://example.com",
        "https://user:secret@example.com",
        "https://example.com?token=secret",
        "https://example.com/#fragment",
        "file:///tmp/objects",
    ] {
        assert!(HttpObjectBackend::new(
            Url::parse(url).unwrap(),
            "token",
            "lease",
            HttpTimeouts::default()
        )
        .is_err());
    }
    for (token, lease) in [
        ("", "lease"),
        ("token", ""),
        ("token\r\ninjected", "lease"),
        ("token", "lease with space"),
    ] {
        assert!(HttpObjectBackend::new(
            Url::parse("https://example.com").unwrap(),
            token,
            lease,
            HttpTimeouts::default()
        )
        .is_err());
    }
    assert!(HttpObjectBackend::new(
        Url::parse("https://example.com").unwrap(),
        "token",
        "lease",
        HttpTimeouts {
            connect: Duration::ZERO,
            request: Duration::from_secs(1)
        }
    )
    .is_err());
}

#[tokio::test]
async fn reader_verifies_real_http_bytes_against_git_object_ids() {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    fn git_oid(kind: &str, bytes: &[u8]) -> ObjectId {
        let mut child = Command::new("git")
            .args(["hash-object", "--stdin", "-t", kind])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(bytes).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        ObjectId::new(String::from_utf8(output.stdout).unwrap().trim()).unwrap()
    }
    let payload = Bytes::from_static(b"old fixed content");
    let blob = git_oid("blob", &payload);
    let tree_payload = Bytes::from(
        [
            b"100644 file.txt\0".as_slice(),
            &hex::decode(blob.as_str()).unwrap(),
        ]
        .concat(),
    );
    let root = git_oid("tree", &tree_payload);
    let server = TestServer::start(Reply::Objects(HashMap::from([
        (blob.to_string(), payload.clone()),
        (root.to_string(), tree_payload),
    ])))
    .await;
    let source = SourceSnapshot {
        root_tree_oid: root,
        ..source()
    };
    let reader = SourceReader::new(
        source.clone(),
        Arc::new(server.backend()),
        ReadLimits::default(),
    )
    .unwrap();
    assert_eq!(
        reader
            .read_file(&RepoPath::new(format!("{}/file.txt", source.scope_path)).unwrap())
            .await
            .unwrap(),
        payload
    );
    let seen = server.seen.lock().unwrap();
    assert!(seen
        .iter()
        .all(|r| r.query["commit_oid"] == source.commit_oid.as_str()));
    assert!(seen.iter().any(|r| r.query["source_path"].is_empty()));
    assert!(seen.iter().any(|r| r.query["source_path"] == "file.txt"));
}
