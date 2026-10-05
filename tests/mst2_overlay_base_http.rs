//! A snapshot mount must not initialize the unrelated dictionary projection.
#![cfg(unix)]

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use axum::{
    extract::{Request, State},
    http::StatusCode,
    response::IntoResponse,
    Router,
};
use scorpiofs::{
    daemon::antares::{AntaresService, AntaresServiceImpl, CreateMountRequest},
    util::config,
};

#[derive(Default)]
struct Requests {
    paths: Mutex<Vec<String>>,
    reject_discovery: AtomicBool,
}

async fn reply(State(state): State<Arc<Requests>>, request: Request) -> impl IntoResponse {
    let path = request.uri().path();
    state.paths.lock().unwrap().push(path.into());
    match path {
        "/api/v2/snapshots/capabilities" => (
            StatusCode::OK,
            [("content-type", "application/json")],
            if state.reject_discovery.load(Ordering::SeqCst) {
                "{}"
            } else {
                include_str!("fixtures/mst2_capabilities_0_2_1.json")
            },
        ),
        // A malformed selected resolve contract is terminal. A dictionary
        // import must not precede it or appear as a fallback after it.
        "/api/v2/snapshots/resolve" => (
            StatusCode::OK,
            [("content-type", "application/json")],
            "{\"publication_sequence\":null}",
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "application/json")],
            "unexpected dictionary request",
        ),
    }
}

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test]
async fn failed_snapshot_mounts_never_import_or_fall_back_to_a_dictionary() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("scorpio.toml");
    std::fs::write(&config_path, "").unwrap();
    let requests = Arc::new(Requests::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().fallback(reply).with_state(requests.clone());
    let _server = Server(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));

    let store = temp.path().join("dictionary-store");
    let mut overrides: std::collections::HashMap<String, String> = [
        ("base_url".into(), base_url.clone()),
        ("mst2_base_url".into(), base_url),
        ("mst2_lower_enabled".into(), "true".into()),
        ("mst2_scope".into(), "/project/app".into()),
    ]
    .into();
    for (key, name) in [
        ("workspace", "workspace"),
        ("store_path", "dictionary-store"),
        ("config_file", "runtime-state.toml"),
        ("antares_upper_root", "upper"),
        ("antares_cl_root", "cl"),
        ("antares_mount_root", "mounts"),
        ("antares_state_file", "antares-state.toml"),
    ] {
        overrides.insert(key.into(), temp.path().join(name).to_str().unwrap().into());
    }
    config::init_config_with(config_path.to_str().unwrap(), overrides).unwrap();

    let service = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        AntaresServiceImpl::new_external_state(None),
    )
    .await
    .expect("snapshot service construction must not load the dictionary");
    assert!(requests.paths.lock().unwrap().is_empty());
    assert_eq!(std::fs::read_dir(&store).unwrap().count(), 0);

    for reject_discovery in [true, false] {
        requests
            .reject_discovery
            .store(reject_discovery, Ordering::SeqCst);
        requests.paths.lock().unwrap().clear();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            service.create_mount(CreateMountRequest {
                job_id: Some("snapshot-mount".into()),
                build_id: None,
                path: "/project/app".into(),
                cl_path: None,
                cl: None,
                mountpoint: None,
                upper_dir: None,
                pinned_refs: None,
                sealed_chain: Vec::new(),
            }),
        )
        .await
        .expect("a rejected snapshot must not wait for dictionary initialization");
        assert!(result.is_err(), "invalid snapshot contract must fail");
        let paths = requests.paths.lock().unwrap().clone();
        if reject_discovery {
            assert_eq!(paths, ["/api/v2/snapshots/capabilities"]);
        } else {
            assert_eq!(
                paths,
                [
                    "/api/v2/snapshots/capabilities",
                    "/api/v2/snapshots/resolve"
                ]
            );
        }
        assert!(service.list_mounts().await.unwrap().is_empty());
        assert_eq!(std::fs::read_dir(&store).unwrap().count(), 0);
    }
}
