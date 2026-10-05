//! SPEC discovery and explicit legacy compatibility over actual HTTP.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{
    body::Body, extract::State, http::StatusCode, response::Response, routing::get, Router,
};
use scorpiofs::snapshot::{
    capabilities::CapabilityAdvertisement,
    types::{Capabilities, CapabilityFeatures},
    Mst2Client, SnapshotErrorCode,
};
use serde_json::{json, Value};

fn canonical() -> Value {
    serde_json::from_str(include_str!("fixtures/mst2_capabilities_0_2_1.json")).unwrap()
}

fn legacy() -> Value {
    json!({
        "protocol_versions": [2], "metadata_codecs": [1],
        "frame_encodings": ["identity", "zstd"],
        "features": {"resolve": true, "directory": true, "leases": true,
            "lookup": true, "metadata_pages": true, "raw_blob": true,
            "objects": true, "chunk_reads": true, "full_hydration": false,
            "offline_export": false, "bindings": false, "immutable_release": false},
        "limits": {"metadata_page_bytes": 16384, "metadata_leaf_entries": 128,
            "max_directory_page_limit": 256, "chunk_size": 1048576,
            "small_object_bytes": 262144},
        "extension": [{"a": 1}, {"a": 2}]
    })
}

struct Server {
    client: Mst2Client,
    requests: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn raw(body: String, status: StatusCode) -> Self {
        let requests = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/v2/snapshots/capabilities",
                get(move |State(requests): State<Arc<AtomicUsize>>| {
                    let body = body.clone();
                    async move {
                        requests.fetch_add(1, Ordering::SeqCst);
                        Response::builder()
                            .status(status)
                            .header("content-type", "application/json")
                            .body(Body::from(body))
                            .unwrap()
                    }
                }),
            )
            .with_state(requests.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            client,
            requests,
            task,
        }
    }

    async fn json(value: Value) -> Self {
        Self::raw(value.to_string(), StatusCode::OK).await
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn reject(value: Value) {
    let server = Server::json(value).await;
    let error = server.client.capability_advertisement().await.unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::IntegrityError);
    assert_eq!(
        server.requests.load(Ordering::SeqCst),
        1,
        "no fallback HTTP request"
    );
}

#[tokio::test]
async fn frozen_canonical_discovery_exposes_actual_features_and_limits() {
    let server = Server::json(canonical()).await;
    let CapabilityAdvertisement::Canonical(caps) =
        server.client.capability_advertisement().await.unwrap()
    else {
        panic!("canonical fixture must select canonical contract")
    };
    assert_eq!(caps.protocol_versions(), &[2]);
    assert_eq!(caps.metadata_codecs(), &[1]);
    assert_eq!(caps.frame_encodings(), &["identity"]);
    assert!(caps.features().strict_publication && caps.features().small_objects);
    assert!(!caps.features().offline_export && !caps.features().region_hints);
    assert_eq!(caps.limits().max_file_bytes, 8_796_093_022_208);
    assert_eq!(caps.limits().chunk_frame_raw_bytes, 1_048_652);
    assert_eq!(caps.limits().max_metadata_items, 64);
    assert_eq!(caps.limits().chunk_batch_bytes, 134_217_728);
    let mut value = canonical();
    value["frame_encodings"] = json!(["zstd", "identity"]);
    value["features"]["directory"] = json!(false);
    value["features"]["offline_export"] = json!(true);
    let server = Server::json(value).await;
    let CapabilityAdvertisement::Canonical(caps) =
        server.client.capability_advertisement().await.unwrap()
    else {
        panic!("canonical")
    };
    assert_eq!(caps.frame_encodings(), &["zstd", "identity"]);
    assert!(!caps.features().directory);
    assert!(caps.features().offline_export);
}

#[tokio::test]
async fn legacy_partial_limits_and_public_struct_literals_remain_compatible() {
    let server = Server::json(legacy()).await;
    let CapabilityAdvertisement::Legacy(caps) =
        server.client.capability_advertisement().await.unwrap()
    else {
        panic!("partial legacy limits cannot be treated as canonical")
    };
    assert!(caps.features.resolve && caps.features.leases && caps.features.objects);
    assert!(!caps.features.full_hydration);
    let _: Capabilities = server.client.capabilities().await.unwrap();
    let _: Capabilities = Capabilities {
        protocol_versions: vec![2],
        metadata_codecs: vec![1],
        frame_encodings: vec!["identity".into()],
        features: CapabilityFeatures {
            resolve: true,
            directory: true,
            leases: true,
            lookup: false,
            metadata_pages: false,
            raw_blob: false,
            objects: false,
            chunk_reads: false,
            full_hydration: false,
        },
    };
    let server = Server::json(json!({"protocol_versions": [2], "metadata_codecs": [1],
        "frame_encodings": ["identity"],
        "features": {"resolve": true, "directory": true, "leases": true}}))
    .await;
    assert!(matches!(
        server.client.capability_advertisement().await.unwrap(),
        CapabilityAdvertisement::Legacy(_)
    ));
}

#[tokio::test]
async fn canonical_contract_is_closed_and_never_falls_back_to_legacy() {
    let fixture = canonical();
    for key in fixture.as_object().unwrap().keys() {
        let mut value = fixture.clone();
        value.as_object_mut().unwrap().remove(key);
        reject(value).await;
    }
    for group in ["features", "limits"] {
        for key in fixture[group].as_object().unwrap().keys() {
            let mut value = fixture.clone();
            value[group].as_object_mut().unwrap().remove(key);
            reject(value).await;
            let mut value = fixture.clone();
            value[group][key] = Value::Null;
            reject(value).await;
        }
        let mut value = fixture.clone();
        value[group]["unknown"] = json!(true);
        reject(value).await;
    }
    let mut value = fixture.clone();
    value["unknown"] = json!(true);
    reject(value).await;
    for feature in ["resolve", "leases", "objects"] {
        let mut value = fixture.clone();
        value["features"][feature] = json!(true);
        reject(value).await;
    }
    for feature in ["strict_publication", "small_objects", "region_hints"] {
        let mut value = legacy();
        value["features"][feature] = json!(true);
        reject(value).await;
    }
    let mut value = legacy();
    value["limits"] = fixture["limits"].clone();
    reject(value).await;
    let mut value = fixture;
    value["features"]["strict_publication"] = json!("true");
    reject(value).await;
}

#[tokio::test]
async fn codec_constants_are_fixed_and_operational_limits_only_tighten() {
    let fixed = [
        "metadata_page_bytes",
        "metadata_leaf_entries",
        "small_object_bytes",
        "object_frame_raw_bytes",
        "chunk_frame_raw_bytes",
        "chunk_size",
    ];
    let fixture = canonical();
    for (key, original) in fixture["limits"].as_object().unwrap() {
        if key == "max_file_bytes" {
            continue;
        }
        let maximum = original.as_u64().unwrap();
        for bad in [
            json!(0),
            json!(maximum + 1),
            json!(-1),
            json!(maximum.to_string()),
            json!(1.5),
            json!(true),
        ] {
            let mut value = fixture.clone();
            value["limits"][key] = bad;
            reject(value).await;
        }
        let mut value = fixture.clone();
        value["limits"][key] = json!(maximum - 1);
        if fixed.contains(&key.as_str()) {
            reject(value).await;
        } else {
            let server = Server::json(value).await;
            assert!(matches!(
                server.client.capability_advertisement().await.unwrap(),
                CapabilityAdvertisement::Canonical(_)
            ));
            let mut value = fixture.clone();
            value["limits"][key] = json!(1);
            let server = Server::json(value).await;
            assert!(server.client.capability_advertisement().await.is_ok());
        }
    }
    for bad in [
        json!("8796093022209"),
        json!("9223372036854775808"),
        json!("18446744073709551616"),
        json!("01"),
        json!("-1"),
        json!("1e3"),
        json!("١"),
        json!(""),
        json!(8796093022208u64),
    ] {
        let mut value = fixture.clone();
        value["limits"]["max_file_bytes"] = bad;
        reject(value).await;
    }
    for good in ["0", "1", "8796093022207"] {
        let mut value = fixture.clone();
        value["limits"]["max_file_bytes"] = json!(good);
        let server = Server::json(value).await;
        let CapabilityAdvertisement::Canonical(caps) =
            server.client.capability_advertisement().await.unwrap()
        else {
            panic!("canonical")
        };
        assert_eq!(caps.limits().max_file_bytes, good.parse::<u64>().unwrap());
    }
}

#[tokio::test]
async fn profile_negotiation_rejects_unknown_duplicate_or_missing_encodings() {
    for (field, bad) in [
        ("protocol_versions", json!([])),
        ("protocol_versions", json!([1])),
        ("protocol_versions", json!([2, 2])),
        ("protocol_versions", json!([2, 3])),
        ("metadata_codecs", json!([])),
        ("metadata_codecs", json!([2])),
        ("metadata_codecs", json!([1, 1])),
        ("metadata_codecs", json!(["1"])),
        ("frame_encodings", json!([])),
        ("frame_encodings", json!(["zstd"])),
        ("frame_encodings", json!(["identity", "identity"])),
        ("frame_encodings", json!(["identity", "gzip"])),
        ("frame_encodings", json!(["identity", "zstd", "identity"])),
    ] {
        let mut value = canonical();
        value[field] = bad;
        reject(value).await;
    }
}

#[tokio::test]
async fn discovery_uses_existing_duplicate_key_body_budget_and_typed_error_checks() {
    let body = canonical().to_string();
    let duplicate = body.replace(
        "\"strict_publication\":true",
        "\"strict_publication\":true,\"strict_publication\":false",
    );
    assert_ne!(duplicate, body);
    for malformed in [
        duplicate,
        format!("{body} {{}}"),
        format!("{} ", " ".repeat(1_048_576)),
    ] {
        let oversize = malformed.len() > 1_048_576;
        let server = Server::raw(malformed, StatusCode::OK).await;
        assert_eq!(
            server
                .client
                .capability_advertisement()
                .await
                .unwrap_err()
                .code,
            if oversize {
                SnapshotErrorCode::LimitExceeded
            } else {
                SnapshotErrorCode::IntegrityError
            }
        );
    }
    let server = Server::raw(
        json!({"error": {"code": "SCOPE_FORBIDDEN", "message": "denied"}}).to_string(),
        StatusCode::FORBIDDEN,
    )
    .await;
    let error = server.client.capability_advertisement().await.unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::ScopeForbidden);
    assert_eq!(error.http_status, 403);
}
