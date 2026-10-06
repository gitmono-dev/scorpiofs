//! Explicit compatibility commands for existing Antares clients.
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};

use super::exit;
use crate::{
    antares::{AntaresManager, AntaresPaths},
    daemon::antares::AntaresServiceImpl,
};

/// Build the config CLI-override map from the optional Antares path flags.
///
/// Returning these as config overrides (rather than mutating `AntaresPaths`
/// after the fact) keeps the documented precedence `CLI > env > file > default`
/// intact and ensures runtime directories are created for the effective paths.
pub fn antares_overrides(
    upper_root: Option<PathBuf>,
    cl_root: Option<PathBuf>,
    mount_root: Option<PathBuf>,
    state_file: Option<PathBuf>,
) -> HashMap<String, String> {
    let mut overrides = HashMap::new();
    if let Some(p) = upper_root {
        overrides.insert("antares_upper_root".to_string(), p.display().to_string());
    }
    if let Some(p) = cl_root {
        overrides.insert("antares_cl_root".to_string(), p.display().to_string());
    }
    if let Some(p) = mount_root {
        overrides.insert("antares_mount_root".to_string(), p.display().to_string());
    }
    if let Some(p) = state_file {
        overrides.insert("antares_state_file".to_string(), p.display().to_string());
    }
    overrides
}

/// Mount an Antares job instance directly via the manager.
pub async fn antares_mount(job_id: &str, cl: Option<&str>) -> i32 {
    let manager = AntaresManager::new(AntaresPaths::from_global_config()).await;
    match manager.mount_job(job_id, cl).await {
        Ok(instance) => {
            println!("mounted job {job_id} at {}", instance.mountpoint.display());
            exit::SUCCESS
        }
        Err(err) => {
            eprintln!("failed to mount job {job_id}: {err}");
            exit::MOUNT
        }
    }
}

/// Unmount an Antares job instance.
pub async fn antares_umount(job_id: &str) -> i32 {
    let manager = AntaresManager::new(AntaresPaths::from_global_config()).await;
    match manager.umount_job(job_id).await {
        Ok(Some(_)) => {
            println!("unmounted job {job_id}");
            exit::SUCCESS
        }
        Ok(None) => {
            eprintln!("job {job_id} not found");
            exit::MOUNT
        }
        Err(err) => {
            eprintln!("failed to unmount job {job_id}: {err}");
            exit::MOUNT
        }
    }
}

/// List tracked Antares job instances.
pub async fn antares_list() -> i32 {
    let manager = AntaresManager::new(AntaresPaths::from_global_config()).await;
    let items = manager.list().await;
    if items.is_empty() {
        println!("no active jobs");
    } else {
        for it in items {
            let cl = it
                .cl_dir
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "(none)".to_string());
            println!(
                "job_id={} mount={} upper={} cl={}",
                it.job_id,
                it.mountpoint.display(),
                it.upper_dir.display(),
                cl
            );
        }
    }
    exit::SUCCESS
}

/// Run the standalone Antares HTTP daemon (the `antares serve` form).
pub async fn antares_serve(addr: SocketAddr) -> i32 {
    use crate::daemon::antares::AntaresDaemon;

    // Bind up-front so a bind failure maps to the dedicated exit code (4).
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("failed to bind Antares HTTP address {addr}: {e}");
            return exit::BIND;
        }
    };
    tracing::info!("starting Antares daemon on {addr}");

    let service = Arc::new(AntaresServiceImpl::new(None).await);
    let daemon = AntaresDaemon::new(service);
    if let Err(e) = daemon.serve_with_listener(listener).await {
        tracing::error!("daemon error: {e}");
        return exit::INTERNAL;
    }
    exit::SUCCESS
}

/// Mount via a running HTTP daemon (recommended for build systems). `endpoint`
/// is the base URL; the request is sent to `{endpoint}/mounts`.
pub async fn http_mount(job_id: Option<&str>, path: &str, cl: Option<&str>, endpoint: &str) -> i32 {
    // Use the async reqwest client: this runs inside the binaries' tokio
    // runtime, where reqwest::blocking would panic.
    let client = reqwest::Client::new();
    let url = format!("{}/mounts", endpoint.trim_end_matches('/'));
    let payload = serde_json::json!({
        "job_id": job_id,
        "path": path,
        "cl": cl,
    });

    match client
        .post(url)
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => match r.json::<serde_json::Value>().await {
            Ok(v) => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&v).unwrap_or_else(|_| v.to_string())
                );
                exit::SUCCESS
            }
            Err(e) => {
                eprintln!("failed to parse response json: {e}");
                exit::INTERNAL
            }
        },
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            eprintln!("http mount failed: status={status} body={body}");
            exit::MOUNT
        }
        Err(e) => {
            eprintln!("http mount request failed: {e}");
            exit::INTERNAL
        }
    }
}
