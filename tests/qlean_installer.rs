#![cfg(all(feature = "qlean-ci", target_os = "linux", target_arch = "x86_64"))]

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{bail, Context, Result};
use qlean::{with_machine, Image, ImageConfig, MachineConfig};
use serde_json::Value;
use tempfile::tempdir;

const VERSION: &str = "v0.0.0-qlean";
const TARGET: &str = "x86_64-unknown-linux-gnu";
const WORKSPACE_KERNEL_TEST: &str =
    "explicit_snapshot_mounts_keep_old_handles_and_dirty_upper_on_shutdown";

fn cargo_target_dir(repo_root: &Path) -> Result<PathBuf> {
    let output = Command::new("cargo")
        .current_dir(repo_root)
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .context("could not query Cargo for its active target directory")?;
    if !output.status.success() {
        bail!(
            "Cargo metadata failed while resolving the target directory:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let metadata: Value = serde_json::from_slice(&output.stdout)
        .context("Cargo returned invalid metadata while resolving the target directory")?;
    metadata
        .get("target_directory")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .context("Cargo metadata did not include target_directory")
}

fn stage_fixture() -> Result<tempfile::TempDir> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let target_dir = cargo_target_dir(repo_root)?;
    let staging = tempdir().context("could not create the Qlean fixture directory")?;
    fs::create_dir_all(staging.path().join("script"))?;

    for relative in [
        "install.sh",
        "script/test_installer.sh",
        "script/test_installer_systemd.sh",
    ] {
        let source = repo_root.join(relative);
        let destination = staging.path().join(relative);
        fs::copy(&source, &destination).with_context(|| format!("could not stage {relative}"))?;
    }

    let source = target_dir.join("release/scorpio");
    if !source.is_file() {
        bail!("missing {source:?}; build the release binary before running the Qlean test");
    }
    fs::copy(&source, staging.path().join("scorpio"))
        .context("could not stage the scorpio release binary")?;

    // The workflow obtains this exact artifact path from Cargo's compiler
    // output. Do not select a possibly stale test executable by globbing.
    let launcher_tests = std::env::var_os("SCORPIO_LAUNCHER_TEST_EXE")
        .map(PathBuf::from)
        .context("build mst2_workspace_launcher_http and set SCORPIO_LAUNCHER_TEST_EXE")?;
    fs::copy(
        &launcher_tests,
        staging.path().join("workspace-launcher-tests"),
    )
    .context("could not stage the exact workspace launcher test executable")?;

    Ok(staging)
}

async fn run_checked(vm: &mut qlean::Machine, command: &str) -> Result<()> {
    let output = vm
        .exec(command)
        .await
        .with_context(|| format!("Qlean could not execute `{command}`"))?;
    if !output.status.success() {
        bail!(
            "Qlean command failed: `{command}`\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

async fn run_workspace_kernel_checked(vm: &mut qlean::Machine, command: &str) -> Result<()> {
    let output = vm
        .exec(command)
        .await
        .with_context(|| format!("Qlean could not execute `{command}`"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}\n{stderr}");
    let named_result = format!("test {WORKSPACE_KERNEL_TEST} ... ok");
    let markers = [
        "PRE_TRANSACTION_HYDRATION_OBSERVE_RUN:",
        "RUNNING_HYDRATION_OBSERVE_RUN:",
        "RUNNING_HYDRATION_CANCEL_RUN:",
        "SHIPPED_DAEMON_STAGE_METERS_RUN:",
        "SHIPPED_DAEMON_METERS_RUN:",
        "SHIPPED_DAEMON_RELEASE_METERS_RUN:",
    ];
    let diagnostics: Vec<_> = markers
        .iter()
        .filter_map(|marker| combined.lines().find(|line| line.starts_with(marker)))
        .collect();
    if !output.status.success()
        || !stdout.lines().any(|line| line == named_result)
        || !stdout.lines().any(|line| {
            line.starts_with("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;")
        })
        || diagnostics.len() != markers.len()
    {
        bail!(
            "Qlean workspace kernel validation failed or lacks its exact test/diagnostics: `{command}`\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }
    // Forward only verified fixture diagnostics on success. Installer command
    // output stays quiet, and a successful zero-test invocation is rejected.
    for diagnostic in diagnostics {
        eprintln!("{diagnostic}");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Linux host with QEMU/KVM and vhost-vsock"]
async fn installer_runs_inside_an_isolated_vm() -> Result<()> {
    let staging = stage_fixture()?;
    let staging_name = staging
        .path()
        .file_name()
        .and_then(|name| name.to_str())
        .context("Qlean fixture directory has no valid name")?;
    let remote_root = PathBuf::from("/tmp").join(staging_name);

    let image = Image::new(ImageConfig::default())
        .await
        .context("could not prepare the Debian image for Qlean")?;
    let config = MachineConfig::default()
        .with_core(2)
        .with_mem(4096)
        .with_timeout(300);

    with_machine(&image, &config, |vm| {
        Box::pin(async move {
            vm.upload(staging.path(), Path::new("/tmp"))
                .await
                .context("could not upload the installer fixture to Qlean")?;

            let root = remote_root.to_string_lossy();
            run_checked(
                vm,
                "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends bash coreutils curl findutils fuse3 kmod python3 sudo tar util-linux && (grep -qxF user_allow_other /etc/fuse.conf || printf 'user_allow_other\\n' >>/etc/fuse.conf)",
            )
            .await?;
            run_checked(
                vm,
                &format!(
                    "chmod 0755 {root}/install.sh {root}/script/test_installer.sh {root}/script/test_installer_systemd.sh {root}/scorpio {root}/workspace-launcher-tests"
                ),
            )
            .await?;
            run_checked(
                vm,
                &format!(
                    "mkdir -p {root}/release/{VERSION}/scorpiofs-{VERSION}-{TARGET} && cp {root}/scorpio {root}/release/{VERSION}/scorpiofs-{VERSION}-{TARGET}/scorpio && tar -C {root}/release/{VERSION} -czf {root}/release/{VERSION}/scorpiofs-{VERSION}-{TARGET}.tar.gz scorpiofs-{VERSION}-{TARGET} && (cd {root}/release/{VERSION} && sha256sum scorpiofs-{VERSION}-{TARGET}.tar.gz > scorpiofs-{VERSION}-{TARGET}.tar.gz.sha256)"
                ),
            )
            .await?;
            run_checked(
                vm,
                &format!(
                    "bash {root}/script/test_installer.sh"
                ),
            )
            .await?;
            run_checked(
                vm,
                &format!(
                    "set -euo pipefail; nohup python3 -m http.server 18080 --bind 0.0.0.0 --directory {root}/release >/tmp/scorpiofs-qlean-http.log 2>&1 </dev/null & server_pid=$!; sleep 1; if ! kill -0 $server_pid 2>/dev/null; then cat /tmp/scorpiofs-qlean-http.log >&2; exit 1; fi; trap 'kill $server_pid 2>/dev/null || true' EXIT; curl --fail --retry 20 --retry-delay 1 --retry-connrefused http://127.0.0.1:18080/{VERSION}/scorpiofs-{VERSION}-{TARGET}.tar.gz.sha256 >/dev/null; SUDO_USER=nobody bash {root}/script/test_installer_systemd.sh {VERSION} http://127.0.0.1:18080 /tmp/scorpiofs-qlean-systemd"
                ),
            )
            .await?;
            run_workspace_kernel_checked(
                vm,
                &format!(
                    "set -euo pipefail; test -c /dev/fuse || modprobe fuse; test -c /dev/fuse; SCORPIO_LAUNCHER_BINARY={root}/scorpio {root}/workspace-launcher-tests --exact {WORKSPACE_KERNEL_TEST} --ignored --nocapture"
                ),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .context("isolated ScorpioFS installer validation failed")?;

    Ok(())
}
