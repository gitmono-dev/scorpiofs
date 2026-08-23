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

    for binary in ["scorpio", "antares"] {
        let source = target_dir.join("release").join(binary);
        let destination = staging.path().join(binary);
        if !source.is_file() {
            bail!("missing {source:?}; build the release binaries before running the Qlean test");
        }
        fs::copy(&source, &destination)
            .with_context(|| format!("could not stage release binary {binary}"))?;
    }

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
                "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends bash coreutils curl findutils fuse3 python3 sudo tar util-linux && (grep -qxF user_allow_other /etc/fuse.conf || printf 'user_allow_other\\n' >>/etc/fuse.conf)",
            )
            .await?;
            run_checked(
                vm,
                &format!(
                    "chmod 0755 {root}/install.sh {root}/script/test_installer.sh {root}/script/test_installer_systemd.sh"
                ),
            )
            .await?;
            run_checked(
                vm,
                &format!(
                    "mkdir -p {root}/release/{VERSION}/scorpiofs-{VERSION}-{TARGET} && cp {root}/scorpio {root}/release/{VERSION}/scorpiofs-{VERSION}-{TARGET}/scorpio && cp {root}/antares {root}/release/{VERSION}/scorpiofs-{VERSION}-{TARGET}/antares && tar -C {root}/release/{VERSION} -czf {root}/release/{VERSION}/scorpiofs-{VERSION}-{TARGET}.tar.gz scorpiofs-{VERSION}-{TARGET} && (cd {root}/release/{VERSION} && sha256sum scorpiofs-{VERSION}-{TARGET}.tar.gz > scorpiofs-{VERSION}-{TARGET}.tar.gz.sha256)"
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
                    "set -euo pipefail; nohup python3 -m http.server 18080 --bind 0.0.0.0 --directory {root}/release >/tmp/scorpiofs-qlean-http.log 2>&1 </dev/null & server_pid=$!; sleep 1; if ! kill -0 $server_pid 2>/dev/null; then cat /tmp/scorpiofs-qlean-http.log >&2; exit 1; fi; trap 'kill $server_pid 2>/dev/null || true' EXIT; curl --fail --retry 20 --retry-delay 1 http://127.0.0.1:18080/{VERSION}/scorpiofs-{VERSION}-{TARGET}.tar.gz.sha256 >/dev/null; SUDO_USER=nobody bash {root}/script/test_installer_systemd.sh {VERSION} http://127.0.0.1:18080 /tmp/scorpiofs-qlean-systemd"
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
