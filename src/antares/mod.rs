//! # Antares: Union Filesystem Overlay Manager
//!
//! Antares provides a union filesystem overlay system for managing copy-on-write
//! workspaces on top of a read-only base (Dicfuse). It is designed for monorepo
//! build systems where each build job needs an isolated writable view of the
//! source tree without actually modifying the base files.
//!
//! ## Key Components
//!
//! - [`AntaresPaths`]: Configuration for layer and state directories
//! - [`AntaresConfig`]: Per-mount configuration (job_id, paths, etc.)
//! - [`AntaresManager`]: Manages mount lifecycle (create, unmount, list)
//!
//! ## Layer Stack
//!
//! Antares composes a three-layer union filesystem:
//!
//! ```text
//! ┌─────────────────┐
//! │   upper (rw)    │  ← Job-specific writes
//! ├─────────────────┤
//! │    CL (rw)      │  ← Optional changelist overlay
//! ├─────────────────┤
//! │  Dicfuse (ro)   │  ← Base monorepo tree
//! └─────────────────┘
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! use scorpiofs::antares::{AntaresManager, AntaresPaths};
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     let paths = AntaresPaths::from_global_config();
//!     let manager = AntaresManager::new(paths).await;
//!     
//!     // Mount with auto-generated path (under configured mount_root)
//!     let config = manager.mount_job("build-42", Some("cl-123")).await?;
//!     println!("Mounted at: {}", config.mountpoint.display());
//!     
//!     // Or mount to any custom directory
//!     let custom_config = manager.mount_job_at(
//!         "build-43",
//!         PathBuf::from("/home/user/my-workspace"),
//!         None,
//!     ).await?;
//!     
//!     // Later, unmount
//!     manager.umount_job("build-42").await?;
//!     manager.umount_job("build-43").await?;
//!     Ok(())
//! }
//! ```

pub mod fuse;

use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

/// The FUSE unmount helper to invoke, resolved once.
///
/// Prefers fuse3's `fusermount3` — which this project's fuse3-based stack uses
/// and which the deployment artifacts (Dockerfile / install.sh / systemd)
/// install — and falls back to fuse2's `fusermount` for fuse2-only hosts.
pub(crate) fn fusermount_bin() -> &'static str {
    use std::sync::OnceLock;
    static BIN: OnceLock<&'static str> = OnceLock::new();
    BIN.get_or_init(|| {
        if binary_on_path("fusermount3") {
            "fusermount3"
        } else if binary_on_path("fusermount") {
            "fusermount"
        } else {
            // Neither present; default to the modern helper so any resulting
            // error names what the docs tell users to install.
            "fusermount3"
        }
    })
}

fn binary_on_path(name: &str) -> bool {
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(name).is_file()))
        .unwrap_or(false)
}

use crate::{
    dicfuse::{Dicfuse, DicfuseManager},
    util::config,
};

fn unmount_grace_duration() -> std::time::Duration {
    const DEFAULT_MS: u64 = 150;
    match std::env::var("ANTARES_UNMOUNT_GRACE_MS") {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(ms) => std::time::Duration::from_millis(ms.clamp(0, 3_000)),
            Err(_) => {
                tracing::warn!(
                    value = %raw,
                    default_ms = DEFAULT_MS,
                    "invalid ANTARES_UNMOUNT_GRACE_MS, using default"
                );
                std::time::Duration::from_millis(DEFAULT_MS)
            }
        },
        Err(_) => std::time::Duration::from_millis(DEFAULT_MS),
    }
}

/// Global paths used by Antares to place layers and state.
#[derive(Debug, Clone)]
pub struct AntaresPaths {
    /// Root directory to place per-job upper layers.
    pub upper_root: PathBuf,
    /// Root directory to place per-job CL layers when requested.
    pub cl_root: PathBuf,
    /// Base directory for mountpoints returned to callers.
    pub mount_root: PathBuf,
    /// Path to persist mount state as TOML.
    pub state_file: PathBuf,
}

impl AntaresPaths {
    pub fn new(
        upper_root: PathBuf,
        cl_root: PathBuf,
        mount_root: PathBuf,
        state_file: PathBuf,
    ) -> Self {
        Self {
            upper_root,
            cl_root,
            mount_root,
            state_file,
        }
    }

    /// Build paths using global config defaults.
    pub fn from_global_config() -> Self {
        Self {
            upper_root: PathBuf::from(config::antares_upper_root()),
            cl_root: PathBuf::from(config::antares_cl_root()),
            mount_root: PathBuf::from(config::antares_mount_root()),
            state_file: PathBuf::from(config::antares_state_file()),
        }
    }
}

/// Persisted config for a mounted Antares job instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AntaresConfig {
    pub job_id: String,
    pub mountpoint: PathBuf,
    pub upper_id: String,
    pub upper_dir: PathBuf,
    pub cl_dir: Option<PathBuf>,
    pub cl_id: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AntaresState {
    mounts: Vec<AntaresConfig>,
}

/// Manager responsible for creating and tracking Antares overlay instances.
pub struct AntaresManager {
    dic: Arc<Dicfuse>,
    paths: AntaresPaths,
    instances: Arc<Mutex<HashMap<String, AntaresConfig>>>,
    /// Active FUSE handles keyed by job_id. Stored separately from `AntaresConfig`
    /// because `AntaresFuse` is not serializable.
    fuse_handles: Arc<Mutex<HashMap<String, fuse::AntaresFuse>>>,
}

impl AntaresManager {
    /// Build an independent Antares manager with its own Dicfuse instance.
    pub async fn new(paths: AntaresPaths) -> Self {
        let dic = DicfuseManager::global().await;
        let instances = Self::load_state(&paths.state_file).unwrap_or_default();
        Self {
            dic,
            paths,
            instances: Arc::new(Mutex::new(instances)),
            fuse_handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create directories and register a job instance with default mountpoint.
    ///
    /// The mountpoint will be created at `{mount_root}/{job_id}` using the
    /// configured mount root directory.
    ///
    /// # Arguments
    /// * `job_id` - Unique identifier for this job
    /// * `cl_name` - Optional CL (changelist) layer name
    ///
    /// # Example
    /// ```rust,ignore
    /// let config = manager.mount_job("build-123", Some("cl-456")).await?;
    /// // Mountpoint will be at: {mount_root}/build-123
    /// ```
    pub async fn mount_job(
        &self,
        job_id: &str,
        cl_name: Option<&str>,
    ) -> std::io::Result<AntaresConfig> {
        let mountpoint = self.paths.mount_root.join(job_id);
        self.mount_job_at(job_id, mountpoint, cl_name).await
    }

    /// Create directories and register a job instance at a custom mountpoint.
    ///
    /// Unlike [`mount_job`], this method allows specifying any directory as
    /// the mountpoint, not limited to the configured mount root.
    ///
    /// # Arguments
    /// * `job_id` - Unique identifier for this job
    /// * `mountpoint` - Custom path where the filesystem will be mounted
    /// * `cl_name` - Optional CL (changelist) layer name
    ///
    /// # Example
    /// ```rust,ignore
    /// let config = manager.mount_job_at(
    ///     "build-123",
    ///     PathBuf::from("/home/user/my-build"),
    ///     None
    /// ).await?;
    /// ```
    pub async fn mount_job_at(
        &self,
        job_id: &str,
        mountpoint: impl Into<PathBuf>,
        cl_name: Option<&str>,
    ) -> std::io::Result<AntaresConfig> {
        let mountpoint = mountpoint.into();
        let start = std::time::Instant::now();
        tracing::info!(
            "antares: mount_job_at start job_id={} mountpoint={} cl={:?}",
            job_id,
            mountpoint.display(),
            cl_name
        );

        // Prepare per-job paths
        let upper_id = Uuid::new_v4().to_string();
        let upper_dir = self.paths.upper_root.join(&upper_id);
        let (cl_id, cl_dir) = match cl_name {
            Some(_) => {
                let id = Uuid::new_v4().to_string();
                (Some(id.clone()), Some(self.paths.cl_root.join(id)))
            }
            None => (None, None),
        };

        std::fs::create_dir_all(&upper_dir)?;
        if let Some(cl) = &cl_dir {
            std::fs::create_dir_all(cl)?;
        }
        std::fs::create_dir_all(&mountpoint)?;

        // Wait for Dicfuse directory tree to be fully loaded before mounting.
        // Without this, the FUSE mount would start with an empty directory tree
        // and callers would get "file not found" errors (e.g., buck2 looking for .buckconfig).
        const DICFUSE_INIT_TIMEOUT_SECS: u64 = 120;
        tracing::info!(
            "antares: mount_job_at waiting for Dicfuse ready (timeout: {}s)",
            DICFUSE_INIT_TIMEOUT_SECS
        );
        match tokio::time::timeout(
            std::time::Duration::from_secs(DICFUSE_INIT_TIMEOUT_SECS),
            self.dic.store.wait_for_ready(),
        )
        .await
        {
            Ok(_) => {
                tracing::info!("antares: mount_job_at Dicfuse ready");
            }
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "Dicfuse initialization timed out after {}s for job {}",
                        DICFUSE_INIT_TIMEOUT_SECS, job_id
                    ),
                ));
            }
        }

        // Create AntaresFuse and mount the union filesystem
        let mut antares_fuse = fuse::AntaresFuse::new(
            mountpoint.clone(),
            self.dic.clone(),
            upper_dir.clone(),
            cl_dir.clone(),
        )
        .await?;

        antares_fuse.mount().await?;

        tracing::info!(
            "antares: mount_job_at FUSE mounted job_id={} mountpoint={}",
            job_id,
            mountpoint.display()
        );

        let instance = AntaresConfig {
            job_id: job_id.to_string(),
            mountpoint,
            upper_id,
            upper_dir,
            cl_dir,
            cl_id,
        };

        self.instances
            .lock()
            .await
            .insert(job_id.to_string(), instance.clone());

        // Store the FUSE handle for later unmount
        self.fuse_handles
            .lock()
            .await
            .insert(job_id.to_string(), antares_fuse);

        self.persist_state().await?;

        tracing::info!(
            "antares: mount_job done job_id={} mountpoint={} elapsed={:.2}s",
            job_id,
            instance.mountpoint.display(),
            start.elapsed().as_secs_f64()
        );
        Ok(instance)
    }

    /// Unmount the FUSE filesystem and remove bookkeeping for a job.
    ///
    /// First attempts to unmount using the stored FUSE handle (proper teardown).
    /// Falls back to `fusermount -u` if no handle is available.
    /// Bookkeeping is always removed regardless of unmount outcome.
    pub async fn umount_job(&self, job_id: &str) -> std::io::Result<Option<AntaresConfig>> {
        use tracing::{info, warn};

        // Look up config first so we can quiesce without holding the state lock.
        let config = match self.instances.lock().await.get(job_id) {
            Some(cfg) => cfg.clone(),
            None => return Ok(None),
        };

        let mount_path = &config.mountpoint;
        info!("Attempting to unmount FUSE mount at {:?}", mount_path);
        let grace = unmount_grace_duration();
        if !grace.is_zero() {
            info!("Quiescing {:?} for {:?} before unmount", mount_path, grace);
            tokio::time::sleep(grace).await;
        }

        // Try to unmount via the stored FUSE handle first (proper teardown)
        let mut fuse_handles = self.fuse_handles.lock().await;
        if let Some(mut fuse) = fuse_handles.remove(job_id) {
            match fuse.unmount().await {
                Ok(()) => {
                    info!("Successfully unmounted {:?} via FUSE handle", mount_path);
                }
                Err(e) => {
                    warn!(
                        "FUSE handle unmount failed for {:?}: {}, falling back to fusermount",
                        mount_path, e
                    );
                    // Fallback to fusermount -u
                    let _ = tokio::process::Command::new(crate::antares::fusermount_bin())
                        .arg("-u")
                        .arg(mount_path)
                        .output()
                        .await;
                }
            }
        } else {
            // No FUSE handle available, use fusermount directly
            let output = tokio::process::Command::new(crate::antares::fusermount_bin())
                .arg("-u")
                .arg(mount_path)
                .output()
                .await?;

            if !output.status.success() {
                let error_msg = String::from_utf8_lossy(&output.stderr);
                if error_msg.contains("not mounted") || error_msg.contains("Invalid argument") {
                    warn!(
                        "Filesystem at {:?} is not mounted, removing bookkeeping only: {}",
                        mount_path, error_msg
                    );
                } else {
                    warn!(
                        "fusermount -u failed with status {} for {:?}: {}",
                        output.status, mount_path, error_msg
                    );
                }
            } else {
                info!("Successfully unmounted {:?} via fusermount", mount_path);
            }
        }
        drop(fuse_handles);

        // Remove from bookkeeping and persist (even if unmount failed)
        let mut instances = self.instances.lock().await;
        let removed = instances.remove(job_id);
        drop(instances);
        self.persist_state().await?;

        Ok(removed)
    }

    /// List all tracked instances.
    pub async fn list(&self) -> Vec<AntaresConfig> {
        self.instances.lock().await.values().cloned().collect()
    }

    /// Access the underlying Dicfuse instance (read-only tree layer).
    pub fn dicfuse(&self) -> Arc<Dicfuse> {
        self.dic.clone()
    }

    fn load_state(path: &Path) -> std::io::Result<HashMap<String, AntaresConfig>> {
        if !path.exists() {
            return Ok(HashMap::new());
        }
        let content = fs::read_to_string(path)?;
        let state: AntaresState = toml::from_str(&content).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("parse state: {e}"))
        })?;
        let mut map = HashMap::new();
        for m in state.mounts {
            map.insert(m.job_id.clone(), m);
        }
        Ok(map)
    }

    async fn persist_state(&self) -> std::io::Result<()> {
        let mounts: Vec<AntaresConfig> = self.instances.lock().await.values().cloned().collect();
        let state = AntaresState { mounts };
        let data = toml::to_string_pretty(&state)
            .map_err(|e| std::io::Error::other(format!("encode state: {e}")))?;
        if let Some(parent) = self.paths.state_file.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut f = File::create(&self.paths.state_file)?;
        f.write_all(data.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::unmount_grace_duration;
    use serial_test::serial;

    fn set_unmount_grace_env(value: Option<&str>) {
        // SAFETY: tests mutate process env in a controlled way and do not run in parallel here.
        unsafe {
            match value {
                Some(value) => std::env::set_var("ANTARES_UNMOUNT_GRACE_MS", value),
                None => std::env::remove_var("ANTARES_UNMOUNT_GRACE_MS"),
            }
        }
    }

    #[test]
    #[serial]
    fn test_unmount_grace_duration_defaults_to_150ms() {
        set_unmount_grace_env(None);
        assert_eq!(
            unmount_grace_duration(),
            std::time::Duration::from_millis(150)
        );
    }

    #[test]
    #[serial]
    fn test_unmount_grace_duration_accepts_explicit_value() {
        set_unmount_grace_env(Some("275"));
        assert_eq!(
            unmount_grace_duration(),
            std::time::Duration::from_millis(275)
        );
        set_unmount_grace_env(None);
    }

    #[test]
    #[serial]
    fn test_unmount_grace_duration_clamps_and_falls_back() {
        set_unmount_grace_env(Some("50000"));
        assert_eq!(
            unmount_grace_duration(),
            std::time::Duration::from_millis(3_000)
        );

        set_unmount_grace_env(Some("not-a-number"));
        assert_eq!(
            unmount_grace_duration(),
            std::time::Duration::from_millis(150)
        );

        set_unmount_grace_env(None);
    }
}
