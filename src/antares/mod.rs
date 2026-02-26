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
//! use scorpio::antares::{AntaresManager, AntaresPaths};
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

use crate::{
    dicfuse::{Dicfuse, DicfuseManager},
    util::config,
};

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
/// This scaffold currently wires directory creation and bookkeeping; the unionfs
/// integration will be added once the layer stack is finalized.
pub struct AntaresManager {
    dic: Arc<Dicfuse>,
    paths: AntaresPaths,
    instances: Arc<Mutex<HashMap<String, AntaresConfig>>>,
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
    /// Attempts to unmount the filesystem using `fusermount -u`. If the filesystem
    /// is not mounted (e.g., it was never mounted or already unmounted), the unmount
    /// attempt will fail but the function will still remove the bookkeeping entry.
    pub async fn umount_job(&self, job_id: &str) -> std::io::Result<Option<AntaresConfig>> {
        use tracing::{info, warn};

        // Lock and get the config, but do not remove yet
        let mut instances = self.instances.lock().await;
        let config = match instances.get(job_id) {
            Some(cfg) => cfg.clone(),
            None => return Ok(None),
        };

        // Attempt to unmount the FUSE mount
        let mount_path = &config.mountpoint;
        info!("Attempting to unmount FUSE mount at {:?}", mount_path);

        let output = tokio::process::Command::new("fusermount")
            .arg("-u")
            .arg(mount_path)
            .output()
            .await?;

        if !output.status.success() {
            let error_msg = String::from_utf8_lossy(&output.stderr);
            // Check if the error is because the filesystem is not mounted
            // In this case, we still proceed to remove bookkeeping
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
                // For other errors, we still remove bookkeeping to avoid stale entries
                // but log the warning
            }
        } else {
            info!("Successfully unmounted {:?}", mount_path);
        }

        // Remove from bookkeeping and persist (even if unmount failed)
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
