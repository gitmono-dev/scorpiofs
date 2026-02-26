//! # Scorpio Filesystem Library
//!
//! Scorpio is a FUSE-based virtual filesystem that provides overlay capabilities
//! for monorepo builds. The library exposes the Antares subsystem for managing
//! union filesystems with copy-on-write semantics.
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use scorpio::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Initialize configuration
//!     scorpio::util::config::init_config("scorpio.toml")?;
//!     
//!     // Create Antares service for managing mounts
//!     let service = AntaresServiceImpl::new(None).await;
//!     
//!     // Create HTTP daemon
//!     let daemon = AntaresDaemon::new(std::sync::Arc::new(service));
//!     
//!     // Or use AntaresManager for direct mount operations
//!     let paths = AntaresPaths::from_global_config();
//!     let manager = AntaresManager::new(paths).await;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Mounting an Antares Directory
//!
//! There are three ways to mount an Antares overlay filesystem:
//!
//! ### Method 1: Using AntaresManager with Default Paths
//!
//! ```rust,ignore
//! use scorpio::antares::{AntaresManager, AntaresPaths};
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     // Initialize configuration first
//!     scorpio::util::config::init_config("scorpio.toml").unwrap();
//!     
//!     // Configure paths for layers
//!     let paths = AntaresPaths::new(
//!         PathBuf::from("/var/lib/antares/upper"),   // upper layer root
//!         PathBuf::from("/var/lib/antares/cl"),      // CL layer root  
//!         PathBuf::from("/var/lib/antares/mounts"),  // mountpoints root
//!         PathBuf::from("/var/lib/antares/state.toml"), // state file
//!     );
//!     
//!     // Create manager
//!     let manager = AntaresManager::new(paths).await;
//!     
//!     // Mount a job instance (mountpoint auto-generated at {mount_root}/{job_id})
//!     let config = manager.mount_job("build-job-123", Some("cl-456")).await?;
//!     println!("Mounted at: {}", config.mountpoint.display());
//!     
//!     // ... do build work ...
//!     
//!     // Unmount when done
//!     manager.umount_job("build-job-123").await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ### Method 2: Using AntaresManager with Custom Mountpoint
//!
//! Mount to any arbitrary directory using `mount_job_at`:
//!
//! ```rust,ignore
//! use scorpio::antares::{AntaresManager, AntaresPaths};
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     scorpio::util::config::init_config("scorpio.toml").unwrap();
//!     
//!     let paths = AntaresPaths::from_global_config();
//!     let manager = AntaresManager::new(paths).await;
//!     
//!     // Mount to a custom directory (any path you choose)
//!     let config = manager.mount_job_at(
//!         "my-build",
//!         "/home/user/workspace/my-project",  // custom mountpoint
//!         None,                               // no CL layer
//!     ).await?;
//!     
//!     println!("Mounted at: {}", config.mountpoint.display());
//!     // Output: Mounted at: /home/user/workspace/my-project
//!     
//!     // Unmount when done
//!     manager.umount_job("my-build").await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ### Method 3: Using AntaresFuse Directly
//!
//! For lower-level control over the FUSE mount:
//!
//! ```rust,ignore
//! use scorpio::antares::fuse::AntaresFuse;
//! use scorpio::dicfuse::DicfuseManager;
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     // Initialize configuration
//!     scorpio::util::config::init_config("scorpio.toml").unwrap();
//!     
//!     // Get shared Dicfuse instance (read-only base layer)
//!     let dicfuse = DicfuseManager::global().await;
//!     
//!     // Create AntaresFuse with custom paths
//!     let mut fuse = AntaresFuse::new(
//!         PathBuf::from("/mnt/my-build"),      // mountpoint
//!         dicfuse,                              // read-only base layer
//!         PathBuf::from("/tmp/upper"),         // writable upper layer
//!         Some(PathBuf::from("/tmp/cl")),      // optional CL layer
//!     ).await?;
//!     
//!     // Mount the filesystem (spawns background FUSE session)
//!     fuse.mount().await?;
//!     println!("Filesystem mounted at /mnt/my-build");
//!     
//!     // ... use the mounted filesystem ...
//!     
//!     // Unmount when done
//!     fuse.unmount().await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ### Method 4: Using HTTP Daemon
//!
//! For production deployments, use the HTTP daemon for centralized mount management:
//!
//! ```rust,ignore
//! use scorpio::daemon::antares::{AntaresDaemon, AntaresServiceImpl, CreateMountRequest};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     scorpio::util::config::init_config("scorpio.toml")?;
//!     
//!     // Create service with mount recovery
//!     let service = Arc::new(AntaresServiceImpl::new_with_recovery(None).await);
//!     
//!     // Start HTTP daemon
//!     let daemon = AntaresDaemon::new(service);
//!     let addr = "0.0.0.0:2726".parse()?;
//!     
//!     // Serve until shutdown signal
//!     daemon.serve(addr).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! Then use HTTP API to create mounts:
//! ```bash
//! curl -X POST http://localhost:2726/mounts \
//!   -H "Content-Type: application/json" \
//!   -d '{"job_id": "build-123", "path": "/third-party/mega"}'
//! ```
//!
//! ## Core Components
//!
//! - [`antares`]: Union filesystem overlay management
//! - [`daemon::antares`]: HTTP API daemon for mount lifecycle management
//! - [`dicfuse`]: Read-only dictionary-based FUSE layer
//! - [`util::config`]: Configuration management

#[macro_use]
extern crate log;

pub mod antares;
pub mod daemon;
pub mod dicfuse;
pub mod fuse;
pub mod manager;
pub mod server;
pub mod util;

/// Commonly used types and traits for working with Antares.
///
/// This module re-exports the most frequently used types for convenience.
///
/// # Usage
///
/// ```rust,ignore
/// use scorpio::prelude::*;
/// ```
pub mod prelude {
    // Antares core types
    pub use crate::antares::{AntaresConfig, AntaresManager, AntaresPaths};

    // Antares FUSE layer
    pub use crate::antares::fuse::AntaresFuse;

    // Dicfuse (read-only base layer)
    pub use crate::dicfuse::DicfuseManager;

    // Daemon types
    pub use crate::daemon::antares::{
        AntaresDaemon, AntaresService, AntaresServiceImpl, ApiError, BuildClRequest,
        CreateMountRequest, ErrorBody, HealthResponse, MountCollection, MountCreated, MountLayers,
        MountLifecycle, MountReadyResponse, MountStatus, PersistedMountState, PersistedState,
        ServiceError,
    };
}

// Re-export key antares types at crate root for convenience
pub use antares::{AntaresConfig, AntaresManager, AntaresPaths};

//const VFS_MAX_INO: u64 = 0xff_ffff_ffff_ffff;
const READONLY_INODE: u64 = 0xffff_ffff;
