//! Downstream compile guard for the dictionary overlay's public Rust API.

use std::{path::PathBuf, sync::Arc};

use libfuse_fs::unionfs::{layer::Layer, OverlayFs};
use scorpiofs::{antares::fuse::AntaresFuse, dicfuse::Dicfuse};

async fn downstream(
    mountpoint: PathBuf,
    upper_dir: PathBuf,
    dictionary: Arc<Dicfuse>,
    replacement: Arc<Dicfuse>,
    lower: Arc<dyn Layer>,
) -> std::io::Result<()> {
    let mut fuse: AntaresFuse =
        AntaresFuse::new(mountpoint.clone(), dictionary, upper_dir.clone(), None).await?;
    let _: Arc<Dicfuse> = fuse.dic.clone();
    let _: Option<Arc<dyn Layer>> = fuse.lower_override.clone();
    // Existing callers can replace these fields directly; overlay construction
    // must continue to consult their current values rather than a saved base.
    fuse.dic = replacement;
    fuse.lower_override = Some(lower.clone());
    fuse.mountpoint = mountpoint;
    fuse.upper_dir = upper_dir;
    fuse.cl_dir = None;
    fuse.frozen_dirs = Vec::new();
    fuse = fuse
        .with_lower_override(lower)
        .with_frozen_layers(Vec::new())?;
    let _: OverlayFs = fuse.build_overlay().await?;
    fuse.mount().await?;
    fuse.unmount().await
}

#[test]
fn original_constructor_fields_and_methods_compile_for_downstream_consumers() {
    // Referencing this function type checks its body without starting Dicfuse
    // imports, network traffic or a real FUSE mount on the CI runner.
    let _consumer = downstream;
}
