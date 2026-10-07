//! Owned content and selective membership signatures for external consumers.

use std::sync::Arc;

use scorpiofs::snapshot::{
    FileMembershipError, ProvenSnapshotFile, SnapshotErrorCode, SnapshotReader,
};

async fn selective_membership(reader: &SnapshotReader) -> Result<(), FileMembershipError> {
    let proven: Arc<ProvenSnapshotFile> = reader.prove_file("file").await?;
    let _ = reader.read_proven_content(&proven, true).await?;
    Ok(())
}

fn membership_error(error: &FileMembershipError) -> Option<SnapshotErrorCode> {
    match error {
        FileMembershipError::NotFile { message: _ } => None,
        FileMembershipError::Snapshot(error) => Some(error.code),
        _ => None,
    }
}

#[test]
fn owned_content_and_membership_api_compiles_without_io() {
    let _ = selective_membership;
    let _ = membership_error;
}
