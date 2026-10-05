//! Immutable read authority and conservative local cache partitioning.
//!
//! A cache domain deliberately distinguishes credentials. The protocol does
//! not yet expose a stable actor identifier, so two tokens are never assumed
//! to denote the same actor. The fingerprint is local partitioning data; it
//! does not prove authorization or permit an offline read.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};

use crate::snapshot::{types::Descriptor, SnapshotError, SnapshotErrorCode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheDomain(String);

impl CacheDomain {
    pub fn id(&self) -> &str {
        &self.0
    }
}

/// The descriptor, scope and authorization generation captured by resolve.
/// Fields are private so changing client configuration or a later resolve
/// cannot change the authority of an existing reader.
#[derive(Debug, Clone)]
pub struct AuthorizedSnapshotContext {
    descriptor: Descriptor,
    authorization_epoch: u64,
    publication_sequence: u64,
    domain: CacheDomain,
}

impl AuthorizedSnapshotContext {
    pub(crate) fn new(
        deployment: &str,
        credential_partition: &str,
        requested_scope: &str,
        descriptor: Descriptor,
        authorization_epoch: &str,
        publication_sequence: &str,
    ) -> Result<Self, SnapshotError> {
        validate_scope(requested_scope)?;
        validate_scope(&descriptor.scope)?;
        if descriptor.scope != requested_scope {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeForbidden,
                "resolve returned a different scope",
            ));
        }
        validate_descriptor(&descriptor)?;
        let authorization_epoch = decimal(authorization_epoch, "authorization_epoch")?;
        let publication_sequence = decimal(publication_sequence, "publication_sequence")?;
        let mut url = url::Url::parse(deployment).map_err(|_| {
            SnapshotError::new(SnapshotErrorCode::ScopeInvalid, "invalid deployment URL")
        })?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeInvalid,
                "deployment URL must be HTTP(S) without credentials, query or fragment",
            ));
        }
        let base_path = url.path().trim_end_matches('/').to_string();
        url.set_path(&base_path);
        let domain = CacheDomain(hash_fields(
            b"mega.scorpio.cache-domain.v1\0",
            &[
                url.as_str().as_bytes(),
                descriptor.instance_id.as_bytes(),
                credential_partition.as_bytes(),
                &authorization_epoch.to_be_bytes(),
                &descriptor.schema_version.to_be_bytes(),
                &descriptor.metadata_codec.to_be_bytes(),
                &descriptor.materialization_policy.to_be_bytes(),
                &descriptor.fs_semantics.to_be_bytes(),
                &descriptor.access_projection.to_be_bytes(),
            ],
        ));
        Ok(Self {
            descriptor,
            authorization_epoch,
            publication_sequence,
            domain,
        })
    }

    pub fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }

    pub fn cache_domain(&self) -> &CacheDomain {
        &self.domain
    }

    pub fn authorization_epoch(&self) -> u64 {
        self.authorization_epoch
    }

    pub fn publication_sequence(&self) -> u64 {
        self.publication_sequence
    }

    /// Canonical scope identity, without lossy substitutions such as
    /// mapping both `/a-b` and `/a_b` to `a_b`.
    pub fn scope_cache_dir(&self, root: &Path) -> PathBuf {
        let scope_id = hash_fields(
            b"mega.scorpio.scope.v1\0",
            &[self.descriptor.scope.as_bytes()],
        );
        root.join("snapshots").join(self.domain.id()).join(scope_id)
    }

    pub fn view_cache_dir(&self, root: &Path) -> Result<PathBuf, SnapshotError> {
        let digest = crate::snapshot::frames::parse_digest(&self.descriptor.snapshot_id)?;
        Ok(self.scope_cache_dir(root).join(hex::encode(digest)))
    }

    /// Bind before reading any reusable page or CAS object. An unbound,
    /// populated legacy directory has no authority provenance and is refused.
    pub fn bind_scope_cache(&self, dir: &Path) -> Result<(), SnapshotError> {
        self.bind_cache(dir, None)
    }

    pub fn bind_view_cache(&self, dir: &Path) -> Result<(), SnapshotError> {
        self.bind_cache(dir, Some(self.descriptor.snapshot_id.clone()))
    }

    fn bind_cache(&self, dir: &Path, snapshot_id: Option<String>) -> Result<(), SnapshotError> {
        bind_directory(
            dir,
            &CacheBinding {
                revision: 1,
                domain: self.domain.id().to_string(),
                scope: self.descriptor.scope.clone(),
                snapshot_id,
            },
        )
    }

    /// Check scope-relative path syntax and the composed full-path budget.
    /// Membership still has to be proved by this snapshot's verified pages.
    pub fn validate_relative_path(&self, path: &str) -> Result<(), SnapshotError> {
        let path = if path.is_empty() { "/" } else { path };
        let relative = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };
        validate_scope(&relative)?;
        let composed = if self.descriptor.scope == "/" {
            relative
        } else if relative == "/" {
            self.descriptor.scope.clone()
        } else {
            format!("{}{relative}", self.descriptor.scope)
        };
        validate_scope(&composed)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CacheBinding {
    revision: u8,
    domain: String,
    scope: String,
    snapshot_id: Option<String>,
}

const AUTHORITY_FILE: &str = "authority.json";
const AUTHORITY_LOCK: &str = "authority.lock";
const AUTHORITY_TMP: &str = "authority.json.tmp";

struct AuthorityLock(File);

impl Drop for AuthorityLock {
    fn drop(&mut self) {
        if let Err(error) = self.0.unlock() {
            tracing::warn!(%error, "failed to release cache authority lock");
        }
    }
}

fn bind_directory(dir: &Path, binding: &CacheBinding) -> Result<(), SnapshotError> {
    let io_error =
        |error: std::io::Error| SnapshotError::new(SnapshotErrorCode::Internal, error.to_string());
    fs::create_dir_all(dir).map_err(io_error)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join(AUTHORITY_LOCK))
        .map_err(io_error)?;
    lock.lock().map_err(io_error)?;
    let _guard = AuthorityLock(lock);
    let path = dir.join(AUTHORITY_FILE);
    match fs::read(&path) {
        Ok(bytes) => {
            let stored: CacheBinding = serde_json::from_slice(&bytes).map_err(|_| {
                SnapshotError::new(
                    SnapshotErrorCode::ScopeForbidden,
                    "cache authority record is malformed",
                )
            })?;
            if stored != *binding {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::ScopeForbidden,
                    "cache belongs to a different authority, scope or snapshot",
                ));
            }
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(error)),
    }
    for entry in fs::read_dir(dir).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        if entry.file_name() == AUTHORITY_LOCK || entry.file_name() == AUTHORITY_TMP {
            continue;
        }
        if entry.file_type().map_err(io_error)?.is_dir()
            && fs::read_dir(entry.path())
                .map_err(io_error)?
                .next()
                .is_none()
        {
            continue;
        }
        return Err(SnapshotError::new(
            SnapshotErrorCode::ScopeForbidden,
            "populated legacy cache has no authority record",
        ));
    }
    let bytes = serde_json::to_vec(binding)
        .map_err(|error| SnapshotError::new(SnapshotErrorCode::Internal, error.to_string()))?;
    let temp = dir.join(AUTHORITY_TMP);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)
        .map_err(io_error)?;
    file.write_all(&bytes).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    fs::rename(&temp, &path).map_err(io_error)?;
    File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(io_error)?;
    Ok(())
}

fn decimal(value: &str, field: &str) -> Result<u64, SnapshotError> {
    if value.is_empty()
        || !value.bytes().all(|b| b.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return Err(SnapshotError::new(
            SnapshotErrorCode::ScopeForbidden,
            format!("invalid {field} in resolve"),
        ));
    }
    let parsed: u64 = value.parse().map_err(|_| {
        SnapshotError::new(
            SnapshotErrorCode::ScopeForbidden,
            format!("out-of-range {field} in resolve"),
        )
    })?;
    if parsed > i64::MAX as u64 {
        return Err(SnapshotError::new(
            SnapshotErrorCode::LimitExceeded,
            format!("{field} exceeds the protocol counter limit"),
        ));
    }
    Ok(parsed)
}

pub(crate) fn validate_scope(scope: &str) -> Result<(), SnapshotError> {
    if scope.len() > 4096 {
        return Err(SnapshotError::new(
            SnapshotErrorCode::LimitExceeded,
            "path exceeds 4096 bytes",
        ));
    }
    let parts: Vec<_> = scope.trim_start_matches('/').split('/').collect();
    if (scope != "/" && parts.len() > 256) || parts.iter().any(|p| p.len() > 255) {
        return Err(SnapshotError::new(
            SnapshotErrorCode::LimitExceeded,
            "full path exceeds profile limits",
        ));
    }
    mst2_codec::descriptor::validate_scope(scope).map_err(|e| {
        SnapshotError::new(
            SnapshotErrorCode::ScopeInvalid,
            format!("invalid path: {e}"),
        )
    })?;
    Ok(())
}

pub(crate) fn validate_descriptor(descriptor: &Descriptor) -> Result<(), SnapshotError> {
    use mst2_codec::descriptor::{
        ServingDescriptor, ACCESS_PROJECTION_EXACT_FULL, FS_SEMANTICS_LINUX_CODE_V1,
        MATERIALIZATION_POLICY_GIT_RAW_V1, METADATA_CODEC, SCHEMA_VERSION,
    };
    if descriptor.schema_version != SCHEMA_VERSION
        || descriptor.metadata_codec != METADATA_CODEC
        || descriptor.materialization_policy != MATERIALIZATION_POLICY_GIT_RAW_V1
        || descriptor.fs_semantics != FS_SEMANTICS_LINUX_CODE_V1
        || descriptor.access_projection != ACCESS_PROJECTION_EXACT_FULL
    {
        return Err(SnapshotError::new(
            SnapshotErrorCode::IntegrityError,
            "unsupported serving descriptor profile",
        ));
    }
    let instance = uuid::Uuid::parse_str(&descriptor.instance_id).map_err(|_| {
        SnapshotError::new(
            SnapshotErrorCode::IntegrityError,
            "invalid descriptor instance UUID",
        )
    })?;
    if instance.is_nil() || instance.to_string() != descriptor.instance_id {
        return Err(SnapshotError::new(
            SnapshotErrorCode::IntegrityError,
            "instance UUID must be non-nil and canonical",
        ));
    }
    let encoded = ServingDescriptor {
        instance_uuid: *instance.as_bytes(),
        namespace_view_id: crate::snapshot::frames::parse_digest(&descriptor.namespace_view_id)?,
        scope: descriptor.scope.clone(),
        metadata_root: crate::snapshot::frames::parse_digest(&descriptor.metadata_root)?,
    };
    let snapshot = encoded.snapshot_id().map_err(|e| {
        SnapshotError::new(
            SnapshotErrorCode::IntegrityError,
            format!("invalid serving descriptor: {e}"),
        )
    })?;
    if descriptor.snapshot_id != format!("sha256:{}", hex::encode(snapshot)) {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            "snapshot_id does not match canonical descriptor",
        ));
    }
    Ok(())
}

fn hash_fields(domain: &[u8], fields: &[&[u8]]) -> String {
    let mut hash = Context::new(&SHA256);
    hash.update(domain);
    for field in fields {
        hash.update(&(field.len() as u64).to_be_bytes());
        hash.update(field);
    }
    hex::encode(hash.finish().as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(scope: &str) -> Descriptor {
        let mut descriptor = Descriptor {
            schema_version: 2,
            metadata_codec: 1,
            instance_id: "11111111-1111-4111-8111-111111111111".into(),
            namespace_view_id: format!("sha256:{}", "55".repeat(32)),
            scope: scope.into(),
            materialization_policy: 1,
            fs_semantics: 1,
            access_projection: 0,
            metadata_root: format!("sha256:{}", "11".repeat(32)),
            snapshot_id: String::new(),
        };
        identify(&mut descriptor);
        descriptor
    }

    fn identify(descriptor: &mut Descriptor) {
        let canonical = mst2_codec::descriptor::ServingDescriptor {
            instance_uuid: *uuid::Uuid::parse_str(&descriptor.instance_id)
                .unwrap()
                .as_bytes(),
            namespace_view_id: crate::snapshot::frames::parse_digest(&descriptor.namespace_view_id)
                .unwrap(),
            scope: descriptor.scope.clone(),
            metadata_root: crate::snapshot::frames::parse_digest(&descriptor.metadata_root)
                .unwrap(),
        };
        descriptor.snapshot_id =
            format!("sha256:{}", hex::encode(canonical.snapshot_id().unwrap()));
    }

    fn context(
        base: &str,
        actor: &str,
        epoch: &str,
        desc: Descriptor,
    ) -> AuthorizedSnapshotContext {
        let scope = desc.scope.clone();
        AuthorizedSnapshotContext::new(base, actor, &scope, desc, epoch, "1").unwrap()
    }

    #[test]
    fn new_commit_keeps_domain_but_actor_deployment_and_policy_changes_do_not() {
        let first = context("https://mega.example", "actor-a", "1", descriptor("/p"));
        let mut next = descriptor("/p");
        next.namespace_view_id = format!("sha256:{}", "66".repeat(32));
        next.metadata_root = format!("sha256:{}", "44".repeat(32));
        identify(&mut next);
        let updated = context("https://mega.example/", "actor-a", "1", next.clone());
        assert_eq!(first.cache_domain(), updated.cache_domain());
        assert_ne!(
            first.view_cache_dir(Path::new("cache")).unwrap(),
            updated.view_cache_dir(Path::new("cache")).unwrap()
        );
        for changed in [
            context("https://other.example", "actor-a", "1", next.clone()),
            context("https://mega.example", "actor-b", "1", next.clone()),
            context("https://mega.example", "actor-a", "2", next.clone()),
        ] {
            assert_ne!(first.cache_domain(), changed.cache_domain());
        }
        next.materialization_policy = 2;
        assert!(AuthorizedSnapshotContext::new(
            "https://mega.example",
            "actor-a",
            "/p",
            next,
            "1",
            "1"
        )
        .is_err());
    }

    #[test]
    fn scope_paths_do_not_collide_or_escape() {
        let dashed = context("https://mega.example", "actor", "1", descriptor("/a-b"));
        let underscored = context("https://mega.example", "actor", "1", descriptor("/a_b"));
        assert_ne!(
            dashed.scope_cache_dir(Path::new("cache")),
            underscored.scope_cache_dir(Path::new("cache"))
        );
        for path in ["../secret", "/../secret", "a//b", "a/./b", "a/", "x\0y"] {
            assert!(dashed.validate_relative_path(path).is_err(), "{path:?}");
        }
        assert!(dashed.validate_relative_path("file.rs").is_ok());
        let mut invalid = descriptor("/a-b");
        invalid.snapshot_id = "sha256:../../secret".into();
        assert!(AuthorizedSnapshotContext::new(
            "https://mega.example",
            "actor",
            "/a-b",
            invalid,
            "1",
            "1"
        )
        .is_err());
    }

    #[test]
    fn resolve_cannot_widen_scope_or_omit_authorization_generation() {
        let error = AuthorizedSnapshotContext::new(
            "https://mega.example",
            "actor",
            "/p",
            descriptor("/"),
            "1",
            "1",
        )
        .unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::ScopeForbidden);
        for epoch in [
            "",
            "01",
            "-1",
            "9223372036854775808",
            "18446744073709551616",
        ] {
            assert!(AuthorizedSnapshotContext::new(
                "https://mega.example",
                "actor",
                "/p",
                descriptor("/p"),
                epoch,
                "1"
            )
            .is_err());
        }
        for base in [
            "file:///tmp",
            "https://user:secret@mega.example",
            "https://mega.example?token=secret",
            "https://mega.example/#x",
        ] {
            assert!(AuthorizedSnapshotContext::new(
                base,
                "actor",
                "/p",
                descriptor("/p"),
                "1",
                "1"
            )
            .is_err());
        }
    }

    #[test]
    fn canonical_descriptor_identity_and_supported_profile_are_required() {
        let valid = descriptor("/p");
        let mut variants = Vec::new();
        let mut changed = valid.clone();
        changed.instance_id = uuid::Uuid::nil().to_string();
        variants.push(changed);
        let mut changed = valid.clone();
        changed.namespace_view_id = format!("sha256:{}", "AA".repeat(32));
        variants.push(changed);
        let mut changed = valid.clone();
        changed.metadata_root = format!("sha256:{}", "22".repeat(32));
        variants.push(changed);
        let mut changed = valid.clone();
        changed.snapshot_id = format!("sha256:{}", "00".repeat(32));
        variants.push(changed);
        for field in 0..5 {
            let mut changed = valid.clone();
            match field {
                0 => changed.schema_version += 1,
                1 => changed.metadata_codec += 1,
                2 => changed.materialization_policy += 1,
                3 => changed.fs_semantics += 1,
                _ => changed.access_projection += 1,
            }
            variants.push(changed);
        }
        for changed in variants {
            assert!(AuthorizedSnapshotContext::new(
                "https://mega.example",
                "actor",
                "/p",
                changed,
                "1",
                "1"
            )
            .is_err());
        }
    }

    #[test]
    fn full_paths_enforce_exact_byte_component_and_basename_budgets() {
        let root = context("https://mega.example", "actor", "1", descriptor("/"));
        assert!(root.validate_relative_path(&"x".repeat(255)).is_ok());
        assert_eq!(
            root.validate_relative_path(&"x".repeat(256))
                .unwrap_err()
                .code,
            SnapshotErrorCode::LimitExceeded
        );
        let at_limit = vec!["x".repeat(255); 16].join("/");
        assert_eq!(at_limit.len() + 1, 4096);
        assert!(root.validate_relative_path(&at_limit).is_ok());
        assert_eq!(
            root.validate_relative_path(&format!("{at_limit}/x"))
                .unwrap_err()
                .code,
            SnapshotErrorCode::LimitExceeded
        );
        let at_components = vec!["x"; 256].join("/");
        assert!(root.validate_relative_path(&at_components).is_ok());
        let scoped = context("https://mega.example", "actor", "1", descriptor("/p"));
        assert_eq!(
            scoped
                .validate_relative_path(&at_components)
                .unwrap_err()
                .code,
            SnapshotErrorCode::LimitExceeded
        );
        assert!(scoped
            .validate_relative_path(&vec!["x"; 255].join("/"))
            .is_ok());
        assert_eq!(
            scoped.validate_relative_path(&at_limit).unwrap_err().code,
            SnapshotErrorCode::LimitExceeded
        );
    }

    #[test]
    fn persistent_binding_refuses_other_actors_epochs_scopes_and_legacy_data() {
        let temp = tempfile::TempDir::new().unwrap();
        let first = context(
            "https://mega.example",
            "very-secret-token",
            "1",
            descriptor("/p"),
        );
        let dir = temp.path().join("shared");
        first.bind_scope_cache(&dir).unwrap();
        first.bind_scope_cache(&dir).unwrap();
        let identity = fs::read_to_string(dir.join(AUTHORITY_FILE)).unwrap();
        assert!(!identity.contains("very-secret-token"));
        for other in [
            context("https://mega.example", "other", "1", descriptor("/p")),
            context(
                "https://mega.example",
                "very-secret-token",
                "2",
                descriptor("/p"),
            ),
            context(
                "https://mega.example",
                "very-secret-token",
                "1",
                descriptor("/q"),
            ),
        ] {
            assert_eq!(
                other.bind_scope_cache(&dir).unwrap_err().code,
                SnapshotErrorCode::ScopeForbidden
            );
        }
        let legacy = temp.path().join("legacy");
        fs::create_dir_all(&legacy).unwrap();
        fs::write(legacy.join("view.json"), b"{}").unwrap();
        assert_eq!(
            first.bind_view_cache(&legacy).unwrap_err().code,
            SnapshotErrorCode::ScopeForbidden
        );
        fs::write(dir.join(AUTHORITY_FILE), b"not-json").unwrap();
        assert_eq!(
            first.bind_scope_cache(&dir).unwrap_err().code,
            SnapshotErrorCode::ScopeForbidden
        );
    }

    #[test]
    fn new_snapshot_shares_scope_authority_but_cannot_claim_old_view_directory() {
        let temp = tempfile::TempDir::new().unwrap();
        let first = context("https://mega.example", "actor", "1", descriptor("/p"));
        let mut desc = descriptor("/p");
        desc.metadata_root = format!("sha256:{}", "22".repeat(32));
        identify(&mut desc);
        let next = context("https://mega.example", "actor", "1", desc);
        first.bind_scope_cache(temp.path()).unwrap();
        next.bind_scope_cache(temp.path()).unwrap();
        let view = temp.path().join("old-view");
        first.bind_view_cache(&view).unwrap();
        assert_eq!(
            next.bind_view_cache(&view).unwrap_err().code,
            SnapshotErrorCode::ScopeForbidden
        );
    }

    #[test]
    fn simultaneous_different_actors_cannot_both_claim_one_directory() {
        let temp = tempfile::TempDir::new().unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let workers: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|actor| {
                let context = context("https://mega.example", actor, "1", descriptor("/p"));
                let barrier = barrier.clone();
                let path = temp.path().to_path_buf();
                std::thread::spawn(move || {
                    barrier.wait();
                    context.bind_scope_cache(&path)
                })
            })
            .collect();
        let results: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| result
                    .as_ref()
                    .is_err_and(|error| error.code == SnapshotErrorCode::ScopeForbidden))
                .count(),
            1
        );
    }
}
