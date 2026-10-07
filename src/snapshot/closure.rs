//! Complete metadata dependency closure derived from a serving descriptor.
//!
//! The constructor verifies bytes and walks from the committed root. Cached
//! directory lists or file manifests are never evidence of completeness.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use mst2_codec::{
    descriptor::{
        ServingDescriptor, ACCESS_PROJECTION_EXACT_FULL, FS_SEMANTICS_LINUX_CODE_V1,
        MATERIALIZATION_POLICY_GIT_RAW_V1, METADATA_CODEC, SCHEMA_VERSION,
    },
    metapage::{page_id, Entry, EntryKind, Page, MAX_DEPTH},
};

use crate::snapshot::{
    frames::parse_digest, Descriptor, SnapshotError, SnapshotErrorCode, SnapshotFile,
};

const MAX_FILE_SIZE: u64 = 8 * 1024 * 1024 * 1024 * 1024;

/// Full-proof work, separate from incremental acquisition traversal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct SnapshotClosureMeters {
    pub collector_route_visits: u64,
    pub collector_page_decodes: u64,
    pub proof_page_hashes: u64,
    pub proof_page_hash_bytes: u64,
    pub proof_page_decodes: u64,
    pub proof_radix_rebuilds: u64,
    pub proof_logical_directories: u64,
    pub proof_logical_files: u64,
    pub fact_directory_summaries: u64,
    pub fact_radix_visits: u64,
    pub fact_entry_visits: u64,
    pub fact_file_copies: u64,
    pub repaired_records: u64,
}

/// Derived only after the complete descriptor-root graph has been proved.
pub(crate) struct VerifiedSubtreeFacts {
    pub root_page_id: String,
    pub page_ids: Vec<String>,
    pub files: Vec<SnapshotFile>,
    pub total_entries: u64,
}

fn take_subtree_fact(
    fact: Arc<VerifiedSubtreeFacts>,
    meters: &mut SnapshotClosureMeters,
) -> VerifiedSubtreeFacts {
    // Collection normally consumes the memo's last owner. Preserve a shared
    // caller's facts while avoiding another copy of every subtree manifest.
    match Arc::try_unwrap(fact) {
        Ok(fact) => fact,
        Err(fact) => {
            meters.fact_file_copies += fact.files.len() as u64;
            VerifiedSubtreeFacts {
                root_page_id: fact.root_page_id.clone(),
                page_ids: fact.page_ids.clone(),
                files: fact.files.clone(),
                total_entries: fact.total_entries,
            }
        }
    }
}

/// A logical directory, including the scope root and empty directories.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotDirectory {
    /// Scope-relative path without a leading slash; the root is `""`.
    pub rel_path: String,
    pub directory_root: String,
}

/// A verified complete metadata closure. Only validated constructors can
/// create this value; its dependency lists cannot be changed by a caller.
#[derive(Debug, Clone)]
pub struct ValidatedSnapshotClosure {
    descriptor: Descriptor,
    descriptor_bytes: Vec<u8>,
    pages: BTreeMap<String, Vec<u8>>,
    directories: Vec<SnapshotDirectory>,
    files: Vec<SnapshotFile>,
}

impl ValidatedSnapshotClosure {
    pub fn from_pages(
        descriptor: &Descriptor,
        pages: BTreeMap<String, Vec<u8>>,
    ) -> Result<Self, SnapshotError> {
        Ok(Self::validate_pages(descriptor, pages, false)?.0)
    }

    pub(crate) fn with_subtree_facts(
        descriptor: &Descriptor,
        pages: BTreeMap<String, Vec<u8>>,
    ) -> Result<(Self, Vec<VerifiedSubtreeFacts>, SnapshotClosureMeters), SnapshotError> {
        Self::validate_pages(descriptor, pages, true)
    }

    fn validate_pages(
        descriptor: &Descriptor,
        pages: BTreeMap<String, Vec<u8>>,
        include_facts: bool,
    ) -> Result<(Self, Vec<VerifiedSubtreeFacts>, SnapshotClosureMeters), SnapshotError> {
        let descriptor_bytes = canonical_descriptor(descriptor)?;
        let mut validator = ClosureValidator::new(&pages, usize::MAX)?;
        validator.walk_directory(
            &descriptor.scope,
            "",
            &descriptor.metadata_root,
            &mut HashSet::new(),
        )?;
        if validator.reached.len() != pages.len() {
            return Err(integrity("metadata closure contains unreachable pages"));
        }
        validator
            .directories
            .sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        validator.files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        let mut meters = validator.meters;
        let facts = if include_facts {
            let roots: BTreeSet<_> = validator
                .directories
                .iter()
                .map(|d| d.directory_root.clone())
                .collect();
            let mut memo = HashMap::new();
            for root in roots {
                validator.subtree_facts(&root, &mut memo, &mut meters)?;
            }
            let mut facts: Vec<_> = memo
                .into_values()
                .map(|fact| take_subtree_fact(fact, &mut meters))
                .collect();
            facts.sort_by(|a, b| a.root_page_id.cmp(&b.root_page_id));
            facts
        } else {
            Vec::new()
        };
        let directories = std::mem::take(&mut validator.directories);
        let files = std::mem::take(&mut validator.files);
        drop(validator);
        Ok((
            Self {
                descriptor: descriptor.clone(),
                descriptor_bytes,
                pages,
                directories,
                files,
            },
            facts,
            meters,
        ))
    }

    /// Reopen from canonical MSD2 bytes and independently verify every page.
    pub fn from_canonical_pages(
        descriptor_bytes: &[u8],
        pages: BTreeMap<String, Vec<u8>>,
    ) -> Result<Self, SnapshotError> {
        let serving = ServingDescriptor::decode(descriptor_bytes)
            .map_err(|e| integrity(format!("invalid serving descriptor: {e}")))?;
        let descriptor = Descriptor {
            schema_version: SCHEMA_VERSION,
            metadata_codec: METADATA_CODEC,
            instance_id: uuid::Uuid::from_bytes(serving.instance_uuid).to_string(),
            namespace_view_id: digest(&serving.namespace_view_id),
            scope: serving.scope.clone(),
            materialization_policy: MATERIALIZATION_POLICY_GIT_RAW_V1,
            fs_semantics: FS_SEMANTICS_LINUX_CODE_V1,
            access_projection: ACCESS_PROJECTION_EXACT_FULL,
            metadata_root: digest(&serving.metadata_root),
            snapshot_id: digest(
                &serving
                    .snapshot_id()
                    .map_err(|e| integrity(e.to_string()))?,
            ),
        };
        let closure = Self::from_pages(&descriptor, pages)?;
        if closure.descriptor_bytes != descriptor_bytes {
            return Err(integrity("serving descriptor bytes are not canonical"));
        }
        Ok(closure)
    }

    pub fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }

    pub fn snapshot_id(&self) -> &str {
        &self.descriptor.snapshot_id
    }

    pub fn descriptor_bytes(&self) -> &[u8] {
        &self.descriptor_bytes
    }

    pub fn pages(&self) -> &BTreeMap<String, Vec<u8>> {
        &self.pages
    }

    pub fn directories(&self) -> &[SnapshotDirectory] {
        &self.directories
    }

    pub fn files(&self) -> &[SnapshotFile] {
        &self.files
    }

    pub(crate) fn matches_descriptor(&self, descriptor: &Descriptor) -> Result<(), SnapshotError> {
        if canonical_descriptor(descriptor)? != self.descriptor_bytes {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeForbidden,
                "closure differs from the fixed authorized serving descriptor",
            ));
        }
        Ok(())
    }
}

fn canonical_descriptor(descriptor: &Descriptor) -> Result<Vec<u8>, SnapshotError> {
    if descriptor.schema_version != SCHEMA_VERSION
        || descriptor.metadata_codec != METADATA_CODEC
        || descriptor.materialization_policy != MATERIALIZATION_POLICY_GIT_RAW_V1
        || descriptor.fs_semantics != FS_SEMANTICS_LINUX_CODE_V1
        || descriptor.access_projection != ACCESS_PROJECTION_EXACT_FULL
    {
        return Err(integrity("unsupported serving descriptor profile"));
    }
    validate_absolute_path(&descriptor.scope)?;
    let instance = uuid::Uuid::parse_str(&descriptor.instance_id)
        .map_err(|_| integrity("invalid descriptor instance UUID"))?;
    if instance.is_nil() || instance.to_string() != descriptor.instance_id {
        return Err(integrity("instance UUID must be non-nil and canonical"));
    }
    let serving = ServingDescriptor {
        instance_uuid: *instance.as_bytes(),
        namespace_view_id: parse_digest(&descriptor.namespace_view_id)?,
        scope: descriptor.scope.clone(),
        metadata_root: parse_digest(&descriptor.metadata_root)?,
    };
    if descriptor.snapshot_id
        != digest(
            &serving
                .snapshot_id()
                .map_err(|e| integrity(e.to_string()))?,
        )
    {
        return Err(integrity("snapshot_id does not match canonical descriptor"));
    }
    serving.encode().map_err(|e| integrity(e.to_string()))
}

/// Prove one directory's complete radix partition without visiting any
/// logical child directory. This uses the full-closure verifier's exact
/// hash, count, cycle and canonical Build(S) checks.
pub(crate) fn verify_directory_pages(
    root: &str,
    pages: &BTreeMap<String, Vec<u8>>,
    max_entries: usize,
) -> Result<Arc<Vec<Entry>>, SnapshotError> {
    let mut validator = ClosureValidator::new(pages, max_entries)?;
    let entries = validator.radix_entries(root, 0, &mut HashSet::new())?;
    if validator.reached.len() != pages.len() {
        return Err(integrity(
            "directory proof contains unreachable radix pages",
        ));
    }
    Ok(entries)
}

struct ClosureValidator<'a> {
    bytes: &'a BTreeMap<String, Vec<u8>>,
    decoded: HashMap<String, Page>,
    entries: HashMap<String, Arc<Vec<Entry>>>,
    reached: HashSet<String>,
    paths: BTreeSet<String>,
    content_sizes: HashMap<String, u64>,
    directories: Vec<SnapshotDirectory>,
    files: Vec<SnapshotFile>,
    meters: SnapshotClosureMeters,
    max_radix_entries: usize,
}

impl ClosureValidator<'_> {
    fn new(
        bytes: &BTreeMap<String, Vec<u8>>,
        max_radix_entries: usize,
    ) -> Result<ClosureValidator<'_>, SnapshotError> {
        let mut validator = ClosureValidator {
            bytes,
            decoded: HashMap::new(),
            entries: HashMap::new(),
            reached: HashSet::new(),
            paths: BTreeSet::new(),
            content_sizes: HashMap::new(),
            directories: Vec::new(),
            files: Vec::new(),
            meters: SnapshotClosureMeters::default(),
            max_radix_entries,
        };
        for (id, bytes) in bytes {
            validator.meters.proof_page_hashes += 1;
            validator.meters.proof_page_hash_bytes += bytes.len() as u64;
            if page_id(bytes) != parse_digest(id)? {
                return Err(integrity(format!("metadata page bytes do not match {id}")));
            }
            validator.decoded.insert(id.clone(), decode_page(bytes)?);
            validator.meters.proof_page_decodes += 1;
        }
        Ok(validator)
    }

    fn radix_page_ids(
        &self,
        id: &str,
        ids: &mut BTreeSet<String>,
        meters: &mut SnapshotClosureMeters,
    ) {
        if !ids.insert(id.to_owned()) {
            return;
        }
        meters.fact_radix_visits += 1;
        if let Page::Branch { children, .. } = &self.decoded[id] {
            for child in children {
                self.radix_page_ids(&digest(&child.child_page_id), ids, meters);
            }
        }
    }

    fn subtree_facts(
        &self,
        root: &str,
        memo: &mut HashMap<String, Arc<VerifiedSubtreeFacts>>,
        meters: &mut SnapshotClosureMeters,
    ) -> Result<Arc<VerifiedSubtreeFacts>, SnapshotError> {
        if let Some(facts) = memo.get(root) {
            return Ok(facts.clone());
        }
        meters.fact_directory_summaries += 1;
        let mut page_ids = BTreeSet::new();
        self.radix_page_ids(root, &mut page_ids, meters);
        let mut files = Vec::new();
        let mut total_entries = 0u64;
        for entry in self.entries[root].iter() {
            meters.fact_entry_visits += 1;
            total_entries = total_entries
                .checked_add(1)
                .ok_or_else(|| limit("subtree entry count overflow"))?;
            let name =
                std::str::from_utf8(&entry.name).map_err(|_| integrity("non-UTF-8 entry"))?;
            if entry.kind == EntryKind::Directory {
                let child = self.subtree_facts(&digest(&entry.child_root), memo, meters)?;
                page_ids.extend(child.page_ids.iter().cloned());
                total_entries = total_entries
                    .checked_add(child.total_entries)
                    .ok_or_else(|| limit("subtree entry count overflow"))?;
                files.extend(child.files.iter().map(|file| SnapshotFile {
                    rel_path: format!("{name}/{}", file.rel_path),
                    ..file.clone()
                }));
                meters.fact_file_copies += child.files.len() as u64;
            } else {
                files.push(SnapshotFile {
                    rel_path: name.to_owned(),
                    size: entry.size,
                    content_digest: digest(&entry.content_id),
                    fs_kind: match entry.kind {
                        EntryKind::Regular => "regular",
                        EntryKind::Executable => "executable",
                        EntryKind::Symlink => "symlink",
                        EntryKind::Directory => unreachable!(),
                    }
                    .to_owned(),
                });
            }
        }
        files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        let facts = Arc::new(VerifiedSubtreeFacts {
            root_page_id: root.to_owned(),
            page_ids: page_ids.into_iter().collect(),
            files,
            total_entries,
        });
        memo.insert(root.to_owned(), facts.clone());
        Ok(facts)
    }

    fn radix_entries(
        &mut self,
        id: &str,
        depth: usize,
        active: &mut HashSet<String>,
    ) -> Result<Arc<Vec<Entry>>, SnapshotError> {
        if depth > MAX_DEPTH {
            return Err(limit("metadata radix depth exceeds 255"));
        }
        if let Some(entries) = self.entries.get(id) {
            return Ok(entries.clone());
        }
        if !active.insert(id.to_string()) {
            return Err(integrity("cycle in metadata radix pages"));
        }
        let page = self
            .decoded
            .get(id)
            .cloned()
            .ok_or_else(|| integrity(format!("missing committed metadata page {id}")))?;
        self.reached.insert(id.to_string());
        let mut entries = match page {
            Page::Leaf { entries } => entries,
            Page::Branch {
                terminal, children, ..
            } => {
                let mut entries: Vec<_> = terminal.into_iter().collect();
                for child in children {
                    let child_entries =
                        self.radix_entries(&digest(&child.child_page_id), depth + 1, active)?;
                    if child_entries.len() as u64 != child.subtree_entries {
                        return Err(integrity(
                            "branch child subtree_entries does not match its entries",
                        ));
                    }
                    if entries
                        .len()
                        .checked_add(child_entries.len())
                        .is_none_or(|len| len > self.max_radix_entries)
                    {
                        return Err(limit("directory entry proof budget exceeded"));
                    }
                    entries.extend(child_entries.iter().cloned());
                }
                entries
            }
        };
        if entries.len() > self.max_radix_entries {
            return Err(limit("directory entry proof budget exceeded"));
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let rebuilt = Page::build(&entries)
            .map_err(|e| integrity(format!("invalid metadata entry partition: {e}")))?;
        if rebuilt != self.bytes[id] {
            return Err(integrity(
                "metadata page is not the canonical partition of its entries",
            ));
        }
        self.meters.proof_radix_rebuilds += 1;
        active.remove(id);
        let entries = Arc::new(entries);
        self.entries.insert(id.to_string(), entries.clone());
        Ok(entries)
    }

    fn walk_directory(
        &mut self,
        scope: &str,
        rel_path: &str,
        root: &str,
        active: &mut HashSet<String>,
    ) -> Result<(), SnapshotError> {
        validate_composed_path(scope, rel_path)?;
        if !active.insert(root.to_string()) {
            return Err(integrity("cycle in logical directory graph"));
        }
        if !self.paths.insert(rel_path.to_string()) {
            return Err(integrity("duplicate logical path in metadata closure"));
        }
        self.directories.push(SnapshotDirectory {
            rel_path: rel_path.to_string(),
            directory_root: root.to_string(),
        });
        self.meters.proof_logical_directories += 1;
        let entries = self.radix_entries(root, 0, &mut HashSet::new())?;
        for entry in entries.iter() {
            let name =
                std::str::from_utf8(&entry.name).map_err(|_| integrity("non-UTF-8 entry name"))?;
            let child_path = if rel_path.is_empty() {
                name.to_string()
            } else {
                format!("{rel_path}/{name}")
            };
            validate_composed_path(scope, &child_path)?;
            if entry.kind == EntryKind::Directory {
                self.walk_directory(scope, &child_path, &digest(&entry.child_root), active)?;
            } else {
                if entry.size > MAX_FILE_SIZE
                    || (entry.kind == EntryKind::Symlink && !(1..=4095).contains(&entry.size))
                {
                    return Err(limit("file or symlink size exceeds serving profile"));
                }
                if !self.paths.insert(child_path.clone()) {
                    return Err(integrity("duplicate logical path in metadata closure"));
                }
                let content_digest = digest(&entry.content_id);
                if let Some(size) = self
                    .content_sizes
                    .insert(content_digest.clone(), entry.size)
                {
                    if size != entry.size {
                        return Err(integrity("one content digest has conflicting sizes"));
                    }
                }
                self.files.push(SnapshotFile {
                    rel_path: child_path,
                    fs_kind: match entry.kind {
                        EntryKind::Regular => "regular",
                        EntryKind::Executable => "executable",
                        EntryKind::Symlink => "symlink",
                        EntryKind::Directory => unreachable!(),
                    }
                    .to_string(),
                    size: entry.size,
                    content_digest,
                });
                self.meters.proof_logical_files += 1;
            }
        }
        active.remove(root);
        Ok(())
    }
}

fn validate_composed_path(scope: &str, relative: &str) -> Result<(), SnapshotError> {
    let full = if relative.is_empty() {
        scope.to_string()
    } else if scope == "/" {
        format!("/{relative}")
    } else {
        format!("{scope}/{relative}")
    };
    validate_absolute_path(&full)
}

fn validate_absolute_path(path: &str) -> Result<(), SnapshotError> {
    if path.len() > 4096
        || (path != "/" && path.trim_start_matches('/').split('/').count() > 256)
        || path.split('/').any(|component| component.len() > 255)
    {
        return Err(limit("full path exceeds serving profile limits"));
    }
    mst2_codec::descriptor::validate_scope(path)
        .map_err(|e| integrity(format!("invalid closure path: {e}")))
}

/// Codec 0.3 decodes a branch's declared counts using an unchecked sum.
/// Preflight only that arithmetic before handing untrusted bytes to it.
pub(crate) fn decode_page(bytes: &[u8]) -> Result<Page, SnapshotError> {
    if bytes.len() >= 20 && bytes[4] == 1 {
        let payload = &bytes[20..];
        let prefix_len = read_u16(payload, 0)? as usize;
        let mut offset = 2 + prefix_len;
        let terminal = *payload
            .get(offset)
            .ok_or_else(|| integrity("truncated branch terminal flag"))?;
        offset += 1;
        if terminal == 1 {
            Entry::decode(payload, &mut offset).map_err(|e| integrity(e.to_string()))?;
        }
        let count = read_u16(bytes, 6)? as usize;
        if count > 256 {
            return Err(integrity("branch has more than 256 children"));
        }
        let mut sum = u64::from(terminal);
        for _ in 0..count {
            let count_bytes: [u8; 8] = payload
                .get(offset + 1..offset + 9)
                .ok_or_else(|| integrity("truncated branch child count"))?
                .try_into()
                .map_err(|_| integrity("invalid branch child count"))?;
            sum = sum
                .checked_add(u64::from_le_bytes(count_bytes))
                .filter(|total| *total <= i64::MAX as u64)
                .ok_or_else(|| integrity("branch entry count overflow"))?;
            offset += 41;
        }
    }
    Page::decode(bytes)
        .map(|(page, _)| page)
        .map_err(|e| integrity(format!("invalid metadata page: {e}")))
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, SnapshotError> {
    let value: [u8; 2] = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| integrity("truncated metadata page"))?
        .try_into()
        .map_err(|_| integrity("invalid metadata integer"))?;
    Ok(u16::from_le_bytes(value))
}

fn digest(id: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(id))
}

fn integrity(message: impl Into<String>) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::IntegrityError, message)
}

fn limit(message: impl Into<String>) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::LimitExceeded, message)
}

#[cfg(test)]
mod tests {
    use mst2_codec::metapage::BranchChild;

    use super::*;

    fn serving(scope: &str, root: [u8; 32]) -> ServingDescriptor {
        ServingDescriptor {
            instance_uuid: *uuid::Uuid::parse_str("11111111-2222-4333-8444-555555555555")
                .unwrap()
                .as_bytes(),
            namespace_view_id: [0x22; 32],
            scope: scope.to_string(),
            metadata_root: root,
        }
    }

    fn insert_page(pages: &mut BTreeMap<String, Vec<u8>>, bytes: Vec<u8>) -> [u8; 32] {
        let id = page_id(&bytes);
        pages.insert(digest(&id), bytes);
        id
    }

    // Build actual canonical pages via the public codec, including every
    // radix child. The expected logical paths below are hand-written.
    fn directory(pages: &mut BTreeMap<String, Vec<u8>>, mut entries: Vec<Entry>) -> [u8; 32] {
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let root = page_id(&Page::build(&entries).unwrap());
        let mut routes = vec![Vec::new()];
        while let Some(route) = routes.pop() {
            let chain = Page::pages_along_route(&entries, &route).unwrap();
            let current = chain.last().unwrap();
            if let Page::Branch { children, .. } = decode_page(current).unwrap() {
                for child in children {
                    let mut next = route.clone();
                    next.push(child.label);
                    routes.push(next);
                }
            }
            for bytes in chain {
                insert_page(pages, bytes);
            }
        }
        root
    }

    fn file(name: &str, size: u64, content: u8) -> Entry {
        Entry::file(EntryKind::Regular, name.as_bytes(), size, [content; 32])
    }

    fn aliases() -> (ServingDescriptor, BTreeMap<String, Vec<u8>>) {
        let mut pages = BTreeMap::new();
        let empty = directory(&mut pages, Vec::new());
        let shared = directory(
            &mut pages,
            vec![Entry::dir(b"nested", empty), file("x", 4, 0x44)],
        );
        let root = directory(
            &mut pages,
            vec![
                Entry::dir(b"empty", empty),
                Entry::dir(b"left", shared),
                Entry::dir(b"right", shared),
            ],
        );
        (serving("/project", root), pages)
    }

    #[test]
    fn subtree_fact_moves_unique_allocations_and_clones_shared_owners() {
        let make_fact = || {
            Arc::new(VerifiedSubtreeFacts {
                root_page_id: "root".into(),
                page_ids: vec!["root".into(), "child".into()],
                files: vec![SnapshotFile {
                    rel_path: "nested/file".into(),
                    fs_kind: "regular".into(),
                    size: 4,
                    content_digest: "sha256:content".into(),
                }],
                total_entries: 2,
            })
        };
        let mut meters = SnapshotClosureMeters::default();
        let unique = make_fact();
        let pages = unique.page_ids.as_ptr();
        let files = unique.files.as_ptr();
        let root = unique.root_page_id.as_ptr();
        let moved = take_subtree_fact(unique, &mut meters);
        assert_eq!(moved.page_ids.as_ptr(), pages);
        assert_eq!(moved.files.as_ptr(), files);
        assert_eq!(moved.root_page_id.as_ptr(), root);
        assert_eq!(meters.fact_file_copies, 0);

        let shared = make_fact();
        let retained = shared.clone();
        let copied = take_subtree_fact(shared, &mut meters);
        assert_eq!(copied.root_page_id, retained.root_page_id);
        assert_eq!(copied.page_ids, retained.page_ids);
        assert_eq!(copied.files, retained.files);
        assert_eq!(copied.total_entries, retained.total_entries);
        assert_ne!(copied.page_ids.as_ptr(), retained.page_ids.as_ptr());
        assert_ne!(copied.files.as_ptr(), retained.files.as_ptr());
        assert_eq!(meters.fact_file_copies, 1);
        assert_eq!(Arc::strong_count(&retained), 1);
    }

    #[test]
    fn complete_alias_proof_counts_only_required_parent_file_copies() {
        let (serving, pages) = aliases();
        let expected = ValidatedSnapshotClosure::from_canonical_pages(
            &serving.encode().unwrap(),
            pages.clone(),
        )
        .unwrap();
        let (closure, facts, meters) =
            ValidatedSnapshotClosure::with_subtree_facts(expected.descriptor(), pages).unwrap();
        assert_eq!(closure.files(), expected.files());
        assert_eq!(closure.directories(), expected.directories());
        assert_eq!(closure.pages(), expected.pages());
        assert_eq!(facts.len(), 3, "root, shared child and empty directory");
        let root = facts
            .iter()
            .find(|fact| fact.root_page_id == expected.descriptor().metadata_root)
            .unwrap();
        assert_eq!(root.files, expected.files());
        assert_eq!(root.page_ids.len(), 3);
        assert_eq!(meters.proof_page_hashes, 3);
        assert_eq!(meters.proof_logical_files, 2);
        assert_eq!(
            meters.fact_file_copies, 2,
            "one file copied into each root alias"
        );
    }

    #[test]
    fn closure_preserves_root_empty_directories_and_logical_aliases() {
        let (descriptor, pages) = aliases();
        let closure =
            ValidatedSnapshotClosure::from_canonical_pages(&descriptor.encode().unwrap(), pages)
                .unwrap();
        assert_eq!(closure.pages().len(), 3, "physical pages are unique");
        assert_eq!(
            closure
                .directories()
                .iter()
                .map(|d| d.rel_path.as_str())
                .collect::<Vec<_>>(),
            ["", "empty", "left", "left/nested", "right", "right/nested"]
        );
        assert_eq!(
            closure
                .files()
                .iter()
                .map(|f| f.rel_path.as_str())
                .collect::<Vec<_>>(),
            ["left/x", "right/x"]
        );
        assert_eq!(
            closure.directories()[2].directory_root,
            closure.directories()[4].directory_root
        );
        assert_eq!(
            closure.snapshot_id(),
            digest(&descriptor.snapshot_id().unwrap())
        );
        assert_eq!(closure.descriptor_bytes(), descriptor.encode().unwrap());
    }

    #[test]
    fn closure_requires_exact_reachable_dependencies_and_hashes_every_page() {
        let (descriptor, pages) = aliases();
        let bytes = descriptor.encode().unwrap();
        let mut missing = pages.clone();
        let empty = digest(&page_id(&Page::build(&[]).unwrap()));
        missing.remove(&empty);
        assert!(ValidatedSnapshotClosure::from_canonical_pages(&bytes, missing).is_err());
        let mut extra = pages.clone();
        directory(&mut extra, vec![file("unrelated", 1, 9)]);
        assert!(ValidatedSnapshotClosure::from_canonical_pages(&bytes, extra).is_err());
        let mut corrupt = pages;
        corrupt.get_mut(&empty).unwrap()[5] = 1;
        assert!(ValidatedSnapshotClosure::from_canonical_pages(&bytes, corrupt).is_err());
    }

    #[test]
    fn canonical_radix_children_are_all_required() {
        let mut pages = BTreeMap::new();
        let entries = (0..192u16)
            .map(|i| {
                file(
                    &format!("{}{:03}", (b'a' + (i / 64) as u8) as char, i),
                    1,
                    8,
                )
            })
            .collect();
        let root = directory(&mut pages, entries);
        let descriptor = serving("/", root).encode().unwrap();
        let closure =
            ValidatedSnapshotClosure::from_canonical_pages(&descriptor, pages.clone()).unwrap();
        assert_eq!(closure.pages().len(), 4);
        assert_eq!(closure.files().len(), 192);
        let child = pages
            .keys()
            .find(|id| **id != digest(&root))
            .unwrap()
            .clone();
        pages.remove(&child);
        assert!(ValidatedSnapshotClosure::from_canonical_pages(&descriptor, pages).is_err());
    }

    #[test]
    fn structurally_decodable_noncanonical_partitions_and_false_counts_are_rejected() {
        for wrong_count in [false, true] {
            let mut pages = BTreeMap::new();
            let left = directory(&mut pages, vec![file("a", 1, 8)]);
            let right = directory(&mut pages, vec![file("b", 1, 8)]);
            // A branch for two entries is decodable but not Build(S). The
            // second variant also lies about a child's actual entry count.
            let root = insert_page(
                &mut pages,
                Page::Branch {
                    prefix: Vec::new(),
                    terminal: None,
                    children: vec![
                        BranchChild {
                            label: b'a',
                            subtree_entries: if wrong_count { 2 } else { 1 },
                            child_page_id: left,
                        },
                        BranchChild {
                            label: b'b',
                            subtree_entries: 1,
                            child_page_id: right,
                        },
                    ],
                }
                .encode()
                .unwrap(),
            );
            let bytes = serving("/", root).encode().unwrap();
            let error = ValidatedSnapshotClosure::from_canonical_pages(&bytes, pages).unwrap_err();
            assert_eq!(error.code, SnapshotErrorCode::IntegrityError);
        }
    }

    #[test]
    fn wrong_radix_prefix_is_rejected_even_with_valid_hashes_and_counts() {
        let mut pages = BTreeMap::new();
        let root = directory(
            &mut pages,
            (0..192).map(|i| file(&format!("n{i:03}"), 1, 8)).collect(),
        );
        let mut root_page = decode_page(&pages.remove(&digest(&root)).unwrap()).unwrap();
        if let Page::Branch { prefix, .. } = &mut root_page {
            *prefix = b"wrong".to_vec();
        } else {
            panic!("wide fixture must be a branch");
        }
        let wrong = insert_page(&mut pages, root_page.encode().unwrap());
        assert!(ValidatedSnapshotClosure::from_canonical_pages(
            &serving("/", wrong).encode().unwrap(),
            pages
        )
        .is_err());
    }

    #[test]
    fn full_scope_path_limits_include_empty_directories_and_utf8_bytes() {
        let mut pages = BTreeMap::new();
        let empty = directory(&mut pages, Vec::new());
        let root = directory(&mut pages, vec![Entry::dir("é".as_bytes(), empty)]);
        // 15 components of 255 bytes, then 252 bytes: prefix is 4093
        // bytes and the empty child's slash + two-byte name reaches 4096.
        let scope = format!(
            "/{}/{}",
            vec!["x".repeat(255); 15].join("/"),
            "x".repeat(252)
        );
        assert_eq!(scope.len(), 4093);
        ValidatedSnapshotClosure::from_canonical_pages(
            &serving(&scope, root).encode().unwrap(),
            pages.clone(),
        )
        .unwrap();
        let oversized = format!("{scope}x");
        assert_eq!(
            ValidatedSnapshotClosure::from_canonical_pages(
                &serving(&oversized, root).encode().unwrap(),
                pages.clone()
            )
            .unwrap_err()
            .code,
            SnapshotErrorCode::LimitExceeded
        );
        let scope = format!("/{}", vec!["x"; 255].join("/"));
        ValidatedSnapshotClosure::from_canonical_pages(
            &serving(&scope, root).encode().unwrap(),
            pages.clone(),
        )
        .unwrap();
        let scope = format!("{scope}/x");
        assert_eq!(
            ValidatedSnapshotClosure::from_canonical_pages(
                &serving(&scope, root).encode().unwrap(),
                pages
            )
            .unwrap_err()
            .code,
            SnapshotErrorCode::LimitExceeded
        );
    }

    #[test]
    fn descriptor_profile_uuid_and_snapshot_identity_are_checked() {
        let (serving, pages) = aliases();
        let closure = ValidatedSnapshotClosure::from_canonical_pages(
            &serving.encode().unwrap(),
            pages.clone(),
        )
        .unwrap();
        for case in 0..4 {
            let mut descriptor = closure.descriptor().clone();
            match case {
                0 => descriptor.schema_version = 3,
                1 => descriptor.instance_id = uuid::Uuid::nil().to_string(),
                2 => descriptor.instance_id = "AAAAAAAA-2222-4333-8444-555555555555".into(),
                _ => descriptor.snapshot_id = digest(&[0; 32]),
            }
            assert!(ValidatedSnapshotClosure::from_pages(&descriptor, pages.clone()).is_err());
        }
        let mut bytes = serving.encode().unwrap();
        bytes.push(0);
        assert!(ValidatedSnapshotClosure::from_canonical_pages(&bytes, pages).is_err());
    }

    #[test]
    fn invalid_file_sizes_and_conflicting_content_sizes_fail_closed() {
        for entries in [
            vec![file("huge", MAX_FILE_SIZE + 1, 8)],
            vec![Entry::file(EntryKind::Symlink, b"empty-link", 0, [8; 32])],
            vec![Entry::file(EntryKind::Symlink, b"long-link", 4096, [8; 32])],
            vec![file("a", 1, 8), file("b", 2, 8)],
        ] {
            let mut pages = BTreeMap::new();
            let root = directory(&mut pages, entries);
            assert!(ValidatedSnapshotClosure::from_canonical_pages(
                &serving("/", root).encode().unwrap(),
                pages
            )
            .is_err());
        }
    }

    #[test]
    fn overflowing_untrusted_branch_counts_return_error_without_codec_panic() {
        let child = page_id(&Page::build(&[file("a", 1, 8)]).unwrap());
        let mut bytes = Page::Branch {
            prefix: Vec::new(),
            terminal: None,
            children: vec![
                BranchChild {
                    label: b'a',
                    subtree_entries: 1,
                    child_page_id: child,
                },
                BranchChild {
                    label: b'b',
                    subtree_entries: 1,
                    child_page_id: child,
                },
            ],
        }
        .encode()
        .unwrap();
        bytes[24..32].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(
            decode_page(&bytes).unwrap_err().code,
            SnapshotErrorCode::IntegrityError
        );
    }
}
