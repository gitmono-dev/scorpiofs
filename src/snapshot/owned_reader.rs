//! Owned direct reads over current lease and fixed-root-proven file facts.

use std::{collections::HashMap, sync::Arc};

use mst2_codec::treeframe::{self, Frame};
use serde::Serialize;

use super::{
    content::{AccountedBuffer, BatchBuilder, BudgetClass},
    frames::parse_digest,
    owned_transport::{consume_frames, request_body, FrameRequest},
    ContentBudgetLimits, ContentBudgetUsage, SnapshotError, SnapshotErrorCode, SnapshotFile,
    SnapshotReader, ValidatedSnapshotClosure, VerifiedContent, VerifiedContentBatch, OBJECT_CAP,
};

fn invalid(message: &str) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::DigestMismatch, message)
}

impl SnapshotReader {
    /// Configure this reader's long-lived local content scope. Subsequent
    /// clones share its policy, permits and fixed-root membership state.
    pub fn with_content_limits(mut self, limits: ContentBudgetLimits) -> Self {
        self.content_scope = super::content::ContentBudget::new(limits);
        self
    }

    pub fn content_usage(&self) -> ContentBudgetUsage {
        self.content_scope.usage()
    }

    /// Seed file facts from a complete proof for this exact fixed descriptor.
    /// Current per-operation lease and path checks still apply independently.
    pub fn seed_content_membership(
        &self,
        closure: &ValidatedSnapshotClosure,
    ) -> Result<(), SnapshotError> {
        closure.matches_descriptor(self.descriptor())?;
        let files: HashMap<_, _> = closure
            .files()
            .iter()
            .map(|file| (file.rel_path.clone(), file.clone()))
            .collect();
        if let Some(current) = self.content_membership.get() {
            if current != &files {
                return Err(invalid("seeded content facts differ from the fixed root"));
            }
            return Ok(());
        }
        // Any concurrent successful initializer derives from the same fixed
        // descriptor/root; failed and cancelled network initialization retries.
        let _ = self.content_membership.set(files);
        Ok(())
    }

    async fn validate_content_member(&self, file: &SnapshotFile) -> Result<(), SnapshotError> {
        self.authorized_context()
            .validate_relative_path(&file.rel_path)?;
        self.ensure_lease().await?;
        let files = self
            .content_membership
            .get_or_try_init(|| async {
                let closure = self.snapshot_closure().await?;
                Ok::<_, SnapshotError>(
                    closure
                        .files()
                        .iter()
                        .map(|file| (file.rel_path.clone(), file.clone()))
                        .collect(),
                )
            })
            .await?;
        let path = file.rel_path.strip_prefix('/').unwrap_or(&file.rel_path);
        let expected = files.get(path).ok_or_else(|| {
            SnapshotError::new(
                SnapshotErrorCode::PathNotFound,
                "requested content is not a file in the fixed root",
            )
        })?;
        if file.content_digest != expected.content_digest
            || file.size != expected.size
            || !(file.fs_kind == expected.fs_kind
                || file.fs_kind == "file" && expected.fs_kind == "regular")
        {
            return Err(invalid(
                "requested content differs from committed path/digest/size/kind",
            ));
        }
        Ok(())
    }

    /// Read a whole file with retained output credits. SnapshotFile is checked
    /// against current lease and fixed-root membership before any body request.
    pub async fn read_content(
        &self,
        file: &SnapshotFile,
        use_frames: bool,
    ) -> Result<Arc<VerifiedContent>, SnapshotError> {
        if file.size > super::client::MAX_BUFFERED_FILE_BYTES {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "whole-file output exceeds 64 MiB",
            ));
        }
        self.validate_content_member(file).await?;
        self.read_owned_file(file, use_frames, &self.content_scope)
            .await
    }

    /// Up to 128 fixed-view paths. Every alias is independently authorized;
    /// unique whole-file outputs and returned table capacity are admitted
    /// before the first body HTTP. No result publishes before every END+EOF.
    pub async fn read_content_batch(
        &self,
        files: &[SnapshotFile],
    ) -> Result<VerifiedContentBatch, SnapshotError> {
        if files.is_empty() || files.len() > 128 {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "owned OBJECT batch must contain 1..128 paths",
            ));
        }
        for file in files {
            self.authorized_context()
                .validate_relative_path(&file.rel_path)?;
            if file.size > OBJECT_CAP {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::LimitExceeded,
                    "owned OBJECT batch contains a large file",
                ));
            }
        }
        for file in files {
            self.validate_content_member(file).await?;
        }
        struct Unit<'a> {
            file: &'a SnapshotFile,
            digest: [u8; 32],
            output: Option<AccountedBuffer>,
        }
        let mut units: [Option<Unit<'_>>; 128] = std::array::from_fn(|_| None);
        let mut count = 0;
        let mut total = 0usize;
        for file in files {
            let digest = parse_digest(&file.content_digest)?;
            if units[..count].iter().any(|unit| {
                unit.as_ref()
                    .is_some_and(|unit| unit.digest == digest && unit.file.size == file.size)
            }) {
                continue;
            }
            total = total
                .checked_add(file.size as usize)
                .ok_or_else(|| invalid("batch total overflow"))?;
            if total > 7 * 1024 * 1024 {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::LimitExceeded,
                    "owned OBJECT batch exceeds 7 MiB unique content",
                ));
            }
            units[count] = Some(Unit {
                file,
                digest,
                output: None,
            });
            count += 1;
        }
        let mut table = BatchBuilder::new(&self.content_scope, count)?;
        for unit in units[..count].iter_mut().flatten() {
            unit.output = Some(AccountedBuffer::new(
                &self.content_scope,
                BudgetClass::Output,
                unit.file.size as usize,
                std::mem::size_of::<VerifiedContent>(),
            )?);
        }
        #[derive(Serialize)]
        struct Item<'a> {
            #[serde(serialize_with = "scope_path")]
            path: &'a str,
            expected_digest: &'a str,
        }
        fn scope_path<S: serde::Serializer>(path: &&str, serializer: S) -> Result<S::Ok, S::Error> {
            struct Path<'a>(&'a str);
            impl std::fmt::Display for Path<'_> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    if !self.0.starts_with('/') {
                        f.write_str("/")?;
                    }
                    f.write_str(self.0)
                }
            }
            serializer.collect_str(&Path(path))
        }
        #[derive(Serialize)]
        struct Request<'a> {
            items: &'a [Item<'a>],
            #[serde(skip_serializing_if = "Option::is_none")]
            encoding: Option<&'static str>,
        }
        // Serialize authorized scope paths with one leading slash directly
        // into the admitted writer; no path/Value/seen heap copies are made.
        let items: [Item<'_>; 128] = std::array::from_fn(|index| {
            let file = units[index.min(count - 1)].as_ref().unwrap().file;
            Item {
                path: &file.rel_path,
                expected_digest: &file.content_digest,
            }
        });
        let mut start = 0;
        let mut coverage = 0u128;
        let mut last_receipt = None;
        while start < count {
            self.ensure_lease().await?;
            let mut end = count;
            let body = loop {
                match request_body(
                    &self.content_scope,
                    &Request {
                        items: &items[start..end],
                        encoding: self.encoding_hint(),
                    },
                ) {
                    Ok(body) => break body,
                    Err(error)
                        if error.code == SnapshotErrorCode::LimitExceeded && end - start > 1 =>
                    {
                        end = start + (end - start) / 2
                    }
                    Err(error) => return Err(error),
                }
            };
            let logical_max = units[start..end]
                .iter()
                .flatten()
                .map(|unit| unit.file.size as usize)
                .sum();
            let receipt = consume_frames(
                self.client(),
                FrameRequest {
                    snapshot: self.snapshot_id(),
                    endpoint: "objects",
                    body,
                    data_kind: treeframe::KIND_OBJECT,
                    item_count: (end - start) as u32,
                    logical_max,
                    allow_zstd: self.encoding_hint() == Some("zstd"),
                },
                &self.content_scope,
                |frame| {
                    let Frame::Object(payload) = frame else {
                        return Err(invalid("non-OBJECT batch data"));
                    };
                    let mut logical = 0u64;
                    for (digest, bytes) in &payload.objects {
                        let index = (start..end)
                            .find(|index| units[*index].as_ref().unwrap().digest == *digest)
                            .ok_or_else(|| invalid("unrequested OBJECT batch unit"))?;
                        if coverage & (1u128 << index) != 0 {
                            return Err(invalid("duplicate OBJECT batch unit"));
                        }
                        let unit = units[index].as_mut().unwrap();
                        unit.output.as_mut().unwrap().append(bytes)?;
                        coverage |= 1u128 << index;
                        logical += bytes.len() as u64;
                    }
                    Ok((payload.objects.len() as u32, logical))
                },
            )
            .await?;
            last_receipt = Some(receipt);
            start = end;
        }
        let expected = if count == 128 {
            u128::MAX
        } else {
            (1u128 << count) - 1
        };
        if coverage != expected {
            return Err(invalid("incomplete OBJECT batch coverage"));
        }
        let receipt = last_receipt.ok_or_else(|| invalid("OBJECT batch has no successful END"))?;
        // All request segments completed before even the first Arc is created.
        for unit in units[..count].iter_mut().flatten() {
            let content = VerifiedContent::publish(
                unit.output.take().unwrap(),
                unit.file.size as usize,
                &unit.digest,
                receipt.content_receipt(),
            )?;
            table.push(&unit.file.content_digest, content)?;
        }
        Ok(table.finish())
    }
}
