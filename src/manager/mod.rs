use std::{
    fs,
    io::Write,
    sync::atomic::{AtomicU64, Ordering},
};

use serde::{Deserialize, Serialize};

use crate::util::config;

pub mod fetch;
pub mod store;

/// Atomically persist `content` to `file_path`.
///
/// Writes to a sibling temp file and renames it into place so that a crash or
/// I/O error mid-write cannot leave a half-written (corrupt) state file. The
/// temp file is created with `O_EXCL` (`create_new`) so it can neither follow a
/// pre-planted symlink nor truncate an existing file, and it is removed on
/// failure.
fn write_atomic(file_path: &str, content: &[u8]) -> std::io::Result<()> {
    static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_path = format!("{file_path}.tmp.{}.{n}", std::process::id());

    // Exclusive create: fails if the path already exists and won't traverse a
    // symlink for the final component.
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)?;

    if let Err(e) = file.write_all(content) {
        drop(file);
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }
    if let Err(e) = file.sync_all() {
        drop(file);
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }
    drop(file);

    if let Err(e) = fs::rename(&tmp_path, file_path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct ScorpioManager {
    // pub url:String,
    // pub workspace:String,
    // pub store_path:String,// the path to store init code (or remote code), name is hash value .
    // pub git_author:String,
    // pub git_email:String,
    pub works: Vec<WorkDir>,
}
#[derive(Serialize, Deserialize, Clone)]
pub struct WorkDir {
    pub path: String,
    pub node: u64,
    pub hash: String,
}

#[allow(unused)]
impl ScorpioManager {
    pub fn from_toml(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(file_path)?;
        let manager: ScorpioManager = toml::de::from_str(&content)?;
        Ok(manager)
    }

    pub fn to_toml(&self, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let content = toml::ser::to_string(self)?;
        write_atomic(file_path, content.as_bytes())?;
        Ok(())
    }

    /// Integrate the temporary storage area files, merge
    /// them into a Tree object and output Commit
    /// (function removed)
    /// Extracts and returns the corresponding workspace for the provided `mono_path`.
    ///
    /// This function iterates over the manager's work directories and selects the one whose path
    /// is either exactly equal to `mono_path` or is a prefix of `mono_path`. In other words, it
    /// finds the workspace that best matches the given path.
    ///
    /// # Parameters
    ///
    /// - `mono_path`: A string slice representing the path to match against the work directories.
    ///
    /// # Returns
    ///
    /// - `Ok(&WorkDir)` if a matching workspace is found.
    /// - `Err("WorkDir not found")` otherwise.
    fn select_work(&self, mono_path: &str) -> Result<&WorkDir, Box<dyn std::error::Error>> {
        for works in self.works.iter() {
            if mono_path.starts_with(&works.path) || mono_path.eq(&works.path) {
                return Ok(works);
            }
        }
        Err(Box::from("WorkDir not found"))
    }

    /// Pushes a commit to the remote mono repository.
    /// (function removed)
    pub fn check_before_mount(&self, mono_path: &str) -> Result<(), String> {
        for work in &self.works {
            // check if work.path and mono_path are equal or parent/child
            if work.path == mono_path
                || (work.path.starts_with(mono_path)
                    && work.path.len() > mono_path.len()
                    && work.path.as_bytes()[mono_path.len()] == b'/')
                || (mono_path.starts_with(&work.path)
                    && mono_path.len() > work.path.len()
                    && mono_path.as_bytes()[work.path.len()] == b'/')
            {
                return Err(work.path.clone());
            }
        }
        Ok(())
    }

    /// Iterate through the manager's works to find the specified path's workspace and remove it.
    pub async fn remove_workspace(
        &mut self,
        mono_path: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(pos) = self.works.iter().position(|work| work.path == mono_path) {
            self.works.remove(pos);
            // Persist to the configured state file (not a hardcoded path) so the
            // read path (main.rs uses `config_file()`) and the write path agree.
            let state_file = config::config_file();
            self.to_toml(state_file).map_err(|e| {
                tracing::error!("failed to persist state file '{state_file}': {e}");
                e
            })?;
            Ok(())
        } else {
            Err(Box::from("Workspace not found"))
        }
    }

    // Adds a mono file to the Scorpio manager's workspace. (removed)
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use super::*;

    #[test]
    fn test_from_toml() {
        let tmp_file = format!("{}/test_from_toml_1.toml", env::temp_dir().display(),);
        let toml_content = r#"
            works = [{ path = "/path/to/work1", hash = "hash1", node = 1}]
        "#;

        fs::write(&tmp_file, toml_content).expect("Unable to write test file");

        let manager = ScorpioManager::from_toml(&tmp_file).expect("Failed to parse TOML");
        assert_eq!(manager.works.len(), 1);
        assert_eq!(manager.works[0].path, "/path/to/work1");
        assert_eq!(manager.works[0].hash, "hash1");

        fs::remove_file(&tmp_file).ok();
    }

    #[test]
    fn test_to_toml() {
        let tmp_file = format!("{}/test_to_toml_2.toml", env::temp_dir().display(),);
        let manager = ScorpioManager {
            works: vec![
                WorkDir {
                    path: "/path/to/work1".to_string(),
                    hash: "hash1".to_string(),
                    node: 4,
                },
                WorkDir {
                    path: "/path/to/work2".to_string(),
                    hash: "hash2".to_string(),
                    node: 5,
                },
            ],
        };

        manager.to_toml(&tmp_file).expect("Failed to write TOML");

        let content = fs::read_to_string(&tmp_file).expect("Unable to read test file");
        assert!(content.contains("path = \"/path/to/work1\""));
        assert!(content.contains("hash = \"hash1\""));

        fs::remove_file(&tmp_file).ok();
    }

    #[test]
    fn test_to_toml_atomic_overwrite_leaves_no_temp() {
        let dir = env::temp_dir();
        let tmp_file = format!("{}/test_atomic_3.toml", dir.display());

        // Pre-existing content that must be fully replaced.
        fs::write(
            &tmp_file,
            "works = [{ path = \"/old\", hash = \"old\", node = 1 }]\n",
        )
        .expect("seed file");

        let manager = ScorpioManager {
            works: vec![WorkDir {
                path: "/path/to/new".to_string(),
                hash: "newhash".to_string(),
                node: 7,
            }],
        };
        manager.to_toml(&tmp_file).expect("Failed to write TOML");

        let content = fs::read_to_string(&tmp_file).expect("Unable to read test file");
        assert!(content.contains("/path/to/new"));
        assert!(!content.contains("/old"), "old content must be replaced");

        // No leftover `<file>.tmp.*` sibling files from the atomic write.
        let stem = "test_atomic_3.toml.tmp";
        let leftovers: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(stem))
            .collect();
        assert!(leftovers.is_empty(), "temp files should be renamed away");

        fs::remove_file(&tmp_file).ok();
    }
}
