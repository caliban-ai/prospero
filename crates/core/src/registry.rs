//! The persisted set of repos Prospero manages.
//!
//! The fleet is intentional, not guessed: a blind socket scan can't map a
//! `hash16` socket name back to a repo, so operators register repos by name.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, Result};

/// A single managed repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredRepo {
    /// Operator-chosen short name (registry key).
    pub name: String,
    /// Canonical repo root path.
    pub root: PathBuf,
}

/// The persisted registry of managed repos.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Registry {
    /// Registered repos, keyed by unique `name`.
    pub repos: Vec<RegisteredRepo>,
}

impl Registry {
    /// Load the registry from `path`, returning an empty registry if the file
    /// does not exist yet.
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Registry::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Persist the registry to `path` (creating parent dirs).
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_vec_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Look up a repo by name.
    pub fn get(&self, name: &str) -> Option<&RegisteredRepo> {
        self.repos.iter().find(|r| r.name == name)
    }

    /// Register a repo. Errors if the name is already taken (idempotent only
    /// when the existing entry has the same root).
    pub fn add(&mut self, name: impl Into<String>, root: impl Into<PathBuf>) -> Result<()> {
        let name = name.into();
        let root = root.into();
        if let Some(existing) = self.get(&name) {
            if existing.root == root {
                return Ok(());
            }
            return Err(CoreError::Discovery(format!(
                "repo name '{name}' already registered with a different root"
            )));
        }
        self.repos.push(RegisteredRepo { name, root });
        Ok(())
    }

    /// Remove a repo by name. Returns whether an entry was removed.
    pub fn remove(&mut self, name: &str) -> bool {
        let before = self.repos.len();
        self.repos.retain(|r| r.name != name);
        self.repos.len() != before
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_get_remove() {
        let mut reg = Registry::default();
        reg.add("prospero", "/dev/prospero").unwrap();
        assert_eq!(
            reg.get("prospero").unwrap().root,
            PathBuf::from("/dev/prospero")
        );
        assert!(reg.remove("prospero"));
        assert!(reg.get("prospero").is_none());
        assert!(!reg.remove("prospero"));
    }

    #[test]
    fn add_same_name_same_root_is_idempotent() {
        let mut reg = Registry::default();
        reg.add("p", "/r").unwrap();
        reg.add("p", "/r").unwrap();
        assert_eq!(reg.repos.len(), 1);
    }

    #[test]
    fn add_same_name_different_root_errors() {
        let mut reg = Registry::default();
        reg.add("p", "/r1").unwrap();
        assert!(reg.add("p", "/r2").is_err());
    }

    #[test]
    fn load_missing_file_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registry.json");
        let reg = Registry::load(&path).unwrap();
        assert!(reg.repos.is_empty());
    }

    #[test]
    fn save_then_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/registry.json");
        let mut reg = Registry::default();
        reg.add("a", "/a").unwrap();
        reg.add("b", "/b").unwrap();
        reg.save(&path).unwrap();
        let loaded = Registry::load(&path).unwrap();
        assert_eq!(reg, loaded);
    }
}
