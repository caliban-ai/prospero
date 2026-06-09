//! The persisted set of repos Prospero manages.
//!
//! The fleet is intentional, not guessed: a blind socket scan can't map a
//! `hash16` socket name back to a repo, so operators register repos by name.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, Result};

/// Per-repo provider/environment configuration applied to its caliband daemon.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoProviderConfig {
    /// Selected provider → `CALIBAN_PROVIDER`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Provider base URL / host → `{PROVIDER}_BASE_URL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// NAME of an env var in prosperod's environment whose value is injected as
    /// `{PROVIDER}_API_KEY` at spawn time. Never the literal secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_from_env: Option<String>,
    /// Raw escape-hatch env overrides (highest precedence within a repo).
    ///
    /// Unlike `api_key_from_env` (a reference), values here are stored verbatim
    /// in `registry.json` and returned by the repos API — do not put secrets
    /// here; use `api_key_from_env` for credentials.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

/// A single managed repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredRepo {
    /// Operator-chosen short name (registry key).
    pub name: String,
    /// Canonical repo root path.
    pub root: PathBuf,
    /// Provider/environment config for this repo's caliband daemon.
    #[serde(default)]
    pub config: RepoProviderConfig,
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
        self.repos.push(RegisteredRepo {
            name,
            root,
            config: RepoProviderConfig::default(),
        });
        Ok(())
    }

    /// Remove a repo by name. Returns whether an entry was removed.
    pub fn remove(&mut self, name: &str) -> bool {
        let before = self.repos.len();
        self.repos.retain(|r| r.name != name);
        self.repos.len() != before
    }

    /// Replace a repo's provider config. Returns whether the repo existed.
    pub fn set_config(&mut self, name: &str, config: RepoProviderConfig) -> bool {
        if let Some(r) = self.repos.iter_mut().find(|r| r.name == name) {
            r.config = config;
            true
        } else {
            false
        }
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

    #[test]
    fn repo_config_defaults_empty() {
        let c = RepoProviderConfig::default();
        assert!(c.provider.is_none() && c.base_url.is_none()
            && c.api_key_from_env.is_none() && c.env.is_empty());
    }

    #[test]
    fn old_registry_json_without_config_loads_with_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registry.json");
        std::fs::write(&path, r#"{"repos":[{"name":"p","root":"/r"}]}"#).unwrap();
        let reg = Registry::load(&path).unwrap();
        assert_eq!(reg.get("p").unwrap().config, RepoProviderConfig::default());
    }

    #[test]
    fn set_config_updates_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registry.json");
        let mut reg = Registry::default();
        reg.add("p", "/r").unwrap();
        let cfg = RepoProviderConfig {
            provider: Some("ollama".into()),
            base_url: Some("http://host:11434".into()),
            ..Default::default()
        };
        assert!(reg.set_config("p", cfg.clone()));
        assert!(!reg.set_config("missing", cfg.clone()));
        reg.save(&path).unwrap();
        let loaded = Registry::load(&path).unwrap();
        assert_eq!(loaded.get("p").unwrap().config, cfg);
    }
}
