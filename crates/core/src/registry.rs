//! The persisted set of workspaces Prospero manages.
//!
//! The fleet is intentional, not guessed: a blind socket scan can't map a
//! `hash16` socket name back to a workspace, so operators register workspaces by
//! name. A workspace is a root directory holding 1..N source checkouts
//! (caliban #281 / ADR 0052); its caliband is keyed on `hash16(root)`.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, Result};

// The per-repo provider config DTO now lives in `prospero-types` (shared with the
// WASM dashboard, prospero #98); re-exported here from its original path.
pub use prospero_types::RepoProviderConfig;
pub use prospero_types::{
    CredentialsRef, IsolationConfig, ProviderInfo, ProviderSpec, WorkspaceConfig, WorkspaceInfo,
    WorkspaceSourceSpec, WorkspaceStatusInfo,
};

/// A single managed workspace's *persisted* identity: name + root + config.
/// Sources are discovered from the filesystem at snapshot-build time (they are
/// not persisted), so they live on the runtime [`crate::model::Workspace`] view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredWorkspace {
    /// Operator-chosen short name (registry key).
    pub name: String,
    /// Canonical workspace root path (the caliband is keyed on `hash16(root)`).
    pub root: PathBuf,
    /// Provider/environment config for this workspace's caliband daemon.
    #[serde(default)]
    pub config: RepoProviderConfig,
}

/// The persisted registry of managed workspaces.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Registry {
    /// Registered workspaces, keyed by unique `name`. The `repos` alias lets a
    /// legacy on-disk registry (`{"repos":[...]}`, pre-#72) load unchanged.
    #[serde(alias = "repos")]
    pub workspaces: Vec<RegisteredWorkspace>,
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

    /// Look up a workspace by name.
    pub fn get(&self, name: &str) -> Option<&RegisteredWorkspace> {
        self.workspaces.iter().find(|r| r.name == name)
    }

    /// Register a workspace. Errors if the name is already taken (idempotent only
    /// when the existing entry has the same root).
    pub fn add(&mut self, name: impl Into<String>, root: impl Into<PathBuf>) -> Result<()> {
        let name = name.into();
        let root = root.into();
        if let Some(existing) = self.get(&name) {
            if existing.root == root {
                return Ok(());
            }
            return Err(CoreError::Conflict(format!(
                "workspace name '{name}' already registered with a different root"
            )));
        }
        // Reject a *different* name occupying the same root: two names for one
        // root alias a single caliband daemon, so both poll the same agents and
        // double-emit into the same event stream. Roots are canonicalized
        // before they reach here (see `FleetManager::add_workspace_with_config`),
        // so this also catches symlink aliases like `/tmp` vs `/private/tmp`. (#47)
        if let Some(other) = self.workspaces.iter().find(|r| r.root == root) {
            return Err(CoreError::Conflict(format!(
                "root {} is already registered as workspace '{}'",
                root.display(),
                other.name
            )));
        }
        self.workspaces.push(RegisteredWorkspace {
            name,
            root,
            config: RepoProviderConfig::default(),
        });
        Ok(())
    }

    /// Remove a workspace by name. Returns whether an entry was removed.
    pub fn remove(&mut self, name: &str) -> bool {
        let before = self.workspaces.len();
        self.workspaces.retain(|r| r.name != name);
        self.workspaces.len() != before
    }

    /// Replace a workspace's provider config. Returns whether it existed.
    pub fn set_config(&mut self, name: &str, config: RepoProviderConfig) -> bool {
        if let Some(r) = self.workspaces.iter_mut().find(|r| r.name == name) {
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
        assert_eq!(reg.workspaces.len(), 1);
    }

    #[test]
    fn add_same_name_different_root_errors() {
        let mut reg = Registry::default();
        reg.add("p", "/r1").unwrap();
        assert!(reg.add("p", "/r2").is_err());
    }

    #[test]
    fn add_different_name_same_root_errors() {
        // Two names for one root would alias a single caliban daemon and
        // double-emit events into the same agent stream. (#47)
        let mut reg = Registry::default();
        reg.add("a", "/r").unwrap();
        let err = reg.add("b", "/r").unwrap_err().to_string();
        assert!(err.contains("workspace 'a'"), "names the holder: {err}");
        assert_eq!(reg.workspaces.len(), 1, "the alias must not be registered");
    }

    #[test]
    fn legacy_repos_json_loads_via_alias() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registry.json");
        // Old on-disk shape used the "repos" key; the alias loads it unchanged.
        std::fs::write(&path, r#"{"repos":[{"name":"p","root":"/r"}]}"#).unwrap();
        let reg = Registry::load(&path).unwrap();
        let ws = reg.get("p").expect("legacy entry loads");
        assert_eq!(ws.root, PathBuf::from("/r"));
        assert_eq!(ws.config, RepoProviderConfig::default());
    }

    #[test]
    fn load_missing_file_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registry.json");
        let reg = Registry::load(&path).unwrap();
        assert!(reg.workspaces.is_empty());
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
        assert!(
            c.provider.is_none()
                && c.base_url.is_none()
                && c.api_key_from_env.is_none()
                && c.env.is_empty()
        );
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
