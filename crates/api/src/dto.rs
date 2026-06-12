//! Request/response payloads for the HTTP API.

use prospero_core::fleet::SpawnRequest;
use prospero_core::registry::RepoProviderConfig;
use serde::{Deserialize, Serialize};

/// Body for `POST /api/repos`.
#[derive(Debug, Deserialize)]
pub struct AddRepoBody {
    /// Operator-chosen short name.
    pub name: String,
    /// Repo root path.
    pub root: String,
    /// Optional initial provider config.
    #[serde(default)]
    pub config: RepoProviderConfig,
}

/// Body for `PUT /api/repos/{name}/config`.
#[derive(Debug, Deserialize)]
pub struct SetConfigBody(pub RepoProviderConfig);

/// Body for `POST /api/repos/{repo}/agents`.
#[derive(Debug, Deserialize)]
pub struct SpawnBody {
    /// Initial prompt / task.
    pub prompt: String,
    /// Optional label.
    #[serde(default)]
    pub label: Option<String>,
    /// Optional model override.
    #[serde(default)]
    pub model: Option<String>,
    /// Isolation mode: `"worktree"` (default) or `"shared"`.
    #[serde(default)]
    pub isolation: Option<String>,
    /// Optional tool allowlist.
    #[serde(default)]
    pub tool_allowlist: Option<Vec<String>>,
    /// Run the agent in interactive mode (awaits operator input).
    #[serde(default)]
    pub interactive: bool,
}

impl SpawnBody {
    /// Convert to a core [`SpawnRequest`]. Isolation defaults to worktree;
    /// only the explicit string `"shared"` opts out.
    pub fn into_request(self) -> SpawnRequest {
        let isolation_worktree = !matches!(self.isolation.as_deref(), Some("shared"));
        SpawnRequest {
            prompt: self.prompt,
            label: self.label,
            model: self.model,
            isolation_worktree,
            tool_allowlist: self.tool_allowlist,
            interactive: self.interactive,
        }
    }
}

/// Response for a successful spawn.
#[derive(Debug, Serialize)]
pub struct SpawnedResponse {
    /// New agent id.
    pub agent_id: String,
    /// Owning repo.
    pub repo: String,
    /// Whether the agent runs in an isolated worktree.
    pub isolated: bool,
}

/// Query params for `GET /api/agents/{id}/events` and `/stream`.
#[derive(Debug, Deserialize)]
pub struct FromSeq {
    /// Return events with `seq >= from` (default 0).
    #[serde(default)]
    pub from: u64,
}

/// Response for `POST /api/agents/{id}/respawn`.
#[derive(Debug, Serialize)]
pub struct RespawnedResponse {
    /// The new agent id.
    pub agent_id: String,
}

/// A repo summary (no agents) for `GET /api/repos`.
#[derive(Debug, Serialize)]
pub struct RepoSummary {
    /// Registry name.
    pub name: String,
    /// Repo root.
    pub root: String,
    /// Caliband health.
    pub health: prospero_core::RepoHealth,
    /// Number of known agents.
    pub agent_count: usize,
    /// Provider/environment config for this repo.
    pub config: RepoProviderConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_body_interactive_round_trips_and_defaults_false() {
        let with: SpawnBody = serde_json::from_str(r#"{"prompt":"p","interactive":true}"#).unwrap();
        assert!(with.into_request().interactive);
        let without: SpawnBody = serde_json::from_str(r#"{"prompt":"p"}"#).unwrap();
        assert!(!without.into_request().interactive);
    }
}
