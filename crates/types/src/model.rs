//! Fleet read-model DTOs (wasm-compatible). Moved out of `prospero-core` so the
//! WASM dashboard can share them (prospero #98); `prospero-core` re-exports each
//! from its original module path. Serde output is unchanged.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// One source checkout within a workspace. (The `discover_sources` filesystem
/// logic stays in `prospero-core`; only this struct is shared.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Source {
    /// Directory basename (unique within a workspace).
    pub name: String,
    /// Absolute path to the source checkout.
    pub path: PathBuf,
}

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
    /// in the repo config store and returned by the repos/fleet API — do not
    /// put secrets here; use `api_key_from_env` for credentials.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

/// A source checkout spec for a workspace: a git remote and where to mount it.
/// Used by the k8s config plane to build a `Workspace` CR's `sources[]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSourceSpec {
    /// Source identifier (matches caliband's workspace source name).
    pub name: String,
    /// Git remote to clone.
    pub repo: String,
    /// Git ref to check out (defaults to `main` when omitted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#ref: Option<String>,
    /// Absolute mount path in the pod (e.g. `/work/caliban`).
    pub path: String,
}

/// A named model provider within a workspace. Each agent binds one by name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderSpec {
    /// Provider identifier, unique within the workspace (e.g. `planner`).
    pub name: String,
    /// Provider kind (e.g. `ollama`, `anthropic`, `openai`).
    pub kind: String,
    /// Override base URL (e.g. `http://192.168.1.240:11434`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Default model for this provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Reference to an existing Secret holding this provider's API key. Keyless
    /// providers (e.g. ollama) omit it. Prospero only *names* the Secret — it
    /// never reads it (the operator validates existence).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_ref: Option<CredentialsRef>,
}

/// A by-name reference to a key within an existing Secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialsRef {
    /// Name of the Secret (same namespace).
    pub secret_name: String,
    /// Key within the Secret's data.
    pub key: String,
}

/// Isolation defaults for agents launched against a workspace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IsolationConfig {
    /// RuntimeClass (e.g. `gvisor`, `kata`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_class: Option<String>,
    /// Worktree isolation strategy (e.g. `per-source`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktrees: Option<String>,
}

/// Backend-neutral workspace configuration accepted at the API boundary
/// (`POST /api/workspaces`, `PUT /api/workspaces/{name}/config`).
///
/// The rich fields (`sources`/`providers`/`default_provider`/`isolation`) drive a
/// k8s `Workspace` CR; the flattened [`RepoProviderConfig`] carries the
/// `LocalFleet` single-provider/env shape. Each backend **projects out the
/// subset it uses**, so one endpoint serves both: `#[serde(flatten)]` keeps
/// legacy local bodies (`{provider, base_url, api_key_from_env, env}`)
/// deserializing unchanged, while k8s reads the named-provider list + Secret
/// references it needs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceConfig {
    /// Human-friendly dashboard label (k8s `displayName`; local ignores it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Git source checkouts (k8s only; local derives its sources from `root`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<WorkspaceSourceSpec>,
    /// Named providers (k8s only; local uses the flattened single provider).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub providers: Vec<ProviderSpec>,
    /// Provider name agents get when they don't request one (k8s only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_provider: Option<String>,
    /// Default isolation for agents (k8s only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation: Option<IsolationConfig>,
    /// LocalFleet single-provider/env configuration. Flattened so the existing
    /// local request shape is unchanged.
    #[serde(flatten)]
    pub local: RepoProviderConfig,
}

/// Aggregate readiness of prosperod, distinct from mere liveness.
///
/// `ready` gates traffic/restarts: it is `true` only when the durable store can
/// accept writes. The workspace-health counts are an informational summary
/// (per-workspace reachability is already surfaced in `/api/workspaces` and `/api/fleet`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Readiness {
    /// Overall ready signal — currently equivalent to `store_writable`.
    pub ready: bool,
    /// Whether the durable event store can accept writes.
    pub store_writable: bool,
    /// Total managed workspaces.
    pub workspaces_total: usize,
    /// Workspaces whose caliband responded to the last poll.
    pub workspaces_healthy: usize,
    /// Workspaces whose caliband was unreachable at the last poll.
    pub workspaces_unreachable: usize,
}

/// Lifecycle state of an agent. Mirrors caliban's `AgentStatus` wire enum exactly
/// so the same value round-trips through both protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    /// Registered, not yet executing.
    Spawning,
    /// Actively running (or attached).
    Running,
    /// Awaiting input; no compute pending.
    Idle,
    /// Stopped via kill.
    Killed,
    /// Finished successfully.
    Done,
    /// Finished with an error.
    Failed,
    /// Supervisor restarted while active; needs recovery.
    Crashed,
}

impl AgentStatus {
    /// True for states where the agent will produce no further work.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            AgentStatus::Killed | AgentStatus::Done | AgentStatus::Failed | AgentStatus::Crashed
        )
    }

    /// True while the agent may still be streaming output worth attaching to.
    pub fn is_active(self) -> bool {
        matches!(self, AgentStatus::Spawning | AgentStatus::Running)
    }
}

/// Connectivity of a managed workspace's caliband daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WorkspaceHealth {
    /// The control socket responded to the last poll.
    Healthy,
    /// The control socket could not be reached; carries the reason.
    Unreachable {
        /// Human-readable reason from the last failed poll.
        reason: String,
    },
}

/// Prospero's view of a single agent (projected from a caliban `AgentRecord`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Agent {
    /// Opaque caliban agent id.
    pub id: String,
    /// Human-readable label.
    pub name: String,
    /// Owning workspace name (Prospero registry key).
    pub workspace: String,
    /// Current lifecycle state.
    pub status: AgentStatus,
    /// RFC-3339 timestamp when the agent was registered.
    pub started_at: String,
    /// True if the agent runs in an isolated git worktree.
    pub isolated: bool,
    /// True if the agent was spawned in interactive mode (accepts operator input).
    pub interactive: bool,
    /// Path to the agent's session directory on disk.
    pub session_dir: PathBuf,
}

/// A managed workspace (root + its source checkouts) and the agents running
/// under its single caliband.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workspace {
    /// Registry key (operator-chosen short name).
    pub name: String,
    /// Canonical workspace root path.
    pub root: PathBuf,
    /// The source checkouts discovered under `root` (1..N). Filesystem-derived
    /// at snapshot-build time, not persisted.
    #[serde(default)]
    pub sources: Vec<Source>,
    /// Health of the workspace's caliband daemon.
    pub health: WorkspaceHealth,
    /// The workspace's provider config (so operators can read back what a workspace is
    /// configured with). Defaults to empty for workspaces with no config set.
    #[serde(default)]
    pub config: RepoProviderConfig,
    /// Agents currently known under this workspace.
    pub agents: Vec<Agent>,
}

/// A point-in-time view of the whole fleet on one host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FleetSnapshot {
    /// Host identity (single host in the first stab).
    pub host: String,
    /// Managed workspaces and their agents.
    pub workspaces: Vec<Workspace>,
}

impl FleetSnapshot {
    /// Find an agent by id across all workspaces, returning `(repo_name, &Agent)`.
    pub fn find_agent(&self, id: &str) -> Option<(&str, &Agent)> {
        self.workspaces.iter().find_map(|r| {
            r.agents
                .iter()
                .find(|a| a.id == id)
                .map(|a| (r.name.as_str(), a))
        })
    }
}

/// Stable identifier for a running agent (caliband's agent id).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentId(pub String);

impl AgentId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl From<&str> for AgentId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}
impl From<String> for AgentId {
    fn from(s: String) -> Self {
        Self(s)
    }
}
impl std::fmt::Display for AgentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_id_roundtrips_str() {
        let id = AgentId::from("agent-abc");
        assert_eq!(id.as_str(), "agent-abc");
        assert_eq!(id.to_string(), "agent-abc");
    }

    #[test]
    fn status_serializes_snake_case() {
        let j = serde_json::to_string(&AgentStatus::Running).unwrap();
        assert_eq!(j, "\"running\"");
    }

    #[test]
    fn status_terminal_and_active_partition_correctly() {
        for s in [AgentStatus::Spawning, AgentStatus::Running] {
            assert!(s.is_active() && !s.is_terminal());
        }
        for s in [
            AgentStatus::Killed,
            AgentStatus::Done,
            AgentStatus::Failed,
            AgentStatus::Crashed,
        ] {
            assert!(s.is_terminal() && !s.is_active());
        }
        // Idle is neither active nor terminal: awaiting input.
        assert!(!AgentStatus::Idle.is_active() && !AgentStatus::Idle.is_terminal());
    }

    #[test]
    fn repo_health_tags_state() {
        let j = serde_json::to_string(&WorkspaceHealth::Healthy).unwrap();
        assert_eq!(j, "{\"state\":\"healthy\"}");
        let j = serde_json::to_string(&WorkspaceHealth::Unreachable {
            reason: "no socket".into(),
        })
        .unwrap();
        assert_eq!(j, "{\"state\":\"unreachable\",\"reason\":\"no socket\"}");
    }

    #[test]
    fn find_agent_searches_across_repos() {
        let snap = FleetSnapshot {
            host: "local".into(),
            workspaces: vec![Workspace {
                name: "prospero".into(),
                root: "/r".into(),
                sources: vec![],
                health: WorkspaceHealth::Healthy,
                config: RepoProviderConfig::default(),
                agents: vec![Agent {
                    id: "a1".into(),
                    name: "x".into(),
                    workspace: "prospero".into(),
                    status: AgentStatus::Running,
                    started_at: "t".into(),
                    isolated: true,
                    interactive: false,
                    session_dir: "/s".into(),
                }],
            }],
        };
        let (workspace, agent) = snap.find_agent("a1").unwrap();
        assert_eq!(workspace, "prospero");
        assert_eq!(agent.id, "a1");
        assert!(snap.find_agent("nope").is_none());
    }
}
