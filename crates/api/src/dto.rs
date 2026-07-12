//! Request/response payloads for the HTTP API.

use prospero_core::fleet::SpawnRequest;
use prospero_core::registry::{RepoProviderConfig, WorkspaceConfig};
use serde::{Deserialize, Serialize};

/// Backend capability signal for the dashboard (`GET /api/capabilities`). Fixed
/// for the process lifetime — the dashboard fetches it once and gates its
/// admin/registry controls on it, so it never offers operations the active
/// backend can't serve. (#99)
#[derive(Debug, Serialize)]
pub struct Capabilities {
    /// Whether the workspace admin/config plane (add / remove / set-config) is
    /// available. `true` for the local backend (registry) and, as of #142, for
    /// k8s (a `Workspace`-CR editor). Only `false` if a backend leaves the
    /// `admin` seam unwired.
    pub admin: bool,
    /// Whether workspace create/config completes asynchronously — the dashboard
    /// uses this to (a) render the k8s config UI (named-provider list +
    /// Secret-reference credentials, vs the local single-provider env-var form)
    /// and (b) treat a save as *accepted, reconciling* rather than *done*.
    /// `false` for local, `true` for k8s. (#143)
    pub async_workspace_ops: bool,
}

/// Body for `POST /api/workspaces`.
#[derive(Debug, Deserialize)]
pub struct AddWorkspaceBody {
    /// Operator-chosen short name.
    pub name: String,
    /// LocalFleet checkout path. Ignored under k8s (sources come from `config`),
    /// so k8s requests may omit it.
    #[serde(default)]
    pub root: String,
    /// Backend-neutral initial configuration. Local reads the flattened
    /// single-provider/env subset; k8s reads sources/providers/etc.
    #[serde(default)]
    pub config: WorkspaceConfig,
}

/// Body for `PUT /api/workspaces/{name}/config`.
#[derive(Debug, Deserialize)]
pub struct SetConfigBody(pub WorkspaceConfig);

/// Body for `POST /api/workspaces/{repo}/agents`.
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
    /// Optional agent-template / frontmatter markdown file path (#6).
    #[serde(default)]
    pub frontmatter_path: Option<String>,
    /// Which named workspace provider to bind (k8s config plane →
    /// `CalibanTask.providerRef`). `None` ⇒ the workspace's default (#142).
    #[serde(default)]
    pub provider_ref: Option<String>,
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
            frontmatter_path: self.frontmatter_path.map(std::path::PathBuf::from),
            provider_ref: self.provider_ref,
        }
    }
}

/// Response for a successful spawn.
#[derive(Debug, Serialize)]
pub struct SpawnedResponse {
    /// New agent id.
    pub agent_id: String,
    /// Owning workspace.
    pub workspace: String,
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

/// Body for `POST /api/agents/{id}/input`.
#[derive(Debug, Deserialize)]
pub struct AgentInputBody {
    /// Message text to inject into the interactive agent.
    pub text: String,
}

/// Response for `POST /api/agents/{id}/respawn`.
#[derive(Debug, Serialize)]
pub struct RespawnedResponse {
    /// The new agent id.
    pub agent_id: String,
}

/// A workspace summary (no agents) for `GET /api/workspaces`.
///
/// The tail fields are populated by the k8s config plane (from `Workspace` CRs)
/// and skipped for the local backend, so local responses are byte-for-byte
/// unchanged.
#[derive(Debug, Serialize)]
pub struct WorkspaceSummary {
    /// Registry name.
    pub name: String,
    /// Workspace root.
    pub root: String,
    /// The source checkouts under the workspace root (1..N).
    pub sources: Vec<prospero_core::Source>,
    /// Caliband health.
    pub health: prospero_core::WorkspaceHealth,
    /// Number of known agents.
    pub agent_count: usize,
    /// Provider/environment config for this workspace.
    pub config: RepoProviderConfig,
    /// Human-friendly label (k8s config plane).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Named providers agents can bind to (k8s config plane).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub providers: Vec<prospero_core::registry::ProviderInfo>,
    /// Provider bound when an agent requests none (k8s config plane).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_provider: Option<String>,
    /// Reconciliation status (k8s config plane); absent for local.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<prospero_core::registry::WorkspaceStatusInfo>,
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

    #[test]
    fn spawn_body_carries_frontmatter_path() {
        let with: SpawnBody =
            serde_json::from_str(r#"{"prompt":"p","frontmatter_path":"/tpl.md"}"#).unwrap();
        assert_eq!(
            with.into_request().frontmatter_path,
            Some(std::path::PathBuf::from("/tpl.md"))
        );
        let without: SpawnBody = serde_json::from_str(r#"{"prompt":"p"}"#).unwrap();
        assert_eq!(without.into_request().frontmatter_path, None);
    }

    #[test]
    fn workspace_summary_exposes_sources() {
        let s = WorkspaceSummary {
            name: "ws".into(),
            root: "/ws".into(),
            sources: vec![prospero_core::Source {
                name: "a".into(),
                path: "/ws/a".into(),
            }],
            health: prospero_core::WorkspaceHealth::Healthy,
            agent_count: 0,
            config: RepoProviderConfig::default(),
            display_name: None,
            providers: Vec::new(),
            default_provider: None,
            status: None,
        };
        let j = serde_json::to_value(&s).unwrap();
        assert_eq!(j["sources"][0]["name"], "a");
    }
}
