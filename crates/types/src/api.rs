//! Control-plane request/response DTOs — the write half of the HTTP contract.
//!
//! These live here rather than in `prospero-api` for the same reason the read
//! model moved in #98: `prospero-api` pulls axum, tokio, and `prospero-core`, so
//! nothing in it compiles to `wasm32`, and the Dioxus dashboard would otherwise
//! have to hand-duplicate every one of these types — reintroducing exactly the
//! client/server drift Rust/WASM was chosen to avoid.
//!
//! Each type derives **both** `Serialize` and `Deserialize`, so one definition
//! serves both ends: the server deserialises a request body the client
//! serialised, and the client deserialises a response the server serialised.
//! `prospero-api` re-exports all of them from their original paths, so serde
//! output and every import site are unchanged.
//!
//! Behaviour stays out: mapping a [`SpawnBody`] onto `prospero-core`'s
//! `SpawnRequest` is an adapter concern and lives in `prospero-api`, per
//! ADR 0006's one-directional `api → core` rule.

use serde::{Deserialize, Serialize};

use crate::model::{
    ProviderInfo, RepoProviderConfig, Source, WorkspaceConfig, WorkspaceHealth,
    WorkspaceSourceSpec, WorkspaceStatusInfo,
};

/// Backend capability signal for the dashboard (`GET /api/capabilities`).
///
/// Fixed for the process lifetime — the dashboard fetches it once and gates its
/// admin/registry controls on it, so it never offers operations the active
/// backend can't serve. (#99)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetConfigBody(pub WorkspaceConfig);

/// Body for `POST /api/workspaces/{repo}/agents`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Whether this spawn should get an isolated git worktree.
    ///
    /// Isolation defaults to worktree; only the explicit string `"shared"` opts
    /// out. Kept here (not in the api adapter) because it is the *meaning of the
    /// field*, and both ends need to agree on it — the dashboard reads it back
    /// to render the isolation control.
    #[must_use]
    pub fn isolation_worktree(&self) -> bool {
        !matches!(self.isolation.as_deref(), Some("shared"))
    }
}

/// Response for a successful spawn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpawnedResponse {
    /// New agent id.
    pub agent_id: String,
    /// Owning workspace.
    pub workspace: String,
    /// Whether the agent runs in an isolated worktree.
    pub isolated: bool,
}

/// Body for `POST /api/agents/{id}/input`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInputBody {
    /// Message text to inject into the interactive agent.
    pub text: String,
}

/// Response for `POST /api/agents/{id}/respawn`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RespawnedResponse {
    /// The new agent id.
    pub agent_id: String,
}

/// A workspace summary (no agents) for `GET /api/workspaces`.
///
/// The tail fields are populated by the k8s config plane (from `Workspace` CRs)
/// and skipped for the local backend, so local responses are byte-for-byte
/// unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSummary {
    /// Registry name.
    pub name: String,
    /// Workspace root.
    pub root: String,
    /// The source checkouts under the workspace root (1..N).
    pub sources: Vec<Source>,
    /// Caliband health.
    pub health: WorkspaceHealth,
    /// Number of known agents.
    pub agent_count: usize,
    /// Provider/environment config for this workspace.
    pub config: RepoProviderConfig,
    /// Human-friendly label (k8s config plane).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// The workspace's configured source **specs** (k8s config plane).
    ///
    /// `sources` above is the *discovered* view — name and path only — because
    /// that is all a local checkout has. A k8s workspace's sources come from a
    /// `Workspace` CR and additionally carry the git remote and ref, and the
    /// config editor needs those to round-trip an edit: without them an
    /// operator editing a workspace would face blank remote fields and have to
    /// retype every one from memory. Empty for the local backend, so local
    /// responses are unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_specs: Vec<WorkspaceSourceSpec>,
    /// Named providers agents can bind to (k8s config plane).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub providers: Vec<ProviderInfo>,
    /// Provider bound when an agent requests none (k8s config plane).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_provider: Option<String>,
    /// Reconciliation status (k8s config plane); absent for local.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<WorkspaceStatusInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The point of the move: every one of these round-trips through serde in
    /// *both* directions, so the client and the server can share one definition.
    #[test]
    fn every_dto_round_trips_in_both_directions() {
        macro_rules! round_trip {
            ($v:expr) => {{
                let v = $v;
                let json = serde_json::to_string(&v).unwrap();
                let back = serde_json::from_str(&json).unwrap();
                assert_eq!(v, back, "round-trip changed the value: {json}");
            }};
        }

        round_trip!(Capabilities {
            admin: true,
            async_workspace_ops: false,
        });
        round_trip!(AddWorkspaceBody {
            name: "ws".into(),
            root: "/w".into(),
            config: WorkspaceConfig::default(),
        });
        round_trip!(SetConfigBody(WorkspaceConfig::default()));
        round_trip!(SpawnBody {
            prompt: "p".into(),
            interactive: true,
            ..SpawnBody::default()
        });
        round_trip!(SpawnedResponse {
            agent_id: "a".into(),
            workspace: "w".into(),
            isolated: true,
        });
        round_trip!(AgentInputBody { text: "hi".into() });
        round_trip!(RespawnedResponse {
            agent_id: "a2".into(),
        });
        round_trip!(WorkspaceSummary {
            name: "ws".into(),
            root: "/w".into(),
            sources: vec![Source {
                name: "s".into(),
                path: "/w/s".into(),
            }],
            health: WorkspaceHealth::Healthy,
            agent_count: 1,
            config: RepoProviderConfig::default(),
            source_specs: Vec::new(),
            display_name: None,
            providers: Vec::new(),
            default_provider: None,
            status: None,
        });
    }

    #[test]
    fn isolation_defaults_to_worktree_and_only_shared_opts_out() {
        let default = SpawnBody::default();
        assert!(default.isolation_worktree());

        let worktree = SpawnBody {
            isolation: Some("worktree".into()),
            ..SpawnBody::default()
        };
        assert!(worktree.isolation_worktree());

        let shared = SpawnBody {
            isolation: Some("shared".into()),
            ..SpawnBody::default()
        };
        assert!(!shared.isolation_worktree());

        // An unrecognised value must not silently drop isolation.
        let nonsense = SpawnBody {
            isolation: Some("Shared".into()),
            ..SpawnBody::default()
        };
        assert!(nonsense.isolation_worktree());
    }

    /// The local backend's `GET /api/workspaces` payload must not gain k8s keys.
    #[test]
    fn local_workspace_summary_omits_the_k8s_tail() {
        let s = WorkspaceSummary {
            name: "ws".into(),
            root: "/w".into(),
            sources: vec![],
            health: WorkspaceHealth::Healthy,
            agent_count: 0,
            config: RepoProviderConfig::default(),
            source_specs: Vec::new(),
            display_name: None,
            providers: Vec::new(),
            default_provider: None,
            status: None,
        };
        let json = serde_json::to_string(&s).unwrap();
        for absent in [
            "display_name",
            "providers",
            "default_provider",
            "status",
            "source_specs",
        ] {
            assert!(!json.contains(absent), "{absent} leaked into {json}");
        }
    }
}
