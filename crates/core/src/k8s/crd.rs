//! Minimal, client-side mirrors of the caliban-operator custom resources
//! (`caliban.caliban-ai.dev/v1alpha1`): [`CalibanTask`] and [`Workspace`].
//!
//! Per [ADR 0008](../../../../docs/adr/0008-k8s-fleet-backend.md) §1, `K8sFleet`
//! does **not** depend on the caliban-operator crate. Instead it declares just
//! the fields it actually sets and reads — the same "couple only through the
//! wire" principle as [ADR 0003](../../../../docs/adr/0003-couple-to-caliban-via-ndjson-wire-format.md),
//! applied to the CRD's serialized form instead of an NDJSON wire format.
//!
//! The operator's CRDs (`caliban-operator/deploy/crd/*.yaml`) and `src/{crd,workspace}.rs`
//! are the source of truth for field names/casing; these mirrors are deliberately
//! a strict subset. Because serde ignores unknown fields by default (we never set
//! `deny_unknown_fields`), a minimal type still deserializes a fuller
//! operator-produced CR — the golden tests below prove it against the operator's
//! committed sample CRs (vendored to `crates/core/tests/fixtures/`).
//!
//! ## Post-#11 shape (breaking change)
//! caliban-operator #11 (merged 2026-07-12) replaced the CalibanTask's inline
//! `spec.workspace` with a **`workspaceRef`** naming a first-class [`Workspace`]
//! CR the operator resolves and pins into `status.resolvedWorkspace` at
//! admission. This mirror follows suit: prospero writes `workspaceRef` /
//! `providerRef` and reads the pinned `resolvedWorkspace`. Pre-v1, no back-compat.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// CalibanTask
// ---------------------------------------------------------------------------

/// Desired state of a caliban task: a reference to the workspace it runs against
/// plus the task to run. Mirrors (a subset of) the operator's `CalibanTaskSpec`.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "caliban.caliban-ai.dev",
    version = "v1alpha1",
    kind = "CalibanTask",
    namespaced,
    status = "CalibanTaskStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct CalibanTaskSpec {
    /// Reference to the namespace-local [`Workspace`] this task runs against
    /// (replaces the removed inline `workspace`).
    pub workspace_ref: WorkspaceRef,
    /// Which of the workspace's providers to bind; defaults (operator-side) to
    /// the workspace's `defaultProvider` (or its sole provider) when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_ref: Option<String>,
    /// The task itself.
    pub task: TaskSpec,
    /// Sandbox isolation configuration (per-run override).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation: Option<IsolationSpec>,
    /// Per-run tool allow-list override for this task's agents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<String>>,
}

/// A by-name reference to a [`Workspace`] in the same namespace.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceRef {
    /// Workspace object name.
    pub name: String,
}

/// A single source checkout in a workspace.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    /// Source identifier (matches caliband's workspace source name).
    pub name: String,
    /// Git remote to clone.
    pub repo: String,
    /// Git ref to check out. Defaults to `main`.
    #[serde(rename = "ref", default = "default_ref")]
    pub r#ref: String,
    /// Absolute mount path in the pod (e.g. `/work/caliban`).
    pub path: String,
}

fn default_ref() -> String {
    "main".to_string()
}

/// The task to run in the workspace.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskSpec {
    /// Initial prompt.
    pub prompt: String,
    /// Agent type (e.g. `general-purpose`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_type: Option<String>,
}

/// Sandbox isolation configuration.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IsolationSpec {
    /// RuntimeClass (e.g. `gvisor`, `kata`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_class: Option<String>,
    /// Worktree isolation strategy (e.g. `per-source`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktrees: Option<String>,
}

/// Observed state of a `CalibanTask` — the subset `K8sFleet` reads.
///
/// `phase` is read as a plain `String` (rather than the operator's `Phase`
/// enum) so an operator-side phase this mirror doesn't know about still
/// deserializes; `K8sFleet` maps the string onto its own `AgentStatus`.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CalibanTaskStatus {
    /// Lifecycle phase (`"Pending"`, `"Provisioning"`, `"Running"`, ...).
    #[serde(default)]
    pub phase: String,
    /// caliband session endpoint (host:port), once the Sandbox is ready.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caliband_endpoint: Option<String>,
    /// The agent-sandbox Sandbox backing this task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_ref: Option<NamedRef>,
    /// The workspace config the operator resolved and pinned at admission
    /// (immutable run). `K8sFleet` reads its `sources` for the agent's
    /// workspace label; absent until the operator reconciles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_workspace: Option<ResolvedWorkspace>,
}

/// A by-name reference to another object in the same namespace.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NamedRef {
    /// Object name.
    pub name: String,
}

/// The workspace config a `CalibanTask` runs against, resolved to a single
/// provider and pinned into `status.resolvedWorkspace` at admission. Prospero
/// reads this; it never writes it (operator-owned).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedWorkspace {
    /// The workspace's source checkouts.
    #[serde(default)]
    pub sources: Vec<Source>,
    /// The single provider this task bound to.
    pub provider: ResolvedProvider,
    /// Non-secret env from the workspace.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvEntry>,
    /// Workspace default isolation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation: Option<IsolationSpec>,
}

/// A provider with its workspace context flattened in — the pinned form.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedProvider {
    /// Provider name.
    pub name: String,
    /// Provider kind (e.g. `ollama`, `anthropic`).
    pub kind: String,
    /// Base URL, if set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Model, if set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Credential Secret reference, if the provider needs one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_ref: Option<CredentialsRef>,
}

// ---------------------------------------------------------------------------
// Workspace
// ---------------------------------------------------------------------------

/// Desired state of a workspace: sources + named providers + defaults. Mirrors
/// (a subset of) the operator's `WorkspaceSpec`. Prospero is a pure *editor* of
/// this CR (ADR 0008 §1); the operator owns reconciliation and Secret validation.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "caliban.caliban-ai.dev",
    version = "v1alpha1",
    kind = "Workspace",
    namespaced,
    status = "WorkspaceStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSpec {
    /// Human-friendly dashboard label.
    pub display_name: String,
    /// The workspace's git checkouts (1..N).
    #[serde(default)]
    pub sources: Vec<Source>,
    /// Named providers (1..N) agents in this workspace can bind to.
    #[serde(default)]
    pub providers: Vec<Provider>,
    /// Provider name agents get when they don't request one. Implicit when
    /// exactly one provider is defined.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_provider: Option<String>,
    /// Non-secret environment injected into every agent pod.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvEntry>,
    /// Default isolation for agents launched against this workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation: Option<IsolationSpec>,
}

/// A named model provider bound within a workspace.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Provider {
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
    /// Reference to an existing Secret for this provider's API key. Keyless
    /// providers (e.g. ollama) omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_ref: Option<CredentialsRef>,
}

/// A by-name reference to a key within an existing Kubernetes Secret. Prospero
/// only names it — it never reads the Secret (the operator validates existence).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CredentialsRef {
    /// Name of the Secret (same namespace).
    pub secret_name: String,
    /// Key within the Secret's data.
    pub key: String,
}

/// A non-secret environment entry.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EnvEntry {
    /// Variable name.
    pub name: String,
    /// Variable value.
    pub value: String,
}

/// Observed state of a `Workspace` — the subset `K8sFleet` reads to surface
/// reconciliation status on the dashboard. `phase` is a plain `String` (like
/// [`CalibanTaskStatus::phase`]) so an unknown operator phase still deserializes.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceStatus {
    /// Lifecycle phase (`"Pending"`, `"Reconciling"`, `"Ready"`, `"Failed"`).
    #[serde(default)]
    pub phase: String,
    /// The `.metadata.generation` this status reflects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Human-readable detail (e.g. `provider 'planner': secret ... not found`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The operator's committed sample `CalibanTask` (post-#11 shape:
    /// `workspaceRef` + `providerRef`), vendored to a fixture. Deserializing it
    /// into the minimal mirror proves the coupling boundary (ADR 0008 §1).
    const CALIBANTASK_SAMPLE: &str = include_str!("../../tests/fixtures/operator-calibantask.yaml");
    /// The operator's committed sample `Workspace`, vendored to a fixture.
    const WORKSPACE_SAMPLE: &str = include_str!("../../tests/fixtures/operator-workspace.yaml");

    #[test]
    fn calibantask_sample_deserializes_and_reserializes_camel_case() {
        let task: CalibanTask =
            serde_yaml::from_str(CALIBANTASK_SAMPLE).expect("deserialize sample CalibanTask");

        assert_eq!(task.spec.workspace_ref.name, "team-a-ws");
        assert_eq!(task.spec.provider_ref.as_deref(), Some("workers"));
        assert_eq!(task.spec.task.prompt, "refactor the auth module");
        assert_eq!(
            task.spec.task.agent_type.as_deref(),
            Some("general-purpose")
        );

        // camelCase keys survive a round-trip (the wire contract).
        let json = serde_json::to_value(&task.spec).unwrap();
        assert!(
            json["workspaceRef"]["name"].is_string(),
            "expected camelCase `workspaceRef`, got: {json}"
        );
        assert!(
            json["task"]["agentType"].is_string(),
            "expected camelCase `agentType`, got: {json}"
        );
        // The removed inline `workspace` must not reappear.
        assert!(
            json.get("workspace").is_none(),
            "inline `workspace` is gone"
        );
    }

    #[test]
    fn workspace_sample_deserializes_named_providers() {
        let ws: Workspace =
            serde_yaml::from_str(WORKSPACE_SAMPLE).expect("deserialize sample Workspace");

        assert_eq!(ws.spec.display_name, "Team A");
        assert_eq!(ws.spec.sources.len(), 1);
        assert_eq!(ws.spec.providers.len(), 2);
        assert_eq!(ws.spec.providers[0].name, "planner");
        assert_eq!(ws.spec.providers[0].kind, "anthropic");
        assert_eq!(
            ws.spec.providers[0]
                .credentials_ref
                .as_ref()
                .map(|c| (c.secret_name.as_str(), c.key.as_str())),
            Some(("anthropic-key", "api-key"))
        );
        // Keyless provider (ollama) omits credentialsRef.
        assert_eq!(ws.spec.providers[1].name, "workers");
        assert!(ws.spec.providers[1].credentials_ref.is_none());
        assert_eq!(
            ws.spec.providers[1].base_url.as_deref(),
            Some("http://192.168.1.240:11434")
        );
        assert_eq!(ws.spec.default_provider.as_deref(), Some("planner"));

        // camelCase survives round-trip.
        let json = serde_json::to_value(&ws.spec).unwrap();
        assert!(json["displayName"].is_string());
        assert!(json["providers"][0]["credentialsRef"]["secretName"].is_string());
    }

    #[test]
    fn calibantask_ref_defaults_and_optionals() {
        let yaml = r#"
apiVersion: caliban.caliban-ai.dev/v1alpha1
kind: CalibanTask
metadata: { name: m, namespace: n }
spec:
  workspaceRef: { name: only-ws }
  task: { prompt: hi }
"#;
        let task: CalibanTask = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(task.spec.workspace_ref.name, "only-ws");
        assert!(task.spec.provider_ref.is_none());
        assert!(task.spec.task.agent_type.is_none());
        assert!(task.spec.isolation.is_none());
        assert!(task.spec.tools.is_none());
    }

    #[test]
    fn status_reads_resolved_workspace_and_tolerates_unknown_fields() {
        // A fuller operator-produced status: pinned resolvedWorkspace plus a
        // `conditions` field this mirror omits (must not fail to deserialize).
        let yaml = r#"
apiVersion: caliban.caliban-ai.dev/v1alpha1
kind: CalibanTask
metadata: { name: refactor-auth, namespace: team-a }
spec:
  workspaceRef: { name: team-a-ws }
  providerRef: workers
  task: { prompt: "go" }
status:
  phase: Running
  calibandEndpoint: "10.0.0.5:9443"
  sandboxRef: { name: refactor-auth-sandbox }
  conditions: [ { type: Ready, status: "True" } ]
  resolvedWorkspace:
    sources:
      - { name: caliban, repo: "git@example:caliban", ref: main, path: /work/caliban }
    provider: { name: workers, kind: ollama, baseUrl: "http://192.168.1.240:11434", model: qwen2.5-coder }
"#;
        let task: CalibanTask = serde_yaml::from_str(yaml).unwrap();
        let status = task.status.expect("status present");
        assert_eq!(status.phase, "Running");
        assert_eq!(status.caliband_endpoint.as_deref(), Some("10.0.0.5:9443"));
        let rw = status
            .resolved_workspace
            .expect("resolvedWorkspace present");
        assert_eq!(rw.sources.len(), 1);
        assert_eq!(rw.sources[0].name, "caliban");
        assert_eq!(rw.provider.name, "workers");
        assert_eq!(rw.provider.kind, "ollama");
    }

    #[test]
    fn workspace_status_reads_phase_and_message() {
        let yaml = r#"
apiVersion: caliban.caliban-ai.dev/v1alpha1
kind: Workspace
metadata: { name: w, namespace: n }
spec:
  displayName: W
  sources: [ { name: only, repo: "git@x:only", path: /work/only } ]
  providers: [ { name: p, kind: ollama } ]
status:
  phase: Failed
  observedGeneration: 3
  message: "provider 'p': secret 'k' key 'v' not found"
"#;
        let ws: Workspace = serde_yaml::from_str(yaml).unwrap();
        let status = ws.status.expect("status present");
        assert_eq!(status.phase, "Failed");
        assert_eq!(status.observed_generation, Some(3));
        assert_eq!(
            status.message.as_deref(),
            Some("provider 'p': secret 'k' key 'v' not found")
        );
        // Source `ref` defaults to main when omitted.
        assert_eq!(ws.spec.sources[0].r#ref, "main");
    }
}
