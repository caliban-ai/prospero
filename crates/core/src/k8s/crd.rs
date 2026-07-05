//! A minimal, client-side mirror of the caliban-operator's `CalibanTask`
//! custom resource (`caliban.caliban-ai.dev/v1alpha1`).
//!
//! Per [ADR 0008](../../../../docs/adr/0008-k8s-fleet-backend.md) §1, `K8sFleet`
//! does **not** depend on the caliban-operator crate. Instead it declares just
//! the fields it actually sets (`workspace.sources`, `task.prompt`, optional
//! `isolation`) and reads (`status.phase`, `status.calibandEndpoint`,
//! `status.sandboxRef`) — the same "couple only through the wire" principle
//! as [ADR 0003](../../../../docs/adr/0003-couple-to-caliban-via-ndjson-wire-format.md),
//! applied to the CRD's serialized form instead of an NDJSON wire format.
//!
//! The operator's CRD (`caliban-operator/deploy/crd/calibantask.yaml`) and its
//! `src/crd.rs` are the source of truth for field names/casing; this mirror is
//! deliberately a strict subset. Because serde ignores unknown fields by
//! default (we never set `deny_unknown_fields`), this minimal type still
//! deserializes a fuller operator-produced CR — the golden test below proves
//! it against a sample CR that includes a field prospero omits (`model`).

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Desired state of a caliban task: a workspace of sources + the task to run.
///
/// Mirrors (a subset of) the operator's `CalibanTaskSpec`.
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
    /// The workspace (1..N source checkouts) the task runs over.
    pub workspace: Workspace,
    /// The task itself.
    pub task: TaskSpec,
    /// Sandbox isolation configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation: Option<IsolationSpec>,
}

/// A workspace: the provisioned source set + optional in-pod aux services.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Workspace {
    /// The guaranteed source checkouts (runtime-extensible).
    pub sources: Vec<Source>,
    /// Optional in-pod aux services (e.g. gonzalod, prosperod) for e2e.
    #[serde(default)]
    pub services: Vec<String>,
}

/// A single source checkout in the workspace.
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
}

/// A by-name reference to another object in the same namespace.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NamedRef {
    /// Object name.
    pub name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mirrors the operator's sample CalibanTask CR (camelCase YAML), with an
    // extra `model` block the operator's CRD has but this mirror omits — this
    // proves the minimal mirror tolerates unknown fields from a fuller
    // operator-produced CR instead of failing to deserialize.
    const SAMPLE: &str = r#"
apiVersion: caliban.caliban-ai.dev/v1alpha1
kind: CalibanTask
metadata:
  name: refactor-auth
  namespace: team-a
spec:
  workspace:
    sources:
      - { name: caliban,  repo: "git@example:caliban",  ref: main,       path: /work/caliban }
      - { name: prospero, repo: "git@example:prospero", ref: feat-xport, path: /work/prospero }
    services: [ gonzalod, prosperod ]
  task:      { prompt: "refactor the auth module", agentType: general-purpose }
  model:     { routerConfigRef: caliban-router }
  isolation: { runtimeClass: gvisor, worktrees: per-source }
status:
  phase: Running
  calibandEndpoint: "10.0.0.5:9443"
  sandboxRef: { name: refactor-auth-sandbox }
"#;

    #[test]
    fn sample_cr_deserializes_and_tolerates_unknown_fields() {
        let task: CalibanTask = serde_yaml::from_str(SAMPLE).expect("deserialize sample");

        assert_eq!(task.spec.workspace.sources.len(), 2);
        assert_eq!(task.spec.workspace.sources[0].name, "caliban");
        assert_eq!(task.spec.workspace.sources[0].r#ref, "main");
        assert_eq!(task.spec.workspace.sources[1].r#ref, "feat-xport");
        assert_eq!(task.spec.task.prompt, "refactor the auth module");
        assert_eq!(
            task.spec.task.agent_type.as_deref(),
            Some("general-purpose")
        );

        let status = task.status.expect("status present");
        assert_eq!(status.phase, "Running");
        assert_eq!(status.caliband_endpoint.as_deref(), Some("10.0.0.5:9443"));
        assert_eq!(
            status.sandbox_ref.as_ref().map(|r| r.name.as_str()),
            Some("refactor-auth-sandbox")
        );
    }

    #[test]
    fn ref_defaults_to_main_when_omitted() {
        let yaml = r#"
apiVersion: caliban.caliban-ai.dev/v1alpha1
kind: CalibanTask
metadata: { name: m, namespace: n }
spec:
  workspace: { sources: [ { name: only, repo: "git@x:only", path: /work/only } ] }
  task: { prompt: hi }
"#;
        let task: CalibanTask = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(task.spec.workspace.sources[0].r#ref, "main");
        assert!(task.spec.workspace.services.is_empty());
        assert!(task.spec.task.agent_type.is_none());
        assert!(task.spec.isolation.is_none());
    }

    #[test]
    fn spec_reserializes_with_camel_case_keys() {
        let task: CalibanTask = serde_yaml::from_str(SAMPLE).expect("deserialize sample");
        let json = serde_json::to_value(&task.spec).unwrap();

        assert!(
            json["task"]["agentType"].is_string(),
            "expected camelCase `agentType`, got: {json}"
        );
        assert!(
            json["isolation"]["runtimeClass"].is_string(),
            "expected camelCase `runtimeClass`, got: {json}"
        );
        assert_eq!(json["workspace"]["sources"][0]["ref"], "main");
    }
}
