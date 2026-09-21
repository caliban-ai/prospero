//! Automations (#220): a stored spawn template plus the trigger that fires it.
//!
//! **No secret appears in this module.** A webhook automation authenticates
//! callers with an HMAC key, and that key lives on `prospero-core`'s
//! `StoredAutomation` instead — so no API response, dashboard view or CLI
//! listing built from these types can leak it, by construction rather than by
//! everyone remembering to redact.

use serde::{Deserialize, Serialize};

/// What starts an automation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub enum Trigger {
    /// A cron schedule, evaluated in UTC. Standard 5-field expressions work;
    /// the 6-field (seconds-first) form is accepted too.
    Cron {
        /// The cron expression.
        schedule: String,
    },
    /// An inbound signed request to `POST /api/automations/{id}/trigger`.
    Webhook,
}

/// The spawn an automation performs each time it fires.
///
/// Mirrors the fields of a manual spawn, so an automation can do nothing a
/// person with `operate` could not do by hand.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct SpawnTemplate {
    /// The agent's task. For webhook triggers this may contain
    /// `{{ dotted.path }}` placeholders filled from the request payload.
    pub task: String,
    /// Optional human-readable label for spawned agents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Optional model override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Optional tool allowlist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_allowlist: Option<Vec<String>>,
    /// Optional agent-template / frontmatter markdown path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frontmatter_path: Option<String>,
    /// Which of the workspace's named providers to bind (k8s config plane).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_ref: Option<String>,
    /// Kill the spawned agent after this many seconds (#221).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    /// Run in an isolated git worktree. Defaults to `true`.
    #[serde(default = "yes")]
    pub isolation_worktree: bool,
    /// How the spawned session handles tool calls needing permission (#238).
    ///
    /// Defaults to `Supervised`, the same fail-closed default a manual spawn
    /// gets. An automation runs with nobody watching, which is an argument for
    /// `Unattended` — but it is the operator's argument to make explicitly,
    /// not something creating an automation should confer by itself.
    #[serde(default)]
    pub permission_posture: crate::model::PermissionPosture,
}

fn yes() -> bool {
    true
}

impl Default for SpawnTemplate {
    fn default() -> Self {
        SpawnTemplate {
            task: String::new(),
            label: None,
            model: None,
            tool_allowlist: None,
            frontmatter_path: None,
            provider_ref: None,
            timeout_secs: None,
            isolation_worktree: true,
            permission_posture: crate::model::PermissionPosture::default(),
        }
    }
}

/// A configured automation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct Automation {
    /// Operator-chosen id (the registry key, unique across the fleet).
    pub id: String,
    /// Target workspace name.
    pub workspace: String,
    /// What fires it.
    pub trigger: Trigger,
    /// What it spawns.
    pub template: SpawnTemplate,
    /// Disabled automations are kept but never fire.
    pub enabled: bool,
    /// RFC 3339 creation time. Doubles as the scheduling anchor before the
    /// first fire, so a new cron automation waits for its next tick rather
    /// than firing the moment it is saved.
    pub created_at: String,
    /// RFC 3339 time of the last fire, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fired_at: Option<String>,
}

/// What caused a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub enum RunSource {
    /// A cron tick came due.
    Schedule,
    /// A signed webhook arrived.
    Webhook,
    /// Someone asked for it explicitly ("run now").
    Manual,
}

/// One recorded firing of an automation, successful or not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct AutomationRun {
    /// The automation that fired.
    pub automation_id: String,
    /// RFC 3339 time of the firing.
    pub fired_at: String,
    /// What caused it.
    pub source: RunSource,
    /// The agent it spawned, absent if the spawn failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// Why it failed, absent on success. A run is recorded either way — an
    /// automation that silently stops working is the failure mode run history
    /// exists to prevent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Body for `POST /api/automations`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct CreateAutomationBody {
    /// Operator-chosen id.
    pub id: String,
    /// Target workspace name.
    pub workspace: String,
    /// What fires it.
    pub trigger: Trigger,
    /// What it spawns.
    pub template: SpawnTemplate,
    /// Create it disabled. Defaults to enabled.
    #[serde(default = "yes")]
    pub enabled: bool,
}

/// Response to `POST /api/automations`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct CreatedAutomationResponse {
    /// The stored automation.
    pub automation: Automation,
    /// For a webhook trigger, the generated signing key — **returned here and
    /// never again**, like an API token. Senders sign their request body with
    /// it and send `X-Prospero-Signature: sha256=<hex>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook_secret: Option<String>,
}

/// Body for `PUT /api/automations/{id}/enabled`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct SetEnabledBody {
    /// Whether the automation should fire.
    pub enabled: bool,
}

/// Response to a fire request (`run now`, or an accepted webhook).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct FiredResponse {
    /// The recorded run.
    pub run: AutomationRun,
}
