//! Prospero's fleet domain model: `Host -> Workspace -> [Agent]`.
//!
//! The read-model DTOs (`Agent`, `Workspace`, `FleetSnapshot`, `AgentStatus`,
//! `WorkspaceHealth`, `Readiness`, `AgentId`) now live in [`prospero_types`] so
//! the WASM dashboard can share them (prospero #98); they are re-exported here
//! from their original path. The control-plane types below (`TaskSpec`,
//! `AgentHandle`, `DrainPolicy`, `FleetChange`) stay in `prospero-core` — they
//! reference core-only types (`SpawnRequest`, `Endpoint`).

use serde::{Deserialize, Serialize};

pub use prospero_types::{
    Agent, AgentId, AgentStatus, FleetSnapshot, Readiness, Workspace, WorkspaceHealth,
};

/// Desired state for one agent — the provider-agnostic spec `ensure_agent` takes.
/// Generalizes today's `(workspace, SpawnRequest)` pair (fleet.rs:618).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub workspace: String,
    pub request: crate::fleet::SpawnRequest,
}

/// Handle to a provisioned agent, resolved when it is attachable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentHandle {
    pub id: AgentId,
    pub workspace: String,
    /// Endpoint the agent's per-agent socket is reachable at. `None` until the
    /// backend has resolved one — e.g. a k8s agent between spawn and Running.
    pub endpoint: Option<crate::caliband::wire::Endpoint>,
}

/// How to stop an agent. `Kill` preserves today's unconditional behavior.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainPolicy {
    #[default]
    Kill,
    Graceful {
        timeout_ms: u64,
    },
}

/// A change in the observed fleet — the item type of `watch_fleet`.
/// Mirrors the poll-diff variants `reconcile` already emits (fleet.rs:811).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FleetChange {
    Discovered {
        id: AgentId,
        workspace: String,
        agent: Agent,
    },
    StatusChanged {
        id: AgentId,
        workspace: String,
        from: AgentStatus,
        to: AgentStatus,
    },
    Gone {
        id: AgentId,
        workspace: String,
    },
    WorkspaceHealth {
        workspace: String,
        health: WorkspaceHealth,
    },
}

#[cfg(test)]
mod fleet_provider_types_tests {
    use super::*;

    #[test]
    fn drain_policy_defaults_to_kill() {
        assert!(matches!(DrainPolicy::default(), DrainPolicy::Kill));
    }
    #[test]
    fn fleet_change_serdes() {
        let c = FleetChange::StatusChanged {
            id: AgentId::from("a1"),
            workspace: "r".into(),
            from: AgentStatus::Spawning,
            to: AgentStatus::Running,
        };
        let j = serde_json::to_string(&c).unwrap();
        let back: FleetChange = serde_json::from_str(&j).unwrap();
        assert_eq!(back, c);
    }
}
