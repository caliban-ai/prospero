//! Deriving the `AgentsSettled` condition prospero reports into
//! `CalibanTask.status` (#228).
//!
//! Per caliban-operator ADR 0005 the operator stays infrastructure-only and
//! never becomes a caliband client, so it cannot tell "agent running" from
//! "agent finished, pod still up". Prospero already polls caliband's agent
//! list, so it owns agent lifecycle and reports it back into the CR.

use crate::AgentStatus;
use crate::k8s::crd::Condition;

/// The condition type prospero owns on `CalibanTask.status.conditions`.
pub const AGENTS_SETTLED: &str = "AgentsSettled";

/// Derive the `AgentsSettled` condition from a task's agents.
///
/// Settled means no agent is still `Spawning`/`Running`/`Idle` — an idle
/// interactive task is deliberately *not* settled, because it is waiting for
/// operator input rather than finished. A task with no agents yet is likewise
/// unsettled. Once settled, any `Failed`/`Crashed` agent makes the whole task
/// `Failed`; otherwise it `Succeeded`.
pub fn agents_settled_condition(agents: &[AgentStatus], now: &str) -> Condition {
    let settled = !agents.is_empty() && agents.iter().all(|s| s.is_terminal());
    let (status, reason) = if !settled {
        ("False", "AgentsActive")
    } else if agents
        .iter()
        .any(|s| matches!(s, AgentStatus::Failed | AgentStatus::Crashed))
    {
        ("True", "Failed")
    } else {
        ("True", "Succeeded")
    };
    Condition {
        r#type: AGENTS_SETTLED.to_string(),
        status: status.to_string(),
        reason: reason.to_string(),
        message: None,
        last_transition_time: now.to_string(),
    }
}

/// The server-side-apply body that reports `condition` on `name`'s status.
///
/// Deliberately minimal: apply semantics mean a field manager owns exactly what
/// it sends, so this carries `status.conditions` with our single entry and
/// nothing else. `phase`, `calibandEndpoint`, `sandboxRef`, `resolvedWorkspace`
/// and the operator's own conditions stay owned by caliban-operator.
pub fn status_condition_patch(name: &str, condition: &Condition) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "caliban.caliban-ai.dev/v1alpha1",
        "kind": "CalibanTask",
        "metadata": { "name": name },
        "status": { "conditions": [condition] },
    })
}

#[cfg(test)]
mod tests {
    use super::{AGENTS_SETTLED, agents_settled_condition};
    use crate::AgentStatus;

    const NOW: &str = "2026-09-15T00:00:00Z";

    #[test]
    fn all_finished_cleanly_is_settled_succeeded() {
        let c = agents_settled_condition(&[AgentStatus::Done, AgentStatus::Killed], NOW);
        assert_eq!(c.r#type, AGENTS_SETTLED);
        assert_eq!(c.status, "True");
        assert_eq!(c.reason, "Succeeded");
        assert_eq!(c.last_transition_time, NOW);
    }

    #[test]
    fn any_bad_ending_is_settled_failed() {
        for bad in [AgentStatus::Failed, AgentStatus::Crashed] {
            let c = agents_settled_condition(&[AgentStatus::Done, bad], NOW);
            assert_eq!(c.status, "True", "{bad:?}");
            assert_eq!(c.reason, "Failed", "{bad:?}");
        }
    }

    #[test]
    fn an_unfinished_agent_is_not_settled() {
        for busy in [
            AgentStatus::Spawning,
            AgentStatus::Running,
            AgentStatus::Idle,
        ] {
            let c = agents_settled_condition(&[AgentStatus::Done, busy], NOW);
            assert_eq!(c.status, "False", "{busy:?}");
            assert_eq!(c.reason, "AgentsActive", "{busy:?}");
        }
    }

    #[test]
    fn no_agents_yet_is_not_settled() {
        let c = agents_settled_condition(&[], NOW);
        assert_eq!(c.status, "False");
        assert_eq!(c.reason, "AgentsActive");
    }

    #[test]
    fn the_patch_body_carries_only_our_condition() {
        let c = agents_settled_condition(&[AgentStatus::Done], NOW);
        let body = super::status_condition_patch("task-7", &c);

        assert_eq!(body["apiVersion"], "caliban.caliban-ai.dev/v1alpha1");
        assert_eq!(body["kind"], "CalibanTask");
        assert_eq!(body["metadata"]["name"], "task-7");

        let conditions = body["status"]["conditions"]
            .as_array()
            .expect("status.conditions is an array");
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0]["type"], AGENTS_SETTLED);
        assert_eq!(conditions[0]["status"], "True");
        assert_eq!(conditions[0]["reason"], "Succeeded");
        assert_eq!(conditions[0]["lastTransitionTime"], NOW);

        // Operator-owned fields must never appear: applying them under our
        // field manager would fight caliban-operator for ownership.
        let status = body["status"].as_object().expect("status is an object");
        assert_eq!(
            status.keys().collect::<Vec<_>>(),
            vec!["conditions"],
            "status must carry conditions only, got {status:?}"
        );
    }
}
