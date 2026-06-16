//! Prospero's normalized event type — the stable contract consumers see.
//!
//! Caliban's raw stream-json frames are normalized into [`FleetEvent`]s by
//! [`crate::caliband::stream::normalize_frame`]. Consumers (CLI `follow`,
//! dashboard SSE, history replay) never see raw caliban frames.

use serde::{Deserialize, Serialize};

use crate::model::{AgentStatus, RepoHealth};

/// Which textual stream a chunk of output came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    /// Assistant-visible text.
    Stdout,
    /// Model reasoning (dropped from history by default).
    Thinking,
}

/// The semantic payload of a fleet event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventKind {
    /// Prospero asked caliband to spawn this agent.
    AgentSpawned,
    /// A poll discovered an agent Prospero had not seen before.
    AgentDiscovered,
    /// The agent's stream emitted its init frame.
    AgentInit {
        /// Model the agent is running.
        model: String,
        /// Tools available to the agent.
        tools: Vec<String>,
        /// Caliban session id.
        session_id: String,
    },
    /// A poll observed a lifecycle transition.
    StatusChanged {
        /// Prior status.
        from: AgentStatus,
        /// New status.
        to: AgentStatus,
    },
    /// A chunk of streamed output.
    Output {
        /// Which stream the chunk belongs to.
        stream: OutputStream,
        /// The text chunk.
        chunk: String,
    },
    /// A tool call started.
    ToolStarted {
        /// Tool name (e.g. "Read").
        name: String,
        /// Tool input (opaque JSON).
        input: serde_json::Value,
    },
    /// A tool call finished.
    ToolFinished {
        /// Tool name.
        name: String,
        /// Whether the tool succeeded.
        ok: bool,
    },
    /// The agent finished; carries the final accounting.
    AgentFinished {
        /// Result subtype (e.g. "success", "max_turns", "budget_exceeded").
        outcome: String,
        /// Total run cost in USD.
        cost_usd: f64,
        /// Number of turns taken.
        turns: u32,
    },
    /// A poll observed the agent disappear from caliband's registry.
    AgentGone,
    /// A durable-store append failed, so the event with `lost_seq` reached the
    /// live bus but is **absent from durable history**. This marker is itself
    /// persisted (best-effort) so a history reader — not just a log scraper —
    /// sees that the live and durable views diverged here. See ADR-0004.
    StorePersistFailed {
        /// The `seq` of the event that could not be persisted.
        lost_seq: u64,
        /// Rendered append error, for diagnosis.
        detail: String,
    },
    /// A repo's caliband health changed.
    RepoHealth {
        /// The new health state.
        state: RepoHealth,
    },
}

/// A normalized, sequenced fleet event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FleetEvent {
    /// Monotonic sequence number assigned by the `FleetManager`.
    pub seq: u64,
    /// RFC-3339 timestamp.
    pub ts: String,
    /// Owning repo name ("" for fleet-level events).
    pub repo: String,
    /// Owning agent id ("" for repo-level events).
    pub agent_id: String,
    /// The event payload.
    pub kind: EventKind,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_kind_is_internally_tagged() {
        let k = EventKind::ToolFinished {
            name: "Read".into(),
            ok: true,
        };
        let v = serde_json::to_value(&k).unwrap();
        assert_eq!(v["kind"], "tool_finished");
        assert_eq!(v["name"], "Read");
        assert_eq!(v["ok"], true);
    }

    #[test]
    fn fleet_event_round_trips() {
        let e = FleetEvent {
            seq: 7,
            ts: "2026-06-05T00:00:00Z".into(),
            repo: "prospero".into(),
            agent_id: "a1".into(),
            kind: EventKind::Output {
                stream: OutputStream::Stdout,
                chunk: "hi".into(),
            },
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: FleetEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }
}
