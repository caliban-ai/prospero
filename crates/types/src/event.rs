//! Prospero's normalized event type — the stable contract consumers see.
//!
//! Caliban's raw stream-json frames are normalized into [`FleetEvent`]s by
//! `prospero_core::caliband::stream::normalize_frame`. Consumers (CLI `follow`,
//! dashboard SSE, history replay) never see raw caliban frames. Moved to
//! `prospero-types` so the WASM dashboard can share the type (prospero #98).

use serde::{Deserialize, Serialize};

use crate::model::{AgentStatus, WorkspaceHealth};

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
        /// Caliban's `tool_use_id`, correlating this start with its
        /// [`ToolFinished`]. `#[serde(default)]` for pre-#106 stored events,
        /// which predate this field (they carry only `name`).
        #[serde(default)]
        id: String,
        /// Tool name (e.g. "Read").
        name: String,
        /// Tool input (opaque JSON).
        input: serde_json::Value,
    },
    /// A tool call finished.
    ToolFinished {
        /// Caliban's `tool_use_id`, correlating this finish with its
        /// [`ToolStarted`]. Caliban's `ToolCallEnd` carries the id but not the
        /// name, so consumers pair on the id. `#[serde(default)]` for pre-#106
        /// stored events.
        #[serde(default)]
        id: String,
        /// Tool name. Empty in practice — caliban's `ToolCallEnd` omits it; the
        /// name is on the matching [`ToolStarted`]. Kept for wire compatibility.
        name: String,
        /// Whether the tool succeeded.
        ok: bool,
        /// What the tool returned, as display text: caliban's `result_text`,
        /// the head of the result, bounded (see
        /// `prospero_core::caliband::stream::TOOL_RESULT_CAP`). `None` for
        /// events stored before #236 or a frame that carried no result.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<String>,
        /// `true` when `result` is only the head of a longer result.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        truncated: bool,
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
    /// A workspace's caliband health changed. (Variant name kept as `RepoHealth`
    /// for event-store wire compatibility; the payload type is the renamed
    /// `WorkspaceHealth`, whose serialization is unchanged.)
    RepoHealth {
        /// The new health state.
        state: WorkspaceHealth,
    },
}

/// The ordered stream a `(repo, agent_id)` pair belongs to. Agent events key on
/// the agent id; repo-level events (no agent) on `repo:<name>`; fleet-level
/// events (neither) on the singleton `fleet` stream. `seq` is monotonic *within*
/// the returned key.
pub fn stream_key_for(repo: &str, agent_id: &str) -> String {
    if !agent_id.is_empty() {
        agent_id.to_string()
    } else if !repo.is_empty() {
        format!("repo:{repo}")
    } else {
        "fleet".to_string()
    }
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
    /// The API token name that caused this event, when a request did so
    /// directly (local-fleet spawn / rm). `None` for poll- or watch-derived
    /// events and when auth is disabled. Additive and optional on the wire (#2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

impl FleetEvent {
    /// The ordered stream this event belongs to (see [`stream_key_for`]).
    pub fn stream_key(&self) -> String {
        stream_key_for(&self.repo, &self.agent_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_kind_is_internally_tagged() {
        let k = EventKind::ToolFinished {
            id: "tu_1".into(),
            name: "Read".into(),
            ok: true,
            result: None,
            truncated: false,
        };
        let v = serde_json::to_value(&k).unwrap();
        assert_eq!(v["kind"], "tool_finished");
        assert_eq!(v["id"], "tu_1");
        assert_eq!(v["name"], "Read");
        assert_eq!(v["ok"], true);
    }

    #[test]
    fn tool_finished_carries_its_result_and_truncation() {
        let k = EventKind::ToolFinished {
            id: "tu_1".into(),
            name: String::new(),
            ok: true,
            result: Some("src/main.rs".into()),
            truncated: true,
        };
        let v = serde_json::to_value(&k).unwrap();
        assert_eq!(v["result"], "src/main.rs");
        assert_eq!(v["truncated"], true);
        let back: EventKind = serde_json::from_value(v).unwrap();
        assert_eq!(back, k);
    }

    #[test]
    fn tool_finished_stored_before_results_still_deserializes() {
        // Events persisted before #236 carry no `result`/`truncated`.
        let old =
            serde_json::json!({"kind": "tool_finished", "id": "tu_1", "name": "", "ok": false});
        let k: EventKind = serde_json::from_value(old).unwrap();
        assert_eq!(
            k,
            EventKind::ToolFinished {
                id: "tu_1".into(),
                name: String::new(),
                ok: false,
                result: None,
                truncated: false,
            }
        );
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
            actor: None,
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: FleetEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn stream_key_picks_agent_then_repo_then_fleet() {
        // Agent-level: the agent id is the stream.
        assert_eq!(stream_key_for("prospero", "a1"), "a1");
        // Repo-level (no agent): namespaced repo stream.
        assert_eq!(stream_key_for("prospero", ""), "repo:prospero");
        // Fleet-level (neither): the singleton fleet stream.
        assert_eq!(stream_key_for("", ""), "fleet");
    }

    #[test]
    fn fleet_event_stream_key_delegates() {
        let e = FleetEvent {
            seq: 1,
            ts: "t".into(),
            repo: "prospero".into(),
            agent_id: "".into(),
            kind: EventKind::AgentGone,
            actor: None,
        };
        assert_eq!(e.stream_key(), "repo:prospero");
    }

    #[test]
    fn actor_is_optional_and_omitted_when_none() {
        let mut e = FleetEvent {
            seq: 1,
            ts: "2026-09-13T00:00:00Z".into(),
            repo: "r".into(),
            agent_id: "a".into(),
            kind: EventKind::AgentSpawned,
            actor: None,
        };
        let v = serde_json::to_value(&e).unwrap();
        assert!(v.get("actor").is_none(), "None must not serialize: {v}");
        // Old payloads without the field still deserialize.
        let old = serde_json::json!({
            "seq": 1, "ts": "t", "repo": "r", "agent_id": "a",
            "kind": {"kind": "agent_spawned"}
        });
        assert_eq!(
            serde_json::from_value::<FleetEvent>(old).unwrap().actor,
            None
        );
        e.actor = Some("alice".into());
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["actor"], "alice");
    }
}
