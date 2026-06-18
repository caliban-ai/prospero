//! Normalize caliban **`TurnEvent`** NDJSON frames into Prospero
//! [`EventKind`]s.
//!
//! Caliban's agent worker writes [`caliban_agent_core::stream::TurnEvent`]
//! records to its stream, each tagged by a PascalCase `"type"` field
//! (`#[serde(tag = "type")]`): `TurnStart`, `AssistantTextDelta`,
//! `AssistantThinkingDelta`, `ToolCallStart`, `ToolCallInputDelta`,
//! `ToolCallEnd`, `TurnEnd`, `RunEnd`. **That enum is the wire contract**
//! (ADR-0003) — this module is the only place that knows its shape.
//!
//! We parse each line into a [`serde_json::Value`] (rather than depending on
//! caliban's crate) so that **unknown frame types are skipped, not fatal** —
//! forward compatibility with caliban. The trade-off is that a *renamed* or
//! *new* variant silently becomes [`Normalized::Unknown`] (counted via the
//! `unknown_frames` metric); `recognizes_every_known_turnevent_type` below is
//! the guard that fails loudly when the known set drifts.

use crate::event::{EventKind, OutputStream};

/// Options controlling normalization.
#[derive(Debug, Clone, Copy, Default)]
pub struct NormalizeOptions {
    /// Include `AssistantThinkingDelta` as `Output { stream: Thinking }`.
    /// Defaults to `false` (privacy/volume).
    pub include_thinking: bool,
}

/// Outcome of normalizing one frame.
#[derive(Debug, PartialEq)]
pub enum Normalized {
    /// A normalized event to emit.
    Event(EventKind),
    /// A recognized frame that intentionally produces no event (e.g. a
    /// `TurnStart` book-keeping frame or a dropped thinking delta).
    Dropped,
    /// An unrecognized frame `type`; the caller should log + count it.
    Unknown,
}

/// Normalize a single parsed caliban `TurnEvent` frame.
pub fn normalize_frame(frame: &serde_json::Value, opts: NormalizeOptions) -> Normalized {
    let ty = match frame.get("type").and_then(|v| v.as_str()) {
        Some(t) => t,
        None => return Normalized::Unknown,
    };

    match ty {
        // Assistant-visible text deltas are the substance of the run.
        "AssistantTextDelta" => Normalized::Event(EventKind::Output {
            stream: OutputStream::Stdout,
            chunk: str_field(frame, "text"),
        }),
        // Reasoning deltas are dropped by default (privacy/volume).
        "AssistantThinkingDelta" => {
            if opts.include_thinking {
                Normalized::Event(EventKind::Output {
                    stream: OutputStream::Thinking,
                    chunk: str_field(frame, "text"),
                })
            } else {
                Normalized::Dropped
            }
        }
        // A tool call opened. The input arrives later via
        // `ToolCallInputDelta`s, so it is not yet known here.
        "ToolCallStart" => Normalized::Event(EventKind::ToolStarted {
            name: str_field(frame, "name"),
            input: serde_json::Value::Null,
        }),
        // A tool call completed. caliban's `ToolCallEnd` carries the
        // `tool_use_id` and `is_error` but not the tool name (that was on the
        // matching `ToolCallStart`), so `name` is left empty.
        "ToolCallEnd" => Normalized::Event(EventKind::ToolFinished {
            name: String::new(),
            ok: !frame
                .get("is_error")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        }),
        // Terminal frame: the whole run finished. This closes the SSE stream.
        "RunEnd" => Normalized::Event(EventKind::AgentFinished {
            outcome: stop_label(frame.get("stopped_for")),
            // caliban reports token usage, not a USD cost; surface 0.0.
            cost_usd: 0.0,
            turns: frame
                .get("turn_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32,
        }),
        // Recognized book-keeping frames we intentionally don't surface:
        // turn boundaries and incremental tool-input JSON (the deltas are
        // already captured as text / the tool lifecycle).
        "TurnStart" | "ToolCallInputDelta" | "TurnEnd" => Normalized::Dropped,
        _ => Normalized::Unknown,
    }
}

fn str_field(frame: &serde_json::Value, key: &str) -> String {
    frame
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

/// Render caliban's `StopCondition` into a short outcome label. It is
/// serialized externally-tagged, so unit variants arrive as a JSON string
/// (`"EndOfTurn"`) and data-carrying variants as a single-key object
/// (`{"MaxTurnsReached": 5}`); both yield the variant name.
fn stop_label(v: Option<&serde_json::Value>) -> String {
    match v {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Object(m)) => m.keys().next().cloned().unwrap_or_default(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn norm(v: serde_json::Value) -> Normalized {
        normalize_frame(&v, NormalizeOptions::default())
    }

    #[test]
    fn assistant_text_delta_maps_to_stdout_output() {
        // Frame copied from caliban's own `attach` fixtures.
        let f = json!({
            "type": "AssistantTextDelta",
            "turn_index": 0, "content_block_index": 0, "text": "hello "
        });
        assert_eq!(
            norm(f),
            Normalized::Event(EventKind::Output {
                stream: OutputStream::Stdout,
                chunk: "hello ".into()
            })
        );
    }

    #[test]
    fn thinking_delta_dropped_by_default_but_included_on_request() {
        let f = json!({
            "type": "AssistantThinkingDelta",
            "turn_index": 0, "content_block_index": 0, "text": "hmm"
        });
        assert_eq!(norm(f.clone()), Normalized::Dropped);
        let opts = NormalizeOptions {
            include_thinking: true,
        };
        assert_eq!(
            normalize_frame(&f, opts),
            Normalized::Event(EventKind::Output {
                stream: OutputStream::Thinking,
                chunk: "hmm".into()
            })
        );
    }

    #[test]
    fn tool_call_start_maps_to_tool_started() {
        assert_eq!(
            norm(json!({
                "type": "ToolCallStart",
                "turn_index": 0, "tool_use_id": "tu_1", "name": "Read"
            })),
            Normalized::Event(EventKind::ToolStarted {
                name: "Read".into(),
                input: serde_json::Value::Null,
            })
        );
    }

    #[test]
    fn tool_call_end_ok_is_inverse_of_is_error() {
        assert_eq!(
            norm(json!({
                "type": "ToolCallEnd",
                "turn_index": 0, "tool_use_id": "tu_1", "is_error": true, "content": []
            })),
            Normalized::Event(EventKind::ToolFinished {
                name: String::new(),
                ok: false
            })
        );
        assert_eq!(
            norm(json!({
                "type": "ToolCallEnd",
                "turn_index": 0, "tool_use_id": "tu_1", "is_error": false, "content": []
            })),
            Normalized::Event(EventKind::ToolFinished {
                name: String::new(),
                ok: true
            })
        );
    }

    #[test]
    fn run_end_is_terminal_and_carries_turns_and_outcome() {
        // Frame shape copied from caliban's `attach` fixtures (StopCondition
        // unit variant serializes as a bare string).
        let f = json!({
            "type": "RunEnd",
            "final_messages": [],
            "total_usage": {"input_tokens": 0, "output_tokens": 0},
            "turn_count": 3,
            "stopped_for": "EndOfTurn"
        });
        assert_eq!(
            norm(f),
            Normalized::Event(EventKind::AgentFinished {
                outcome: "EndOfTurn".into(),
                cost_usd: 0.0,
                turns: 3
            })
        );
    }

    #[test]
    fn run_end_outcome_from_data_carrying_stop_condition() {
        let f = json!({
            "type": "RunEnd",
            "final_messages": [], "total_usage": {},
            "turn_count": 10,
            "stopped_for": {"MaxTurnsReached": 10}
        });
        assert_eq!(
            norm(f),
            Normalized::Event(EventKind::AgentFinished {
                outcome: "MaxTurnsReached".into(),
                cost_usd: 0.0,
                turns: 10
            })
        );
    }

    #[test]
    fn turn_boundary_and_tool_input_frames_are_dropped_not_unknown() {
        for f in [
            json!({"type": "TurnStart", "turn_index": 0, "message_id": "m1", "model": "x"}),
            json!({"type": "ToolCallInputDelta", "turn_index": 0, "tool_use_id": "tu_1", "partial_json": "{"}),
            json!({"type": "TurnEnd", "turn_index": 0}),
        ] {
            assert_eq!(norm(f), Normalized::Dropped);
        }
    }

    #[test]
    fn unknown_or_typeless_frames_are_unknown() {
        assert_eq!(norm(json!({"type": "FutureThing"})), Normalized::Unknown);
        assert_eq!(norm(json!({"no_type": true})), Normalized::Unknown);
    }

    /// Contract guard: every variant of caliban's `TurnEvent` enum (the wire
    /// contract, ADR-0003) must be recognized — i.e. never `Unknown`. If
    /// caliban adds or renames a variant, sync the `match` in `normalize_frame`
    /// and this list together. Source of truth:
    /// `caliban-agent-core::stream::TurnEvent`.
    #[test]
    fn recognizes_every_known_turnevent_type() {
        let opts = NormalizeOptions {
            include_thinking: true,
        };
        for ty in [
            "TurnStart",
            "AssistantTextDelta",
            "AssistantThinkingDelta",
            "ToolCallStart",
            "ToolCallInputDelta",
            "ToolCallEnd",
            "TurnEnd",
            "RunEnd",
        ] {
            let got = normalize_frame(&json!({ "type": ty }), opts);
            assert_ne!(
                got,
                Normalized::Unknown,
                "caliban TurnEvent::{ty} is not recognized by normalize_frame"
            );
        }
    }
}
