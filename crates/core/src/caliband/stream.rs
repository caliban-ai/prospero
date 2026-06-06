//! Normalize caliban headless **stream-json** frames into Prospero
//! [`EventKind`]s.
//!
//! Caliban frames are tagged by a `"type"` field. We parse each NDJSON line
//! into a [`serde_json::Value`] (rather than a fixed enum) so that **unknown
//! frame types are skipped, not fatal** — forward compatibility with caliban.

use crate::event::{EventKind, OutputStream};

/// Options controlling normalization.
#[derive(Debug, Clone, Copy)]
pub struct NormalizeOptions {
    /// Include `thinking` deltas as `Output { stream: Thinking }`.
    /// Defaults to `false` (privacy/volume).
    pub include_thinking: bool,
}

impl Default for NormalizeOptions {
    fn default() -> Self {
        Self {
            include_thinking: false,
        }
    }
}

/// Outcome of normalizing one frame.
#[derive(Debug, PartialEq)]
pub enum Normalized {
    /// A normalized event to emit.
    Event(EventKind),
    /// A recognized frame that intentionally produces no event (e.g. a
    /// dropped `thinking` delta).
    Dropped,
    /// An unrecognized frame `type`; the caller should log + count it.
    Unknown,
}

/// Normalize a single parsed caliban stream-json frame.
pub fn normalize_frame(frame: &serde_json::Value, opts: NormalizeOptions) -> Normalized {
    let ty = match frame.get("type").and_then(|v| v.as_str()) {
        Some(t) => t,
        None => return Normalized::Unknown,
    };

    match ty {
        "system" => {
            // Only the init subtype carries data we surface.
            if frame.get("subtype").and_then(|v| v.as_str()) == Some("init") {
                Normalized::Event(EventKind::AgentInit {
                    model: str_field(frame, "model"),
                    tools: frame
                        .get("tools")
                        .and_then(|v| v.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|t| t.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default(),
                    session_id: str_field(frame, "session_id"),
                })
            } else {
                Normalized::Dropped
            }
        }
        "text" => Normalized::Event(EventKind::Output {
            stream: OutputStream::Stdout,
            chunk: str_field(frame, "delta"),
        }),
        "thinking" => {
            if opts.include_thinking {
                Normalized::Event(EventKind::Output {
                    stream: OutputStream::Thinking,
                    chunk: str_field(frame, "delta"),
                })
            } else {
                Normalized::Dropped
            }
        }
        "tool_use" => Normalized::Event(EventKind::ToolStarted {
            name: str_field(frame, "name"),
            input: frame
                .get("input")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        }),
        "tool_result" => Normalized::Event(EventKind::ToolFinished {
            name: str_field(frame, "name"),
            ok: !frame
                .get("is_error")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        }),
        "result" => Normalized::Event(EventKind::AgentFinished {
            outcome: str_field(frame, "subtype"),
            cost_usd: frame
                .get("total_cost_usd")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            turns: frame.get("turns").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
        }),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn norm(v: serde_json::Value) -> Normalized {
        normalize_frame(&v, NormalizeOptions::default())
    }

    #[test]
    fn init_frame_maps_to_agent_init() {
        let got = norm(json!({
            "type": "system", "subtype": "init",
            "model": "claude-sonnet-4-6",
            "tools": ["Read", "Bash"],
            "session_id": "s1"
        }));
        assert_eq!(
            got,
            Normalized::Event(EventKind::AgentInit {
                model: "claude-sonnet-4-6".into(),
                tools: vec!["Read".into(), "Bash".into()],
                session_id: "s1".into(),
            })
        );
    }

    #[test]
    fn text_frame_maps_to_stdout_output() {
        assert_eq!(
            norm(json!({"type": "text", "delta": "hello"})),
            Normalized::Event(EventKind::Output {
                stream: OutputStream::Stdout,
                chunk: "hello".into()
            })
        );
    }

    #[test]
    fn thinking_dropped_by_default_but_included_on_request() {
        assert_eq!(
            norm(json!({"type": "thinking", "delta": "hmm"})),
            Normalized::Dropped
        );
        let opts = NormalizeOptions {
            include_thinking: true,
        };
        assert_eq!(
            normalize_frame(&json!({"type": "thinking", "delta": "hmm"}), opts),
            Normalized::Event(EventKind::Output {
                stream: OutputStream::Thinking,
                chunk: "hmm".into()
            })
        );
    }

    #[test]
    fn tool_result_ok_is_inverse_of_is_error() {
        assert_eq!(
            norm(json!({"type": "tool_result", "name": "Read", "is_error": true})),
            Normalized::Event(EventKind::ToolFinished {
                name: "Read".into(),
                ok: false
            })
        );
        assert_eq!(
            norm(json!({"type": "tool_result", "name": "Read"})),
            Normalized::Event(EventKind::ToolFinished {
                name: "Read".into(),
                ok: true
            })
        );
    }

    #[test]
    fn result_frame_carries_cost_and_turns() {
        assert_eq!(
            norm(
                json!({"type": "result", "subtype": "success", "total_cost_usd": 1.25, "turns": 5})
            ),
            Normalized::Event(EventKind::AgentFinished {
                outcome: "success".into(),
                cost_usd: 1.25,
                turns: 5
            })
        );
    }

    #[test]
    fn unknown_or_typeless_frames_are_unknown() {
        assert_eq!(norm(json!({"type": "future_thing"})), Normalized::Unknown);
        assert_eq!(norm(json!({"no_type": true})), Normalized::Unknown);
    }
}
