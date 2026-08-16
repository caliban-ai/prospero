//! Turn a flat stream of events into a readable timeline (#179).
//!
//! `stream.rs` decides *what arrived*; this decides *how it reads*. A long run
//! is a wall of interleaved output and tool chatter, and the structure that
//! makes it legible is grouping each tool call's start with its finish, hoisting
//! the opening context into a header, and coalescing runs of output.
//!
//! No Dioxus and no `web-sys`, so every decision below is unit-tested on the
//! host target — same rule as `stream.rs`.
//!
//! **Pairing is on the id, never the name.** Caliban's `ToolCallEnd` carries the
//! `tool_use_id` but leaves `name` empty, so pairing by name left every tool
//! stuck "running" in v1 (#106). Pre-#106 stored events carry no id at all, so
//! an id-less finish falls back to the oldest still-open call.

use prospero_types::{AgentStatus, EventKind};

use crate::stream::Entry;

/// How a tool call ended, as far as the stream has revealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolOutcome {
    /// Started, no finish seen yet.
    Running,
    /// Finished successfully.
    Ok,
    /// Finished with an error.
    Failed,
}

impl ToolOutcome {
    /// Short label for the outcome pill.
    pub fn label(self) -> &'static str {
        match self {
            ToolOutcome::Running => "running",
            ToolOutcome::Ok => "ok",
            ToolOutcome::Failed => "failed",
        }
    }

    /// Tone class, matching the status pills used elsewhere.
    pub fn tone(self) -> &'static str {
        match self {
            ToolOutcome::Running => "tone-running",
            ToolOutcome::Ok => "tone-done",
            ToolOutcome::Failed => "tone-bad",
        }
    }
}

/// One tool call, start paired with finish.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    /// Sequence of the `ToolStarted` frame — the timeline's sort key.
    pub seq: u64,
    /// Caliban's `tool_use_id`. Empty for pre-#106 events.
    pub id: String,
    /// Tool name, taken from the start frame (the finish omits it).
    pub name: String,
    /// The call's input, opaque JSON.
    pub input: serde_json::Value,
    /// Outcome, or `Running` while unpaired.
    pub outcome: ToolOutcome,
    /// Wall time between start and finish, when both timestamps parsed.
    pub duration_ms: Option<i64>,
}

/// One renderable block of the timeline.
#[derive(Debug, Clone, PartialEq)]
pub enum Segment {
    /// The agent's opening context, hoisted out of the log body.
    Init {
        /// Model the agent is running.
        model: String,
        /// Tools available to it.
        tools: Vec<String>,
        /// Caliban session id.
        session_id: String,
    },
    /// A run of coalesced stdout/stderr.
    Output {
        /// The concatenated text.
        text: String,
    },
    /// A tool call.
    Tool(ToolCall),
    /// A lifecycle transition.
    Status {
        /// Prior status.
        from: AgentStatus,
        /// New status.
        to: AgentStatus,
    },
    /// The terminal accounting.
    Finished {
        /// Result subtype from caliban.
        outcome: String,
        /// Total run cost in USD.
        cost_usd: f64,
        /// Turns taken.
        turns: u32,
    },
    /// The bus dropped events here.
    Gap {
        /// How many were skipped.
        skipped: u64,
    },
    /// A frame that failed to decode.
    Undecodable {
        /// Why it failed.
        why: String,
    },
    /// Any other event kind, shown by name rather than dropped.
    Other {
        /// The event's kind label.
        label: String,
    },
}

/// Group a delivered stream into timeline segments, preserving order.
///
/// Output is coalesced only across *consecutive* output frames: anything else
/// closes the current block, so a tool call that happened between two chunks
/// still renders between them rather than after both.
pub fn group(entries: &[Entry]) -> Vec<Segment> {
    let mut segs: Vec<Segment> = Vec::new();
    // Index into `segs` of the output block still accepting chunks, if any.
    let mut open_output: Option<usize> = None;
    // Indices into `segs` of tool calls awaiting a finish, in start order.
    let mut open_tools: Vec<usize> = Vec::new();
    // Start timestamps, parallel to `open_tools`, for the duration.
    let mut started_at: std::collections::HashMap<usize, String> = std::collections::HashMap::new();

    for entry in entries {
        let event = match entry {
            Entry::Gap { skipped } => {
                open_output = None;
                segs.push(Segment::Gap { skipped: *skipped });
                continue;
            }
            Entry::Undecodable { why } => {
                open_output = None;
                segs.push(Segment::Undecodable { why: why.clone() });
                continue;
            }
            Entry::Event(e) => e,
        };

        match &event.kind {
            EventKind::Output { chunk, .. } => {
                match open_output {
                    Some(i) => {
                        if let Segment::Output { text } = &mut segs[i] {
                            text.push_str(chunk);
                        }
                    }
                    None => {
                        segs.push(Segment::Output {
                            text: chunk.clone(),
                        });
                        open_output = Some(segs.len() - 1);
                    }
                }
                continue;
            }
            EventKind::AgentInit {
                model,
                tools,
                session_id,
            } => {
                segs.push(Segment::Init {
                    model: model.clone(),
                    tools: tools.clone(),
                    session_id: session_id.clone(),
                });
            }
            EventKind::ToolStarted { id, name, input } => {
                segs.push(Segment::Tool(ToolCall {
                    seq: event.seq,
                    id: id.clone(),
                    name: name.clone(),
                    input: input.clone(),
                    outcome: ToolOutcome::Running,
                    duration_ms: None,
                }));
                let at = segs.len() - 1;
                open_tools.push(at);
                started_at.insert(at, event.ts.clone());
            }
            EventKind::ToolFinished { id, ok, .. } => {
                // Pair on the id; fall back to the oldest open call for
                // pre-#106 events that carry none. `name` is deliberately
                // ignored — the finish frame leaves it empty (#106).
                let found = if id.is_empty() {
                    open_tools.first().copied()
                } else {
                    open_tools
                        .iter()
                        .copied()
                        .find(|i| matches!(&segs[*i], Segment::Tool(t) if t.id == *id))
                        .or_else(|| open_tools.first().copied())
                };
                if let Some(i) = found {
                    let duration = started_at.get(&i).and_then(|s| duration_ms(s, &event.ts));
                    if let Segment::Tool(t) = &mut segs[i] {
                        t.outcome = if *ok {
                            ToolOutcome::Ok
                        } else {
                            ToolOutcome::Failed
                        };
                        t.duration_ms = duration;
                    }
                    open_tools.retain(|x| *x != i);
                    started_at.remove(&i);
                }
                // A finish pairs into an existing segment; it never opens one,
                // so the output block stays open across it.
                continue;
            }
            EventKind::StatusChanged { from, to } => {
                segs.push(Segment::Status {
                    from: *from,
                    to: *to,
                });
            }
            EventKind::AgentFinished {
                outcome,
                cost_usd,
                turns,
            } => {
                segs.push(Segment::Finished {
                    outcome: outcome.clone(),
                    cost_usd: *cost_usd,
                    turns: *turns,
                });
            }
            other => {
                segs.push(Segment::Other {
                    label: crate::ui::kind_label(other).to_string(),
                });
            }
        }
        // Everything that fell through opened a non-output segment.
        open_output = None;
    }

    segs
}

/// Milliseconds between two RFC-3339 timestamps, or `None` if either is
/// unparseable or the finish precedes the start.
fn duration_ms(start: &str, end: &str) -> Option<i64> {
    let a = crate::view_model::rfc3339_to_millis(start)?;
    let b = crate::view_model::rfc3339_to_millis(end)?;
    let d = b - a;
    if d < 0.0 { None } else { Some(d as i64) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prospero_types::{FleetEvent, OutputStream};

    fn ev(seq: u64, ts: &str, kind: EventKind) -> Entry {
        Entry::Event(Box::new(FleetEvent {
            seq,
            ts: ts.into(),
            repo: "r".into(),
            agent_id: "a".into(),
            kind,
        }))
    }

    fn started(id: &str, name: &str) -> EventKind {
        EventKind::ToolStarted {
            id: id.into(),
            name: name.into(),
            input: serde_json::json!({ "path": "/x.rs" }),
        }
    }

    fn out(chunk: &str) -> EventKind {
        EventKind::Output {
            stream: OutputStream::Stdout,
            chunk: chunk.into(),
        }
    }

    fn tool(segs: &[Segment]) -> ToolCall {
        segs.iter()
            .find_map(|s| match s {
                Segment::Tool(t) => Some(t.clone()),
                _ => None,
            })
            .expect("expected a tool segment")
    }

    #[test]
    fn start_and_finish_pair_into_one_entry() {
        let segs = group(&[
            ev(1, "2026-08-01T10:00:00+00:00", started("tu_1", "Read")),
            ev(
                2,
                "2026-08-01T10:00:02+00:00",
                EventKind::ToolFinished {
                    id: "tu_1".into(),
                    name: "Read".into(),
                    ok: true,
                },
            ),
        ]);

        assert_eq!(
            segs.len(),
            1,
            "the pair must collapse to one segment: {segs:?}"
        );
        let t = tool(&segs);
        assert_eq!(t.name, "Read");
        assert_eq!(t.outcome, ToolOutcome::Ok);
        assert_eq!(t.duration_ms, Some(2000));
    }

    /// #106: caliban's `ToolCallEnd` omits the name. Pairing on the name left
    /// every tool stuck "running"; pairing on the id must survive it.
    #[test]
    fn pairing_survives_a_finish_with_a_blank_name() {
        let segs = group(&[
            ev(1, "2026-08-01T10:00:00+00:00", started("tu_1", "Read")),
            ev(
                2,
                "2026-08-01T10:00:01+00:00",
                EventKind::ToolFinished {
                    id: "tu_1".into(),
                    name: String::new(),
                    ok: true,
                },
            ),
        ]);

        let t = tool(&segs);
        assert_eq!(
            t.outcome,
            ToolOutcome::Ok,
            "a blank name must not break pairing (#106)"
        );
        assert_eq!(t.name, "Read", "the name comes from the start frame");
    }

    /// Pre-#106 stored events carry no id at all; fall back to the oldest open
    /// call so history still pairs.
    #[test]
    fn an_id_less_finish_pairs_with_the_oldest_open_call() {
        let segs = group(&[
            ev(1, "2026-08-01T10:00:00+00:00", started("", "Read")),
            ev(2, "2026-08-01T10:00:00+00:00", started("", "Write")),
            ev(
                3,
                "2026-08-01T10:00:01+00:00",
                EventKind::ToolFinished {
                    id: String::new(),
                    name: String::new(),
                    ok: false,
                },
            ),
        ]);

        let tools: Vec<&ToolCall> = segs
            .iter()
            .filter_map(|s| match s {
                Segment::Tool(t) => Some(t),
                _ => None,
            })
            .collect();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0].outcome, ToolOutcome::Failed, "oldest pairs first");
        assert_eq!(tools[1].outcome, ToolOutcome::Running);
    }

    #[test]
    fn an_unfinished_call_stays_running() {
        let segs = group(&[ev(1, "2026-08-01T10:00:00+00:00", started("tu_1", "Bash"))]);
        assert_eq!(tool(&segs).outcome, ToolOutcome::Running);
        assert_eq!(tool(&segs).duration_ms, None);
    }

    #[test]
    fn consecutive_output_coalesces_into_one_block() {
        let segs = group(&[ev(1, "t", out("hello ")), ev(2, "t", out("world"))]);
        assert_eq!(
            segs,
            vec![Segment::Output {
                text: "hello world".into()
            }]
        );
    }

    /// The narrative must still read top to bottom: a tool call between two
    /// chunks renders between them, not after both.
    #[test]
    fn a_tool_call_splits_the_output_around_it() {
        let segs = group(&[
            ev(1, "2026-08-01T10:00:00+00:00", out("before")),
            ev(2, "2026-08-01T10:00:00+00:00", started("tu_1", "Read")),
            ev(
                3,
                "2026-08-01T10:00:01+00:00",
                EventKind::ToolFinished {
                    id: "tu_1".into(),
                    name: String::new(),
                    ok: true,
                },
            ),
            ev(4, "2026-08-01T10:00:02+00:00", out("after")),
        ]);

        assert!(
            matches!(segs[0], Segment::Output { ref text } if text == "before"),
            "segments: {segs:?}"
        );
        assert!(matches!(segs[1], Segment::Tool(_)), "segments: {segs:?}");
        assert!(
            matches!(segs[2], Segment::Output { ref text } if text == "after"),
            "segments: {segs:?}"
        );
    }

    #[test]
    fn init_becomes_a_header_segment() {
        let segs = group(&[ev(
            1,
            "t",
            EventKind::AgentInit {
                model: "claude-opus-5".into(),
                tools: vec!["Read".into(), "Bash".into()],
                session_id: "sess_1".into(),
            },
        )]);

        match &segs[0] {
            Segment::Init {
                model,
                tools,
                session_id,
            } => {
                assert_eq!(model, "claude-opus-5");
                assert_eq!(tools.len(), 2);
                assert_eq!(session_id, "sess_1");
            }
            other => panic!("expected an init header, got {other:?}"),
        }
    }

    #[test]
    fn finish_becomes_a_summary_segment() {
        let segs = group(&[ev(
            1,
            "t",
            EventKind::AgentFinished {
                outcome: "EndOfTurn".into(),
                cost_usd: 0.42,
                turns: 3,
            },
        )]);

        assert_eq!(
            segs[0],
            Segment::Finished {
                outcome: "EndOfTurn".into(),
                cost_usd: 0.42,
                turns: 3,
            }
        );
    }

    /// Gaps and undecodable frames are part of the narrative, not noise to drop
    /// — a discontinuous timeline has to say so.
    #[test]
    fn gaps_and_undecodable_frames_stay_in_the_timeline() {
        let segs = group(&[
            Entry::Gap { skipped: 4 },
            Entry::Undecodable {
                why: "bad tag".into(),
            },
        ]);
        assert_eq!(
            segs,
            vec![
                Segment::Gap { skipped: 4 },
                Segment::Undecodable {
                    why: "bad tag".into()
                },
            ]
        );
    }

    /// A gap between two chunks must break the output block — otherwise text
    /// from either side of a discontinuity is silently concatenated.
    #[test]
    fn a_gap_breaks_the_output_block() {
        let segs = group(&[
            ev(1, "t", out("before")),
            Entry::Gap { skipped: 2 },
            ev(4, "t", out("after")),
        ]);
        assert_eq!(segs.len(), 3, "segments: {segs:?}");
    }

    #[test]
    fn an_unparseable_timestamp_leaves_the_duration_absent() {
        let segs = group(&[
            ev(1, "nonsense", started("tu_1", "Read")),
            ev(
                2,
                "also nonsense",
                EventKind::ToolFinished {
                    id: "tu_1".into(),
                    name: String::new(),
                    ok: true,
                },
            ),
        ]);
        let t = tool(&segs);
        assert_eq!(t.outcome, ToolOutcome::Ok, "pairing still works");
        assert_eq!(t.duration_ms, None, "but the duration is unknown");
    }
}
