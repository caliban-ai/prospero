//! The pure, synchronous tail state machine behind `agent_stream`.
//!
//! It turns each broadcast `recv()` outcome into a set of [`Frame`]s to forward
//! and a decision to keep tailing or close — with no axum or async in sight, so
//! the `Lagged` self-heal path is unit-testable without HTTP or a real channel.

use prospero_core::event::EventKind;
use prospero_core::{FleetEvent, FleetManager};
use tokio::sync::broadcast::error::RecvError;

/// One unit the stream forwards to the client.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Frame {
    /// A real agent event (serialized to the default, unnamed `message` event).
    Event(FleetEvent),
    /// A control signal that `skipped` events were dropped after `last_seq`.
    Gap { skipped: u64, last_seq: u64 },
}

/// What the [`Tailer`] decides to do with one broadcast message.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Step {
    /// Forward these frames (in order), keep tailing.
    Emit(Vec<Frame>),
    /// Forward these frames, then close the stream (terminal event seen).
    EmitAndClose(Vec<Frame>),
    /// Nothing to forward (other agent, or already-delivered seq).
    Skip,
    /// Bus closed; close the stream.
    Close,
}

/// Payload of a `gap` SSE event: `skipped` events were dropped after `last_seq`.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct GapSignal {
    pub skipped: u64,
    pub last_seq: u64,
}

/// Source of persisted events for replay-based self-heal.
pub(crate) trait HistorySource {
    /// Events for `agent_id` with `seq >= from`, in order.
    fn history(&self, agent_id: &str, from: u64) -> Vec<FleetEvent>;
}

impl HistorySource for FleetManager {
    fn history(&self, agent_id: &str, from: u64) -> Vec<FleetEvent> {
        FleetManager::history(self, agent_id, from).unwrap_or_default()
    }
}

/// Drives the live-tail loop for one agent's SSE stream.
pub(crate) struct Tailer<H: HistorySource> {
    agent_id: String,
    /// Highest seq already forwarded to the client (the dedup high-water mark).
    last_delivered: u64,
    history: H,
}

impl<H: HistorySource> Tailer<H> {
    /// `last_delivered` is the last seq emitted during initial history replay
    /// (0 if none).
    pub(crate) fn new(agent_id: String, last_delivered: u64, history: H) -> Self {
        Self {
            agent_id,
            last_delivered,
            history,
        }
    }

    pub(crate) fn on_recv(&mut self, r: Result<FleetEvent, RecvError>) -> Step {
        match r {
            Ok(ev) if ev.agent_id == self.agent_id && ev.seq > self.last_delivered => {
                self.last_delivered = ev.seq;
                let terminal = is_terminal(&ev);
                let frames = vec![Frame::Event(ev)];
                if terminal {
                    Step::EmitAndClose(frames)
                } else {
                    Step::Emit(frames)
                }
            }
            Ok(_) => Step::Skip, // other agent, or already-delivered seq
            Err(RecvError::Lagged(skipped)) => {
                let mut frames = vec![Frame::Gap {
                    skipped,
                    last_seq: self.last_delivered,
                }];
                let mut terminal = false;
                for ev in self.history.history(&self.agent_id, self.last_delivered + 1) {
                    if ev.seq <= self.last_delivered {
                        continue; // defensive dedup
                    }
                    self.last_delivered = ev.seq;
                    terminal = is_terminal(&ev);
                    frames.push(Frame::Event(ev));
                    if terminal {
                        break;
                    }
                }
                if terminal {
                    Step::EmitAndClose(frames)
                } else {
                    Step::Emit(frames)
                }
            }
            Err(RecvError::Closed) => Step::Close,
        }
    }
}

fn is_terminal(ev: &FleetEvent) -> bool {
    matches!(ev.kind, EventKind::AgentFinished { .. })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(seq: u64, agent: &str) -> FleetEvent {
        FleetEvent {
            seq,
            ts: "2026-06-13T00:00:00Z".into(),
            repo: "r".into(),
            agent_id: agent.into(),
            kind: EventKind::Output {
                stream: prospero_core::OutputStream::Stdout,
                chunk: format!("c{seq}"),
            },
        }
    }

    fn finished(seq: u64, agent: &str) -> FleetEvent {
        FleetEvent {
            seq,
            ts: "2026-06-13T00:00:00Z".into(),
            repo: "r".into(),
            agent_id: agent.into(),
            kind: EventKind::AgentFinished {
                outcome: "success".into(),
                cost_usd: 0.0,
                turns: 1,
            },
        }
    }

    /// Fake store: returns held events with `seq >= from` for the matching agent.
    struct FakeHistory(Vec<FleetEvent>);
    impl HistorySource for FakeHistory {
        fn history(&self, agent_id: &str, from: u64) -> Vec<FleetEvent> {
            self.0
                .iter()
                .filter(|e| e.agent_id == agent_id && e.seq >= from)
                .cloned()
                .collect()
        }
    }

    #[test]
    fn forwards_in_order_event_for_this_agent() {
        let mut t = Tailer::new("a".into(), 0, FakeHistory(vec![]));
        assert_eq!(
            t.on_recv(Ok(ev(1, "a"))),
            Step::Emit(vec![Frame::Event(ev(1, "a"))])
        );
    }

    #[test]
    fn skips_other_agents_and_already_delivered() {
        let mut t = Tailer::new("a".into(), 5, FakeHistory(vec![]));
        assert_eq!(t.on_recv(Ok(ev(9, "b"))), Step::Skip); // other agent
        assert_eq!(t.on_recv(Ok(ev(5, "a"))), Step::Skip); // <= last_delivered
        assert_eq!(t.on_recv(Ok(ev(3, "a"))), Step::Skip); // older than high-water
    }

    #[test]
    fn terminal_event_closes() {
        let mut t = Tailer::new("a".into(), 0, FakeHistory(vec![]));
        assert_eq!(
            t.on_recv(Ok(finished(2, "a"))),
            Step::EmitAndClose(vec![Frame::Event(finished(2, "a"))])
        );
    }

    #[test]
    fn lagged_emits_gap_then_replays_missed_events() {
        // Delivered up to seq 2; store holds 3 and 4 we never saw live.
        let store = FakeHistory(vec![ev(3, "a"), ev(4, "a")]);
        let mut t = Tailer::new("a".into(), 2, store);
        assert_eq!(
            t.on_recv(Err(RecvError::Lagged(7))),
            Step::Emit(vec![
                Frame::Gap {
                    skipped: 7,
                    last_seq: 2
                },
                Frame::Event(ev(3, "a")),
                Frame::Event(ev(4, "a")),
            ])
        );
        // High-water advanced: a later live re-delivery of seq 4 is a no-op.
        assert_eq!(t.on_recv(Ok(ev(4, "a"))), Step::Skip);
    }

    #[test]
    fn lagged_replay_containing_terminal_closes() {
        let store = FakeHistory(vec![ev(3, "a"), finished(4, "a")]);
        let mut t = Tailer::new("a".into(), 2, store);
        assert_eq!(
            t.on_recv(Err(RecvError::Lagged(1))),
            Step::EmitAndClose(vec![
                Frame::Gap {
                    skipped: 1,
                    last_seq: 2
                },
                Frame::Event(ev(3, "a")),
                Frame::Event(finished(4, "a")),
            ])
        );
    }

    #[test]
    fn lagged_with_nothing_newer_emits_gap_only() {
        // Persist is also behind: nothing newer than what we delivered.
        let mut t = Tailer::new("a".into(), 5, FakeHistory(vec![ev(5, "a")]));
        assert_eq!(
            t.on_recv(Err(RecvError::Lagged(2))),
            Step::Emit(vec![Frame::Gap {
                skipped: 2,
                last_seq: 5
            }])
        );
    }

    #[test]
    fn closed_bus_closes() {
        let mut t = Tailer::new("a".into(), 0, FakeHistory(vec![]));
        assert_eq!(t.on_recv(Err(RecvError::Closed)), Step::Close);
    }
}
