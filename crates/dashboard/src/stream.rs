//! Per-agent event stream state: what has been rendered, where to resume, and
//! whether the connection is healthy.
//!
//! No Dioxus and no `web-sys` here — the browser plumbing lives in `ui.rs`, and
//! everything that decides *what happens* is plain data, unit-tested on the
//! host target. That matters more than usual for this file: v1 shipped a
//! reconnect storm that duplicated the timeline unboundedly (#105), and the
//! defence against it is the sequence bookkeeping below.

use std::time::Duration;

use prospero_types::{EventKind, FleetEvent, GapSignal};

/// Longest a reconnect will ever wait between attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(15);

/// Health of the live connection, as shown to the operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamState {
    /// Opening, nothing received yet.
    Connecting,
    /// Receiving.
    Live,
    /// Dropped; retrying. Carries the attempt count so the UI can say so.
    Reconnecting { attempt: u32 },
    /// The agent finished and the server closed the stream. **Not** an error —
    /// a completed run looks exactly like this and must not read as broken.
    Closed,
}

impl StreamState {
    /// Whether this state means "something is wrong", as opposed to a run that
    /// simply ended.
    pub fn is_problem(&self) -> bool {
        matches!(self, StreamState::Reconnecting { .. })
    }
}

/// One rendered entry in the stream.
#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    /// A real event from the agent.
    Event(Box<FleetEvent>),
    /// A notice that the bus dropped events. Rendered inline so the timeline is
    /// honest about being discontinuous instead of quietly skipping.
    Gap { skipped: u64 },
    /// A frame the client could not decode, with the reason.
    Undecodable { why: String },
}

/// Everything known about one agent's stream.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamSession {
    /// Which agent this follows.
    pub agent_id: String,
    /// Entries in delivery order.
    pub entries: Vec<Entry>,
    /// Highest sequence number rendered, if any.
    pub last_seq: Option<u64>,
    /// Connection health.
    pub state: StreamState,
    /// Consecutive failed connection attempts.
    pub attempt: u32,
}

impl StreamSession {
    /// Start following an agent.
    pub fn new(agent_id: impl Into<String>) -> Self {
        Self {
            agent_id: agent_id.into(),
            entries: Vec::new(),
            last_seq: None,
            state: StreamState::Connecting,
            attempt: 0,
        }
    }

    /// Where a (re)connection should resume from.
    ///
    /// The server's `from` is inclusive, so this is one past the last rendered
    /// sequence. Resuming at `last_seq` instead would re-deliver the final event
    /// on every reconnect — which is precisely how a reconnect loop turns into
    /// an unbounded duplicate timeline (#105).
    pub fn resume_from(&self) -> u64 {
        self.last_seq.map_or(0, |s| s + 1)
    }

    /// Accept an event, ignoring anything already rendered.
    ///
    /// Returns whether it was added. The dedup is belt-and-braces: the server
    /// already dedups against `from`, but a reconnect racing an in-flight
    /// message can still deliver an overlap, and rendering it twice is exactly
    /// the bug this guards.
    pub fn push(&mut self, event: FleetEvent) -> bool {
        if let Some(last) = self.last_seq
            && event.seq <= last
        {
            return false;
        }
        let terminal = is_terminal(&event);
        self.last_seq = Some(event.seq);
        self.entries.push(Entry::Event(Box::new(event)));
        self.state = if terminal {
            StreamState::Closed
        } else {
            StreamState::Live
        };
        self.attempt = 0;
        true
    }

    /// Record a bus gap.
    pub fn push_gap(&mut self, gap: GapSignal) {
        // A gap of nothing is not worth a line in the timeline.
        if gap.skipped == 0 {
            return;
        }
        self.entries.push(Entry::Gap {
            skipped: gap.skipped,
        });
        // The server self-heals by replaying from the store, so the events
        // after the gap still arrive; only note that some were missed. Do NOT
        // advance last_seq to the gap's last_seq — that would skip past events
        // the replay is about to deliver.
    }

    /// Record a frame that could not be decoded.
    ///
    /// Shown rather than swallowed. A client/server wire mismatch otherwise
    /// presents as an empty pane that never leaves "connecting" — indis-
    /// tinguishable from a hung agent, and far harder to diagnose than a line
    /// saying what failed to parse.
    pub fn push_undecodable(&mut self, why: impl Into<String>) {
        self.entries.push(Entry::Undecodable { why: why.into() });
    }

    /// The connection dropped.
    ///
    /// A stream that already reached its terminal event is *finished*, not
    /// broken: the server closes it deliberately. Treating that close as a
    /// failure would put a healthy completed run into a permanent retry loop.
    pub fn disconnected(&mut self) {
        if self.state == StreamState::Closed {
            return;
        }
        self.attempt = self.attempt.saturating_add(1);
        self.state = StreamState::Reconnecting {
            attempt: self.attempt,
        };
    }

    /// How long to wait before the next attempt: exponential, capped.
    pub fn backoff(&self) -> Duration {
        backoff_for(self.attempt)
    }

    /// Whether this session should keep trying to reconnect.
    pub fn should_retry(&self) -> bool {
        self.state != StreamState::Closed
    }
}

/// Exponential backoff, capped at [`MAX_BACKOFF`].
///
/// Capped rather than unbounded so a long outage still recovers promptly once
/// the daemon returns, and floored at a real delay so a server that instantly
/// closes the connection cannot spin the client at full speed (#105).
pub fn backoff_for(attempt: u32) -> Duration {
    if attempt == 0 {
        return Duration::from_millis(500);
    }
    let millis = 500u64.saturating_mul(1u64 << attempt.min(6));
    Duration::from_millis(millis).min(MAX_BACKOFF)
}

/// Whether an event ends the run.
pub fn is_terminal(event: &FleetEvent) -> bool {
    matches!(event.kind, EventKind::AgentFinished { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use prospero_types::OutputStream;

    fn event(seq: u64) -> FleetEvent {
        FleetEvent {
            seq,
            agent_id: "a1".into(),
            repo: "ws".into(),
            ts: "2026-08-15T12:00:00Z".into(),
            kind: EventKind::Output {
                stream: OutputStream::Stdout,
                chunk: format!("line {seq}"),
            },
        }
    }

    fn finished(seq: u64) -> FleetEvent {
        FleetEvent {
            seq,
            agent_id: "a1".into(),
            repo: "ws".into(),
            ts: "2026-08-15T12:00:00Z".into(),
            kind: EventKind::AgentFinished {
                outcome: "success".into(),
                turns: 3,
                cost_usd: 0.12,
            },
        }
    }

    #[test]
    fn a_fresh_session_resumes_from_zero() {
        let s = StreamSession::new("a1");
        assert_eq!(s.resume_from(), 0);
        assert_eq!(s.state, StreamState::Connecting);
    }

    #[test]
    fn resume_is_one_past_the_last_rendered_event() {
        // Resuming AT last_seq would re-deliver it every reconnect — the
        // unbounded-duplicate bug from #105.
        let mut s = StreamSession::new("a1");
        s.push(event(7));
        assert_eq!(s.last_seq, Some(7));
        assert_eq!(s.resume_from(), 8);
    }

    #[test]
    fn already_rendered_events_are_dropped_not_duplicated() {
        let mut s = StreamSession::new("a1");
        assert!(s.push(event(1)));
        assert!(s.push(event(2)));
        // A reconnect racing an in-flight message re-delivers the overlap.
        assert!(!s.push(event(1)), "seq 1 was already rendered");
        assert!(!s.push(event(2)), "seq 2 was already rendered");
        assert_eq!(s.entries.len(), 2);

        // Forward progress still works.
        assert!(s.push(event(3)));
        assert_eq!(s.entries.len(), 3);
    }

    /// The regression that motivates all of the above: a flapping connection
    /// must not grow the timeline without bound.
    #[test]
    fn a_reconnect_storm_does_not_grow_the_timeline() {
        let mut s = StreamSession::new("a1");
        for seq in 1..=3 {
            s.push(event(seq));
        }
        for _ in 0..50 {
            s.disconnected();
            // The server replays from `resume_from`, but a racing tail can
            // re-send what we already have.
            for seq in 1..=3 {
                s.push(event(seq));
            }
        }
        assert_eq!(s.entries.len(), 3, "timeline grew on reconnect");
    }

    #[test]
    fn a_terminal_event_closes_the_stream_and_stops_retrying() {
        let mut s = StreamSession::new("a1");
        s.push(event(1));
        s.push(finished(2));
        assert_eq!(s.state, StreamState::Closed);
        assert!(!s.should_retry());

        // The server closes a finished stream deliberately; that close must not
        // be read as a failure or the run would retry forever.
        s.disconnected();
        assert_eq!(s.state, StreamState::Closed, "a finished run went to retry");
        assert!(!s.state.is_problem());
    }

    #[test]
    fn a_drop_before_the_end_is_a_problem_and_counts_attempts() {
        let mut s = StreamSession::new("a1");
        s.push(event(1));
        s.disconnected();
        assert_eq!(s.state, StreamState::Reconnecting { attempt: 1 });
        assert!(s.state.is_problem());
        s.disconnected();
        assert_eq!(s.state, StreamState::Reconnecting { attempt: 2 });
        assert!(s.should_retry());

        // A successful event resets the attempt counter.
        s.push(event(2));
        assert_eq!(s.attempt, 0);
        assert_eq!(s.state, StreamState::Live);
    }

    #[test]
    fn a_gap_is_rendered_but_does_not_advance_the_sequence() {
        let mut s = StreamSession::new("a1");
        s.push(event(1));
        s.push_gap(GapSignal {
            skipped: 4,
            last_seq: 5,
        });
        assert_eq!(s.entries.len(), 2);
        assert!(matches!(s.entries[1], Entry::Gap { skipped: 4 }));
        // Advancing to the gap's last_seq would skip the events the server's
        // self-heal replay is about to deliver.
        assert_eq!(s.last_seq, Some(1));
        assert_eq!(s.resume_from(), 2);
    }

    #[test]
    fn an_empty_gap_is_not_rendered() {
        let mut s = StreamSession::new("a1");
        s.push_gap(GapSignal {
            skipped: 0,
            last_seq: 3,
        });
        assert!(s.entries.is_empty());
    }

    #[test]
    fn an_undecodable_frame_is_surfaced_not_swallowed() {
        let mut s = StreamSession::new("a1");
        s.push(event(1));
        s.push_undecodable("event: missing field `kind`");
        assert_eq!(s.entries.len(), 2);
        match &s.entries[1] {
            Entry::Undecodable { why } => assert!(why.contains("kind")),
            other => panic!("expected an undecodable entry, got {other:?}"),
        }
        // It must not disturb the sequence bookkeeping.
        assert_eq!(s.last_seq, Some(1));
        assert_eq!(s.resume_from(), 2);
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        assert_eq!(backoff_for(0), Duration::from_millis(500));
        assert!(backoff_for(2) > backoff_for(1));
        assert!(backoff_for(4) > backoff_for(3));
        // Capped, so a long outage still recovers promptly once the daemon is
        // back rather than waiting minutes.
        assert_eq!(backoff_for(50), MAX_BACKOFF);
        // Never zero — a server that instantly closes must not spin the client.
        for attempt in 0..60 {
            assert!(backoff_for(attempt) >= Duration::from_millis(500));
            assert!(backoff_for(attempt) <= MAX_BACKOFF);
        }
    }

    #[test]
    fn out_of_order_history_never_rewinds_the_high_water_mark() {
        let mut s = StreamSession::new("a1");
        s.push(event(10));
        assert!(!s.push(event(4)), "an older event must not be rendered");
        assert_eq!(s.last_seq, Some(10));
        assert_eq!(s.resume_from(), 11);
    }
}
