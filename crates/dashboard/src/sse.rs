//! Browser plumbing for the per-agent SSE stream.
//!
//! Everything that *decides* something — what to resume from, what to dedup,
//! how long to back off — lives in [`crate::stream`] and is unit-tested on the
//! host target. This module only moves bytes from an `EventSource` into that
//! state machine.

use futures::StreamExt;
use gloo_net::eventsource::futures::EventSource;
use prospero_types::{FleetEvent, GapSignal};

use crate::stream::StreamSession;

/// The two subscriptions, merged into one stream so their ordering is
/// preserved as delivered.
enum Merged<T> {
    Event(T),
    Gap(T),
}

/// One thing that came off the wire.
pub enum Incoming {
    /// A `FleetEvent` from the default (unnamed) SSE event.
    Event(Box<FleetEvent>),
    /// A `gap` event: the bus dropped messages (#28).
    Gap(GapSignal),
    /// A frame arrived that could not be parsed.
    ///
    /// Surfaced rather than dropped: silently ignoring these is how a client
    /// ends up showing an empty pane stuck on "connecting" with no explanation,
    /// which is exactly the failure this cost during development.
    Unparseable(String),
    /// The connection ended — cleanly or otherwise. The state machine decides
    /// which, since a finished run's stream closes deliberately.
    Ended,
}

/// Open the stream for `agent_id`, resuming at `from`, and feed each message to
/// `on_message` until the connection ends.
///
/// The URL carries `from` so a reconnect resumes rather than replaying the
/// whole run — the server's contract is replay-then-tail from a sequence floor.
pub async fn follow(agent_id: &str, from: u64, mut on_message: impl FnMut(Incoming)) {
    let url = format!(
        "/api/agents/{}/stream?from={from}",
        crate::api::encode_segment(agent_id)
    );

    let mut source = match EventSource::new(&url) {
        Ok(s) => s,
        Err(_) => {
            on_message(Incoming::Ended);
            return;
        }
    };

    // Two named subscriptions: the default channel carries FleetEvents, and
    // `gap` carries the lag signal. Subscribing to both before the first poll
    // so no frame is missed between them.
    let (Ok(events), Ok(gaps)) = (source.subscribe("message"), source.subscribe("gap")) else {
        on_message(Incoming::Ended);
        return;
    };

    // Merge the two subscriptions into one ordered stream rather than selecting
    // over them: `select!` needs a fused stream, and interleaving is exactly
    // what we want anyway — a gap notice belongs in the timeline at the point
    // it arrived, not batched separately.
    let mut merged = futures::stream::select(events.map(Merged::Event), gaps.map(Merged::Gap));

    while let Some(item) = merged.next().await {
        match item {
            Merged::Event(Ok((_, ev))) => match ev.data().as_string() {
                Some(text) => match serde_json::from_str::<FleetEvent>(&text) {
                    Ok(parsed) => on_message(Incoming::Event(Box::new(parsed))),
                    Err(e) => on_message(Incoming::Unparseable(format!("event: {e}"))),
                },
                None => on_message(Incoming::Unparseable("event had no text data".into())),
            },
            Merged::Gap(Ok((_, ev))) => match ev.data().as_string() {
                Some(text) => match serde_json::from_str::<GapSignal>(&text) {
                    Ok(gap) => on_message(Incoming::Gap(gap)),
                    Err(e) => on_message(Incoming::Unparseable(format!("gap: {e}"))),
                },
                None => on_message(Incoming::Unparseable("gap had no text data".into())),
            },
            // A closed or errored source lands here. The server closes a
            // finished run on purpose, so this is not inherently a failure —
            // `StreamSession` is what distinguishes them.
            _ => break,
        }
    }

    // Dropping the EventSource closes the underlying connection; being explicit
    // so a reconnect never leaves the previous one open (which is how a
    // reconnect loop turns into N concurrent streams).
    source.close();
    on_message(Incoming::Ended);
}

/// Apply one incoming message to a session.
pub fn apply(session: &mut StreamSession, incoming: Incoming) {
    match incoming {
        Incoming::Event(ev) => {
            session.push(*ev);
        }
        Incoming::Gap(gap) => session.push_gap(gap),
        Incoming::Unparseable(why) => session.push_undecodable(why),
        Incoming::Ended => session.disconnected(),
    }
}
