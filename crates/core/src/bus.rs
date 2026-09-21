//! Live event distribution behind a trait.
//!
//! Standalone uses [`InProcessBus`] (a tokio broadcast channel); the clustered
//! [`crate::DistributedBus`] (Postgres `LISTEN/NOTIFY`) drops in behind the same
//! trait — see the topology design spec §3.2. Both expose the live tail as a
//! per-stream [`BusSubscription`]; consumers dedup the history/live overlap on
//! `seq`.

use std::pin::Pin;

use tokio::sync::broadcast;
use tokio_stream::Stream;

use crate::event::{FleetEvent, stream_key_for};

/// One item from a per-stream live subscription (transport-agnostic).
#[derive(Debug, Clone, PartialEq)]
// `Event` is far larger than `Lagged`, which the lint flags because every
// message costs the larger size. Boxing the payload is the usual fix and is
// the wrong trade here: it would add an allocation to the path *every* agent's
// output flows through, to save a handful of bytes on `Lagged` — which only
// `InProcessBus` emits, and only when a subscriber falls behind.
#[allow(clippy::large_enum_variant)]
pub enum BusEvent {
    /// A live event on the subscribed stream.
    Event(FleetEvent),
    /// `skipped` events were dropped for a slow local subscriber; the consumer
    /// must self-heal by replaying from the durable store. Only [`InProcessBus`]
    /// emits this — the clustered bus reads the store on every doorbell, so it
    /// cannot lag.
    Lagged(u64),
}

/// A live, per-stream subscription: an ordered stream of [`BusEvent`]s for one
/// stream key. Ends (`None`) when the bus is gone.
pub type BusSubscription = Pin<Box<dyn Stream<Item = BusEvent> + Send>>;

/// Publishes events to live subscribers.
pub trait EventBus: Send + Sync {
    /// Fan an event out to current subscribers. Never blocks on slow/absent
    /// receivers (delivery is best-effort relative to the durable store).
    fn publish(&self, event: FleetEvent);

    /// A live subscription to one stream's events. The returned stream yields
    /// only events whose stream key equals `stream_key`; consumers dedup the
    /// initial-history/live overlap on `seq`.
    fn subscribe(&self, stream_key: &str) -> BusSubscription;

    /// A live subscription to EVERY stream's events, unfiltered. Needed by
    /// fleet-wide watchers (e.g. `FleetManager::watch_changes`) that can't name
    /// a stream key in advance — a brand-new agent's own id keys its
    /// `AgentDiscovered` event, so no one can pre-subscribe to it by key.
    fn subscribe_all(&self) -> BusSubscription;
}

/// In-process broadcast bus — the standalone implementation.
pub struct InProcessBus {
    tx: broadcast::Sender<FleetEvent>,
}

impl InProcessBus {
    /// A bus buffering up to `capacity` events for slow subscribers.
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self { tx }
    }
}

impl EventBus for InProcessBus {
    fn publish(&self, event: FleetEvent) {
        // No subscribers is fine; ignore the send error.
        let _ = self.tx.send(event);
    }

    fn subscribe(&self, stream_key: &str) -> BusSubscription {
        // Register the broadcast receiver EAGERLY (synchronously, here) so it
        // captures events from this point — before the caller reads initial
        // history — even though the stream body below is polled lazily.
        let mut rx = self.tx.subscribe();
        let key = stream_key.to_string();
        Box::pin(async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(ev) if stream_key_for(&ev.repo, &ev.agent_id) == key => {
                        yield BusEvent::Event(ev);
                    }
                    Ok(_) => continue, // an event on a different stream
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        yield BusEvent::Lagged(n);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }

    fn subscribe_all(&self) -> BusSubscription {
        // Same eager-registration discipline as `subscribe`, just without the
        // per-key filter.
        let mut rx = self.tx.subscribe();
        Box::pin(async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(ev) => yield BusEvent::Event(ev),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        yield BusEvent::Lagged(n);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventKind;
    use tokio_stream::StreamExt;

    fn ev_for(seq: u64, agent: &str) -> FleetEvent {
        FleetEvent {
            seq,
            ts: "t".into(),
            repo: "r".into(),
            agent_id: agent.into(),
            kind: EventKind::AgentSpawned,
            actor: None,
            on_behalf_of: None,
        }
    }

    #[tokio::test]
    async fn publish_reaches_a_subscriber_on_its_stream() {
        let bus = InProcessBus::new(8);
        let mut sub = bus.subscribe("a");
        bus.publish(ev_for(1, "a"));
        assert_eq!(sub.next().await, Some(BusEvent::Event(ev_for(1, "a"))));
    }

    #[tokio::test]
    async fn subscriber_only_sees_its_own_stream() {
        let bus = InProcessBus::new(8);
        let mut sub = bus.subscribe("a");
        bus.publish(ev_for(1, "b")); // other stream — filtered out
        bus.publish(ev_for(2, "a")); // our stream — delivered
        assert_eq!(sub.next().await, Some(BusEvent::Event(ev_for(2, "a"))));
    }

    #[tokio::test]
    async fn publish_with_no_subscriber_is_a_noop() {
        let bus = InProcessBus::new(8);
        bus.publish(ev_for(1, "a")); // must not panic
    }

    #[tokio::test]
    async fn subscribe_all_sees_every_stream_unfiltered() {
        let bus = InProcessBus::new(8);
        let mut sub = bus.subscribe_all();
        bus.publish(ev_for(1, "a"));
        bus.publish(ev_for(2, "b"));
        assert_eq!(sub.next().await, Some(BusEvent::Event(ev_for(1, "a"))));
        assert_eq!(sub.next().await, Some(BusEvent::Event(ev_for(2, "b"))));
    }

    // ---- #8: bounded-buffer behaviour under pressure -----------------------
    //
    // These are deliberately *not* timing tests. `broadcast` buffers on `send`,
    // so a receiver that is registered and then not polled falls behind by an
    // amount the test chooses exactly. That makes the drop count, the surviving
    // window, and the recovery all assertable without sleeping or racing —
    // which is the mistake that got the last soak-shaped test `#[ignore]`d
    // (#132).

    /// Capacity 4, 10 events published before a single poll: the subscriber
    /// must be told it lost the 6 oldest rather than silently skipping them,
    /// must then receive exactly the 4 the buffer still holds, and must keep
    /// working afterwards. That last part is the one that matters — a lag is
    /// only acceptable if the subscription survives it.
    #[tokio::test]
    async fn a_slow_subscriber_drops_a_bounded_prefix_then_keeps_working() {
        let bus = InProcessBus::new(4);
        let mut sub = bus.subscribe_all();

        for seq in 1..=10 {
            bus.publish(ev_for(seq, "a"));
        }

        assert_eq!(
            sub.next().await,
            Some(BusEvent::Lagged(6)),
            "10 published into a 4-deep buffer must report exactly the 6 dropped"
        );
        for seq in 7..=10 {
            assert_eq!(
                sub.next().await,
                Some(BusEvent::Event(ev_for(seq, "a"))),
                "the buffer's surviving window is the newest `capacity` events"
            );
        }

        // Liveness after the drop: the subscription self-heals rather than
        // wedging or ending.
        bus.publish(ev_for(11, "a"));
        assert_eq!(
            sub.next().await,
            Some(BusEvent::Event(ev_for(11, "a"))),
            "a subscription must survive its own lag"
        );
    }

    /// The other half of the contract: a consumer that keeps up is never
    /// penalised. Interleaving publish and receive keeps the receiver at most
    /// one event behind, so no `Lagged` may appear at any point.
    #[tokio::test]
    async fn a_subscriber_that_keeps_up_never_lags() {
        let bus = InProcessBus::new(4);
        let mut sub = bus.subscribe_all();

        for seq in 1..=64 {
            bus.publish(ev_for(seq, "a"));
            assert_eq!(
                sub.next().await,
                Some(BusEvent::Event(ev_for(seq, "a"))),
                "a consumer draining each event must never be told it lagged"
            );
        }
    }

    /// Fan-out under pressure: every subscriber has its own read position, so
    /// one slow consumer must not corrupt or starve the others. All 64 fall
    /// behind identically here, and each must independently report the same
    /// bounded loss and then recover — no shared cursor, no cross-talk.
    #[tokio::test]
    async fn many_concurrent_subscribers_each_lag_independently_and_recover() {
        const SUBSCRIBERS: usize = 64;
        let bus = InProcessBus::new(4);
        let mut subs: Vec<_> = (0..SUBSCRIBERS).map(|_| bus.subscribe_all()).collect();

        for seq in 1..=10 {
            bus.publish(ev_for(seq, "a"));
        }
        bus.publish(ev_for(11, "a"));

        for (i, sub) in subs.iter_mut().enumerate() {
            assert_eq!(
                sub.next().await,
                Some(BusEvent::Lagged(7)),
                "subscriber {i} must report its own loss, not a shared one"
            );
            for seq in 8..=11 {
                assert_eq!(
                    sub.next().await,
                    Some(BusEvent::Event(ev_for(seq, "a"))),
                    "subscriber {i} must still receive the surviving window"
                );
            }
        }
    }

    /// A subscriber that goes away mid-pressure must not affect the ones that
    /// remain — dropping a receiver is normal SSE client behaviour (a closed
    /// browser tab), not an error the bus should propagate.
    #[tokio::test]
    async fn dropping_one_subscriber_under_pressure_does_not_disturb_the_rest() {
        let bus = InProcessBus::new(4);
        let mut staying = bus.subscribe_all();
        let leaving = bus.subscribe_all();

        bus.publish(ev_for(1, "a"));
        drop(leaving);
        bus.publish(ev_for(2, "a"));

        assert_eq!(staying.next().await, Some(BusEvent::Event(ev_for(1, "a"))));
        assert_eq!(staying.next().await, Some(BusEvent::Event(ev_for(2, "a"))));
    }
}
