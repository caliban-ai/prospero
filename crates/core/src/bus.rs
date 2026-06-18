//! Live event distribution behind a trait.
//!
//! Standalone uses [`InProcessBus`] (a tokio broadcast channel). The clustered
//! `DistributedBus` (Postgres `LISTEN/NOTIFY`) drops in behind the same trait in
//! a later phase — see the topology design spec §3.2.

use tokio::sync::broadcast;

use crate::event::FleetEvent;

/// Publishes events to live subscribers.
pub trait EventBus: Send + Sync {
    /// Fan an event out to current subscribers. Never blocks on slow/absent
    /// receivers (delivery is best-effort relative to the durable store).
    fn publish(&self, event: FleetEvent);

    /// A receiver for the live event tail (all streams; consumers filter).
    fn subscribe(&self) -> broadcast::Receiver<FleetEvent>;
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

    fn subscribe(&self) -> broadcast::Receiver<FleetEvent> {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventKind;

    fn ev(seq: u64) -> FleetEvent {
        FleetEvent {
            seq,
            ts: "t".into(),
            repo: "r".into(),
            agent_id: "a".into(),
            kind: EventKind::AgentSpawned,
        }
    }

    #[test]
    fn publish_reaches_a_live_subscriber() {
        let bus = InProcessBus::new(8);
        let mut rx = bus.subscribe();
        bus.publish(ev(1));
        assert_eq!(rx.try_recv().unwrap().seq, 1);
    }

    #[test]
    fn publish_with_no_subscriber_is_a_noop() {
        let bus = InProcessBus::new(8);
        bus.publish(ev(1)); // must not panic
    }
}
