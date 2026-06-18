//! Clustered live distribution via Postgres `LISTEN/NOTIFY` — the doorbell.
//!
//! The owner replica appends an event to Postgres (durable), then
//! `NOTIFY prospero_events '<stream_key>:<seq>'`. The payload is a *pointer*,
//! not the event, so it sidesteps NOTIFY's ~8 KB cap and keeps Postgres the
//! single source of truth. A subscriber on any replica holds one `LISTEN`
//! connection; on each doorbell for its stream it `replay`s the delta from the
//! durable store. See the topology design spec §3.2; clustered mode is
//! durable-first (§4) — the live tail carries only what is durable.
//!
//! **Scaling note:** each `subscribe` holds one dedicated `LISTEN` connection
//! from the pool for the subscription's lifetime, so live subscribers and pool
//! connections grow 1:1. When 2d shares a single pool across the store, bus,
//! ownership, and config-store, size that pool for the expected concurrent SSE
//! fan-out (or, if that ceiling is ever hit, multiplex one listener across
//! streams — deferred until measured).

use std::sync::Arc;

use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};

use crate::Result;
use crate::bus::{BusEvent, BusSubscription, EventBus};
use crate::event::FleetEvent;
use crate::store::Store;

/// Postgres `NOTIFY` channel for the event doorbell.
const CHANNEL: &str = "prospero_events";

/// Clustered `EventBus`: a doorbell over the durable store (spec §3.2).
pub struct DistributedBus {
    pool: PgPool,
    store: Arc<dyn Store>,
}

impl DistributedBus {
    /// Build a bus on its own pool (standalone-of-the-bus wiring / tests). In
    /// `clustered` deployment 2d will instead share one pool across the store,
    /// bus, ownership, and config-store via [`DistributedBus::new`].
    pub async fn connect(url: &str, store: Arc<dyn Store>) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .connect(url)
            .await
            .map_err(|e| crate::error::CoreError::Store(format!("connecting to postgres: {e}")))?;
        Ok(Self { pool, store })
    }

    /// Build a bus on an existing pool (shared-pool clustered wiring).
    pub fn new(pool: PgPool, store: Arc<dyn Store>) -> Self {
        Self { pool, store }
    }
}

impl EventBus for DistributedBus {
    fn publish(&self, event: FleetEvent) {
        // Doorbell only: the event is already (best-effort) durable in Postgres.
        // Payload is a pointer "<stream_key>:<seq>", never the event itself.
        // Fire-and-forget (best-effort vs. the durable store, ADR-0004); a lost
        // NOTIFY is recovered by the next doorbell's delta replay or the
        // poll-fallback escape hatch (spec §3.2).
        let pool = self.pool.clone();
        let payload = format!("{}:{}", event.stream_key(), event.seq);
        tokio::spawn(async move {
            if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
                .bind(CHANNEL)
                .bind(&payload)
                .execute(&pool)
                .await
            {
                tracing::warn!(target: "prospero_bus", error = %e, "pg_notify failed");
            }
        });
    }

    fn subscribe(&self, stream_key: &str) -> BusSubscription {
        let pool = self.pool.clone();
        let store = self.store.clone();
        let key = stream_key.to_string();
        Box::pin(async_stream::stream! {
            // One dedicated LISTEN connection per subscriber.
            let mut listener = match PgListener::connect_with(&pool).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::warn!(target: "prospero_bus", error = %e, "PgListener connect failed");
                    return;
                }
            };
            if let Err(e) = listener.listen(CHANNEL).await {
                tracing::warn!(target: "prospero_bus", error = %e, "LISTEN failed");
                return;
            }

            // Seed from the durable high-water: the bus is the LIVE tail (new
            // events only); the SSE history path backfills the rest, and the
            // consumer dedups the overlap on `seq`.
            let mut last_seq = store.high_water(&key).await.unwrap_or(0);

            loop {
                let notif = match listener.recv().await {
                    Ok(n) => n,
                    // `PgListener::recv` re-connects and re-LISTENs internally on a
                    // dropped connection, so an `Err` here is a terminal listener
                    // failure: end the subscription. The SSE client is responsible
                    // for re-subscribing (and replays history on reconnect), so no
                    // durable event is lost — consistent with the best-effort
                    // doorbell posture (§3.2).
                    Err(e) => {
                        tracing::warn!(target: "prospero_bus", error = %e, "LISTEN recv failed");
                        break;
                    }
                };
                // Payload is "<stream_key>:<seq>"; stream keys may contain ':'
                // (e.g. "repo:foo"), so split on the LAST colon.
                let Some((nkey, _seq)) = notif.payload().rsplit_once(':') else {
                    continue;
                };
                if nkey != key {
                    continue; // doorbell for another stream
                }
                // Doorbell rung: replay the durable delta and advance.
                match store.replay(&key, last_seq + 1).await {
                    Ok(events) => {
                        for ev in events {
                            if ev.seq <= last_seq {
                                continue;
                            }
                            last_seq = ev.seq;
                            yield BusEvent::Event(ev);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(target: "prospero_bus", error = %e, "doorbell replay failed");
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventKind;
    use crate::postgres_store::PostgresStore;
    use std::time::Duration;
    use tokio_stream::StreamExt;

    fn ev(seq: u64, agent: &str) -> FleetEvent {
        FleetEvent {
            seq,
            ts: "2026-06-18T00:00:00+00:00".into(),
            repo: "r".into(),
            agent_id: agent.into(),
            kind: EventKind::AgentSpawned,
        }
    }

    /// A process-unique agent id so the two gated tests — which share one
    /// persistent Postgres DB and run in parallel — never collide on a stream
    /// key (their NOTIFY payloads target distinct streams, and `high_water` /
    /// `replay` are stream-scoped). Avoids a global TRUNCATE that would wipe a
    /// sibling test's rows mid-run.
    fn unique_agent(tag: &str) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{tag}-{nanos}-{n}")
    }

    #[tokio::test]
    async fn doorbell_delivers_a_live_event_to_a_subscriber() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP doorbell_delivers_a_live_event_to_a_subscriber: DATABASE_URL unset");
            return;
        };

        let store = PostgresStore::connect(&url).await.unwrap();
        let store: Arc<dyn Store> = Arc::new(store);
        let bus = DistributedBus::connect(&url, store.clone()).await.unwrap();

        let agent = unique_agent("agent-deliver");
        let mut sub = bus.subscribe(&agent);

        // The subscriber establishes its LISTEN connection and seeds its
        // high-water lazily on first poll; that moment isn't observable, and a
        // single fixed sleep races it under a slow/instrumented build (e.g.
        // coverage). The bus only tails events appended AFTER it seeds, so we
        // drive delivery with a bounded retry — append a fresh event and ring
        // the doorbell until one lands after the listener is live. (Production
        // doesn't need this: the SSE handler reads history right after
        // subscribing, which backfills any event in the setup window.)
        let recv =
            tokio::spawn(
                async move { tokio::time::timeout(Duration::from_secs(20), sub.next()).await },
            );

        let mut delivered = None;
        for seq in 1..=40u64 {
            let e = ev(seq, &agent);
            store.append(&e).await.unwrap();
            bus.publish(e);
            tokio::time::sleep(Duration::from_millis(150)).await;
            if recv.is_finished() {
                delivered = Some(recv.await.unwrap().expect("doorbell timed out"));
                break;
            }
        }

        match delivered.expect("subscriber never received a doorbell event after 40 tries") {
            Some(BusEvent::Event(ev)) => assert_eq!(ev.agent_id, agent),
            other => panic!("expected a live event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn doorbell_ignores_other_streams() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP doorbell_ignores_other_streams: DATABASE_URL unset");
            return;
        };

        let store = PostgresStore::connect(&url).await.unwrap();
        let store: Arc<dyn Store> = Arc::new(store);
        let bus = DistributedBus::connect(&url, store.clone()).await.unwrap();

        let ours = unique_agent("agent-ours");
        let theirs = unique_agent("agent-theirs");
        let mut sub = bus.subscribe(&ours);
        let recv = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(800), sub.next()).await
        });
        tokio::time::sleep(Duration::from_millis(400)).await;

        // An event on a DIFFERENT stream: its doorbell must not wake our sub.
        let other = ev(1, &theirs);
        store.append(&other).await.unwrap();
        bus.publish(other);

        assert!(
            recv.await.unwrap().is_err(),
            "should have timed out (no event on our stream)"
        );
    }
}
