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
//! from this bus's pool for the subscription's lifetime, so live subscribers and
//! pool connections grow 1:1. The daemon gives the bus its own pool (the
//! clustered seams use a pool each today; a single shared pool is a future
//! tuning option), sized by [`DistributedBus::connect`] for the expected
//! concurrent SSE fan-out. If that ceiling is ever hit, multiplex one listener
//! across streams — deferred until measured.

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
    /// Build a bus on its own pool. Each live subscription pins one connection
    /// for its `LISTEN`, so the pool is sized above sqlx's default of 10 to
    /// allow a useful SSE fan-out before `subscribe`/`publish` start queuing on
    /// the pool; raise it further (or share a pool) for high-fan-out deployments.
    pub async fn connect(url: &str, store: Arc<dyn Store>) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(32)
            .connect(url)
            .await
            .map_err(|e| crate::error::CoreError::Store(format!("connecting to postgres: {e}")))?;
        Ok(Self { pool, store })
    }

    /// Build a bus on an existing pool (shared-pool wiring — size the pool for
    /// the SSE fan-out, since each subscription pins a `LISTEN` connection).
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

            // Seed at 0, so the FIRST doorbell replays the whole durable stream
            // and the consumer's `seq`-dedup drops the history overlap. Seeding
            // from a late `high_water()` read instead would be a gap: this
            // subscription's LISTEN + seed run lazily on first poll — AFTER the
            // SSE handler has already read history — so an event appended in that
            // window would be below a late high-water and never replayed by any
            // later doorbell, yet also absent from the history snapshot. Seeding
            // at 0 makes delivery independent of the subscribe-vs-history race
            // (the cost is one deduped re-read of the stream on the first
            // doorbell; subsequent doorbells replay only the delta as `last_seq`
            // advances). A floor passed in by the consumer could bound this, but
            // that is a future optimization, not a correctness need.
            let mut last_seq = 0u64;

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

    fn subscribe_all(&self) -> BusSubscription {
        let pool = self.pool.clone();
        let store = self.store.clone();
        Box::pin(async_stream::stream! {
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

            // Unlike `subscribe` (one stream, one `last_seq`), an unfiltered
            // doorbell can arrive for any stream key, so the high-water mark is
            // tracked per key, seeded at 0 the same way and for the same reason
            // (see `subscribe`'s comment): correctness over a late-seed race,
            // at the cost of one deduped re-read per stream on its first
            // doorbell.
            let mut last_seq: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

            loop {
                let notif = match listener.recv().await {
                    Ok(n) => n,
                    Err(e) => {
                        tracing::warn!(target: "prospero_bus", error = %e, "LISTEN recv failed");
                        break;
                    }
                };
                let Some((nkey, _seq)) = notif.payload().rsplit_once(':') else {
                    continue;
                };
                let from = last_seq.get(nkey).copied().unwrap_or(0) + 1;
                match store.replay(nkey, from).await {
                    Ok(events) => {
                        for ev in events {
                            let cur = last_seq.entry(nkey.to_string()).or_insert(0);
                            if ev.seq <= *cur {
                                continue;
                            }
                            *cur = ev.seq;
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

    /// Serialize the Postgres-gated bus tests against each other. They share one
    /// database and — critically — one NOTIFY channel: `subscribe_all` replays
    /// from the store for *every* notification on that channel, so a sibling
    /// test publishing concurrently floods this channel and, under the full
    /// suite's CPU pressure, can starve `subscribe_all`'s doorbell loop until it
    /// times out. Distinct `unique_agent` keys keep their *data* from colliding;
    /// this guard keeps their *doorbell traffic* from colliding. Held across
    /// awaits, so it must be a `tokio` mutex. Each test takes it right after the
    /// `DATABASE_URL` guard (an unset-DB skip never contends).
    static BUS_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn doorbell_delivers_a_live_event_to_a_subscriber() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP doorbell_delivers_a_live_event_to_a_subscriber: DATABASE_URL unset");
            return;
        };
        let _serial = BUS_TEST_SERIAL.lock().await;

        let store = PostgresStore::connect(&url).await.unwrap();
        let store: Arc<dyn Store> = Arc::new(store);
        let bus = DistributedBus::connect(&url, store.clone()).await.unwrap();

        let agent = unique_agent("agent-deliver");
        let mut sub = bus.subscribe(&agent);

        // The subscriber establishes its LISTEN connection lazily on first poll;
        // that moment isn't observable, and a single fixed sleep races it under a
        // slow/instrumented build (e.g. coverage). So drive delivery with a
        // bounded retry — append an event and ring the doorbell until the
        // (now-live) listener replays it.
        let recv =
            tokio::spawn(
                async move { tokio::time::timeout(Duration::from_secs(30), sub.next()).await },
            );

        // Keep nudging until the subscriber actually receives an event, NOT for a
        // fixed number of tries: under the full suite's CPU pressure the lazy
        // LISTEN can take many seconds to come up, and if the nudges stop before
        // then, nothing is ever replayed. The cap (~25s of nudging) sits under
        // the 30s recv timeout so a genuine hang still fails rather than hangs.
        let mut delivered = None;
        for seq in 1..=250u64 {
            let e = ev(seq, &agent);
            store.append(&e).await.unwrap();
            bus.publish(e);
            tokio::time::sleep(Duration::from_millis(100)).await;
            if recv.is_finished() {
                delivered = Some(recv.await.unwrap().expect("doorbell timed out"));
                break;
            }
        }

        match delivered.expect("subscriber never received a doorbell event") {
            Some(BusEvent::Event(ev)) => assert_eq!(ev.agent_id, agent),
            other => panic!("expected a live event, got {other:?}"),
        }
    }

    /// Regression for the cross-replica subscribe-window gap: the subscriber's
    /// LISTEN + seed run lazily on first poll, so an event already durable
    /// BEFORE that first doorbell (e.g. appended on the owner replica between a
    /// reader replica's history read and its first poll) must still be
    /// delivered. Seeding `last_seq` from 0 replays it; a late `high_water` seed
    /// would skip it forever.
    #[tokio::test]
    async fn delivers_an_event_that_predates_the_first_doorbell() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!(
                "SKIP delivers_an_event_that_predates_the_first_doorbell: DATABASE_URL unset"
            );
            return;
        };
        let _serial = BUS_TEST_SERIAL.lock().await;

        let store = PostgresStore::connect(&url).await.unwrap();
        let store: Arc<dyn Store> = Arc::new(store);
        let bus = DistributedBus::connect(&url, store.clone()).await.unwrap();

        let agent = unique_agent("agent-predate");
        // Append the "window" event BEFORE the subscriber's first poll.
        let early = ev(1, &agent);
        store.append(&early).await.unwrap();

        let mut sub = bus.subscribe(&agent);
        let recv =
            tokio::spawn(
                async move { tokio::time::timeout(Duration::from_secs(30), sub.next()).await },
            );

        // Ring the doorbell until the (now-live) listener replays the delta;
        // re-NOTIFY is idempotent (replay starts from last_seq+1 = 1). Keep
        // nudging until it's delivered, not for a fixed window: under the full
        // suite's CPU pressure the lazy LISTEN can come up well after a short
        // fixed window would have stopped nudging, leaving nothing to replay it.
        let mut delivered = None;
        for _ in 0..250 {
            bus.publish(early.clone());
            tokio::time::sleep(Duration::from_millis(100)).await;
            if recv.is_finished() {
                delivered = Some(recv.await.unwrap().expect("doorbell timed out"));
                break;
            }
        }

        match delivered.expect("never delivered the pre-doorbell event") {
            Some(BusEvent::Event(ev)) => {
                assert_eq!(ev.agent_id, agent);
                assert_eq!(
                    ev.seq, 1,
                    "the event appended before the first doorbell must arrive"
                );
            }
            other => panic!("expected the early event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn doorbell_ignores_other_streams() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP doorbell_ignores_other_streams: DATABASE_URL unset");
            return;
        };
        let _serial = BUS_TEST_SERIAL.lock().await;

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

    /// `subscribe_all` (unlike `subscribe`) has no stream-key filter, and
    /// tracks a `last_seq` per discovered key rather than one fixed key. This
    /// is the opposite assertion from `doorbell_ignores_other_streams`: two
    /// DIFFERENT streams must both reach one unfiltered subscription, and
    /// ringing either doorbell repeatedly must not re-deliver an already-seen
    /// event on either key (per-key high-water advances independently).
    #[tokio::test]
    async fn subscribe_all_delivers_events_from_multiple_streams() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!(
                "SKIP subscribe_all_delivers_events_from_multiple_streams: DATABASE_URL unset"
            );
            return;
        };
        let _serial = BUS_TEST_SERIAL.lock().await;

        let store = PostgresStore::connect(&url).await.unwrap();
        let store: Arc<dyn Store> = Arc::new(store);
        let bus = DistributedBus::connect(&url, store.clone()).await.unwrap();

        let agent_a = unique_agent("agent-all-a");
        let agent_b = unique_agent("agent-all-b");

        // `subscribe_all` is global and unfiltered by design, so under a shared
        // test database it also observes events from *sibling* tests running
        // concurrently (their own `unique_agent(...)` streams). This test is
        // only about OUR two streams: consume until both have arrived, skipping
        // any foreign stream key (and lag signals), while asserting neither of
        // ours is ever delivered twice. Taking "the first two events" verbatim
        // would flake whenever a concurrent test's event interleaves first.
        let mut sub = bus.subscribe_all();
        let a_recv = agent_a.clone();
        let b_recv = agent_b.clone();
        let recv = tokio::spawn(async move {
            let mut seen = std::collections::HashSet::new();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
            while seen.len() < 2 {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                match tokio::time::timeout(remaining, sub.next()).await {
                    Ok(Some(BusEvent::Event(ev))) => {
                        if ev.agent_id == a_recv || ev.agent_id == b_recv {
                            assert!(
                                seen.insert(ev.agent_id.clone()),
                                "duplicate delivery for stream {} (per-key high-water not advancing)",
                                ev.agent_id
                            );
                        }
                        // Foreign stream keys (concurrent tests) are expected — skip them.
                    }
                    // Lag signals aren't a delivery of one of our streams — keep waiting.
                    Ok(Some(BusEvent::Lagged(_))) => {}
                    Ok(None) => panic!("subscription closed before both streams arrived"),
                    Err(_) => panic!("doorbell timed out; saw {seen:?} of our two streams"),
                }
            }
            seen
        });

        // Same bounded-retry doorbell-ring pattern as
        // `doorbell_delivers_a_live_event_to_a_subscriber`: the LISTEN
        // connection is established lazily on first poll, so ring both
        // doorbells repeatedly (idempotent — replay starts from last_seq+1 per
        // key). Crucially, keep ringing until the subscriber has actually
        // consumed both events (`recv.is_finished()`), NOT for a fixed window:
        // under a saturated runtime (the full parallel test suite) the
        // subscribe_all task's lazy LISTEN can take several seconds to come up,
        // and if the nudges stop before then, no later doorbell ever replays our
        // rows and the subscriber times out having seen nothing.
        let event_a = ev(1, &agent_a);
        let event_b = ev(1, &agent_b);
        store.append(&event_a).await.unwrap();
        store.append(&event_b).await.unwrap();
        for _ in 0..440 {
            if recv.is_finished() {
                break;
            }
            bus.publish(event_a.clone());
            bus.publish(event_b.clone());
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let seen = recv.await.unwrap();
        assert_eq!(
            seen.len(),
            2,
            "subscribe_all must deliver events from both streams, unfiltered"
        );
    }
}
