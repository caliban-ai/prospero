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

    /// [`EventBus::subscribe_all`], but the `LISTEN` is established **before**
    /// this returns rather than lazily on the stream's first poll (#132).
    ///
    /// The lazy form cannot tell a caller when it is safe to publish. A
    /// doorbell rung in the gap is simply not heard, and because the per-key
    /// high-water starts at 0, the miss is only repaired by the *next* doorbell
    /// for that key — which, for a stream that just went quiet, may never come.
    /// Ringing repeatedly and hoping is what the #132 test was doing, and it
    /// still starved to zero deliveries on a contended runner.
    ///
    /// Connection and `LISTEN` failures surface here as an `Err` instead of
    /// silently ending the stream, which is the other thing the lazy form
    /// cannot express.
    pub async fn subscribe_all_ready(&self) -> Result<BusSubscription> {
        let mut listener = PgListener::connect_with(&self.pool)
            .await
            .map_err(|e| crate::error::CoreError::Store(format!("PgListener connect: {e}")))?;
        listener
            .listen(CHANNEL)
            .await
            .map_err(|e| crate::error::CoreError::Store(format!("LISTEN {CHANNEL}: {e}")))?;
        Ok(Box::pin(Self::drain_doorbell(listener, self.store.clone())))
    }

    /// The unfiltered doorbell loop, over an already-established `LISTEN`.
    ///
    /// Shared by [`EventBus::subscribe_all`] and [`Self::subscribe_all_ready`]
    /// so the two differ only in *when* they start listening, never in what
    /// they deliver.
    ///
    /// Unlike `subscribe` (one stream, one `last_seq`), an unfiltered doorbell
    /// can arrive for any stream key, so the high-water mark is tracked per key,
    /// seeded at 0 the same way and for the same reason (see `subscribe`'s
    /// comment): correctness over a late-seed race, at the cost of one deduped
    /// re-read per stream on its first doorbell.
    fn drain_doorbell(
        mut listener: PgListener,
        store: Arc<dyn Store>,
    ) -> impl futures::Stream<Item = BusEvent> + Send {
        async_stream::stream! {
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
        }
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
            // Same machinery as `subscribe_all_ready` from here on — only the
            // moment `LISTEN` is established differs (lazily, on this first
            // poll, versus before the call returns).
            let inner = DistributedBus::drain_doorbell(listener, store);
            futures::pin_mut!(inner);
            while let Some(item) = futures::StreamExt::next(&mut inner).await {
                yield item;
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
            actor: None,
            on_behalf_of: None,
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

    // Multi-threaded runtime: these tests spawn a consumer task and the bus
    // spawns a pg_notify task per publish, all of which must make progress
    // concurrently with the publish loop. On the default current-thread runtime
    // they contend cooperatively on one thread and — under the slow, instrumented
    // coverage build especially — can starve the listener so no doorbell is ever
    // processed. Real threads keep the listener draining while we publish.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
    // Multi-threaded runtime: these tests spawn a consumer task and the bus
    // spawns a pg_notify task per publish, all of which must make progress
    // concurrently with the publish loop. On the default current-thread runtime
    // they contend cooperatively on one thread and — under the slow, instrumented
    // coverage build especially — can starve the listener so no doorbell is ever
    // processed. Real threads keep the listener draining while we publish.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

    // Multi-threaded runtime: these tests spawn a consumer task and the bus
    // spawns a pg_notify task per publish, all of which must make progress
    // concurrently with the publish loop. On the default current-thread runtime
    // they contend cooperatively on one thread and — under the slow, instrumented
    // coverage build especially — can starve the listener so no doorbell is ever
    // processed. Real threads keep the listener draining while we publish.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

    /// Ring the doorbell for `agent` **synchronously**, so the test controls
    /// exactly when the NOTIFY lands relative to a subscription's `LISTEN`.
    /// `DistributedBus::publish` spawns its notify, which is fine in production
    /// but makes ordering unobservable in a test.
    async fn ring(bus: &DistributedBus, agent: &str, seq: u64) {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(CHANNEL)
            .bind(format!("{agent}:{seq}"))
            .execute(&bus.pool)
            .await
            .expect("pg_notify");
    }

    /// `subscribe_all` (unlike `subscribe`) has no stream-key filter, and
    /// tracks a `last_seq` per discovered key rather than one fixed key. This
    /// is the opposite assertion from `doorbell_ignores_other_streams`: two
    /// DIFFERENT streams must both reach one unfiltered subscription, and
    /// ringing either doorbell repeatedly must not re-deliver an already-seen
    /// event on either key (per-key high-water advances independently).
    ///
    /// #132: this used to be `#[ignore]`d. `subscribe_all` returns a lazy
    /// stream whose `LISTEN` is only established on the first poll, so a test
    /// had no way to know when it was safe to ring. The old version worked
    /// around that by re-ringing both doorbells up to 440 times over 44s and
    /// waiting up to 45s — which still starved to zero deliveries on an
    /// oversubscribed CI runner, so it was pulled from the gate entirely.
    ///
    /// `subscribe_all_ready` removes the guess: `LISTEN` is established before
    /// it returns, so one ring per stream is enough and the assertion is about
    /// delivery rather than about how long we were willing to wait.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

        store.append(&ev(1, &agent_a)).await.unwrap();
        store.append(&ev(1, &agent_b)).await.unwrap();

        // Established `LISTEN` before the first ring: both doorbells below are
        // rung at a listener that is already on the channel.
        let mut sub = bus.subscribe_all_ready().await.unwrap();
        ring(&bus, &agent_a, 1).await;
        ring(&bus, &agent_b, 1).await;

        // `subscribe_all` is global and unfiltered by design, so under a shared
        // test database it also observes events from *sibling* tests running
        // concurrently (their own `unique_agent(...)` streams). This test is
        // only about OUR two streams: consume until both have arrived, skipping
        // any foreign stream key (and lag signals), while asserting neither of
        // ours is ever delivered twice.
        let mut seen = std::collections::HashSet::new();
        while seen.len() < 2 {
            match tokio::time::timeout(Duration::from_secs(30), sub.next()).await {
                Ok(Some(BusEvent::Event(delivered))) => {
                    if delivered.agent_id == agent_a || delivered.agent_id == agent_b {
                        assert!(
                            seen.insert(delivered.agent_id.clone()),
                            "duplicate delivery for stream {} (per-key high-water not advancing)",
                            delivered.agent_id
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

        assert_eq!(
            seen.len(),
            2,
            "subscribe_all must deliver events from both streams, unfiltered"
        );
    }

    /// #132: the guarantee the test above rests on, asserted on its own.
    ///
    /// Exactly **one** doorbell is rung, after `subscribe_all_ready` returns
    /// and never again. That is what the lazy `subscribe_all` cannot survive:
    /// its `LISTEN` is established on the first poll, which here happens after
    /// the ring, so the notification is already gone and — with no later
    /// doorbell for the key to trigger the catch-up replay — the event is never
    /// delivered. This is the exact hazard the old test's 440-ring loop existed
    /// to paper over.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn subscribe_all_ready_is_listening_before_it_returns() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!(
                "SKIP subscribe_all_ready_is_listening_before_it_returns: DATABASE_URL unset"
            );
            return;
        };
        let _serial = BUS_TEST_SERIAL.lock().await;

        let store = PostgresStore::connect(&url).await.unwrap();
        let store: Arc<dyn Store> = Arc::new(store);
        let bus = DistributedBus::connect(&url, store.clone()).await.unwrap();

        let agent = unique_agent("agent-ready");
        store.append(&ev(1, &agent)).await.unwrap();

        let mut sub = bus.subscribe_all_ready().await.unwrap();
        ring(&bus, &agent, 1).await;

        loop {
            match tokio::time::timeout(Duration::from_secs(30), sub.next()).await {
                Ok(Some(BusEvent::Event(delivered))) if delivered.agent_id == agent => break,
                Ok(Some(_)) => {}
                Ok(None) => panic!("subscription closed before our event arrived"),
                Err(_) => panic!(
                    "a single doorbell rung after subscribe_all_ready was not delivered — \
                     the subscription was not listening when it returned"
                ),
            }
        }
    }
}
