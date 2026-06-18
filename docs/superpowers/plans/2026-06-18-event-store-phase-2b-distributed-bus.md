# Event Store Phase 2b — `DistributedBus` (LISTEN/NOTIFY doorbell) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Work ONLY in the worktree at `.claude/worktrees/event-store-phase-2-clustered` on branch `worktree-event-store-phase-2-clustered`. Do NOT `git checkout` any other branch or commit (detached-HEAD hazard).

**Goal:** Generalize the `EventBus` seam to a per-stream subscription, and add the clustered `DistributedBus` that uses Postgres `LISTEN/NOTIFY` as a doorbell over the durable store as transport — so a replica can live-tail a stream another replica owns.

**Architecture:** `EventBus::subscribe(stream_key)` returns a transport-agnostic `Stream<BusEvent>` (`BusEvent::Event(FleetEvent)` | `BusEvent::Lagged(u64)`). `InProcessBus` (standalone) keeps today's `tokio::broadcast`, now filtered per stream and self-healing via `Lagged`. `DistributedBus` (clustered) holds an `Arc<dyn Store>`: `publish` fires `NOTIFY prospero_events '<stream_key>:<seq>'` (a *pointer*, not the event — sidesteps NOTIFY's ~8 KB cap); a subscriber holds one `LISTEN` connection and, on each doorbell for its stream, `replay`s the delta from the durable store. The SSE consumer's seq-dedup absorbs the history/live overlap; clustered mode is durable-first per spec §4 (the live tail reads only what is durable).

**Tech Stack:** Rust (edition 2024, tokio), `sqlx` (`PgListener`, `pg_notify`), `async-stream` + `tokio-stream` for the boxed `Stream`, `async-trait`.

**Spec:** `docs/superpowers/specs/2026-06-17-event-store-topology-design.md` §3.2 (`EventBus`) and §4 (durability posture).

---

## File Structure

- **Modify** `crates/core/Cargo.toml` — add `tokio-stream` + `async-stream` deps.
- **Modify** `crates/core/src/bus.rs` — new `BusEvent` enum, `BusSubscription` alias, per-stream `subscribe(stream_key)` trait method, reworked `InProcessBus`, updated unit tests.
- **Modify** `crates/core/src/fleet.rs` — `FleetManager::subscribe(stream_key)`; fix the two in-crate test subscribers (≈ lines 1392, 1437).
- **Modify** `crates/core/tests/fleet_integration.rs` — fix the six `manager.subscribe()` test subscribers.
- **Modify** `crates/api/src/sse.rs` + `crates/api/src/sse/tail.rs` — consume the per-stream `BusEvent` stream instead of a raw `broadcast::Receiver`.
- **Modify** `crates/core/src/lib.rs` — export `BusEvent`, `BusSubscription`; add `distributed_bus` module + `DistributedBus` re-export.
- **Create** `crates/core/src/distributed_bus.rs` — `DistributedBus` + DATABASE_URL-gated round-trip test.

---

## Task 1: Per-stream `EventBus` subscription (`BusEvent` stream)

This task changes the `EventBus` trait signature, so it MUST update every consumer in the same task to keep the whole workspace compiling green (the api crate's SSE path and all test subscribers). Production consumes the bus in exactly one place — the SSE `agent_stream` (per-agent); everything else is tests.

**Files:**
- Modify: `crates/core/Cargo.toml`
- Modify: `crates/core/src/bus.rs`
- Modify: `crates/core/src/fleet.rs:362-364` (the `subscribe` wrapper) and the two in-crate test subscribers near `crates/core/src/fleet.rs:1392` and `:1437`
- Modify: `crates/core/tests/fleet_integration.rs` (six subscribers: lines ~143, 195, 254, 282, 307, 388)
- Modify: `crates/api/src/sse.rs`, `crates/api/src/sse/tail.rs`
- Modify: `crates/core/src/lib.rs`

- [ ] **Step 1: Add the stream deps to core**

In `crates/core/Cargo.toml`, under `[dependencies]` (these already exist in `[workspace.dependencies]`, used by the api crate), add:

```toml
tokio-stream.workspace = true
async-stream = { workspace = true }
```

- [ ] **Step 2: Rewrite `crates/core/src/bus.rs`**

Replace the whole file with:

```rust
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
}
```

- [ ] **Step 3: Update `crates/core/src/lib.rs` exports**

Change the bus re-export line to include the new types:

```rust
pub use bus::{BusEvent, BusSubscription, EventBus, InProcessBus};
```

(Leave the `distributed_bus` module/export for Task 2.)

- [ ] **Step 4: Update `FleetManager::subscribe` in `crates/core/src/fleet.rs`**

Replace the method at `crates/core/src/fleet.rs:361-364`:

```rust
    /// Subscribe to one stream's live event tail (see [`crate::EventBus`]).
    /// Watchers of a single agent pass the agent id (its stream key); repo/fleet
    /// watchers pass `repo:<name>` / `fleet`.
    pub fn subscribe(&self, stream_key: &str) -> crate::bus::BusSubscription {
        self.inner.emitter.bus.subscribe(stream_key)
    }
```

- [ ] **Step 5: Fix the two in-crate test subscribers in `fleet.rs`**

Near `crates/core/src/fleet.rs:1392` and `:1437` the tests do `let mut rx = emitter.bus.subscribe();` then `rx.recv().await`. For each: change to a per-stream subscription for the agent that test exercises, and consume `BusEvent`. Read each test to find which agent/stream it watches and what it asserts, then apply this pattern:

```rust
// before:
let mut rx = emitter.bus.subscribe();
// ... later: let ev = rx.recv().await.unwrap();  (a FleetEvent)

// after:
use tokio_stream::StreamExt; // add at top of the test module if not present
let mut sub = emitter.bus.subscribe(&crate::event::stream_key_for(REPO, AGENT));
// ... later:
let ev = match sub.next().await {
    Some(crate::bus::BusEvent::Event(ev)) => ev,
    other => panic!("expected a live event, got {other:?}"),
};
```

Use the same `repo`/`agent_id` the test emits with for `stream_key_for`. If a test asserts on multiple events for one agent, loop `sub.next()` the same way. Subscribe BEFORE the emit that produces the awaited event (keep the existing ordering — `subscribe` already registers eagerly).

- [ ] **Step 6: Fix the six subscribers in `crates/core/tests/fleet_integration.rs`**

Same mechanical change at lines ~143, 195, 254, 282, 307, 388. Each `let mut rx = h.manager.subscribe();` becomes `let mut sub = h.manager.subscribe(<stream_key>);` and each `rx.recv().await` becomes a `sub.next().await` match on `Some(BusEvent::Event(ev))`. Add `use tokio_stream::StreamExt;` and `use prospero_core::BusEvent;` (or fully-qualify) at the top of the test file. Determine each `<stream_key>` from the agent/repo that test drives — most watch a single agent, so the key is that agent id. Preserve subscribe-before-emit ordering.

- [ ] **Step 7: Update the SSE tail state machine `crates/api/src/sse/tail.rs`**

The `Tailer` now consumes `Option<BusEvent>` (the stream item) instead of `Result<FleetEvent, RecvError>`. The bus already filters to one stream, so the per-event `agent_id` check is redundant but kept as a defensive guard; the `seq` dedup stays. Replace the `on_recv` signature and body, and drop the `broadcast` import:

```rust
use prospero_core::BusEvent;
// remove: use tokio::sync::broadcast::error::RecvError;

    pub(crate) async fn on_recv(&mut self, item: Option<BusEvent>) -> Step {
        match item {
            Some(BusEvent::Event(ev))
                if ev.agent_id == self.agent_id && ev.seq > self.last_delivered =>
            {
                self.last_delivered = ev.seq;
                let terminal = is_terminal(&ev);
                let frames = vec![Frame::Event(ev)];
                if terminal {
                    Step::EmitAndClose(frames)
                } else {
                    Step::Emit(frames)
                }
            }
            Some(BusEvent::Event(_)) => Step::Skip, // other stream, or already-delivered seq
            Some(BusEvent::Lagged(skipped)) => {
                let mut frames = vec![Frame::Gap {
                    skipped,
                    last_seq: self.last_delivered,
                }];
                let mut terminal = false;
                for ev in self
                    .history
                    .history(&self.agent_id, self.last_delivered + 1)
                    .await
                {
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
            None => Step::Close, // bus/stream ended
        }
    }
```

Update the tests in this file's `mod tests`: each `t.on_recv(Ok(ev(..)))` becomes `t.on_recv(Some(BusEvent::Event(ev(..))))`; `t.on_recv(Err(RecvError::Lagged(n)))` becomes `t.on_recv(Some(BusEvent::Lagged(n)))`; `t.on_recv(Err(RecvError::Closed))` becomes `t.on_recv(None)`. Add `use prospero_core::BusEvent;` to the test module. The `skips_other_agents_and_already_delivered` test still passes (the defensive `agent_id` guard is retained).

- [ ] **Step 8: Update the SSE handler `crates/api/src/sse.rs`**

Replace the subscribe + loop. Subscribe per-stream (the agent id is the stream key) BEFORE reading history, and drive the loop off `sub.next()`:

```rust
use tokio_stream::StreamExt; // add to imports

    // Subscribe BEFORE reading history so no live event is missed in the gap.
    let mut sub = st.manager.subscribe(&id);
    let history = st.manager.history(&id, q.from).await.unwrap_or_default();
```

and in the live-tail loop:

```rust
        let mut tailer = Tailer::new(id, last_delivered, st.manager.clone());
        loop {
            match tailer.on_recv(sub.next().await).await {
                Step::Emit(frames) => {
                    for f in frames { yield Ok(frame_to_event(&f)); }
                }
                Step::EmitAndClose(frames) => {
                    for f in frames { yield Ok(frame_to_event(&f)); }
                    break;
                }
                Step::Skip => continue,
                Step::Close => break,
            }
        }
```

(The doc comment at the top of the file mentions "broadcast bus"; update the wording to "live event bus" since the subscription is now transport-agnostic.)

- [ ] **Step 9: Build + test the whole workspace**

Run (the testkit feature is needed for the api/integration tests):

```bash
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```

Expected: PASS. No DATABASE_URL needed for Task 1 (Postgres-gated tests take the skip path). Then `cargo fmt --all` and `cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings` clean.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "feat(core): per-stream EventBus subscription (BusEvent stream)

Generalize EventBus::subscribe() to subscribe(stream_key) -> Stream<BusEvent>,
the transport-agnostic seam the clustered DistributedBus needs. InProcessBus
filters the broadcast per stream and surfaces slow-consumer lag as
BusEvent::Lagged; the SSE Tailer consumes the new stream and keeps its
seq-dedup + gap self-heal. Production consumes the bus only via the per-agent
SSE path; test subscribers updated to the per-stream API.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `DistributedBus` — Postgres LISTEN/NOTIFY doorbell

Additive: a second `EventBus` impl behind the trait from Task 1. The whole workspace stays green; the new round-trip test is DATABASE_URL-gated (skips when unset).

**Files:**
- Create: `crates/core/src/distributed_bus.rs`
- Modify: `crates/core/src/lib.rs`

- [ ] **Step 1: Write the failing round-trip test (create the file with the test, stub the type)**

Create `crates/core/src/distributed_bus.rs` with the module doc, an empty `DistributedBus` placeholder, and this test, so it compiles-but-fails / skips first:

```rust
//! Clustered live distribution via Postgres `LISTEN/NOTIFY` — the doorbell.
//!
//! The owner replica appends an event to Postgres (durable), then
//! `NOTIFY prospero_events '<stream_key>:<seq>'`. The payload is a *pointer*,
//! not the event, so it sidesteps NOTIFY's ~8 KB cap and keeps Postgres the
//! single source of truth. A subscriber on any replica holds one `LISTEN`
//! connection; on each doorbell for its stream it `replay`s the delta from the
//! durable store. See the topology design spec §3.2; clustered mode is
//! durable-first (§4) — the live tail carries only what is durable.

use std::pin::Pin;
use std::sync::Arc;

use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
use tokio_stream::Stream;

use crate::Result;
use crate::bus::{BusEvent, BusSubscription, EventBus};
use crate::event::{FleetEvent, stream_key_for};
use crate::store::Store;

/// Postgres `NOTIFY` channel for the event doorbell.
const CHANNEL: &str = "prospero_events";

/// Clustered `EventBus`: a doorbell over the durable store (spec §3.2).
pub struct DistributedBus {
    pool: PgPool,
    store: Arc<dyn Store>,
}

// (impl added in the steps below)

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

    #[tokio::test]
    async fn doorbell_delivers_a_live_event_to_a_subscriber() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP doorbell_delivers_a_live_event_to_a_subscriber: DATABASE_URL unset");
            return;
        };

        let store = PostgresStore::connect(&url).await.unwrap();
        store.reset_for_tests().await.unwrap();
        let store: Arc<dyn Store> = Arc::new(store);
        let bus = DistributedBus::connect(&url, store.clone()).await.unwrap();

        let mut sub = bus.subscribe("agent-x");

        // Poll the subscription in a task so its LISTEN connection is
        // established BEFORE we append+notify (otherwise the doorbell races
        // listener setup and the recv would block).
        let recv = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(5), sub.next()).await
        });
        tokio::time::sleep(Duration::from_millis(400)).await;

        let e = ev(1, "agent-x");
        store.append(&e).await.unwrap();
        bus.publish(e.clone());

        let got = recv.await.unwrap().expect("timed out waiting for doorbell");
        assert_eq!(got, Some(BusEvent::Event(e)));
    }

    #[tokio::test]
    async fn doorbell_ignores_other_streams() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP doorbell_ignores_other_streams: DATABASE_URL unset");
            return;
        };

        let store = PostgresStore::connect(&url).await.unwrap();
        store.reset_for_tests().await.unwrap();
        let store: Arc<dyn Store> = Arc::new(store);
        let bus = DistributedBus::connect(&url, store.clone()).await.unwrap();

        let mut sub = bus.subscribe("agent-x");
        let recv = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(800), sub.next()).await
        });
        tokio::time::sleep(Duration::from_millis(400)).await;

        // An event on a DIFFERENT stream: its doorbell must not wake our sub.
        let other = ev(1, "agent-y");
        store.append(&other).await.unwrap();
        bus.publish(other);

        assert!(recv.await.unwrap().is_err(), "should have timed out (no event on our stream)");
    }
}
```

- [ ] **Step 2: Run the test to confirm it fails to compile (no impl yet)**

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test \
  cargo test -p prospero-core --features prospero-core/testkit distributed_bus 2>&1 | tail -20
```

Expected: compile error (`DistributedBus::connect` / `subscribe` / `publish` not found).

- [ ] **Step 3: Implement `DistributedBus`**

Add, between the struct and the `#[cfg(test)]` module:

```rust
impl DistributedBus {
    /// Build a bus on its own pool (standalone-of-the-bus wiring / tests). In
    /// `clustered` deployment 2d will instead share one pool across the store,
    /// bus, ownership, and config-store via [`DistributedBus::new`].
    pub async fn connect(url: &str, store: Arc<dyn Store>) -> Result<Self> {
        let pool = PgPoolOptions::new().connect(url).await?;
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
        let payload = format!(
            "{}:{}",
            stream_key_for(&event.repo, &event.agent_id),
            event.seq
        );
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
```

- [ ] **Step 4: Wire the module into `crates/core/src/lib.rs`**

Add (alphabetically — `distributed_bus` sorts after `config_store`/`bus`, before `event`):

```rust
pub mod distributed_bus;
pub use distributed_bus::DistributedBus;
```

- [ ] **Step 5: Run the gated tests against PG18**

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test \
  cargo test -p prospero-core --features prospero-core/testkit distributed_bus 2>&1 | tail -20
```

Expected: `doorbell_delivers_a_live_event_to_a_subscriber ... ok` and `doorbell_ignores_other_streams ... ok`. Also confirm the skip path: re-run WITHOUT `DATABASE_URL` and see both print `SKIP ...` and pass.

- [ ] **Step 6: Full gate**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test \
  cargo test --workspace --features prospero-core/testkit
```

Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(core): DistributedBus — Postgres LISTEN/NOTIFY doorbell

The clustered EventBus. publish() fires NOTIFY prospero_events with a
'<stream_key>:<seq>' pointer (not the event — sidesteps NOTIFY's ~8KB cap);
a subscriber holds one LISTEN connection and replays the durable delta from
the store on each doorbell for its stream, seeded from high_water so it
carries only the live tail. Clustered mode is durable-first (spec §4): the
live path reads only what is durable. DATABASE_URL-gated round-trip tests
against Postgres 18.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

- **Spec §3.2 coverage:** `subscribe(stream_key) -> stream<FleetEvent>` ✓ (as `BusSubscription`/`BusEvent`); NOTIFY pointer payload ✓; replay-the-delta on doorbell ✓; both impls behind one trait ✓. Poll-fallback escape hatch (b) is documented as future, not built (YAGNI for first cut) — noted in `publish` doc comment.
- **Spec §4 durable-first:** the clustered live tail reads only durable events (replay from store); a failed append → the event never propagates cross-replica, surfaced by the best-effort `StorePersistFailed` marker the Emitter already appends. Documented in the module doc.
- **Compile coupling:** the trait signature change and all consumers (SSE + every test subscriber) are in Task 1, so the workspace is green at each task boundary (Phase 0's Task-2/3 lesson).
- **Eager-subscribe guarantee:** `InProcessBus::subscribe` registers the broadcast receiver synchronously before returning the lazy stream, preserving "subscribe before history". `DistributedBus` seeds from `high_water` and relies on seq-dedup + doorbell-delta for the overlap; the listener-setup race is covered by the test's spawn-then-sleep ordering and noted as the poll-fallback's job in steady state.
- **Stream keys with `:`:** the NOTIFY payload is parsed with `rsplit_once(':')`, so `repo:foo:<seq>` resolves to key `repo:foo`.
- **Type consistency:** `BusEvent`, `BusSubscription`, `EventBus::subscribe(&self, &str)`, `DistributedBus::{connect,new}`, `stream_key_for` used identically across both tasks.
