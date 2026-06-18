# Event-Store Phase 0 — Seam Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce the per-stream `seq` model and the `EventBus` and `Ownership` trait seams into `prospero-core`, with their degenerate single-host implementations, as a behavior-preserving refactor — so the later sqlite (Phase 1) and Postgres/clustered (Phase 2) work drops in without forking the daemon.

**Architecture:** One `FleetManager` runtime keeps its current behavior. Three changes land behind the existing public surface: (1) `seq` becomes monotonic *per stream key* (agent / `repo:<name>` / `fleet`) instead of one global counter; (2) the broadcast bus moves behind an `EventBus` trait with an `InProcessBus` impl; (3) a new `Ownership` trait gates which agents this process drives, with a `SelfOwnsAll` no-op impl. `FleetManager::new(config, store)` and `FleetManager::subscribe()` keep their exact signatures, so the `prospero-api` crate, `prosperod`, and all existing tests are untouched.

**Tech Stack:** Rust (edition 2024), tokio (`broadcast`, `sync`), `prospero-core` crate. Design source: `docs/superpowers/specs/2026-06-17-event-store-topology-design.md` §3.1–§3.3, §5 (Phase 0).

**Verification gate (run after every task's final step, mirrors `.github/workflows/ci.yml`):**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```

(If the `--features prospero-core/testkit` flag errors for a given crate, the CI uses a `TESTKIT` env expansion; `cargo test --workspace` alone still compiles the `#[cfg(test)]` paths. Prefer the gate above; fall back to the bare commands if the feature path is rejected.)

---

## File Structure

- **Modify** `crates/core/src/event.rs` — add `FleetEvent::stream_key()` + the free `stream_key_for(repo, agent_id)` helper (Task 1).
- **Modify** `crates/core/src/store.rs` — `Store::high_water` and `Store::replay` become stream-key-scoped; update `JsonlStore` + its tests; add a reusable `Store` conformance battery (Tasks 2, 6).
- **Create** `crates/core/src/bus.rs` — `EventBus` trait + `InProcessBus` (Task 4).
- **Create** `crates/core/src/ownership.rs` — `Ownership` trait + `Lease` + `SelfOwnsAll` (Task 5).
- **Modify** `crates/core/src/fleet.rs` — per-stream seq in `Emitter`; wire `EventBus` and `Ownership` into `Emitter`/`Inner`/`FleetManager`; update in-file test doubles (Tasks 3, 4, 5).
- **Modify** `crates/core/src/lib.rs` — declare + re-export the two new modules (Tasks 4, 5).
- **Modify** `crates/api/tests/api_integration.rs` — update the `UnwritableStore` test double to the new `Store` signature (Task 2).

No changes to `crates/api/src/**`, `crates/daemon/src/**`, or `crates/cli/**` source — only the `UnwritableStore` test double moves with the trait.

---

## Task 1: `stream_key` on `FleetEvent`

Every event belongs to exactly one ordered stream. This is the foundation for per-stream `seq`.

**Files:**
- Modify: `crates/core/src/event.rs`
- Test: `crates/core/src/event.rs` (in-file `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module at the bottom of `crates/core/src/event.rs`:

```rust
    #[test]
    fn stream_key_picks_agent_then_repo_then_fleet() {
        // Agent-level: the agent id is the stream.
        assert_eq!(stream_key_for("prospero", "a1"), "a1");
        // Repo-level (no agent): namespaced repo stream.
        assert_eq!(stream_key_for("prospero", ""), "repo:prospero");
        // Fleet-level (neither): the singleton fleet stream.
        assert_eq!(stream_key_for("", ""), "fleet");
    }

    #[test]
    fn fleet_event_stream_key_delegates() {
        let e = FleetEvent {
            seq: 1,
            ts: "t".into(),
            repo: "prospero".into(),
            agent_id: "".into(),
            kind: EventKind::AgentGone,
        };
        assert_eq!(e.stream_key(), "repo:prospero");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p prospero-core event::tests::stream_key_picks_agent_then_repo_then_fleet`
Expected: FAIL — `cannot find function stream_key_for in this scope`.

- [ ] **Step 3: Write minimal implementation**

In `crates/core/src/event.rs`, add the free function just above the `FleetEvent` struct definition (after the `EventKind` enum):

```rust
/// The ordered stream a `(repo, agent_id)` pair belongs to. Agent events key on
/// the agent id; repo-level events (no agent) on `repo:<name>`; fleet-level
/// events (neither) on the singleton `fleet` stream. `seq` is monotonic *within*
/// the returned key.
pub fn stream_key_for(repo: &str, agent_id: &str) -> String {
    if !agent_id.is_empty() {
        agent_id.to_string()
    } else if !repo.is_empty() {
        format!("repo:{repo}")
    } else {
        "fleet".to_string()
    }
}
```

Then add an inherent method on `FleetEvent` (a new `impl FleetEvent` block directly below the struct):

```rust
impl FleetEvent {
    /// The ordered stream this event belongs to (see [`stream_key_for`]).
    pub fn stream_key(&self) -> String {
        stream_key_for(&self.repo, &self.agent_id)
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p prospero-core event::tests`
Expected: PASS (both new tests + the existing `event_kind_is_internally_tagged`, `fleet_event_round_trips`).

- [ ] **Step 5: Commit**

```bash
git add crates/core/src/event.rs
git commit -m "feat(core): add stream_key to FleetEvent for per-stream sequencing"
```

---

## Task 2: `Store` becomes stream-key-scoped

`high_water` and `replay` move from "global / by agent_id" to "by stream key". For agent streams the key *is* the agent id, so `FleetManager::history(agent_id, ..)` keeps working unchanged.

**Files:**
- Modify: `crates/core/src/store.rs` (trait, `JsonlStore` impl, tests)
- Modify: `crates/api/tests/api_integration.rs:164` (the `UnwritableStore` test double)

- [ ] **Step 1: Write the failing test**

Replace the body of the existing `high_water_recovers_max_seq_across_reopen` test in `crates/core/src/store.rs` and add a new per-stream test. In the `tests` module, add:

```rust
    #[test]
    fn high_water_is_scoped_per_stream() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        // Two independent agent streams, each with its own seq line.
        store.append(&ev(1, "a", "one")).unwrap();
        store.append(&ev(1, "b", "one")).unwrap();
        store.append(&ev(2, "a", "two")).unwrap();

        assert_eq!(store.high_water("a").unwrap(), 2);
        assert_eq!(store.high_water("b").unwrap(), 1);
        assert_eq!(store.high_water("missing").unwrap(), 0);
    }
```

Note: `ev(seq, agent, chunk)` builds a `FleetEvent` with `agent_id = agent` and `repo = "r"`, so its stream key is the agent id.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p prospero-core store::tests::high_water_is_scoped_per_stream`
Expected: FAIL — `this method takes 0 arguments but 1 argument was supplied` (or a compile error on the trait).

- [ ] **Step 3: Change the trait and `JsonlStore` impl**

In `crates/core/src/store.rs`, change the trait method signatures:

```rust
pub trait Store: Send + Sync {
    /// Append one event to durable storage.
    fn append(&self, event: &FleetEvent) -> Result<()>;

    /// Replay events for one stream with `seq >= from_seq`, in `seq` order.
    fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>>;

    /// The highest `seq` ever persisted for `stream_key` (0 if none). Used to
    /// resume that stream's sequence counter across daemon restarts.
    fn high_water(&self, stream_key: &str) -> Result<u64>;

    /// Whether the backend can currently accept writes. A cheap, non-destructive
    /// probe used by the readiness endpoint.
    fn writable(&self) -> bool;
}
```

Update the `impl Store for JsonlStore` methods `replay` and `high_water`:

```rust
    fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
        let mut events: Vec<FleetEvent> = self
            .read_all()?
            .into_iter()
            .filter(|e| e.stream_key() == stream_key && e.seq >= from_seq)
            .collect();
        events.sort_by_key(|e| e.seq);
        Ok(events)
    }

    fn high_water(&self, stream_key: &str) -> Result<u64> {
        Ok(self
            .read_all()?
            .iter()
            .filter(|e| e.stream_key() == stream_key)
            .map(|e| e.seq)
            .max()
            .unwrap_or(0))
    }
```

- [ ] **Step 4: Fix the existing `JsonlStore` tests to pass stream keys**

In the same `tests` module, update the existing tests that call `replay`/`high_water` with no key:

- `append_and_replay_filters_by_agent_and_seq`: `store.replay("a", 0)` and `store.replay("a", 3)` already pass `"a"` as the first arg — these now read as stream keys and still pass. No change needed.
- `high_water_recovers_max_seq_across_reopen`: change `reopened.high_water().unwrap()` to `reopened.high_water("a").unwrap()` (the two events use agent `"a"`).
- `high_water_is_zero_when_empty`: change `store.high_water().unwrap()` to `store.high_water("a").unwrap()`.
- `corrupt_trailing_line_is_tolerated`: change `store.high_water().unwrap()` to `store.high_water("a").unwrap()`.

- [ ] **Step 5: Update the `UnwritableStore` test double in the api crate**

In `crates/api/tests/api_integration.rs`, find the `impl Store for UnwritableStore` block (near line 164) and update its `replay`/`high_water` signatures to match the trait. The double delegates to its inner `JsonlStore`:

```rust
    fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
        self.0.replay(stream_key, from_seq)
    }
    fn high_water(&self, stream_key: &str) -> Result<u64> {
        self.0.high_water(stream_key)
    }
```

(Keep its `append`/`writable` as they are — `writable` is what the test forces to `false`.)

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p prospero-core store::tests`
Expected: PASS (all store tests, including the new `high_water_is_scoped_per_stream`).

Run: `cargo build -p prospero-api --tests`
Expected: compiles (the `UnwritableStore` double matches the new trait).

- [ ] **Step 7: Commit**

```bash
git add crates/core/src/store.rs crates/api/tests/api_integration.rs
git commit -m "feat(core): scope Store::replay and high_water to stream keys"
```

---

## Task 3: Per-stream `seq` in the `Emitter`

Replace the single global `AtomicU64` counter with a per-stream-key counter map seeded lazily from the store's per-stream high-water mark.

**Files:**
- Modify: `crates/core/src/fleet.rs` (`Emitter` struct, `next_event`, `FleetManager::new`, in-file test doubles + `emitter_with`)
- Test: `crates/core/src/fleet.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/core/src/fleet.rs` (the `emitter_with` helper already exists there and will be updated in Step 3):

```rust
    #[test]
    fn seq_is_monotonic_per_stream_not_global() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let emitter = emitter_with(store);

        // Interleave two agents; each stream numbers from 1 independently.
        emitter.emit("r", "a1", EventKind::AgentSpawned);
        emitter.emit("r", "a2", EventKind::AgentSpawned);
        emitter.emit("r", "a1", EventKind::AgentGone);

        let a1 = emitter.store.replay("a1", 0).unwrap();
        let a2 = emitter.store.replay("a2", 0).unwrap();
        assert_eq!(a1.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(a2.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn seq_resumes_per_stream_from_high_water() {
        let dir = tempfile::tempdir().unwrap();
        // Pre-seed the store: stream "a1" already reached seq 5.
        {
            let store = crate::store::JsonlStore::open(dir.path()).unwrap();
            store.append(&ev(5, "a1", "old")).unwrap();
        }
        let store = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let emitter = emitter_with(store);
        emitter.emit("r", "a1", EventKind::AgentGone);

        let a1 = emitter.store.replay("a1", 0).unwrap();
        // The new event continues from the stored high-water (5 → 6).
        assert_eq!(a1.last().unwrap().seq, 6);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p prospero-core fleet::tests::seq_is_monotonic_per_stream_not_global`
Expected: FAIL — the current global counter numbers the three events 1,2,3, so `a1` would be `[1, 3]`, not `[1, 2]`.

- [ ] **Step 3: Change the `Emitter` to a per-stream counter**

In `crates/core/src/fleet.rs`, update the imports at the top: remove `use std::sync::atomic::{AtomicU64, Ordering};` and ensure `HashMap` is imported (it already is via `use std::collections::{HashMap, HashSet};`).

Replace the `Emitter` struct definition:

```rust
/// Stamps and dispatches events; cheaply cloneable into background tasks.
#[derive(Clone)]
struct Emitter {
    store: Arc<dyn Store>,
    bus: broadcast::Sender<FleetEvent>,
    /// Next `seq` per stream key, seeded lazily from the store's high-water.
    seqs: Arc<Mutex<HashMap<String, u64>>>,
    metrics: Arc<Metrics>,
}
```

Replace `Emitter::next_event`:

```rust
    fn next_event(&self, repo: &str, agent_id: &str, kind: EventKind) -> FleetEvent {
        let stream_key = crate::event::stream_key_for(repo, agent_id);
        let seq = {
            let mut seqs = self.seqs.lock().unwrap();
            let next = match seqs.get(&stream_key) {
                Some(n) => n + 1,
                // First event this run for the stream: resume from durable
                // high-water. A read failure here is rare (the store was just
                // opened); fall back to 0 and log, consistent with ADR-0004's
                // best-effort posture.
                None => {
                    self.store.high_water(&stream_key).unwrap_or_else(|e| {
                        tracing::warn!(
                            target: "prospero_fleet", stream = %stream_key, error = %e,
                            "high_water read failed seeding per-stream seq; starting at 0"
                        );
                        0
                    }) + 1
                }
            };
            seqs.insert(stream_key, next);
            next
        };
        FleetEvent {
            seq,
            ts: chrono::Utc::now().to_rfc3339(),
            repo: repo.to_string(),
            agent_id: agent_id.to_string(),
            kind,
        }
    }
```

- [ ] **Step 4: Update `FleetManager::new` to drop the global high-water seed**

In `FleetManager::new`, replace the bus/emitter construction:

```rust
        let registry = Registry::load(&config.registry_path())?;
        let (bus, _) = broadcast::channel(config.event_buffer);
        let emitter = Emitter {
            store,
            bus,
            seqs: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::default()),
        };
```

(Delete the `let high_water = store.high_water()?;` line — the seed is now lazy and per-stream. `store` is still moved into the `Emitter`.)

- [ ] **Step 5: Update the in-file test doubles**

In the `tests` module of `fleet.rs`:

1. `FlakyStore`'s `impl Store` — update `replay`/`high_water` signatures to match the trait:

```rust
        fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
            self.inner.replay(stream_key, from_seq)
        }
        fn high_water(&self, stream_key: &str) -> Result<u64> {
            self.inner.high_water(stream_key)
        }
```

2. The `emitter_with` helper — replace the `seq` field with `seqs`:

```rust
    fn emitter_with(store: Arc<dyn Store>) -> Emitter {
        let (bus, _keep) = broadcast::channel(16);
        Emitter {
            store,
            bus,
            seqs: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::default()),
        }
    }
```

3. The `FlakyStore`-based tests fail specific seqs. `append_failure_emits_persist_gap_marker_visible_to_history` and `append_failure_and_success_advance_metrics` construct `FlakyStore::new(inner, [1])` and emit a single agent event, whose stream-`seq` is still `1` (first event for that stream). These remain correct — no change. Verify by reading the assertions; they key on `lost_seq: 1` and `seq == 1`, which still hold.

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p prospero-core fleet::tests`
Expected: PASS — including the two new per-stream tests and the unchanged gap-marker/metrics tests.

- [ ] **Step 7: Commit**

```bash
git add crates/core/src/fleet.rs
git commit -m "feat(core): per-stream seq counters in the event Emitter"
```

---

## Task 4: `EventBus` trait + `InProcessBus`

Move the broadcast bus behind a trait. `FleetManager::subscribe()` keeps returning `broadcast::Receiver<FleetEvent>`, so SSE consumers are untouched.

**Files:**
- Create: `crates/core/src/bus.rs`
- Modify: `crates/core/src/lib.rs` (declare + re-export)
- Modify: `crates/core/src/fleet.rs` (`Emitter.bus` type, `emit`, `FleetManager::new`, `subscribe`, `emitter_with`)
- Test: `crates/core/src/bus.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test**

Create `crates/core/src/bus.rs` with the trait, a placeholder impl, and the test:

```rust
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
```

- [ ] **Step 2: Declare the module and run the test to verify it fails**

In `crates/core/src/lib.rs`, add `pub mod bus;` (alphabetically, after `pub mod caliband;`... actually after `pub mod attach`? follow existing order — place `pub mod bus;` between `pub mod caliband;` and the next; the modules are listed alphabetically so insert `pub mod bus;` before `pub mod caliband;`). Add the re-export near the others:

```rust
pub use bus::{EventBus, InProcessBus};
```

Run: `cargo test -p prospero-core bus::tests`
Expected: PASS for the two bus tests in isolation (this module is self-contained). If you prefer strict red-green, temporarily assert `rx.try_recv().unwrap().seq == 999` to see a FAIL, then restore to `1`.

- [ ] **Step 3: Wire `InProcessBus` into the `Emitter`**

In `crates/core/src/fleet.rs`:

1. Add the import: `use crate::bus::{EventBus, InProcessBus};`
2. Change the `Emitter.bus` field type from `broadcast::Sender<FleetEvent>` to `Arc<dyn EventBus>`:

```rust
#[derive(Clone)]
struct Emitter {
    store: Arc<dyn Store>,
    bus: Arc<dyn EventBus>,
    seqs: Arc<Mutex<HashMap<String, u64>>>,
    metrics: Arc<Metrics>,
}
```

3. In `Emitter::emit`, replace `let _ = self.bus.send(event);` with `self.bus.publish(event);`.
4. In `Emitter::emit_persist_gap`, replace `let _ = self.bus.send(marker);` with `self.bus.publish(marker);`.
5. In `FleetManager::new`, construct the bus as a trait object:

```rust
        let registry = Registry::load(&config.registry_path())?;
        let bus: Arc<dyn EventBus> = Arc::new(InProcessBus::new(config.event_buffer));
        let emitter = Emitter {
            store,
            bus,
            seqs: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::default()),
        };
```

6. In `FleetManager::subscribe`, delegate through the trait (signature unchanged):

```rust
    /// Subscribe to the live event bus.
    pub fn subscribe(&self) -> broadcast::Receiver<FleetEvent> {
        self.inner.emitter.bus.subscribe()
    }
```

7. In the `tests` module, update `emitter_with` to build an `InProcessBus`, and any test that reads from `emitter.bus.subscribe()` (e.g. `append_failure_emits_persist_gap_marker_visible_to_history`, `healthy_append_emits_no_gap_marker`):

```rust
    fn emitter_with(store: Arc<dyn Store>) -> Emitter {
        Emitter {
            store,
            bus: Arc::new(InProcessBus::new(16)),
            seqs: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::default()),
        }
    }
```

The gap-marker tests call `emitter.bus.subscribe()` — that now resolves through `EventBus::subscribe` and still returns a `broadcast::Receiver`, so `rx.try_recv()` keeps working with no test-body change.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p prospero-core`
Expected: PASS — `bus::tests`, `fleet::tests` (gap-marker tests still read the live bus), and `event`/`store` tests.

Run: `cargo build -p prospero-api -p prospero-daemon --tests`
Expected: compiles — `subscribe()` signature is unchanged, so `crates/api/src/sse.rs` is untouched.

- [ ] **Step 5: Commit**

```bash
git add crates/core/src/bus.rs crates/core/src/lib.rs crates/core/src/fleet.rs
git commit -m "feat(core): EventBus trait + InProcessBus behind FleetManager::subscribe"
```

---

## Task 5: `Ownership` trait + `SelfOwnsAll`

Introduce the ownership seam and gate the single attach choke-point through it. In standalone (`SelfOwnsAll`) every acquire succeeds, so behavior is identical — but the gate is where Phase 2's lease check will live.

**Files:**
- Create: `crates/core/src/ownership.rs`
- Modify: `crates/core/src/lib.rs` (declare + re-export)
- Modify: `crates/core/src/fleet.rs` (`Inner.ownership`, `FleetManager::new`, `start_attach`, attach-task cleanup)
- Test: `crates/core/src/ownership.rs` + `crates/core/src/fleet.rs`

- [ ] **Step 1: Write the failing test (the trait + impl)**

Create `crates/core/src/ownership.rs`:

```rust
//! Which process is the single writer for a given stream.
//!
//! Standalone uses [`SelfOwnsAll`]: one process owns every stream, so the lease
//! is a no-op. The clustered `LeasedOwnership` (a Postgres lease row + reaper)
//! drops in behind the same trait in a later phase — see the topology design
//! spec §3.3. The `epoch` on [`Lease`] exists now so control-fencing can be
//! added later without a wire change.

use crate::error::Result;

/// A claim on a stream's single-writer role. `epoch` is a monotonic fencing
/// token (always 0 under [`SelfOwnsAll`]).
#[derive(Debug, Clone)]
pub struct Lease {
    /// The owned stream key.
    pub stream_key: String,
    /// Monotonic fencing epoch for the claim.
    pub epoch: u64,
}

/// Single-writer ownership of streams.
pub trait Ownership: Send + Sync {
    /// Claim `stream_key` if it is free (or already held by this process).
    /// Returns the lease, or `None` if another writer owns it.
    fn try_acquire(&self, stream_key: &str) -> Option<Lease>;

    /// Extend a held lease. Errors if the lease was lost (stolen/expired).
    fn renew(&self, lease: &Lease) -> Result<()>;

    /// Release a held stream so a peer may claim it.
    fn release(&self, stream_key: &str);

    /// Whether this process currently owns `stream_key`.
    fn owns(&self, stream_key: &str) -> bool;
}

/// Standalone ownership: this process owns every stream unconditionally.
pub struct SelfOwnsAll;

impl Ownership for SelfOwnsAll {
    fn try_acquire(&self, stream_key: &str) -> Option<Lease> {
        Some(Lease {
            stream_key: stream_key.to_string(),
            epoch: 0,
        })
    }
    fn renew(&self, _lease: &Lease) -> Result<()> {
        Ok(())
    }
    fn release(&self, _stream_key: &str) {}
    fn owns(&self, _stream_key: &str) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_owns_all_always_acquires_and_owns() {
        let o = SelfOwnsAll;
        let lease = o.try_acquire("a1").expect("standalone always acquires");
        assert_eq!(lease.stream_key, "a1");
        assert_eq!(lease.epoch, 0);
        assert!(o.owns("a1"));
        assert!(o.owns("anything-else"));
        o.renew(&lease).unwrap();
        o.release("a1"); // no-op, must not panic
    }
}
```

- [ ] **Step 2: Declare the module and run the test to verify it passes**

In `crates/core/src/lib.rs`, add `pub mod ownership;` (alphabetical — after `pub mod model;`, before `pub mod provider_env;`) and the re-export:

```rust
pub use ownership::{Lease, Ownership, SelfOwnsAll};
```

Run: `cargo test -p prospero-core ownership::tests`
Expected: PASS.

- [ ] **Step 3: Write the failing wiring test**

Add to the `tests` module in `crates/core/src/fleet.rs`:

```rust
    #[tokio::test]
    async fn ownership_gates_the_attach_path() {
        // A FleetManager built with SelfOwnsAll attaches normally: spawning an
        // agent records it in the attached set (ownership never refuses).
        use crate::testkit::FakeCaliband;

        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
        config.ensure.autostart = false;
        let root = dir.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        let socket = crate::discovery::resolve_socket(&root, &config.discovery_env).unwrap();
        let _fake = FakeCaliband::start_at(&socket).await.unwrap();

        let store = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).unwrap();
        mgr.add_repo("p", &root).await.unwrap();

        let id = mgr.spawn_agent("p", SpawnRequest::new("hi")).await.unwrap();
        // Ownership acquired → the agent is in the attached set.
        assert!(mgr.is_attached(&id).await, "owned agent must be attached");
    }
```

This needs a small test helper `is_attached`. Add it as a public method on `FleetManager` (used by tests/observability, mirroring `cached_client_names`):

```rust
    /// Whether a per-agent attach task is currently registered (test/obs helper).
    pub async fn is_attached(&self, agent_id: &str) -> bool {
        self.inner.attached.lock().unwrap().contains(agent_id)
    }
```

- [ ] **Step 4: Run the wiring test to verify it fails**

Run: `cargo test -p prospero-core fleet::tests::ownership_gates_the_attach_path`
Expected: FAIL — `no method named is_attached` until Step 3's helper compiles, then FAIL/ERROR because `Inner` has no `ownership` field yet (added in Step 5). (If both helper and field are added together, this goes green in Step 6.)

- [ ] **Step 5: Wire `Ownership` into `Inner`, `new`, and `start_attach`**

In `crates/core/src/fleet.rs`:

1. Add the import: `use crate::ownership::{Ownership, SelfOwnsAll};`
2. Add a field to `Inner`:

```rust
struct Inner {
    config: FleetConfig,
    snapshot: RwLock<FleetSnapshot>,
    registry: RwLock<Registry>,
    clients: Mutex<HashMap<String, CalibandClient>>,
    attached: Mutex<HashSet<String>>,
    emitter: Emitter,
    ownership: Arc<dyn Ownership>,
    shutdown: watch::Sender<bool>,
}
```

3. In `FleetManager::new`, construct it in the `Inner { .. }` literal:

```rust
                emitter,
                ownership: Arc::new(SelfOwnsAll),
                shutdown: watch::channel(false).0,
```

4. Gate `start_attach` on ownership, and release on task end. Replace the opening of `start_attach`:

```rust
    async fn start_attach(&self, repo: &str, agent_id: &str, client: CalibandClient) {
        // Only drive an agent this process owns. Standalone always acquires;
        // clustered consults the lease (Phase 2).
        if self.inner.ownership.try_acquire(agent_id).is_none() {
            return;
        }
        {
            let mut attached = self.inner.attached.lock().unwrap();
            if !attached.insert(agent_id.to_string()) {
                self.inner.ownership.release(agent_id); // re-acquired but already attached
                return; // already attached
            }
        }
```

And in the spawned task's cleanup at the end of `start_attach`, release the lease alongside the attached-set removal:

```rust
            attached.attached.lock().unwrap().remove(&agent_id);
            attached.ownership.release(&agent_id);
```

(`attached` here is the `self.inner.clone()` captured as `let attached = self.inner.clone();` — it now also exposes `.ownership`.)

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p prospero-core fleet::tests`
Expected: PASS — including `ownership_gates_the_attach_path` and all prior fleet tests (`spawn_passes_repo_provider_into_spawnspec`, etc., which exercise the same attach path and must still attach).

- [ ] **Step 7: Commit**

```bash
git add crates/core/src/ownership.rs crates/core/src/lib.rs crates/core/src/fleet.rs
git commit -m "feat(core): Ownership trait + SelfOwnsAll gating the attach path"
```

---

## Task 6: `Store` conformance battery

A single reusable test function that asserts the per-stream `Store` contract, run against `JsonlStore` now and reused for the sqlite/Postgres impls in later phases (spec §6).

**Files:**
- Modify: `crates/core/src/store.rs` (add a `#[cfg(test)]` conformance fn + a test that runs it against `JsonlStore`)

- [ ] **Step 1: Write the failing test**

In the `tests` module of `crates/core/src/store.rs`, add a generic conformance routine and a `JsonlStore` test that drives it:

```rust
    /// The behavioral contract every `Store` must satisfy. Reused by later
    /// backends (sqlite, Postgres) so parity is enforced, not assumed.
    fn store_conformance(store: &dyn Store) {
        // Empty store: high-water 0, replay empty, writable.
        assert_eq!(store.high_water("a").unwrap(), 0);
        assert!(store.replay("a", 0).unwrap().is_empty());
        assert!(store.writable());

        // Appends are per-stream ordered and isolated.
        store.append(&ev(1, "a", "a1")).unwrap();
        store.append(&ev(1, "b", "b1")).unwrap();
        store.append(&ev(2, "a", "a2")).unwrap();

        assert_eq!(store.high_water("a").unwrap(), 2);
        assert_eq!(store.high_water("b").unwrap(), 1);

        let a = store.replay("a", 0).unwrap();
        assert_eq!(a.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2]);
        // from_seq is inclusive lower bound.
        let a_from2 = store.replay("a", 2).unwrap();
        assert_eq!(a_from2.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![2]);
        // Stream isolation: "b" never sees "a"'s events.
        let b = store.replay("b", 0).unwrap();
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn jsonl_store_satisfies_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        store_conformance(&store);
    }
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p prospero-core store::tests::jsonl_store_satisfies_conformance`
Expected: PASS.

(This is a characterization test over already-implemented behavior; it goes green immediately. Its value is as the reusable battery for Phase 1/2. To see it exercise a failure, temporarily change `vec![1, 2]` to `vec![1, 3]` and observe FAIL, then restore.)

- [ ] **Step 3: Commit**

```bash
git add crates/core/src/store.rs
git commit -m "test(core): reusable Store conformance battery (per-stream contract)"
```

---

## Task 7: Full gate + wrap-up

- [ ] **Step 1: Run the complete CI-mirror gate**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```

Expected: all four pass. If `clippy` flags the `Arc<dyn EventBus>`/`Arc<dyn Ownership>` clones or the new modules, fix per its guidance (no `#[allow]` without cause).

- [ ] **Step 2: Confirm no behavior drift in dependents**

Run the dependent crates' integration tests explicitly:

```bash
cargo test -p prospero-api
cargo test -p prospero-cli
```

Expected: PASS. These exercise `subscribe()`, `history()`, and the readiness/SSE paths — all of which kept their signatures, so they validate that Phase 0 is behavior-preserving.

- [ ] **Step 3: Final commit (if `cargo fmt` made changes)**

```bash
git add -A
git commit -m "style(core): cargo fmt after Phase 0 seam refactor" || echo "nothing to format"
```

---

## Self-Review Notes (for the implementer)

- **Behavior preservation is the acceptance bar.** No SSE output, no history result, and no readiness response should change. The only observable semantic change is that `seq` now restarts per stream — which the per-agent replay/SSE join already keyed on (`history(agent_id, ..)` == `replay(stream_key, ..)` for agents), so dashboards are unaffected.
- **Out of scope for Phase 0 (do NOT add here):** the storage-layer `global_ordinal` column (Phase 1, lands with the sqlite/`sqlx` backend — `JsonlStore`'s line order is its implicit ordinal); per-stream `subscribe(stream_key)` on `EventBus` (Phase 2, when `DistributedBus` needs it); any real lease/reaper logic (Phase 2); `ConfigStore` (Phase 1). Keep `EventBus` at `publish` + global `subscribe()`, and `Ownership` at the no-op `SelfOwnsAll`.
- **Type consistency check:** the field is `seqs` (not `seq`) everywhere; `EventBus::publish` consumes the event by value; `Store::replay`/`high_water` take `stream_key: &str` as the first arg in every impl (`JsonlStore`, `FlakyStore`, `UnwritableStore`).

---

## Next Plans (not in scope here)

- **Phase 1** — sqlite `Store` + `ConfigStore` via `sqlx`, `global_ordinal` column, retention (#3, #4).
- **Phase 2** — `PostgresStore`, `DistributedBus` (LISTEN/NOTIFY), `LeasedOwnership` (lease + reaper), per-stream `subscribe`, k8s artifacts.
- **Phase 3** — API auth (#2), epoch control-fencing (caliban upstream dependency).
