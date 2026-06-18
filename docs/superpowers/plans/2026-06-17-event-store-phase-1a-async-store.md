# Event-Store Phase 1a — Async `Store` Trait Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the `Store` trait (and the `Emitter`/`history`/SSE-tail code paths that touch it) from synchronous to **async**, behavior-preserving, with `JsonlStore` remaining the only backend — so the `sqlx`-backed `SqliteStore` (Phase 1b) drops into a clean async seam.

**Architecture:** `Store` becomes an `#[async_trait]` trait (`async fn append/replay/high_water/writable`). `JsonlStore` keeps its synchronous file logic inside async method bodies. The event `Emitter` becomes async; the per-stream seq seed is restructured so the durable `high_water` read happens **without** holding the `seqs` lock across an `.await` (using a `tokio::sync::Mutex` + double-checked seed). `FleetManager::history`/`readiness` become async (their callers are already async). The api crate's SSE tail state machine (`HistorySource`/`Tailer`) becomes async too. No new backend, no `sqlx`, no behavior change.

**Tech Stack:** Rust (edition 2024), tokio, `async-trait` (new dep), `prospero-core` + `prospero-api`. Design source: `docs/superpowers/specs/2026-06-17-event-store-topology-design.md` §3 + §5 (Phase 1); decision to go async confirmed for `sqlx`/Postgres alignment.

**Verification gate (mirrors `.github/workflows/ci.yml`; `TESTKIT = --features prospero-core/testkit`):**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```

**Prerequisite:** builds on Phase 0 (per-stream `Store`, `EventBus`, `Ownership` — PR #43). Branch from that work.

---

## Why this is one tightly-coupled change

Making `Store::append` async forces **every** `impl Store` and **every** call site to change in the same compile unit, or the workspace won't build. The tasks are therefore ordered so each ends at a **compiling, green** checkpoint: Task 1 leaves `prospero-core` compiling (trait + `JsonlStore` + store tests), Task 2 leaves `prospero-core` fully green (the `Emitter`/`FleetManager` callers), Task 3 leaves `prospero-api` green. Commit at each task boundary.

## File Structure

- **Modify** `Cargo.toml` (workspace) — add `async-trait` to `[workspace.dependencies]`.
- **Modify** `crates/core/Cargo.toml` — depend on `async-trait`.
- **Modify** `crates/core/src/store.rs` — `#[async_trait]` trait + `JsonlStore` impl; async store tests + conformance (Task 1).
- **Modify** `crates/core/src/fleet.rs` — async `Emitter`, `seqs` → `tokio::sync::Mutex`, async `history`/`readiness`, `.await` every `emit`; async test doubles + tests (Task 2).
- **Modify** `crates/api/src/handlers.rs`, `crates/api/src/sse.rs`, `crates/api/src/sse/tail.rs` — `.await` the now-async `history`; async `HistorySource`/`Tailer` (Task 3).
- **Modify** `crates/api/tests/api_integration.rs` — `UnwritableStore` async impl (Task 3).

---

## Task 1: Async `Store` trait + `JsonlStore`

**Files:**
- Modify: `Cargo.toml`, `crates/core/Cargo.toml`
- Modify: `crates/core/src/store.rs`

- [ ] **Step 1: Add the `async-trait` dependency**

In the workspace root `Cargo.toml`, under `[workspace.dependencies]` (alongside the existing entries like `tokio`, `serde`), add:

```toml
async-trait = "0.1"
```

In `crates/core/Cargo.toml`, under `[dependencies]`, add (matching how sibling deps reference the workspace):

```toml
async-trait = { workspace = true }
```

Run `cargo build -p prospero-core` — expect it to still build (dep added, unused so far; a warning about unused dep is fine until Step 2).

- [ ] **Step 2: Write the failing (async) store test**

In `crates/core/src/store.rs`, the existing `tests` module tests are synchronous. Convert the first one to async to drive the trait change. Replace `append_and_replay_filters_by_agent_and_seq`:

```rust
    #[tokio::test]
    async fn append_and_replay_filters_by_agent_and_seq() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        store.append(&ev(1, "a", "one")).await.unwrap();
        store.append(&ev(2, "b", "two")).await.unwrap();
        store.append(&ev(3, "a", "three")).await.unwrap();

        let a_events = store.replay("a", 0).await.unwrap();
        assert_eq!(a_events.len(), 2);
        assert_eq!(a_events[0].seq, 1);
        assert_eq!(a_events[1].seq, 3);

        let from2 = store.replay("a", 3).await.unwrap();
        assert_eq!(from2.len(), 1);
        assert_eq!(from2[0].seq, 3);
    }
```

Run: `cargo test -p prospero-core store::tests::append_and_replay_filters_by_agent_and_seq`
Expected: FAIL to compile — `.await` on a non-async `append`/`replay`.

- [ ] **Step 3: Make the trait async**

In `crates/core/src/store.rs`, add the import at the top (with the other `use`s):

```rust
use async_trait::async_trait;
```

Change the trait definition to:

```rust
/// A durable, append-only event log keyed by stream.
#[async_trait]
pub trait Store: Send + Sync {
    /// Append one event to durable storage.
    async fn append(&self, event: &FleetEvent) -> Result<()>;

    /// Replay events for one stream with `seq >= from_seq`, in `seq` order.
    async fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>>;

    /// The highest `seq` ever persisted for `stream_key` (0 if none). Used to
    /// resume that stream's sequence counter across daemon restarts.
    async fn high_water(&self, stream_key: &str) -> Result<u64>;

    /// Whether the backend can currently accept writes. A cheap, non-destructive
    /// probe used by the readiness endpoint.
    async fn writable(&self) -> bool;
}
```

- [ ] **Step 4: Make the `JsonlStore` impl async**

Change `impl Store for JsonlStore` to `#[async_trait] impl Store for JsonlStore`, and add `async` to each method. The bodies are UNCHANGED (synchronous file I/O inside an async fn is acceptable for this debug/dev backend — `read_all` stays a sync private helper):

```rust
#[async_trait]
impl Store for JsonlStore {
    async fn append(&self, event: &FleetEvent) -> Result<()> {
        let mut line = serde_json::to_string(event)?;
        line.push('\n');
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| CoreError::Store("event store write lock poisoned".into()))?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(line.as_bytes())?;
        Ok(())
    }

    async fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
        let mut events: Vec<FleetEvent> = self
            .read_all()?
            .into_iter()
            .filter(|e| e.stream_key() == stream_key && e.seq >= from_seq)
            .collect();
        events.sort_by_key(|e| e.seq);
        Ok(events)
    }

    async fn high_water(&self, stream_key: &str) -> Result<u64> {
        Ok(self
            .read_all()?
            .iter()
            .filter(|e| e.stream_key() == stream_key)
            .map(|e| e.seq)
            .max()
            .unwrap_or(0))
    }

    async fn writable(&self) -> bool {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .is_ok()
    }
}
```

Note: `write_lock` stays a `std::sync::Mutex` and is released within the same statement (not held across an `.await`), so it's fine.

- [ ] **Step 5: Convert the remaining store tests + the conformance battery to async**

Every other `#[test]` in `store.rs` that calls `append`/`replay`/`high_water`/`writable` becomes `#[tokio::test] async fn` with `.await` on those calls. Specifically convert: `high_water_recovers_max_seq_across_reopen`, `high_water_is_zero_when_empty`, `writable_reflects_store_permissions`, `corrupt_trailing_line_is_tolerated`, `high_water_is_scoped_per_stream`, and `jsonl_store_satisfies_conformance`. Worked example for one:

```rust
    #[tokio::test]
    async fn high_water_is_scoped_per_stream() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        store.append(&ev(1, "a", "one")).await.unwrap();
        store.append(&ev(1, "b", "one")).await.unwrap();
        store.append(&ev(2, "a", "two")).await.unwrap();
        assert_eq!(store.high_water("a").await.unwrap(), 2);
        assert_eq!(store.high_water("b").await.unwrap(), 1);
        assert_eq!(store.high_water("missing").await.unwrap(), 0);
    }
```

Make `store_conformance` async and await its calls, and call it with `.await`:

```rust
    async fn store_conformance(store: &dyn Store) {
        assert_eq!(store.high_water("a").await.unwrap(), 0);
        assert!(store.replay("a", 0).await.unwrap().is_empty());
        assert!(store.writable().await);

        store.append(&ev(1, "a", "a1")).await.unwrap();
        store.append(&ev(1, "b", "b1")).await.unwrap();
        store.append(&ev(2, "a", "a2")).await.unwrap();

        assert_eq!(store.high_water("a").await.unwrap(), 2);
        assert_eq!(store.high_water("b").await.unwrap(), 1);

        let a = store.replay("a", 0).await.unwrap();
        assert_eq!(a.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2]);
        let a_from2 = store.replay("a", 2).await.unwrap();
        assert_eq!(a_from2.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![2]);
        let b = store.replay("b", 0).await.unwrap();
        assert_eq!(b.len(), 1);
    }

    #[tokio::test]
    async fn jsonl_store_satisfies_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        store_conformance(&store).await;
    }
```

For `writable_reflects_store_permissions`, keep its body but make it `#[tokio::test] async fn` and `.await` the two `store.writable()` calls.

- [ ] **Step 6: Verify the core lib + store tests compile and pass**

```bash
cargo build -p prospero-core            # EXPECT: FAIL — fleet.rs callers not yet async (that's Task 2)
cargo test -p prospero-core --lib store::tests 2>&1 | tail -20
```

`cargo build -p prospero-core` will FAIL at this point because `fleet.rs` still calls the now-async methods synchronously — that is expected and fixed in Task 2 (the change is compile-coupled). Do NOT try to fix fleet.rs here beyond what Task 2 specifies. To confirm Task 1's store.rs is internally correct, you can temporarily check just the file with `cargo check -p prospero-core 2>&1 | rg "store.rs"` and confirm there are NO errors pointing at `store.rs` (all remaining errors should point at `fleet.rs`).

- [ ] **Step 7: Commit (WIP checkpoint — core not yet building; note it in the message)**

```bash
git add Cargo.toml crates/core/Cargo.toml crates/core/src/store.rs
git commit -m "feat(core)!: make Store trait async via async-trait (wip: callers in next commit)"
```

---

## Task 2: Async `Emitter`, `history`, `readiness` + fleet callers

**Files:**
- Modify: `crates/core/src/fleet.rs`

- [ ] **Step 1: Switch `seqs` to a tokio async mutex and make the `Emitter` async**

In `crates/core/src/fleet.rs`, update imports: remove `use std::sync::{Arc, Mutex};` usage of `Mutex` ONLY where it backed `seqs` — but `Mutex` (std) is still used for `clients`/`attached` in `Inner`, so KEEP `use std::sync::{Arc, Mutex};`. Add tokio's async mutex by referring to it fully-qualified to avoid a name clash: use `tokio::sync::Mutex as AsyncMutex`. Add to the existing `use tokio::sync::{...}` line: `use tokio::sync::{Mutex as AsyncMutex, RwLock, broadcast, watch};`.

Change the `Emitter` struct's `seqs` field type:

```rust
#[derive(Clone)]
struct Emitter {
    store: Arc<dyn Store>,
    bus: Arc<dyn EventBus>,
    /// Next `seq` per stream key, seeded lazily from the store's high-water.
    seqs: Arc<AsyncMutex<HashMap<String, u64>>>,
    metrics: Arc<Metrics>,
}
```

Replace `next_event` with an async version that seeds the per-stream counter **without holding the lock across the `high_water` await** (double-checked seed):

```rust
    async fn next_event(&self, repo: &str, agent_id: &str, kind: EventKind) -> FleetEvent {
        let stream_key = crate::event::stream_key_for(repo, agent_id);
        let seq = self.next_seq(&stream_key).await;
        FleetEvent {
            seq,
            ts: chrono::Utc::now().to_rfc3339(),
            repo: repo.to_string(),
            agent_id: agent_id.to_string(),
            kind,
        }
    }

    /// Allocate the next per-stream `seq`. The durable `high_water` read on a
    /// stream's first event happens WITHOUT holding the `seqs` lock, so the lock
    /// is never held across `.await` (no executor stall / cross-stream serialize).
    async fn next_seq(&self, stream_key: &str) -> u64 {
        // Fast path: counter already seeded for this stream this run.
        {
            let mut seqs = self.seqs.lock().await;
            if let Some(n) = seqs.get(stream_key) {
                let next = n + 1;
                seqs.insert(stream_key.to_string(), next);
                return next;
            }
        }
        // Slow path: first event this run — read durable high-water unlocked.
        let seeded = self.store.high_water(stream_key).await.unwrap_or_else(|e| {
            tracing::warn!(
                target: "prospero_fleet", stream = %stream_key, error = %e,
                "high_water read failed seeding per-stream seq; starting at 0"
            );
            0
        });
        let mut seqs = self.seqs.lock().await;
        // Re-check: a concurrent task may have seeded while we read high_water.
        let next = match seqs.get(stream_key) {
            Some(n) => n + 1,
            None => seeded + 1,
        };
        seqs.insert(stream_key.to_string(), next);
        next
    }
```

- [ ] **Step 2: Make `emit` and `emit_persist_gap` async**

Replace `emit`:

```rust
    async fn emit(&self, repo: &str, agent_id: &str, kind: EventKind) {
        let event = self.next_event(repo, agent_id, kind).await;
        let lost_seq = event.seq;
        let append_err = match self.store.append(&event).await {
            Ok(()) => {
                self.metrics.record_append_ok();
                None
            }
            Err(e) => {
                self.metrics.record_append_failure();
                Some(e)
            }
        };
        self.bus.publish(event);
        if let Some(e) = append_err {
            tracing::warn!(target: "prospero_fleet", error = %e, "failed to persist event");
            self.emit_persist_gap(repo, agent_id, lost_seq, e).await;
        }
    }
```

Replace `emit_persist_gap`'s signature with `async fn` and `.await` the `next_event`/`append` calls inside it:

```rust
    async fn emit_persist_gap(&self, repo: &str, agent_id: &str, lost_seq: u64, err: CoreError) {
        let marker = self
            .next_event(
                repo,
                agent_id,
                EventKind::StorePersistFailed {
                    lost_seq,
                    detail: err.to_string(),
                },
            )
            .await;
        match self.store.append(&marker).await {
            Ok(()) => self.metrics.record_append_ok(),
            Err(e) => {
                self.metrics.record_append_failure();
                tracing::warn!(target: "prospero_fleet", error = %e, "failed to persist store-gap marker");
            }
        }
        self.bus.publish(marker);
    }
```

- [ ] **Step 3: `.await` every `emit` call site, and make `history`/`readiness` async**

In `crates/core/src/fleet.rs`, add `.await` to each `self.inner.emitter.emit(...)` / `emitter.emit(...)` call. The call sites are:
- `spawn_agent`: `self.inner.emitter.emit(repo, &id, EventKind::AgentSpawned).await;`
- `mark_unreachable`: `.emit(repo, "", EventKind::RepoHealth { state: new_health }).await;`
- `reconcile`: the `AgentDiscovered`, `StatusChanged`, `AgentGone`, and `RepoHealth { Healthy }` emits — add `.await` to each.
- `attach_once` (the free function): `emitter.emit(repo, agent_id, kind).await;`

Make `history` async (the api callers already run in async contexts):

```rust
    /// Replay a stream's history from the store, with `seq >= from_seq`. Callers
    /// watching a single agent pass the agent id, which is that agent's stream
    /// key (see [`crate::event::stream_key_for`]); repo/fleet-level history uses
    /// the `repo:<name>` / `fleet` keys.
    pub async fn history(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
        self.inner.emitter.store.replay(stream_key, from_seq).await
    }
```

In `readiness`, await the writability probe:

```rust
        let store_writable = self.inner.emitter.store.writable().await;
```

`FleetManager::new` is unchanged — it no longer touches the store at construction (Phase 0 removed the eager high-water seed), so it stays a synchronous `fn new`. Confirm the `Emitter { .. }` literal there builds `seqs: Arc::new(AsyncMutex::new(HashMap::new()))`.

- [ ] **Step 4: Update the in-file test doubles + tests to async**

In the `tests` module of `fleet.rs`:

1. `FlakyStore` — make its `impl Store` async:

```rust
    #[async_trait::async_trait]
    impl Store for FlakyStore {
        async fn append(&self, event: &FleetEvent) -> Result<()> {
            if self.fail_seqs.lock().unwrap().contains(&event.seq) {
                return Err(CoreError::Store("injected append failure".into()));
            }
            self.inner.append(event).await
        }
        async fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
            self.inner.replay(stream_key, from_seq).await
        }
        async fn high_water(&self, stream_key: &str) -> Result<u64> {
            self.inner.high_water(stream_key).await
        }
        async fn writable(&self) -> bool {
            self.inner.writable().await
        }
    }
```

(Add `use async_trait::async_trait;` to the test module, or reference it fully-qualified as shown.)

2. `emitter_with` — build the async-mutex `seqs`:

```rust
    fn emitter_with(store: Arc<dyn Store>) -> Emitter {
        Emitter {
            store,
            bus: Arc::new(InProcessBus::new(16)),
            seqs: Arc::new(AsyncMutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::default()),
        }
    }
```

3. Convert every test that calls `emit`/`replay`/`high_water` or builds an emitter to `#[tokio::test] async fn` with `.await`. These are: `seq_is_monotonic_per_stream_not_global`, `seq_resumes_per_stream_from_high_water`, `append_failure_emits_persist_gap_marker_visible_to_history`, `healthy_append_emits_no_gap_marker`, `append_failure_and_success_advance_metrics`. Worked example:

```rust
    #[tokio::test]
    async fn seq_is_monotonic_per_stream_not_global() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let emitter = emitter_with(store);

        emitter.emit("r", "a1", EventKind::AgentSpawned).await;
        emitter.emit("r", "a2", EventKind::AgentSpawned).await;
        emitter.emit("r", "a1", EventKind::AgentGone).await;

        let a1 = emitter
            .store
            .replay(&crate::event::stream_key_for("r", "a1"), 0)
            .await
            .unwrap();
        let a2 = emitter
            .store
            .replay(&crate::event::stream_key_for("r", "a2"), 0)
            .await
            .unwrap();
        assert_eq!(a1.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(a2.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1]);
    }
```

For `seq_resumes_per_stream_from_high_water`, the pre-seed append also becomes async: `store.append(&ev(5, "a1", "old")).await.unwrap();` (inside a `JsonlStore::open` block — note `JsonlStore::open` is still sync; only the trait methods are async).

For the gap-marker tests, the existing `emitter.bus.subscribe()` + `rx.try_recv()` lines are unchanged (the bus is unaffected); only `emitter.emit(...)` gains `.await` and the `store.replay(...)` assertion gains `.await`. The test fns become `#[tokio::test] async fn`.

The existing `#[tokio::test]` integration-style tests in this module (`restart_caliband_shuts_down_and_clears_client`, `send_agent_input_rejects_...`, `spawn_passes_repo_provider_into_spawnspec`, `ensure_config_for_merges_default_and_repo_config`, `run_drains_and_returns_on_shutdown`) already build a real `JsonlStore` via `FleetManager::new` and call only already-async `FleetManager` methods — they need NO change unless they call `emit`/`replay`/`high_water` directly (they don't). Leave them as-is.

- [ ] **Step 5: Verify the core crate is fully green**

```bash
cargo build -p prospero-core --all-targets --features testkit
cargo test -p prospero-core --features testkit 2>&1 | tail -15
cargo clippy -p prospero-core --lib --features testkit -- -D warnings
```
Expected: builds, all core tests pass (per-stream seq behavior identical; gap-marker still works), clippy clean. If clippy flags `clippy::await_holding_lock`, it means the `seqs` lock is held across an `.await` somewhere — re-check `next_seq` follows the drop-then-await structure above.

- [ ] **Step 6: Commit**

```bash
git add crates/core/src/fleet.rs
git commit -m "feat(core)!: async Emitter/history/readiness; per-stream seq seed off-lock"
```

---

## Task 3: Async ripple through the `prospero-api` crate

**Files:**
- Modify: `crates/api/src/handlers.rs`, `crates/api/src/sse.rs`, `crates/api/src/sse/tail.rs`, `crates/api/tests/api_integration.rs`

- [ ] **Step 1: `.await` the now-async `history` in handlers + sse**

In `crates/api/src/handlers.rs`, `get_agent_events` (line ~120):

```rust
    Ok(Json(st.manager.history(&id, q.from).await?))
```

In `crates/api/src/sse.rs`, `agent_stream` (line ~31):

```rust
    let history = st.manager.history(&id, q.from).await.unwrap_or_default();
```

- [ ] **Step 2: Make `HistorySource` + `Tailer` async (sse/tail.rs)**

The tail state machine calls `history` during the `Lagged` self-heal. Use `#[async_trait]` for `HistorySource` (not a native `async fn` in trait): the SSE handler future must be `Send`, and `async-trait`'s boxed `+ Send` futures avoid the brittle `Send`-leakage errors native async-fn-in-trait can produce inside axum handlers.

First, make `async-trait` available to the api crate as a regular dependency — in `crates/api/Cargo.toml` under `[dependencies]`, add:

```toml
async-trait = { workspace = true }
```

Add the import to `crates/api/src/sse/tail.rs` (with the other `use`s):

```rust
use async_trait::async_trait;
```

Change the `HistorySource` trait + its `FleetManager` impl:

```rust
/// Source of persisted events for replay-based self-heal.
#[async_trait]
pub(crate) trait HistorySource {
    /// Events for `agent_id` with `seq >= from`, in order.
    async fn history(&self, agent_id: &str, from: u64) -> Vec<FleetEvent>;
}

#[async_trait]
impl HistorySource for FleetManager {
    async fn history(&self, agent_id: &str, from: u64) -> Vec<FleetEvent> {
        FleetManager::history(self, agent_id, from)
            .await
            .unwrap_or_default()
    }
}
```

Make `Tailer::on_recv` async and `.await` the history call:

```rust
    pub(crate) async fn on_recv(&mut self, r: Result<FleetEvent, RecvError>) -> Step {
        match r {
            Ok(ev) if ev.agent_id == self.agent_id && ev.seq > self.last_delivered => {
                self.last_delivered = ev.seq;
                let terminal = is_terminal(&ev);
                let frames = vec![Frame::Event(ev)];
                if terminal { Step::EmitAndClose(frames) } else { Step::Emit(frames) }
            }
            Ok(_) => Step::Skip,
            Err(RecvError::Lagged(skipped)) => {
                let mut frames = vec![Frame::Gap { skipped, last_seq: self.last_delivered }];
                let mut terminal = false;
                for ev in self.history.history(&self.agent_id, self.last_delivered + 1).await {
                    if ev.seq <= self.last_delivered {
                        continue;
                    }
                    self.last_delivered = ev.seq;
                    terminal = is_terminal(&ev);
                    frames.push(Frame::Event(ev));
                    if terminal {
                        break;
                    }
                }
                if terminal { Step::EmitAndClose(frames) } else { Step::Emit(frames) }
            }
            Err(RecvError::Closed) => Step::Close,
        }
    }
```

Note: `Tailer` is declared `pub(crate) struct Tailer<H: HistorySource>`. With a native `async fn` in `HistorySource`, add `+ Sync` where needed only if the compiler asks; the generic use here does not require `async-trait`.

- [ ] **Step 3: `.await` the `on_recv` call in the SSE loop**

In `crates/api/src/sse.rs`, the tail loop (line ~57) becomes:

```rust
        loop {
            match tailer.on_recv(rx.recv().await).await {
                Step::Emit(frames) => {
                    for f in frames {
                        yield Ok(frame_to_event(&f));
                    }
                }
                Step::EmitAndClose(frames) => {
                    for f in frames {
                        yield Ok(frame_to_event(&f));
                    }
                    break;
                }
                Step::Skip => continue,
                Step::Close => break,
            }
        }
```

- [ ] **Step 4: Make the tail unit tests async**

In `crates/api/src/sse/tail.rs` `tests` module, the `FakeHistory` impl becomes async, and every `on_recv` test becomes `#[tokio::test] async fn` with `.await`. Worked examples:

```rust
    struct FakeHistory(Vec<FleetEvent>);
    #[async_trait]
    impl HistorySource for FakeHistory {
        async fn history(&self, agent_id: &str, from: u64) -> Vec<FleetEvent> {
            self.0
                .iter()
                .filter(|e| e.agent_id == agent_id && e.seq >= from)
                .cloned()
                .collect()
        }
    }

    #[tokio::test]
    async fn forwards_in_order_event_for_this_agent() {
        let mut t = Tailer::new("a".into(), 0, FakeHistory(vec![]));
        assert_eq!(
            t.on_recv(Ok(ev(1, "a"))).await,
            Step::Emit(vec![Frame::Event(ev(1, "a"))])
        );
    }
```

Convert ALL eight tail tests the same way (`skips_other_agents_and_already_delivered`, `terminal_event_closes`, `lagged_emits_gap_then_replays_missed_events`, `lagged_replay_containing_terminal_closes`, `lagged_replay_respects_last_delivered_floor`, `lagged_with_nothing_newer_emits_gap_only`, `closed_bus_closes`): add `#[tokio::test] async fn` and `.await` each `on_recv(...)`.

- [ ] **Step 5: Make the `UnwritableStore` test double async**

In `crates/api/tests/api_integration.rs`, the `impl Store for UnwritableStore` becomes `#[async_trait] impl Store` with async methods. (Add `use async_trait::async_trait;` to the test file — `async-trait` is already a regular dependency of the api crate from Step 2, so it's available to integration tests.) The `writable` override stays `false`; the others delegate with `.await`:

```rust
    #[async_trait]
    impl Store for UnwritableStore {
        async fn append(&self, event: &FleetEvent) -> Result<()> {
            self.0.append(event).await
        }
        async fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
            self.0.replay(stream_key, from_seq).await
        }
        async fn high_water(&self, stream_key: &str) -> Result<u64> {
            self.0.high_water(stream_key).await
        }
        async fn writable(&self) -> bool {
            false
        }
    }
```

(Adapt the inner-field access — `self.0` here — to the actual field name if it differs.)

- [ ] **Step 6: Verify the api crate is green**

```bash
cargo build -p prospero-api --all-targets
cargo test -p prospero-api 2>&1 | tail -20
cargo clippy -p prospero-api --all-targets -- -D warnings
```
Expected: builds, all api tests pass (the tail self-heal tests verify identical behavior, now async).

- [ ] **Step 7: Commit**

```bash
git add crates/api/src/handlers.rs crates/api/src/sse.rs crates/api/src/sse/tail.rs crates/api/tests/api_integration.rs crates/api/Cargo.toml
git commit -m "feat(api)!: await async Store history; async SSE tail state machine"
```

---

## Task 4: Full gate + behavior-preservation check

- [ ] **Step 1: Run the complete CI-mirror gate**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```
Expected: all four pass. Watch specifically for `clippy::await_holding_lock` (must not fire — `seqs` is never locked across an `.await`).

- [ ] **Step 2: Confirm behavior preservation**

The whole point is zero behavior change with `JsonlStore`. The integration suites are the proof:

```bash
cargo test -p prospero-core --features testkit --test fleet_integration
cargo test -p prospero-api
cargo test -p prospero-cli
```
Expected: PASS. `fleet_integration` exercises poll/attach/history/persistence end-to-end; `prospero-api` exercises SSE replay + the `Lagged` self-heal; both must be green with no assertion changes.

- [ ] **Step 3: Final fmt commit if needed**

```bash
cargo fmt --all
git diff --quiet || git commit -am "style: cargo fmt after async Store migration"
```

---

## Self-Review Notes (for the implementer)

- **The migration is compile-coupled.** `prospero-core` does not build between Task 1 and Task 2 — that's expected and called out. Don't try to "fix" `fleet.rs` early in Task 1.
- **The one real hazard is `await_holding_lock`.** The `next_seq` double-checked structure exists specifically so the `seqs` async mutex is never held across the `high_water().await`. If you simplify it back to a single locked block with the await inside, clippy will (correctly) reject it.
- **`JsonlStore` does blocking fs inside async fns** — acceptable for this debug/dev backend and unchanged behavior. Do NOT introduce `spawn_blocking` or tokio::fs here; the sqlx-backed `SqliteStore` (Phase 1b) is where real async I/O lands.
- **`FleetManager::new` stays sync** — it constructs the seams and touches no store. Do not make it async (that would ripple to `prosperod` main and every test).
- **Out of scope (Phase 1b+):** `SqliteStore`, `sqlx`, the `global_ordinal` column, `ConfigStore`, retention. This plan ONLY moves the trait + callers to async with `JsonlStore` intact.
- **Type/name consistency:** `seqs: Arc<AsyncMutex<HashMap<String,u64>>>` (tokio mutex aliased `AsyncMutex`); the std `Mutex` still backs `Inner.clients`/`attached`. `Store` methods are all `async fn`. `HistorySource::history` and `Tailer::on_recv` are `async`.

## Next Plans (not in scope here)

- **Phase 1b** — `SqliteStore` via `sqlx` (sqlite), `(stream_key, seq)` index, `global_ordinal` column; make it the daemon default; keep `JsonlStore` as a dev/test backend.
- **Phase 1c** — `ConfigStore` trait + sqlite impl (Registry + per-repo provider config), sharing the DB.
- **Phase 1d** — retention (#4): `DELETE`-by-age / partitioning behind a Store method.
- **Phase 2** — `PostgresStore`, `DistributedBus` (LISTEN/NOTIFY), `LeasedOwnership` (lease + reaper).
