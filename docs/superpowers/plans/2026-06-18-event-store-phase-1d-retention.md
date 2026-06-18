# Event-Store Phase 1d — Age-Based Retention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add age-based retention (ticket #4) — a `Store::prune(before_ts)` method on every backend, a `FleetManager::prune_older_than(max_age)` convenience, and a `prosperod --retention-days N` background loop that periodically deletes events older than N days (default: disabled).

**Architecture:** `prune` is a new async `Store` trait method: `SqliteStore` does `DELETE FROM events WHERE ts < ?` (returning `rows_affected`); `JsonlStore` rewrites its file keeping only newer lines. `ts` is RFC-3339, which sorts lexically, so a string compare is a correct age cutoff. The daemon spawns an hourly retention task only when `--retention-days > 0`. A shared `testkit::store_prune_conformance` proves both backends prune identically.

**Tech Stack:** Rust (edition 2024), tokio, sqlx (sqlite), chrono. Design source: `docs/superpowers/specs/2026-06-17-event-store-topology-design.md` §4 (#4). Builds on Phase 1b (SqliteStore).

**Verification gate (`TESTKIT = --features prospero-core/testkit`):**
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```

---

## File Structure

- **Modify** `crates/core/src/store.rs` — add `async fn prune(&self, before_ts: &str) -> Result<u64>` to the trait + `JsonlStore` impl.
- **Modify** `crates/core/src/sqlite_store.rs` — `SqliteStore::prune` impl.
- **Modify** `crates/core/src/testkit.rs` — add `pub async fn store_prune_conformance(&dyn Store)`.
- **Modify** `crates/core/src/fleet.rs` — `FleetManager::prune_older_than`; update the `FlakyStore` test double.
- **Modify** `crates/api/tests/api_integration.rs` — update the `UnwritableStore` test double.
- **Modify** `crates/daemon/src/main.rs` — `--retention-days` arg + the retention loop.

---

## Task 1: `Store::prune` on every backend

**Files:** `crates/core/src/store.rs`, `crates/core/src/sqlite_store.rs`, `crates/core/src/testkit.rs`, `crates/core/src/fleet.rs` (FlakyStore), `crates/api/tests/api_integration.rs` (UnwritableStore)

- [ ] **Step 1: Add the shared prune conformance battery + a failing test**

In `crates/core/src/testkit.rs`, append:
```rust
/// Retention contract: `prune(before_ts)` deletes events with `ts < before_ts`
/// (RFC-3339, lexically ordered) and returns the count removed, leaving newer
/// events intact. Backends call this to prove identical retention semantics.
pub async fn store_prune_conformance(store: &dyn crate::store::Store) {
    use crate::event::{EventKind, FleetEvent};

    fn ev(seq: u64, ts: &str) -> FleetEvent {
        FleetEvent {
            seq,
            ts: ts.into(),
            repo: "r".into(),
            agent_id: "a".into(),
            kind: EventKind::AgentSpawned,
        }
    }

    store.append(&ev(1, "2026-01-01T00:00:00Z")).await.unwrap();
    store.append(&ev(2, "2026-03-01T00:00:00Z")).await.unwrap();
    store.append(&ev(3, "2026-06-01T00:00:00Z")).await.unwrap();

    // Prune everything strictly older than March → removes only seq 1.
    let removed = store.prune("2026-03-01T00:00:00Z").await.unwrap();
    assert_eq!(removed, 1);

    let remaining = store.replay("a", 0).await.unwrap();
    assert_eq!(remaining.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![2, 3]);

    // Idempotent: a second prune at the same cutoff removes nothing.
    assert_eq!(store.prune("2026-03-01T00:00:00Z").await.unwrap(), 0);
}
```

Add a failing test in `crates/core/src/sqlite_store.rs` `tests`:
```rust
    #[tokio::test]
    async fn sqlite_store_prunes_by_age() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path()).await.unwrap();
        crate::testkit::store_prune_conformance(&store).await;
    }
```
Run `cargo test -p prospero-core sqlite_store_prunes_by_age` → FAIL to compile (`prune` not on `Store`).

- [ ] **Step 2: Add `prune` to the trait + `JsonlStore`**

In `crates/core/src/store.rs`, add to the `Store` trait (after `writable`):
```rust
    /// Delete events with `ts < before_ts` (RFC-3339, lexically ordered).
    /// Returns the number removed. Backs age-based retention (#4).
    async fn prune(&self, before_ts: &str) -> Result<u64>;
```

Add to `#[async_trait] impl Store for JsonlStore`:
```rust
    async fn prune(&self, before_ts: &str) -> Result<u64> {
        // Hold the write lock across read+rewrite so a concurrent `append`
        // (which also takes this lock) cannot be clobbered by the rewrite.
        // All ops here are synchronous — the guard is never held across `.await`.
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| CoreError::Store("event store write lock poisoned".into()))?;
        let all = self.read_all()?;
        let before = all.len();
        let kept: Vec<FleetEvent> = all.into_iter().filter(|e| e.ts.as_str() >= before_ts).collect();
        let removed = (before - kept.len()) as u64;
        if removed == 0 {
            return Ok(0);
        }
        let mut body = String::new();
        for e in &kept {
            body.push_str(&serde_json::to_string(e)?);
            body.push('\n');
        }
        std::fs::write(&self.path, body)?;
        Ok(removed)
    }
```

Add a JsonlStore prune test in `store.rs` `tests`:
```rust
    #[tokio::test]
    async fn jsonl_store_prunes_by_age() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        crate::testkit::store_prune_conformance(&store).await;
    }
```

- [ ] **Step 3: Add `prune` to `SqliteStore`**

In `crates/core/src/sqlite_store.rs` `impl Store`:
```rust
    async fn prune(&self, before_ts: &str) -> Result<u64> {
        let res = sqlx::query("DELETE FROM events WHERE ts < ?")
            .bind(before_ts)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("prune: {e}")))?;
        Ok(res.rows_affected())
    }
```

- [ ] **Step 4: Update the test doubles**

In `crates/core/src/fleet.rs` `tests`, the `FlakyStore` `impl Store` — add:
```rust
        async fn prune(&self, before_ts: &str) -> Result<u64> {
            self.inner.prune(before_ts).await
        }
```
In `crates/api/tests/api_integration.rs`, the `UnwritableStore` `impl Store` — add:
```rust
        async fn prune(&self, before_ts: &str) -> Result<u64> {
            self.0.prune(before_ts).await
        }
```

- [ ] **Step 5: Verify**
```bash
cargo test -p prospero-core sqlite_store_prunes_by_age jsonl_store_prunes_by_age 2>&1 | tail -6
cargo test -p prospero-core --features testkit 2>&1 | tail -5
cargo build -p prospero-api --tests
cargo clippy -p prospero-core --lib --features testkit -- -D warnings
cargo fmt --all -- --check
```
All pass. If fmt diffs, `cargo fmt --all`.

- [ ] **Step 6: Commit**
```bash
git add crates/core/src/store.rs crates/core/src/sqlite_store.rs crates/core/src/testkit.rs crates/core/src/fleet.rs crates/api/tests/api_integration.rs
git commit -m "feat(core): Store::prune for age-based retention (#4)"
```

---

## Task 2: `FleetManager::prune_older_than` + daemon retention loop

**Files:** `crates/core/src/fleet.rs`, `crates/daemon/src/main.rs`

- [ ] **Step 1: Write a failing test for `prune_older_than`**

In `crates/core/src/fleet.rs` `tests`, add:
```rust
    #[tokio::test]
    async fn prune_older_than_removes_aged_events() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        // Seed one ancient and one fresh event for the same stream.
        store
            .append(&FleetEvent {
                seq: 1,
                ts: "2000-01-01T00:00:00Z".into(),
                repo: "r".into(),
                agent_id: "a".into(),
                kind: EventKind::AgentSpawned,
            })
            .await
            .unwrap();
        store
            .append(&FleetEvent {
                seq: 2,
                ts: chrono::Utc::now().to_rfc3339(),
                repo: "r".into(),
                agent_id: "a".into(),
                kind: EventKind::AgentGone,
            })
            .await
            .unwrap();

        let config = FleetConfig::new("local", dir.path());
        let mgr = FleetManager::new(config, store).unwrap();

        // Anything older than ~1 day ago: removes the ancient event, keeps the fresh one.
        let removed = mgr
            .prune_older_than(std::time::Duration::from_secs(24 * 3600))
            .await
            .unwrap();
        assert_eq!(removed, 1);
        assert_eq!(mgr.history("a", 0).await.unwrap().len(), 1);
    }
```
Run it → FAIL (`prune_older_than` undefined).

- [ ] **Step 2: Implement `prune_older_than`**

In `crates/core/src/fleet.rs`, add a method on `FleetManager` (near `history`):
```rust
    /// Delete persisted events older than `max_age`. Returns the count removed.
    /// Backs the daemon's age-based retention loop (#4).
    pub async fn prune_older_than(&self, max_age: std::time::Duration) -> Result<u64> {
        let max = chrono::Duration::from_std(max_age)
            .unwrap_or_else(|_| chrono::Duration::zero());
        let before = (chrono::Utc::now() - max).to_rfc3339();
        self.inner.emitter.store.prune(&before).await
    }
```
Run the test → PASS.

- [ ] **Step 3: Add the `--retention-days` arg + loop to the daemon**

In `crates/daemon/src/main.rs`, add to `struct Args` (after `default_env`):
```rust
    /// Delete events older than this many days on an hourly loop. 0 disables.
    #[arg(long, default_value_t = 0)]
    retention_days: u64,
```

After the `manager` is built and before/after the poll loop is spawned, add the retention task (only when enabled):
```rust
    if args.retention_days > 0 {
        let m = manager.clone();
        let max_age = Duration::from_secs(args.retention_days * 24 * 3600);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(3600));
            loop {
                tick.tick().await;
                match m.prune_older_than(max_age).await {
                    Ok(n) if n > 0 => {
                        tracing::info!(target: "prosperod", pruned = n, "retention swept old events")
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(target: "prosperod", error = %e, "retention prune failed")
                    }
                }
            }
        });
    }
```
(`Duration` is already imported as `std::time::Duration` in main.rs. `tokio::time::interval` ticks immediately on the first `tick().await`, so a sweep runs at startup then hourly.)

- [ ] **Step 4: Verify**
```bash
cargo test -p prospero-core prune_older_than_removes_aged_events 2>&1 | tail -4
cargo build -p prospero-daemon
cargo test -p prospero-daemon
cargo clippy -p prospero-daemon --all-targets -- -D warnings
cargo fmt --all -- --check
```
All pass.

- [ ] **Step 5: Commit**
```bash
git add crates/core/src/fleet.rs crates/daemon/src/main.rs
git commit -m "feat(daemon): --retention-days hourly prune loop (#4)"
```

---

## Task 3: Full gate

- [ ] **Step 1: Run the complete gate**
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```
Expected: all pass. Both backends drive `store_prune_conformance` — shared green is the parity proof.

- [ ] **Step 2: fmt fixup commit if needed**
```bash
cargo fmt --all
git diff --quiet || git commit -am "style: cargo fmt after retention"
```

---

## Self-Review Notes (for the implementer)

- **RFC-3339 sorts lexically** — `e.ts.as_str() >= before_ts` / `ts < ?` is a correct age cutoff because all timestamps are UTC `Z` RFC-3339. Do not parse timestamps for the comparison.
- **`JsonlStore::prune` rewrites the whole file** — acceptable for the dev/debug backend; the sqlite `DELETE` is the production path.
- **Retention is opt-in** — `--retention-days 0` (default) disables it entirely; no prune task is spawned.
- **`prune` is a hard delete** — pruned history is gone (the point of retention). It deletes by age regardless of whether an agent is still active; that's intended for first-stab retention.
- **Out of scope:** per-stream count caps, compaction/vacuum, partitioning — age-based `DELETE` only. `ConfigStore` is a separate plan (1c).

## Next Plan

- **Phase 1c** — `ConfigStore` trait + sqlite impl (Registry + per-repo provider config), sharing the DB; `FleetManager::new` becomes async to load config at startup.
