# Event-Store Phase 1b — sqlite `Store` via `sqlx` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `sqlx`-backed `SqliteStore` implementing the async `Store` trait (indexed `(stream_key, seq)` replay, `global_ordinal` as the rowid), make it the `prosperod` default backend (fresh start — existing `events.jsonl` is left untouched and unread), and promote the `Store` conformance battery into `testkit` so both backends prove parity.

**Architecture:** A new `SqliteStore` (`crates/core/src/sqlite_store.rs`) holds a `sqlx::SqlitePool`. One `events` table: `(global_ordinal INTEGER PK AUTOINCREMENT, stream_key, seq, ts, repo, agent_id, kind TEXT)` with `UNIQUE(stream_key, seq)` (which is also the replay index). `append`→INSERT, `replay`→indexed range scan, `high_water`→`MAX(seq)`, `writable`→a rolled-back write probe. The SQL is written with `sqlx`'s runtime query API (no compile-time `DATABASE_URL`/macros) so the same queries port to Postgres in Phase 2. `JsonlStore` stays as the dev/test backend.

**Tech Stack:** Rust (edition 2024), tokio, `sqlx` (new dep: `runtime-tokio` + `sqlite`), `async-trait`. Design source: `docs/superpowers/specs/2026-06-17-event-store-topology-design.md` §3.1, §4 (Phase 1). Builds on Phase 1a (async `Store`).

**Verification gate (`TESTKIT = --features prospero-core/testkit`):**
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```

---

## File Structure

- **Modify** `Cargo.toml` (workspace) — add `sqlx` to `[workspace.dependencies]`.
- **Modify** `crates/core/Cargo.toml` — depend on `sqlx`.
- **Create** `crates/core/src/sqlite_store.rs` — `SqliteStore` (pool, schema, async `Store` impl) + its tests.
- **Modify** `crates/core/src/lib.rs` — declare + re-export `SqliteStore`.
- **Modify** `crates/core/src/testkit.rs` — add `pub async fn store_conformance(&dyn Store)` (promoted from `store.rs`).
- **Modify** `crates/core/src/store.rs` — remove the local `store_conformance`; the `jsonl_store_satisfies_conformance` test now calls `crate::testkit::store_conformance`.
- **Modify** `crates/daemon/src/main.rs` — construct `SqliteStore` as the default backend.

---

## Task 1: `SqliteStore` + conformance promotion

**Files:** `Cargo.toml`, `crates/core/Cargo.toml`, `crates/core/src/sqlite_store.rs` (new), `crates/core/src/lib.rs`, `crates/core/src/testkit.rs`, `crates/core/src/store.rs`

- [ ] **Step 1: Add the `sqlx` dependency**

Workspace root `Cargo.toml`, under `[workspace.dependencies]`:
```toml
sqlx = { version = "0.8", default-features = false, features = ["runtime-tokio", "sqlite"] }
```
`crates/core/Cargo.toml`, under `[dependencies]`:
```toml
sqlx = { workspace = true }
```
Run `cargo build -p prospero-core` — still builds (dep unused so far; warning OK).

- [ ] **Step 2: Promote `store_conformance` into `testkit`**

In `crates/core/src/testkit.rs`, add at the end of the file (it's already gated `#[cfg(any(test, feature = "testkit"))]` at the module level via `lib.rs`):
```rust
/// The behavioral contract every [`crate::store::Store`] must satisfy. Backends
/// (jsonl, sqlite, Postgres) call this with a freshly-opened, empty store to
/// prove parity — so a new backend is correct by construction, not by hope.
pub async fn store_conformance(store: &dyn crate::store::Store) {
    use crate::event::{EventKind, FleetEvent, OutputStream};

    fn ev(seq: u64, agent: &str, chunk: &str) -> FleetEvent {
        FleetEvent {
            seq,
            ts: "t".into(),
            repo: "r".into(),
            agent_id: agent.into(),
            kind: EventKind::Output {
                stream: OutputStream::Stdout,
                chunk: chunk.into(),
            },
        }
    }

    // Empty store.
    assert_eq!(store.high_water("a").await.unwrap(), 0);
    assert!(store.replay("a", 0).await.unwrap().is_empty());
    assert!(store.writable().await);

    // Per-stream ordered + isolated appends.
    store.append(&ev(1, "a", "a1")).await.unwrap();
    store.append(&ev(1, "b", "b1")).await.unwrap();
    store.append(&ev(2, "a", "a2")).await.unwrap();

    assert_eq!(store.high_water("a").await.unwrap(), 2);
    assert_eq!(store.high_water("b").await.unwrap(), 1);

    let a = store.replay("a", 0).await.unwrap();
    assert_eq!(a.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2]);
    // from_seq is an inclusive lower bound.
    let a_from2 = store.replay("a", 2).await.unwrap();
    assert_eq!(a_from2.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![2]);
    // Stream isolation.
    let b = store.replay("b", 0).await.unwrap();
    assert_eq!(b.len(), 1);
}
```

In `crates/core/src/store.rs` `tests` module: DELETE the local `async fn store_conformance(...)` definition, and change `jsonl_store_satisfies_conformance` to call the promoted one:
```rust
    #[tokio::test]
    async fn jsonl_store_satisfies_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        crate::testkit::store_conformance(&store).await;
    }
```
(Keep the module's other tests and its local `ev(seq, agent, chunk)` helper — they're still used.)

- [ ] **Step 3: Write the failing `SqliteStore` test**

Create `crates/core/src/sqlite_store.rs` with ONLY the test module first, so it fails to compile (no `SqliteStore` yet):
```rust
//! sqlx-backed sqlite [`Store`] — the default `prosperod` backend.
//!
//! One `events` table; `global_ordinal` (the rowid) records durable insertion
//! order for future fleet-wide queries, while consumers see the per-stream
//! `seq`. The SQL uses sqlx's runtime query API (no compile-time `DATABASE_URL`)
//! so the same statements port to Postgres in Phase 2.

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(seq: u64, agent: &str) -> crate::event::FleetEvent {
        crate::event::FleetEvent {
            seq,
            ts: "t".into(),
            repo: "r".into(),
            agent_id: agent.into(),
            kind: crate::event::EventKind::AgentSpawned,
        }
    }

    #[tokio::test]
    async fn sqlite_store_satisfies_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path()).await.unwrap();
        crate::testkit::store_conformance(&store).await;
    }

    #[tokio::test]
    async fn reopen_resumes_per_stream_high_water() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = SqliteStore::open(dir.path()).await.unwrap();
            s.append(&ev(5, "a")).await.unwrap();
        }
        let s = SqliteStore::open(dir.path()).await.unwrap();
        assert_eq!(s.high_water("a").await.unwrap(), 5);
    }

    #[tokio::test]
    async fn duplicate_stream_seq_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let s = SqliteStore::open(dir.path()).await.unwrap();
        s.append(&ev(1, "a")).await.unwrap();
        // Same (stream_key, seq) violates the UNIQUE backstop.
        assert!(s.append(&ev(1, "a")).await.is_err());
    }
}
```
Declare the module: in `crates/core/src/lib.rs` add `pub mod sqlite_store;` (alphabetically — immediately before `pub mod store;`) and `pub use sqlite_store::SqliteStore;` near the other re-exports.

Run `cargo test -p prospero-core sqlite_store` → FAIL to compile (`SqliteStore` undefined).

- [ ] **Step 4: Implement `SqliteStore`**

In `crates/core/src/sqlite_store.rs`, above the `#[cfg(test)] mod tests`, add:
```rust
use std::path::Path;

use async_trait::async_trait;
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

use crate::error::{CoreError, Result};
use crate::event::FleetEvent;
use crate::store::Store;

/// One-table schema. `global_ordinal` (rowid) is durable insertion order;
/// `UNIQUE(stream_key, seq)` is the per-stream monotonicity backstop and also
/// the index `replay` scans.
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS events (\
    global_ordinal INTEGER PRIMARY KEY AUTOINCREMENT,\
    stream_key TEXT NOT NULL,\
    seq        INTEGER NOT NULL,\
    ts         TEXT NOT NULL,\
    repo       TEXT NOT NULL,\
    agent_id   TEXT NOT NULL,\
    kind       TEXT NOT NULL,\
    UNIQUE(stream_key, seq)\
)";

/// sqlx/sqlite-backed durable event store.
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    /// Open (creating it + parent dirs if missing) the store at `dir/events.db`.
    pub async fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("events.db");
        let opts = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .connect_with(opts)
            .await
            .map_err(|e| CoreError::Store(format!("opening sqlite store: {e}")))?;
        sqlx::query(SCHEMA)
            .execute(&pool)
            .await
            .map_err(|e| CoreError::Store(format!("initializing sqlite schema: {e}")))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl Store for SqliteStore {
    async fn append(&self, event: &FleetEvent) -> Result<()> {
        let kind = serde_json::to_string(&event.kind)?;
        sqlx::query(
            "INSERT INTO events (stream_key, seq, ts, repo, agent_id, kind) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(event.stream_key())
        .bind(event.seq as i64)
        .bind(&event.ts)
        .bind(&event.repo)
        .bind(&event.agent_id)
        .bind(kind)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("append: {e}")))?;
        Ok(())
    }

    async fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
        let rows = sqlx::query(
            "SELECT seq, ts, repo, agent_id, kind FROM events \
             WHERE stream_key = ? AND seq >= ? ORDER BY seq",
        )
        .bind(stream_key)
        .bind(from_seq as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("replay: {e}")))?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let decode = |e: sqlx::Error| CoreError::Store(format!("replay decode: {e}"));
            let seq: i64 = row.try_get("seq").map_err(decode)?;
            let ts: String = row.try_get("ts").map_err(decode)?;
            let repo: String = row.try_get("repo").map_err(decode)?;
            let agent_id: String = row.try_get("agent_id").map_err(decode)?;
            let kind_json: String = row.try_get("kind").map_err(decode)?;
            events.push(FleetEvent {
                seq: seq as u64,
                ts,
                repo,
                agent_id,
                kind: serde_json::from_str(&kind_json)?,
            });
        }
        Ok(events)
    }

    async fn high_water(&self, stream_key: &str) -> Result<u64> {
        let row = sqlx::query("SELECT COALESCE(MAX(seq), 0) AS hw FROM events WHERE stream_key = ?")
            .bind(stream_key)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("high_water: {e}")))?;
        let hw: i64 = row
            .try_get("hw")
            .map_err(|e| CoreError::Store(format!("high_water decode: {e}")))?;
        Ok(hw as u64)
    }

    async fn writable(&self) -> bool {
        // Non-destructive write probe: a schema write inside a rolled-back
        // transaction. Detects a read-only / full store the way `append` would,
        // without persisting anything.
        match self.pool.begin().await {
            Ok(mut tx) => {
                let ok = sqlx::query("CREATE TABLE IF NOT EXISTS _writable_probe (x INTEGER)")
                    .execute(&mut *tx)
                    .await
                    .is_ok();
                let _ = tx.rollback().await;
                ok
            }
            Err(_) => false,
        }
    }
}
```

Run:
```bash
cargo test -p prospero-core sqlite_store 2>&1 | tail -15
```
Expected: PASS — `sqlite_store_satisfies_conformance`, `reopen_resumes_per_stream_high_water`, `duplicate_stream_seq_is_rejected`.

- [ ] **Step 5: Verify the core crate is green**
```bash
cargo test -p prospero-core --features testkit 2>&1 | tail -8
cargo clippy -p prospero-core --lib --features testkit -- -D warnings
```
Expected: all pass; clippy clean. (The jsonl conformance test now also drives the promoted `testkit::store_conformance`.)

- [ ] **Step 6: Commit**
```bash
git add Cargo.toml crates/core/Cargo.toml crates/core/src/sqlite_store.rs crates/core/src/lib.rs crates/core/src/testkit.rs crates/core/src/store.rs
git commit -m "feat(core): sqlx-backed SqliteStore + promote Store conformance to testkit"
```

---

## Task 2: Make `SqliteStore` the daemon default

**Files:** `crates/daemon/src/main.rs`

- [ ] **Step 1: Swap the backend construction**

In `crates/daemon/src/main.rs`:
- Change the import `use prospero_core::store::JsonlStore;` to `use prospero_core::store::SqliteStore;` — NOTE: confirm the actual import path; `SqliteStore` is re-exported at `prospero_core::SqliteStore` and `prospero_core::sqlite_store::SqliteStore`. Use `use prospero_core::sqlite_store::SqliteStore;`.
- Replace the store construction (currently `let store = Arc::new(JsonlStore::open(&data_dir).with_context(|| "opening event store")?);`) with:
```rust
    let store = Arc::new(
        SqliteStore::open(&data_dir)
            .await
            .with_context(|| "opening event store")?,
    );
```
(`main` is already `async`, so `.await` is fine; `with_context` works because `CoreError` is `std::error::Error`. If the `?`/`with_context` chain complains, map first: `.map_err(anyhow::Error::from)` then `.with_context(...)` — but `with_context` on a `Result<_, CoreError>` should work since `anyhow::Context` is impl'd for any `E: Error + Send + Sync + 'static`.)

- [ ] **Step 2: Verify the daemon builds + the binary boots a sqlite store**
```bash
cargo build -p prospero-daemon
cargo test -p prospero-daemon
```
Expected: builds and its unit test passes. (No behavior test for the boot path here; the integration suites use `JsonlStore` directly and are unaffected.)

- [ ] **Step 3: Commit**
```bash
git add crates/daemon/src/main.rs
git commit -m "feat(daemon): default the event store to SqliteStore (fresh start)"
```

---

## Task 3: Full gate + parity check

- [ ] **Step 1: Run the complete gate**
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```
Expected: all pass. Both `jsonl_store_satisfies_conformance` and `sqlite_store_satisfies_conformance` drive the SAME `testkit::store_conformance` battery — that shared green is the parity proof.

- [ ] **Step 2: Commit any fmt fixup**
```bash
cargo fmt --all
git diff --quiet || git commit -am "style: cargo fmt after SqliteStore"
```

---

## Self-Review Notes (for the implementer)

- **Fresh start, no migration.** The daemon now writes `events.db`; an existing `events.jsonl` is left on disk and ignored. Do NOT add a jsonl→sqlite importer (a deliberate decision; a migration utility can come later).
- **`JsonlStore` stays.** It remains the backend for all existing integration/e2e tests and as a debug backend. Do not remove it.
- **No `sqlx::query!` macros.** Use the runtime API (`sqlx::query`, `row.try_get`) so there's no compile-time `DATABASE_URL` requirement and the SQL ports to Postgres in Phase 2.
- **`global_ordinal` is write-only for now.** The column exists (rowid) for future fleet-wide/timeline reads (#5); no read path consumes it yet — that's intentional, not a gap.
- **`writable` probe** creates+rolls back a throwaway table to exercise the write path. It never persists `_writable_probe`.
- **Out of scope (later phases):** `ConfigStore` (1c), retention/`prune` (1d), `PostgresStore`/`DistributedBus`/`LeasedOwnership` (Phase 2), exposing `global_ordinal` to queries (#5).

## Next Plans

- **Phase 1c** — `ConfigStore` trait + sqlite impl (Registry + per-repo provider config), sharing the DB; migrate `FleetManager`'s registry persistence behind it.
- **Phase 1d** — retention (#4): a `Store::prune(before_ts)` / age-based `DELETE`, wired to a daemon retention config.
