# Event-Store Phase 2a — Postgres Backends Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `PostgresStore` and `PostgresConfigStore` implementing the existing async `Store`/`ConfigStore` traits via sqlx (Postgres dialect), proving the portability claim by running the SAME `testkit` conformance batteries against Postgres. Postgres tests are gated on `DATABASE_URL`; CI runs a `postgres:17` service so they execute and stay covered.

**Architecture:** Parallel to the sqlite backends, with Postgres dialect differences: `$1..$N` placeholders (not `?`), `BIGSERIAL`/`BIGINT`, `PgPool`. The conformance batteries (`store_conformance`, `store_prune_conformance`, `config_store_conformance`) are dialect-agnostic — they only call trait methods — so the Postgres impls reuse them directly. A `#[cfg(any(test, feature="testkit"))] reset_for_tests` truncates between batteries (shared DB across test runs). `JsonlStore`/`SqliteStore` are untouched.

**Tech Stack:** Rust (edition 2024), tokio, sqlx (add `postgres` feature), async-trait, a `postgres:17` service in CI. Design source: `docs/superpowers/specs/2026-06-17-event-store-topology-design.md` §4 (Phase 2). Builds on Phase 1 (async traits + sqlite backends + conformance batteries), now on `main`.

**Local Postgres for verification:** a throwaway container is already running:
`DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test` (Postgres 17). Prefix Postgres test commands with it. Without it, the Postgres tests skip (and print SKIP) — they must still compile.

**Verification gate (`TESTKIT = --features prospero-core/testkit`):**
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test cargo test --workspace --features prospero-core/testkit
```

---

## File Structure

- **Modify** `Cargo.toml` (workspace) — add `postgres` to sqlx's feature list.
- **Create** `crates/core/src/postgres_store.rs` — `PostgresStore` + gated tests.
- **Create** `crates/core/src/postgres_config_store.rs` — `PostgresConfigStore` + gated tests.
- **Modify** `crates/core/src/lib.rs` — declare + re-export both.
- **Modify** `.github/workflows/ci.yml` — `postgres:17` service + `DATABASE_URL` on the `check` AND `coverage` jobs.

---

## Task 1: `PostgresStore`

**Files:** `Cargo.toml`, `crates/core/src/postgres_store.rs` (new), `crates/core/src/lib.rs`

- [ ] **Step 1: Add the `postgres` sqlx feature**

Workspace `Cargo.toml`, change the sqlx line's features to include `postgres`:
```toml
sqlx = { version = "0.8", default-features = false, features = ["runtime-tokio", "sqlite", "postgres"] }
```
Run `cargo build -p prospero-core` — still builds.

- [ ] **Step 2: Create `postgres_store.rs` with a gated failing test**

Create `crates/core/src/postgres_store.rs`:
```rust
//! sqlx-backed Postgres [`Store`] — the clustered-tier event backend.
//!
//! Mirrors [`crate::sqlite_store::SqliteStore`] with Postgres dialect (`$N`
//! placeholders, `BIGSERIAL`/`BIGINT`). Runs the same `testkit` conformance
//! batteries, gated on `DATABASE_URL` (skipped when unset). See spec §3/§4.

#[cfg(test)]
mod tests {
    use super::*;

    /// Connect to the test Postgres, or skip if `DATABASE_URL` is unset.
    async fn connect_or_skip() -> Option<PostgresStore> {
        let url = std::env::var("DATABASE_URL").ok()?;
        let store = PostgresStore::connect(&url).await.unwrap();
        store.reset_for_tests().await.unwrap();
        Some(store)
    }

    #[tokio::test]
    async fn postgres_store_satisfies_conformance() {
        let Some(store) = connect_or_skip().await else {
            eprintln!("SKIP postgres_store_satisfies_conformance: DATABASE_URL unset");
            return;
        };
        crate::testkit::store_conformance(&store).await;
        store.reset_for_tests().await.unwrap();
        crate::testkit::store_prune_conformance(&store).await;
    }
}
```
In `crates/core/src/lib.rs`: add `pub mod postgres_store;` (alphabetically — after `pub mod ownership;`, before `pub mod provider_env;`) and `pub use postgres_store::PostgresStore;`.
Run `DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test cargo test -p prospero-core postgres_store` → FAIL (PostgresStore undefined).

- [ ] **Step 3: Implement `PostgresStore`** (above the test module)
```rust
use async_trait::async_trait;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::error::{CoreError, Result};
use crate::event::FleetEvent;
use crate::store::Store;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS events (\
    global_ordinal BIGSERIAL PRIMARY KEY,\
    stream_key TEXT NOT NULL,\
    seq        BIGINT NOT NULL,\
    ts         TEXT NOT NULL,\
    repo       TEXT NOT NULL,\
    agent_id   TEXT NOT NULL,\
    kind       TEXT NOT NULL,\
    UNIQUE(stream_key, seq)\
)";

/// sqlx/Postgres-backed durable event store (clustered tier).
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Connect to Postgres at `url` and ensure the schema exists.
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .connect(url)
            .await
            .map_err(|e| CoreError::Store(format!("connecting to postgres: {e}")))?;
        sqlx::query(SCHEMA)
            .execute(&pool)
            .await
            .map_err(|e| CoreError::Store(format!("initializing postgres schema: {e}")))?;
        Ok(Self { pool })
    }

    /// Truncate all events. Test-only (resets between conformance batteries).
    #[cfg(any(test, feature = "testkit"))]
    pub async fn reset_for_tests(&self) -> Result<()> {
        sqlx::query("TRUNCATE events")
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("reset: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl Store for PostgresStore {
    async fn append(&self, event: &FleetEvent) -> Result<()> {
        let kind = serde_json::to_string(&event.kind)?;
        sqlx::query(
            "INSERT INTO events (stream_key, seq, ts, repo, agent_id, kind) \
             VALUES ($1, $2, $3, $4, $5, $6)",
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
             WHERE stream_key = $1 AND seq >= $2 ORDER BY seq",
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
        let row =
            sqlx::query("SELECT COALESCE(MAX(seq), 0) AS hw FROM events WHERE stream_key = $1")
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
        // Non-destructive write probe: a sentinel insert inside a rolled-back txn.
        let Ok(mut tx) = self.pool.begin().await else {
            return false;
        };
        let ok = sqlx::query(
            "INSERT INTO events (stream_key, seq, ts, repo, agent_id, kind) \
             VALUES ('__writable_probe__', -1, '', '', '', 'null')",
        )
        .execute(&mut *tx)
        .await
        .is_ok();
        let _ = tx.rollback().await;
        ok
    }

    async fn prune(&self, before_ts: &str) -> Result<u64> {
        let res = sqlx::query("DELETE FROM events WHERE ts < $1")
            .bind(before_ts)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("prune: {e}")))?;
        Ok(res.rows_affected())
    }
}
```
Run `DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test cargo test -p prospero-core postgres_store 2>&1 | tail -8` → the conformance test PASSES (runs both batteries against Postgres).
Also confirm the skip path: `cargo test -p prospero-core postgres_store 2>&1 | rg SKIP` (no DATABASE_URL) prints the SKIP line and the test passes (skips).

- [ ] **Step 4: Verify**
```bash
cargo clippy -p prospero-core --lib --features testkit -- -D warnings
cargo fmt --all -- --check
DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test cargo test -p prospero-core --features testkit 2>&1 | tail -6
```
All pass. If fmt diffs, `cargo fmt --all`.

- [ ] **Step 5: Commit**
```bash
git add Cargo.toml Cargo.lock crates/core/src/postgres_store.rs crates/core/src/lib.rs
git commit -m "feat(core): PostgresStore (clustered event backend, DATABASE_URL-gated tests)"
```

---

## Task 2: `PostgresConfigStore`

**Files:** `crates/core/src/postgres_config_store.rs` (new), `crates/core/src/lib.rs`

- [ ] **Step 1: Gated failing test**

Create `crates/core/src/postgres_config_store.rs`:
```rust
//! sqlx-backed Postgres [`ConfigStore`] — the clustered-tier config backend.
//!
//! Mirrors [`crate::config_store::SqliteConfigStore`] with Postgres dialect.
//! Runs the same conformance battery, gated on `DATABASE_URL`.

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn postgres_config_store_satisfies_conformance() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP postgres_config_store_satisfies_conformance: DATABASE_URL unset");
            return;
        };
        let store = PostgresConfigStore::connect(&url).await.unwrap();
        store.reset_for_tests().await.unwrap();
        crate::testkit::config_store_conformance(&store).await;
    }
}
```
In `lib.rs`: `pub mod postgres_config_store;` (after `pub mod postgres_store;`) and `pub use postgres_config_store::PostgresConfigStore;`.
Run gated → FAIL (undefined).

- [ ] **Step 2: Implement** (above the test module)
```rust
use async_trait::async_trait;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::error::{CoreError, Result};
use crate::config_store::ConfigStore;
use crate::registry::RegisteredRepo;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS repos (\
    name   TEXT PRIMARY KEY,\
    root   TEXT NOT NULL,\
    config TEXT NOT NULL\
)";

/// sqlx/Postgres-backed config store (clustered tier).
pub struct PostgresConfigStore {
    pool: PgPool,
}

impl PostgresConfigStore {
    /// Connect to Postgres at `url` and ensure the schema exists.
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .connect(url)
            .await
            .map_err(|e| CoreError::Store(format!("connecting to postgres: {e}")))?;
        sqlx::query(SCHEMA)
            .execute(&pool)
            .await
            .map_err(|e| CoreError::Store(format!("initializing config schema: {e}")))?;
        Ok(Self { pool })
    }

    /// Truncate all repos. Test-only.
    #[cfg(any(test, feature = "testkit"))]
    pub async fn reset_for_tests(&self) -> Result<()> {
        sqlx::query("TRUNCATE repos")
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("reset: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl ConfigStore for PostgresConfigStore {
    async fn list_repos(&self) -> Result<Vec<RegisteredRepo>> {
        let rows = sqlx::query("SELECT name, root, config FROM repos ORDER BY name")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("list_repos: {e}")))?;
        let mut repos = Vec::with_capacity(rows.len());
        for row in rows {
            let decode = |e: sqlx::Error| CoreError::Store(format!("list_repos decode: {e}"));
            let name: String = row.try_get("name").map_err(decode)?;
            let root: String = row.try_get("root").map_err(decode)?;
            let config_json: String = row.try_get("config").map_err(decode)?;
            repos.push(RegisteredRepo {
                name,
                root: root.into(),
                config: serde_json::from_str(&config_json)?,
            });
        }
        Ok(repos)
    }

    async fn upsert_repo(&self, repo: &RegisteredRepo) -> Result<()> {
        let config = serde_json::to_string(&repo.config)?;
        let root = repo
            .root
            .to_str()
            .ok_or_else(|| CoreError::Store(format!("non-UTF8 repo root path: {:?}", repo.root)))?;
        sqlx::query(
            "INSERT INTO repos (name, root, config) VALUES ($1, $2, $3) \
             ON CONFLICT (name) DO UPDATE SET root = excluded.root, config = excluded.config",
        )
        .bind(&repo.name)
        .bind(root)
        .bind(config)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("upsert_repo: {e}")))?;
        Ok(())
    }

    async fn delete_repo(&self, name: &str) -> Result<bool> {
        let res = sqlx::query("DELETE FROM repos WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("delete_repo: {e}")))?;
        Ok(res.rows_affected() > 0)
    }
}
```
Run gated test → PASS.

- [ ] **Step 3: Verify + Commit**
```bash
cargo clippy -p prospero-core --lib --features testkit -- -D warnings
cargo fmt --all -- --check
DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test cargo test -p prospero-core --features testkit postgres 2>&1 | tail -6
git add crates/core/src/postgres_config_store.rs crates/core/src/lib.rs
git commit -m "feat(core): PostgresConfigStore (clustered config backend)"
```

---

## Task 3: CI — `postgres:17` service + `DATABASE_URL`

**Files:** `.github/workflows/ci.yml`

- [ ] **Step 1: Add the service + env to the `check` job**

Under `jobs.check`, add a `services:` block (sibling of `steps:`) and set `DATABASE_URL` on the `cargo test` step. The service:
```yaml
    services:
      postgres:
        image: postgres:17
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: prospero_test
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
```
Change the `cargo test` step to set `DATABASE_URL` (so the gated Postgres tests run):
```yaml
      - name: cargo test
        if: steps.changes.outputs.code == 'true'
        env:
          DATABASE_URL: postgres://postgres:postgres@localhost:5432/prospero_test
        run: cargo test --workspace $TESTKIT
```

- [ ] **Step 2: Add the service + env to the `coverage` job**

The coverage gate (85% floor) runs the tests under llvm-cov; the Postgres code must be exercised there too or coverage drops. Add the SAME `services.postgres` block under `jobs.coverage`, and set `DATABASE_URL` on the `Run coverage gate` step:
```yaml
      - name: Run coverage gate
        if: needs.check.outputs.code == 'true'
        env:
          DATABASE_URL: postgres://postgres:postgres@localhost:5432/prospero_test
        run: scripts/coverage.sh
```

- [ ] **Step 3: Commit**
```bash
git add .github/workflows/ci.yml
git commit -m "ci: postgres:17 service + DATABASE_URL so Postgres backend tests run & cover"
```
(This can only be fully verified once pushed; locally, confirm the YAML is valid — e.g. `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))"`.)

---

## Task 4: Full gate (with local Postgres)

- [ ] **Step 1: Run the complete gate against the local container**
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test cargo test --workspace --features prospero-core/testkit
```
Expected: all pass, including the Postgres conformance tests (NOT skipped, since DATABASE_URL is set). The SAME batteries pass on sqlite and Postgres — that's the portability proof.

- [ ] **Step 2: Confirm the skip path is clean**
```bash
cargo test -p prospero-core --features testkit postgres 2>&1 | rg "SKIP|test result"
```
Without `DATABASE_URL`, the Postgres tests print SKIP and pass.

- [ ] **Step 3: fmt fixup commit if needed**
```bash
cargo fmt --all
git diff --quiet || git commit -am "style: cargo fmt after Postgres backends"
```

---

## Self-Review Notes (for the implementer)

- **Dialect differences vs sqlite:** `$1..$N` placeholders (not `?`), `BIGSERIAL` (not `AUTOINCREMENT`), `BIGINT` (not `INTEGER`). The query *structure* matches the sqlite impls; only dialect tokens differ.
- **`DATABASE_URL` gating:** tests skip (printing `SKIP`) when unset, so `cargo test` stays green on machines without Postgres. They MUST still compile. CI sets `DATABASE_URL` so they actually run.
- **`reset_for_tests`** is `#[cfg(any(test, feature="testkit"))]` — not in prod builds. Conformance assumes an empty store; truncate before each battery.
- **Coverage:** the gated tests must run in BOTH CI jobs (`check` + `coverage`) or the new Postgres code is uncovered and the 85% gate fails. That's why Task 3 touches both jobs.
- **Out of scope (later sub-phases):** `DistributedBus` / LISTEN-NOTIFY (2b), `LeasedOwnership` (2c), daemon standalone-vs-clustered selection (2d). 2a is ONLY the two backends + their gated tests + the CI service.

## Next (this branch)
2b `DistributedBus`, 2c `LeasedOwnership`, 2d daemon config-selection — all on `worktree-event-store-phase-2-clustered`, landing as one Phase 2 PR.
