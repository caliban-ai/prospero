# Event-Store Phase 1c — `ConfigStore` (registry on the shared DB) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the managed-repo registry (names → root + per-repo provider config) off `registry.json` and into the shared sqlite DB behind a `ConfigStore` trait, so standalone is fully on sqlite and the seam is ready for a shared Postgres config tier in Phase 2.

**Architecture:** A new `ConfigStore` trait (`list_repos` / `upsert_repo` / `delete_repo`) with a `SqliteConfigStore` backed by a `repos` table in the same `events.db`. `FleetManager` keeps its in-memory `Registry` cache but persists through `ConfigStore` instead of writing `registry.json`. `FleetManager::new(config, store)` stays 2-arg but becomes **async** and builds a default `SqliteConfigStore` from `config.data_dir`; an injection seam `FleetManager::with_config_store(config, store, config_store)` lets Phase 2 supply a Postgres-backed impl. A `testkit::config_store_conformance` battery proves backends behave identically.

**Tech Stack:** Rust (edition 2024), tokio, sqlx (sqlite), async-trait. Design source: `docs/superpowers/specs/2026-06-17-event-store-topology-design.md` §3.4, §5 (Phase 1). Builds on Phase 1b (sqlx/SqliteStore).

**Verification gate (`TESTKIT = --features prospero-core/testkit`):**
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```

---

## File Structure

- **Create** `crates/core/src/config_store.rs` — `ConfigStore` trait + `SqliteConfigStore` + tests.
- **Modify** `crates/core/src/lib.rs` — declare + re-export.
- **Modify** `crates/core/src/testkit.rs` — `config_store_conformance` battery.
- **Modify** `crates/core/src/fleet.rs` — `Inner.config_store`; async `new` + `with_config_store`; persist via `ConfigStore`; drop `registry_path`/`Registry::load`/`reg.save`; update the 7 in-file test call sites.
- **Modify** `crates/daemon/src/main.rs`, `crates/cli/tests/e2e_smoke.rs`, `crates/api/tests/api_integration.rs`, `crates/core/tests/fleet_integration.rs` — add `.await` to `FleetManager::new`.

`Registry` (the in-memory struct in `registry.rs`) and its file load/save are LEFT in place (still used by `registry.rs`'s own tests); `FleetManager` simply stops calling them.

---

## Task 1: `ConfigStore` trait + `SqliteConfigStore`

**Files:** `crates/core/src/config_store.rs` (new), `crates/core/src/lib.rs`, `crates/core/src/testkit.rs`

- [ ] **Step 1: Add the conformance battery + a failing test**

In `crates/core/src/testkit.rs`, append:
```rust
/// Contract every [`crate::config_store::ConfigStore`] must satisfy: upsert is
/// insert-or-update by name, list returns all repos (name-ordered), delete is
/// idempotent. Backends call this to prove identical config semantics.
pub async fn config_store_conformance(store: &dyn crate::config_store::ConfigStore) {
    use crate::registry::{RegisteredRepo, RepoProviderConfig};

    assert!(store.list_repos().await.unwrap().is_empty());

    let r = RegisteredRepo {
        name: "p".into(),
        root: "/r".into(),
        config: RepoProviderConfig {
            provider: Some("ollama".into()),
            ..Default::default()
        },
    };
    store.upsert_repo(&r).await.unwrap();
    let repos = store.list_repos().await.unwrap();
    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0].name, "p");
    assert_eq!(repos[0].root, std::path::PathBuf::from("/r"));
    assert_eq!(repos[0].config.provider.as_deref(), Some("ollama"));

    // Upsert updates in place (no duplicate).
    let mut r2 = r.clone();
    r2.config.provider = Some("anthropic".into());
    store.upsert_repo(&r2).await.unwrap();
    let repos = store.list_repos().await.unwrap();
    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0].config.provider.as_deref(), Some("anthropic"));

    // Delete is idempotent.
    assert!(store.delete_repo("p").await.unwrap());
    assert!(!store.delete_repo("p").await.unwrap());
    assert!(store.list_repos().await.unwrap().is_empty());
}
```

Create `crates/core/src/config_store.rs` with the module doc + test module only (fails until Step 2):
```rust
//! Mutable config records (the managed-repo registry) on the shared DB.
//!
//! Distinct from [`crate::store::Store`] because the access pattern is key-value
//! upsert/read, not append/replay. Standalone uses [`SqliteConfigStore`] in the
//! same `events.db`; a Postgres-backed impl drops in behind the trait in the
//! clustered tier (Phase 2). See the topology design spec §3.4.

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_config_store_satisfies_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteConfigStore::open(dir.path()).await.unwrap();
        crate::testkit::config_store_conformance(&store).await;
    }
}
```
In `crates/core/src/lib.rs`: add `pub mod config_store;` (alphabetically — after `pub mod caliband;`, before `pub mod discovery;`) and `pub use config_store::{ConfigStore, SqliteConfigStore};`.

Run `cargo test -p prospero-core sqlite_config_store` → FAIL (undefined).

- [ ] **Step 2: Implement the trait + `SqliteConfigStore`**

In `crates/core/src/config_store.rs`, above the test module:
```rust
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

use crate::error::{CoreError, Result};
use crate::registry::RegisteredRepo;

/// Durable, mutable store for the managed-repo registry.
#[async_trait]
pub trait ConfigStore: Send + Sync {
    /// All registered repos, ordered by name.
    async fn list_repos(&self) -> Result<Vec<RegisteredRepo>>;
    /// Insert or update a repo (keyed by `name`).
    async fn upsert_repo(&self, repo: &RegisteredRepo) -> Result<()>;
    /// Remove a repo by name. Returns whether a row was deleted.
    async fn delete_repo(&self, name: &str) -> Result<bool>;
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS repos (\
    name   TEXT PRIMARY KEY,\
    root   TEXT NOT NULL,\
    config TEXT NOT NULL\
)";

/// sqlx/sqlite-backed config store — shares `events.db` with the event store.
pub struct SqliteConfigStore {
    pool: SqlitePool,
}

impl SqliteConfigStore {
    /// Open (creating it + parent dirs if missing) the config store in
    /// `dir/events.db` (the same file the event store uses).
    pub async fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("events.db");
        let opts = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .connect_with(opts)
            .await
            .map_err(|e| CoreError::Store(format!("opening config store: {e}")))?;
        sqlx::query(SCHEMA)
            .execute(&pool)
            .await
            .map_err(|e| CoreError::Store(format!("initializing config schema: {e}")))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl ConfigStore for SqliteConfigStore {
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
        let root = repo.root.to_string_lossy().to_string();
        sqlx::query(
            "INSERT INTO repos (name, root, config) VALUES (?, ?, ?) \
             ON CONFLICT(name) DO UPDATE SET root = excluded.root, config = excluded.config",
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
        let res = sqlx::query("DELETE FROM repos WHERE name = ?")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("delete_repo: {e}")))?;
        Ok(res.rows_affected() > 0)
    }
}
```
Run `cargo test -p prospero-core sqlite_config_store` → PASS.

- [ ] **Step 3: Commit**
```bash
git add crates/core/src/config_store.rs crates/core/src/lib.rs crates/core/src/testkit.rs
git commit -m "feat(core): ConfigStore trait + SqliteConfigStore (registry on shared DB)"
```

---

## Task 2: Move `FleetManager` registry persistence onto `ConfigStore`

**Files:** `crates/core/src/fleet.rs`, `crates/daemon/src/main.rs`, `crates/cli/tests/e2e_smoke.rs`, `crates/api/tests/api_integration.rs`, `crates/core/tests/fleet_integration.rs`

- [ ] **Step 1: Add `config_store` to `Inner`; make `new` async + add `with_config_store`**

In `crates/core/src/fleet.rs`:
- Add the import: `use crate::config_store::{ConfigStore, SqliteConfigStore};`
- Add a field to `Inner` (after `registry`):
```rust
    registry: RwLock<Registry>,
    config_store: Arc<dyn ConfigStore>,
```
- Replace `FleetManager::new`. The current body loads the registry from a file and builds the snapshot. New version (builds the default `SqliteConfigStore`, delegates to `with_config_store`):
```rust
    /// Build a manager, loading the persisted registry from a default
    /// [`SqliteConfigStore`] in `config.data_dir` (the same dir as the event
    /// store). For an injected config backend (e.g. Postgres), use
    /// [`Self::with_config_store`].
    pub async fn new(config: FleetConfig, store: Arc<dyn Store>) -> Result<Self> {
        let config_store = Arc::new(SqliteConfigStore::open(&config.data_dir).await?);
        Self::with_config_store(config, store, config_store).await
    }

    /// Build a manager with an explicit [`ConfigStore`].
    pub async fn with_config_store(
        config: FleetConfig,
        store: Arc<dyn Store>,
        config_store: Arc<dyn ConfigStore>,
    ) -> Result<Self> {
        let registry = Registry {
            repos: config_store.list_repos().await?,
        };
        let bus: Arc<dyn EventBus> = Arc::new(InProcessBus::new(config.event_buffer));
        let emitter = Emitter {
            store,
            bus,
            seqs: Arc::new(AsyncMutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::default()),
        };
        let snapshot = FleetSnapshot {
            host: config.host.clone(),
            repos: registry
                .repos
                .iter()
                .map(|r| Repo {
                    name: r.name.clone(),
                    root: r.root.clone(),
                    health: RepoHealth::Healthy,
                    agents: Vec::new(),
                })
                .collect(),
        };
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                snapshot: RwLock::new(snapshot),
                registry: RwLock::new(registry),
                config_store,
                clients: Mutex::new(HashMap::new()),
                attached: Mutex::new(HashSet::new()),
                emitter,
                ownership: Arc::new(SelfOwnsAll),
                shutdown: watch::channel(false).0,
            }),
        })
    }
```
(This mirrors the existing `new` body exactly except: the registry comes from `config_store.list_repos()` instead of `Registry::load`, and `Inner` gains `config_store`. Keep the field order of `Inner` consistent with its struct definition.)

- DELETE the now-unused `FleetConfig::registry_path` method (around `fleet.rs:165`). `FleetConfig.data_dir` stays (it's where the default config store + the daemon's event store live).

- [ ] **Step 2: Persist mutations via `ConfigStore` instead of `reg.save`**

Three methods currently call `reg.save(&self.inner.config.registry_path())?`. Replace each:

`add_repo_with_config` — after `reg.set_config(&name, config);`, replace the `reg.save(...)?;` line with an upsert of the freshly-registered repo (still inside the `registry.write()` scope; clone the record out to persist):
```rust
            reg.add(name.clone(), root.clone())?;
            reg.set_config(&name, config);
            let repo = reg
                .get(&name)
                .cloned()
                .expect("repo just inserted must exist");
            self.inner.config_store.upsert_repo(&repo).await?;
```

`remove_repo` — replace `reg.save(...)?;` with:
```rust
            let removed = reg.remove(name);
            if removed {
                self.inner.config_store.delete_repo(name).await?;
            }
```
(Keep the surrounding `removed` flow that returns it; only the persistence line changes.)

`set_repo_config_registry_only` — replace `reg.save(...)?;` with:
```rust
        if !reg.set_config(repo, config) {
            return Err(CoreError::RepoNotFound(repo.to_string()));
        }
        let record = reg
            .get(repo)
            .cloned()
            .expect("repo exists after successful set_config");
        self.inner.config_store.upsert_repo(&record).await?;
```

(All three already run inside `async fn`s that hold `self.inner.registry.write().await`; the `tokio::sync::RwLock` write guard may be held across these `.await`s. Keep the upsert/delete inside the existing lock scope so the cache and the durable store stay consistent.)

- [ ] **Step 3: Update the 7 in-file `FleetManager::new` call sites**

In `crates/core/src/fleet.rs` tests, add `.await` to every `FleetManager::new(config, store).unwrap()` → `FleetManager::new(config, store).await.unwrap()`. There are 7 (the tests at the lines that build a manager: `restart_caliband_*`, `send_agent_input_*`, `spawn_passes_*`, `ensure_config_for_*`, `run_drains_*`, `ownership_gates_*`, `prune_older_than_*`). They are all `#[tokio::test]`, so `.await` is valid.

Run `cargo test -p prospero-core --features testkit 2>&1 | tail -10` → all pass (config now persists to sqlite; the in-memory behavior is unchanged).

- [ ] **Step 4: Update the remaining `FleetManager::new` call sites (other crates)**

Add `.await` in each (all are in async fns):
- `crates/daemon/src/main.rs:101`: `let manager = FleetManager::new(config, store).await.with_context(|| "building fleet manager")?;`
- `crates/cli/tests/e2e_smoke.rs:56`: `... = FleetManager::new(config, store).await.unwrap();`
- `crates/api/tests/api_integration.rs` lines 76, 171, 428: `... .await.unwrap();`
- `crates/core/tests/fleet_integration.rs` lines 65, 341: `... .await.unwrap();`

- [ ] **Step 5: Verify the full workspace**
```bash
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit 2>&1 | tail -12
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo fmt --all -- --check
```
All pass. If clippy flags `await_holding_lock` on the registry `RwLock`, note it: this is a `tokio::sync::RwLock` (async-aware), not std — `await_holding_lock` only fires for `std`/`parking_lot` guards, so it should NOT trigger. If it does, the registry lock is the wrong type — re-check it's `tokio::sync::RwLock`. If fmt diffs, run `cargo fmt --all`.

- [ ] **Step 6: Commit**
```bash
git add crates/core/src/fleet.rs crates/daemon/src/main.rs crates/cli/tests/e2e_smoke.rs crates/api/tests/api_integration.rs crates/core/tests/fleet_integration.rs
git commit -m "feat(core): persist managed-repo registry via ConfigStore (sqlite)"
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
Expected: all pass.

- [ ] **Step 2: fmt fixup commit if needed**
```bash
cargo fmt --all
git diff --quiet || git commit -am "style: cargo fmt after ConfigStore"
```

---

## Self-Review Notes (for the implementer)

- **`registry.json` is gone for the daemon.** Repo config now lives in `events.db`'s `repos` table (fresh start — an existing `registry.json` is left on disk, unread). Do NOT add a registry.json→sqlite importer.
- **`Registry` (the struct) stays.** It's still the in-memory cache and is still tested in `registry.rs`. Only `FleetManager`'s *file* persistence (`Registry::load`/`reg.save`/`registry_path`) is removed; do not delete `Registry::load`/`save` themselves.
- **`new` is now async** — every call site gains `.await`. There are 14 total (1 daemon, 1 cli test, 3 api tests, 2 core integration tests, 7 fleet unit tests).
- **The injection seam** is `with_config_store`; `new` is the convenience that builds the sqlite default. Phase 2 supplies a Postgres `ConfigStore` via `with_config_store`.
- **Shared DB:** `SqliteConfigStore` opens the SAME `events.db` as `SqliteStore` (different table: `repos`). WAL + busy_timeout handle concurrent access.
- **Out of scope:** Postgres `ConfigStore` (Phase 2), Gonzalo, any registry.json migration.

## This completes Phase 1.
After 1c, Phase 1 (1a async, 1b SqliteStore, 1c ConfigStore, 1d retention) is done. Next is Phase 2 (Postgres `PostgresStore` + `PostgresConfigStore`, `DistributedBus`, `LeasedOwnership`).
