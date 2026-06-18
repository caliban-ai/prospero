# Event Store Phase 2c — `LeasedOwnership` (Postgres lease + heartbeat) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax. Work ONLY in the worktree at `.claude/worktrees/event-store-phase-2-clustered` on branch `worktree-event-store-phase-2-clustered`. Do NOT `git checkout` any other branch or commit (detached-HEAD hazard).

**Goal:** Make the `Ownership` seam async and add the clustered `LeasedOwnership` — a Postgres lease row per active stream (`try_acquire` = INSERT … ON CONFLICT WHERE expired/ours, `renew` heartbeat, `release` = delete) with epoch fencing — so exactly one replica writes a stream and a dead owner's streams are taken over after the lease TTL.

**Architecture:** `try_acquire`/`renew`/`release` become `async` (Postgres I/O); `owns` stays sync (an in-memory check on the hot poll path). `SelfOwnsAll` is unchanged in behaviour. `LeasedOwnership` keeps a `held: Mutex<HashMap<stream_key, epoch>>` mirror of the leases this replica holds; `owns` reads it, `heartbeat()` renews every held lease and drops any it has lost. The shared poll loop is the reaper/takeover: `try_acquire` succeeds on an expired lease, so a polled-but-unowned agent is adopted within a tick (no separate reaper). `FleetManager::start_attach` is reworked to fix the two inline-documented Phase-2 hazards: idempotent re-acquire (don't fail/steal from yourself) and never `release` a stream an already-running local task still drives.

**Tech Stack:** Rust (edition 2024, tokio), `sqlx` (Postgres, DB-clock `now()` + `make_interval` so lease expiry is immune to per-replica clock skew), `async-trait`.

**Spec:** `docs/superpowers/specs/2026-06-17-event-store-topology-design.md` §3.3 (`Ownership` — leases, reaper, failover, fencing).

---

## File Structure

- **Modify** `crates/core/src/ownership.rs` — async trait (`try_acquire`/`renew`/`release` async; `owns` sync); async `SelfOwnsAll`.
- **Modify** `crates/core/src/fleet.rs` — `.await` the now-async ownership calls; rework `start_attach` (hazard fixes); add a `with_seams(config, store, config_store, bus, ownership)` constructor that `with_config_store` delegates to with the standalone defaults; add a refusing-ownership gating test.
- **Create** `crates/core/src/leased_ownership.rs` — `LeasedOwnership` + DATABASE_URL-gated tests.
- **Modify** `crates/core/src/lib.rs` — export `LeasedOwnership`.

---

## Task 1: Async `Ownership` seam + `start_attach` hazard fixes + injection seam

Changing the trait to async breaks `fleet.rs`, so this task updates every consumer in one go (the whole workspace must compile green). `Ownership` is used only inside `crates/core` (no api/cli/daemon references), so the ripple is contained to `ownership.rs` + `fleet.rs`.

**Files:**
- Modify: `crates/core/src/ownership.rs`
- Modify: `crates/core/src/fleet.rs` (the `use` at :26, the `with_config_store` constructor ~:309, `start_attach` ~:794, the attach-task cleanup ~:843)

- [ ] **Step 1: Make the `Ownership` trait async in `crates/core/src/ownership.rs`**

Replace the trait + `SelfOwnsAll` impl (keep the file's module doc and the `Lease` struct as-is). `try_acquire`/`renew`/`release` become async; `owns` stays sync (it backs the hot poll-loop reconciliation and `LeasedOwnership` answers it from an in-memory mirror).

```rust
use async_trait::async_trait;

use crate::error::Result;

// ... (Lease struct unchanged) ...

/// Single-writer ownership of streams.
#[async_trait]
pub trait Ownership: Send + Sync {
    /// Claim `stream_key` if it is free, expired, or already held by THIS
    /// process (idempotent — re-acquiring your own live lease returns it and
    /// does not change its epoch). Returns `None` if another live replica owns
    /// it.
    async fn try_acquire(&self, stream_key: &str) -> Option<Lease>;

    /// Extend a held lease. `Err` if the lease was lost (stolen/expired) — which
    /// is how a replica learns it is no longer the owner.
    async fn renew(&self, lease: &Lease) -> Result<()>;

    /// Release a held stream so a peer may claim it immediately (graceful
    /// hand-off), rather than waiting for TTL expiry.
    async fn release(&self, stream_key: &str);

    /// Whether this process currently owns `stream_key`. Cheap/in-memory: it is
    /// consulted on the poll loop's hot path.
    fn owns(&self, stream_key: &str) -> bool;
}

/// Standalone ownership: this process owns every stream unconditionally.
pub struct SelfOwnsAll;

#[async_trait]
impl Ownership for SelfOwnsAll {
    async fn try_acquire(&self, stream_key: &str) -> Option<Lease> {
        Some(Lease {
            stream_key: stream_key.to_string(),
            epoch: 0,
        })
    }
    async fn renew(&self, _lease: &Lease) -> Result<()> {
        Ok(())
    }
    async fn release(&self, _stream_key: &str) {}
    fn owns(&self, _stream_key: &str) -> bool {
        true
    }
}
```

Update the existing `self_owns_all_always_acquires_and_owns` test to `#[tokio::test]` and `.await` the async calls:

```rust
    #[tokio::test]
    async fn self_owns_all_always_acquires_and_owns() {
        let o = SelfOwnsAll;
        let lease = o.try_acquire("a1").await.expect("standalone always acquires");
        assert_eq!(lease.stream_key, "a1");
        assert_eq!(lease.epoch, 0);
        assert!(o.owns("a1"));
        assert!(o.owns("anything-else"));
        o.renew(&lease).await.unwrap();
        o.release("a1").await; // no-op, must not panic
    }
```

- [ ] **Step 2: Add the `with_seams` injection constructor in `crates/core/src/fleet.rs`**

`with_config_store` currently hardcodes `InProcessBus` + `SelfOwnsAll`. Extract a `with_seams` that takes both as injected `Arc`s (Phase 2d wires the clustered `DistributedBus` + `LeasedOwnership` through it), and have `with_config_store` delegate with the standalone defaults. Replace the body of `with_config_store` (the part from building `bus` onward) so it becomes:

```rust
    /// Build a manager with an explicit [`ConfigStore`] and the standalone
    /// `EventBus`/`Ownership` seams.
    pub async fn with_config_store(
        config: FleetConfig,
        store: Arc<dyn Store>,
        config_store: Arc<dyn ConfigStore>,
    ) -> Result<Self> {
        let bus: Arc<dyn EventBus> = Arc::new(InProcessBus::new(config.event_buffer));
        let ownership: Arc<dyn Ownership> = Arc::new(SelfOwnsAll);
        Self::with_seams(config, store, config_store, bus, ownership).await
    }

    /// Build a manager with every topology seam injected. Standalone passes
    /// `InProcessBus` + `SelfOwnsAll`; clustered (Phase 2d) passes
    /// `DistributedBus` + `LeasedOwnership`.
    pub async fn with_seams(
        config: FleetConfig,
        store: Arc<dyn Store>,
        config_store: Arc<dyn ConfigStore>,
        bus: Arc<dyn EventBus>,
        ownership: Arc<dyn Ownership>,
    ) -> Result<Self> {
        let registry = Registry {
            repos: config_store.list_repos().await?,
        };
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
                ownership,
                shutdown: watch::channel(false).0,
            }),
        })
    }
```

(Keep the existing `new` delegating to `with_config_store` unchanged.)

- [ ] **Step 3: Rework `start_attach` to fix the two ownership hazards**

Replace `start_attach` (~`crates/core/src/fleet.rs:794`). The fixes: (1) consult the local `attached` set first so an already-running task is left alone (its lease stands); (2) `try_acquire` is idempotent, so a lost insert race must NOT `release` (the winner holds the same lease); (3) the attach task releases the lease on exit for prompt failover hand-off.

```rust
    async fn start_attach(&self, repo: &str, agent_id: &str, client: CalibandClient) {
        // Already driving this agent locally? Its attach task holds (and, in
        // clustered mode, heartbeats) the lease — leave it untouched.
        if self.inner.attached.lock().unwrap().contains(agent_id) {
            return;
        }
        // Claim the stream. Standalone always acquires; clustered consults the
        // Postgres lease and returns `None` if another live replica owns it.
        // `try_acquire` is idempotent for a stream THIS process already holds.
        if self.inner.ownership.try_acquire(agent_id).await.is_none() {
            return;
        }
        {
            let mut attached = self.inner.attached.lock().unwrap();
            if !attached.insert(agent_id.to_string()) {
                // Lost a race to another start_attach for the same agent. It now
                // owns the (idempotently-shared) lease and will release it on
                // exit — we must NOT release here or we would orphan its writer.
                return;
            }
        }
        let repo = repo.to_string();
        let agent_id = agent_id.to_string();
        let emitter = self.inner.emitter.clone();
        let normalize = self.inner.config.normalize;
        let backoff = self.inner.config.attach_backoff;
        let mut shutdown = self.inner.shutdown.subscribe();
        let attached = self.inner.clone();

        tokio::spawn(async move {
            let result = attach_loop(
                &client,
                &repo,
                &agent_id,
                &emitter,
                normalize,
                backoff,
                &mut shutdown,
            )
            .await;
            if let Err(e) = result {
                tracing::warn!(
                    target: "prospero_fleet",
                    %repo, %agent_id, error = %e,
                    "attach task ended with error"
                );
            }
            attached.attached.lock().unwrap().remove(&agent_id);
            // Release for prompt failover hand-off (clustered); no-op standalone.
            attached.ownership.release(&agent_id).await;
        });
    }
```

> Note on renewal: a `LeasedOwnership` lease is kept alive by the daemon's periodic `heartbeat()` (wired in Phase 2d), not by the attach task — so the lease is intentionally not threaded into the task here. The old inline "thread the Lease into the attach task" note is superseded by the centralized heartbeat.

- [ ] **Step 4: Add a gating test with a refusing `Ownership` double**

Add to `fleet.rs`'s `#[cfg(test)] mod tests` (near `ownership_gates_the_attach_path`). It proves that when ownership refuses, the agent is NOT attached — exercising the clustered "another replica owns it" path via `with_seams`:

```rust
    #[tokio::test]
    async fn refused_ownership_blocks_the_attach_path() {
        use crate::bus::InProcessBus;
        use crate::ownership::{Lease, Ownership};
        use crate::testkit::FakeCaliband;
        use async_trait::async_trait;

        // Ownership that never grants a lease (simulates a peer-owned stream).
        struct NeverOwns;
        #[async_trait]
        impl Ownership for NeverOwns {
            async fn try_acquire(&self, _: &str) -> Option<Lease> {
                None
            }
            async fn renew(&self, _: &Lease) -> crate::error::Result<()> {
                Ok(())
            }
            async fn release(&self, _: &str) {}
            fn owns(&self, _: &str) -> bool {
                false
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
        config.ensure.autostart = false;
        let root = dir.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        let socket = crate::discovery::resolve_socket(&root, &config.discovery_env).unwrap();
        let _fake = FakeCaliband::start_at(&socket).await.unwrap();

        let store: Arc<dyn Store> = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let config_store: Arc<dyn ConfigStore> =
            Arc::new(crate::config_store::SqliteConfigStore::open(dir.path()).await.unwrap());
        let bus: Arc<dyn EventBus> = Arc::new(InProcessBus::new(config.event_buffer));
        let ownership: Arc<dyn Ownership> = Arc::new(NeverOwns);
        let mgr = FleetManager::with_seams(config, store, config_store, bus, ownership)
            .await
            .unwrap();
        mgr.add_repo("p", &root).await.unwrap();

        let id = mgr.spawn_agent("p", SpawnRequest::new("hi")).await.unwrap();
        assert!(
            !mgr.is_attached(&id),
            "peer-owned agent must NOT be attached locally"
        );
    }
```

(If `SqliteConfigStore::open`'s exact path/signature differs, mirror what `FleetManager::new` constructs for the default config store — check the top of the `new` constructor.)

- [ ] **Step 5: Build + test the workspace**

```bash
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```

Expected: PASS (no DATABASE_URL needed — Task 1 adds no Postgres tests). Then `cargo fmt --all` + `cargo fmt --all -- --check` clean and `cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings` clean.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(core): async Ownership seam + start_attach hazard fixes

Make Ownership::{try_acquire,renew,release} async (owns stays sync for the hot
poll path) so a Postgres-backed lease can drop in. Rework start_attach to fix
the two Phase-2 hazards it documented: it consults the local attached set
first (an already-running task keeps its lease), relies on idempotent
try_acquire instead of acquire-then-release, and releases the lease on task
exit for prompt failover hand-off. Add a with_seams() constructor injecting the
EventBus + Ownership seams (Phase 2d wires the clustered impls), and a
refusing-ownership test proving the gate blocks a peer-owned stream.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `LeasedOwnership` — Postgres lease row + heartbeat

Additive: a second `Ownership` impl. Workspace stays green; the lease tests are DATABASE_URL-gated.

**Files:**
- Create: `crates/core/src/leased_ownership.rs`
- Modify: `crates/core/src/lib.rs`

- [ ] **Step 1: Write the failing tests (create the file with module doc, struct stub, and tests)**

Create `crates/core/src/leased_ownership.rs`. The tests use **process-unique stream keys** and **unique replica ids** (nanosecond ts + atomic counter) so the gated tests run in parallel against the one shared Postgres DB without a global `TRUNCATE` clobbering siblings (the Phase 2b lesson). TTL is set short so expiry/steal is testable quickly.

```rust
//! Clustered single-writer ownership via a Postgres lease row per stream.
//!
//! One row per active stream — `(stream_key, owner_replica_id, epoch,
//! expires_at)`. `try_acquire` is `INSERT … ON CONFLICT … WHERE expired-or-ours`
//! (so it both claims a free stream and reaps an expired one — the shared poll
//! loop is the reaper/takeover, spec §3.3); the owner extends `expires_at` via
//! `heartbeat`/`renew`; `renew` failing is how a replica learns its lease was
//! stolen. Expiry uses the Postgres clock (`now()` + `make_interval`) so it is
//! immune to per-replica clock skew. `owns` answers from an in-memory mirror of
//! held leases (cheap, for the poll hot path); the `UNIQUE(stream_key, seq)`
//! store constraint plus the `epoch` fencing token are the two-writer backstops.

use std::collections::HashMap;
use std::sync::Mutex;

use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::Result;
use crate::error::CoreError;
use crate::ownership::{Lease, Ownership};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS leases (\
    stream_key TEXT PRIMARY KEY,\
    owner_replica_id TEXT NOT NULL,\
    epoch BIGINT NOT NULL,\
    expires_at TIMESTAMPTZ NOT NULL)";

/// Clustered `Ownership`: a Postgres lease per stream (spec §3.3).
pub struct LeasedOwnership {
    pool: PgPool,
    replica_id: String,
    /// Lease TTL in seconds (DB-clock relative). The daemon must call
    /// [`LeasedOwnership::heartbeat`] well within this window.
    ttl_secs: f64,
    /// In-memory mirror of leases this replica believes it holds: key → epoch.
    held: Mutex<HashMap<String, u64>>,
}

// (impl added below)

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique(tag: &str) -> String {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        format!("{tag}-{nanos}-{}", N.fetch_add(1, Ordering::Relaxed))
    }

    async fn owner(url: &str, ttl_secs: f64) -> LeasedOwnership {
        LeasedOwnership::connect(url, unique("replica"), ttl_secs).await.unwrap()
    }

    macro_rules! db_url {
        ($name:literal) => {
            match std::env::var("DATABASE_URL") {
                Ok(u) => u,
                Err(_) => {
                    eprintln!(concat!("SKIP ", $name, ": DATABASE_URL unset"));
                    return;
                }
            }
        };
    }

    #[tokio::test]
    async fn acquires_a_free_stream_and_owns_it() {
        let url = db_url!("acquires_a_free_stream_and_owns_it");
        let o = owner(&url, 30.0).await;
        let key = unique("s");
        let lease = o.try_acquire(&key).await.expect("free stream acquires");
        assert_eq!(lease.stream_key, key);
        assert!(lease.epoch >= 1);
        assert!(o.owns(&key));
    }

    #[tokio::test]
    async fn reacquiring_own_live_lease_is_idempotent_and_keeps_epoch() {
        let url = db_url!("reacquiring_own_live_lease_is_idempotent_and_keeps_epoch");
        let o = owner(&url, 30.0).await;
        let key = unique("s");
        let first = o.try_acquire(&key).await.unwrap();
        let again = o.try_acquire(&key).await.expect("own lease re-acquires");
        assert_eq!(again.epoch, first.epoch, "re-acquiring your own lease must not bump epoch");
    }

    #[tokio::test]
    async fn a_live_lease_blocks_another_replica() {
        let url = db_url!("a_live_lease_blocks_another_replica");
        let key = unique("s");
        let a = owner(&url, 30.0).await;
        let b = owner(&url, 30.0).await;
        assert!(a.try_acquire(&key).await.is_some());
        assert!(b.try_acquire(&key).await.is_none(), "peer must not steal a live lease");
        assert!(!b.owns(&key));
    }

    #[tokio::test]
    async fn an_expired_lease_is_stolen_with_a_bumped_epoch_and_renew_then_fails() {
        let url = db_url!("an_expired_lease_is_stolen_with_a_bumped_epoch_and_renew_then_fails");
        let key = unique("s");
        let a = owner(&url, 1.0).await; // 1s TTL
        let b = owner(&url, 30.0).await;
        let a_lease = a.try_acquire(&key).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(1200)).await; // let it expire

        let b_lease = b.try_acquire(&key).await.expect("expired lease is stolen");
        assert!(b_lease.epoch > a_lease.epoch, "takeover must bump the fencing epoch");
        // The dethroned owner learns it lost the lease on its next renew.
        assert!(a.renew(&a_lease).await.is_err(), "stale owner's renew must fail");
        assert!(b.owns(&key));
    }

    #[tokio::test]
    async fn renew_extends_a_held_lease() {
        let url = db_url!("renew_extends_a_held_lease");
        let o = owner(&url, 2.0).await;
        let key = unique("s");
        let lease = o.try_acquire(&key).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        o.renew(&lease).await.expect("owner renews its live lease");
        // After renew the lease is good for another full TTL, so a peer can't steal.
        let peer = owner(&url, 30.0).await;
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        assert!(peer.try_acquire(&key).await.is_none(), "renewed lease stays held");
    }

    #[tokio::test]
    async fn release_frees_the_stream_for_a_peer() {
        let url = db_url!("release_frees_the_stream_for_a_peer");
        let key = unique("s");
        let a = owner(&url, 30.0).await;
        let b = owner(&url, 30.0).await;
        a.try_acquire(&key).await.unwrap();
        a.release(&key).await;
        assert!(!a.owns(&key));
        assert!(b.try_acquire(&key).await.is_some(), "released stream is claimable");
    }

    #[tokio::test]
    async fn heartbeat_renews_all_held_and_drops_lost_leases() {
        let url = db_url!("heartbeat_renews_all_held_and_drops_lost_leases");
        let kept = unique("s");
        let lost = unique("s");
        let a = owner(&url, 2.0).await;
        let thief = owner(&url, 30.0).await;
        a.try_acquire(&kept).await.unwrap();
        a.try_acquire(&lost).await.unwrap();
        // A peer steals `lost` out from under `a` (forced, simulating a takeover
        // after a missed heartbeat). Use the raw DELETE+claim via try_acquire on
        // an expired lease is slow, so steal directly:
        thief.force_steal(&lost).await;

        a.heartbeat().await;
        assert!(a.owns(&kept), "still-held lease survives heartbeat");
        assert!(!a.owns(&kept) == false); // (kept stays owned)
        assert!(!a.owns(&lost), "heartbeat drops a lease lost to a peer");
    }
}
```

- [ ] **Step 2: Run the tests to confirm they fail to compile (no impl yet)**

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test \
  cargo test -p prospero-core --features prospero-core/testkit leased_ownership 2>&1 | tail -20
```

Expected: compile errors (`connect`/`try_acquire`/`heartbeat`/`force_steal` missing).

- [ ] **Step 3: Implement `LeasedOwnership`**

Add between the struct and the test module. Note the DB-clock SQL (`now()`, `make_interval(secs => $N)`), the epoch CASE (keep on re-acquire, bump on steal), and the `#[cfg(any(test, feature = "testkit"))]` helpers (`reset_for_tests`, `force_steal`) mirroring `postgres_store.rs`.

```rust
impl LeasedOwnership {
    /// Connect, ensure the lease table exists, and identify this replica.
    /// `ttl_secs` is the lease lifetime; call [`Self::heartbeat`] well within it.
    pub async fn connect(url: &str, replica_id: String, ttl_secs: f64) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .connect(url)
            .await
            .map_err(|e| CoreError::Store(format!("connecting to postgres: {e}")))?;
        sqlx::query(SCHEMA)
            .execute(&pool)
            .await
            .map_err(|e| CoreError::Store(format!("creating leases table: {e}")))?;
        Ok(Self {
            pool,
            replica_id,
            ttl_secs,
            held: Mutex::new(HashMap::new()),
        })
    }

    /// Build on an existing pool (shared-pool clustered wiring, Phase 2d).
    pub fn new(pool: PgPool, replica_id: String, ttl_secs: f64) -> Self {
        Self {
            pool,
            replica_id,
            ttl_secs,
            held: Mutex::new(HashMap::new()),
        }
    }

    /// Renew every lease this replica holds; drop any it has lost. The daemon's
    /// reconciliation tick calls this (Phase 2d).
    pub async fn heartbeat(&self) {
        let leases: Vec<Lease> = {
            let held = self.held.lock().unwrap();
            held.iter()
                .map(|(k, e)| Lease { stream_key: k.clone(), epoch: *e })
                .collect()
        };
        for lease in leases {
            if self.renew(&lease).await.is_err() {
                self.held.lock().unwrap().remove(&lease.stream_key);
                tracing::warn!(
                    target: "prospero_ownership",
                    stream = %lease.stream_key, "lease lost; dropping ownership"
                );
            }
        }
    }

    #[cfg(any(test, feature = "testkit"))]
    pub async fn reset_for_tests(&self) -> Result<()> {
        sqlx::query("TRUNCATE leases")
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("truncating leases: {e}")))?;
        self.held.lock().unwrap().clear();
        Ok(())
    }

    /// Test helper: unconditionally take a stream for this replica (used to
    /// simulate a takeover by a peer without waiting out a TTL).
    #[cfg(any(test, feature = "testkit"))]
    pub async fn force_steal(&self, stream_key: &str) {
        let row = sqlx::query(
            "INSERT INTO leases (stream_key, owner_replica_id, epoch, expires_at) \
             VALUES ($1, $2, 1, now() + make_interval(secs => $3)) \
             ON CONFLICT (stream_key) DO UPDATE \
                SET owner_replica_id = excluded.owner_replica_id, \
                    epoch = leases.epoch + 1, \
                    expires_at = excluded.expires_at \
             RETURNING epoch",
        )
        .bind(stream_key)
        .bind(&self.replica_id)
        .bind(self.ttl_secs)
        .fetch_one(&self.pool)
        .await
        .expect("force_steal");
        let epoch: i64 = row.get("epoch");
        self.held.lock().unwrap().insert(stream_key.to_string(), epoch as u64);
    }
}

#[async_trait::async_trait]
impl Ownership for LeasedOwnership {
    async fn try_acquire(&self, stream_key: &str) -> Option<Lease> {
        // Claim a free key, reap an expired one, or idempotently re-confirm our
        // own. Epoch is kept when re-acquiring ours, bumped when stealing an
        // expired lease (fences the dead owner). The WHERE makes the UPDATE — and
        // thus the RETURNING row — vanish when another live replica owns it.
        let row = sqlx::query(
            "INSERT INTO leases (stream_key, owner_replica_id, epoch, expires_at) \
             VALUES ($1, $2, 1, now() + make_interval(secs => $3)) \
             ON CONFLICT (stream_key) DO UPDATE \
                SET owner_replica_id = excluded.owner_replica_id, \
                    epoch = CASE WHEN leases.owner_replica_id = excluded.owner_replica_id \
                                 THEN leases.epoch ELSE leases.epoch + 1 END, \
                    expires_at = excluded.expires_at \
             WHERE leases.expires_at < now() \
                OR leases.owner_replica_id = excluded.owner_replica_id \
             RETURNING epoch",
        )
        .bind(stream_key)
        .bind(&self.replica_id)
        .bind(self.ttl_secs)
        .fetch_optional(&self.pool)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(target: "prospero_ownership", stream = %stream_key, error = %e, "try_acquire failed");
            None
        })?;
        let epoch: i64 = row.get("epoch");
        let epoch = epoch as u64;
        self.held.lock().unwrap().insert(stream_key.to_string(), epoch);
        Some(Lease { stream_key: stream_key.to_string(), epoch })
    }

    async fn renew(&self, lease: &Lease) -> Result<()> {
        // Extend only while we still hold it at the SAME epoch — a steal advances
        // owner/epoch, so 0 rows affected means we lost the lease.
        let res = sqlx::query(
            "UPDATE leases SET expires_at = now() + make_interval(secs => $1) \
             WHERE stream_key = $2 AND owner_replica_id = $3 AND epoch = $4",
        )
        .bind(self.ttl_secs)
        .bind(&lease.stream_key)
        .bind(&self.replica_id)
        .bind(lease.epoch as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("renewing lease: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(CoreError::Store(format!(
                "lease for {} lost (stolen or expired)",
                lease.stream_key
            )));
        }
        Ok(())
    }

    async fn release(&self, stream_key: &str) {
        self.held.lock().unwrap().remove(stream_key);
        if let Err(e) = sqlx::query("DELETE FROM leases WHERE stream_key = $1 AND owner_replica_id = $2")
            .bind(stream_key)
            .bind(&self.replica_id)
            .execute(&self.pool)
            .await
        {
            tracing::warn!(target: "prospero_ownership", stream = %stream_key, error = %e, "releasing lease failed");
        }
    }

    fn owns(&self, stream_key: &str) -> bool {
        self.held.lock().unwrap().contains_key(stream_key)
    }
}
```

Fix the one deliberately-awkward assertion the test stub left in `heartbeat_renews_all_held_and_drops_lost_leases` — replace the `assert!(!a.owns(&kept) == false);` placeholder line with a clean `assert!(a.owns(&kept));` (it is a duplicate of the line above it; just delete the placeholder line).

- [ ] **Step 4: Export from `crates/core/src/lib.rs`**

Add (alphabetical — after `event` / before `ownership` exports; place the `pub mod` near the other `pub mod`s and the `pub use` near the others):

```rust
pub mod leased_ownership;
pub use leased_ownership::LeasedOwnership;
```

- [ ] **Step 5: Run the gated tests against PG18**

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test \
  cargo test -p prospero-core --features prospero-core/testkit leased_ownership 2>&1 | tail -25
```

Expected: all seven tests pass. Confirm the skip path too: re-run WITHOUT `DATABASE_URL` — each prints `SKIP …` and passes.

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
git commit -m "feat(core): LeasedOwnership — Postgres lease + heartbeat (clustered)

One lease row per stream: try_acquire is INSERT ON CONFLICT WHERE expired-or-
ours (claims free, reaps expired, idempotently re-confirms own — keeping epoch
on re-acquire, bumping it on a steal to fence the dead owner); renew extends
expires_at only at the held epoch, so a stolen lease surfaces as a renew error;
release deletes for prompt hand-off; owns answers from an in-memory mirror for
the poll hot path; heartbeat renews all held and drops any lost. Lease expiry
uses the Postgres clock (now() + make_interval) so it is immune to replica
clock skew. DATABASE_URL-gated tests against Postgres 18.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

- **Spec §3.3 coverage:** trait `try_acquire`/`renew`/`release`/`owns` ✓; lease row `(stream_key, owner_replica_id, epoch, expires_at)` ✓; `try_acquire` = INSERT ON CONFLICT WHERE expired ✓ (plus the "ours" idempotent arm the start_attach fix requires); heartbeat extends `expires_at` ✓; `renew` fails on steal ✓; reaper/takeover = the shared poll loop driving `try_acquire` (no separate reaper) ✓; graceful `release` on attach-task exit ✓; epoch fencing token carried + bumped on takeover ✓ (full control-fencing enforcement remains the deferred upstream-caliban dependency, unchanged).
- **Two-writer backstop:** `UNIQUE(stream_key, seq)` (already in the Postgres store) + epoch — both present; documented in the module doc.
- **Compile coupling:** the async-trait change + every `fleet.rs` consumer + the `with_seams` seam are all in Task 1, so the workspace compiles green at the task boundary (Phase 0 lesson). Task 2 is purely additive.
- **owns stays sync:** in-memory mirror, no `.await` rippled into the poll hot path; the trait keeps `owns` non-async even under `#[async_trait]` (only async fns are rewritten).
- **Clock skew:** all expiry math is DB-side (`now()` + `make_interval`), never Rust-side — correct for multi-replica.
- **Parallel-test isolation:** unique stream keys + unique replica ids, no reliance on a global `TRUNCATE` between parallel tests (Phase 2b lesson). `reset_for_tests`/`force_steal` are `#[cfg(any(test, feature="testkit"))]`, matching `postgres_store.rs`.
- **Hazard fixes verified by test:** `refused_ownership_blocks_the_attach_path` (Task 1) proves the gate; the idempotent-reacquire + no-release-on-race reasoning is encoded in `start_attach` and covered for `LeasedOwnership` by `reacquiring_own_live_lease_is_idempotent_and_keeps_epoch`.
- **Renewal ownership:** centralized in `heartbeat()` (daemon calls it in 2d), not threaded into each attach task — the module doc and the start_attach note say so, resolving the old inline TODO.
- **Type consistency:** `LeasedOwnership::{connect,new,heartbeat,reset_for_tests,force_steal}`, `ttl_secs: f64`, `held: Mutex<HashMap<String,u64>>`, `Lease{stream_key,epoch}`, `CoreError::Store` used identically across tasks and matching `postgres_store.rs`.
