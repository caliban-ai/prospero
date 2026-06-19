# Cluster Lifecycle Dedup (#59) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** In clustered mode, make exactly one replica emit each poll-derived lifecycle event (`AgentDiscovered`/`StatusChanged`/`AgentGone`/`RepoHealth`), eliminating the duplicate rows in the durable log/SSE/dashboard.

**Architecture:** Gate the lifecycle `emit(...)` calls in `reconcile()`/`mark_unreachable()` on a per-repo "lifecycle lease" — `ownership.try_acquire("repo:<name>")`, computed once per poll in `poll_repo_once` and threaded in as a bool. Attach-stream events keep their per-agent lease; snapshot writes stay ungated. `SelfOwnsAll` always owns, so standalone is unchanged.

**Tech Stack:** Rust, tokio, the existing `Ownership` trait (`SelfOwnsAll` / `LeasedOwnership`), `JsonlStore`/`FakeCaliband` test harness.

## Global Constraints

- Change is confined to `crates/core/src/fleet.rs` (impl) and its test module + `crates/core/tests/fleet_integration.rs` (tests). No changes to `Ownership`, the lease table/schema, the store, or the wire.
- The local gate must stay green with the project's exact CI commands (see Task 3), including the Postgres-gated tests with `DATABASE_URL` set.
- Lifecycle lease key = `crate::event::stream_key_for(repo, "")` (= `repo:<name>`).
- Reasoning comments must reference `(#59)` where the gate is introduced, matching the existing `(#51)`/`(#49)` convention in this file.

---

### Task 1: Repo lifecycle lease gate + two-replica regression test

**Files:**
- Modify: `crates/core/src/fleet.rs` — `poll_repo_once` (~755), `reconcile` (~794), `mark_unreachable` (~777)
- Test: `crates/core/tests/fleet_integration.rs` (new `#[tokio::test]`)

**Interfaces:**
- Consumes: `Ownership::try_acquire(&str) -> Option<Lease>` (existing); `crate::event::stream_key_for(repo, agent_id) -> String` (existing).
- Produces: `reconcile(&self, repo: &str, records: Vec<AgentRecord>, client: CalibandClient, own_lifecycle: bool)` and `mark_unreachable(&self, repo: &str, reason: String, own_lifecycle: bool)` — new trailing `own_lifecycle` param on both private methods.

- [ ] **Step 1: Write the failing two-replica regression test**

Append to `crates/core/tests/fleet_integration.rs`:

```rust
#[tokio::test]
async fn clustered_status_change_emits_once_not_per_replica() {
    // Two managers share one data dir (store + config store) and one FakeCaliband
    // socket — i.e. two clustered replicas over one repo. A status transition both
    // observe must land in the durable log exactly once: only the repo-lifecycle-
    // lease owner emits. Before #59 both emitted, duplicating the event. (#59)
    use prospero_core::bus::InProcessBus;
    use prospero_core::config_store::SqliteConfigStore;
    use prospero_core::ownership::{Lease, Ownership, SelfOwnsAll};
    use prospero_core::store::Store;
    use prospero_core::{ConfigStore, EventBus};
    use async_trait::async_trait;

    // Owns every stream EXCEPT the repo lifecycle lease ("repo:" keys).
    struct RepoBlind;
    #[async_trait]
    impl Ownership for RepoBlind {
        async fn try_acquire(&self, key: &str) -> Option<Lease> {
            if key.starts_with("repo:") {
                None
            } else {
                Some(Lease { stream_key: key.to_string(), epoch: 1 })
            }
        }
        async fn renew(&self, _: &Lease) -> prospero_core::error::Result<()> { Ok(()) }
        async fn release(&self, _: &str) {}
        fn owns(&self, key: &str) -> bool { !key.starts_with("repo:") }
    }

    let repo_dir = tempfile::tempdir().unwrap();
    let runtime_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let repo_root = repo_dir.path().canonicalize().unwrap();

    let env = DiscoveryEnv {
        caliban_daemon_runtime_dir: Some(runtime_dir.path().to_path_buf()),
        xdg_runtime_dir: None,
        tmpdir: None,
    };
    let socket = control_socket_path(&repo_root, &env);
    let mut fake = FakeCaliband::start_at(&socket).await.unwrap();
    let dir = socket.parent().unwrap().to_path_buf();
    let rec = test_record("agent001", &dir, AgentStatus::Idle, false);
    fake.add_agent(rec, vec![]).await;

    let mk = || {
        let mut config = FleetConfig::new("h", data_dir.path());
        config.discovery_env = env.clone();
        config.ensure = EnsureConfig { autostart: false, ..EnsureConfig::default() };
        config
    };
    let shared_store: Arc<dyn Store> = Arc::new(JsonlStore::open(data_dir.path()).unwrap());

    // Replica A owns everything (emits lifecycle); replica B is repo-blind (suppresses it).
    let cfg_a: Arc<dyn ConfigStore> =
        Arc::new(SqliteConfigStore::open(data_dir.path()).await.unwrap());
    let bus_a: Arc<dyn EventBus> = Arc::new(InProcessBus::new(1024));
    let a = FleetManager::with_seams(mk(), shared_store.clone(), cfg_a, bus_a, Arc::new(SelfOwnsAll))
        .await
        .unwrap();
    a.add_repo("repo", &repo_root).await.unwrap();

    let cfg_b: Arc<dyn ConfigStore> =
        Arc::new(SqliteConfigStore::open(data_dir.path()).await.unwrap());
    let bus_b: Arc<dyn EventBus> = Arc::new(InProcessBus::new(1024));
    let b = FleetManager::with_seams(mk(), shared_store.clone(), cfg_b, bus_b, Arc::new(RepoBlind))
        .await
        .unwrap();
    b.poll_all_once().await; // B picks up the shared repo

    // Both discover the Idle agent, then both observe Idle -> Done.
    a.poll_repo_once("repo").await;
    b.poll_repo_once("repo").await;
    fake.set_status("agent001", AgentStatus::Done);
    a.poll_repo_once("repo").await;
    b.poll_repo_once("repo").await;

    // Read the shared durable log for the agent stream and count StatusChanged.
    let events = shared_store.replay("agent001", 0).await.unwrap();
    let status_changes = events
        .iter()
        .filter(|e| matches!(
            e.kind,
            EventKind::StatusChanged { from: AgentStatus::Idle, to: AgentStatus::Done }
        ))
        .count();
    assert_eq!(
        status_changes, 1,
        "exactly one replica may emit the lifecycle event; got {status_changes}"
    );
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p prospero-core --features testkit --test fleet_integration clustered_status_change_emits_once_not_per_replica`
Expected: COMPILE error (`reconcile`/`mark_unreachable` still take the old arity once Step 3 lands) — at this step it should FAIL the assertion with `got 2` (both replicas emit, since the gate doesn't exist yet). If the test references `Store::replay`/imports that don't resolve, fix the imports until it compiles and then fails on `assert_eq!(.., 1)` with 2.

- [ ] **Step 3: Implement the gate in `fleet.rs`**

In `poll_repo_once`, compute ownership once and thread it through:

```rust
pub async fn poll_repo_once(&self, repo: &str) {
    self.inner.emitter.metrics.record_repo_poll();
    // Designate a single authoritative emitter for this repo's poll-derived
    // lifecycle events. The lease keys off the repo's own event stream; in
    // standalone (SelfOwnsAll) this is always owned, so behavior is unchanged. (#59)
    let own_lifecycle = self
        .inner
        .ownership
        .try_acquire(&crate::event::stream_key_for(repo, ""))
        .await
        .is_some();
    let client = match self.client_for(repo).await {
        Ok(c) => c,
        Err(e) => {
            self.mark_unreachable(repo, e.to_string(), own_lifecycle).await;
            return;
        }
    };
    match client.list().await {
        Ok(records) => self.reconcile(repo, records, client, own_lifecycle).await,
        Err(e) => {
            self.inner.clients.lock().unwrap().remove(repo);
            self.mark_unreachable(repo, e.to_string(), own_lifecycle).await;
        }
    }
}
```

Update `mark_unreachable` to gate only the emit (keep the snapshot health write):

```rust
async fn mark_unreachable(&self, repo: &str, reason: String, own_lifecycle: bool) {
    let mut snap = self.inner.snapshot.write().await;
    if let Some(r) = snap.repos.iter_mut().find(|r| r.name == repo) {
        let new_health = RepoHealth::Unreachable { reason: reason.clone() };
        if r.health != new_health {
            r.health = new_health.clone();
            drop(snap);
            if own_lifecycle {
                self.inner
                    .emitter
                    .emit(repo, "", EventKind::RepoHealth { state: new_health })
                    .await;
            }
        }
    }
}
```

Update `reconcile`'s signature and gate the four lifecycle emits. New signature:

```rust
async fn reconcile(
    &self,
    repo: &str,
    records: Vec<AgentRecord>,
    client: CalibandClient,
    own_lifecycle: bool,
) {
```

Gate `AgentDiscovered` and `StatusChanged` by extending their match guards (so an
ungated `own_lifecycle == false` falls through to the no-op arm):

```rust
            match prior.get(&rec.id) {
                // New to the snapshot. Suppress "discovered" for agents we just
                // spawned (already attached + emitted AgentSpawned). Only the
                // repo lifecycle-lease owner emits it. (#59)
                None if own_lifecycle && !attached_now.contains(&rec.id) => {
                    self.inner
                        .emitter
                        .emit(repo, &rec.id, EventKind::AgentDiscovered)
                        .await;
                }
                None => {}
                // Only the repo lifecycle-lease owner emits transitions. (#59)
                Some(&old) if own_lifecycle && old != rec.status => {
                    self.inner
                        .emitter
                        .emit(
                            repo,
                            &rec.id,
                            EventKind::StatusChanged { from: old, to: rec.status },
                        )
                        .await;
                }
                _ => {}
            }
```

Gate `AgentGone`:

```rust
        // Agents that disappeared from caliban's registry. Only the repo
        // lifecycle-lease owner emits it. (#59)
        for (old_id, _) in prior.iter() {
            if own_lifecycle && !records.iter().any(|r| &r.id == old_id) {
                self.inner
                    .emitter
                    .emit(repo, old_id, EventKind::AgentGone)
                    .await;
            }
        }
```

Gate the `RepoHealth` → Healthy recovery emit (keep the snapshot write ungated):

```rust
                if was_unreachable {
                    drop(snap);
                    if own_lifecycle {
                        self.inner
                            .emitter
                            .emit(
                                repo,
                                "",
                                EventKind::RepoHealth { state: RepoHealth::Healthy },
                            )
                            .await;
                    }
                }
```

- [ ] **Step 4: Run the regression test to verify it passes**

Run: `cargo test -p prospero-core --features testkit --test fleet_integration clustered_status_change_emits_once_not_per_replica`
Expected: PASS (`status_changes == 1`).

- [ ] **Step 5: Commit**

```bash
git add crates/core/src/fleet.rs crates/core/tests/fleet_integration.rs
git commit -m "fix(core): gate poll-derived lifecycle events on a per-repo lease (#59)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Focused gating unit tests (lifecycle + repo health)

**Files:**
- Test: `crates/core/tests/fleet_integration.rs` (two new `#[tokio::test]`)

**Interfaces:**
- Consumes: the `RepoBlind` ownership and `with_seams` wiring established in Task 1. To avoid duplication, lift `RepoBlind` to a module-level helper struct in the test file (move it above the test fns) so both tasks' tests share it.

- [ ] **Step 1: Lift `RepoBlind` to module scope**

Move the `RepoBlind` struct + its `impl Ownership` (from Task 1's test) to just below the imports in `crates/core/tests/fleet_integration.rs`, and delete the inline copy from the Task 1 test. Add the needed `use` lines at module top: `use prospero_core::ownership::{Lease, Ownership};` and `use async_trait::async_trait;`.

- [ ] **Step 2: Write the lifecycle-suppression test**

```rust
#[tokio::test]
async fn non_owner_suppresses_agent_lifecycle_events() {
    // A replica that does not own the repo lifecycle lease emits no
    // AgentDiscovered / StatusChanged even though it polls and attaches. (#59)
    use prospero_core::bus::InProcessBus;
    use prospero_core::config_store::SqliteConfigStore;
    use prospero_core::store::Store;
    use prospero_core::{ConfigStore, EventBus};

    let repo_dir = tempfile::tempdir().unwrap();
    let runtime_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let repo_root = repo_dir.path().canonicalize().unwrap();
    let env = DiscoveryEnv {
        caliban_daemon_runtime_dir: Some(runtime_dir.path().to_path_buf()),
        xdg_runtime_dir: None,
        tmpdir: None,
    };
    let socket = control_socket_path(&repo_root, &env);
    let mut fake = FakeCaliband::start_at(&socket).await.unwrap();
    let dir = socket.parent().unwrap().to_path_buf();
    fake.add_agent(test_record("agent001", &dir, AgentStatus::Idle, false), vec![]).await;

    let mut config = FleetConfig::new("h", data_dir.path());
    config.discovery_env = env;
    config.ensure = EnsureConfig { autostart: false, ..EnsureConfig::default() };
    let store: Arc<dyn Store> = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let cfg: Arc<dyn ConfigStore> =
        Arc::new(SqliteConfigStore::open(data_dir.path()).await.unwrap());
    let bus: Arc<dyn EventBus> = Arc::new(InProcessBus::new(1024));
    let mgr = FleetManager::with_seams(config, store.clone(), cfg, bus, Arc::new(RepoBlind))
        .await
        .unwrap();
    mgr.add_repo("repo", &repo_root).await.unwrap();

    mgr.poll_repo_once("repo").await; // discover (suppressed)
    fake.set_status("agent001", AgentStatus::Done);
    mgr.poll_repo_once("repo").await; // transition (suppressed)

    let events = store.replay("agent001", 0).await.unwrap();
    assert!(
        !events.iter().any(|e| matches!(e.kind, EventKind::AgentDiscovered)),
        "non-owner must not emit AgentDiscovered"
    );
    assert!(
        !events.iter().any(|e| matches!(e.kind, EventKind::StatusChanged { .. })),
        "non-owner must not emit StatusChanged"
    );
}
```

- [ ] **Step 3: Write the repo-health-suppression test**

```rust
#[tokio::test]
async fn non_owner_suppresses_repo_health_events() {
    // A repo-blind replica polling an unreachable repo updates its own snapshot
    // health but emits no RepoHealth event. (#59)
    use prospero_core::bus::InProcessBus;
    use prospero_core::config_store::SqliteConfigStore;
    use prospero_core::store::Store;
    use prospero_core::{ConfigStore, EventBus};

    let repo_dir = tempfile::tempdir().unwrap();
    let runtime_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let repo_root = repo_dir.path().canonicalize().unwrap();
    let env = DiscoveryEnv {
        caliban_daemon_runtime_dir: Some(runtime_dir.path().to_path_buf()),
        xdg_runtime_dir: None,
        tmpdir: None,
    };
    // No FakeCaliband started → repo is unreachable.
    let mut config = FleetConfig::new("h", data_dir.path());
    config.discovery_env = env;
    config.ensure = EnsureConfig { autostart: false, ..EnsureConfig::default() };
    let store: Arc<dyn Store> = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let cfg: Arc<dyn ConfigStore> =
        Arc::new(SqliteConfigStore::open(data_dir.path()).await.unwrap());
    let bus: Arc<dyn EventBus> = Arc::new(InProcessBus::new(1024));
    let mgr = FleetManager::with_seams(config, store.clone(), cfg, bus, Arc::new(RepoBlind))
        .await
        .unwrap();
    mgr.add_repo("repo", &repo_root).await.unwrap();

    mgr.poll_repo_once("repo").await;

    // Snapshot health still updates locally...
    let snap = mgr.snapshot().await;
    let repo = snap.repos.iter().find(|r| r.name == "repo").unwrap();
    assert!(matches!(repo.health, RepoHealth::Unreachable { .. }));
    // ...but no RepoHealth event is emitted to the durable log.
    let events = store.replay("repo:repo", 0).await.unwrap();
    assert!(
        !events.iter().any(|e| matches!(e.kind, EventKind::RepoHealth { .. })),
        "non-owner must not emit RepoHealth"
    );
}
```

- [ ] **Step 4: Run the new tests + the standalone regression guard**

Run: `cargo test -p prospero-core --features testkit --test fleet_integration non_owner_ status_change_emits_event_across_polls`
Expected: PASS — both `non_owner_*` tests pass, and `status_change_emits_event_across_polls` (SelfOwnsAll owner) still emits its event (proves the owner path is intact).

- [ ] **Step 5: Commit**

```bash
git add crates/core/tests/fleet_integration.rs
git commit -m "test(core): cover repo-lifecycle-lease gating for lifecycle + repo health (#59)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Full local gate + live two-replica re-verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full CI gate with Postgres-gated tests**

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:55432/prospero_test
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```
Expected: all green; the `leased_ownership`/`postgres_*`/`distributed_bus` PG-gated tests run (not skipped). If `fmt` flags anything, run `cargo fmt --all` and re-check.

- [ ] **Step 2: Live two-replica re-verification (reproduce #59, confirm fixed)**

Build the daemon from this branch, then run the exact deep-round repro: two replicas on a fresh PG DB + real caliband + a spawned agent, and assert the durable log has **no** duplicate `status_changed`/`repo_health`.

```bash
cargo build --workspace --bins
docker exec prospero-pg-test psql -U postgres -c "DROP DATABASE IF EXISTS prospero_fix59 WITH (FORCE)"
docker exec prospero-pg-test psql -U postgres -c "CREATE DATABASE prospero_fix59"
CB=/Users/johnford2002/dev/caliban-ai/caliban/target/debug/caliband
DBU=postgres://postgres:postgres@localhost:55432/prospero_fix59
mkdir -p /tmp/prospero-qa/fix59A /tmp/prospero-qa/fix59B /tmp/prospero-qa/fix59repo
git -C /tmp/prospero-qa/fix59repo init -q
PROSPERO_DATA_DIR=/tmp/prospero-qa/fix59A ./target/debug/prosperod --addr 127.0.0.1:7878 --database-url "$DBU" --replica-id repA --lease-ttl-secs 8 --caliband-bin "$CB" --poll-interval-ms 1000 &
PROSPERO_DATA_DIR=/tmp/prospero-qa/fix59B ./target/debug/prosperod --addr 127.0.0.1:7879 --database-url "$DBU" --replica-id repB --lease-ttl-secs 8 --caliband-bin "$CB" --poll-interval-ms 1000 &
sleep 3
PROSPERO_ADDR=http://127.0.0.1:7878 ./target/debug/prospero repo add r /tmp/prospero-qa/fix59repo
PROSPERO_ADDR=http://127.0.0.1:7878 ./target/debug/prospero repo config r --provider ollama --base-url http://192.168.1.240:11434
sleep 3
PROSPERO_ADDR=http://127.0.0.1:7878 ./target/debug/prospero spawn r "Reply with exactly: OK" --model gemma4:12b-mlx
sleep 18
# Expect: every (stream_key, kind, from, to) appears once — no count > 1.
docker exec prospero-pg-test psql -U postgres -d prospero_fix59 -c \
  "SELECT stream_key, substring(kind,1,60), count(*) FROM events GROUP BY stream_key, kind HAVING count(*) > 1"
```
Expected: **zero rows** from the `HAVING count(*) > 1` query (no duplicates). Then tear down: `pkill -x prosperod; pkill -f 'caliband --repo-root /private/tmp/prospero-qa'; docker exec prospero-pg-test psql -U postgres -c "DROP DATABASE IF EXISTS prospero_fix59 WITH (FORCE)"`.

- [ ] **Step 3: Done — hand back to sprint mode (cai-ship-it)**

No commit (verification only). The branch is ready for `cai-ship-it`.
