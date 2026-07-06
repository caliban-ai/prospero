# No local FleetManager under k8s — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Under `PROSPERO_FLEET=k8s`, serve `K8sFleet` over an independently-composed `(store, bus)` — no `FleetManager`, no poll loop, no `ownership`/heartbeat/`config_store`. `local` stays byte-for-byte identical.

**Architecture:** Extract retention policy into a core free fn (`prune_store_older_than`, TDD). Restructure `main` into Phase 1 (compose `(store, bus)` per topology) + Phase 2 (backend select: local builds manager+seams+poll; k8s builds K8sFleet only).

**Tech Stack:** Rust 2024, tokio, anyhow, prospero-core seams (`Store`, `EventBus`, `ConfigStore`, `Ownership`, `FleetManager`, `K8sFleet`).

## Global Constraints

- Gate = CLAUDE.md gate with `$TESTKIT` = `--features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s`.
- `local` behavior unchanged (standalone AND clustered).
- Drop `ownership`/heartbeat/`config_store` under k8s (approved).
- Retention runs in both arms off the shared `store`.

---

### Task 1: Core `prune_store_older_than` helper (TDD)

**Files:**
- Modify: `crates/core/src/store.rs` (add free fn + test)
- Modify: `crates/core/src/fleet.rs:566` (`prune_older_than` delegates)

**Interfaces:**
- Produces: `pub async fn prune_store_older_than(store: &dyn Store, max_age: std::time::Duration) -> Result<u64>`.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` in `crates/core/src/store.rs` (create the module if absent; import what the sibling tests use — `JsonlStore`, `FleetEvent`, `EventKind`). Timestamps are RFC3339 strings; `prune(before_ts)` removes events with `ts < before_ts`.

```rust
#[tokio::test]
async fn prune_store_older_than_removes_only_aged_events() {
    let dir = tempfile::tempdir().unwrap();
    let store = JsonlStore::open(dir.path()).unwrap();
    // One event stamped ~2h ago, one ~now.
    let old_ts = (chrono::Utc::now() - chrono::Duration::hours(2)).to_rfc3339();
    let new_ts = chrono::Utc::now().to_rfc3339();
    for (seq, ts) in [(0u64, &old_ts), (1u64, &new_ts)] {
        store
            .append(&FleetEvent {
                seq,
                ts: ts.clone(),
                repo: "r".into(),
                agent_id: "a".into(),
                kind: EventKind::Output {
                    stream: crate::event::OutputStream::Stdout,
                    chunk: "x".into(),
                },
            })
            .await
            .unwrap();
    }
    // Prune everything older than 1h ⇒ only the 2h-old event goes.
    let removed = super::prune_store_older_than(&store, std::time::Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(removed, 1);
    let left = store.replay(&crate::event::stream_key_for("r", "a"), 0).await.unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].seq, 1);
}
```

> Verify the exact `append`/`FleetEvent`/`EventKind::Output` shape against the existing store tests before running; match their field names precisely.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p prospero-core --features testkit prune_store_older_than`
Expected: FAIL to compile (`prune_store_older_than` not defined).

- [ ] **Step 3: Implement the helper**

Add to `crates/core/src/store.rs` (top-level, after the trait):

```rust
/// Delete events older than `max_age` from `store`, returning the count
/// removed. The daemon's age-based retention policy (#4), independent of
/// `FleetManager` so the k8s arm — which builds no manager (#83) — can prune too.
pub async fn prune_store_older_than(
    store: &dyn Store,
    max_age: std::time::Duration,
) -> Result<u64> {
    let max = chrono::Duration::from_std(max_age).unwrap_or_else(|_| chrono::Duration::zero());
    let before = (chrono::Utc::now() - max).to_rfc3339();
    store.prune(&before).await
}
```

- [ ] **Step 4: Delegate `FleetManager::prune_older_than` to it (DRY)**

Replace the body at `crates/core/src/fleet.rs:566-570`:

```rust
    pub async fn prune_older_than(&self, max_age: std::time::Duration) -> Result<u64> {
        crate::store::prune_store_older_than(self.inner.emitter.store.as_ref(), max_age).await
    }
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p prospero-core --features testkit prune_store_older_than`
Expected: PASS. Then `cargo test -p prospero-core --features testkit` — the existing suite (incl. any retention test) still green.

- [ ] **Step 6: Commit**

```bash
git add crates/core/src/store.rs crates/core/src/fleet.rs
git commit -m "feat(core): prune_store_older_than free fn; FleetManager delegates (#83)"
```

---

### Task 2: Restructure the daemon composition

**Files:**
- Modify: `crates/daemon/src/main.rs` (the topology + backend selection block, ~164-313)

**Interfaces:**
- Consumes: `prune_store_older_than` (Task 1); `FleetManager::with_seams`; `K8sFleet` + #82 network wiring.

The target `main` flow (replaces today's "build manager, then select backend"):

```text
config = FleetConfig::from(args)                       // as today

// Phase 1 — shared observability plane, per topology.
let (store, bus): (Arc<dyn Store>, Arc<dyn EventBus>) = if let Some(url) = &args.database_url {
    let store = Arc::new(PostgresStore::connect(url).await?);
    let bus   = Arc::new(DistributedBus::connect(url, store.clone()).await?);
    (store, bus)
} else {
    let store = Arc::new(SqliteStore::open(&data_dir).await?);
    let bus: Arc<dyn EventBus> = Arc::new(InProcessBus::new(config.event_buffer));
    (store, bus)
};

// Phase 2 — backend select.
let mut poll_handle: Option<JoinHandle<()>> = None;
let mut heartbeat_handle: Option<JoinHandle<()>> = None;
let mut manager_for_shutdown: Option<FleetManager> = None;

let (fleet, admin) = match args.fleet_backend {
    Local => {
        // per-topology config_store + ownership (+ heartbeat when clustered)
        let (config_store, ownership) = if let Some(url) = &args.database_url {
            let cs = Arc::new(PostgresConfigStore::connect(url).await?);
            let own = Arc::new(LeasedOwnership::connect(url, resolve_replica_id(...), args.lease_ttl_secs).await?);
            // spawn heartbeat (moved verbatim from today's clustered branch), set heartbeat_handle
            (cs as Arc<dyn ConfigStore>, own as Arc<dyn Ownership>)
        } else {
            (Arc::new(SqliteConfigStore::open(&data_dir).await?) as _, Arc::new(SelfOwnsAll) as _)
        };
        let manager = FleetManager::with_seams(config, store.clone(), config_store, bus.clone(), ownership).await?;
        let local = LocalFleet::new(manager.clone());
        poll_handle = Some(tokio::spawn(manager.clone().run()));
        manager_for_shutdown = Some(manager);
        (Arc::new(local.clone()) as Arc<dyn FleetProvider>, Some(Arc::new(local) as Arc<dyn FleetAdmin>))
    }
    #[cfg(feature = "k8s")]
    K8s => {
        let client = build_kube_client(args.kubeconfig.as_deref()).await?;   // #82
        let ns = std::env::var("PROSPERO_K8S_NAMESPACE").unwrap_or_else(|_| "default".into());
        let api = prospero_core::KubeTaskApi::new(client, &ns);
        let tls = load_session_plane_tls(args.k8s_caliband_ca_file.as_deref(), &args.k8s_caliband_server_name)?; // #82
        let token = args.k8s_caliband_token_file.as_deref().map(read_token_file).transpose()?;                  // #82
        let k8s = prospero_core::K8sFleet::new(api, bus.clone(), store.clone()).with_network(tls, token);
        tracing::info!(target: "prosperod", backend = "k8s", namespace = %ns, "serving via K8sFleet (no FleetManager)");
        (Arc::new(k8s) as Arc<dyn FleetProvider>, None)
    }
    #[cfg(not(feature = "k8s"))]
    K8s => anyhow::bail!("PROSPERO_FLEET=k8s requires --features k8s ..."),
};

// Retention — both arms, off the shared store.
if args.retention_days > 0 {
    let s = store.clone();
    let max_age = Duration::from_secs(args.retention_days * 24 * 3600);
    tokio::spawn(async move { /* hourly tick */ prospero_core::prune_store_older_than(s.as_ref(), max_age).await ... });
}

let app = prospero_api::router(fleet, admin, store.clone(), bus.clone());
// serve ...

// Shutdown: drain only what exists.
if let Some(m) = &manager_for_shutdown { m.begin_shutdown(); }
if let Some(h) = poll_handle { let _ = h.await; }
if let Some(hb) = heartbeat_handle { hb.abort(); }
```

- [ ] **Step 1: Rewrite Phase 1 (compose `(store, bus)`) replacing the `manager = if let Some(url)…` block**

Build `(store, bus)` per topology as above. Keep the existing `tracing::info!` topology lines (standalone / clustered). Preserve `config` construction unchanged above Phase 1.

- [ ] **Step 2: Rewrite Phase 2 (backend match) to build the manager only under `local`**

Move the heartbeat spawn (today's clustered branch, main.rs ~190-199) into the `local` + clustered path verbatim, assigning `heartbeat_handle`. The k8s arm uses the #82 helpers and `store.clone()`/`bus.clone()` directly — no manager.

- [ ] **Step 3: Switch retention to `prune_store_older_than(store)` in both arms**

Replace the `manager.prune_older_than` retention loop (main.rs ~265-283) with one that closes over `store.clone()` and calls `prospero_core::prune_store_older_than(s.as_ref(), max_age)`. Keep the same hourly tick + logging.

- [ ] **Step 4: Fix shutdown to drain only-what-exists**

Replace `manager.begin_shutdown()` + unconditional `poll_handle.await` with the `Option`-guarded drains shown above. Remove the now-unused unconditional `manager` binding.

- [ ] **Step 5: Adjust imports**

`FleetManager`, `LocalFleet`, `PostgresConfigStore`, `SqliteConfigStore`, `InProcessBus`, `SelfOwnsAll`, `LeasedOwnership`, `Ownership`, `ConfigStore` are used; drop any that become unused. `InProcessBus` is `prospero_core::bus::InProcessBus`; `SqliteConfigStore`/`ConfigStore` from `prospero_core`. Let clippy name any unused import.

- [ ] **Step 6: Run the full gate**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets --features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s -- -D warnings && cargo build --workspace --all-targets --features … && cargo test --workspace --features …`
Expected: PASS. Also confirm a non-k8s build is clean: `cargo build -p prospero-daemon`.

- [ ] **Step 7: Verify the acceptance by reading the k8s arm**

Confirm the `FleetBackend::K8s` arm names none of: `FleetManager`, `config_store`, `ownership`, `heartbeat`, `LocalFleet`. (grep it.)

- [ ] **Step 8: Commit**

```bash
git add crates/daemon/src/main.rs
git commit -m "cleanup(daemon): build no FleetManager/ownership/heartbeat under k8s (#83)"
```

---

### Task 3: Docs touch-up

**Files:**
- Modify: `docs/container.md` (Fleet backends / k8s section)

- [ ] **Step 1: Note the composition difference**

Add one line under the k8s fleet-backend section: under `k8s`, prosperod serves
`K8sFleet` over the shared event store/bus and does **not** run a local
`FleetManager`, poll loop, or lease heartbeat (those are `local`-only).

- [ ] **Step 2: Commit**

```bash
git add docs/container.md
git commit -m "docs(container): note k8s runs no local FleetManager (#83)"
```

---

## Self-Review

- **Spec coverage:** retention helper (Task 1), Phase 1 shared plane (Task 2.1), local-only manager/seams (2.2), retention both arms (2.3), shutdown drains-what-exists (2.4), drop ownership/heartbeat under k8s (2.2 — k8s arm omits them), docs (Task 3). Covered.
- **Placeholders:** the Task 2 flow is illustrative pseudocode intentionally (the change is a whole-function restructure); each step names the exact block it replaces. No `TODO`s.
- **Type consistency:** `prune_store_older_than(&dyn Store, Duration) -> Result<u64>` used in core delegation (1.4), retention (2.3), and its test (1.1). `FleetManager::with_seams(config, store, config_store, bus, ownership)` matches `fleet.rs:430`. `router(fleet, admin, store, bus)` matches `api/src/lib.rs:41`. `K8sFleet::new(api, bus, store).with_network(tls, token)` matches the #82 arm.
- **Risk:** standalone-local now uses `with_seams` (not `new`); confirm `new`→`with_config_store`→`with_seams` builds the same seams (`InProcessBus` + `SelfOwnsAll` + `SqliteConfigStore`) — it does (fleet.rs:410-425). Behavior identical.
