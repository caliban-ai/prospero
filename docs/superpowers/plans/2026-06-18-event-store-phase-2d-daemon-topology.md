# Event Store Phase 2d — Daemon standalone-vs-clustered selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax. Work ONLY in the worktree at `.claude/worktrees/event-store-phase-2-clustered` on branch `worktree-event-store-phase-2-clustered`. Do NOT `git checkout` any other branch or commit.

**Goal:** Let `prosperod` select its topology at startup — **standalone** (sqlite `Store` + `SqliteConfigStore` + `InProcessBus` + `SelfOwnsAll`, today's behaviour) or **clustered** (`PostgresStore` + `PostgresConfigStore` + `DistributedBus` + `LeasedOwnership` + a lease-heartbeat loop) — chosen by whether a Postgres URL is configured, wired through `FleetManager::with_seams`.

**Architecture:** A new `--database-url` (env `PROSPERO_DATABASE_URL`) selects the tier: absent → standalone; present → clustered. Clustered also takes `--replica-id` (default: the `HOSTNAME` env — the pod name in k8s), `--lease-ttl-secs`, and `--heartbeat-interval-ms` (default: a third of the TTL). The clustered builder connects the four Postgres-backed seams and spawns a heartbeat task that calls `LeasedOwnership::heartbeat()` on the interval; the daemon keeps a concrete `Arc<LeasedOwnership>` for that loop while handing a trait-object clone to `with_seams`. Graceful shutdown drains the poll loop (whose per-stream attach tasks already `release()` their leases) and aborts the heartbeat task.

**Tech Stack:** Rust (tokio, clap, anyhow), the Phase 2a–2c core seams.

**Spec:** `docs/superpowers/specs/2026-06-17-event-store-topology-design.md` §3 (the seam table — one topology-blind loop, a startup config selects each seam impl).

---

## File Structure

- **Modify** `crates/daemon/src/main.rs` — CLI args for the clustered tier; a `build_manager` that branches standalone/clustered; the heartbeat loop; a topology log line; graceful heartbeat teardown; unit tests for the pure helpers.

(Separate Postgres pools per seam via each backend's `connect(url)` are used for this first cut — each ensures its own schema via the shared `crate::pg` helper from Phase 2c. A single shared pool is a later tuning option; note it, don't build it.)

---

## Task 1: Topology selection + clustered wiring + heartbeat loop

**Files:**
- Modify: `crates/daemon/src/main.rs`

- [ ] **Step 1: Add the clustered CLI args**

Add these fields to `struct Args` (after `retention_days`):

```rust
    /// Postgres connection URL. When set, prosperod runs in CLUSTERED mode
    /// (Postgres store/config + LISTEN/NOTIFY bus + leased ownership); when
    /// unset, it runs STANDALONE (sqlite + in-process bus + self-owns-all).
    #[arg(long, env = "PROSPERO_DATABASE_URL")]
    database_url: Option<String>,

    /// Clustered only: this replica's identity for lease ownership. Defaults to
    /// the HOSTNAME env (the pod name under k8s). MUST be unique per replica.
    #[arg(long, env = "PROSPERO_REPLICA_ID")]
    replica_id: Option<String>,

    /// Clustered only: lease time-to-live in seconds. A stream's owner must
    /// heartbeat within this window or a peer may take the stream over.
    #[arg(long, default_value_t = 30.0)]
    lease_ttl_secs: f64,

    /// Clustered only: how often (ms) to renew held leases. Defaults to a third
    /// of the lease TTL.
    #[arg(long)]
    heartbeat_interval_ms: Option<u64>,
```

- [ ] **Step 2: Add the pure helpers (with unit tests)**

Add these free functions near `default_data_dir`:

```rust
/// This replica's lease identity: the explicit `--replica-id`, else the
/// `HOSTNAME` env (the pod name in k8s), else a local fallback.
fn resolve_replica_id(explicit: Option<&str>) -> String {
    explicit
        .map(str::to_string)
        .or_else(|| std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty()))
        .unwrap_or_else(|| "prosperod-local".to_string())
}

/// Heartbeat period: explicit `--heartbeat-interval-ms`, else a third of the
/// lease TTL (clamped to at least 1s so a tiny TTL can't busy-loop).
fn heartbeat_interval(explicit_ms: Option<u64>, lease_ttl_secs: f64) -> Duration {
    match explicit_ms {
        Some(ms) => Duration::from_millis(ms.max(1)),
        None => {
            let secs = (lease_ttl_secs / 3.0).max(1.0);
            Duration::from_secs_f64(secs)
        }
    }
}
```

And extend the `#[cfg(test)] mod tests` with:

```rust
    use super::{heartbeat_interval, resolve_replica_id};
    use std::time::Duration;

    #[test]
    fn replica_id_prefers_explicit_then_falls_back() {
        assert_eq!(resolve_replica_id(Some("r7")), "r7");
        // With no explicit id, falls back to HOSTNAME or the local default.
        // (We don't mutate process env here; just assert it returns non-empty.)
        assert!(!resolve_replica_id(None).is_empty());
    }

    #[test]
    fn heartbeat_defaults_to_a_third_of_ttl_and_is_clamped() {
        assert_eq!(heartbeat_interval(Some(500), 30.0), Duration::from_millis(500));
        assert_eq!(heartbeat_interval(None, 30.0), Duration::from_secs(10));
        // Tiny TTL clamps to >= 1s; explicit 0 clamps to >= 1ms.
        assert_eq!(heartbeat_interval(None, 0.6), Duration::from_secs(1));
        assert_eq!(heartbeat_interval(Some(0), 30.0), Duration::from_millis(1));
    }
```

- [ ] **Step 3: Build the manager per topology in `main`**

Replace the current store + manager construction (the `let store = …; let manager = FleetManager::new(…)` block, ~lines 96–103) with a topology branch. Keep `use` additions at the top: `prospero_core::{DistributedBus, LeasedOwnership, PostgresStore, PostgresConfigStore}`, the trait objects `prospero_core::store::Store`, `prospero_core::config_store::ConfigStore`, `prospero_core::bus::EventBus`, `prospero_core::ownership::Ownership`, and `prospero_core::fleet::FleetManager`. Also import `tokio::task::JoinHandle`.

```rust
    // Select the storage/ownership topology. A Postgres URL ⇒ clustered.
    let mut heartbeat_handle: Option<JoinHandle<()>> = None;
    let manager = if let Some(url) = args.database_url.clone() {
        let replica_id = resolve_replica_id(args.replica_id.as_deref());
        let store: Arc<dyn Store> = Arc::new(
            PostgresStore::connect(&url)
                .await
                .with_context(|| "connecting clustered event store")?,
        );
        let config_store: Arc<dyn ConfigStore> = Arc::new(
            PostgresConfigStore::connect(&url)
                .await
                .with_context(|| "connecting clustered config store")?,
        );
        let bus: Arc<dyn EventBus> = Arc::new(
            DistributedBus::connect(&url, store.clone())
                .await
                .with_context(|| "connecting clustered event bus")?,
        );
        let ownership = Arc::new(
            LeasedOwnership::connect(&url, replica_id.clone(), args.lease_ttl_secs)
                .await
                .with_context(|| "connecting clustered ownership")?,
        );

        // Heartbeat: renew this replica's held leases so it keeps its streams.
        let interval = heartbeat_interval(args.heartbeat_interval_ms, args.lease_ttl_secs);
        let hb = ownership.clone();
        heartbeat_handle = Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                hb.heartbeat().await;
            }
        }));

        tracing::info!(
            target: "prosperod",
            topology = "clustered", %replica_id,
            lease_ttl_secs = args.lease_ttl_secs,
            heartbeat_ms = interval.as_millis() as u64,
            "selected clustered topology"
        );

        FleetManager::with_seams(
            config,
            store,
            config_store,
            bus,
            ownership as Arc<dyn Ownership>,
        )
        .await
        .with_context(|| "building clustered fleet manager")?
    } else {
        let store = Arc::new(
            SqliteStore::open(&data_dir)
                .await
                .with_context(|| "opening event store")?,
        );
        tracing::info!(target: "prosperod", topology = "standalone", "selected standalone topology");
        FleetManager::new(config, store)
            .await
            .with_context(|| "building fleet manager")?
    };
```

- [ ] **Step 4: Tear down the heartbeat on shutdown**

After the poll loop is drained (`poll_handle.await`), abort the heartbeat task so it stops renewing. Add right after the existing `if let Err(e) = poll_handle.await { … }` block:

```rust
    if let Some(hb) = heartbeat_handle {
        hb.abort();
    }
```

(Held leases are released by the per-stream attach tasks as `begin_shutdown` drains them — the heartbeat just needs to stop; aborting is sufficient.)

- [ ] **Step 5: Build + test**

```bash
cargo build --workspace --all-targets --features prospero-core/testkit
cargo test --workspace --features prospero-core/testkit
```

Expected: PASS (no DATABASE_URL needed — the new daemon tests are pure-function tests; the clustered path is exercised by the core gated tests). `cargo fmt --all` + `cargo fmt --all -- --check` clean and `cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings` clean.

- [ ] **Step 6: Smoke-check both topologies compile-wire correctly**

Standalone start (no URL) — should log `topology="standalone"` and serve; Ctrl-C to stop:

```bash
PROSPERO_DATA_DIR=$(mktemp -d) timeout 3 cargo run -p prospero-daemon -- --no-autostart --addr 127.0.0.1:7900 2>&1 | rg -i "standalone|listening" | head
```

Clustered start (PG18 on 55432) — should log `topology="clustered"` with a replica id and serve:

```bash
PROSPERO_DATA_DIR=$(mktemp -d) timeout 4 cargo run -p prospero-daemon -- \
  --no-autostart --addr 127.0.0.1:7901 \
  --database-url postgres://postgres:postgres@localhost:55432/prospero_test \
  --replica-id smoke-1 --lease-ttl-secs 5 2>&1 | rg -i "clustered|listening|error" | head
```

Expected: standalone logs `topology="standalone"` + `listening`; clustered logs `topology="clustered"` + `replica_id` + `listening`, with no connection error. (These are manual smoke checks — `timeout` ends the process; a non-zero timeout exit is fine.)

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(daemon): select standalone vs clustered topology at startup

A --database-url (env PROSPERO_DATABASE_URL) flips prosperod from standalone
(sqlite + in-process bus + self-owns-all) to clustered: PostgresStore +
PostgresConfigStore + DistributedBus + LeasedOwnership, wired through
FleetManager::with_seams. Clustered adds --replica-id (default: HOSTNAME / pod
name), --lease-ttl-secs, and --heartbeat-interval-ms, and spawns a heartbeat
loop renewing this replica's held leases; graceful shutdown drains the poll
loop (whose attach tasks release their leases) and aborts the heartbeat.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

- **Spec §3 coverage:** the poll loop stays topology-blind; a startup config (`--database-url`) selects each seam impl via `with_seams` ✓. All four clustered seams wired (`PostgresStore`/`PostgresConfigStore`/`DistributedBus`/`LeasedOwnership`) ✓.
- **Heartbeat ownership:** the daemon keeps a concrete `Arc<LeasedOwnership>` for the heartbeat loop and passes a `Arc<dyn Ownership>` clone to `with_seams` — `heartbeat()` is an inherent method, not on the trait, so this split is required ✓. `MissedTickBehavior::Delay` avoids a renew burst after a stall.
- **Lease lifecycle:** held leases are renewed by the heartbeat and released by each attach task on exit (Phase 2c `start_attach`); shutdown drains the poll loop then aborts the heartbeat — no lease is renewed after shutdown begins ✓.
- **Standalone unchanged:** the `else` branch is today's exact `SqliteStore` + `FleetManager::new` path; default behaviour (no `--database-url`) is byte-for-byte the same plus one log line ✓.
- **Replica-id safety:** documented as MUST-be-unique; defaults to `HOSTNAME` (unique per pod in k8s) with a local fallback — surfaced in the arg help and the startup log ✓.
- **Pool count:** four `connect(url)` pools for the first cut (each ensures its own schema via the shared `crate::pg` helper); a single shared pool is noted as a later tuning option, not built (YAGNI) ✓.
- **Testability:** the topology decision's pure pieces (`resolve_replica_id`, `heartbeat_interval`) are unit-tested; the connect-and-serve path is integration-level and covered by the core gated tests + the manual smoke checks ✓.
- **Type consistency:** `with_seams(config, store, config_store, bus, ownership)` arg order matches Phase 2c; `DistributedBus::connect(url, store)` and `LeasedOwnership::connect(url, replica_id, ttl_secs)` signatures match Phase 2b/2c ✓.
