# No local FleetManager under k8s — Design

**Ticket:** caliban-ai/prospero#83 (k8s epic #274). Follow-on to #76/#82, same daemon arm.

## Problem

`prosperod` builds a local `FleetManager` in **every** mode. Under
`PROSPERO_FLEET=k8s` the manager owns the shared `store`/`bus` (which the k8s
path borrows via `manager.store()`/`manager.bus()`), and its background poll
loop iterates an empty registry forever — harmless but wasteful, and it plants
a "local" backend object inside a k8s deployment.

Two further pieces are composed alongside the manager and, under k8s, read by
nothing: leased `ownership` and its `heartbeat`. `K8sFleet` takes no
`Ownership`, so `LeasedOwnership` hands out zero leases and the heartbeat renews
nothing.

## Goal

Under k8s, serve `K8sFleet` over an independently-composed `(store, bus)` with
**no `FleetManager`, no poll loop, and no `config_store`/`ownership`/heartbeat**.
`local` behavior stays byte-for-byte identical.

## Decision (settled)

**Drop `ownership` + `heartbeat` under k8s** (walked through and approved).
Rationale: under k8s they have no consumer — dropping them removes machinery
that already does nothing there, and takes away no capability that exists.
Multi-replica k8s stream de-duplication is a separate, already-unsolved gap; if
built later it arrives as new code with a real consumer, wired deliberately
rather than inherited by accident.

## Composition, restructured

`crates/daemon/src/main.rs`, in two phases:

**Phase 1 — the observability plane `(store, bus)`, shared by both backends.**
Branch on `--database-url`:
- clustered → `PostgresStore` + `DistributedBus`
- standalone → `SqliteStore` + `InProcessBus::new(config.event_buffer)`

Today the standalone `bus` is built *inside* `FleetManager::new`; hoisting it
out is what lets the k8s arm get a `bus` with no manager.

**Phase 2 — backend select:**
- **`local`:** additionally build `config_store` + `ownership` (per topology:
  `PostgresConfigStore`+`LeasedOwnership`+heartbeat when clustered, else
  `SqliteConfigStore`+`SelfOwnsAll`), then
  `FleetManager::with_seams(config, store, config_store, bus, ownership)` →
  `LocalFleet` → poll loop → `begin_shutdown` on drain. Both topologies now go
  through `with_seams` (standalone previously used `new`, which delegates to
  `with_seams` with the same seams — so the built manager is identical).
- **`k8s`:** `K8sFleet` over the shared `(store, bus)` (plus the #82 session-plane
  network config). No manager, no `LocalFleet`, no poll loop, no
  `config_store`/`ownership`/heartbeat.

**Retention** (`--retention-days`) runs in **both** arms off the shared `store`,
not the manager.

**Router / shutdown:** `router(fleet, admin, store, bus)` from the shared plane;
k8s has no poll loop to drain and no heartbeat handle.

## Retention helper (core, DRY + testable)

Extract the age→timestamp+prune policy currently inside
`FleetManager::prune_older_than` into a core free function so the daemon needs
no `chrono` and both arms share one implementation:

```rust
// crates/core/src/store.rs (or fleet.rs)
/// Delete events older than `max_age` from `store`. The daemon's retention
/// policy, independent of `FleetManager`.
pub async fn prune_store_older_than(
    store: &dyn Store,
    max_age: std::time::Duration,
) -> Result<u64>;
```

`FleetManager::prune_older_than` delegates to it (no behavior change). The
daemon's retention loop calls `prospero_core::prune_store_older_than(store.as_ref(), max_age)`
in both arms.

## Testing

- **Core unit test** for `prune_store_older_than`: open a `JsonlStore`, append
  events with old + recent timestamps, prune with a `max_age`, assert only the
  old ones are removed and the returned count matches. This is the one genuine
  new unit under test.
- **`FleetManager::prune_older_than` delegation** stays covered by any existing
  retention test (behavior unchanged).
- The daemon `main` restructure is not unit-testable (pure I/O composition); its
  safety net is (a) unchanged `local` behavior under the existing suite and
  (b) compile-time proof the k8s arm no longer names `FleetManager`,
  `config_store`, `ownership`, or the heartbeat.

Honest caveat: this is primarily a structural refactor — the executing-plans
discipline (small steps, gate after each) carries more of the weight than
red-green TDD.

## Scope (YAGNI)

- No new k8s-HA ownership — explicitly deferred (its own future ticket).
- No change to `local` semantics, clustered or standalone.
- No change to the API surface or the 405 behavior of the `FleetAdmin` routes
  under k8s.

## Acceptance

- `PROSPERO_FLEET=k8s` constructs no `FleetManager`, no local poll loop, and no
  `ownership`/heartbeat (verifiable by reading the k8s arm — those names do not
  appear in it).
- `local` (standalone and clustered) behaves exactly as today.
- Retention works in both arms.
- Full gate green with the CI `TESTKIT` feature set (incl. `prospero-daemon/k8s`).
