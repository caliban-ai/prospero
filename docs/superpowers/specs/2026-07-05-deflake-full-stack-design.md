# De-flake `cli_drives_the_full_stack` — Design

**Ticket:** caliban-ai/prospero#85 (`kind/flake`). Flaked on CI for #76 (PR #81) and again for #83 (PR #92, coverage gate).

## Symptom

`crates/cli/tests/e2e_smoke.rs::cli_drives_the_full_stack`: after
`prospero workspace config repo --provider ollama`, the immediately-following
`GET /api/workspaces` sometimes returns the workspace with `config: {}`. Passes
reliably locally; flakes under CI load; clears on re-run.

## Root cause (a real product race, not just a test-timing issue)

Two code paths mutate the in-memory registry, and they interleave badly:

- **`FleetManager::set_repo_config_registry_only`** (config write): takes the
  registry write lock, calls `reg.set_config(repo, cfg)`, **releases the lock**,
  then persists via `config_store.upsert_repo(&record)` **outside** the lock.
- **`FleetManager::refresh_registry_from_store`** (runs every poll cycle, #50):
  reads `config_store.list_repos()` **before** taking the lock, then takes the
  write lock and **wholesale-replaces** `reg.workspaces = durable`.

Racy interleave:
1. poll's `refresh` reads `list_repos()` → durable still has `config: {}`.
2. `set_config` writes the in-memory registry (`ollama`) and persists it.
3. poll's `refresh` takes the lock and replaces `reg.workspaces` with the stale
   `durable` it read in step 1 → the in-memory `ollama` is **clobbered back to
   `{}`**.

`GET /api/workspaces` serves `snapshot()`, which joins config from the live
registry (fleet.rs:515) — so it faithfully reports the clobbered `{}`. The next
poll re-reads `list_repos()` (now durable = `ollama`) and self-heals — hence
transient, load-sensitive, and cleared-on-re-run. The `refresh` doc even claims
it's "a cheap, idempotent no-op" for standalone; the wholesale replace is **not**
idempotent against a concurrent local write.

## Fix — both layers (as requested)

### B — close the prod race (root cause)

Serialize the two operations on the **async** registry `RwLock` by holding it
across the config-store I/O in each:

- `set_repo_config_registry_only`: hold `registry.write()` across the
  `config_store.upsert_repo(...)` (move the persist inside the guard).
- `refresh_registry_from_store`: hold `registry.write()` across the
  `config_store.list_repos()` read + the wholesale replace (move the read inside
  the guard).

Because both critical sections now span their config-store await, they can't
interleave: whichever grabs the registry lock first runs read-modify/replace to
completion before the other, and in **either** order the in-memory config ends
up `ollama`. The registry RwLock is `tokio::sync::RwLock`, so awaiting under the
guard is sound; config-store impls never re-enter the registry lock, so there's
no lock-ordering inversion / deadlock.

*Tradeoff:* `refresh` now holds the registry write lock during one
`list_repos()` per poll (~2 s). Standalone (sqlite) is sub-millisecond;
clustered (Postgres) adds a few ms of registry-read contention per poll —
acceptable, and correctness-first. Documented inline.

### A — make the e2e assertion resilient (defense in depth)

`/api/workspaces` serves the poll-refreshed snapshot; health/agent-count are
legitimately eventually-consistent (the workspace goes `unreachable` right after
the config-triggered restart). A robust e2e assertion shouldn't assume the
snapshot reflects a write within microseconds under load. Replace the single
immediate read with a **bounded poll** (retry `GET /api/workspaces` until
`config.provider == "ollama"`, ~5 s deadline, then assert). This keeps the test
deterministic even against the eventually-consistent parts of the endpoint, and
guards against regressions independent of B.

## Testing

**B — deterministic regression test** (`crates/core/src/fleet.rs` test module):
A controllable `ConfigStore` double, `SlowListConfigStore`, whose `list_repos`
snapshots its state, **then sleeps** (opening the exact window), then returns the
pre-sleep snapshot. Test flow:
1. Build a `FleetManager` via `with_config_store(config, store, slow_cfg)`,
   `autostart=false`; `add_repo("r", root)` (durable + registry get `config:{}`).
2. Spawn `refresh_registry_from_store()` (with the fix it holds the registry lock
   across the slow `list_repos`).
3. After a short delay (refresh is mid-read), call
   `set_repo_config_registry_only("r", { provider: "ollama" })`.
4. Await the refresh task; assert the registry/snapshot config for `r` is
   `ollama`, **not** clobbered to `{}`.

This fails deterministically **before** the fix (refresh's lock-free read lets
`set_config` interleave, then the replace clobbers) and passes **after** it.

**A — the e2e** (`cli_drives_the_full_stack`) becomes deterministic via the
bounded poll; validated by running it under CPU load many times locally.

## Scope (YAGNI)

- No registry versioning / generation counters — the lock-serialization fix is
  simpler and sufficient.
- No change to `refresh`'s clustered convergence semantics (it still replaces
  from durable; it just does so atomically w.r.t. local writes).
- No change to the `/api/workspaces` contract or `snapshot()`'s config join.

## Acceptance

- The registry race is closed: a concurrent `set_config` + `refresh` never leaves
  the in-memory config clobbered (deterministic test proves it).
- `cli_drives_the_full_stack` is deterministic under CI load.
- Full gate green with the CI `TESTKIT` feature set.
