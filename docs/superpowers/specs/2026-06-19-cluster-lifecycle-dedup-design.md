# Design: Dedup clustered poll-derived lifecycle events (#59)

## Problem

In clustered mode every replica runs its own poll loop. `reconcile()`
(`crates/core/src/fleet.rs`) emits poll-derived lifecycle events —
`AgentDiscovered`, `StatusChanged`, `AgentGone` — from **each replica's own
in-memory snapshot diff**, and `RepoHealth` is emitted from both `reconcile()`
and `mark_unreachable()`. Only the *attach* path (`start_attach`) is lease-gated.
So every non-owner replica independently re-emits the same lifecycle transition,
producing duplicate rows in the durable log (and on SSE / the dashboard).

Verified in the 2026-06-19 QA run: two identical `status_changed{running→done}`
at seq 8 & 9; two identical `repo_health` at seq 1 & 2. The #49 fix prevents the
append *crash* but lands the duplicate at a distinct seq, so the logical
duplicate persists. With N replicas, each poll-derived event is emitted N times.
`AgentDiscovered` is only *accidentally* de-duped today by the spawner's
`attached_now` suppression — that masks it at 2 replicas but breaks at 3+.

## Approach: per-repo lifecycle lease

Split event emission by its **source**, and make each source single-writer:

| Event source | Events | Single-writer mechanism |
|---|---|---|
| **Poll observation** (the `list()` reply diff) | `AgentDiscovered`, `StatusChanged`, `AgentGone`, `RepoHealth` | **repo lifecycle lease** (new) — key `repo:<name>` |
| **Attach stream** | `Output`, `ToolStarted`, `ToolFinished`, `AgentFinished` | **per-agent lease** (existing, `start_attach`) |
| **Spawn** | `AgentSpawned` | emitted once by the spawning replica (unchanged) |

A replica emits poll-derived lifecycle events for a repo **only if it owns that
repo's lifecycle lease**. The lease key is `stream_key_for(repo, "")` =
`repo:<name>` — the same key `RepoHealth` events already use, so the lease that
gates a repo's lifecycle emission keys cleanly off that repo's event stream.

This reuses the existing `Ownership` abstraction unchanged
(`try_acquire`/`owns`/`renew`/`release`, the Postgres `leases` table, the
in-memory `held` map, and the heartbeat task that renews all held leases). No
new trait, table, or column.

### Why this over the alternatives

- **Per-agent gating** (gate `StatusChanged`/`AgentGone` on `owns(agent_id)`)
  breaks two cases: `AgentDiscovered` fires *before* the per-agent lease is
  acquired (emit at the per-record loop, attach at the end of `reconcile`), and
  `StatusChanged{→terminal}` fires *after* the attach task releases the lease on
  finish — both would be dropped. And `RepoHealth` has no per-agent lease, so it
  would still need a separate repo-level election. The repo lifecycle lease
  sidesteps all three because it is independent of per-agent attach lifecycle.
- **Store-level idempotency** changes the seq/append model and needs a stable
  fingerprint for two legitimately-identical consecutive transitions. Overkill.

## Mechanics

### Acquire / steal cadence

`poll_repo_once` computes ownership once per cycle, before reconciling:

```rust
let own_lifecycle = self.inner.ownership
    .try_acquire(&stream_key_for(repo, "")).await.is_some();
```

`try_acquire` semantics (already implemented in `LeasedOwnership`):
- owner → idempotently re-confirms (`Some`) → keeps emitting;
- non-owner, lease live → `None` → stays silent;
- non-owner, lease expired (owner died) → steals with bumped epoch (`Some`) →
  becomes the new emitter. **Failover is automatic** and the held lease is
  renewed by the existing heartbeat task.
- `SelfOwnsAll` (standalone) → always `Some` → lifecycle always emitted →
  **standalone behavior unchanged**.

The `own_lifecycle` bool is threaded into `reconcile(...)` and
`mark_unreachable(...)`, which gate every lifecycle `emit(...)` call on it.

### What stays ungated

- **Snapshot updates** (`snapshot.write()` of `r.health` and `r.agents`) run on
  every replica every poll. Non-owners keep an accurate `prior` snapshot, so
  when one later steals the lease its `prior == current` and it emits future
  transitions cleanly — no spurious replay burst on handoff.
- **`to_attach` / `start_attach`** stays gated on the **per-agent** lease.
  Attach (content streaming + #51 failover) is orthogonal to repo lifecycle
  ownership: a replica may own an agent's content lease (failover holder) while a
  different replica owns the repo lifecycle lease. Each event type is still
  single-sourced; the store orders them by seq.

### Repo health in a cluster

Repo health is inherently a per-replica observation (replica A may reach
caliband while B cannot). Today both emit, producing cross-replica flapping in
the log. Under this design the durable log reflects the **single authoritative
view of the lifecycle-lease owner** — consistent and non-flapping, strictly
better than today. A transient unreachable seen only by a non-owner is not
logged; that is the accepted trade for a coherent single-writer log.

## Scope of change

- `crates/core/src/fleet.rs` only:
  - `poll_repo_once`: compute `own_lifecycle`, pass to `reconcile` /
    `mark_unreachable`.
  - `reconcile(repo, records, client, own_lifecycle)`: gate the
    `AgentDiscovered`, `StatusChanged`, `AgentGone`, and `RepoHealth`(→Healthy)
    emits on `own_lifecycle`. Keep snapshot writes and `start_attach` ungated.
  - `mark_unreachable(repo, reason, own_lifecycle)`: gate the `RepoHealth`
    (→Unreachable) emit on `own_lifecycle`.
- No changes to `Ownership`, the lease table, the store, or the wire.

## Testing (TDD)

Unit tests in `fleet.rs` (the suite already has `with_seams(..., ownership)` and
a `NeverOwns`/custom-ownership pattern):

1. **Standalone unchanged:** with `SelfOwnsAll`, a status transition across two
   `reconcile` passes emits exactly one `StatusChanged` (regression guard;
   existing lifecycle tests must stay green).
2. **Non-owner suppresses lifecycle:** with an `Ownership` that returns `None`
   for `try_acquire("repo:<name>")`, a `reconcile` pass over records that would
   normally emit `AgentDiscovered`/`StatusChanged`/`AgentGone` emits **none** of
   them to the store/bus.
3. **Owner still emits:** with an `Ownership` that grants the repo key, the same
   pass emits exactly one of each expected event.
4. **Two replicas, one event:** two `FleetManager`s over a shared sqlite store
   (the established two-replica simulation), ownership granting the repo lease to
   exactly one — assert the shared store ends with a single `StatusChanged` for a
   transition both observe (the direct regression for #59).
5. **RepoHealth gated:** a non-owner observing reachable→unreachable emits no
   `RepoHealth`; the owner emits one.

## Out of scope

- Changing `AgentSpawned`-vs-`AgentDiscovered` semantics when spawner ≠
  lifecycle-owner (both remain single, distinct events — not a duplicate).
- Any change to per-agent failover (#51), the seq-conflict retry (#49), or the
  registry refresh (#50).
