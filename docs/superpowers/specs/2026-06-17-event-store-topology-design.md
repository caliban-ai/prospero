# Design: Topology-configurable event storage for Prospero

**Date:** 2026-06-17
**Status:** proposed (design approved in brainstorm; pending spec review)
**Input:** [`docs/superpowers/specs/2026-06-17-event-store-topology-handoff.md`](2026-06-17-event-store-topology-handoff.md)
**Supersedes design-wise:** the open question framed by the handoff ("sqlite vs Postgres")

---

## 1. Summary

The question was never "sqlite vs Postgres." It is **which deployment topology Prospero
targets**, and the storage backend falls out of that. The decision: **support both topologies
as first-class, user-selected deployment configurations**, served by a **single `prosperod`
runtime** whose distributed concerns sit behind three trait seams. "Standalone" is the
*degenerate wiring* of the same runtime, not a separate binary or code path.

- **`standalone`** — one `prosperod` owns its host's agents; local single-writer storage.
  On k8s: a StatefulSet/Deployment with a PVC. This is today's architecture, preserved.
- **`clustered`** — N stateless `prosperod` replicas behind a Service share one store, with
  per-agent ownership, cross-replica live distribution, and lease-based failover. On k8s: a
  Deployment + HPA + managed Postgres.

Neither is one-size-fits-all: `standalone` is cheaper and simpler; `clustered` buys
history-survives-any-pod-death and live-session failover at the cost of an external Postgres
and more moving parts. The customer chooses.

## 2. Background (ground truth)

Prospero is a control plane *over* `caliband` (ADR 0002), coupled only via the ndjson wire
format (ADR 0003). Caliban exposes **only live state** and keeps no history; Prospero persists
a normalized fleet event journal to satisfy "observe = live + history" (ADR 0004).

- `crates/core/src/event.rs` — `FleetEvent { seq, ts, repo, agent_id, kind }`; `seq` is a
  global monotonic counter assigned by the `FleetManager` (single-writer assumption).
- `crates/core/src/store.rs` — the `Store` trait (`append` / `replay(agent_id, from_seq)` /
  `high_water` / `writable`) and `JsonlStore`, the append-only first-stab impl.
- ADR 0004 — the hybrid model: poll for fleet status, attach on demand for per-agent detail,
  append every event to a `Store`, serve a client as replay-then-live-tail joined on `seq`.
  Three load-bearing assumptions: **single-writer `seq`**, **in-process broadcast bus**,
  **best-effort durability that never blocks live SSE**.

**A key clarification that reframes "HA."** An agent's *execution* lives in `caliband`, not in
`prosperod`. Prospero asks caliban to spawn an agent, then *observes* it. So when a `prosperod`
dies, the agent keeps running — there is nothing to "fail over" in the execution sense. What a
`prosperod` death actually costs is: live watchers disconnect; events emitted during the
downtime window are never appended (and caliban can't backfill, so they're permanently lost —
the same loss ADR 0004 already tolerates); interactive control is unavailable. On restart the
poll loop re-discovers the still-running agent and re-attaches.

"HA" therefore decomposes into three guarantees with very different costs:

1. **History HA** — after a pod/volume dies, the durable record is intact and any surviving
   replica can serve replay. A **shared store** buys this directly. *Cheap.*
2. **Live-session failover** — a different replica takes over attach + control of an in-flight
   agent without waiting for the dead pod to reschedule. Needs the **lease** + **cross-replica
   bus**. *Expensive — the hard part of `clustered`.*
3. **Orphan prevention** — caliban agents don't leak when their owner dies. This is a
   **reconciliation loop**, needed in *both* topologies; a shared store does **not** solve it
   by itself.

`clustered` delivers all three; `standalone` delivers history-HA-on-same-volume + orphan
prevention via the same reconciliation loop, but not cross-pod live failover.

## 3. Architecture — one runtime, three seams

One `prosperod` binary, one core loop (poll caliban `List` → attach on demand → normalize to
`FleetEvent` → append + publish). The loop is **topology-blind**; a startup config selects the
implementation of each seam.

| Seam | `standalone` | `clustered` |
|------|--------------|-------------|
| **`Store`** *(exists)* | `SqliteStore` (PVC) | `PostgresStore` |
| **`EventBus`** *(new)* | `InProcessBus` (today's broadcast) | `DistributedBus` (LISTEN/NOTIFY doorbell) |
| **`Ownership`** *(new)* | `SelfOwnsAll` (no-op lease) | `LeasedOwnership` (lease + reaper) |
| **`ConfigStore`** *(new)* | sqlite | Postgres |

Both `Store` impls (and both `ConfigStore` impls) go through **`sqlx`**, so the SQL is written
once and the sqlite-vs-Postgres difference is mostly the connection string plus a couple of
dialect details.

### 3.1 `seq` becomes per-stream, with a storage-layer `global_ordinal`

`replay(agent_id, from_seq)` is **already per-agent**, so we generalize `agent_id` to a
**stream key**: every event belongs to exactly one ordered stream —

- agent events → stream = `agent_id`
- repo-level events (`agent_id == ""`, `repo` set) → stream = `repo:<name>`
- fleet-level events (both `""`) → stream = `fleet`

`high_water` becomes `high_water(stream_key)`; `UNIQUE(stream_key, seq)` is the Postgres
backstop. In `standalone`, one writer owns every stream so monotonicity is free (today's
behavior). In `clustered`, each stream is owned by one replica, so `seq` stays monotonic
*per stream* with no cross-replica coordination.

**`seq` is writer-assigned (before append), not store-assigned.** This is what preserves the
ability to give a live event a stable identity even when its durable append fails (see §4).

**Global order without a global counter.** Per-stream `seq` gives up a *stored global
monotonic integer* across all events. That integer has **no correctness role** here — agent
streams are independent, with no cross-stream causality the system tracks — so it is purely
presentational. We recover it cheaply where it's actually consumed:

- A storage-only **`global_ordinal`** column (Postgres `BIGSERIAL` / sqlite rowid) records the
  order events landed in the durable log. It is **not** part of the `FleetEvent` contract and
  the live bus never touches it. Fleet-wide *read* queries (e.g. a cross-agent timeline) order
  by `global_ordinal` for a stable total order; for a timeline this is better than `ts`
  (monotonic, no clock skew).

This separates the two concepts the current `u64 seq` conflates: the **consumer-visible
per-stream sequence** vs. the **storage insertion order**. (Considered and rejected:
DB-assigned global `seq`, which would invert seq-before-publish and break ADR 0004's live
durability posture; and a lease/HLC global sequencer, which adds coordination for an order no
more meaningful than `global_ordinal`.)

### 3.2 `EventBus` — live distribution

Trait: `publish(event)` and `subscribe(stream_key) -> stream<FleetEvent>`. The watch handler
above it is topology-blind: capture high-water → `store.replay(X, from)` for history →
`bus.subscribe(X)` for live → dedup the overlap on `seq`.

- **`InProcessBus` (standalone):** today's `tokio::broadcast`. `publish` fans to local
  subscribers. Unchanged.

- **`DistributedBus` (clustered):** the problem is a client on replica-1 wanting live events
  for agent X that replica-2 owns and ingests. Mechanism: **`LISTEN/NOTIFY` as a doorbell,
  the store as the transport.**
  - The owner replica appends the event to Postgres (gets `seq` + `global_ordinal`), then
    `NOTIFY prospero_events '<stream_key>:<high_seq>'`. **The payload is a pointer, not the
    event** — sidesteps NOTIFY's ~8 KB cap and keeps Postgres the single source of truth.
  - A replica with a client watching X holds one `LISTEN` connection; on a wakeup for X it runs
    `replay(X, last_seen_seq)` and pushes the delta to its SSE client. The "live tail" on a
    non-owner is **replay-the-delta on each doorbell**, which naturally batches.

  **Escape hatches, both behind the same trait:** (a) drop the doorbell and short-poll
  `replay(X, last_seq)` on a timer — same read path; (b) if Postgres NOTIFY fanout is ever the
  bottleneck at scale, swap the transport for NATS/Redis — a transport swap *inside* the trait,
  deferred until measured.

### 3.3 `Ownership` — leases, the reaper, and failover

Trait: `try_acquire(stream_key) -> Option<Lease>`, `renew(&Lease)`, `release(&Lease)`,
`owns(stream_key) -> bool`.

- **`SelfOwnsAll` (standalone):** `try_acquire` always returns a lease, `owns` is always true,
  `renew`/`release` are no-ops. No lease table. One writer owns every stream.

- **`LeasedOwnership` (clustered):** one Postgres row per active stream —
  `(stream_key, owner_replica_id, epoch, expires_at)`. `try_acquire` is
  `INSERT ... ON CONFLICT WHERE expired`; the owner heartbeats to extend `expires_at`; `renew`
  fails if the row was stolen, which is how a replica learns it lost ownership.

**One reconciliation loop, shared and topology-blind** — the existing poll loop plus three
lines:

```text
for agent in poll_list():            # caliban's ground truth
    if not ownership.owns(agent):
        if ownership.try_acquire(agent):   # standalone: always; clustered: only if free/expired
            attach_and_ingest(agent)        # adopt → re-attach to caliban's live stream
```

This single loop is **the orphan reaper** (an agent caliban runs but no replica owns is picked
up within one tick), **the restart-rediscovery path** (standalone re-adopts after a restart),
and **the failover-takeover path** (a dead owner's lease expires, a peer acquires and
re-attaches). Takeover latency = lease TTL + tick interval, both tunable.

**Graceful release** extends the existing drain-on-shutdown path (#29/#41): `release()` held
leases on the way out so a clean rollout hands off ownership immediately instead of waiting for
TTL expiry.

**Fencing (the "two writers" hazard).** A paused-then-resumed replica may believe it still
holds a since-stolen lease. Two layered defenses:

- *Appends are fenced by the store:* `UNIQUE(stream_key, seq)` means a stale writer collides
  with the new owner's advanced `seq` and fails — it cannot corrupt the log, and the failure
  tells it it lost ownership.
- *Interactive control needs a fencing token:* the lease carries a monotonic `epoch`, and
  input-to-caliban must present it so stale-epoch writes are rejected. **Enforcement is an
  upstream caliban dependency.** For the first `clustered` cut the lease *carries* the epoch but
  full control-fencing is **deferred** (this is the same single-writer-lease concern the
  interactive-control handoff flagged; we give it a home, not full closure).

### 3.4 `ConfigStore` — Registry + per-repo provider config

Config (the Registry `name → path`, and per-repo provider config from #21/#42) is small,
read-often, written-rarely, and must be shared across replicas in `clustered`. It sits behind
its own `ConfigStore` seam (distinct from `Store` because the access pattern is key-value
upsert/read, not append/replay), with sqlite (standalone) and Postgres (clustered) impls via
the same `sqlx` plumbing, sharing the same physical database as events.

Cross-replica config-cache invalidation reuses the **same `LISTEN/NOTIFY` doorbell**: a write
fires `NOTIFY prospero_config`, peers drop their cached copy and re-read.

**Gonzalo is out of scope** for this design. The `ConfigStore` seam does not preclude a future
Gonzalo-backed impl, but no such impl is in scope here.

## 4. Durability posture (config-dependent)

ADR 0004's "live is never blocked by a failed append" becomes a **`standalone`** property.
`clustered` is **durable-first**: because cross-replica live delivery reads from the store, an
event that fails to append is never seen by other replicas' watchers — you only go live once
you're durable. This is a deliberate, *stronger* guarantee for the HA-seeking customer who
chose `clustered`, and it is documented in the new ADR (see §7).

| | `standalone` | `clustered` |
|---|---|---|
| Durability vs. live | best-effort; live never blocked (ADR 0004) | durable-first; live gated on successful append |
| Failed append marker | `StorePersistFailed { lost_seq }` on the local bus | append must succeed before the doorbell fires |

## 5. Sequencing

Land the *shape* first, the expensive `clustered` impls last.

- **Phase 0 — Seam refactor, zero behavior change.** Introduce `EventBus` and `Ownership`;
  rewire today's daemon as `InProcessBus` + `SelfOwnsAll`; move `seq` to per-stream and
  formalize `global_ordinal`. Standalone behaves identically — but the seams are proven against
  the real working path before any distributed code exists.
- **Phase 1 — sqlite `Store` + `ConfigStore` via `sqlx`.** Rescopes #3 (sqlite-backed Store)
  and reframes #4 (retention as sqlite `DELETE`-by-age / partitioning). Indexed
  `(stream_key, seq)` replay; `global_ordinal` as the rowid. Standalone fully on sqlite.
  **Ships value; no clustered code yet.**
- **Phase 2 — Postgres impls behind the same seams.** `PostgresStore` / `PostgresConfigStore`
  (reuse Phase-1 SQL), `DistributedBus` (NOTIFY doorbell), `LeasedOwnership` (lease table +
  the reconciliation loop from Phase 0, now with a non-trivial impl). Plus k8s artifacts:
  standalone = StatefulSet/Deployment + PVC; clustered = Deployment + HPA + managed Postgres.
- **Phase 3 — deferred / dependency-gated.** API auth (#2, urgent once a Postgres tier is
  exposed beyond localhost) and epoch control-fencing (needs caliban-side enforcement).

## 6. Dev/test

`sqlx` lets local dev and tests run against sqlite (fast, no daemon, no container) while
Postgres is the deployed `clustered` backend. The key artifact is a **conformance suite**: one
trait-level test battery for `Store`, `EventBus`, and `Ownership`, run against *both* impls
(Postgres via testcontainers in CI), so parity is enforced rather than hoped for.

## 7. ADR impact (immutability respected)

Existing ADR bodies are **not edited**. Changes happen only via new records plus the sanctioned
status/link lifecycle update on the superseded record.

- **New ADR (next number, e.g. 0005)** captures the topology-configurable storage decision:
  per-stream `seq` + `global_ordinal`, the three seams, and the config-dependent durability
  posture. It **supersedes ADR 0004** — 0004's body is unchanged; its status flips to
  "superseded by NNNN" with a bidirectional link.
- The same new ADR (or, if substantive enough to stand alone, a second new ADR) **revisits the
  control-plane-topology stance of ADR 0002** (1:1-with-host → config-selectable co-located vs.
  shared tier). ADR 0002 receives only a reference/lifecycle link if warranted — **no content
  edits**.
- ADR 0003 (ndjson wire) is unaffected.

## 8. Affected tickets

| Item | Impact |
|------|--------|
| **#3** sqlite Store | Rescoped to Phase 1: sqlite `Store` via `sqlx`, per-stream `seq` + `global_ordinal`. |
| **#4** retention/rotation | Reframed as sqlite/Postgres retention (`DELETE` by age / partitioning). |
| **#1** multi-host fleet | `clustered` subsumes it (one store, any replica sees all). Multi-node *standalone* aggregation is out of scope (see §9). |
| **#2** API auth | Phase 3; urgent for a Postgres tier beyond localhost. |
| **#5** richer dashboard | Benefits — fleet-wide queries become one indexed query in `clustered`; `global_ordinal` gives a stable timeline order. |
| **#21/#42** per-repo provider config | Moves behind `ConfigStore`; shared across replicas in `clustered`. |

## 9. Scope boundaries

- **In scope:** the `Store` / `EventBus` / `Ownership` / `ConfigStore` seams; the per-stream
  `seq` + `global_ordinal` model; sqlite (standalone) and Postgres (clustered) backends; the
  reconciliation/lease/reaper loop; the phased rollout.
- **Out of scope:** cross-host fleet aggregation for a **multi-node standalone** deployment
  (#1's scatter/gather — elaborated in §9.1); Gonzalo; full epoch control-fencing enforcement
  (caliban upstream dependency); API auth design (#2, tracked separately).

### 9.1 Why multi-node standalone aggregation is deferred

There is a **third** deployment shape lurking behind the two this spec designs, and we are
explicitly *not* solving it: **multi-node standalone** — many independent `prosperod`
instances (e.g. a DaemonSet), each owning its own host's agents in its own local sqlite store,
with a desire for a **single fleet-wide view spanning all of them**.

**Why it's a genuinely separate problem.** Neither config we ship solves it:

- A single `standalone` only knows its own host — it is an island of truth.
- `clustered` *does* give a unified fleet view, but its unity comes from the **shared store**,
  which multi-node standalone deliberately rejects. You cannot get there by configuring either
  one; it needs a **new component**: an aggregator / query-federation tier that fans out to
  each standalone `prosperod` over RPC (the gRPC/HTTP transport the framework spec §9/§10 and
  ticket #1 already gesture at) and merges at query and stream time.

**What that aggregator actually entails — i.e. what would balloon this spec.** Scatter/gather
is not one feature; it is a subsystem:

- **Query fan-out + partial-failure semantics** — a fleet-wide list/timeline must hit every
  node's store and merge, with defined behavior when a node is down (degraded/partial results).
- **Live stream fan-in** — tailing "the whole fleet" means subscribing across N nodes and
  merging N live streams, with backpressure and cross-node ordering.
- **Cross-node ordering** — there is no shared `seq`; merging happens by `ts` / per-node
  `global_ordinal`.
- **Membership/discovery** — which nodes exist, their health, join/leave.
- **Cross-node transport + auth** — the same authn/authz (#2) the rest of the system defers,
  now on a node-to-node hop, with the `host` identity on the wire becoming load-bearing.

Each of those is a project in its own right. Letting them in would turn "how is an event
durably stored and distributed" into "...and also a distributed query-federation, membership,
and multi-transport layer."

**Why deferring is safe — the seams make it additive, not a rewrite.** Nothing in this design
forecloses it:

- The **per-stream `seq` + `global_ordinal`** model already abandons any global-counter
  assumption, so cross-node merge uses exactly the `ts` / per-node-ordinal discipline we
  adopted here — there is nothing to unwind later.
- The `Store` / `EventBus` traits are **per-node-local**. An aggregator sits *above* them as a
  **consumer** (calling `replay` / `subscribe` against each node's `prosperod` via RPC), not
  inside them — a new tier layered on top, not a change to the storage contract.
- The `host` identity already exists on the event type; the only missing piece is the
  transport, which is ticket #1's job, not this spec's.

**The honest gap this leaves.** A customer who wants many hosts, *rejects* a shared Postgres
(ops aversion, append latency, or per-host data-locality/compliance), **and** wants a single
pane of glass has **no answer in this spec** — they must either accept `clustered` or wait for
a future federation tier. We judge that acceptable because the large majority of "unified fleet
view at enterprise scale" demand is precisely what `clustered` exists to serve; multi-node
standalone federation is a narrower, later constituency.

**Relationship to ticket #1.** #1 ("multi-host fleet") therefore **bifurcates** rather than
being "done": it is *subsumed* for `clustered` (the shared store already yields a fleet-wide
view) and *deferred* for `standalone` (this federation tier). #1 should be split to reflect
that.

## 10. Open questions / deferred dependencies

- Lease TTL and reconciliation tick defaults (tunable; pick sane values in Phase 2).
- Exact `global_ordinal` semantics under concurrent Postgres writers (commit order — a valid
  linearization; confirm it satisfies the #5 timeline needs).
- Caliban-side epoch enforcement for control-fencing (Phase 3 dependency).
- Postgres operational choice for `clustered`: managed (RDS/CloudSQL) vs. operator (CNPG) —
  deployment concern, not a `Store` design concern.

## 11. Pointers

- `crates/core/src/store.rs` — `Store` trait + `JsonlStore`.
- `crates/core/src/event.rs` — `FleetEvent`, `EventKind`.
- `docs/adr/0002-control-plane-over-caliband.md`
- `docs/adr/0004-hybrid-live-and-durable-observability.md` — superseded by the new ADR.
- `docs/superpowers/specs/2026-06-17-event-store-topology-handoff.md` — the brainstorm input.
- `docs/superpowers/specs/2026-06-11-interactive-agent-control-handoff.md` — single-writer-lease note.
