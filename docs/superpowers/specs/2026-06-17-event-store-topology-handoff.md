# Handoff: Prospero event-store topology & backend (sqlite vs Postgres)

**Date:** 2026-06-17
**Purpose:** Launch a fresh **brainstorming** session to decide how Prospero's durable
event store should be backed and deployed long-term. Start that session with the
`superpowers:brainstorming` skill — this document is the input, not a finished design.

**One-line framing:** The choice is *not* "sqlite vs Postgres." It is **which deployment
topology Prospero targets**, and the storage backend falls out of that. The owner leaned
toward **Topology B (shared control-plane tier / Postgres)** in discussion, but the brainstorm
should pressure-test that against Topology A rather than treat it as settled.

---

## 1. Background — what the event store is

Prospero is a **control plane over `caliband`** (ADR 0002), coupled to caliban only via the
ndjson wire format (ADR 0003). Caliban exposes **only live state** and keeps no history, so
Prospero persists a normalized **fleet event journal** to satisfy "observe = live + history"
(ADR 0004).

Ground-truth shape (`crates/core/src/event.rs`):

```rust
pub struct FleetEvent {
    pub seq: u64,        // monotonic, "assigned by the FleetManager" (single-writer assumption)
    pub ts: String,      // RFC-3339
    pub repo: String,    // "" for fleet-level events
    pub agent_id: String,// "" for repo-level events
    pub kind: EventKind, // AgentSpawned | AgentInit | StatusChanged | Output | ToolStarted | ... 
}
```

The `Store` trait (`crates/core/src/store.rs`) — deliberately backend-agnostic so a new
backend drops in "without touching callers":

```rust
pub trait Store: Send + Sync {
    fn append(&self, event: &FleetEvent) -> Result<()>;
    fn replay(&self, agent_id: &str, from_seq: u64) -> Result<Vec<FleetEvent>>; // per-agent, seq-ordered
    fn high_water(&self) -> Result<u64>;   // global high-water, resumes seq across restarts
    fn writable(&self) -> bool;            // cheap readiness probe
}
```

Current impl: **`JsonlStore`** — append-only `events.jsonl`, single file, replay filters by
agent. First-stab; rotation/sharding/retention deferred.

**Read path:** a client that starts watching gets **replay from `Store`** then a **live tail
from an in-process broadcast bus**, joined on the monotonic `seq` (ADR 0004).

### Key assumptions baked into the current design (these are what's up for revision)
- **Single writer.** `seq` is one global monotonic counter assigned by the `FleetManager`,
  resumed from `high_water()` at startup.
- **In-process broadcast bus.** Live tail is served from the same daemon that ingested the event.
- **Best-effort durability.** A failed `Store.append` is logged + metered but **never stops the
  live SSE** — ADR 0004 explicitly favors a never-down fleet view over guaranteed persistence.
- **Per-host scope.** "One Prospero deployment manages the local host" (spec §3). Multi-host is a
  **non-goal in the first stab**; the type carries a `host` identity but the **transport is
  deferred** (spec §9/§10: "gRPC/HTTP to remote prosperods, or remote socket relay").

### Not specced anywhere (confirmed by grep)
- No k8s / Kubernetes / StatefulSet / replica / Postgres references in `docs/`.
- The specced multi-host model keeps each prosperod a **per-host single writer** and aggregates
  at query/stream time — it does **not** imply a shared database.

So adopting a shared DB is a genuine **new architectural decision**, not an implementation detail.

---

## 2. The two approaches

### Topology A — per-host daemon, local state (current design)
Each `prosperod` owns its host's agents; storage is local and single-writer. On k8s this is a
StatefulSet (or DaemonSet) with a PVC per pod. A control plane (when multi-host lands) aggregates
remote daemons over gRPC/HTTP at query/stream time. **Backend: sqlite on a PVC** (or keep JSONL +
rotation).

**Pros**
- **Matches the current architecture** (ADR 0002/0004, spec §3/§9). No rearchitecture; `seq`,
  in-process bus, and best-effort durability all stay valid.
- **Lowest-latency append.** Local fsync, no network hop on the hot ingest path — preserves
  ADR 0004's durability posture cleanly.
- **sqlite is a fine k8s citizen** for single-writer: managed block storage (EBS/PD) on a PVC,
  pod pinned to its volume. Gives indexed `(agent_id, seq)` replay — the #3 motivation — without
  an external dependency.
- **Smallest blast radius / ops weight.** No DB to run, scale, back up, or secure.
- Unblocks #3 and #4 immediately with no upstream design dependency.

**Cons**
- **No horizontal scale of a single fleet view.** sqlite can't be shared by multiple writers, so
  you can't run N stateless API/ingest replicas over one store.
- **Pod is stateful.** Rescheduling means moving/re-attaching the PVC; HA is per-host, not via
  replica failover.
- **Cross-host aggregation is a separate transport problem** you still have to build for multi-host
  (#1) — local storage doesn't help there.
- Sharded-per-host storage makes fleet-wide queries (cost charts, cross-agent timelines — #5)
  a scatter/gather rather than one indexed query.

### Topology B — shared control-plane tier, Postgres (owner's lean)
N stateless `prosperod`/API replicas behind a Service share **one event store**. Backend:
**Postgres** (managed RDS/CloudSQL or an operator like CNPG).

**Pros**
- **Horizontal scale + replica failover.** Any replica can serve replay/SSE for any agent;
  losing a replica doesn't lose the store. Natural fit for k8s Deployments + HPA.
- **The store mechanics get easier.** `append` → `INSERT`; `replay` → indexed range scan on
  `(agent_id, seq)`; `high_water` → `SELECT max(seq) ...`. Postgres gives transactional append,
  a real sequence type, and `jsonb` payloads for free.
- **One place for fleet-wide queries** — #5 (cost charts, cross-agent timelines) becomes one
  indexed SQL query instead of scatter/gather.
- **Shared registry/config** (managed repos, per-repo provider config from #21/#42) can live in
  the same store, which multi-host (#1) needs anyway.

**Cons / what it forces (the real work — see §3)**
- **Breaks three core assumptions of ADR 0004** (single-writer `seq`, in-process bus, local-fsync
  durability). This is an **ADR-level rearchitecture**, not just #3's backend.
- **Network on the hot append path.** A failed append is now "Postgres unreachable," not a local
  disk error. ADR 0004 already tolerates failed appends, so it's not fatal — but you've traded a
  local fsync for a network round-trip and a new external dependency in the critical path.
- **Multi-writer coordination** (seq ownership, interactive-control lease) becomes mandatory.
- **The cross-replica live bus is a brand-new subsystem** (§3.2) — arguably bigger than the store.
- Heavier ops: a Postgres to run, secure (ties into #2 auth), scale, and back up.

---

## 3. What Topology B actually forces (must be designed before #3 is meaningful)

The Postgres **table** is the easy part. Choosing a shared multi-writer tier creates these
sub-problems, which change the `Store` contract and the daemon's core loop:

### 3.1 `seq` generation under multiple writers
The single global monotonic counter is a single-writer assumption. Realistic fix: **partition
ingest by agent** — each agent's stream is written by exactly one replica, so `seq` stays
monotonic *per agent*, with `UNIQUE(agent_id, seq)` in Postgres as the backstop. That implies an
**agent → replica ownership** model. (`high_water` likely becomes **per-agent**:
`high_water(agent_id)`.) Decide: per-agent seq vs global Postgres `SEQUENCE` vs hybrid.

### 3.2 Cross-replica live distribution — the heart of it
Today the live tail is an **in-process** broadcast bus. With N replicas, a client connected to
replica-1 may want live events for an agent ingested by replica-2. Options to evaluate:
Postgres `LISTEN/NOTIFY`, a dedicated pub/sub (NATS/Redis), or replay-from-DB + short poll.
**This is probably the largest single design decision and may dominate the backend choice.**

### 3.3 Multi-writer lease for interactive control
The interactive-agent-control handoff (`docs/superpowers/specs/2026-06-11-...`) already flags
**single-writer lease** as an open concern. Topology B makes it real: two replicas must not both
drive one agent's input. Needs a lease/ownership primitive (could reuse §3.1's ownership model).

### 3.4 Shared registry / config
Registry (managed repos `name → path`) and per-repo provider config (#21/#42) must be shared
across replicas — either in Postgres, or via Gonzalo (see §5).

---

## 4. Affected tickets & ADRs

| Item | Impact under Topology B |
|------|-------------------------|
| **#3** sqlite-backed Store | **Re-scope → Postgres-backed Store.** Blocked on §3.1 (seq/ownership). Consider building via `sqlx` so local dev/tests can still use sqlite while Postgres is the deployed backend. |
| **#4** retention/rotation/compaction | Reframed as Postgres retention (partitioning / `DELETE` by age / `pg_partman`) rather than JSONL rotation. |
| **#1** multi-host fleet | Substantially overlaps Topology B — the "aggregate remote prosperods" transport may be replaced/subsumed by "N replicas share one store." Reconcile the two. |
| **#2** API auth | Becomes more urgent — a shared Postgres tier exposed beyond localhost needs authn/authz and DB credential management. |
| **#5** richer dashboard | *Benefits* — one shared store makes fleet-wide cost/timeline queries a single indexed query. |
| **ADR 0004** hybrid live+durable | **Must be amended/superseded** — single-writer `seq`, in-process bus, and local-fsync durability assumptions all change. Write a new ADR. |
| **ADR 0002** control-plane-over-caliband | Revisit: does the control plane stay 1:1 with a host, or become a shared tier in front of many caliband hosts? |

If the brainstorm lands Topology B, the natural artifacts are: **a new ADR** (amending 0004) +
a **design spec** under `docs/superpowers/specs/`, then re-scoped/new tickets fall out of it.

---

## 5. The Gonzalo angle (carried over from prior discussion)

`caliban-ai/gonzalo` is "a robust, shareable persistence layer for caliban" — a **versioned,
conflict-aware `Record`/`Store`** core with pluggable substrates (fs/git/s3/remote daemon) and a
shared conformance suite. Prior conclusion:

- **The event journal is a poor fit for Gonzalo's *domain* layer** — it's single-writer-per-agent,
  append-only, seq-ordered; it doesn't need revisions or conflict-merge. Don't route hot-path
  events through Gonzalo.
- **Gonzalo *is* a good fit for Prospero's mutable config records** — Registry + per-repo provider
  config — exactly the conflict-aware, shareable-across-replicas case Topology B's §3.4 raises.
- Possible middle path: back the event Store with **`gonzalo-core`'s substrate traits** (engine
  reuse) rather than Gonzalo's domain layer — but that's an optimization, not the core decision.

Worth a deliberate decision in the brainstorm: **Postgres for everything**, vs **Postgres for
events + Gonzalo for config**.

---

## 6. Questions to resolve in the brainstorm

1. Is Topology B truly the target, or is per-host-daemon-on-k8s (Topology A) sufficient for the
   near term with the `Store` trait preserving the option? What concretely *requires* shared
   multi-writer storage — and when?
2. If B: what's the **agent → replica ownership** model (§3.1)? Does an agent pin to one replica?
3. If B: how is the **cross-replica live bus** (§3.2) realized — `LISTEN/NOTIFY`, external pub/sub,
   or replay+poll? (This likely dominates the design.)
4. How does Topology B reconcile with the already-specced multi-host transport (#1)? One mechanism
   or two?
5. `seq` semantics: keep global monotonic, or move to per-agent? What does the dashboard/replay
   join require?
6. Durability posture: does ADR 0004's "best-effort, never block live SSE" survive a network DB,
   and is that still acceptable?
7. Config storage: Postgres vs Gonzalo (§5)?
8. Migration/sequencing: can #3 ship as a `sqlx`-based Store (sqlite-now / Postgres-later) without
   committing the whole rearchitecture, or does the contract change too much to start?

---

## 7. Pointers (ground truth)

- `crates/core/src/store.rs` — `Store` trait + `JsonlStore`.
- `crates/core/src/event.rs` — `FleetEvent`, `EventKind`.
- `docs/adr/0002-control-plane-over-caliband.md`
- `docs/adr/0003-couple-to-caliban-via-ndjson-wire-format.md`
- `docs/adr/0004-hybrid-live-and-durable-observability.md` — **the ADR most affected.**
- `docs/superpowers/specs/2026-06-05-prospero-framework-design.md` — §3 (model), §9 (non-goals),
  §10 (future: multi-host transport, sqlite Store, retention).
- `docs/superpowers/specs/2026-06-11-interactive-agent-control-handoff.md` — single-writer-lease note.
- Board (GitHub Projects v2 #1): #1 multi-host, #2 auth, #3 sqlite Store, #4 retention, #5 dashboard.
- `caliban-ai/gonzalo` README — persistence-layer crate map.
