# Design: harden K8sFleet session plane — prospero #77

- **Date:** 2026-07-05
- **Issue:** caliban-ai/prospero#77 · follow-up to #64 · relates #76 (API reroute, merged) · epic #274
- **Scope (chosen):** I1 full + M1 fix + M2 refactor.

## Problem

Three follow-ups from the #64 whole-branch review, now timely since the API
reroute (#76) landed:

- **I1 (Important) — stream leg unproven over TCP.** `k8s_session_plane.rs`
  networks only the *control* connection over TCP+TLS; the per-agent stream
  endpoint from `attach` is a same-process **Unix** socket (a `FakeCaliband`
  limitation noted in #71). In production the pod caliband must return a
  **routable TCP** endpoint from `attach`, or `open_stream` →
  `CalibandUnreachable` and the attach loop retries forever. Nothing proves the
  stream leg works over the network.
- **M1 (Minor) — restart naming vs `task_name` idempotency.**
  `K8sFleet::restart_agent` names the fresh CR `restart_name(old, nonce)`, not
  `task_name(spec)`. Dormant today (ensure_agent is called once per explicit
  spawn), but a declarative reconcile that drives `ensure_agent` from
  desired-state would compute `task_name(spec)` (the *original* name), find
  nothing, and apply a **duplicate** CR.
- **M2 (Minor) — per-subscription `watch_fleet`.** Each `watch_fleet()` spawns a
  fresh poll loop from an empty snapshot, so every subscriber re-reports all
  present agents as `Discovered`, and an agent deleted before a subscriber's
  first `list()` never yields `Gone` for that subscriber.

## Design

### M2 — one shared poll loop; `watch_fleet` = seed + tail

Mirror `LocalFleet`'s seed-then-tail shape:

- `K8sFleet` gains a `changes: broadcast::Sender<FleetChange>` (created at
  construction) and spawns **one** canonical poll-diff loop that owns the
  authoritative `known: HashMap<name, (AgentStatus, workspace)>` and broadcasts
  each diff exactly once. The loop lives for the fleet's lifetime (the fleet is
  `Arc`-held by the daemon); a `watch_poll_interval` still tunes cadence.
- `watch_fleet()` no longer spawns a poll loop. It:
  1. subscribes: `let mut rx = self.changes.subscribe();`
  2. **seeds** from a fresh `snapshot()` — yields `Discovered` for each present
     agent, recording their ids in a `seen` set;
  3. **tails** `rx`: forwards each `FleetChange`, skipping a `Discovered` whose
     id is already in `seen` (the seed/tail overlap);
  4. on `broadcast::error::RecvError::Lagged`, **re-seeds** from `snapshot()`
     (bounded self-heal, mirroring the bus tail) and continues;
  5. on `Closed`, ends the stream.
- Returns `BoxStream<'static, FleetChange>` built with `async_stream::stream!`.

**Exactly-once `Gone`:** the shared loop broadcasts `Gone` once; every live
subscriber receives it. A subscriber that joined *after* the delete never saw
the agent (its seed omits it), so no spurious `Gone` — consistent.

**Construction:** the poll loop starts in `with_poll_config` (the single
constructor path). Test constructors get the same loop with the short
`watch_poll_interval`. A `broadcast` buffer of 256 tolerates burst diffs; the
`snapshot()` re-seed covers any lag.

### I1 — `FakeCaliband` per-agent stream over TCP+TLS

Extend the harness (behind `testkit`):

- `spawn_stream_listener` gains a TCP+TLS sibling that binds via
  `transport::Listener::bind(BindSpec { Tcp, tls, token })` (the seam added in
  #71) and serves the same scripted NDJSON frames over the accepted `BoxConn`.
- `FakeCaliband::start_tcp_tls` records its TLS material so per-agent stream
  listeners reuse it, and its `Spawned`/`AttachAck` replies advertise a **TCP**
  `Endpoint { addr }` for the per-agent stream (not a Unix path). A per-agent
  TCP listener binds `127.0.0.1:0`; the resolved `host:port` is what `attach`
  returns.
- New integration test (`k8s_session_plane.rs` or a sibling):
  `K8sFleet::start_agent_stream` dials the control endpoint over TCP+TLS+token,
  `attach` returns a **TCP** per-agent endpoint, `open_stream` dials *that* over
  TCP+TLS, and the scripted frames normalize into the shared store under the
  agent's stream key — proving the whole stream leg is network-routable.

### M1 — restart reuses `task_name(spec)`

- `restart_agent(id)`: read the old CR, take its `spec`, compute
  `task_name(&spec)` (the original spec-deterministic name), **delete** the old
  CR, **wait** (bounded poll) for `get(name)` to return `None`, then **apply** a
  fresh CR under the *same* `task_name`. Return `AgentId::from(task_name)` (the
  same stable id — the trait allows "possibly new").
- Delete `restart_name` + the `restart_nonce` field.
- This makes CR names a pure function of spec, so `ensure_agent(same spec)`
  after a restart targets the one existing CR (idempotent) rather than applying
  a duplicate.
- The delete→apply-same-name race is closed by the wait-for-gone poll (`FakeK8s`
  deletes synchronously; real kube deletion with finalizers is covered by the
  bounded wait).

## Error handling

- `watch_fleet` `list()` failure → log + retry next interval (unchanged).
- `broadcast` Lagged → re-seed, never silently drop.
- `restart_agent` wait-for-gone timeout → `CoreError::Fleet(...)` (surfaced, not
  hung).

## Testing strategy (TDD)

1. **M2** — two concurrent `watch_fleet` subscribers over a `FakeK8s`: both see
   an applied CR as `Discovered`; deleting it yields `Gone` **once** per live
   subscriber; a subscriber that joins after the delete gets neither.
2. **I1** — the TCP per-agent stream integration test above (frames land in the
   store via a fully-networked control + stream path).
3. **M1** — `restart_agent` then `ensure_agent(spec)` observe a single CR (no
   duplicate); `restart` returns the `task_name(spec)` id; naming-determinism
   guard.
4. Existing k8s tests stay green; the shared-loop change keeps
   `watch_fleet`'s `Discovered/StatusChanged/Gone` contract.

## Consequences

- **Positive:** the k8s session plane is proven network-routable end-to-end
  (removes the last "works only same-process" caveat); `watch_fleet` scales to N
  subscribers with one poll cadence and exactly-once `Gone`; CR naming is
  idempotent, unblocking a future declarative reconcile.
- **Negative:** the shared-loop rework touches `watch_fleet` + its tests; the
  `FakeCaliband` TCP-stream extension adds harness surface. `restart_agent`
  returning the same id changes its observable behavior (documented; trait
  permits it).
- **Deferred:** native `kube::runtime::watcher` (server-side watch vs polling) —
  the poll-diff seam is unchanged, only its ownership; real-cluster endpoint
  advertisement is the operator's job (caliban #283).
