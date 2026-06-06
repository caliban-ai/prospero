# Prospero Orchestration Framework — Design

**Date:** 2026-06-05
**Status:** Approved (pending spec review)
**Scope:** First-stab framework for launching, managing, and observing multiple Caliban agents.

## 1. Summary

Prospero is the **agent orchestration layer** for the Caliban agent harness. It is a
**control plane**: it discovers and drives Caliban's existing per-repo supervisor daemons
(`caliband`), aggregates their agents into one fleet view, and exposes that fleet for
launching, managing, and observing through a CLI, an HTTP/JSON API, and a minimal web
dashboard.

Prospero does **not** re-implement process supervision — `caliband` already spawns,
lists, kills, respawns, and attaches to background agents over a Unix-socket NDJSON
protocol. Prospero sits above many calibands and adds: a fleet-wide model, durable run
**history** (caliband only exposes live state), normalized **events**, and the
observability/control surfaces (CLI, API, dashboard).

### Key decisions (from brainstorming)

1. **Role:** control plane over `caliband` daemons (delegates supervision).
2. **First-stab scope:** full vertical slice — `prospero-core` + `prosperod` + `prospero`
   CLI + read/write HTTP API + SSE + a minimal web dashboard.
3. **Coupling:** Prospero owns a **thin NDJSON client**; the caliband **wire format is the
   only contract** (no dependency on the `caliban-supervisor` crate).
4. **State/observability model:** **Hybrid (C)** — poll `List` for cheap fleet status,
   attach per-agent streams on demand (while active or watched), normalize onto an
   in-memory model **and** a durable JSONL event log behind a `Store` trait.
5. **Multiple agents per repo:** a repo hosts *N* concurrent agents (parallel streams of
   work on one codebase). **Worktree isolation is the default** for spawns; shared-tree is
   an explicit opt-out.

## 2. Domain model

```
Host ─┬─ Repo "prospero"  ─(one caliband)─┬─ Agent  (worktree: feature-x)   running
      │                                   ├─ Agent  (worktree: bugfix-y)    running
      │                                   └─ Agent  (shared tree)           idle
      └─ Repo "caliban"   ─(one caliband)─┬─ Agent  …
                                          └─ Agent  …
```

- **Agent** is the primary unit. **Repo** is a grouping that can host many concurrent
  agents. **Host** is a single machine (one Prospero deployment manages the local host in
  the first stab; the type carries a host identity so multi-host can be added without a
  model change).
- Parallel work on the same codebase is realized via caliban's `isolation_worktree`: each
  agent gets its own git worktree so concurrent edits don't collide.

## 3. Caliban integration surface (the contract)

Caliban exposes (verified against the caliban repo):

- **`caliband`** — per-repo supervisor daemon. Control socket at
  `${CALIBAN_DAEMON_RUNTIME_DIR:-$XDG_RUNTIME_DIR/caliban}/<hash16(canonical_repo_root)>.sock`
  (fallback `$TMPDIR/caliban-daemon/<hash16>.sock`), where `hash16` = first 16 hex chars of
  `SHA-256(canonical_path)`.
- **Control protocol** — newline-delimited JSON, one request → one reply per connection.
  Requests: `List`, `Spawn { spec }`, `Attach { id }`, `Kill { id }`, `Respawn { id }`,
  `Rm { id, force }`, `Status`, `Shutdown`. Replies include `Listed { agents }`,
  `Spawned { id, socket_path }`, `AttachAck { socket_path }`, `Status(DaemonStatus)`,
  and `Error { error: SupervisorError }` (`NotFound` / `InvalidState` / `Internal`).
- **`SpawnSpec`** — `{ label?, frontmatter_path?, initial_prompt, model?, tool_allowlist?,
  isolation_worktree (bool), inherit_hooks (bool, default true) }`.
- **`AgentRecord`** — `{ id, name, status, started_at, session_dir, socket_path, spec }`.
- **`AgentStatus`** — `Spawning | Running | Idle | Killed | Done | Failed | Crashed`.
- **Per-agent stream** — `Attach` returns a per-agent socket; reading it yields caliban's
  headless **stream-json** frames (`system/init`, `text{delta}`, `thinking{delta}`,
  `tool_use`, `tool_result`, `result{...}`, etc.).

Caliban exposes only **live** state — no history. That gap is Prospero's to fill.

## 4. Architecture & crate boundaries

Dependencies flow one direction: `cli`/`daemon` → `api` → `core`. Nothing depends on the
daemon.

```
prospero (CLI)  ── HTTP/JSON ─▶ prosperod ─┬─ prospero-api (axum: REST + SSE + dashboard)
                                           └─ prospero-core (model, client, fleet manager)
                                                     │ NDJSON over Unix sockets
                                                     ▼
                                   caliband(repo A)  caliband(repo B)  …
```

### `prospero-core` — orchestration brain (no web framework in its public API)

- **Domain model:** `Host`, `Repo`, `Agent`, `AgentStatus`, `FleetSnapshot`, and a
  normalized `FleetEvent` (Prospero's own type, distinct from caliban wire frames).
- **`CalibandClient`:** thin client — mirrored `CtlRequest`/`CtlReply`/`AgentRecord`/
  `SpawnSpec` serde types + NDJSON framing over `tokio::net::UnixStream`. The only coupling
  to caliban.
- **`Discovery`:** repo root → control-socket path (mirrors caliban's rule); can spawn
  `caliban` daemon (`caliband --repo-root …`) on demand (`autostart`, configurable).
- **`Registry`:** persisted set of managed repos (`name → root path`). The fleet is
  intentional, not guessed (a blind socket scan can't map hash→repo).
- **`FleetManager`:** runtime heart — per-repo poll loop, on-demand per-agent attach tasks,
  `Arc<RwLock<FleetSnapshot>>`, and a `tokio::sync::broadcast` event bus.
- **`Store`:** trait for durable history; first impl `JsonlStore` (append-only event log +
  registry persistence). sqlite can implement the same trait later.

### `prospero-api` — HTTP adapter

`axum` `Router` + DTOs + handlers over `FleetManager`. REST (read + write), SSE event
stream, and serving the dashboard's static assets. Depends only on `core`.

### `prospero-daemon` (`prosperod`) — process entry

Wires a `FleetManager` + the `api` router into a long-running server. Owns the tokio
runtime, config (bind addr, data dir, poll interval, autostart), logging, graceful
shutdown.

### `prospero-cli` (`prospero`) — operator CLI

`clap` commands that are thin calls to prosperod's HTTP API (one control surface, not two
protocols). Blocking `ureq` client keeps the CLI dependency-light. Commands:
`repo add/list/rm`, `spawn`, `ls`, `logs`/`follow`, `kill`, `respawn`, `rm`, `status`.

### Dashboard

Static `index.html` + `app.js` served by `prospero-api`; lists repos with agents grouped
underneath and live-streams a selected agent via SSE. No Node toolchain / bundler.

## 5. Data flow

### (1) Control — launching a parallel work stream

```
prospero spawn --repo prospero --prompt "refactor X" [--worktree feature-x] [--shared-tree]
  → HTTP POST /api/repos/prospero/agents { prompt, worktree?, model?, label?, isolation? }
  → FleetManager → Discovery.ensure_caliband(repo)
                 → CalibandClient.spawn(SpawnSpec { initial_prompt, model, label,
                       isolation_worktree: true /* DEFAULT; --shared-tree ⇒ false */, … })
                 ← Spawned { id, socket_path }
                 → emit Event::AgentSpawned; start attach task for id
  ← 201 { agent_id, repo, worktree }
```

Worktree-by-default is enforced at the API boundary: `isolation` defaults to `worktree`;
opting out is the explicit `--shared-tree` flag. Many spawns against one repo simply
produce many agents under that repo's single caliband.

### (2) Live state — the poll loop

```
every poll_interval (default ~2s), per managed repo:
  CalibandClient.list() → Vec<AgentRecord>
  reconcile vs in-memory FleetSnapshot:
    new id           → Event::AgentDiscovered (+ start attach if active)
    status changed   → Event::StatusChanged { from, to }
    disappeared      → Event::AgentGone
  publish diffs to the bus; update snapshot
  socket unreachable → Repo.health = Unreachable (non-fatal; backoff retry)
```

### (3) Streaming — attach, normalize, fan out

```
attach task per active/watched agent:
  CalibandClient.attach(id) → per-agent socket; read caliban stream-json frames; normalize:
    system/init          → AgentInit { model, tools, session_id }
    text {delta}         → Output { stream: Stdout, chunk }
    thinking {delta}     → Output { stream: Thinking, chunk }   (dropped by default)
    tool_use {name,input}→ ToolStarted { name, input }
    tool_result {is_err} → ToolFinished { name, ok }
    result {subtype,…}   → AgentFinished { outcome, cost_usd, turns }
  each Event → (a) broadcast bus → SSE subscribers
             → (b) Store.append(repo, agent_id, event)   // durable history
```

### Normalized event type (Prospero's stable contract)

```rust
struct FleetEvent { seq: u64, ts: String, repo: String, agent_id: String, kind: EventKind }
enum EventKind {
  AgentSpawned, AgentDiscovered, AgentInit { model, tools, session_id },
  StatusChanged { from: AgentStatus, to: AgentStatus },
  Output { stream: OutputStream /* Stdout | Thinking */, chunk: String },
  ToolStarted { name, input }, ToolFinished { name, ok },
  AgentFinished { outcome, cost_usd: f64, turns: u32 },
  AgentGone, RepoHealth { state },
}
```

Consumers never see raw caliban frames. **Observe = live + history, unified:** a client
that starts watching gets **replay** from `Store` (last N or full log) **then** a live SSE
tail from the bus — same `FleetEvent` type, joined on the monotonic `seq`. Even after an
agent finishes and caliband forgets it, Prospero retains the full story on disk.

### REST surface (first stab)

```
GET    /api/fleet                      FleetSnapshot (all repos + agents)
GET    /api/repos                      managed repos + health
POST   /api/repos                      register { name, root }
DELETE /api/repos/{name}               unregister
GET    /api/repos/{repo}/agents        agents for one repo
POST   /api/repos/{repo}/agents        spawn (worktree default)
GET    /api/agents/{id}                agent detail + latest status
GET    /api/agents/{id}/events?from=   history (replay) as JSON
GET    /api/agents/{id}/stream         SSE live tail (optional replay+tail)
POST   /api/agents/{id}/kill           kill
POST   /api/agents/{id}/respawn        respawn
DELETE /api/agents/{id}                rm
GET    /healthz                        daemon liveness
```

## 6. Error handling & resilience

Guiding rule: **a failure in one repo or one agent must never take down the fleet view or
the daemon.** Failures are surfaced as state, not propagated as panics.

| Failure | Behavior |
|---|---|
| Repo's caliband socket missing/unreachable | Poll task sets `Repo.health = Unreachable`, emits `RepoHealth`, retries with backoff. Other repos unaffected. |
| caliband not running for a registered repo | Discovery spawns `caliband --repo-root …` (`autostart=true`). Spawn failure ⇒ repo `Unreachable` with error surfaced. |
| Per-agent attach stream drops mid-run | Log + health note; re-attach with backoff if agent still in `List`; else finalize from last known state. |
| Malformed/unknown caliban frame | Skip-and-log (forward-compatible); unknown `type` never crashes the normalizer. Drift counter exposed. |
| caliband `SupervisorError` (NotFound/InvalidState) | Mapped to typed `prospero-core` error → HTTP 404/409; never 500. |
| `Store.append` (disk) fails | Log + error metric; live SSE continues. Durability degrades, fleet does not stop. |
| prosperod restart | Reload `Registry`, re-poll to rebuild `FleetSnapshot` (orphaned agents show caliban's `Crashed`); history intact; `seq` resumes from persisted high-water mark. |
| Slow/stuck SSE client | Per-subscriber bounded buffer; a client that can't keep up is dropped (lagged), never stalls the bus. |

**Backpressure:** bounded channels with drop-oldest; a dropped-count is surfaced rather
than blocking producers — favor liveness over guaranteed delivery (first stab).

**Error types:** `prospero-core` defines a `thiserror` enum (`CalibandUnreachable`,
`Protocol`, `AgentNotFound`, `InvalidState`, `Discovery`, `Store`); `prospero-api` maps each
to a status code + JSON `{ error, kind }`; binaries use `anyhow` only at the top edge.

**Concurrency:** one tokio runtime in prosperod; shared state in `Arc<RwLock<FleetSnapshot>>`
(read-mostly); one poll task per repo; one attach task per active/watched agent; a
broadcast bus decouples producers from SSE consumers. Spawned-task panics are caught at the
task boundary and converted to health events.

**Attach-task lifecycle (bounded work):** started when an agent is `Spawning/Running` or a
client subscribes; stopped when the agent is terminal (`Done/Failed/Killed`) *and* no client
is following. Streaming work stays proportional to active + watched agents.

## 7. Testing strategy

**Cornerstone: a fake caliban.** An in-test harness that listens on a Unix socket and
speaks the same NDJSON protocol (control + per-agent stream). Because the wire format is the
only coupling, the fake enables deterministic end-to-end testing of the whole control plane
with no real caliban, no API keys, no LLM calls.

1. **Unit (`prospero-core`):** NDJSON framing + wire-type serde round-trips against golden
   samples; frame normalizer (table-driven, incl. unknown-frame skip + thinking-dropped);
   discovery socket-path resolution (golden vectors from caliban's `runtime.rs`); snapshot
   reconciliation; `JsonlStore` append/replay/`seq` recovery + corrupt-line tolerance.
2. **Integration (fake caliban + real `FleetManager`/`CalibandClient`):** spawn ⇒ assert
   `isolation_worktree == true` by default (`false` with `--shared-tree`); poll discovery;
   attach streams scripted frames ⇒ assert `FleetEvent` sequence on bus *and* store;
   parallel-agents-same-repo grouping + independent attach tasks; resilience (stream drop ⇒
   re-attach; refused connection ⇒ `Unreachable`; `NotFound` ⇒ typed error, no panic);
   replay-then-tail with no gap/dup on `seq`.
3. **API (`prospero-api`):** drive the axum `Router` in-process (`tower::ServiceExt::oneshot`)
   over a fake-backed `FleetManager`; assert status codes/bodies, error→status mapping
   (404/409 vs never-500), well-formed `text/event-stream` SSE.
4. **CLI (`prospero`):** clap parsing unit tests; one smoke test against a stubbed HTTP
   server asserting each subcommand hits the right method/path/body (esp. spawn ⇒ worktree).
5. **E2E smoke (one test, feature/ignore-gated):** boot a real `prosperod` on an ephemeral
   port against the fake caliban; run real `prospero` CLI commands; assert observable output.

**TDD posture:** normalizer, framing, discovery resolution, store, reconciliation are pure
and tested first. The fake caliban is built early (everything integration-level needs it).

**Out of scope (first stab, called out not dropped):** tests against a *real* caliban
binary + live model (manual/CI-gated later); load/soak of many concurrent attach tasks;
dashboard browser/E2E (thin static JS exercised via API + manual check).

## 8. Dependencies introduced

- **Workspace-wide:** `tokio`, `serde` / `serde_json`, `tracing` / `tracing-subscriber`,
  `thiserror`, `anyhow`.
- **`prospero-api` / `prosperod`:** `axum` (HTTP + SSE), `tower`/`tower-http` (static files).
- **`prospero-cli`:** `clap`, `ureq` (blocking HTTP).
- **Hashing for discovery:** `sha2` (+ hex) to mirror caliban's socket-path rule.

Deliberately *not* chosen in the first stab: sqlite (the `Store` trait leaves the path
open), a JS framework / bundler, any multi-host transport.

## 9. Non-goals (first stab)

- Multi-host fleets (type carries host identity; transport deferred).
- Authn/authz on the API (assume localhost / trusted operator).
- Real-caliban + live-LLM automated E2E.
- Log retention / rotation policy (full per-agent log on disk for now).
- Reusing or depending on the `caliban-supervisor` crate.

## 10. Open questions / future work

- **History retention:** when to rotate/compact `JsonlStore`; sqlite `Store` impl.
- **Multi-host:** agent transport (gRPC/HTTP to remote prosperods, or remote socket relay).
- **Auth:** token/mTLS once the API leaves localhost.
- **Dashboard depth:** richer per-agent timelines, tool-call inspection, cost charts.
- **Spawn ergonomics:** frontmatter/agent-template support (caliban's `frontmatter_path`).
