# Handoff: wire up interactive sub-agent control (consume Caliban ADR-0047 / #81)

**Date:** 2026-06-11
**Audience:** an agent working in the `prospero` repo
**Status:** ready to plan & implement — all upstream (caliban) work is already shipped; this is Prospero-side only.

---

## TL;DR

Caliban just landed the full interactive sub-agent contract (issue caliban-ai/caliban#81,
ADR-0047): a `caliband` worker can now run in **interactive mode**, report an **`Idle`**
("awaiting input") state, and accept **operator input** over its per-agent socket.

**Prospero has not consumed any of it.** Our mirrored wire types lag the upstream protocol,
and we attach to agents read-only. The result: Prospero can only orchestrate fire-and-forget
autonomous agents — it cannot spawn an interactive agent, and cannot send a message to one
that is waiting. Closing this is the one remaining gap for full agent orchestration.

This handoff has everything you need; you should not need the caliban repo open, but the
canonical source is `caliban/crates/caliban-supervisor/src/proto.rs` and
`caliban/src/attach.rs`, plus `caliban/adrs/0047-interactive-background-subagents.md`.

---

## Background: what Caliban now exposes (already shipped, do not change)

The external-control contract an orchestrator consumes:

1. **`SpawnSpec.interactive: bool`** (`#[serde(default)]`, default `false`). When `true`, the
   worker runs in interactive mode: at each end-of-run boundary it awaits inbound operator
   messages over the per-agent socket instead of finishing.

2. **`AgentStatus::Idle`** — a real, reported lifecycle state. The worker reports
   `Running ↔ Idle` to the daemon via a new `CtlRequest::ReportStatus { id, status }`
   (worker→daemon only — **we never send this**). It surfaces to us through the normal
   `AgentRecord.status` we already read from `List`. Lifecycle:
   `Spawning → Running ↔ Idle → Done | Failed | Killed | Crashed`.

3. **Bidirectional per-agent socket.** The same Unix socket we attach to for the outbound
   `TurnEvent` stream also accepts **inbound** NDJSON frames when the agent is interactive.
   The inbound frame type (from `caliban/src/attach.rs`) is:

   ```rust
   #[serde(tag = "type")]
   enum AttachInbound {
       UserMessage { text: String }, // inject a message, resume the run
       EndInput,                     // finish the run (operator done)
   }
   ```

   Wire examples (one JSON object per line):
   ```json
   {"type":"UserMessage","text":"also check the tests"}
   {"type":"EndInput"}
   ```

4. **Bounded idle lifetime.** The worker self-terminates after an idle timeout
   (default 300s, `CALIBAN_AGENT_IDLE_TIMEOUT_SECS`); the timer resets while a client is
   attached/streaming. Nothing for us to implement, but it informs UX: an idle agent left
   untouched will eventually go `Done` on its own.

Per `caliban/docs/parity-gap-matrix.md` row G, "Interactive / idle / await-input" is ✅ with
all five #81 tickets landed. The harness half of orchestration is complete.

---

## The gap on our side (what's wrong today)

Verified by diffing `prospero/crates/core/src/caliband/wire.rs` against caliban's `proto.rs`:

1. **`SpawnSpec` is missing `interactive`** — `wire.rs:46`. Because the upstream field is
   `#[serde(default)]`, this drift is **silent**: our spawns serialize without it, caliband
   defaults it to `false`, and we *never* get an interactive agent. No error, it just never
   happens. `SpawnRequest::into_spec` (`crates/core/src/fleet.rs:58`) likewise hardcodes the
   spec with no interactive field.

2. **No inbound send path.** We attach read-only. `CalibandClient::open_stream`
   (`crates/core/src/caliband/client.rs:133`) wraps the socket in a `BufReader<UnixStream>`
   and only ever reads; `stream.rs`'s normalizer only decodes outbound frames. There is no
   method that writes `AttachInbound` frames. The `UnixStream` is duplex — we need its
   write half (or `tokio::io::split`).

3. **No API / CLI / dashboard verb** to send input or end-input. The dashboard explicitly
   *skips* re-attaching idle agents (`crates/api/dashboard/app.js:282`,
   `ACTIVE_STATUSES` includes `"idle"` only for the active badge, not for input).

4. **No drift guard.** Our wire "golden" tests round-trip our *own* types, so they passed
   despite the missing field. There is no fixture asserting our `SpawnSpec` is wire-compatible
   with caliban's serialized form — which is exactly why #1 slipped through.

Minor / non-issues: our `CtlRequest` omits `ReportStatus`. That's fine — it's worker→daemon
and we never send it. Leave it out (or add it for documentation parity only).

---

## Goal

Let an operator, through Prospero, (a) launch an **interactive** agent and (b) **send a
message** (and **end-input**) to a running or idle interactive agent — end to end across
core → API → CLI → dashboard.

---

## Task breakdown

Suggested order; each item is independently testable. Run the verification gate (below)
after each.

### 1. Mirror the wire field + add a drift guard
- Add `interactive: bool` (`#[serde(default)]`) to `SpawnSpec` in
  `crates/core/src/caliband/wire.rs`, matching caliban's field exactly (doc comment too).
- Add a **wire-compatibility fixture test**: assert a JSON spec containing
  `"interactive":true` deserializes into our `SpawnSpec` with the flag set, and that our
  serialized spec contains the field. Ideally pin a golden JSON string copied from caliban so
  future upstream drift fails loudly. (This is the regression guard for the whole class of bug.)

### 2. Plumb `interactive` through the spawn path
- `SpawnRequest` (`crates/core/src/fleet.rs:31`): add `pub interactive: bool`, default `false`
  in `SpawnRequest::new` (keep worktree-isolation default `true` untouched).
- `SpawnRequest::into_spec` (`fleet.rs:58`): pass it through.
- `SpawnBody` (`crates/api/src/dto.rs:23`) + `into_request`: add `#[serde(default)] interactive`.
- Dashboard launch modal (`crates/api/dashboard/`): add an "interactive" checkbox; send it in
  the POST body. (Follow the existing advanced-fields pattern.)

### 3. Inbound send path in the caliband client
- Add a method to `CalibandClient` (or a small `AgentInput` handle) that, given a per-agent
  socket path (from `attach`), connects and writes `AttachInbound` frames as NDJSON:
  - `send_user_message(socket_path, text)` → writes `{"type":"UserMessage","text":...}\n`
  - `send_end_input(socket_path)` → writes `{"type":"EndInput"}\n`
- Define a local `AttachInbound` mirror (in `wire.rs`, with the `#[serde(tag = "type")]`
  shape above) — keep it alongside the other mirrored types.
- Mind the socket: `open_stream` currently keeps only the read half. For sending you can open
  a fresh connection to the same socket path (simplest), or `tokio::io::split` a single
  connection if you want one duplex attachment. A fresh write-only connection per send is the
  low-risk choice and matches caliban's "all attach connections feed a shared inbox" model.
- Add a `FleetManager` method (`crates/core/src/fleet.rs`) that resolves the agent → its
  caliband client + socket and forwards the send. Reject with a clear error if the agent is
  terminal or was not spawned interactive.

### 4. API surface
- In `crates/api/src/lib.rs` router, add:
  - `POST /api/agents/{id}/input` — body `{ "text": "..." }` → `send_user_message`.
  - `POST /api/agents/{id}/end-input` → `send_end_input`.
- Handlers in `crates/api/src/handlers.rs` next to `kill_agent`/`respawn_agent`
  (`handlers.rs:121-139`); reuse their error→status mapping. DTOs in `dto.rs`.

### 5. CLI verb
- Add `prospero send <agent-id> <text>` and `prospero end-input <agent-id>` in
  `crates/cli/src/main.rs` (thin ureq calls to the two new endpoints), matching the existing
  subcommand style.

### 6. Dashboard input UX
- For agents whose status is `idle` (and optionally `running` interactive), show an input box +
  "send" and "end input" buttons that POST to the new endpoints. Update
  `crates/api/dashboard/app.js` — note the existing `idle` handling at `app.js:282`; idle agents
  currently aren't re-attached, so decide whether sending input should also (re)open the SSE
  stream to show the resumed turn.

### 7. Tests
- Core: unit-test the `AttachInbound` serialization + the send methods against a throwaway
  Unix socket (mirror the style in `crates/core/src/caliband/` tests; see the existing
  testkit at `crates/core/src/testkit.rs`).
- API: cover `POST /input` and `/end-input` (200/204 happy path, 404 unknown id, and the
  reject-when-terminal/non-interactive case), mirroring the existing
  `kill`/`respawn`/`config` handler tests.

---

## Out of scope (explicitly deferred — don't gold-plate)

- **Multi-writer coordination / single-writer lease.** Caliban's own spec lists this as a
  follow-up; concurrent operators interleave by arrival order. Prospero is the natural owner of
  a lease *later*, but for v1 just document the interleave behavior.
- **Richer-than-text inbound** (images, tool results), **agent forking/branching**,
  **multi-host fleets**, **API auth**, **log rotation / sqlite store**, **frontmatter
  templates**. All are pre-existing non-goals in the framework design spec; leave them.

---

## Verification gate (run before claiming done / pushing)

From `prospero/`:
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace --all-targets
cargo test --workspace
```
(`cargo fmt --all` first to auto-fix.) All four must pass — CI checks every step; fmt is the
easiest to forget.

A good manual smoke test: spawn an agent with `interactive: true`, watch it reach `Idle` in
`prospero list` / the dashboard, `prospero send <id> "…"`, and confirm via the SSE stream that
it resumes (goes `Running`), then `prospero end-input <id>` and confirm it finishes `Done`.

---

## Key references

**Prospero (to change):**
- `crates/core/src/caliband/wire.rs:46` — `SpawnSpec` (add `interactive`); add `AttachInbound`
- `crates/core/src/caliband/client.rs:70,78,133` — `spawn`/`attach`/`open_stream` (add send path)
- `crates/core/src/fleet.rs:31,58` — `SpawnRequest` / `into_spec`
- `crates/api/src/lib.rs:30-53` — router (add two routes)
- `crates/api/src/handlers.rs:121-139` — kill/respawn handlers (model new ones on these)
- `crates/api/src/dto.rs:23` — `SpawnBody` (add `interactive`); add input DTOs
- `crates/cli/src/main.rs` — add `send` / `end-input` subcommands
- `crates/api/dashboard/app.js:282` — idle handling; launch modal + input box
- `crates/core/src/testkit.rs:315` — spawn-spec test helper

**Caliban (upstream contract, already shipped — read-only):**
- `caliban/crates/caliban-supervisor/src/proto.rs` — `SpawnSpec.interactive` (~:96),
  `CtlRequest::ReportStatus` (~:145), `AgentStatus` (~:17)
- `caliban/src/attach.rs:20` — `AttachInbound { UserMessage{text}, EndInput }`
- `caliban/adrs/0047-interactive-background-subagents.md` — design rationale
- `caliban/docs/parity-gap-matrix.md` — row G (sub-agents), all ✅
