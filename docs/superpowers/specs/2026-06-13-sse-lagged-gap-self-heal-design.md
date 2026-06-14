# SSE self-healing on `Lagged` — design

**Issue:** caliban-ai/prospero #28 — *fix(api): SSE silently drops lagged events with no gap signal to the client*
**Date:** 2026-06-13
**Status:** Approved (brainstorming)

## Problem

The SSE handler treats a slow consumer's `RecvError::Lagged` as `continue`
(`crates/api/src/sse.rs:53`), silently skipping the gap. A dashboard or
`prospero follow` client that falls behind misses events mid-run with no
indication, so the operator believes they saw a complete stream when they did
not. This violates ADR-0004's "live **and** durable" invariant: the live view
silently diverges from what actually happened.

## Goal

On `Lagged`, the stream must (a) tell the client a gap occurred, carrying the
skipped count, and (b) self-heal by replaying the missed events from the durable
store so the client ends up with a complete, in-order stream.

## Approach: self-heal via replay + signal

On `RecvError::Lagged(n)`:

1. Emit a **named `gap` SSE event** carrying `GapSignal { skipped: n, last_seq }`,
   where `last_seq` is the seq of the last event actually delivered (the gap
   boundary).
2. **Replay from the durable store** — `history(id, last_delivered + 1)`,
   yielding only events with `seq > last_delivered` and advancing
   `last_delivered`. This fills the hole the broadcast bus dropped.
3. If a replayed event is terminal (`AgentFinished`), close the stream; otherwise
   resume tailing the live bus.

A named event (rather than an in-band marker on the default `message` event) is
the idiomatic SSE control signal and cannot be mis-parsed as a real
`FleetEvent` by clients that only handle `onmessage`.

## Architecture — extract a testable tail state machine

The current handler interleaves axum glue with the tail loop, which makes
"force a `Lagged`" untestable: the async stream consumes the bus as fast as it is
polled, so a deterministic lag cannot be provoked at the HTTP level. The fix
splits the concern:

- **`Tailer` state machine** (`crates/api/src/sse/tail.rs`) — pure, synchronous,
  no axum. Holds `agent_id`, a `last_delivered` high-water seq, and a
  `HistorySource`. Its single method
  `on_recv(Result<FleetEvent, RecvError>) -> Step` decides what to do with each
  bus message. It is unit-tested by feeding it a synthetic `RecvError::Lagged(n)`
  — no HTTP, no real broadcast channel needed.

- **`HistorySource` trait** — `fn history(&self, id: &str, from: u64) -> Vec<FleetEvent>`.
  The real implementation wraps the `FleetManager`; tests supply a fake that
  returns a scripted set of persisted events. This seam is what makes replay
  deterministic under test.

- **Thin handler** (`sse.rs`) — replays the initial history (unchanged), then
  loops `rx.recv().await` through the `Tailer`, mapping each emitted `Frame` to
  an axum `Event`.

### Types

```rust
/// One unit the stream forwards to the client.
enum Frame {
    Event(FleetEvent),               // a real agent event (unnamed `message`)
    Gap { skipped: u64, last_seq: u64 }, // a `gap` control signal
}

/// What the Tailer decides to do with one bus message.
enum Step {
    Emit(Vec<Frame>),         // forward these frames, keep tailing
    EmitAndClose(Vec<Frame>), // forward these frames, then close (terminal seen)
    Skip,                     // nothing to forward (other agent / already delivered)
    Close,                    // bus closed; close the stream
}

/// Payload of a `gap` SSE event.
#[derive(serde::Serialize)]
struct GapSignal { skipped: u64, last_seq: u64 }
```

`on_recv` behavior:

- `Ok(ev)` for this agent with `ev.seq > last_delivered` → advance
  `last_delivered`; `EmitAndClose([Event(ev)])` if terminal else
  `Emit([Event(ev)])`.
- `Ok(_)` (other agent, or `seq <= last_delivered`) → `Skip`.
- `Err(Lagged(n))` → build `[Gap { skipped: n, last_seq: last_delivered }]` then
  append replayed events from `history(id, last_delivered + 1)` filtered to
  `seq > last_delivered`, advancing `last_delivered`; `EmitAndClose` if any
  replayed event is terminal, else `Emit`.
- `Err(Closed)` → `Close`.

### Dedup via a single high-water mark

Today the live loop guards on an immutable `last_seq` snapshot
(`sse.rs:45`). The `Tailer` replaces it with the mutable `last_delivered` that
advances on every yield. This gives free dedup: any live event re-delivered after
a replay has `seq <= last_delivered` and is dropped — so self-heal never produces
duplicates at the SSE layer.

## Lag tolerance (documented, not a new knob)

The tolerance already exists: `event_buffer: 1024` (`fleet.rs:109`) is the
per-subscriber broadcast capacity. A doc comment in `sse.rs` will state that
exceeding it triggers a `gap` signal plus durable self-heal rather than a silent
skip. No new configuration is introduced.

**Known limit (ties into #25 / #22):** if a missed event also failed to persist,
replay cannot recover it. The `gap` signal still fires, so the client is never
*silently* misled — it knows history is incomplete. Fully closing that hole is
the behavioral fix tracked by #25 and the metering by #22; out of scope here.

## Testing

Unit tests on `Tailer` (`tail.rs`), driving `on_recv` directly:

1. `Lagged(n)` → a `Gap { skipped: n, last_seq }` frame with the correct count
   and boundary, followed by the replayed frames.
2. `Lagged` where replay contains the terminal event → emits the frames then
   signals close.
3. `Lagged` but the store has nothing newer (persist also behind) → gap frame
   only, correct count, stream continues.
4. Repeated `Lagged` → heals on each occurrence.
5. Dedup → a live `Ok(ev)` with `seq <= last_delivered` after a replay is
   dropped (no duplicate).

These satisfy the issue's acceptance criterion: "a test forces a `Lagged` and
asserts the gap event is delivered with the skipped count."

## Client (dashboard)

Add a minimal `evtSource.addEventListener("gap", …)` in
`crates/api/dashboard/app.js` to surface a transient "fell behind — recovered N
events" affordance. Secondary to the AC (which is verified at the SSE layer) but
completes the operator-facing loop.

## Scope

- `crates/api/src/sse.rs` — thin handler, doc comment on lag tolerance.
- `crates/api/src/sse/tail.rs` — new `Tailer`, `Frame`, `Step`, `HistorySource`,
  `GapSignal`, and unit tests.
- `crates/api/dashboard/app.js` — `gap` listener.

Out of scope: changes to the broadcast bus, a new config knob, a new ADR, the
attach-layer reconnection/dedup (#26), and the store-append divergence behavior
(#25).
