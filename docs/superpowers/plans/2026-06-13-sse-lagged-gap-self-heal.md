# SSE Self-Healing on `Lagged` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On a slow-consumer `RecvError::Lagged`, the agent SSE stream emits a named `gap` signal carrying the skipped count and self-heals by replaying the missed events from the durable store, instead of silently skipping the gap.

**Architecture:** Extract a pure, synchronous `Tailer` state machine (`crates/api/src/sse/tail.rs`) that maps each `Result<FleetEvent, RecvError>` from the broadcast bus to a `Step` (frames to emit / close). A `HistorySource` trait abstracts durable replay so the `Lagged` path is unit-testable without HTTP or a real channel. The axum handler in `sse.rs` becomes thin glue mapping frames to SSE `Event`s.

**Tech Stack:** Rust, axum SSE (`async-stream`), tokio `broadcast`, serde.

---

## File Structure

- `crates/api/src/sse/tail.rs` — **new**: `Frame`, `Step`, `GapSignal`, `HistorySource`, `Tailer`, `impl HistorySource for FleetManager`, unit tests. One responsibility: decide what to forward for each bus message.
- `crates/api/src/sse.rs` — **modify**: declare `mod tail;`, replace the inline tail loop with the `Tailer`, map `Frame`→`Event`, document lag tolerance.
- `crates/api/dashboard/app.js` — **modify**: add a `gap` event listener for an operator affordance.

Reference facts (verified):
- `FleetManager::history(&self, agent_id: &str, from_seq: u64) -> Result<Vec<FleetEvent>>` (`fleet.rs:210`), returns events with `seq >= from_seq`. `FleetManager` is `#[derive(Clone)]` (`fleet.rs:157`).
- `FleetEvent { seq: u64, ts: String, repo: String, agent_id: String, kind: EventKind }`, derives `Debug, Clone, PartialEq` (`event.rs:85`).
- Terminal kind: `EventKind::AgentFinished { outcome: String, cost_usd: f64, turns: u32 }`.
- Broadcast capacity (lag tolerance): `event_buffer: 1024` (`fleet.rs:109`).
- Core re-exports: `prospero_core::{EventKind, FleetEvent, FleetManager}`.

---

### Task 1: `Tailer` state machine + types (TDD)

**Files:**
- Create: `crates/api/src/sse/tail.rs`
- Modify: `crates/api/src/sse.rs:1` (add `mod tail;` near the top, after the module doc-comment)

- [ ] **Step 1: Create the module file with types and a stubbed `Tailer`**

```rust
//! The pure, synchronous tail state machine behind `agent_stream`.
//!
//! It turns each broadcast `recv()` outcome into a set of [`Frame`]s to forward
//! and a decision to keep tailing or close — with no axum or async in sight, so
//! the `Lagged` self-heal path is unit-testable without HTTP or a real channel.

use prospero_core::event::EventKind;
use prospero_core::{FleetEvent, FleetManager};
use tokio::sync::broadcast::error::RecvError;

/// One unit the stream forwards to the client.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Frame {
    /// A real agent event (serialized to the default, unnamed `message` event).
    Event(FleetEvent),
    /// A control signal that `skipped` events were dropped after `last_seq`.
    Gap { skipped: u64, last_seq: u64 },
}

/// What the [`Tailer`] decides to do with one broadcast message.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Step {
    /// Forward these frames (in order), keep tailing.
    Emit(Vec<Frame>),
    /// Forward these frames, then close the stream (terminal event seen).
    EmitAndClose(Vec<Frame>),
    /// Nothing to forward (other agent, or already-delivered seq).
    Skip,
    /// Bus closed; close the stream.
    Close,
}

/// Source of persisted events for replay-based self-heal.
pub(crate) trait HistorySource {
    /// Events for `agent_id` with `seq >= from`, in order.
    fn history(&self, agent_id: &str, from: u64) -> Vec<FleetEvent>;
}

impl HistorySource for FleetManager {
    fn history(&self, agent_id: &str, from: u64) -> Vec<FleetEvent> {
        FleetManager::history(self, agent_id, from).unwrap_or_default()
    }
}

/// Drives the live-tail loop for one agent's SSE stream.
pub(crate) struct Tailer<H: HistorySource> {
    agent_id: String,
    /// Highest seq already forwarded to the client (the dedup high-water mark).
    last_delivered: u64,
    history: H,
}

impl<H: HistorySource> Tailer<H> {
    /// `last_delivered` is the last seq emitted during initial history replay
    /// (0 if none).
    pub(crate) fn new(agent_id: String, last_delivered: u64, history: H) -> Self {
        Self { agent_id, last_delivered, history }
    }

    pub(crate) fn on_recv(&mut self, _r: Result<FleetEvent, RecvError>) -> Step {
        Step::Skip // replaced in Step 3
    }
}

fn is_terminal(ev: &FleetEvent) -> bool {
    matches!(ev.kind, EventKind::AgentFinished { .. })
}
```

Then add the module declaration in `sse.rs` (after the `//!` header, before the `use` block):

```rust
mod tail;
```

- [ ] **Step 2: Add the failing tests at the bottom of `tail.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn ev(seq: u64, agent: &str) -> FleetEvent {
        FleetEvent {
            seq,
            ts: "2026-06-13T00:00:00Z".into(),
            repo: "r".into(),
            agent_id: agent.into(),
            kind: EventKind::Output {
                stream: prospero_core::OutputStream::Stdout,
                chunk: format!("c{seq}"),
            },
        }
    }

    fn finished(seq: u64, agent: &str) -> FleetEvent {
        FleetEvent {
            seq,
            ts: "2026-06-13T00:00:00Z".into(),
            repo: "r".into(),
            agent_id: agent.into(),
            kind: EventKind::AgentFinished {
                outcome: "success".into(),
                cost_usd: 0.0,
                turns: 1,
            },
        }
    }

    /// Fake store: returns held events with `seq >= from` for the matching agent.
    struct FakeHistory(Vec<FleetEvent>);
    impl HistorySource for FakeHistory {
        fn history(&self, agent_id: &str, from: u64) -> Vec<FleetEvent> {
            self.0
                .iter()
                .filter(|e| e.agent_id == agent_id && e.seq >= from)
                .cloned()
                .collect()
        }
    }

    #[test]
    fn forwards_in_order_event_for_this_agent() {
        let mut t = Tailer::new("a".into(), 0, FakeHistory(vec![]));
        assert_eq!(t.on_recv(Ok(ev(1, "a"))), Step::Emit(vec![Frame::Event(ev(1, "a"))]));
    }

    #[test]
    fn skips_other_agents_and_already_delivered() {
        let mut t = Tailer::new("a".into(), 5, FakeHistory(vec![]));
        assert_eq!(t.on_recv(Ok(ev(9, "b"))), Step::Skip); // other agent
        assert_eq!(t.on_recv(Ok(ev(5, "a"))), Step::Skip); // <= last_delivered
        assert_eq!(t.on_recv(Ok(ev(3, "a"))), Step::Skip); // older than high-water
    }

    #[test]
    fn terminal_event_closes() {
        let mut t = Tailer::new("a".into(), 0, FakeHistory(vec![]));
        assert_eq!(
            t.on_recv(Ok(finished(2, "a"))),
            Step::EmitAndClose(vec![Frame::Event(finished(2, "a"))])
        );
    }

    #[test]
    fn lagged_emits_gap_then_replays_missed_events() {
        // Delivered up to seq 2; store holds 3 and 4 we never saw live.
        let store = FakeHistory(vec![ev(3, "a"), ev(4, "a")]);
        let mut t = Tailer::new("a".into(), 2, store);
        assert_eq!(
            t.on_recv(Err(RecvError::Lagged(7))),
            Step::Emit(vec![
                Frame::Gap { skipped: 7, last_seq: 2 },
                Frame::Event(ev(3, "a")),
                Frame::Event(ev(4, "a")),
            ])
        );
        // High-water advanced: a later live re-delivery of seq 4 is a no-op.
        assert_eq!(t.on_recv(Ok(ev(4, "a"))), Step::Skip);
    }

    #[test]
    fn lagged_replay_containing_terminal_closes() {
        let store = FakeHistory(vec![ev(3, "a"), finished(4, "a")]);
        let mut t = Tailer::new("a".into(), 2, store);
        assert_eq!(
            t.on_recv(Err(RecvError::Lagged(1))),
            Step::EmitAndClose(vec![
                Frame::Gap { skipped: 1, last_seq: 2 },
                Frame::Event(ev(3, "a")),
                Frame::Event(finished(4, "a")),
            ])
        );
    }

    #[test]
    fn lagged_with_nothing_newer_emits_gap_only() {
        // Persist is also behind: nothing newer than what we delivered.
        let mut t = Tailer::new("a".into(), 5, FakeHistory(vec![ev(5, "a")]));
        assert_eq!(
            t.on_recv(Err(RecvError::Lagged(2))),
            Step::Emit(vec![Frame::Gap { skipped: 2, last_seq: 5 }])
        );
    }

    #[test]
    fn closed_bus_closes() {
        let mut t = Tailer::new("a".into(), 0, FakeHistory(vec![]));
        assert_eq!(t.on_recv(Err(RecvError::Closed)), Step::Close);
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p prospero-api tail::`
Expected: FAIL — `on_recv` returns `Skip`, so the `forwards_*`, `terminal_*`, and `lagged_*` assertions fail.

- [ ] **Step 4: Implement `on_recv`**

Replace the stub `on_recv` body with:

```rust
    pub(crate) fn on_recv(&mut self, r: Result<FleetEvent, RecvError>) -> Step {
        match r {
            Ok(ev) if ev.agent_id == self.agent_id && ev.seq > self.last_delivered => {
                self.last_delivered = ev.seq;
                let terminal = is_terminal(&ev);
                let frames = vec![Frame::Event(ev)];
                if terminal {
                    Step::EmitAndClose(frames)
                } else {
                    Step::Emit(frames)
                }
            }
            Ok(_) => Step::Skip, // other agent, or already-delivered seq
            Err(RecvError::Lagged(skipped)) => {
                let mut frames = vec![Frame::Gap { skipped, last_seq: self.last_delivered }];
                let mut terminal = false;
                for ev in self.history.history(&self.agent_id, self.last_delivered + 1) {
                    if ev.seq <= self.last_delivered {
                        continue; // defensive dedup
                    }
                    self.last_delivered = ev.seq;
                    terminal = is_terminal(&ev);
                    frames.push(Frame::Event(ev));
                    if terminal {
                        break;
                    }
                }
                if terminal {
                    Step::EmitAndClose(frames)
                } else {
                    Step::Emit(frames)
                }
            }
            Err(RecvError::Closed) => Step::Close,
        }
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p prospero-api tail::`
Expected: PASS — all 7 tests green.

- [ ] **Step 6: Commit**

```bash
git add crates/api/src/sse/tail.rs crates/api/src/sse.rs
git commit -m "feat(api): add Tailer self-heal state machine for SSE Lagged (#28)"
```

---

### Task 2: Wire the handler to the `Tailer`

**Files:**
- Modify: `crates/api/src/sse.rs` (the `agent_stream` body and helpers)

- [ ] **Step 1: Rewrite `agent_stream`'s tail loop to drive the `Tailer`**

Replace the `let body = stream! { ... };` block (currently `sse.rs:32-57`) with:

```rust
    let body = stream! {
        // 1) Replay persisted history, stopping if it already contains the
        //    terminal event. Track the last seq we delivered as the dedup
        //    high-water mark for the live tail.
        let mut last_delivered = 0u64;
        for ev in history {
            let terminal = is_terminal(&ev);
            last_delivered = ev.seq;
            yield Ok(to_event(&ev));
            if terminal {
                return;
            }
        }

        // 2) Tail live events, self-healing across a slow-consumer `Lagged`:
        //    the per-subscriber broadcast buffer is the lag tolerance
        //    (`FleetConfig::event_buffer`, default 1024). Exceed it and we emit
        //    a `gap` signal plus replay the missed events from the durable
        //    store, rather than silently skipping them.
        let mut tailer = Tailer::new(id, last_delivered, st.manager.clone());
        loop {
            match tailer.on_recv(rx.recv().await) {
                Step::Emit(frames) => {
                    for f in frames {
                        yield Ok(frame_to_event(&f));
                    }
                }
                Step::EmitAndClose(frames) => {
                    for f in frames {
                        yield Ok(frame_to_event(&f));
                    }
                    break;
                }
                Step::Skip => continue,
                Step::Close => break,
            }
        }
    };
```

Note `last_seq` is no longer used after this change — it is removed in Step 2.

- [ ] **Step 2: Update imports, remove the dead `last_seq`, add `frame_to_event`**

In `sse.rs`, update the `use` block and helpers:
- Remove `let last_seq = history.last()...;` line (now `sse.rs:30`).
- Add to imports: `use tail::{Frame, Step, Tailer, GapSignal};` (and keep `mod tail;` from Task 1).
- Remove the now-unused `use tokio::sync::broadcast::error::RecvError;` (the match moved into `tail.rs`).

Add a `frame_to_event` helper next to `to_event`:

```rust
fn frame_to_event(frame: &Frame) -> Event {
    match frame {
        Frame::Event(ev) => to_event(ev),
        Frame::Gap { skipped, last_seq } => Event::default()
            .event("gap")
            .json_data(GapSignal { skipped: *skipped, last_seq: *last_seq })
            .unwrap_or_else(|_| Event::default().event("gap").data("{}")),
    }
}
```

Add the `GapSignal` payload type to `tail.rs` (re-exported via the `use tail::...GapSignal`):

```rust
/// Payload of a `gap` SSE event: `skipped` events were dropped after `last_seq`.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct GapSignal {
    pub skipped: u64,
    pub last_seq: u64,
}
```

`is_terminal` stays in `sse.rs` for the history-replay loop; `tail.rs` keeps its own copy for the live path (both are one-line `matches!`, kept local to avoid a cross-module dependency for a trivial predicate).

- [ ] **Step 3: Build and run the whole api test suite**

Run: `cargo test -p prospero-api`
Expected: PASS — Task 1 unit tests plus any existing api tests; no warnings about unused `last_seq`/`RecvError`.

- [ ] **Step 4: Commit**

```bash
git add crates/api/src/sse.rs crates/api/src/sse/tail.rs
git commit -m "feat(api): drive SSE tail via Tailer, emit gap + self-heal on Lagged (#28)"
```

---

### Task 3: Dashboard `gap` affordance

**Files:**
- Modify: `crates/api/dashboard/app.js` (near the `EventSource` setup, ~`app.js:567-569`)

- [ ] **Step 1: Add a `gap` listener after the `onmessage` handler**

After the existing `evtSource.onmessage = (e) => appendEvent(JSON.parse(e.data));` line, add:

```javascript
  // The backend self-heals a slow-consumer gap (replays the missed events from
  // the durable store) and sends this `gap` signal so we can show it happened.
  evtSource.addEventListener("gap", (e) => {
    let info = {};
    try { info = JSON.parse(e.data); } catch { /* ignore malformed */ }
    const note = document.createElement("div");
    note.className = "empty-hint";
    note.textContent =
      `Fell behind — recovered ${info.skipped ?? "?"} dropped event(s) from history.`;
    streamLogEl.appendChild(note);
    streamLogEl.scrollTop = streamLogEl.scrollHeight;
  });
```

- [ ] **Step 2: Sanity-check the JS parses (no build step for the static dashboard)**

Run: `node --check crates/api/dashboard/app.js`
Expected: no output, exit 0.

- [ ] **Step 3: Commit**

```bash
git add crates/api/dashboard/app.js
git commit -m "feat(dashboard): surface SSE gap recovery affordance (#28)"
```

---

### Task 4: Full verification gate

- [ ] **Step 1: Run the complete local gate**

Run, in order, from the worktree root:

```bash
cargo fmt --all
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace --all-targets
cargo test --workspace
```

Expected: fmt clean, clippy zero warnings, build OK, all tests pass.

- [ ] **Step 2: Commit any fmt-only changes**

```bash
git add -A
git commit -m "style: cargo fmt (#28)" || echo "nothing to format"
```

---

## Self-Review

**Spec coverage:**
- Named `gap` event with skipped count → Task 1 (`Frame::Gap`, `lagged_emits_gap_*` test), Task 2 (`frame_to_event`, `GapSignal`). ✓
- Self-heal via durable replay → Task 1 `on_recv` `Lagged` arm + `HistorySource`; real impl wraps `FleetManager::history`. ✓
- Dedup via single high-water mark → `last_delivered` replaces `last_seq`; `lagged_emits_gap_then_replays_missed_events` asserts no re-delivery. ✓
- Terminal-during-replay closes → `lagged_replay_containing_terminal_closes`. ✓
- Persist-also-behind degrades to signal-only → `lagged_with_nothing_newer_emits_gap_only`. ✓
- Lag tolerance documented → Task 2 Step 1 doc comment referencing `event_buffer`. ✓
- Testable lag (AC) → `Tailer` fed a synthetic `RecvError::Lagged(n)`. ✓
- Client affordance → Task 3. ✓

**Placeholder scan:** none — every step has concrete code/commands.

**Type consistency:** `Tailer::new(String, u64, H)`, `on_recv(Result<FleetEvent, RecvError>) -> Step`, `Frame::{Event, Gap}`, `Step::{Emit, EmitAndClose, Skip, Close}`, `GapSignal { skipped, last_seq }`, `HistorySource::history(&str, u64) -> Vec<FleetEvent>` — consistent across Tasks 1 and 2.
