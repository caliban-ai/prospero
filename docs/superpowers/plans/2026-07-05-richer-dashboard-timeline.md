# Richer dashboard — agent timeline + tool inspector — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replace the raw agent stream-log with a turn-grouped timeline whose tool calls are expandable (input JSON + ok/fail), and enrich the header with cost/turns/outcome — vanilla JS, no build step, reusing the existing shell.

**Architecture:** Accumulate the agent's SSE events in an array; a pure `groupEvents` fold turns them into render segments (init / turns / tool spans / markers / finish); `renderTimeline` paints them, honoring a per-tool expanded-state set and scroll position. A Rust contract test guards the `EventKind` JSON shapes the fold consumes.

**Tech Stack:** vanilla JS (`crates/api/dashboard/app.js` + `index.html`, served via `include_str!`); Rust test in `crates/api/tests/api_integration.rs`.

## Global Constraints

- No JS toolchain / build step; edit the two dashboard files in place.
- No new API endpoints.
- Every event maps to a segment or a raw fallback — never dropped.
- Gate `$TESTKIT` = `--features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s`.

---

### Task 1: Server-side contract test for the timeline's data shapes (TDD guard)

**Files:** Modify `crates/api/tests/api_integration.rs`.

- [ ] **Step 1: Add the contract test**

Seed the store directly with the `EventKind`s the timeline renders, then assert
`/api/agents/{id}/events` serializes the exact JSON fields the JS reads. Add
(imports: `EventKind`, `FleetEvent` — check the file's existing `use` of
`prospero_core::event`; the `events_endpoint_returns_history_after_poll` test
shows the `["kind"]["kind"]` access pattern):

```rust
#[tokio::test]
async fn events_endpoint_exposes_tool_and_cost_shapes_for_the_timeline() {
    use prospero_core::event::{EventKind, FleetEvent};
    let h = setup().await;
    let store = h.manager.store();
    let ev = |seq, kind| FleetEvent {
        seq,
        ts: "2026-07-05T00:00:00Z".to_string(),
        repo: "repo".to_string(),
        agent_id: "agent001".to_string(),
        kind,
    };
    store.append(&ev(1, EventKind::ToolStarted {
        name: "Read".to_string(),
        input: serde_json::json!({ "path": "/x.rs" }),
    })).await.unwrap();
    store.append(&ev(2, EventKind::ToolFinished { name: "Read".to_string(), ok: true }))
        .await.unwrap();
    store.append(&ev(3, EventKind::AgentFinished {
        outcome: "success".to_string(),
        cost_usd: 0.12,
        turns: 4,
    })).await.unwrap();

    let resp = h.router.clone().oneshot(
        Request::builder().uri("/api/agents/agent001/events").body(Body::empty()).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    let arr = v.as_array().unwrap();
    // Exactly the fields groupEvents()/renderTimeline() read:
    assert!(arr.iter().any(|e| e["kind"]["kind"] == "tool_started"
        && e["kind"]["name"] == "Read" && e["kind"]["input"]["path"] == "/x.rs"),
        "tool_started shape: {v}");
    assert!(arr.iter().any(|e| e["kind"]["kind"] == "tool_finished" && e["kind"]["ok"] == true),
        "tool_finished shape: {v}");
    assert!(arr.iter().any(|e| e["kind"]["kind"] == "agent_finished"
        && e["kind"]["cost_usd"] == 0.12 && e["kind"]["turns"] == 4),
        "agent_finished shape: {v}");
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p prospero-api --features prospero-core/testkit events_endpoint_exposes_tool_and_cost_shapes`
Expected: PASS (guards existing serialization — locks the contract the JS depends on). If it fails, the wire shape differs from the spec and the JS field access must match whatever it actually is — reconcile before touching JS.

- [ ] **Step 3: Commit**

```bash
git add crates/api/tests/api_integration.rs
git commit -m "test(api): lock the event JSON shapes the dashboard timeline consumes (#5)"
```

---

### Task 2: Timeline fold + render in `app.js`

**Files:** Modify `crates/api/dashboard/app.js`.

- [ ] **Step 1: Add module state for the event array + expanded set**

Near the other stream state (top of file, by `streamLogEl`):

```js
let streamEvents = [];          // accumulated events for the selected agent
const expandedTools = new Set(); // tool span keys (seq) the user expanded
```

- [ ] **Step 2: Reset on select; accumulate + re-render on each event**

In `selectAgent` replace `streamLogEl.innerHTML = "";` with:
```js
  streamEvents = [];
  expandedTools.clear();
  streamLogEl.innerHTML = "";
```
Rewrite `appendEvent` to accumulate + update header ctx + re-render:
```js
function appendEvent(ev) {
  streamEvents.push(ev);
  const k = ev.kind;
  if (streamCtx) {
    if (k.kind === "agent_init") streamCtx.model = k.model;
    else if (k.kind === "agent_finished") {
      streamCtx.cost = k.cost_usd; streamCtx.turns = k.turns; streamCtx.outcome = k.outcome;
    } else if (k.kind === "status_changed") streamCtx.status = k.to;
    paintStreamHead();
  }
  renderTimeline();
}
```

- [ ] **Step 3: Add `groupEvents` (pure fold) + `renderTimeline`**

```js
// Fold the ordered event list into render segments. Total: every event becomes
// a segment or a raw fallback line; tool spans pair ToolStarted→ToolFinished by
// name (FIFO for repeats); assistant output coalesces into the current turn.
function groupEvents(events) {
  const segs = [];
  let turn = null;                 // { kind:"turn", tools:[], text:"" }
  const openTools = {};            // name -> tool row awaiting its finish
  const startTurn = () => { turn = { kind: "turn", tools: [], text: "" }; segs.push(turn); };
  for (const ev of events) {
    const k = ev.kind, seq = ev.seq;
    switch (k.kind) {
      case "agent_init":
        segs.push({ kind: "init", model: k.model, tools: k.tools || [] }); break;
      case "output":
        if (!turn) startTurn();
        turn.text += k.chunk; break;
      case "tool_started": {
        if (!turn) startTurn();
        const row = { seq, name: k.name, input: k.input, ok: null };
        turn.tools.push(row);
        (openTools[k.name] = openTools[k.name] || []).push(row);
        break;
      }
      case "tool_finished": {
        const q = openTools[k.name];
        if (q && q.length) q.shift().ok = k.ok;
        break;
      }
      case "status_changed":
        segs.push({ kind: "status", from: k.from, to: k.to }); break;
      case "agent_finished":
        segs.push({ kind: "finished", outcome: k.outcome, cost: k.cost_usd, turns: k.turns }); break;
      case "store_persist_failed":
        segs.push({ kind: "sys", text: `persist gap at seq ${k.lost_seq}` }); break;
      default:
        segs.push({ kind: "raw", text: k.kind });
    }
  }
  return segs;
}

function renderTimeline() {
  const nearBottom = streamEl.scrollHeight - streamEl.scrollTop - streamEl.clientHeight < 40;
  const segs = groupEvents(streamEvents);
  let html = "", turnNo = 0;
  for (const s of segs) {
    if (s.kind === "init") {
      html += `<div class="tl-init"><span class="k">init</span> `
        + `model=${escapeHtml(s.model || "?")} · ${s.tools.length} tools</div>`;
    } else if (s.kind === "turn") {
      turnNo++;
      html += `<div class="tl-turn"><div class="tl-turn-h">turn ${turnNo}</div>`;
      for (const t of s.tools) {
        const pill = t.ok === null ? `<span class="tl-pill run">running</span>`
          : t.ok ? `<span class="tl-pill ok">ok</span>` : `<span class="tl-pill fail">fail</span>`;
        const open = expandedTools.has(t.seq);
        html += `<div class="tl-tool" data-seq="${t.seq}">`
          + `<div class="tl-tool-h"><span class="tool">▸ ${escapeHtml(t.name)}</span>${pill}</div>`
          + (open ? `<pre class="tl-input">${escapeHtml(JSON.stringify(t.input, null, 2))}</pre>` : "")
          + `</div>`;
      }
      if (s.text.trim()) html += `<div class="tl-out">${escapeHtml(s.text)}</div>`;
      html += `</div>`;
    } else if (s.kind === "status") {
      html += `<div class="tl-status muted">${escapeHtml(s.from)} → ${escapeHtml(s.to)}</div>`;
    } else if (s.kind === "finished") {
      html += `<div class="tl-fin fin">● ${escapeHtml(s.outcome)} · ${s.turns} turns · $${s.cost.toFixed(4)}</div>`;
    } else if (s.kind === "sys") {
      html += `<div class="tl-status muted">⚠ ${escapeHtml(s.text)}</div>`;
    } else {
      html += `<div class="ev muted">${escapeHtml(s.text)}</div>`;
    }
  }
  streamLogEl.innerHTML = html || `<div class="muted">waiting for events…</div>`;
  streamLogEl.querySelectorAll(".tl-tool").forEach((el) => {
    el.querySelector(".tl-tool-h").onclick = () => {
      const seq = Number(el.dataset.seq);
      if (expandedTools.has(seq)) expandedTools.delete(seq); else expandedTools.add(seq);
      renderTimeline();
    };
  });
  if (nearBottom) streamEl.scrollTop = streamEl.scrollHeight;
}
```

- [ ] **Step 4: Keep the `gap` note working** — the SSE `gap` listener appends a
  note to `streamLogEl`; leave it, but move its `appendChild` to happen *before*
  the next `renderTimeline` won't clobber it — simplest: on `gap`, push a synthetic
  marker into `streamEvents` instead:
```js
  evtSource.addEventListener("gap", (e) => {
    let info = {}; try { info = JSON.parse(e.data); } catch {}
    streamEvents.push({ seq: -1, kind: { kind: "store_persist_failed",
      lost_seq: info.skipped ?? 0, detail: "recovered from history" } });
    renderTimeline();
  });
```

- [ ] **Step 5: Manual smoke in a scratch harness** (no backend needed): temporarily
  set `streamEvents` to a synthetic array in the console / a scratch HTML file and
  confirm `groupEvents` + `renderTimeline` produce turns, expandable tools, finish.
  (Verified for real in Task 4.)

- [ ] **Step 6: Commit**

```bash
git add crates/api/dashboard/app.js
git commit -m "feat(dashboard): turn-grouped agent timeline with expandable tool rows (#5)"
```

---

### Task 3: Timeline + header styling in `index.html`

**Files:** Modify `crates/api/dashboard/index.html`.

- [ ] **Step 1: Add CSS** for the new classes, reusing the existing palette
  (accent `#8ab4f8`, tool `#c792ea`, ok `#6bd968`, fail `#ef5350`, muted
  `#6b7280`, card `#171a21`):

```css
      .tl-init { color: #9aa0aa; padding: 4px 0 8px; border-bottom: 1px solid #1c2029; margin-bottom: 8px; }
      .tl-turn { margin: 10px 0; padding-left: 10px; border-left: 2px solid #232733; }
      .tl-turn-h { color: #6b7280; font-size: 11px; text-transform: uppercase; letter-spacing: .5px; margin-bottom: 4px; }
      .tl-tool { background: #171a21; border: 1px solid #232733; border-radius: 6px; margin: 4px 0; }
      .tl-tool-h { display: flex; align-items: center; gap: 8px; padding: 5px 8px; cursor: pointer; }
      .tl-tool-h:hover { background: #1b1f27; }
      .tl-input { margin: 0; padding: 8px; border-top: 1px solid #232733; color: #aab; font-size: 12px; white-space: pre-wrap; overflow-x: auto; }
      .tl-pill { margin-left: auto; font-size: 10px; padding: 1px 7px; border-radius: 999px; background: #2d3340; }
      .tl-pill.ok { background: #1f3d2b; color: #6bd968; }
      .tl-pill.fail { background: #3a2424; color: #ef9a9a; }
      .tl-pill.run { background: #3a3320; color: #e0c56b; }
      .tl-out { color: #d7dae0; white-space: pre-wrap; margin: 6px 0 0; }
      .tl-status { font-size: 11px; padding: 2px 0; }
      .tl-fin { margin-top: 10px; padding-top: 8px; border-top: 1px solid #1c2029; }
```

- [ ] **Step 2: Enrich the header** — in `paintStreamHead` (app.js), show outcome +
  a "running" state. Replace the `cost` line so it reads
  `success · 4 turns · $0.1200` when finished and `— running` while live:
```js
  const cost = c.cost != null && c.turns != null
    ? `<span class="sh-meta">· ${escapeHtml(c.outcome || "done")} · ${c.turns} turns · $${c.cost.toFixed(4)}</span>`
    : `<span class="sh-meta">· running</span>`;
```

- [ ] **Step 3: Commit**

```bash
git add crates/api/dashboard/index.html crates/api/dashboard/app.js
git commit -m "feat(dashboard): timeline styling + cost/outcome header (#5)"
```

---

### Task 4: Visual verification (browser) + gate

**Files:** none (verification).

- [ ] **Step 1: Run the full gate** (Rust side):
  `cargo fmt --all && cargo clippy --workspace --all-targets $TESTKIT -- -D warnings && cargo build --workspace --all-targets $TESTKIT && cargo test --workspace $TESTKIT`

- [ ] **Step 2: Visual check** — run prosperod locally (standalone), point a browser
  at it, drive an agent via the CLI (or the e2e fake path), and confirm: the
  timeline groups turns, tool rows expand to show input JSON + ok/fail pill, and
  the header shows cost/turns/outcome on finish. Use the browser automation
  tools; capture a screenshot into the scratchpad and attach it to the PR.

- [ ] **Step 3: Confirm the dashboard-serves tests still pass**
  `cargo test -p prospero-api --features prospero-core/testkit serves_dashboard_index dashboard_app_js`

---

## Self-Review

- **Spec coverage:** timeline fold (Task 2), tool inspector expand (Task 2/3),
  cost/outcome header (Task 3), contract test (Task 1), visual check (Task 4).
  Overview/charts/nav intentionally absent (→ #95). Covered.
- **Placeholders:** none — full `groupEvents`/`renderTimeline`, CSS, and the Rust test.
- **Totality:** `groupEvents` `default` arm emits a raw fallback; unmatched
  `tool_finished` is ignored safely; unclosed `tool_started` renders `running`.
- **Type consistency:** JS reads `kind.kind` ∈ {output, tool_started, tool_finished,
  agent_init, agent_finished, status_changed, store_persist_failed} with fields
  {chunk; name,input; name,ok; model,tools; outcome,cost_usd,turns; from,to;
  lost_seq} — matches `EventKind` and the Task 1 contract test.
