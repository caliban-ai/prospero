# Dashboard Layout & Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A frontend-only polish pass on the Prospero dashboard — sticky headers, cleaner agent rows, a stream-pane header bar with a live-connection indicator, empty/loading states, hover-reveal row actions, and responsive single-column collapse — with no new data or backend changes.

**Architecture:** Edits confined to the two embedded dashboard assets (`crates/api/dashboard/index.html` CSS+structure, `crates/api/dashboard/app.js` render logic). Everything shown is derivable from the existing fleet snapshot (`id/name/status/started_at/isolated/interactive`) or from events already streamed. Vanilla JS/CSS, no build step.

**Tech Stack:** Vanilla JS + CSS, embedded via `include_str!` in `crates/api/src/dashboard.rs` (so changes require a `prosperod` rebuild + restart to appear).

**Design spec:** `docs/superpowers/specs/2026-06-13-dashboard-polish-design.md`

---

## Working notes

- **No JS test runner.** Each task verifies with `cargo build --bin prosperod` (clean), `node --check crates/api/dashboard/app.js` (clean), and `curl` greps confirming the new code shipped (requires rebuild+restart). The interactive **browser pass is consolidated in Task 4** — implementers can't drive a browser, so per-task browser checks are deferred there.
- **Preserve all existing behavior**: the modals (add-repo/launch/settings), row actions, the interactive input box + its poll-time input-preservation, and SSE streaming must keep working. The polish must not regress them.
- **Rebuild-and-restart** (for the `curl` asset checks):
  ```bash
  pkill -f 'target/debug/prosperod'; sleep 1
  cargo run --bin prosperod -- --caliband-bin "$HOME/dev/caliban-ai/caliban/target/release/caliband" > /tmp/prosperod.log 2>&1 &
  until grep -q listening /tmp/prosperod.log; do sleep 1; done
  ```

## File structure

| File | Change | Responsibility |
|------|--------|----------------|
| `crates/api/dashboard/index.html` | Modify | Spacing tokens; sticky pane/repo/stream headers; hover-reveal & meta styles; stream-head + empty-state styles; responsive media query; `#stream` restructured into `#stream-head` + `#stream-log`; remove the `#conn` page-header span. |
| `crates/api/dashboard/app.js` | Modify | `elapsed`/`agentMeta` helpers; fleet count; agent meta line; `lastFleet`/`findAgent`; stream-head paint (`selectAgent`/`appendEvent`); empty state; responsive `show-stream` toggle + back affordance. |

---

## Task 1: Sidebar polish — spacing tokens, sticky headers, fleet count, agent meta, hover-reveal actions

**Files:** Modify `crates/api/dashboard/index.html`, `crates/api/dashboard/app.js`

- [ ] **Step 1: CSS — spacing tokens + sidebar polish (index.html).**

Change the `:root` line to add spacing tokens:
```css
      :root { color-scheme: light dark; --sp-1: 4px; --sp-2: 6px; --sp-3: 10px; --sp-4: 14px; }
```

Replace the `#fleet` rule and `#fleet-head` rule. Current:
```css
      #fleet { overflow-y: auto; border-right: 1px solid #232733; padding: 12px; }
```
```css
      #fleet-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
```
with:
```css
      #fleet { overflow-y: auto; border-right: 1px solid #232733; padding: 0 12px 12px; }
      #fleet-head { display: flex; justify-content: space-between; align-items: center;
                    position: sticky; top: 0; z-index: 3; background: #0f1115;
                    padding: 12px 0 8px; }
```

Replace the `.repo-head` rule. Current:
```css
      .repo-head { display: flex; justify-content: space-between; align-items: center; }
```
with (sticky just below the fleet header; `top` ≈ fleet-head height, tunable):
```css
      .repo-head { display: flex; justify-content: space-between; align-items: center;
                   position: sticky; top: 44px; z-index: 2; background: #0f1115; padding: 4px 0; }
```

Replace the `.acts` rule and the `.repo-head-actions` rule to make actions hover-reveal (de-emphasized by default; fully shown on hover / keyboard focus / selection — opacity keeps them tabbable, unlike `display:none`). Current:
```css
      .acts { display: flex; gap: 4px; }
```
```css
      .repo-head-actions { display: flex; gap: 4px; }
```
with:
```css
      .acts { display: flex; gap: var(--sp-1); opacity: .4; transition: opacity .1s; }
      .agent:hover .acts, .agent:focus-within .acts, .agent.selected .acts { opacity: 1; }
      .repo-head-actions { display: flex; gap: var(--sp-1); opacity: .5; transition: opacity .1s; }
      .repo-head:hover .repo-head-actions, .repo-head:focus-within .repo-head-actions { opacity: 1; }
      .meta { color: #6b7280; font-size: 10px; }
```

Replace the `.agent` rule to use tokens / tidy spacing. Current:
```css
      .agent {
        display: flex; justify-content: space-between; gap: 8px;
        padding: 6px 8px; margin: 4px 0; border-radius: 6px;
        background: #171a21; cursor: pointer; border: 1px solid transparent;
      }
```
with:
```css
      .agent {
        display: flex; justify-content: space-between; gap: var(--sp-2);
        padding: var(--sp-2) var(--sp-3); margin: var(--sp-2) 0; border-radius: 7px;
        background: #171a21; cursor: pointer; border: 1px solid transparent;
      }
```

- [ ] **Step 2: index.html — give the fleet label an id.** Change:
```html
          <span class="fleet-label">fleet</span>
```
to:
```html
          <span class="fleet-label" id="fleet-count">fleet</span>
```

- [ ] **Step 3: app.js — `elapsed`/`agentMeta` helpers.** Add near the other helpers (e.g. just before `renderFleet`):
```js
// Human-ish elapsed string from an RFC-3339 timestamp (client-side, no new data).
function elapsed(startedAt) {
  const ms = Date.now() - new Date(startedAt).getTime();
  if (!isFinite(ms) || ms < 0) return "";
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  return `${Math.floor(m / 60)}h`;
}

// Derivable per-agent meta line (no cost/turns — not in the snapshot).
function agentMeta(agent) {
  const e = elapsed(agent.started_at);
  if (!e) return "";
  if (agent.status === "idle") return ` · idle ${e}`;
  if (isActive(agent.status)) return ` · ${e}`;
  return ` · ${agent.status} ${e} ago`;
}
```

- [ ] **Step 4: app.js — fleet count in the sticky header (`renderFleet`).** At the top of `renderFleet(fleet)`, right after the `healthyRepos = …` assignment, add:
```js
  const agentTotal = fleet.repos.reduce((n, r) => n + r.agents.length, 0);
  const rc = fleet.repos.length;
  document.getElementById("fleet-count").textContent =
    `fleet · ${rc} repo${rc === 1 ? "" : "s"} · ${agentTotal} agent${agentTotal === 1 ? "" : "s"}`;
```

- [ ] **Step 5: app.js — agent meta line (`renderAgent`).** Replace the `info.innerHTML = …` assignment. Current:
```js
  info.innerHTML =
    `<span class="name">${escapeHtml(agent.name)}</span> ${wt}<br><span class="id">${escapeHtml(agent.id)}</span>`;
```
with:
```js
  info.innerHTML =
    `<span class="name">${escapeHtml(agent.name)}</span> ${wt}<br>` +
    `<span class="id">${escapeHtml(agent.id)}</span><span class="meta">${escapeHtml(agentMeta(agent))}</span>`;
```

- [ ] **Step 6: Build, verify, (browser deferred to Task 4).**
```bash
cargo build --bin prosperod 2>&1 | tail -1
node --check crates/api/dashboard/app.js && echo js-ok
```
Both clean. Then rebuild-and-restart (Working notes) and:
```bash
curl -s http://127.0.0.1:7878/app.js | grep -c "function agentMeta"        # expect 1
curl -s http://127.0.0.1:7878/app.js | grep -cF 'fleet · '                  # expect 1
curl -s http://127.0.0.1:7878/ | grep -c 'id="fleet-count"'                 # expect 1
curl -s http://127.0.0.1:7878/ | grep -cF 'position: sticky'               # expect >= 2 (fleet-head + repo-head)
curl -s http://127.0.0.1:7878/api/fleet | head -c 60                        # fleet still serves
```
Expected: 1, 1, 1, ≥2, valid JSON.

- [ ] **Step 7: Commit.**
```bash
git add crates/api/dashboard/index.html crates/api/dashboard/app.js
git commit -m "feat(dashboard): sidebar polish — sticky headers, fleet count, agent meta, hover-reveal actions"
```

---

## Task 2: Stream-pane header bar + empty state

**Files:** Modify `crates/api/dashboard/index.html`, `crates/api/dashboard/app.js`

- [ ] **Step 1: index.html — restructure `#stream` + remove the `#conn` span.**

Replace:
```html
      <section id="stream"><div class="muted">select an agent to stream its output</div></section>
```
with:
```html
      <section id="stream">
        <div id="stream-head" class="hidden"></div>
        <div id="stream-log"></div>
      </section>
```

Remove the page-header connection span (the live indicator moves into the stream header):
```html
      <span class="sub" id="conn"></span>
```
(delete that line).

- [ ] **Step 2: index.html — CSS for `#stream` / header / log / empty state.** Replace the `#stream` rule. Current:
```css
      #stream { overflow-y: auto; padding: 12px; white-space: pre-wrap; }
```
with:
```css
      #stream { overflow-y: auto; padding: 0; }
      #stream-log { padding: 12px; white-space: pre-wrap; }
      #stream-head { display: flex; align-items: center; gap: var(--sp-3);
                     position: sticky; top: 0; z-index: 2; background: #0f1115;
                     padding: 8px 12px; border-bottom: 1px solid #1c2029; }
      .sh-name { color: #8ab4f8; font-weight: 600; }
      .sh-meta { color: #6b7280; font-size: 11px; }
      .sh-conn { margin-left: auto; font-size: 10px; }
      .sh-conn.live { color: #6bd968; }
      .sh-conn.closed { color: #ef5350; }
      .sh-back { display: none; font: 11px ui-monospace, monospace; padding: 2px 8px;
                 border-radius: 6px; border: 1px solid #2d3340; background: #1b1f27; color: #9aa0aa; cursor: pointer; }
      .empty { display: flex; flex-direction: column; align-items: center; justify-content: center;
               min-height: 50vh; gap: 6px; color: #6b7280; }
      .empty-title { color: #9aa0aa; font-size: 13px; }
      .empty-hint { font-size: 11px; }
```

- [ ] **Step 3: app.js — globals + lastFleet/findAgent.** Replace the globals block. Current:
```js
const fleetEl = document.getElementById("fleet");
const fleetListEl = document.getElementById("fleet-list");
const streamEl = document.getElementById("stream");
const hostEl = document.getElementById("host");
const connEl = document.getElementById("conn");
const modalRoot = document.getElementById("modal-root");

let selectedAgent = null;
let evtSource = null;
let healthyRepos = []; // names of reachable repos, for the launch picker
```
with:
```js
const fleetEl = document.getElementById("fleet");
const fleetListEl = document.getElementById("fleet-list");
const streamEl = document.getElementById("stream");
const streamHeadEl = document.getElementById("stream-head");
const streamLogEl = document.getElementById("stream-log");
const hostEl = document.getElementById("host");
const modalRoot = document.getElementById("modal-root");

let selectedAgent = null;
let evtSource = null;
let healthyRepos = []; // names of reachable repos, for the launch picker
let lastFleet = null; // most recent fleet snapshot, for stream-head lookups
let streamCtx = null; // { id, name, status, model, cost, turns } for the streamed agent

function findAgent(id) {
  if (!lastFleet) return null;
  for (const r of lastFleet.repos) {
    for (const a of r.agents) if (a.id === id) return a;
  }
  return null;
}

function streamIsLive() {
  return !!evtSource && evtSource.readyState === 1; // EventSource.OPEN
}
```

- [ ] **Step 4: app.js — `lastFleet` + stream-head refresh in `renderFleet`.** At the very top of `renderFleet(fleet)` (before the `healthyRepos = …` line), add:
```js
  lastFleet = fleet;
```
At the END of `renderFleet` (just before its closing `}`, after the input-restore loop), add — keep the selected agent's header badge fresh on each poll:
```js
  if (streamCtx) {
    const a = findAgent(streamCtx.id);
    if (a) {
      streamCtx.name = a.name;
      streamCtx.status = a.status;
      paintStreamHead();
    }
  }
```

- [ ] **Step 5: app.js — empty state + `paintStreamHead`.** Add these functions (near `selectAgent`):
```js
function openEmptyStream() {
  selectedAgent = null;
  streamCtx = null;
  if (evtSource) { evtSource.close(); evtSource = null; }
  streamHeadEl.classList.add("hidden");
  streamLogEl.innerHTML =
    `<div class="empty"><div class="empty-title">No agent selected</div>` +
    `<div class="empty-hint">Pick an agent from the fleet to stream its live output.</div></div>`;
}

function paintStreamHead() {
  if (!streamCtx) return;
  const c = streamCtx;
  const badge = `<span class="badge ${c.status}">${escapeHtml(c.status)}</span>`;
  const model = c.model ? `<span class="sh-meta">${escapeHtml(c.model)}</span>` : "";
  const cost = c.cost != null
    ? `<span class="sh-meta">· $${c.cost.toFixed(4)} · ${c.turns} turns</span>` : "";
  const dot = streamIsLive()
    ? `<span class="sh-conn live">● live</span>` : `<span class="sh-conn closed">⚠ closed</span>`;
  streamHeadEl.innerHTML =
    `<button class="sh-back" title="back to fleet">← fleet</button>` +
    `<span class="sh-name">${escapeHtml(c.name)}</span> ${badge} ${model} ${cost} ${dot}`;
  streamHeadEl.querySelector(".sh-back").onclick = () => document.body.classList.remove("show-stream");
  streamHeadEl.classList.remove("hidden");
}
```

- [ ] **Step 6: app.js — rewrite `selectAgent` to populate the header + log.** Replace the whole `selectAgent` function. Current:
```js
function selectAgent(id) {
  selectedAgent = id;
  refreshFleet();
  streamEl.innerHTML = "";
  if (evtSource) evtSource.close();
  connEl.textContent = `▶ streaming ${id}`;
  evtSource = new EventSource(`/api/agents/${id}/stream`);
  evtSource.onmessage = (e) => appendEvent(JSON.parse(e.data));
  evtSource.onerror = () => {
    connEl.textContent = `⚠ stream closed for ${id}`;
  };
}
```
with:
```js
function selectAgent(id) {
  selectedAgent = id;
  document.body.classList.add("show-stream"); // responsive: reveal the stream pane
  const a = findAgent(id);
  streamCtx = { id, name: (a && a.name) || id, status: (a && a.status) || "", model: null, cost: null, turns: null };
  refreshFleet();
  streamLogEl.innerHTML = "";
  if (evtSource) evtSource.close();
  evtSource = new EventSource(`/api/agents/${encodeURIComponent(id)}/stream`);
  evtSource.onopen = () => paintStreamHead();
  evtSource.onmessage = (e) => appendEvent(JSON.parse(e.data));
  evtSource.onerror = () => paintStreamHead();
  paintStreamHead();
}
```

- [ ] **Step 7: app.js — `appendEvent` updates the header + appends to the log.** Replace the start and end of `appendEvent`. Change the opening (after `const k = ev.kind;`) to update header state first:
```js
function appendEvent(ev) {
  const k = ev.kind;
  if (streamCtx) {
    if (k.kind === "agent_init") streamCtx.model = k.model;
    else if (k.kind === "agent_finished") { streamCtx.cost = k.cost_usd; streamCtx.turns = k.turns; }
    else if (k.kind === "status_changed") streamCtx.status = k.to;
    paintStreamHead();
  }
  const line = document.createElement("div");
  line.className = "ev";
  let body;
```
(the `switch (k.kind) { … }` body stays exactly as-is). Change the final three lines from:
```js
  line.innerHTML = body;
  streamEl.appendChild(line);
  streamEl.scrollTop = streamEl.scrollHeight;
}
```
to (append to the log container, scroll the pane):
```js
  line.innerHTML = body;
  streamLogEl.appendChild(line);
  streamEl.scrollTop = streamEl.scrollHeight;
}
```

- [ ] **Step 8: app.js — show the empty state on load.** At the bottom init block, change:
```js
document.getElementById("add-repo-btn").onclick = openAddRepoModal;
refreshFleet();
setInterval(refreshFleet, 3000);
```
to:
```js
document.getElementById("add-repo-btn").onclick = openAddRepoModal;
openEmptyStream();
refreshFleet();
setInterval(refreshFleet, 3000);
```

- [ ] **Step 9: Build, verify.**
```bash
cargo build --bin prosperod 2>&1 | tail -1
node --check crates/api/dashboard/app.js && echo js-ok
```
Then rebuild-and-restart and:
```bash
curl -s http://127.0.0.1:7878/ | grep -c 'id="stream-head"'        # expect 1
curl -s http://127.0.0.1:7878/ | grep -c 'id="stream-log"'         # expect 1
curl -s http://127.0.0.1:7878/ | grep -c 'id="conn"'              # expect 0 (removed)
curl -s http://127.0.0.1:7878/app.js | grep -c "function paintStreamHead"  # expect 1
curl -s http://127.0.0.1:7878/app.js | grep -c "function openEmptyStream"  # expect 1
```
Expected: 1, 1, 0, 1, 1.

- [ ] **Step 10: Commit.**
```bash
git add crates/api/dashboard/index.html crates/api/dashboard/app.js
git commit -m "feat(dashboard): stream-pane header bar (name/status/model/live) + empty state"
```

---

## Task 3: Responsive single-column collapse

**Files:** Modify `crates/api/dashboard/index.html`

- [ ] **Step 1: CSS — responsive media query.** In `crates/api/dashboard/index.html`, before `</style>`, add:
```css
      @media (max-width: 720px) {
        main { grid-template-columns: 1fr; }
        #fleet { border-right: none; }
        #stream { display: none; }
        body.show-stream #fleet { display: none; }
        body.show-stream #stream { display: block; }
        .sh-back { display: inline-block; }
      }
```

> Behavior: above 720px both panes show side-by-side as today (the query doesn't apply, and `.sh-back` stays `display:none` from its base rule). At ≤720px the fleet shows by default; `selectAgent` adds `show-stream` (Task 2) which swaps to the stream pane, and the `← fleet` back button (revealed only here) clears `show-stream` to return. No JS change — the `show-stream` toggling and back button were already wired in Task 2.

- [ ] **Step 2: Build, verify.**
```bash
cargo build --bin prosperod 2>&1 | tail -1
curl -s http://127.0.0.1:7878/ | grep -c "max-width: 720px"   # expect 1 (after rebuild+restart)
```
(Rebuild-and-restart first if checking the served asset.) Expected: clean build, 1.

- [ ] **Step 3: Commit.**
```bash
git add crates/api/dashboard/index.html
git commit -m "feat(dashboard): responsive single-column collapse under 720px"
```

---

## Task 4: Final verification — browser pass + tidy

**Files:** none (verification only).

- [ ] **Step 1: Gates.**
```bash
cargo build --bin prosperod 2>&1 | tail -1
node --check crates/api/dashboard/app.js && echo js-ok
cargo fmt --all --check && echo fmt-ok    # no Rust changed, but confirm clean
```

- [ ] **Step 2: Browser pass** (rebuild-and-restart, then open `http://127.0.0.1:7878`). Confirm:
  1. The fleet header reads `fleet · N repos · M agents` and **stays pinned** while scrolling; repo headers stay pinned within their section.
  2. Agent rows show the two-line layout with the derivable meta (`· 2m` / `· idle 40s` / `· done 5m ago`); row actions are faint by default and **fully appear on hover, on keyboard Tab (focus), and on the selected row**.
  3. Selecting an agent shows the **stream header bar** (name, status badge, model once `agent_init` arrives, `● live` while connected → `⚠ closed` on close; `$cost · N turns` after `agent_finished`).
  4. With **no agent selected**, the stream pane shows the centered empty state.
  5. Narrow the window below 720px: it collapses to one column (fleet only); selecting an agent shows the stream with a working `← fleet` back button; widening restores the two-pane view.
  6. Existing flows still work: add/launch/settings modals, kill/respawn/remove, the interactive input box (and typing survives the 3s poll).

- [ ] **Step 3: Finish the branch.** Use **superpowers:finishing-a-development-branch**.

---

## Self-review notes

- **Spec coverage:** sticky pane headers + count (Task 1), sticky repo headers (Task 1), cleaner rows + derivable meta (Task 1), spacing tokens (Task 1), hover-reveal w/ hover+focus-within+selected (Task 1), stream-pane header bar w/ live dot + model + cost/turns (Task 2), empty/loading states (Task 2 + existing loading text), responsive collapse w/ back affordance (Task 3), browser verification (Task 4). All spec §3–§6 map to a task. No new data used (meta is `started_at`-derived; model/cost/turns come from the already-streamed `agent_init`/`agent_finished` events).
- **Type/name consistency:** new helpers `elapsed`, `agentMeta`, `findAgent`, `streamIsLive`, `paintStreamHead`, `openEmptyStream`; globals `streamHeadEl`, `streamLogEl`, `lastFleet`, `streamCtx`; CSS ids `#fleet-count`, `#stream-head`, `#stream-log`; classes `.meta`, `.sh-name`/`.sh-meta`/`.sh-conn`/`.sh-back`, `.empty*`; body class `show-stream` — each defined once and referenced consistently across tasks. `appendEvent` now targets `streamLogEl` (the `streamEl` scroll target is unchanged for `scrollTop`). The removed `connEl` global is no longer referenced anywhere (only `selectAgent` used it, which is rewritten).
- **Known tunable:** `.repo-head { top: 44px }` is an approximate offset for the fleet-header height; if visual overlap appears in the browser pass, nudge it to match.
