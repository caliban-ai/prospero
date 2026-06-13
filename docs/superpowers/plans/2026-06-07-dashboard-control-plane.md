# Dashboard Control-Plane Controls Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the Prospero web dashboard to CLI parity — register/remove repos, launch agents, and kill/respawn/remove agents — so an operator never has to leave the dashboard.

**Architecture:** Frontend-only change to two embedded assets: `crates/api/dashboard/index.html` (styles + structure + modal root) and `crates/api/dashboard/app.js` (API helpers, modals, row actions). Create actions use modals; row actions are inline buttons. All actions hit existing, already-tested HTTP endpoints — **no Rust/backend changes**.

**Tech Stack:** Vanilla JS (no build step, no dependencies), axum-served embedded assets, dark-monospace dashboard already in place.

**Design spec:** `docs/superpowers/specs/2026-06-07-dashboard-control-plane-design.md`

---

## Working notes (read before starting)

**Assets are compiled in.** `crates/api/src/dashboard.rs` embeds the dashboard with
`include_str!`. Editing `index.html`/`app.js` has **no effect** until `prosperod` is
rebuilt and restarted. The standard **rebuild-and-restart** procedure, referenced by every
task's verify step:

```bash
# from repo root: stop any running daemon, rebuild, relaunch with the real caliband wired in
pkill -f 'target/debug/prosperod' 2>/dev/null; sleep 1
cargo run --bin prosperod -- \
  --caliband-bin "$HOME/dev/caliban-ai/caliban/target/release/caliband" \
  > /tmp/prosperod.log 2>&1 &
# wait for readiness
until grep -q "listening" /tmp/prosperod.log; do sleep 1; done
echo "daemon up on http://127.0.0.1:7878"
```

**No JS test harness exists.** Verification per task is: (a) `curl` the served asset to
confirm the new code shipped, (b) `curl` the relevant `/api/...` endpoint to confirm the
real fleet effect, and (c) a browser interaction at http://127.0.0.1:7878. This matches
spec §6 (manual end-to-end).

**Sanity gate after every asset edit** (cheap, catches obvious breakage — embedded assets
won't fail the build for JS errors, so this is the guard):

```bash
cargo build --bin prosperod   # must succeed; include_str! re-embeds the edited files
```

**Branch:** all work lands on `dashboard-control-plane` (already created; spec committed there).

---

## File structure

| File | Change | Responsibility |
|------|--------|----------------|
| `crates/api/dashboard/index.html` | Modify | Add CSS for modal/banner/buttons/forms; restructure `#fleet` into `#fleet-head` + `#banner` + `#fleet-list`; add `#modal-root`. |
| `crates/api/dashboard/app.js` | Modify | Add `api()`/`showBanner()`/`openModal()`/`closeModal()` helpers; refactor render into `#fleet-list`; add-repo & launch modals; status-aware row actions. |

No other files change. `crates/api/src/dashboard.rs` already serves whatever these two files contain.

---

## Task 1: Foundation — layout restructure, CSS, and helper layer

Establishes the scaffolding everything else builds on, with **no behavior change yet**: the
dashboard must look and work exactly as before, plus an inert `＋ add repo` button and a
hidden banner/modal root.

**Files:**
- Modify: `crates/api/dashboard/index.html`
- Modify: `crates/api/dashboard/app.js`

- [ ] **Step 1: Add the new CSS block to `index.html`**

In `crates/api/dashboard/index.html`, immediately before the closing `</style>` tag (after the existing `.muted` rule), insert:

```css
      /* --- control-plane additions --- */
      .hidden { display: none; }
      #fleet-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
      .fleet-label { color: #6b7280; font-size: 11px; text-transform: uppercase; letter-spacing: .5px; }
      .banner { background: #3a2424; color: #ef9a9a; border: 1px solid #5a3030; border-radius: 6px;
                padding: 6px 10px; margin-bottom: 10px; font-size: 12px; cursor: pointer; }
      .ctl-btn { font: 11px ui-monospace, monospace; padding: 3px 9px; border-radius: 6px;
                 border: 1px solid #2d3340; background: #1b1f27; color: #aab; cursor: pointer; }
      .ctl-btn:hover { border-color: #3a4256; }
      .ctl-btn.primary { background: #1f2a44; color: #8ab4f8; border-color: #2d3a5a; }
      .ctl-btn:disabled { opacity: .5; cursor: default; }
      .ctl-btn.launch { margin: 6px 0; }
      .repo-head { display: flex; justify-content: space-between; align-items: center; }
      .act-btn { font: 10px ui-monospace, monospace; padding: 1px 7px; border-radius: 5px;
                 border: 1px solid #2d3340; background: #1b1f27; color: #9aa0aa; cursor: pointer; }
      .act-btn:hover { border-color: #3a4256; }
      .act-btn.danger { color: #ef9a9a; border-color: #3a2424; }
      .act-btn:disabled { opacity: .5; cursor: default; }
      .agent-right { display: flex; flex-direction: column; align-items: flex-end; gap: 4px; }
      .acts { display: flex; gap: 4px; }
      .modal-root.hidden { display: none; }
      .scrim { position: fixed; inset: 0; background: rgba(0,0,0,.55);
               display: flex; align-items: flex-start; justify-content: center; z-index: 50; }
      .modal { margin-top: 80px; width: 340px; background: #13161c; border: 1px solid #3a4256;
               border-radius: 10px; padding: 14px; box-shadow: 0 12px 40px rgba(0,0,0,.6); }
      .form-title { color: #8ab4f8; font-size: 11px; text-transform: uppercase; letter-spacing: .5px;
                    margin-bottom: 10px; }
      .fl { display: block; font-size: 11px; color: #9aa0aa; margin-bottom: 8px; }
      .in { display: block; width: 100%; margin-top: 3px; background: #0f1115; border: 1px solid #2d3340;
            border-radius: 5px; color: #d7dae0; padding: 5px 7px; font: 12px ui-monospace, monospace; }
      textarea.in { resize: vertical; }
      .chk { display: block; font-size: 12px; color: #aab; margin-bottom: 8px; }
      .adv-toggle { color: #6b7280; font-size: 11px; cursor: pointer; margin-bottom: 8px; }
      .form-err { color: #ef9a9a; font-size: 11px; min-height: 14px; margin-bottom: 6px; }
      .form-actions { display: flex; justify-content: flex-end; gap: 8px; }
```

- [ ] **Step 2: Restructure the `#fleet` section and add the modal root in `index.html`**

Replace this existing line:

```html
      <section id="fleet"><div class="muted">loading fleet…</div></section>
```

with:

```html
      <section id="fleet">
        <div id="fleet-head">
          <span class="fleet-label">fleet</span>
          <button id="add-repo-btn" class="ctl-btn">＋ add repo</button>
        </div>
        <div id="banner" class="banner hidden"></div>
        <div id="fleet-list"><div class="muted">loading fleet…</div></div>
      </section>
```

Then, immediately before the `<script src="/app.js"></script>` line, add:

```html
    <div id="modal-root" class="modal-root hidden"></div>
```

- [ ] **Step 3: Add new globals and helper functions to `app.js`**

In `crates/api/dashboard/app.js`, replace the existing globals block:

```js
const fleetEl = document.getElementById("fleet");
const streamEl = document.getElementById("stream");
const hostEl = document.getElementById("host");
const connEl = document.getElementById("conn");

let selectedAgent = null;
let evtSource = null;
```

with:

```js
const fleetEl = document.getElementById("fleet");
const fleetListEl = document.getElementById("fleet-list");
const streamEl = document.getElementById("stream");
const hostEl = document.getElementById("host");
const connEl = document.getElementById("conn");
const modalRoot = document.getElementById("modal-root");

let selectedAgent = null;
let evtSource = null;

// --- API + UX helpers -------------------------------------------------------

// JSON API call. Returns parsed JSON (or null for empty bodies).
// Throws Error(message) carrying the server's `error` string on non-2xx.
async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (!res.ok) {
    throw new Error((data && data.error) || `${res.status} ${res.statusText}`);
  }
  return data;
}

// Transient, click-to-dismiss error banner above the fleet list.
function showBanner(msg) {
  const bar = document.getElementById("banner");
  bar.textContent = msg;
  bar.classList.remove("hidden");
  bar.onclick = () => bar.classList.add("hidden");
}

// Modal infrastructure — one node at a time inside #modal-root.
function openModal(node) {
  modalRoot.innerHTML = "";
  const scrim = document.createElement("div");
  scrim.className = "scrim";
  scrim.onclick = (e) => { if (e.target === scrim) closeModal(); };
  const box = document.createElement("div");
  box.className = "modal";
  box.appendChild(node);
  scrim.appendChild(box);
  modalRoot.appendChild(scrim);
  modalRoot.classList.remove("hidden");
}

function closeModal() {
  modalRoot.classList.add("hidden");
  modalRoot.innerHTML = "";
}
```

- [ ] **Step 4: Point rendering at `#fleet-list` instead of `#fleet`**

In `app.js`, in `refreshFleet()`, change the catch branch target from `fleetEl` to `fleetListEl`:

```js
  } catch (e) {
    fleetListEl.innerHTML = `<div class="health unreachable">fleet unreachable: ${e}</div>`;
  }
```

In `renderFleet(fleet)`, change the two `fleetEl` references to `fleetListEl`:

```js
function renderFleet(fleet) {
  if (!fleet.repos.length) {
    fleetListEl.innerHTML = `<div class="muted">no repos registered</div>`;
    return;
  }
  fleetListEl.innerHTML = "";
```

and the append at the end of the repo loop:

```js
    fleetListEl.appendChild(box);
```

(Leave the `fleetEl` global defined — it still scopes the section; only rendering moves to `#fleet-list`.)

- [ ] **Step 5: Build, rebuild-and-restart, verify no regression**

```bash
cargo build --bin prosperod
```
Expected: builds clean.

Then run the rebuild-and-restart procedure from "Working notes". Then:

```bash
curl -s http://127.0.0.1:7878/app.js | grep -c "function openModal"
```
Expected: `1`

```bash
curl -s http://127.0.0.1:7878/ | grep -c 'id="modal-root"'
```
Expected: `1`

In the browser at http://127.0.0.1:7878: the fleet pane now shows a `fleet  ＋ add repo` header and lists the `prospero` repo and any agents exactly as before. Clicking `＋ add repo` does nothing yet (wired in Task 2). No console errors.

- [ ] **Step 6: Commit**

```bash
git add crates/api/dashboard/index.html crates/api/dashboard/app.js
git commit -m "feat(dashboard): scaffolding for control-plane controls (layout, css, helpers)"
```

---

## Task 2: Add-repo modal

**Files:**
- Modify: `crates/api/dashboard/app.js`

- [ ] **Step 1: Add `openAddRepoModal()` to `app.js`**

Add this function (place it after `closeModal()`):

```js
// --- Add-repo modal ---------------------------------------------------------

function openAddRepoModal() {
  const form = document.createElement("div");
  form.innerHTML =
    `<div class="form-title">add repo</div>` +
    `<label class="fl">name<input class="in" id="ar-name" placeholder="my-repo"></label>` +
    `<label class="fl">path<input class="in" id="ar-root" placeholder="/path/to/repo"></label>` +
    `<div class="form-err" id="ar-err"></div>` +
    `<div class="form-actions">` +
      `<button class="ctl-btn" id="ar-cancel">cancel</button>` +
      `<button class="ctl-btn primary" id="ar-submit">add</button>` +
    `</div>`;
  openModal(form);
  form.querySelector("#ar-cancel").onclick = closeModal;
  const submit = form.querySelector("#ar-submit");
  submit.onclick = async () => {
    const name = form.querySelector("#ar-name").value.trim();
    const root = form.querySelector("#ar-root").value.trim();
    const err = form.querySelector("#ar-err");
    err.textContent = "";
    if (!name || !root) { err.textContent = "name and path are required"; return; }
    submit.disabled = true;
    try {
      await api("POST", "/api/repos", { name, root });
      closeModal();
      refreshFleet();
    } catch (e) {
      err.textContent = String(e.message || e);
      submit.disabled = false;
    }
  };
}
```

- [ ] **Step 2: Wire the `＋ add repo` button at init**

At the bottom of `app.js`, change the init block:

```js
refreshFleet();
setInterval(refreshFleet, 3000);
```

to:

```js
document.getElementById("add-repo-btn").onclick = openAddRepoModal;
refreshFleet();
setInterval(refreshFleet, 3000);
```

- [ ] **Step 3: Build, rebuild-and-restart, verify**

```bash
cargo build --bin prosperod
```
Expected: clean. Then rebuild-and-restart (Working notes).

```bash
curl -s http://127.0.0.1:7878/app.js | grep -c "function openAddRepoModal"
```
Expected: `1`

Browser: click `＋ add repo`, enter name `prospero2` and path `/Users/johnford2002/dev/caliban-ai/prospero`, click `add`. Modal closes; a second repo appears in the sidebar within ~3s. Cross-check:

```bash
curl -s http://127.0.0.1:7878/api/repos | grep -c '"name":"prospero2"'
```
Expected: `1`

Error path: click `＋ add repo` again, enter the same name `prospero2` and any path, click `add`. The modal stays open and shows an inline error (a duplicate-name conflict), not a silent failure.

Clean up the test repo so later tasks start fresh:

```bash
curl -s -X DELETE http://127.0.0.1:7878/api/repos/prospero2 -o /dev/null -w "%{http_code}\n"
```
Expected: `200`

- [ ] **Step 4: Commit**

```bash
git add crates/api/dashboard/app.js
git commit -m "feat(dashboard): add-repo modal"
```

---

## Task 3: Launch-agent modal

**Files:**
- Modify: `crates/api/dashboard/app.js`

- [ ] **Step 1: Add `openLaunchModal()` to `app.js`**

Add after `openAddRepoModal()`:

```js
// --- Launch-agent modal -----------------------------------------------------

function openLaunchModal(repoName) {
  const form = document.createElement("div");
  form.innerHTML =
    `<div class="form-title">launch agent</div>` +
    `<label class="fl">repo<input class="in" id="la-repo" value="${repoName}"></label>` +
    `<label class="fl">task<textarea class="in" id="la-task" rows="3" placeholder="describe the task"></textarea></label>` +
    `<label class="chk"><input type="checkbox" id="la-wt" checked> worktree isolation</label>` +
    `<div class="adv-toggle" id="la-adv-toggle">▸ advanced</div>` +
    `<div class="hidden" id="la-adv">` +
      `<label class="fl">label<input class="in" id="la-label"></label>` +
      `<label class="fl">model<input class="in" id="la-model"></label>` +
      `<label class="fl">tools<input class="in" id="la-tools" placeholder="comma,separated"></label>` +
    `</div>` +
    `<div class="form-err" id="la-err"></div>` +
    `<div class="form-actions">` +
      `<button class="ctl-btn" id="la-cancel">cancel</button>` +
      `<button class="ctl-btn primary" id="la-submit">spawn</button>` +
    `</div>`;
  openModal(form);

  const adv = form.querySelector("#la-adv");
  const advToggle = form.querySelector("#la-adv-toggle");
  advToggle.onclick = () => {
    adv.classList.toggle("hidden");
    advToggle.textContent = adv.classList.contains("hidden") ? "▸ advanced" : "▾ advanced";
  };

  form.querySelector("#la-cancel").onclick = closeModal;
  const submit = form.querySelector("#la-submit");
  submit.onclick = async () => {
    const repo = form.querySelector("#la-repo").value.trim();
    const prompt = form.querySelector("#la-task").value.trim();
    const err = form.querySelector("#la-err");
    err.textContent = "";
    if (!repo || !prompt) { err.textContent = "repo and task are required"; return; }

    const body = { prompt };
    if (!form.querySelector("#la-wt").checked) body.isolation = "shared";
    const label = form.querySelector("#la-label").value.trim();
    const model = form.querySelector("#la-model").value.trim();
    const tools = form.querySelector("#la-tools").value.trim();
    if (label) body.label = label;
    if (model) body.model = model;
    if (tools) body.tool_allowlist = tools.split(",").map((s) => s.trim()).filter(Boolean);

    submit.disabled = true;
    try {
      const res = await api("POST", `/api/repos/${encodeURIComponent(repo)}/agents`, body);
      closeModal();
      refreshFleet();
      if (res && res.agent_id) selectAgent(res.agent_id);
    } catch (e) {
      err.textContent = String(e.message || e);
      submit.disabled = false;
    }
  };
}
```

- [ ] **Step 2: Render a per-repo `＋ launch agent` button in `renderFleet()`**

In `renderFleet()`, the current repo loop builds `box.innerHTML` with name + health, then appends agents. Replace the body of the `for (const repo of fleet.repos)` loop with a DOM-built version that adds the launch button under healthy repos:

```js
  for (const repo of fleet.repos) {
    const box = document.createElement("div");
    box.className = "repo";
    const healthy = repo.health.state === "healthy";
    const healthTxt = healthy ? "healthy" : `unreachable: ${repo.health.reason || ""}`;

    const name = document.createElement("div");
    name.className = "name";
    name.textContent = repo.name;
    box.appendChild(name);

    const health = document.createElement("div");
    health.className = `health ${healthy ? "healthy" : "unreachable"}`;
    health.textContent = healthTxt;
    box.appendChild(health);

    if (healthy) {
      const launch = document.createElement("button");
      launch.className = "ctl-btn launch";
      launch.textContent = "＋ launch agent";
      launch.onclick = () => openLaunchModal(repo.name);
      box.appendChild(launch);
    }

    if (!repo.agents.length) {
      const none = document.createElement("div");
      none.className = "muted";
      none.textContent = "  (no agents)";
      box.appendChild(none);
    }
    for (const agent of repo.agents) {
      box.appendChild(renderAgent(agent));
    }
    fleetListEl.appendChild(box);
  }
```

- [ ] **Step 3: Build, rebuild-and-restart, verify**

```bash
cargo build --bin prosperod
```
Expected: clean. Then rebuild-and-restart.

```bash
curl -s http://127.0.0.1:7878/app.js | grep -c "function openLaunchModal"
```
Expected: `1`

Browser: under the healthy `prospero` repo, click `＋ launch agent`. Enter a task (e.g. `list the files in the repo root`). Click `▸ advanced` — the label/model/tools fields reveal and the caret flips to `▾`. Click `spawn`. The modal closes and the new agent is auto-selected and streaming in the right pane. Cross-check the agent exists:

```bash
curl -s http://127.0.0.1:7878/api/fleet | grep -c '"agents":\[{'
```
Expected: `1` or more (the repo now has at least one agent).

> Note: a real spawn invokes caliban and makes live model calls — it needs whatever API credentials caliban expects. If credentials are unavailable, verify the request path instead by confirming the modal POSTs and surfaces the server's error inline (still proves the UI wiring).

- [ ] **Step 4: Commit**

```bash
git add crates/api/dashboard/app.js
git commit -m "feat(dashboard): launch-agent modal with advanced fields"
```

---

## Task 4: Status-aware agent row actions (kill / respawn / remove)

**Files:**
- Modify: `crates/api/dashboard/app.js`

- [ ] **Step 1: Add the row-action utilities to `app.js`**

Add after `openLaunchModal()`:

```js
// --- Row actions ------------------------------------------------------------

const ACTIVE_STATUSES = new Set(["spawning", "running", "idle"]);
function isActive(status) { return ACTIVE_STATUSES.has(status); }

// Build a small inline action button. `onClick` receives the button element so
// the handler can disable it while the request is in flight.
function actionBtn(text, cls, onClick) {
  const b = document.createElement("button");
  b.className = "act-btn" + (cls ? " " + cls : "");
  b.textContent = text;
  b.onclick = (e) => { e.stopPropagation(); onClick(b); };
  return b;
}

// Fire a mutating request. `confirmMsg` (when set) gates via window.confirm.
// Disables `btn` while in flight; surfaces failures in the banner; refreshes on success.
async function rowAction(method, path, confirmMsg, btn) {
  if (confirmMsg && !window.confirm(confirmMsg)) return;
  if (btn) btn.disabled = true;
  try {
    await api(method, path);
    refreshFleet();
  } catch (e) {
    showBanner(String(e.message || e));
    if (btn) btn.disabled = false;
  }
}
```

- [ ] **Step 2: Add action buttons to `renderAgent()`**

Replace the entire `renderAgent()` function with:

```js
function renderAgent(agent) {
  const row = document.createElement("div");
  row.className = "agent" + (agent.id === selectedAgent ? " selected" : "");

  const wt = agent.isolated ? `<span class="wt">⌥ worktree</span>` : `<span class="wt">shared</span>`;
  const info = document.createElement("span");
  info.innerHTML =
    `<span class="name">${agent.name}</span> ${wt}<br><span class="id">${agent.id}</span>`;

  const right = document.createElement("span");
  right.className = "agent-right";
  const badge = document.createElement("span");
  badge.className = `badge ${agent.status}`;
  badge.textContent = agent.status;
  right.appendChild(badge);

  const acts = document.createElement("div");
  acts.className = "acts";
  if (isActive(agent.status)) {
    acts.appendChild(actionBtn("kill", "danger", (b) =>
      rowAction("POST", `/api/agents/${agent.id}/kill`, `Kill agent ${agent.name}?`, b)));
  } else {
    acts.appendChild(actionBtn("respawn", "", (b) =>
      rowAction("POST", `/api/agents/${agent.id}/respawn`, null, b)));
    acts.appendChild(actionBtn("remove", "danger", (b) =>
      rowAction("DELETE", `/api/agents/${agent.id}`, `Remove agent ${agent.name}?`, b)));
  }
  right.appendChild(acts);

  row.appendChild(info);
  row.appendChild(right);
  row.onclick = () => selectAgent(agent.id);
  return row;
}
```

- [ ] **Step 3: Build, rebuild-and-restart, verify**

```bash
cargo build --bin prosperod
```
Expected: clean. Then rebuild-and-restart.

```bash
curl -s http://127.0.0.1:7878/app.js | grep -c "function rowAction"
```
Expected: `1`

Browser (with an agent present from Task 3):
1. While the agent is active (running/idle), the row shows a red `kill` button. Click it → confirm dialog appears → accept → status moves to a terminal state within ~3s.
2. Once terminal, the row shows `respawn` and a red `remove`. Click `respawn` (no confirm) → a fresh agent appears active.
3. On a terminal agent, click `remove` → confirm → the agent disappears from the fleet.

Cross-check a kill took effect (status no longer active):

```bash
curl -s http://127.0.0.1:7878/api/fleet
```
Expected: the targeted agent's `status` is one of `killed|done|failed|crashed`.

Clicking an action button must NOT also select/stream the agent (the row's click is suppressed via `stopPropagation`). Confirm the right pane does not switch when clicking a button.

- [ ] **Step 4: Commit**

```bash
git add crates/api/dashboard/app.js
git commit -m "feat(dashboard): status-aware agent row actions (kill/respawn/remove)"
```

---

## Task 5: Repo remove action

**Files:**
- Modify: `crates/api/dashboard/app.js`

- [ ] **Step 1: Add a `remove` button to the repo header in `renderFleet()`**

This reuses `actionBtn()` and `rowAction()` defined in Task 4. In `renderFleet()`, replace the block that creates and appends the repo `name` div:

```js
    const name = document.createElement("div");
    name.className = "name";
    name.textContent = repo.name;
    box.appendChild(name);
```

with a header row that holds the name and a `remove` button:

```js
    const head = document.createElement("div");
    head.className = "repo-head";
    const name = document.createElement("span");
    name.className = "name";
    name.textContent = repo.name;
    head.appendChild(name);
    head.appendChild(actionBtn("remove", "danger", (b) =>
      rowAction("DELETE", `/api/repos/${encodeURIComponent(repo.name)}`,
                `Remove repo ${repo.name}?`, b)));
    box.appendChild(head);
```

- [ ] **Step 2: Build, rebuild-and-restart, verify**

```bash
cargo build --bin prosperod
```
Expected: clean. Then rebuild-and-restart.

```bash
curl -s http://127.0.0.1:7878/app.js | grep -c 'repo-head'
```
Expected: `1` or more.

Browser: register a throwaway repo (`＋ add repo` → name `tmp-remove`, path `/Users/johnford2002/dev/caliban-ai/prospero`). It appears with a `remove` button beside its name. Click `remove` → confirm → the repo disappears within ~3s. Cross-check:

```bash
curl -s http://127.0.0.1:7878/api/repos | grep -c '"name":"tmp-remove"'
```
Expected: `0`

- [ ] **Step 3: Commit**

```bash
git add crates/api/dashboard/app.js
git commit -m "feat(dashboard): remove-repo row action"
```

---

## Task 6: Full end-to-end verification pass (spec §6)

No code changes — confirm the whole control surface works together against the live daemon,
exactly as the spec's acceptance checklist requires.

**Files:** none (verification only).

- [ ] **Step 1: Ensure a clean, running daemon**

Run the rebuild-and-restart procedure. Confirm `http://127.0.0.1:7878` loads.

- [ ] **Step 2: Walk the full lifecycle in the browser, cross-checking the API**

1. **Add a repo** via the modal → it appears in the sidebar.
   `curl -s http://127.0.0.1:7878/api/repos` lists it.
2. **Launch an agent** under a healthy repo → modal closes, the new agent is auto-selected and streaming.
3. **Kill** the running agent (accept the confirm) → status goes terminal.
4. **Respawn** it → a new agent id, active again.
5. **Remove** a terminal agent (accept the confirm) → it drops from the fleet.
6. **Remove the repo** (accept the confirm) → it unregisters.
7. **Error surfacing:** attempt a duplicate add-repo → inline error in the modal; trigger a
   row-action failure (e.g. remove an already-gone agent via a stale row) → red banner at the
   top of the fleet pane. No action fails silently.

- [ ] **Step 3: Final state check + finish the branch**

Confirm `git status` is clean and all six feature commits are present:

```bash
git log --oneline dashboard-control-plane -7
```
Expected: the spec commit plus Tasks 1–5 feature commits.

Then use **superpowers:finishing-a-development-branch** to decide how to integrate (merge / PR / cleanup).

---

## Self-review notes

- **Spec coverage:** add-repo (Task 2), remove-repo (Task 5), launch-agent w/ advanced fields & post-spawn stream (Task 3), kill/respawn/remove agent w/ status-awareness (Task 4), confirmation on the three destructive actions (Tasks 4–5 via `rowAction` confirm), error surfacing inline + banner (helpers in Task 1, used throughout), busy-state disabling and eager `refreshFleet()` (baked into every handler), manual E2E (Task 6). All spec sections map to a task.
- **No backend changes:** every endpoint used (`POST /api/repos`, `DELETE /api/repos/{name}`, `POST /api/repos/{name}/agents`, `POST /api/agents/{id}/kill`, `POST /api/agents/{id}/respawn`, `DELETE /api/agents/{id}`) already exists in `crates/api/src/lib.rs`.
- **Type/name consistency:** `api()`, `showBanner()`, `openModal()`/`closeModal()`, `actionBtn()`, `rowAction()`, `isActive()`, `openAddRepoModal()`, `openLaunchModal()` are each defined once and referenced consistently. `fleetListEl` is introduced in Task 1 and used by all later render edits. `SpawnBody` field names (`prompt`, `label`, `model`, `isolation`, `tool_allowlist`) and the `{ name, root }` add-repo body match the verified DTOs in `crates/api/src/dto.rs`.
