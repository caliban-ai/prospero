# Prospero dashboard — control-plane controls

**Date:** 2026-06-07
**Status:** Approved design, ready for implementation plan
**Scope:** Make the web dashboard a full control surface for the fleet — at parity
with the `prospero` CLI — so an operator never has to leave the dashboard to
register repos, launch agents, or manage running agents.

## 1. Problem

The dashboard today is observe-only. `crates/api/dashboard/app.js` makes a single
`fetch("/api/fleet")` call on a 3s poll and live-streams a selected agent over SSE.
It has no controls: you cannot register a repo, launch an agent, or kill/respawn/remove
one without dropping to the CLI.

The HTTP API already exposes every write action (all implemented and tested); only the
frontend is missing. This design adds the missing controls — **frontend only, no
backend changes**.

### CLI ↔ dashboard parity gap

| Action | CLI | API endpoint | In dashboard today |
|--------|-----|--------------|--------------------|
| Register repo | `repo add` | `POST /api/repos` | ❌ → add |
| Remove repo | `repo rm` | `DELETE /api/repos/{name}` | ❌ → add |
| List fleet | `ls` | `GET /api/fleet` | ✅ |
| Launch agent | `spawn` | `POST /api/repos/{name}/agents` | ❌ → add |
| Stream events | `follow` | `GET /api/agents/{id}/stream` (SSE) | ✅ |
| Kill agent | `kill` | `POST /api/agents/{id}/kill` | ❌ → add |
| Respawn agent | `respawn` | `POST /api/agents/{id}/respawn` | ❌ → add |
| Remove agent | `rm` | `DELETE /api/agents/{id}` | ❌ → add |

## 2. Constraints

- **Frontend only.** Changes are confined to `crates/api/dashboard/index.html`
  (styles + modal markup) and `crates/api/dashboard/app.js` (handlers). No Rust changes.
- **No build step.** Vanilla JS, no bundler, no dependencies — unchanged.
- **Match the existing aesthetic.** Dark monospace palette already defined in
  `index.html` (`#0f1115` bg, `#8ab4f8` accent, `#c792ea` repo names, `#6bd968` healthy,
  `#171a21` panels). New UI reuses these tokens.

## 3. Interaction model

**Modals for create actions; inline buttons for row actions.** (Chosen over an
all-inline disclosure model to keep the compact sidebar uncluttered while still
keeping per-row controls immediate.)

## 4. Components

### 4.1 Add-repo modal

- **Trigger:** `＋ add repo` control in the fleet-pane header.
- **Fields:** `name` (text, required), `root` (path text, required).
- **Submit:** `POST /api/repos` with `{ name, root }`.
- **Success:** close modal, call `refreshFleet()` immediately.
- **Error:** show the API `error` string inline in the modal; leave it open.

### 4.2 Launch-agent modal

- **Trigger:** per-repo `＋ launch agent` button. The clicked repo is pre-selected;
  the modal still shows a repo picker (defaults to that repo, editable) so a launch
  can be retargeted without closing.
- **Visible fields:**
  - `task` — textarea → `prompt` (required).
  - `☑ worktree isolation` — checkbox, default **on**. Checked → omit `isolation`
    (server default is worktree); unchecked → send `isolation: "shared"`.
- **Collapsed `▸ advanced` section:**
  - `label` → `label`.
  - `model` → `model`.
  - `tools` — comma-separated text → `tool_allowlist: string[]` (split, trim, drop empties).
- **Submit:** `POST /api/repos/{name}/agents` with the assembled `SpawnBody`. Omit any
  optional field left blank.
- **Success:** server returns `201 { agent_id, repo, isolated }`. Close the modal,
  call `refreshFleet()`, then `selectAgent(agent_id)` to auto-stream the new agent in
  the right pane.
- **Error:** show the API `error` string inline; leave the modal open.

### 4.3 Row actions (inline, status-aware)

Agent status values: `Spawning | Running | Idle | Killed | Done | Failed | Crashed`.
Treat the first three as **active**, the rest as **terminal**.

- **Repo row:** `remove` button → `DELETE /api/repos/{name}`.
- **Agent row:**
  - `kill` — shown when **active** → `POST /api/agents/{id}/kill`.
  - `respawn` — shown when **terminal** → `POST /api/agents/{id}/respawn`.
  - `remove` — shown when **terminal** → `DELETE /api/agents/{id}`.

After any row action succeeds, call `refreshFleet()` immediately.

## 5. Cross-cutting behavior

- **Confirmation.** `window.confirm()` gates the three destructive actions — **kill**,
  **remove agent**, **remove repo** — before the request fires. Spawn, respawn, and
  add-repo fire immediately (respawn and add-repo are non-destructive; spawn is creative).
- **Error surfacing.** The API returns `{ error: string, kind: string }` with a real HTTP
  status (e.g. `409 invalid_state`, `503 unreachable`, `404 not_found`). Create modals show
  `error` inline. Row-action failures show a transient, dismissable banner at the top of the
  fleet pane (the row has no room for inline text).
- **Busy state.** While a request is in flight, disable the originating button (and the
  modal's submit) to prevent double-submission; re-enable on completion or error.
- **Eager refresh.** Every mutation calls `refreshFleet()` on success rather than waiting
  for the 3s poll, so the UI reflects the change immediately.
- **No change to streaming.** The existing SSE `selectAgent`/`appendEvent` path is reused
  as-is; the `isolated` flag already drives the `⌥ worktree` / `shared` tag.

## 6. Testing

- **Backend:** unchanged, so the existing Rust suite
  (`cargo test --workspace --features prospero-core/testkit`) remains the coverage for the
  API. No new Rust tests.
- **Frontend (manual end-to-end against live `prosperod`):** with the daemon running
  (real `caliband` wired in), exercise the full surface through the UI and cross-check
  each step against `GET /api/fleet`:
  1. Add a repo via the modal → appears in the sidebar.
  2. Launch an agent → modal closes, new agent auto-selected and streaming.
  3. Kill it (confirm) → status goes terminal.
  4. Respawn it → new agent id, active again.
  5. Remove an agent (confirm) → drops from the fleet.
  6. Remove the repo (confirm) → unregistered.
  7. Trigger an error (e.g. add a repo with a bad path / duplicate name) → inline/banner
     error shown, no silent failure.

## 7. Non-goals

- No backend/API/protocol changes.
- No auth, multi-host, or persistence changes (out of scope per the framework spec).
- No build tooling, framework, or dependency additions — stays vanilla JS.
- No redesign of the existing fleet list / stream panes beyond adding the new controls.
