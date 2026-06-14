# Dashboard layout & polish

**Date:** 2026-06-13
**Status:** Approved design, ready for implementation plan
**Scope:** A frontend-only polish pass on the Prospero web dashboard — refine layout, density, sticky headers, empty/loading states, responsiveness, and the stream pane's framing. No new features, no new data, no backend changes.

## 1. Goal & non-goals

Make the existing dashboard *feel* better without changing what it does. The dashboard works (fleet sidebar + live SSE stream, control-plane actions, provider config, interactive input) but is visually bare: the stream pane is a raw header-less log, agent rows are cluttered, nothing is sticky, there's no real empty state, and the fixed two-column grid doesn't adapt to narrow widths.

**Non-goals (explicitly deferred):**
- No richer per-agent observability (tool-call cards, timelines) — that's board #5's "per-agent observability" / "cost charts" directions, deferred.
- No new backend data. The fleet snapshot carries only `id, name, repo, status, started_at, isolated, interactive` — anything shown must be derivable from that or from events we already stream.
- No build step / framework / dependencies — stays vanilla JS + CSS embedded via `include_str!`.

## 2. Constraints

- Files: `crates/api/dashboard/index.html` (CSS + minor structure) and `crates/api/dashboard/app.js` only. Assets are embedded at compile time, so changes need a `prosperod` rebuild + restart to appear.
- All existing behavior — modals (add-repo, launch, settings), row actions (kill/respawn/remove), interactive input box, SSE streaming, the 3s poll with input-preservation — must remain intact.
- Match the existing dark-monospace palette (`#0f1115` bg, `#8ab4f8` accent, `#c792ea` repo names, `#6bd968` healthy/running, `#171a21` panels, etc.).

## 3. The polish — six areas

### 3.1 Sticky pane headers
- The fleet pane's header becomes a sticky bar (`position: sticky; top: 0`) reading `fleet · N repos · N agents` (counts computed client-side from the snapshot) with the `＋ add repo` button pinned right. The banner stays directly under it.
- The stream pane gets its own sticky header (§3.5).

### 3.2 Sticky repo headers
- Within the scrolling fleet list, each repo's header row (name + `⚙` settings + `remove`) is `position: sticky; top: <fleet-header-height>` so the repo you're scrolling through stays labeled when it has many agents. Health line and the `＋ launch agent` button scroll normally beneath it.

### 3.3 Cleaner agent rows
- Two-line layout: line 1 = agent name + worktree/shared tag; line 2 = id + a **derivable meta** string:
  - active (`spawning`/`running`): `· <elapsed>` since `started_at` (e.g. `· 2m`)
  - `idle`: `· idle <elapsed>`
  - terminal (`done`/`failed`/`killed`/`crashed`): `· <status> <elapsed> ago`
  - Elapsed is computed client-side from `started_at` (RFC-3339) at render time; it advances naturally on the existing 3s poll re-render. No cost/turns (not in the snapshot).
- Right side: status badge over the action group (§4).

### 3.4 Consistent spacing & type scale
- Introduce a small set of CSS custom properties for spacing (e.g. `--sp-1: 4px; --sp-2: 6px; --sp-3: 10px`) and reuse them for row padding/margins, header padding, and gaps, replacing the current ad-hoc values. Normalize badge font-size/padding and the id/meta muted type so the hierarchy is consistent.

### 3.5 Stream-pane header bar
- A sticky bar at the top of the stream pane that, when an agent is selected, shows: the agent name (accent), its current status badge, its model (captured from the `agent_init` event), and a live-connection indicator — `● live` (green) while the SSE stream is open, `⚠ closed` (red) on `onerror`/terminal close. Once an `agent_finished` event arrives, append `· $<cost> · <turns> turns` to the bar.
- This replaces the easily-missed "▶ streaming X" text currently in the page header (that page-header text is removed).
- When no agent is selected, the bar is hidden and the pane shows the empty state (§3.6).

### 3.6 Empty/loading states + responsive
- **Empty state (stream):** a centered, muted block — a short "Select an agent to stream its output" with a one-line hint — instead of the current single muted line.
- **Loading state (fleet):** the existing "loading fleet…" muted text is kept but styled consistently; the "no repos registered" state gets a centered treatment with a hint to use `＋ add repo`.
- **Responsive:** a media query at `max-width: 720px` collapses the two-column grid to a single column. Default view shows the fleet; selecting an agent reveals the stream pane with a `← fleet` back affordance in the stream header that returns to the fleet list. Above 720px, both panes show side by side as today. Implemented by toggling a body/root class (e.g. `.show-stream`) set in `selectAgent` and cleared by the back affordance; the media query drives whether that class changes which pane is visible.

## 4. Interaction & accessibility

- **Hover-reveal row actions:** the per-agent action buttons (and the repo-row `remove`/`⚙`) are visually de-emphasized by default and fully shown when the row is hovered, keyboard-focused, or selected — `.agent:hover .acts`, `.agent:focus-within .acts`, `.agent.selected .acts` (and the repo-head equivalent). This declutters the sidebar without stranding keyboard or touch users (focus-within covers keyboard tabbing; selection covers the active agent; on touch, a tap selects the row which reveals its actions).
- The interactive input box (idle interactive agents) remains always-visible (it's the primary action for those rows), unaffected by the hover-reveal.

## 5. Implementation notes

- **index.html:** the bulk of the change — spacing tokens, sticky rules, hover-reveal rules, the responsive media query, empty-state styles, the stream-header-bar styles, and a small structural addition for the stream-pane header element. Remove the page-header "streaming X" span.
- **app.js (targeted edits):**
  - `renderFleet`: compute + render the `N repos · N agents` count in the sticky pane header; mark repo headers sticky (class only — CSS does the work); preserve the existing input-preservation and structure.
  - `renderAgent`: add the derivable meta line; move actions into the hover-reveal group (class change; logic unchanged).
  - `selectAgent` / `appendEvent`: populate the stream-pane header bar (name, status, model from `agent_init`, live dot from connection state, cost/turns from `agent_finished`); render the empty state when nothing is selected; set/clear the responsive `.show-stream` class + wire the `← fleet` back affordance.
  - A small `elapsed(started_at)` helper for the meta strings and stream timing.
- No change to the API/core; backend tests unaffected.

## 6. Testing

No JS test runner (vanilla, no build). Verify with:
- `cargo build --bin prosperod` clean; `node --check crates/api/dashboard/app.js` clean.
- A browser pass after rebuild+restart: sticky pane/repo/stream headers while scrolling; hover **and** keyboard-focus reveal of row actions; the stream header bar (name/status/model/live-dot, cost/turns after finish); the empty state with no selection; responsive collapse to one column under 720px with the `← fleet` toggle; existing flows (launch, kill/respawn/remove, settings, interactive input) still work.
- `curl` greps of the served asset confirm the new structure shipped.

## 7. Out of scope

Per-agent observability (tool-call inspection, timelines), fleet-level cost charts/metrics, any backend/data change, and any new dependency or build tooling.
