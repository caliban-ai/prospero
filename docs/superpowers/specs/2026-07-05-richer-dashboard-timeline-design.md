# Richer dashboard — agent timeline + tool-call inspector (interim) — Design

**Ticket:** caliban-ai/prospero#5 (`kind/feature`). Interim vanilla-JS increment; the
full flashy re-platform is the **Dashboard v2 (Rust/WASM, Dioxus) epic #95**.

## Scope (deliberately narrowed)

Because #95 will re-platform the whole dashboard in Dioxus, #5 is the
highest-value / least-throwaway increment we can ship against the working local
backend **now**, reusing the existing two-pane shell and controls:

**In scope**
1. **Structured agent timeline** — replace today's raw `#stream-log` (a flat
   append of lines) with a run view that groups the agent's event stream into
   **turns** and renders each event in context.
2. **Tool-call inspector** — render each tool call as an expandable row showing
   `ToolStarted.input` (pretty JSON) paired with its `ToolFinished` `ok`/fail;
   collapsed by default, click to expand the input.
3. **Cost/turns summary** — a single stat line in the agent header from the
   terminal `AgentFinished{cost_usd, turns, outcome}` (e.g. `success · 4 turns ·
   $0.12`); shows `— running` until the run finishes.

**Explicitly deferred to #95** (v2-shaped, would be throwaway in vanilla JS):
fleet overview, cost **charts**, a cost-aggregation endpoint, and the tabbed
navigation shell.

**Not available today (documented limitation):** tool *result bodies* are not in
the event stream — only `input` + a success bool. The inspector shows input +
outcome; full result payloads depend on a caliban-side change and are out of
scope.

## Data source (unchanged)

`GET /api/agents/{id}/events` (history replay) + the existing SSE
`GET /api/agents/{id}/stream` (live tail) — both already consumed by the current
stream view. Each item is a `FleetEvent { seq, ts, repo, agent_id, kind }` with
`kind` one of the `EventKind` variants. No new endpoints.

## Timeline model (client-side grouping)

A pure function `groupEvents(events) -> [Segment]` folds the ordered event list
into render segments. It needs no server change — turns are inferred from the
stream:

- **`AgentInit{model, tools, session_id}`** → a header segment (model + tool list).
- A **turn boundary** starts at each `ToolStarted` that follows non-tool output,
  or on the first event after init; assistant `Output{stream, chunk}` accumulates
  into the current turn's text (contiguous chunks coalesced).
- **`ToolStarted{name, input}`** opens a tool span; the next matching
  **`ToolFinished{name, ok}`** closes it (match by name, FIFO for repeats) →
  one inspectable tool row `{name, input, ok}`.
- **`StatusChanged{from, to}`** → a thin inline marker.
- **`AgentFinished{outcome, cost_usd, turns}`** → the terminal summary (also
  feeds the header stat).
- **`StorePersistFailed` / `RepoHealth` / `AgentGone`** → small muted system
  markers (kept visible, not hidden).

Unpaired `ToolStarted` (still running) renders as an open span with a spinner
dot; unknown/unhandled kinds render as a raw fallback line (never dropped).

Live SSE events append through the same `groupEvents` incremental path (or a
re-fold of the accumulated list — simplest correct first: keep the event array,
re-fold on each append; the arrays are small per agent).

## UI (reuse the existing shell + palette)

The right pane (`#stream`) becomes the timeline. Same dark/mono theme, same
`#stream-head` (name, meta, live/closed conn dot) — extended with the cost/turns
stat. Turns are visually delimited; tool rows use the existing `.tool` accent
(`#c792ea`), ok/fail a green/red pill reusing the badge styles. No new
dependencies, no build step — edit `crates/api/dashboard/{index.html,app.js}`;
still served via `include_str!`.

## Testing (honest, given the no-JS-toolchain decision)

There is no JS test runner in prospero (and #95, not #5, is where a real frontend
test story would land). So:

1. **Server-side contract test (CI-enforced, Rust):** extend
   `crates/api/tests/api_integration.rs` to drive the testkit fake through a run
   that emits `ToolStarted{input}` → `ToolFinished{ok}` → `AgentFinished{cost_usd,
   turns}`, then assert `GET /api/agents/{id}/events` returns those `EventKind`
   shapes (the exact JSON fields the timeline consumes). This guards the
   data contract the JS depends on, in CI.
2. **Dashboard still serves (existing Rust tests):** `serves_dashboard_index` /
   `dashboard_app_js_has_javascript_content_type` continue to pass.
3. **Visual verification (manual, pre-PR):** run prosperod locally against a fake
   caliband, load the dashboard in a browser, drive an agent, confirm the
   timeline groups turns, tool rows expand to show input + ok/fail, and the
   header shows cost/turns on finish. Capture a screenshot for the PR.

The pure `groupEvents` fold is written to be obviously correct and total (every
event maps to a segment or a raw fallback); its behavior is exercised end-to-end
by the visual check and guarded at the data-contract layer by test (1).

## Acceptance

- The agent view shows a **turn-grouped timeline** instead of a raw log.
- Tool calls are **expandable**, showing input JSON + ok/fail.
- The header shows **cost + turns + outcome** once the run finishes.
- No new endpoints, no build step, dashboard still served embedded.
- Overview/charts/nav shell remain out of scope (tracked in #95).
