# Prospero ↔ Vibe Kanban parity gap matrix

> **What this is:** a living comparison between **Prospero** (this project — the
> agent-orchestration control plane over caliband) and **Vibe Kanban**, an
> open-source local orchestrator that runs many coding-agent CLIs in parallel
> git worktrees behind a kanban board. Refresh it whenever a major feature lands
> or Vibe Kanban ships a new capability.
>
> **Why here.** Vibe Kanban is not a coding agent: it launches, supervises, and
> reviews the output of agents that are (Claude Code, Codex, Gemini CLI, …),
> each in an isolated worktree. That is Prospero's category — a control plane
> over coding-agent workers — scoped to one host. It is the most-adopted
> open-source analogue of a single-host Prospero, so it is the natural
> comparison for Prospero's *local* mode. **It is sunsetting** (bloop shut down
> 2026-04-10; community-maintained since): if it goes dormant, re-point this
> slot at Claude Squad.
>
> **Companion document:** [`capability-inventory.md`](capability-inventory.md)
> — a dated snapshot of Vibe Kanban's documented surface; refresh both together.

**Legend:** ✅ Prospero has an equivalent · 🟡 partial · 🔴 gap · **n/a** =
Vibe Kanban-surface concept with no intended Prospero analogue. **n/a (Ariel)**
= in scope for the caliban-ai stack, but owned by
[Ariel](https://github.com/caliban-ai/ariel), the chat-bridge service in front
of prosperod — not by Prospero. A ✅ means "Prospero does the equivalent thing,"
not byte-identical. Rows are scored by the caliban evaluation tree's rule: ✅
only when a production call path reaches the capability.

**Last refreshed:** 2026-09-13 (initial capture. Vibe Kanban surface from
[`capability-inventory.md`](capability-inventory.md) snapshot 2026-09-13;
Prospero state verified against the code on the same date). Auth-adjacent rows
refreshed 2026-09-13 for #2 — token auth shipped, `crates/api/src/auth/`,
ADR-0010.

> **Caveat:** rows tagged **⚠** depend on a Vibe Kanban fact still flagged
> uncertain in the inventory, or a Prospero detail not re-verified against the
> code.

---

## A. Control-plane architecture

| Capability (Vibe Kanban) | Prospero | Notes |
|---|---|---|
| Local backend server serving a web UI | ✅ | `prosperod` serves the REST/SSE API and the embedded Dioxus/WASM dashboard from one process (`crates/api/src/lib.rs`, ADR-0002) |
| Repo → workspace → agent-session model | ✅ | workspaces (`/api/workspaces`) own agents (`/api/workspaces/{workspace}/agents`); Vibe Kanban adds an issue layer on top (see J) |

## B. Agent launch & fleet lifecycle

| Capability (Vibe Kanban) | Prospero | Notes |
|---|---|---|
| Launch a coding-agent session per task | ✅ | `prospero spawn`, `POST /api/workspaces/{workspace}/agents`, and the dashboard's spawn action (`crates/dashboard/src/api.rs`) |
| Follow-up messages to a running session | 🟡 | `prospero send` / `POST /api/agents/{id}/input` / dashboard `send_input`, plus `end-input` — only for agents spawned `interactive: true` (scored 🟡 consistently across the matrices) |
| Stop / restart a session | ✅ | `kill`, `respawn`, `rm` across CLI, API, and dashboard. ⚠ Vibe Kanban's own kill/restart semantics are unverified |
| Auto-queue follow-ups while the agent is busy; cancel queued messages | 🔴 | no input queue in `crates/api/src/handlers.rs` or `crates/core/src/fleet.rs`; input goes straight to caliband ⚠ |
| Per-repo setup / cleanup / dev-server scripts | 🔴 | no control-plane script surface. caliban hooks fire *inside* the agent (`inherit_hooks` on the spawn spec, `crates/core/src/caliband/wire.rs`), which is not the same thing |
| Orphaned / expired worktree cleanup | 🔴 | retention prunes *events* (`Store::prune`), not worktrees; worktree lifecycle is left to caliban ⚠ |

## C. Isolation

| Capability (Vibe Kanban) | Prospero | Notes |
|---|---|---|
| Git-worktree isolation per session | ✅ | worktree isolation **by default** (`isolation_worktree: true`, `crates/core/src/fleet.rs`; ADR-0005); `--shared-tree` to opt out |

## D. Observability

| Capability (Vibe Kanban) | Prospero | Notes |
|---|---|---|
| Execution logs in the web UI | ✅ | live SSE stream (`/api/agents/{id}/stream`) with history replay (`/api/agents/{id}/events`), rendered by the dashboard and `prospero follow` |
| Inline diff review with comments | 🔴 | the dashboard renders events, not the agent's worktree diff; no review/comment surface |

## E. Interfaces

| Capability (Vibe Kanban) | Prospero | Notes |
|---|---|---|
| Web UI | ✅ | embedded dashboard, same-origin only (`crates/api/src/dashboard.rs` CSP) |
| Backend HTTP API | ✅ ⚠ | Prospero's REST + SSE API is the documented contract for CLI, dashboard, and Ariel; Vibe Kanban's REST API is undocumented (⚠ in inventory) |
| MCP server over the fleet (issues / workspaces / sessions) | 🔴 | Prospero exposes REST/SSE, not MCP (no MCP code in `crates/`) |
| PR creation with AI-generated descriptions | 🔴 | no PR surface in `crates/`; opening PRs is left to the agent |

## F. Registry, discovery & multi-backend

| Capability (Vibe Kanban) | Prospero | Notes |
|---|---|---|
| Register repos | ✅ | `prospero workspace add <name> <root>` / `POST /api/workspaces` / dashboard `add_workspace` |
| Heterogeneous agent CLIs (10+: Claude Code, Codex, Gemini CLI, Copilot, …) | 🟡 | Prospero drives **caliban** agents only (ADR-0003 wire coupling), across any model caliban supports. Non-caliban backends 🔴 — [ADR-0011](../../../adr/0011-acp-as-a-second-drive-protocol.md) adds the ACP driver seam but keeps their lifecycle in caliband |
| Remote access (reverse proxy / SSH / tunnel) | 🟡 | `PROSPERO_ADDR` binds any address and the container binds `0.0.0.0` (`docs/container.md`), but Prospero ships no reverse-proxy, SSH, or tunnel mechanism of its own — that's left to the operator's fronting infrastructure. Since #2, an exposed bind at least requires a token (`crates/api/src/auth/`, ADR-0010) |

## G. Model / provider handling

| Capability (Vibe Kanban) | Prospero | Notes |
|---|---|---|
| Provider choice delegated to the agent | n/a (delegated) | same shape: Prospero delegates model/provider to caliban |
| Model selector in the UI | 🟡 ⚠ | per-workspace provider config is settable from the dashboard (`set_workspace_config`) and restarts that workspace's caliband; per-spawn model choice from the UI not verified. Vibe Kanban's selector scope is itself ⚠ |

## H. Extensibility

| Capability (Vibe Kanban) | Prospero | Notes |
|---|---|---|
| Centralized agent configuration | 🟡 | per-workspace provider config (`PUT /api/workspaces/{name}/config`); no multi-product agent profiles, since there is one agent product |
| MCP client configuration for launched agents | n/a (delegated) | MCP servers are configured in caliban, not the control plane |

## I. Auth / security / deployment / scale

| Capability (Vibe Kanban) | Prospero | Notes |
|---|---|---|
| Configurable origins allowlist | 🟡 | the dashboard page ships a strict same-origin CSP (`crates/api/src/dashboard.rs`) and inbound requests now require auth (`crates/api/src/auth/`, ADR-0010, #2), but there is still no configurable origin allowlist |
| Analytics toggle | n/a | Prospero ships no product analytics to toggle ⚠ |

## J. Out of scope for Prospero (Vibe Kanban-distinctive)

All **n/a** — listed only to mark the boundary.

| Capability (Vibe Kanban) | Prospero | Notes |
|---|---|---|
| Kanban issue board (tags, relationships) as the intake surface | n/a | Prospero takes tasks from its API/CLI/dashboard; issue tracking stays in GitHub (and chat intake is Ariel's) |
| Built-in browser / devtools preview of the app under development | n/a | an IDE/agent-level tool, not a control-plane concern |

---

## Read: same idea, different depth

- **Vibe Kanban is wider at the edges.** It drives 10+ agent products, closes
  the loop from task to reviewed diff to PR inside one UI, and lets other agents
  drive the fleet over MCP. That is a strong *single-developer* workflow.
- **Prospero is deeper in the middle.** It trades breadth of agents for a
  documented control-plane contract (REST + SSE), durable history on JSONL /
  SQLite / Postgres, clustered HA, and a Kubernetes fleet backend (ADR-0008) —
  none of which Vibe Kanban documents. Prospero runs a team's fleet;
  Vibe Kanban runs one developer's laptop.
- **The longevity gap favours Prospero.** Vibe Kanban has no vendor since
  2026-04-10 and no release since v0.1.44. Its best ideas are worth borrowing,
  not racing.

## Prospero-distinctive gaps worth a ticket

Capabilities Vibe Kanban has that Prospero lacks and that are *in scope* for a
control plane:

1. **MCP server over the fleet** (E) — the same gap the OpenClaw matrix flags;
   two competitors now make it a pattern, not a one-off.
2. **Review loop: worktree diff view + PR creation** (D, E) — surface what an
   agent changed and hand it off as a PR, without leaving the dashboard.
3. **Heterogeneous worker backends** (F) — also flagged by OpenClaw; ADR-0003's
   wire-only coupling is the natural seam.
4. **Worktree lifecycle hygiene** (B) — setup/cleanup scripts and orphaned
   worktree cleanup at the control-plane level.

---

## Refresh process

1. When a Prospero feature lands: tick the relevant row(s) in the same PR
   (🔴 → 🟡 → ✅), citing the evidence in Notes.
2. When Vibe Kanban ships something new — or goes dormant — refresh
   [`capability-inventory.md`](capability-inventory.md) first, then propagate here.
   If dormant, replace this pair with a Claude Squad comparison.
3. Resolve **⚠** rows against Vibe Kanban's repo / Prospero code when you touch them.
4. Keep Section J as a boundary marker — don't let issue-board or preview rows creep into the Prospero backlog.
5. Bump the **Last refreshed** date at the top.
