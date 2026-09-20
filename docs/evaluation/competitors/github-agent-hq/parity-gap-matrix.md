# Prospero ↔ GitHub Agent HQ parity gap matrix

> **What this is:** a living comparison between **Prospero** (this project — the
> agent-orchestration control plane over caliband) and **GitHub Agent HQ /
> mission control**, GitHub's hosted command center over the Copilot cloud agent
> and third-party Claude / Codex agents. Refresh it whenever a major feature
> lands or GitHub ships a new capability (or promotes a preview to GA).
>
> **Why here.** Agent HQ is not a coding engine — it launches, steers and tracks
> *fleets* of coding agents running in parallel across repositories, which is
> Prospero's category. It is the hosted, multi-vendor end of that category:
> sessions run on GitHub Actions and are started and delivered through GitHub
> issues, PRs and branches. Prospero is the self-hosted end: a daemon over
> caliban agents in worktrees or on Kubernetes. The comparison is about the
> control plane, not about Copilot/Claude/Codex as agents (those live in
> caliban's evaluation tree).
>
> **Companion document:** [`capability-inventory.md`](capability-inventory.md)
> — a dated snapshot of Agent HQ's documented surface; refresh both together.

**Legend:** ✅ Prospero has an equivalent · 🟡 partial · 🔴 gap · **n/a** =
Agent HQ-surface concept with no intended Prospero analogue (Prospero is a
self-hosted dev-fleet control plane, not a hosted forge feature). **n/a
(Ariel)** = in scope for the caliban-ai stack, but owned by
[Ariel](https://github.com/caliban-ai/ariel), the chat-bridge service that
sits in front of prosperod — not by Prospero. A ✅ means "Prospero does the
equivalent thing," not byte-identical. Rows are scored by the caliban
evaluation tree's rule: ✅ only when a production call path reaches the
capability.

**Last refreshed:** 2026-09-13 (initial capture. Agent HQ surface from
[`capability-inventory.md`](capability-inventory.md) snapshot 2026-09-13;
Prospero state verified against `crates/` — API routes in
`crates/api/src/lib.rs`, spawn fields in `crates/types/src/api.rs`, CLI in
`crates/cli/src/main.rs`). Auth row refreshed 2026-09-13 for #2 (token auth
shipped, `crates/api/src/auth/`, ADR-0010).

> **Caveat:** rows tagged **⚠** depend on an Agent HQ fact flagged public
> preview / uncertain in the inventory, or a Prospero detail not re-verified end
> to end on every fleet backend. Agent HQ rows marked *(preview)* are scored
> against the documented preview surface.

---

## A. Control-plane architecture

| Capability (Agent HQ) | Prospero | Notes |
|---|---|---|
| Single control plane for many concurrent agent sessions | ✅ | `prosperod` (ADR-0002); one daemon over every workspace's agents — Agent HQ is a SaaS control plane |
| Typed session/task state model | ✅ | normalized `FleetEvent` + agent status (ADR-0003/0004); Agent HQ exposes 8 task states (`queued` … `waiting_for_user` … `cancelled`) |
| Work output linked back to the session that produced it | 🔴 | Prospero records the agent's event history, but has no link from the agent's commits/branch to its session; Agent HQ links each commit to its session log |
| Deployment model | n/a | Agent HQ is hosted-only on GitHub Actions; Prospero is self-hosted (standalone, clustered on Postgres, or in-cluster) — a category difference, not a gap |

## B. Agent launch & fleet lifecycle

| Capability (Agent HQ) | Prospero | Notes |
|---|---|---|
| Start a session with a task prompt | ✅ | `POST /api/workspaces/{workspace}/agents` (`SpawnBody.prompt`); `prospero spawn` |
| Many sessions in parallel across repos | ✅ | multiple agents per workspace, many workspaces per daemon; `GET /api/fleet` |
| Steer a running session | 🟡 | `POST /api/agents/{id}/input` + `/end-input`, `prospero send` / `end-input`, implemented by both `LocalFleet` and `K8sFleet` (`send_input`). Only agents spawned `interactive: true` accept input — a default one-shot agent can't be steered mid-run ⚠ |
| Stop a session, keeping its work | ✅ | `POST /api/agents/{id}/kill` / `prospero kill`; the worktree survives the kill (ADR-0005) |
| Archive sessions | 🟡 | `DELETE /api/agents/{id}` / `prospero rm` removes an agent; no archive state that hides it but keeps it browsable |
| Re-run a session | ✅ | `POST /api/agents/{id}/respawn` / `prospero respawn` — Agent HQ has no documented equivalent (Prospero ahead) |
| Scheduled / event-triggered sessions | 🔴 | no scheduler or trigger surface in `crates/`; Agent HQ runs sessions on schedules and events |
| Session time limit | 🔴 | no per-agent wall-clock or turn cap on spawn (`SpawnBody` has no limit field). Agent HQ enforces a 59-minute hard cap (a constraint for them, a missing guardrail for Prospero) |

## C. Isolation

| Capability (Agent HQ) | Prospero | Notes |
|---|---|---|
| Isolated environment per session | ✅ | git worktree per agent by default (ADR-0005; `SpawnBody.isolation` = `worktree` / `shared`); Agent HQ uses an ephemeral Actions environment |
| Ephemeral compute per session | 🟡 | `K8sFleet` places each agent as a `CalibanTask` on Kubernetes (ADR-0008); `LocalFleet` agents share the host |
| Guardrails on what the agent can change | 🟡 | per-spawn `tool_allowlist` bounds the agent's tools; no push-side guardrail equivalent to branch protection / rulesets, because Prospero doesn't push |

## D. Observability

| Capability (Agent HQ) | Prospero | Notes |
|---|---|---|
| Real-time session log (reasoning + tool use) | ✅ | SSE `GET /api/agents/{id}/stream`; `prospero follow`; dashboard live view |
| Durable history of past sessions | ✅ | `GET /api/agents/{id}/events` replay from JSONL / SQLite / Postgres stores (ADR-0004) |
| Token usage / session length per session | 🟡 | `agent_finished` carries `cost_usd` + `turns` (printed by the CLI); `GET /api/usage` aggregates per workspace and the dashboard renders it. No per-session token/duration overview panel ⚠ |
| Search past sessions | 🔴 | no search endpoint or command; Agent HQ offers natural-language session search from CLI / VS Code |
| Web command center | ✅ | embedded Dioxus/WASM dashboard |

## E. Interfaces

| Capability (Agent HQ) | Prospero | Notes |
|---|---|---|
| Web UI | ✅ | dashboard served by `prospero-api` |
| CLI | ✅ | `prospero` (`workspace`, `spawn`, `ls`, `status`, `follow`, `kill`, `respawn`, `rm`, `send`, `end-input`) |
| REST API to create / list / read tasks | ✅ | spawn, `GET /api/fleet`, `GET /api/agents/{id}` — shipped and used by the CLI + dashboard; Agent HQ's task API is still *(preview)* |
| REST control of a running session (stream / steer / stop) | ✅ | stream, input and kill are all REST/SSE — Agent HQ documents none of these over REST (Prospero ahead) |
| IDE integrations (VS Code / JetBrains) | 🔴 | none |
| MCP exposure | 🔴 | Prospero exposes REST/SSE, not MCP. (Agent HQ uses MCP as a tool source *for the agent*; caliban owns that side) |
| Mobile app | n/a (Ariel) | Prospero has no mobile client; on-the-go notify / steer is Ariel's chat-bridge scope, not a Prospero app |

## F. Registry, discovery & multi-backend

| Capability (Agent HQ) | Prospero | Notes |
|---|---|---|
| Multiple agent products behind one control plane | 🔴 | caliban-only; Agent HQ drives Copilot + Claude + Codex *(preview)*. Prospero's wire-only coupling (ADR-0003) is the extension point; [ADR-0011](../../../adr/0011-acp-as-a-second-drive-protocol.md) makes ACP that second wire |
| Custom agents | 🟡 | per-spawn agent templates via `frontmatter_path` (#6); no installable agent-app registry |
| Admin policy over which agents may run | 🔴 | no policy surface; Agent HQ gates third-party agents by org/enterprise policy |
| Multi-repo fleet | ✅ | workspace registry (`/api/workspaces`, `prospero workspace add`) |

## G. Model / provider handling

| Capability (Agent HQ) | Prospero | Notes |
|---|---|---|
| Per-task model selection | ✅ | `SpawnBody.model` override; `provider_ref` picks a named workspace provider on k8s (#142) |
| Provider credentials managed by the control plane | 🟡 | per-workspace providers (`WorkspaceConfig.providers`, Secret references on k8s); on `LocalFleet` credentials come from the host environment |

## H. Extensibility

| Capability (Agent HQ) | Prospero | Notes |
|---|---|---|
| Lifecycle hooks | 🟡 | caliban hooks fire inside agents; Prospero has no control-plane hook surface |
| Skills / custom instructions (`agents.md`) | n/a (delegated) | agent-level concern owned by caliban |
| MCP servers for the agent | n/a (delegated) | caliban's MCP client, not the control plane |

## I. Auth / security / deployment / scale

| Capability (Agent HQ) | Prospero | Notes |
|---|---|---|
| Token auth on the control-plane API | ✅ | named bearer tokens with `read`/`operate`/`admin` scopes, checked by axum middleware on every inbound route (`crates/api/src/auth/`, ADR-0010, #2) — comparable granularity to Agent HQ's fine-grained PAT / GitHub App token with "Agent tasks" permission |
| Audit of agent activity | 🟡 | durable event log is replayable over the API; no dedicated audit surface. Agent HQ's own audit coverage is ⚠ unverified |
| Usage-based billing / quotas | n/a | self-hosted; cost is reported (`/api/usage`), not billed |
| Scale-out / HA | ✅ | clustered mode — N `prosperod` replicas on Postgres with leased ownership (`crates/core/src/leased_ownership.rs`); Agent HQ scales as SaaS |
| Durable-log retention | 🟡 | age-based `--retention-days` prune shipped (#43); JSONL rotation open (#4) |

## J. Out of scope for Prospero (Agent HQ-distinctive)

All **n/a** — these are consequences of Agent HQ being a feature *of the GitHub
forge*. A self-hosted control plane shouldn't chase them. Listed only to mark
the boundary.

| Capability (Agent HQ) | Prospero | Notes |
|---|---|---|
| Start a session by assigning a GitHub issue / @-mentioning on a PR | n/a | forge-native intake. A forge-agnostic trigger (webhook → spawn) would be the Prospero-shaped version, tracked under B's trigger row |
| PR as the session's output, gated by branch protection / rulesets | n/a | Prospero leaves the agent's worktree for the operator (or the agent's own `git`/`gh` tools) to deliver; opening PRs is agent/workflow behavior, not the control plane's |
| Slack / Teams notifications and conversation | n/a (Ariel) | [Ariel](https://github.com/caliban-ai/ariel) owns chat |
| GitHub Mobile / Copilot app clients | n/a (Ariel) | first-party clients of a SaaS; mobile reach for the fleet comes through Ariel |
| Actions-minutes + AI-credit billing | n/a | hosted pricing model |
| Agent apps installed as GitHub Apps | n/a | forge marketplace mechanism |

---

## Read: hosted breadth vs self-hosted depth

- **Agent HQ wins on reach.** Several agent vendors, GitHub-native intake
  (issues, PRs, schedules) and output (PRs linked to session logs), clients
  everywhere (web, IDEs, CLI, app, mobile), plus session search and an admin
  policy layer.
- **Prospero wins on control.** It is self-hosted (standalone, clustered HA on
  Postgres, or on Kubernetes via `K8sFleet`) with a durable event store under
  your control. Its REST/SSE API can already stream, steer, stop and respawn
  sessions, which Agent HQ's preview API can't. And it has no session time
  ceiling.
- **The auth gap has closed.** Agent HQ's public API is the thinner one;
  Prospero's is richer and, since #2, token-authenticated too
  (`crates/api/src/auth/`, ADR-0010).

## Prospero-distinctive gaps worth a ticket

Capabilities Agent HQ has that Prospero lacks and that are *in scope* for a
self-hosted control plane (the forge-native and chat surface is deliberately
excluded):

1. **Multiple agent backends** (F) — drive non-caliban agents (Claude Code,
   Codex) behind the same fleet model. This is the headline capability of Agent
   HQ, and ADR-0003's wire-only coupling makes it an adapter problem.
2. **Scheduled / event-triggered spawns** (B) — a cron + inbound-webhook trigger
   surface, the forge-agnostic answer to "assign an issue to an agent".
3. **Session guardrails: time/turn limits** (B) — a per-spawn wall-clock or turn
   cap, so a runaway agent is stopped by policy rather than an operator.
4. **Session search + commit ↔ session linkage** (A, D) — find a past session
   and trace a commit back to the run that wrote it, over the durable store
   Prospero already has.

---

## Refresh process

1. When a Prospero feature lands: tick the relevant row(s) in the same PR
   (🔴 → 🟡 → ✅), citing the evidence in Notes.
2. When GitHub ships something new, or a *(preview)* goes GA: refresh
   [`capability-inventory.md`](capability-inventory.md) first, then propagate
   here.
3. Resolve **⚠** rows against GitHub's live docs / Prospero code when you touch
   them.
4. Keep Section J as a boundary marker — don't let forge-native or chat rows
   creep into the Prospero backlog (chat goes to Ariel).
5. Bump the **Last refreshed** date at the top.
