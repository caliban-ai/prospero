# Prospero ↔ OpenHands Agent Canvas parity gap matrix

> **What this is:** a living comparison between **Prospero** (this project — the
> agent-orchestration control plane over caliband) and the **OpenHands Agent
> Canvas + Agent Server** control layer. Refresh it whenever a major feature
> lands or OpenHands ships a new capability.
>
> **Why here (and not in caliban).** The OpenHands *agent* is a coding engine
> and belongs to caliban's comparison. Agent Canvas is not: it is a self-hosted
> control surface that dispatches conversations to Agent Server backends, which
> run the native OpenHands agent or other products over ACP (Claude Code,
> Codex, Gemini CLI). Canvas → Agent Server → worker is the same shape as
> Prospero's CLI/dashboard → `prosperod` → caliban agent, which makes it the
> closest open-source, self-hostable analogue in Prospero's category.
>
> **Companion document:** [`capability-inventory.md`](capability-inventory.md)
> — a dated snapshot of the Canvas / Agent Server documented surface; refresh
> both together.

**Legend:** ✅ Prospero has an equivalent · 🟡 partial · 🔴 gap · **n/a** =
competitor-surface concept with no intended Prospero analogue (Prospero is a
dev-fleet control plane; agent-level concerns are delegated to caliban). **n/a
(Ariel)** = in scope for the caliban-ai stack, but owned by
[Ariel](https://github.com/caliban-ai/ariel), the chat-bridge service that sits
in front of prosperod — not by Prospero. A ✅ means "Prospero does the
equivalent thing," not byte-identical. Rows are scored by the caliban
evaluation tree's rule: ✅ only when a production call path reaches the
capability.

**Last refreshed:** 2026-09-13 (initial capture. OpenHands surface from
[`capability-inventory.md`](capability-inventory.md) snapshot 2026-09-13,
v1.18.0; Prospero state verified against the code on the same date).

> **Caveat:** rows tagged **⚠** depend on an OpenHands fact still flagged
> uncertain in the inventory or a Prospero detail not re-verified against the
> code.

---

## A. Control-plane architecture

| Capability (OpenHands) | Prospero | Notes |
|---|---|---|
| Control surface separate from the agent runtime | ✅ | CLI + dashboard are clients of `prosperod`, which drives caliband and never runs the agent loop itself (ADR-0002) |
| One server hosts many concurrent sessions | ✅ | many agents per workspace under one `prosperod` (`/api/workspaces/{workspace}/agents`, `crates/api/src/lib.rs`) |
| Credentials held server-side, not in the client | ✅ | provider config lives on the server (`PUT /api/workspaces/{name}/config`; `WorkspaceConfig` in `crates/types/src/model.rs`); the read side omits Secret references |

## B. Agent launch & fleet lifecycle

| Capability (OpenHands) | Prospero | Notes |
|---|---|---|
| Start a conversation / agent | ✅ | `prospero spawn`; `POST /api/workspaces/{workspace}/agents` |
| Send follow-up messages into a running session | 🟡 | `prospero send` / `end-input` (`crates/cli/src/main.rs`); `POST /api/agents/{id}/input` and `/end-input`. Only agents spawned `interactive: true` accept input |
| Stop / re-run a session | ✅ | `prospero kill` / `respawn`; `POST /api/agents/{id}/kill`, `/respawn`. ⚠ OpenHands' own kill/respawn semantics are unverified |
| Fork / branch a conversation | 🔴 | no fork route or command (`crates/api/src/lib.rs`, `crates/cli/src/main.rs`) |
| Scheduled runs (cron) | 🔴 | no scheduler anywhere in `crates/`; OpenHands has an Automation Server with run states, "Run now", enable/disable |
| Webhook / event triggers | 🔴 | no inbound webhook route; spawns are operator-initiated only |
| File and command operations through the server API | 🔴 | the API exposes agent lifecycle and input, not workspace file / exec operations |

## C. Isolation

| Capability (OpenHands) | Prospero | Notes |
|---|---|---|
| Host-process runtime (no isolation) | ✅ | `--shared-tree` opts out of the default worktree (ADR-0005) |
| Container / pod runtime with mounts and network policy | 🟡 | `K8sFleet` places each agent on Kubernetes (ADR-0008); no per-agent container runtime on the local fleet. ⚠ per-agent streaming on k8s has had NetworkPolicy gaps |
| VM / Modal backends | 🔴 | local processes or Kubernetes only |
| Managed hosted sandbox (OpenHands Cloud) | n/a | Prospero is self-hosted software, not a hosted service |

## D. Observability

| Capability (OpenHands) | Prospero | Notes |
|---|---|---|
| Live event stream | ✅ | SSE `GET /api/agents/{id}/stream` (`crates/api/src/sse.rs`); OpenHands uses WebSocket |
| Durable history that survives the session | ✅ | `Store` trait (ADR-0004) with JSONL, SQLite, and Postgres backends; replay via `GET /api/agents/{id}/events` |
| Web dashboard | ✅ | embedded Dioxus/WASM dashboard served by `prospero-api` |
| Tag and filter conversations | 🔴 | no filter or search in `crates/dashboard/src` or the API |
| LLM cost / token usage view | 🟡 | `GET /api/usage` (cost, turns, outcomes per workspace) rendered by the dashboard (`crates/dashboard/src/api.rs`); no CLI `usage` command |
| Export activity logs (CSV / JSON) | 🟡 | per-agent event history is JSON over `GET /api/agents/{id}/events`; no export action or CSV |

## E. Interfaces

| Capability (OpenHands) | Prospero | Notes |
|---|---|---|
| REST API | ✅ | `prospero-api` (axum) under `/api/*` |
| Published OpenAPI spec / docs UI | 🔴 | no OpenAPI generation in `crates/` |
| Health / readiness endpoints | ✅ | `/healthz`, `/readyz` (`crates/api/src/lib.rs`; `docs/container.md`) |
| Server info endpoint | 🟡 | `GET /api/capabilities` (backend-aware feature flags) and `GET /api/metrics`; no version / build-info endpoint |
| Server-push events | ✅ | SSE (OpenHands uses WebSocket) |
| Operator CLI | ✅ | `prospero` (thin HTTP client over `prosperod`) |
| MCP settings for agent tools | n/a | tool-side MCP is an agent concern, delegated to caliban |

## F. Registry, discovery & multi-backend

| Capability (OpenHands) | Prospero | Notes |
|---|---|---|
| Drive several agent products (native + ACP: Claude Code, Codex, Gemini CLI) | 🔴 | caliban agents only, over caliban's NDJSON wire format (ADR-0003); no ACP or other-agent adapter |
| One UI across multiple execution servers | 🔴 | no transport to remote prosperods (#1); clustered mode is HA for one control plane, not aggregation |

## G. Model / provider handling

| Capability (OpenHands) | Prospero | Notes |
|---|---|---|
| Per-backend provider / LLM profiles | ✅ | per-workspace provider config, with named providers and a default on k8s (`WorkspaceConfig.providers`, `default_provider` in `crates/types/src/model.rs`) |
| Local / OpenAI-compatible providers, subscription logins | n/a (delegated) | model access is caliban's (OpenAI adapter + `base_url` for local runners) |

## H. Extensibility

| Capability (OpenHands) | Prospero | Notes |
|---|---|---|
| Plugins at the control-plane level | 🔴 | no plugin surface in Prospero |
| Skills / `AGENTS.md` | n/a | agent-level instructions, owned by caliban |
| GitHub / Linear / Notion integrations | 🔴 | no issue-tracker or docs integration; spawns take a free-text task |

## I. Auth / security / deployment / scale

| Capability (OpenHands) | Prospero | Notes |
|---|---|---|
| API-key auth on the execution server, with rotation | 🔴 | no authn/authz on the REST/SSE API (`crates/api/src`; #2). Prospero → caliband traffic carries TLS + a bearer token (`crates/core/src/caliband/transport.rs`), but inbound clients are unguarded |
| Encrypted secrets at rest | 🟡 | k8s providers reference Kubernetes Secrets rather than holding keys (`crates/types/src/model.rs`). ⚠ local-fleet provider env storage is not encrypted by Prospero |
| SSO / RBAC / audit logs (Enterprise) | 🔴 | none; identity and audit records are planned in gonzalo (#277 / #278), not shipped |
| Budget controls (Enterprise) | 🔴 | usage is reported (`/api/usage`) but not enforced |
| Self-hosted deployment (npx / Docker) | ✅ | container image, standalone or clustered (`docs/container.md`) |

## J. Out of scope for Prospero (OpenHands-distinctive)

| Capability (OpenHands) | Prospero | Notes |
|---|---|---|
| Slack integration | n/a (Ariel) | chat surfaces belong to [Ariel](https://github.com/caliban-ai/ariel), which bridges Discord → Slack → Teams onto prosperod's API |
| Desktop app (Electron, **preview**) | n/a | Prospero ships a web dashboard; a desktop shell is not a control-plane concern |
| Product telemetry (PostHog, on by default) | n/a | Prospero ships no telemetry; nothing to match |

---

## Read: the same shape, a different centre of gravity

- **OpenHands is broader at the edges.** Heterogeneous workers over ACP, a
  first-class automation service (cron, webhooks, events), one UI over many
  servers, and authenticated execution servers out of the box.
- **Prospero is deeper at the core.** Worktree isolation by default, a durable
  `FleetEvent` store with SQLite and Postgres backends, clustered HA with
  leased ownership, age-based retention, and a Kubernetes fleet provider.
  OpenHands documents none of retention, HA, or self-hosted Kubernetes outside
  Enterprise (all ⚠ in the inventory).
- **Governance is paywalled on their side and missing on ours.** SSO, RBAC,
  audit, and budgets are OpenHands Enterprise features; Prospero has none of
  them, and even the open-source OpenHands server has API keys where Prospero
  has nothing.

## Prospero-distinctive gaps worth a ticket

Capabilities OpenHands has that Prospero lacks and that are *in scope* for a
control plane:

1. **Heterogeneous worker backends** (F) — ACP is now a real, documented way to
   drive Claude Code, Codex, and Gemini CLI. An ACP adapter behind Prospero's
   fleet model would close the biggest category gap.
2. **Control-plane API auth** (I) — #2. OpenHands' session API keys with
   rotation are a small, concrete reference design.
3. **Automations** (B) — scheduled and webhook-triggered spawns with tracked
   run states.
4. **Multi-host aggregation** (F) — #1; OpenHands does it client-side with a
   backend switcher, a cheaper model than a server-side relay.
5. **OpenAPI spec** (E) — low effort, and it makes the API usable by
   third-party clients (including Ariel).

---

## Refresh process

1. When a Prospero feature lands: tick the relevant row(s) in the same PR
   (🔴 → 🟡 → ✅), citing the evidence in Notes.
2. When OpenHands ships something new: refresh
   [`capability-inventory.md`](capability-inventory.md) first, then propagate here.
3. Resolve **⚠** rows against OpenHands' live docs / Prospero code when you touch them.
4. Keep Section J as a boundary marker — chat rows go to Ariel, not the Prospero backlog.
5. Bump the **Last refreshed** date at the top.
