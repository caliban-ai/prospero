# OpenHands Agent Canvas + Agent Server documented-capability inventory

> **Static snapshot — captured 2026-09-13.**
>
> Structured snapshot of the documented surface of **OpenHands Agent Canvas**
> and the **OpenHands Agent Server** it drives, captured from the canonical docs
> at `https://docs.openhands.dev/*` and the `OpenHands/*` GitHub repos. This is
> the *source* feeding [`parity-gap-matrix.md`](parity-gap-matrix.md). It is
> intentionally a point-in-time capture, not a live mirror.
>
> **⚠ Category note — read first.** OpenHands is best known as an open-source
> coding *agent*; that engine is caliban's comparison, not Prospero's. The slice
> captured here is the **control layer**: Agent Canvas describes itself as "the
> self-hosted developer control center for coding agents and automations" and
> is explicitly "not an agent runtime or sandbox". It dispatches conversations
> to one or more **Agent Server** backends, which run the native OpenHands agent
> *or* other agent products over ACP (Claude Code, Codex, Gemini CLI). That
> client → server → heterogeneous-worker split is the same shape as Prospero's
> CLI/dashboard → `prosperod` → caliban agents, so this is a close control-plane
> analogue. Score only the Canvas + Agent Server + Automation Server slice.
>
> **Currency marker:** OpenHands **v1.18.0, released 2026-09-11**. Canvas is
> TypeScript (npm/npx, Docker, Electron desktop preview); Agent Server is Python
> (the software-agent SDK). License: **MIT** per the repo README (⚠ verify
> whether an enterprise directory is carved out).
>
> **Re-baseline cadence:** refresh manually before each parity-prioritization
> review. When refreshing, re-fetch the upstream docs, update the sections
> below, bump the snapshot date in this header, and propagate any new rows into
> `parity-gap-matrix.md` in the same commit.
>
> Conventions: *surfaces* = user-visible primitives; **Preview** / **Enterprise**
> mark capabilities that are not generally available in the open-source
> self-hosted build; ⚠ marks facts not confirmed in canonical docs.

## 1. Overview / surfaces

- **What it is:** a self-hosted control surface (Agent Canvas) over one or more agent-execution backends (Agent Server), plus an Automation Server for scheduled and triggered runs.
- **Key surfaces:** Canvas web UI (browser client), Canvas desktop app (Electron, **preview**), Agent Server REST + WebSocket API, SDK/CLI, Automation Server.
- **Repos:** `github.com/OpenHands/OpenHands`, `OpenHands/agent-canvas`, `OpenHands/software-agent-sdk`.
- **Hosted sibling:** OpenHands Cloud (managed platform sandbox); not part of the self-hosted slice.

## 2. Architecture

Four documented components:

- **Agent Canvas** — browser client. Stores only backend connection information (and a per-backend API key); holds no agent runtime.
- **Agent Server** — runs conversations. Each server binds one host/port and serves "multiple concurrent conversations".
- **Automation Server** — stores schedules and triggers, tracks runs, dispatches conversations to an Agent Server.
- **Workspace / sandbox** — where the agent actually executes (see §4).

## 3. Launch & lifecycle

- Start conversations; send messages into a running conversation.
- File and command operations exposed through the Agent Server.
- **Fork / branch** a conversation.
- **Automations:** triggers are cron schedules, webhooks, and backend events. Run states: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`, `SKIPPED`. "Run now" and enable/disable per automation.
- ⚠ verify — explicit kill / respawn semantics for a single conversation were not documented in the pages read.

## 4. Isolation / runtimes

- Three documented runtime placements: **host process** (no isolation), **container / pod** "with its mounts and network policy", or a **managed platform sandbox** (OpenHands Cloud).
- Backend setup guides for **Docker**, **VM**, **Cloud**, and **Modal**.
- Kubernetes appears as an "API remote runtime" and in enterprise material. ⚠ verify a self-hosted Kubernetes path outside Enterprise.

## 5. Observability

- **Live stream:** WebSocket `WS /conversations/{id}/events/socket`.
- **Durable history:** the Agent Server persists `conversations/` and `bash_events/` under its workspace directory; the SDK persists full event logs plus LLM usage statistics.
- **Dashboard:** Canvas web UI; conversation **tags and filtering** (added in v1.18).
- **Cost / usage:** automation activity logs show LLM cost in USD and export to **CSV / JSON**; Canvas has a **Token Usage** panel.
- ⚠ verify — retention policy for persisted conversations was not documented in the pages read.

## 6. Interfaces

- **REST** under `/api/*`, with an **OpenAPI** UI at `/docs`.
- **Health / info endpoints:** `/health`, `/ready`, `/server_info`.
- **Push events:** WebSocket (see §5).
- **Web UI** (Canvas) and a **CLI** (via the SDK).
- **MCP:** MCP settings on the client side, for giving agents tools.
- **Webhooks:** inbound, as automation triggers. ⚠ verify outbound push / webhook notifications.

## 7. Agents / multi-backend

- **Native OpenHands agent**, plus **ACP agents**: Claude Code (`@agentclientprotocol/claude-agent-acp`), Codex (`@zed-industries/codex-acp`), Gemini CLI (`--acp`), and custom ACP servers.
- Agent choice is stored **per backend**.
- **Multiple Agent Servers:** Canvas connects to several backends through a backend switcher, so one UI spans several hosts.

## 8. Model & provider handling

- Direct provider API keys, OpenHands LLM keys, **ACP subscription logins** (take priority over environment keys), or local / OpenAI-compatible providers.
- **LLM profiles** for reusable model configurations.

## 9. Extensibility

- **Plugins**, **skills**, `AGENTS.md`, and **MCP** integrations.
- **Integrations:** Slack, GitHub, Linear, Notion.

## 10. Auth / security / deployment

- **Agent Server auth:** requires the `X-Session-API-Key` header; `OH_SESSION_API_KEYS_*` supports key rotation; `OH_SECRET_KEY` encrypts stored secrets. Docs warn never to expose the server unauthenticated.
- **Canvas** stores a per-backend API key.
- **Telemetry:** PostHog, on by default.
- **Enterprise only** (per the openhands.dev blog; ⚠ verify in docs): SSO, RBAC, audit logs, budget controls, and an "Agent Control Plane".
- **Deployment:** npm / npx, Docker, Electron desktop (**preview**).
- ⚠ verify — high availability / multi-replica operation of the Agent Server was not documented in the pages read.

---

## Notable / distinctive vs Prospero

1. **Heterogeneous workers over a standard protocol.** ACP lets one Agent Server drive OpenHands, Claude Code, Codex, or Gemini CLI; Prospero drives only caliban.
2. **Automations are first-class.** Cron, webhook, and event triggers with tracked run states live in a dedicated Automation Server.
3. **One UI over many servers.** The Canvas backend switcher spans hosts; Prospero has no multi-host transport.
4. **Authenticated by default.** Session API keys with rotation and encrypted secrets on the execution server.
5. **Enterprise tier** holds the governance surface (SSO, RBAC, audit, budgets), so the open-source build is thinner there than the marketing suggests.

## Explicit uncertainties to re-verify before the next parity pass

- **(a)** License carve-outs: is any enterprise directory excluded from MIT?
- **(b)** Kill / respawn semantics for a conversation (§3).
- **(c)** Self-hosted Kubernetes runtime outside Enterprise (§4).
- **(d)** Retention and HA of the Agent Server (§5, §10).
- **(e)** Outbound webhooks / push notifications (§6).
- **(f)** Which governance features are Enterprise-only, confirmed in docs rather than the blog (§10).

---

## Source pages (fetched 2026-09-13)

| Page | URL | Notes |
|---|---|---|
| Agent Canvas overview | `docs.openhands.dev/openhands/usage/agent-canvas/overview` | positioning, "control center" |
| Agent Canvas architecture | `docs.openhands.dev/openhands/usage/agent-canvas/architecture` | four components |
| Agent Canvas backends | `docs.openhands.dev/openhands/usage/agent-canvas/backends` | runtimes, backend switcher |
| ACP agents | `docs.openhands.dev/openhands/usage/agent-canvas/acp-agents` | Claude Code / Codex / Gemini CLI |
| Managing automations | `docs.openhands.dev/openhands/usage/agent-canvas/managing-automations` | triggers, run states, cost logs |
| Agent Server (SDK) | `docs.openhands.dev/sdk/arch/agent-server` | REST / WS API, auth, persistence |
| OpenHands repo + releases | `github.com/OpenHands/OpenHands` | v1.18.0, MIT |
| Software agent SDK | `github.com/OpenHands/software-agent-sdk` | Agent Server runtime |
| Third-party listing | `followagents.com/en/agents/openhands-agent-canvas` | corroboration only |
| OpenHands blog | `openhands.dev/blog/open-source-ai-coding-agents` | Enterprise features (search snippet; ⚠) |
