# Coder (Coder Agents) documented-capability inventory

> **Static snapshot — captured 2026-09-13.**
>
> Structured snapshot of **Coder**'s documented AI-agent surface, captured from
> the canonical docs at `https://coder.com/docs/ai-coder/*`, the `coder/coder`
> release notes, and the `coder/agentapi` repo. This is the *source* feeding
> [`parity-gap-matrix.md`](parity-gap-matrix.md). It is intentionally a
> point-in-time capture, not a live mirror.
>
> **⚠ Status change — read first.** Coder's agent story changed shape between
> releases. **Coder Tasks** (driving third-party agent CLIs — Claude Code,
> Codex, Goose, Aider, … — through **AgentAPI**) is **removed from new
> releases starting v2.37** and survives only on the ESR / v2.36 line. Its
> replacement, **Coder Agents**, is "a standalone agent written in Go", "not a
> wrapper around third-party agent tools". This inventory — and the matrix — score
> **current Coder Agents**; Tasks / AgentAPI are recorded as **legacy** where
> they illuminate the comparison.
>
> **Category note.** Coder is a self-hosted control plane (`coderd` + PostgreSQL)
> that provisions isolated developer workspaces on Kubernetes, Docker, or VMs and
> now runs its own coding agent against them, streaming events and storing chat
> history in Postgres with multi-replica HA. That deployment shape — self-hosted,
> Kubernetes-native, Postgres-backed, HA — is Prospero's; the agent model
> (one built-in agent vs Prospero's caliban workers) is where they differ.
>
> **Currency marker:** **v2.37.0** mainline (2026-09-01); **v2.36.5** stable
> (2026-09-10). Use these and the doc slugs below to gauge drift on the next
> re-baseline.
>
> **Re-baseline cadence:** refresh manually before each parity-prioritization
> review. When refreshing, re-fetch the upstream docs, update the sections
> below, bump the snapshot date in this header, and propagate any new rows into
> `parity-gap-matrix.md` in the same commit.
>
> Conventions: **Premium** = gated to Coder's paid tier; **Early Access** /
> **experimental** = documented but not GA; **⚠ verify** = not confirmed against
> a primary docs page at capture.

## 1. Overview / surfaces

- **What it is:** a self-hosted platform for cloud development environments
  whose AI surface (**Coder Agents**) runs a built-in coding agent inside the
  control plane against user-owned workspaces.
- **Key surfaces:** `coderd` control plane, web UI (chat), `coder` CLI, REST
  Chats API, WebSocket streams, Terraform workspace templates.
- **Runtime / license:** Go control plane + PostgreSQL. ⚠ verify license — the
  core appears to be AGPL-3.0 with a Premium tier (seen in search snippets
  only). `coder/agentapi` is MIT, Go.
- **Repos:** `github.com/coder/coder`, `github.com/coder/agentapi`.

## 2. Architecture

- **Agent loop in the control plane:** the loop runs in `coderd` and "never
  enters workspace"; credentials are held by the control plane only, and
  workspaces can be fully network-isolated.
- **Durable state:** "All chat state is stored in the Coder database"
  (PostgreSQL).
- **Legacy (Tasks / AgentAPI):** each third-party agent CLI ran behind an
  AgentAPI HTTP shim exposing `/messages`, `/message`, `/status`, and SSE
  `/events` — per-agent HTTP + SSE, architecturally close to Prospero's
  per-agent stream over caliband.

## 3. Agent lifecycle (Chats)

- Create chats; send, **queue**, and **edit** messages; **interrupt** (stop);
  **archive**.
- The root agent "can spawn **child agents** to work on independent tasks in
  parallel".
- Workspaces are provisioned automatically, only when the agent needs one.
- **Plan mode.**
- **Legacy:** the Tasks CLI (`coder exp task create|list|status|log|send|delete`)
  was **experimental** as of v2.27.

## 4. Isolation

- Workspaces come from **Coder templates** (Terraform) targeting Docker,
  Kubernetes, or VMs.
- "The agent can only access workspaces owned by the user."
- Template routing and optimization choose which template an agent uses.

## 5. Observability

- **Live stream:** WebSocket `GET /api/v2/chats/{chat}/stream`, event types
  `message_part`, `message`, `status`, `error`, `retry`, `queue_update`;
  reconnect with `after_id`.
- **Fleet-wide watch:** `GET /api/v2/chats/watch`.
- **Durable history:** Postgres.
- **Web UI** with **full-text chat search** (v2.37).
- **Cost / usage:** AI Gateway and AI Governance are **Premium**. ⚠ verify
  what usage / cost reporting they expose.

## 6. Interfaces

- **REST Chats API** — promoted from `/api/experimental` to `/api/v2` in v2.37.
- **WebSocket** streams (per chat + watch).
- **Web chat UI**, **`coder` CLI**.
- **MCP server settings** (org-scoped in v2.37) — MCP servers the agent can use.
- **Legacy:** AgentAPI's per-agent HTTP + SSE (§2).

## 7. Agent backends & integrations

- **Current:** Coder's own agent only.
- IDE integrations listed for **Cursor, Zed, Devin**.
- **Agent Relay** — connect cloud-hosted agents to self-hosted workspaces —
  **Early Access**.
- **Legacy (AgentAPI):** Claude Code, Amazon Q, OpenCode, Goose, Aider, Gemini,
  Copilot, Amp, Codex, Auggie, Cursor CLI.

## 8. Model & provider support

- Anthropic, OpenAI, Google, Azure OpenAI, Amazon Bedrock, OpenAI-compatible
  endpoints, OpenRouter, Vercel AI Gateway.
- Configured deployment-wide; models can be **scoped to organizations**.

## 9. Extensibility

- **Skills**, **MCP**, and **"chat lifecycle hooks"** (v2.37 release notes).
  ⚠ verify hook docs.
- Template routing / optimization (§4).

## 10. Auth / security / deployment / scale

- **Identity:** user identity attached to every agent action; RBAC template
  visibility and per-template access controls (v2.37).
- **HA:** "multiple instances simultaneously connect to the same Postgres
  endpoint"; Helm `coder.replicaCount`. ⚠ verify whether HA needs Premium (the
  HA page states no requirement).
- **Agent Firewall** and **AI Gateway** — **Premium**.
- ⚠ verify — chat / event retention policy (not documented in pages read).

---

## Notable / distinctive vs Prospero

1. **Agent loop in the control plane, credentials never in the workspace** —
   the opposite placement from Prospero, whose caliban workers hold their own
   provider credentials.
2. **Multi-user identity + RBAC** on every agent action — Prospero has no user
   model.
3. **Chat-shaped steering** — queue, edit, interrupt, child agents, plan mode.
4. **Template-provisioned workspaces** (Terraform) across Docker / Kubernetes /
   VMs, rather than git worktrees or per-agent pods.
5. **Gave up heterogeneous workers** — Tasks / AgentAPI drove 11 agent CLIs;
   Coder Agents drives one.

## Explicit uncertainties to re-verify before the next parity pass

- **(a)** License of the `coder/coder` core vs Premium features.
- **(b)** Usage / cost reporting behind AI Gateway / AI Governance (§5).
- **(c)** Chat lifecycle hook documentation (§9).
- **(d)** Whether multi-replica HA requires Premium (§10).
- **(e)** Retention policy for chat state (§10).

---

## Source pages (fetched 2026-09-13)

| Page | URL | Notes |
|---|---|---|
| AI coder overview | `coder.com/docs/ai-coder` | AI surface entry point |
| Coder Agents | `coder.com/docs/ai-coder/agents` | architecture, lifecycle, providers |
| Tasks (now serves Agents content) | `coder.com/docs/ai-coder/tasks` | legacy slug |
| Tasks core principles | `coder.com/docs/ai-coder/tasks-core-principles` | legacy model |
| Tasks → Chats migration | `coder.com/docs/ai-coder/agents/tasks-to-chats-migration` | Chats API, removal in v2.37 |
| High availability | `coder.com/docs/admin/networking/high-availability` | multi-replica on Postgres |
| Automating Tasks via CLI/API | `coder.com/blog/automate-coder-tasks-via-cli-and-api` | legacy CLI |
| Releases | `github.com/coder/coder/releases` | v2.37.0 / v2.36.5 notes |
| AgentAPI | `github.com/coder/agentapi` | legacy per-agent HTTP + SSE shim |
