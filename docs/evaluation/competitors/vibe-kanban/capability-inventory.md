# Vibe Kanban documented-capability inventory

> **Static snapshot — captured 2026-09-13.**
>
> Structured snapshot of **Vibe Kanban**'s documented surface, captured from the
> `BloopAI/vibe-kanban` GitHub repo (README + releases) and its docs site
> (`vibekanban.mintlify.dev`). This is the *source* feeding
> [`parity-gap-matrix.md`](parity-gap-matrix.md). It is intentionally a
> point-in-time capture, not a live mirror.
>
> **⚠ Sunset status — read first.** The README states **"Vibe Kanban is
> sunsetting."** bloop, the company behind it, **shut down on 2026-04-10**; the
> hosted/cloud and team features ended roughly 30 days later. The project
> continues as **community-maintained open source**, and local mode still works
> *(per third-party review — vibecoding.app)*. Treat every row below as a
> snapshot of a product with no vendor behind it. **If the repo goes dormant,
> replace this comparison with [Claude Squad](https://github.com/smtg-ai/claude-squad)**
> (an actively maintained tmux + worktree multi-agent TUI) — the next-closest
> open-source local orchestrator.
>
> **Category note (why this lives in the Prospero repo).** Vibe Kanban is a
> **local orchestrator** for coding agents: a Rust backend + web UI that
> launches many heterogeneous coding-agent CLIs in parallel, each in its own git
> worktree, and exposes the fleet over MCP. It does not write code itself — it
> launches, supervises, and reviews the output of agents that do. That is
> Prospero's category (a control plane over coding-agent workers), scoped to a
> single host. Of the open-source orchestrators, it is the most adopted
> (≈28.1k GitHub stars at capture).
>
> **Currency marker:** latest release **v0.1.44 (2026-04-24)** — cut two weeks
> after the shutdown; no release since at capture. Distributed via
> `npx vibe-kanban`.
>
> **Re-baseline cadence:** refresh manually before each parity-prioritization
> review — and first check whether the repo is still maintained (see the sunset
> note). When refreshing, re-fetch the upstream sources, update the sections
> below, bump the snapshot date in this header, and propagate any new rows into
> `parity-gap-matrix.md` in the same commit.
>
> Conventions: *surfaces* = user-visible primitives. **⚠ verify** = not
> confirmed in the primary sources read. *(third-party)* = sourced from a
> review rather than the repo or docs.

## 1. Overview / surfaces

- **What it is:** a local orchestration app for coding agents organised around a kanban board: issues map to workspaces, and each workspace runs coding-agent sessions in an isolated git worktree.
- **Key surfaces:** a **local backend server** serving a **web UI**; a **local MCP server**; PR creation from the UI.
- **Runtime / license:** Rust backend + Node/pnpm web frontend; **Apache-2.0**.
- **Repo:** `github.com/BloopAI/vibe-kanban`. Docs: `vibekanban.com` / `vibekanban.mintlify.dev`.

## 2. Install & onboarding

- **Install / run:** `npx vibe-kanban` launches the backend and opens the web UI.
- **Ports:** configurable via `PORT` / `BACKEND_PORT`.
- **Platforms:** wherever Node + the agent CLIs run. ⚠ verify the platform support matrix.

## 3. Architecture (A)

- **Local backend server** (Rust) serves the web UI and the MCP endpoint; state is local to the host.
- **Data model:** kanban **issues** (with tags and relationships) → **workspaces** (one git worktree each) → coding-agent **sessions**.
- ⚠ verify — the persistence layer and schema for sessions/logs were not documented in the sources read.

## 4. Agent launch & lifecycle (B)

- **Launch:** create a workspace + coding-agent session per task/issue.
- **Follow-ups:** send follow-up messages to a session; follow-ups are **auto-queued when the executor is busy**, and queued messages can be **cancelled**.
- **Repo scripts:** per-repo **setup**, **cleanup**, and **dev-server** scripts run around a workspace.
- **Cleanup:** orphaned / expired workspaces are cleaned up automatically (opt out with `DISABLE_WORKTREE_CLEANUP`).
- ⚠ verify — explicit kill / restart semantics for a running session beyond cancelling queued messages.

## 5. Isolation (C)

- **Git worktrees only** — each workspace is a separate worktree.
- **No container or Kubernetes backend** documented.

## 6. Observability (D)

- **Execution logs** in the web UI (release notes).
- **Inline diff review** with comments on agent changes.
- **Built-in browser / devtools preview** of the app under development.
- ⚠ verify — durable history store (what survives a restart) and usage / cost tracking; neither is documented in the sources read.

## 7. Interfaces (E)

- **Web UI** (primary).
- **MCP server:** `npx -y vibe-kanban@latest --mcp`; tools cover **issues, tags, relationships, repos, workspaces, and sessions** — i.e. the fleet is drivable by other MCP clients.
- **PR creation** from a workspace, with **AI-generated PR descriptions**.
- ⚠ verify — a public, documented REST API (the web UI talks to the backend, but no API reference was found).

## 8. Registry, discovery & multi-backend (F)

- **Repos** are registered with the app; workspaces are created against them.
- **Heterogeneous agents — "10+ coding agents":** Claude Code, Codex, Gemini CLI, GitHub Copilot, Amp, Cursor, OpenCode, Droid, CCR, Qwen Code.
- **Remote access:** via reverse proxy, SSH, or tunnel mode. The app itself is **single-host**.

## 9. Model / provider handling (G)

- **Delegated** to each agent CLI (each carries its own provider/auth).
- A **model selector** is mentioned in release notes. ⚠ verify scope (per session vs per agent profile).

## 10. Extensibility (H)

- **Repo scripts** (setup / cleanup / dev-server — see §4).
- **MCP client configuration** for the agents it launches.
- **Centralized agent configuration** (agent profiles managed in one place).
- No plugin SDK or hook surface documented.

## 11. Auth / security / deployment / scale (I)

- **Origins allowlist** is configurable for the backend.
- **Analytics** can be toggled off.
- **Self-hosted "Cloud" guide** exists in the docs, but cloud / team features were **discontinued** with the shutdown.
- ⚠ verify — authentication, HA, and log retention: none documented.

---

## Notable / distinctive vs Prospero

1. **Drives many agent products**, not one — 10+ CLIs behind one board; Prospero drives only caliban.
2. **MCP server over the fleet** — other agents can create issues/workspaces/sessions through MCP.
3. **Review loop in the UI** — inline diffs with comments, a dev-server preview, and PR creation with generated descriptions.
4. **Task-board front end** — the kanban board is the primary way work enters the fleet.
5. **Local, single-host, worktree-only** — no container/k8s placement, no documented auth, HA, or retention.
6. **No vendor** — sunsetting since 2026-04-10; community-maintained.

## Explicit uncertainties to re-verify before the next parity pass

- **(a)** Maintenance status — is there any release after v0.1.44 (2026-04-24)? If dormant, swap in Claude Squad.
- **(b)** Durable history and usage/cost tracking (§6).
- **(c)** Whether a documented REST API exists (§7).
- **(d)** Kill / restart semantics for a running session (§4).
- **(e)** Model-selector scope (§9).
- **(f)** Authentication, HA, and retention (§11).
- **(g)** Local-mode viability after the shutdown is *(third-party)*; confirm against the repo.

---

## Source pages (fetched 2026-09-13)

| Page | URL | Notes |
|---|---|---|
| Repo README | `github.com/BloopAI/vibe-kanban` | positioning, sunset notice, agents list, license, env vars |
| Releases | `github.com/BloopAI/vibe-kanban/releases` | v0.1.44 (2026-04-24); execution logs, model selector, queueing |
| MCP server docs | `vibekanban.mintlify.dev/docs/integrations/vibe-kanban-mcp-server` | `--mcp` invocation + tool surface |
| Review *(third-party)* | `vibecoding.app/blog/vibe-kanban-review` | shutdown timeline, local mode still working |
| MCP listing *(third-party)* | lobehub MCP catalog | wrapper listing only; **not** used for capability claims |
