# GitHub Agent HQ / mission control documented-capability inventory

> **Static snapshot — captured 2026-09-13.**
>
> Structured snapshot of **GitHub Agent HQ** — the *mission control* surface
> over the **Copilot cloud agent** and the **third-party coding agents**
> (Anthropic Claude, OpenAI Codex) — captured from `docs.github.com`, the
> GitHub REST reference, the GitHub Blog and the GitHub Changelog. This is the
> *source* feeding [`parity-gap-matrix.md`](parity-gap-matrix.md). It is
> intentionally a point-in-time capture, not a live mirror.
>
> **⚠ Category note — read first.** Agent HQ is a **hosted, proprietary SaaS**
> control plane: it is not self-hostable, and every agent session runs as a
> **GitHub Actions** run. GitHub describes mission control as "a single command
> center to assign, steer, and track the work of multiple agents". That is
> Prospero's category — launch, steer and observe a fleet of coding agents
> working in parallel — delivered as a feature of the GitHub platform instead of
> a self-hosted daemon. It is the market-defining **hosted multi-vendor**
> analogue; read the [parity matrix](parity-gap-matrix.md) with that framing.
>
> **Currency marker:** no single version (SaaS). Dated milestones:
> mission control shipped **2025-10-28**; Claude and Codex third-party agents
> entered **public preview 2026-02-04**; per-task model selection
> **2026-04-14**; the agent tasks REST API entered **public preview** on
> **2026-05-13** (Business / Enterprise) and **2026-06-04** (Pro / Pro+ / Max).
>
> **Re-baseline cadence:** refresh manually before each parity-prioritization
> review. Re-fetch the source pages below, update the sections, bump the
> snapshot date in this header, and propagate new rows into
> `parity-gap-matrix.md` in the same commit. Re-check every **public preview**
> flag — GA changes the scoring input.
>
> Conventions: *surfaces* = user-visible primitives. Anything documented as
> **public preview** is flagged as such; **⚠ verify** marks a fact seen only in
> press coverage or not confirmed against the docs.

## 1. Overview / surfaces

- **What it is:** a hosted command center on github.com for assigning coding
  work to agents, steering it while it runs, and tracking many sessions across
  repositories. Output lands as a pull request or branch.
- **Agents:** Copilot cloud agent; **Claude (Anthropic)** and **Codex
  (OpenAI)** as third-party agents (**public preview**); custom agents and agent
  apps installed as GitHub Apps.
- **Key surfaces:** the Agents tab / Agents page on github.com, issue
  assignment, @-mentions on pull requests, GitHub Mobile, VS Code, JetBrains,
  the Copilot CLI, the Copilot app, and the agent tasks REST API (**public
  preview**).
- **Runtime / license:** proprietary SaaS; sessions execute on GitHub Actions.

## 2. Architecture

- **SaaS control plane.** Each agent session is a GitHub Actions run in an
  "ephemeral development environment, powered by GitHub Actions".
- **Output model:** the session's work is a PR or branch; each commit links back
  to the session log that produced it.
- **Governance:** branch protection and repository rulesets constrain what the
  agent can push.

## 3. Launch & session lifecycle

- **Start a session from:** the Agents tab / page, assigning an issue to an
  agent, @-mentioning an agent on a PR, GitHub Mobile, VS Code, JetBrains, the
  Copilot CLI, or the Copilot app.
- **Triggers:** schedule- or event-triggered execution.
- **Steer** a running session with follow-up instructions — each steer
  consumes AI credits.
- **Stop session** ends the Actions run; commits already pushed are kept.
- **Archive** sessions.
- **Session limit:** a hard cap of **59 minutes** per session.
- **Task states (REST):** `queued`, `in_progress`, `completed`, `failed`,
  `idle`, `waiting_for_user`, `timed_out`, `cancelled`.

## 4. Isolation

- Ephemeral GitHub Actions environment per session.
- Branch protection / rulesets bound the blast radius of what gets pushed.
- No self-hosted, container-runtime or Kubernetes placement is documented for
  the agent session itself. ⚠ verify — whether self-hosted Actions runners can
  host agent sessions was not covered by the pages read.

## 5. Observability

- **Real-time session logs** of the agent's reasoning and tool use.
- **Session overview** shows token usage and session length.
- **Search past sessions** in natural language from the Copilot CLI and VS Code.
- **Usage** is reported in AI credits; billing is Actions minutes + AI credits.

## 6. Interfaces

- **Web UI** (Agents tab / page), **IDEs** (VS Code, JetBrains), **Copilot
  CLI**, **Copilot app**, **GitHub Mobile**.
- **REST — agent tasks API (public preview):**
  `POST /agents/repos/{owner}/{repo}/tasks`,
  `GET /agents/repos/{owner}/{repo}/tasks`, `GET /agents/tasks`,
  `GET /agents/tasks/{task_id}`.
- **Not in the REST API:** no documented endpoint for streaming session logs,
  stopping a session, or steering it.
- **MCP:** the GitHub and Playwright MCP servers are enabled by default *for the
  agent* (MCP as a client-side tool source, not the control plane exposed as an
  MCP server).

## 7. Registry / multi-backend

- **Copilot cloud agent** (first-party).
- **Claude** (Anthropic) and **Codex** (OpenAI) — third-party agents, **public
  preview**; an admin policy must enable them.
- **Custom agents** and **agent apps** installed as GitHub Apps.
- ⚠ verify — press coverage also lists Google, Devin and Grok agents; not in the
  docs read.

## 8. Model & provider handling

- **Per-task model selection** (Changelog 2026-04-14): Anthropic models for the
  Claude agent, OpenAI models for the Codex agent.
- Provider credentials and routing are GitHub-managed; billed through AI
  credits.

## 9. Extensibility

- **Hooks** — run "custom shell commands at key points during agent execution".
- **Skills**, **custom agents**, **MCP servers**, and `agents.md` instructions.

## 10. Auth / security / deployment

- **Auth:** fine-grained PAT or GitHub App user token with the **"Agent tasks"**
  permission.
- **Policy:** org/enterprise admin policy gates third-party agents.
- **Deployment:** hosted only; not self-hostable.
- ⚠ verify — audit-log coverage of agent sessions. A separate API for auditing
  cloud-agent repository configuration was announced (Changelog 2026-05-18) but
  not read.

---

## Notable / distinctive vs Prospero

1. **Multi-vendor by default.** One command center drives Copilot, Claude and
   Codex (third-party agents in public preview); Prospero drives only caliban.
2. **GitHub-native work intake and output.** Issues, PR @-mentions and
   schedules/events start sessions; PRs and branches are the output, with
   commit → session-log links.
3. **Hosted and billed per use.** Actions minutes + AI credits; no self-hosting,
   no data-plane choice, a 59-minute session ceiling.
4. **Everywhere-clients.** Web, IDEs, CLI, desktop app and mobile all reach the
   same session list.
5. **Thin public API.** The REST surface (public preview) can create, list and
   read tasks, but cannot stream, steer or stop them.

## Explicit uncertainties to re-verify before the next parity pass

- **(a)** Whether self-hosted Actions runners (or any customer-controlled
  compute) can host agent sessions (§4).
- **(b)** Google / Devin / Grok agent availability (§7) — press only.
- **(c)** Audit-log coverage of sessions and the cloud-agent configuration audit
  API (§10).
- **(d)** GA dates for the third-party agents and the agent tasks REST API —
  both public preview at capture.
- **(e)** Whether the REST API grows stream / steer / stop endpoints (§6).

---

## Source pages (fetched 2026-09-13)

| Page | URL | Notes |
|---|---|---|
| Mission control how-to | `github.blog/ai-and-ml/github-copilot/how-to-orchestrate-agents-using-mission-control/` | "single command center" positioning |
| Agent management | `docs.github.com/en/copilot/concepts/agents/cloud-agent/agent-management` | sessions, steer, stop, archive |
| Third-party coding agents | `docs.github.com/en/copilot/concepts/agents/about-third-party-coding-agents` | Claude / Codex, public preview, policy |
| Manage and track agents | `docs.github.com/…/manage-and-track-agents` | session logs, overview, search |
| About cloud agent | `docs.github.com/…/cloud-agent/about-cloud-agent` | Actions environment, limits, hooks, MCP |
| Agent tasks REST API | `docs.github.com/en/rest/agent-tasks/agent-tasks` | endpoints + task states (public preview) |
| GitHub Changelog | entries 2026-02-04, 2026-04-14, 2026-05-13, 2026-06-04 | preview / model-selection / API milestones (search results) |
