# OpenClaw documented-capability inventory

> **Static snapshot — captured 2026-07-18.**
>
> Structured snapshot of **OpenClaw**'s documented surface, captured from the
> canonical docs at `https://docs.openclaw.ai/*` and the `openclaw/openclaw`
> GitHub repo. This is the *source* feeding
> [`parity-gap-matrix.md`](parity-gap-matrix.md). It is intentionally a
> point-in-time capture, not a live mirror.
>
> **⚠ Category note — read first (and why this lives in the Prospero repo).**
> OpenClaw is a self-hosted, multi-channel **personal-assistant gateway** ("a
> personal AI assistant you run on your own devices … it answers you on the
> channels you already use", "the lobster way 🦞"). Its **core is a control
> plane**: a gateway daemon that launches, routes to, and observes agent
> *workers* — and for coding work its `coding-agent` skill **delegates to
> background workers (Codex, Claude Code, or OpenCode)** in isolated git
> worktrees. That control-plane core is the same species as **Prospero** (the
> agent-orchestration layer over caliband), which is why the OpenClaw
> comparison lives here rather than in the caliban repo: against a single
> terminal agent most of OpenClaw's surface is out of scope, but against
> Prospero the launch / fleet / observe / persist / dashboard rows are *real*
> parity. Read the [parity matrix](parity-gap-matrix.md) with that framing.
> (The one caliban-side angle — caliban as a delegated *worker backend* — is
> noted in the caliban repo's `docs/evaluation/competitors/openclaw/`.)
>
> **Currency marker:** distributed via npm `openclaw@latest` (Node.js /
> TypeScript, MIT); no single release version was surfaced at capture. Use the
> Node floor (24.15+ recommended; 22.22.3+ / 25.9+) and the doc slugs below to
> gauge drift on the next re-baseline.
>
> **Re-baseline cadence:** refresh manually before each parity-prioritization
> review. When refreshing, re-fetch the upstream docs, update the sections
> below, bump the snapshot date in this header, and propagate any new rows into
> `parity-gap-matrix.md` in the same commit.
>
> Conventions: *surfaces* = user-visible primitives; "Config = X" lines name
> the canonical configuration mechanism.

## 1. Overview / surfaces

- **What it is:** A local-first **Gateway** (control plane) that connects 20+ chat channels to AI agents and tools, with mobile/desktop nodes, a web UI, and a skills/plugins registry. Coding is one skill among many (messaging, media, browser, automation, memory/wiki).
- **Key surfaces:** **Gateway daemon** (`openclaw gateway`; typed WebSocket API on `127.0.0.1:18789`), **CLI** (`openclaw`), **Web Control UI**, **macOS app** (menu bar) + **Windows Hub**, **mobile nodes** (iOS/Android with Canvas, camera, voice), **headless nodes**, **channel plugins** (Discord, Slack, Telegram, WhatsApp [Baileys], Signal, iMessage, Matrix, Google Chat, MS Teams, Zalo, WebChat, …).
- **Runtime / license:** Node.js (24.15+ recommended; 22.22.3+ / 25.9+) / TypeScript; **MIT**.
- **Repo:** `github.com/openclaw/openclaw`. Docs: `docs.openclaw.ai`.

## 2. Install & onboarding

- **Install:** `npm install -g openclaw@latest`, then `openclaw onboard --install-daemon`.
- **Onboarding:** `openclaw onboard` / `openclaw setup` — guided flow (verifies inference, then Gateway, workspace, channels, skills, health). `openclaw setup --baseline` for a non-guided baseline. `openclaw configure` to modify an existing setup.
- **Platforms:** macOS, Linux, Windows (per onboarding docs).

## 3. Architecture

- **Gateway daemon:** single host, the only component holding a WhatsApp session; maintains all platform connections; exposes a **typed WebSocket API**; validates frames against JSON Schema (TypeBox → JSON Schema → Swift models); emits typed events (`agent`, `chat`, `presence`, `health`, `heartbeat`, `cron`). Supervised via launchd/systemd; `openclaw gateway {start,stop,restart,run,status,health,probe,diagnostics export,usage-cost}`.
- **Control-plane clients** (macOS app, CLI, web UI, automations) connect over WebSocket to `127.0.0.1:18789`, send requests, subscribe to events.
- **Nodes** (macOS/iOS/Android/headless) connect with `role: node` + declared capabilities/commands, exposing `canvas.*`, `camera.*`, `screen.record`, `location.get`, etc.
- **Agent request model:** client submits an `agent` request → Gateway `ack {runId, status:"accepted"}` → streaming `event:agent` updates → final `{runId, status, summary}`.
- **Auth / pairing:** device identity on `connect`; new device IDs need approval → device token; loopback auto-approvable; Tailnet/LAN connects need explicit approval; signed `connect.challenge` nonce (`v3` binds `platform`/`deviceFamily`). Gateway auth modes: shared-secret token/password, Tailscale Serve identity, trusted-proxy headers, or `"none"`. Remote access via Tailscale/VPN/SSH tunnel; optional TLS + pinning.

## 4. CLI reference (grouped; exhaustive by subcommand family)

- **Setup / config:** `onboard`, `setup` (`--baseline`), `configure`, `config` (get/set/unset/file/schema/validate), `doctor`, `dashboard`, `completion`, `update` (wizard/status/repair), `backup` (create/verify), `migrate` (list/plan/apply), `reset`, `uninstall`.
- **Agents / sessions:** `agent` (single interaction), `agents` (list/add/delete/bindings/bind/unbind/set-identity), `sessions` (cleanup), `transcripts` (list/show/path), `attach`, `acp` (access-control protocol).
- **Messaging / channels:** `message` (send/broadcast/poll/react/read/edit/delete/pin/…/role/channel/member/voice/event/timeout/kick/ban), `channels` (list/status/capabilities/resolve/logs/add/remove/login/logout), `pairing` (list/approve), `qr`.
- **Models / inference:** `models` (list/status/set/set-image/aliases/fallbacks/image-fallbacks/scan/auth), `infer`|`capability` (list/inspect/model/image/audio/tts/video/web/embedding), `promos` (list/claim), `memory` (status/index/search), `commitments` (list/dismiss), `wiki` (status/doctor/init/compile/lint/ingest/search/synthesis/imports[okf,bridge,chatgpt,obsidian]).
- **MCP:** `mcp` (**serve** [Gateway as MCP server], list/show/set/unset).
- **Runtime / sandbox / UI:** `tui` (+ `chat`/`terminal` = `tui --local`), `sandbox` (list/recreate/explain), `approvals` (get/set/allowlist) + `exec-policy` (show/preset/set), `browser` (status/start/stop/tabs/open/navigate/click/type/screenshot/snapshot/pdf/evaluate/…).
- **Automation:** `cron` (list/add/edit/rm/enable/disable/runs/run), `tasks` (list/audit/show/notify/cancel/flow), `hooks` (list/info/check/enable/disable/install/update), `webhooks` (gmail).
- **Nodes / devices:** `nodes` (status/describe/approve/reject/invoke/notify/push/canvas/camera/screen), `devices` (list/remove/approve/reject/rotate/revoke), `node` (run/status/install/stop/restart), `worker`, `directory` (self/peers/groups), `system` (event/heartbeat/presence).
- **Skills / plugins:** `skills` (search/install/update/verify/workshop/list/info/check), `plugins` (list/search/inspect/install/uninstall/update/enable/disable/doctor/build/validate/init/registry/marketplace).
- **Security / ops:** `security` (audit), `secrets` (reload/audit/configure/apply), `audit`, `health`, `status`, `logs`, `proxy` (start/run/coverage/sessions/query/blob/purge), `daemon` (legacy service control).
- **Global flags:** `--dev` (isolate under `~/.openclaw-dev`, port 19001), `--profile <name>` (isolate under `~/.openclaw-<name>`), `--container <name>` (Docker/Podman), `--log-level`, `--no-color`, `--json`, `--plain`, `-V`/`--version`.
- **Chat slash commands:** `/status`, `/trace`, `/config`, `/debug` (gated by `commands.debug: true`).

## 5. Tools & capabilities

- **Runtime:** `exec`, `process`, `terminal`, `code_execution` (provider-backed Python).
- **Files:** `read`, `write`, `edit`, `apply_patch`.
- **Human input:** `ask_user` (structured decisions).
- **Web:** `web_search`, `x_search`, `web_fetch`.
- **Browser:** `browser` (automated session control; mirrored by the `openclaw browser` CLI).
- **Messaging:** `message` (channel replies/actions).
- **Sessions / agents:** `sessions_*`, `subagents`, `agents_list`, `session_status`, `get_goal`/`create_goal`/`update_goal`.
- **Automation:** `cron`, `heartbeat_respond`.
- **Infra:** `gateway`, `nodes`.
- **Media:** `image`, `image_generate`, `music_generate`, `video_generate`, `tts`.
- **Large-catalog discovery:** `tool_search`, `tool_search_code`, `tool_describe` (defer full schema exposure — analogous to lazy MCP loading).
- **Plugin-provided:** Diffs (file/markdown diff render), Show widget (inline SVG/HTML), LLM Task (JSON workflow steps), **Lobster** (typed workflows with resumable approvals), **Tokenjuice** (exec/bash output compacting), **Canvas** (node control + A2UI rendering).
- **Permissions:** `tools.allow` / `tools.deny`, active profile, provider restrictions, sandbox state, channel permissions, per-agent restrictions for delegated runs.

## 6. Coding-agent skill (how OpenClaw "codes")

- **Delegation model:** the `coding-agent` skill **hands substantial dev work to background workers — Codex, Claude Code, or OpenCode** — for feature builds, PR reviews, large refactors, and issue→PR loops (not simple/read-only edits).
- **Isolation:** work happens in **isolated git worktrees** (never the primary checkout or `~/.openclaw`); fetches the canonical target base + source branch immediately before creating the worktree; branch/ancestry checks keep work current with base.
- **Execution:** workers edit within the worktree and run shell via **PTY** (for Codex/OpenCode); **Claude Code runs without PTY, in permission-bypass mode**.
- **Verification:** proof mechanisms — verify initial HEAD, ancestry checks before push, review cycles "until no accepted actionable findings".
- **Comms:** workers report completion/failure through OpenClaw's **messaging** system (channels), not system events/heartbeats.

## 7. Model & provider support

- **60+ providers**, including **Anthropic (Claude via API *and* the Claude CLI)**, OpenAI, Google Gemini, Mistral, Cohere, xAI, Amazon Bedrock, DeepSeek, Groq, Together, Moonshot/Kimi, Qwen, Tencent (Doubao), Perplexity. **Local runners:** Ollama, LM Studio, vLLM, SGLang, inferrs. **Routing gateways:** ClawRouter, LiteLLM, OpenRouter.
- **Config:** model as `"provider/model"`; per-agent selection, e.g. `{ agents: { defaults: { model: { primary: "anthropic/claude-opus-4-6" } } } }`. `openclaw models` manages set/aliases/**fallbacks**/image-fallbacks/auth.
- **Modalities:** text + image/audio/tts/video/web/embedding via `infer`; transcription providers (Deepgram, ElevenLabs, OpenAI); image/music/video **failover**.
- ⚠ verify — reasoning-effort / thinking-token controls were not documented on the providers page at capture.

## 8. Memory / knowledge

- **`memory`** (status/index/search) — indexed session memory.
- **`wiki`** — a knowledge base: init/compile/lint/ingest/search/synthesis + imports from OKF, ChatGPT export, and Obsidian.
- **Skills** (`SKILL.md`) provide reusable workflows/instructions from workspaces, shared dirs, or plugins.

## 9. Skills, plugins & ClawHub registry

- **Skills:** `openclaw skills {search,install,update,verify,workshop,list,info,check}`; `SKILL.md` + supporting files.
- **Plugins:** Plugin SDK + manifest contracts; add tools, skills, channels, providers, hooks, capabilities. `openclaw plugins {…,build,validate,init,registry,marketplace}`.
- **ClawHub** (`docs.openclaw.ai/clawhub`): the **public registry** for skills + code/bundle plugins. Install via `openclaw skills install @scope/name` / `openclaw plugins install clawhub:<pkg>`; publish via the separate **`clawhub` CLI** (`clawhub skill publish …`, `clawhub package publish …`). **Trust model:** anyone can upload, but publishing needs a GitHub account old enough to pass an upload gate; automated checks on releases; moderator review + user reporting; problematic content hidden from public catalogs.

## 10. MCP

- **`openclaw mcp serve`** — run the **Gateway as an MCP server**.
- **`openclaw mcp {list,show,set,unset}`** — inspect/configure MCP (server registration).
- ⚠ verify — the `/tools` page did not describe MCP, but the CLI exposes it; confirm client-vs-server split + config shape against the MCP concept page on the next pass.

## 11. Sandbox & approvals

- **`openclaw sandbox`** (list/recreate/explain) — managed sandbox environments.
- **`openclaw approvals`** (get/set/allowlist) + **`exec-policy`** (show/preset/set) — approval/allowlist policy for tool + command execution.
- **`--container <name>`** runs OpenClaw inside a Docker/Podman container; coding workers additionally isolate via git worktrees (§6).

## 12. Automation & scheduling

- **`cron`** (list/add/edit/rm/enable/disable/runs/run) — scheduled agent runs.
- **`tasks`** (+ flows) — task orchestration/audit/maintenance.
- **`hooks`** (list/info/check/enable/disable/install/update) — lifecycle hooks; installable/updatable like plugins.
- **`webhooks`** — e.g. Gmail setup/run.

## 13. Observability / ops

- **Diagnostics:** `doctor`, `dashboard`, `status`, `health`, `logs`, `audit`, `security audit`, `gateway diagnostics export`, `gateway usage-cost`.
- **Secrets:** `secrets` (reload/audit/configure/apply).
- **Transcripts:** `transcripts` (list/show/path).

## 14. Nodes / devices / multi-device

- **Mobile + headless nodes** with capability declarations; `nodes` (canvas/camera/screen/notify/push), `devices` (pairing/approve/reject/rotate/revoke), `node` (run/install/stop/restart), `worker`.
- **Directory:** `directory` (self/peers/groups) for peer/mesh topology.

---

## Notable / distinctive vs a terminal coding agent (caliban / Claude Code / Codex)

1. **It orchestrates *other* coding agents.** The coding path delegates to Codex / Claude Code / OpenCode workers in git worktrees — OpenClaw is a head/gateway, not a coding engine. Caliban could be a *backend worker* for it.
2. **Multi-channel messaging is the primary surface** (WhatsApp/Slack/Discord/Signal/iMessage/Telegram/… via channel plugins) — an entirely different delivery model from a terminal REPL.
3. **Gateway + node mesh:** a single WebSocket control plane with mobile/desktop nodes exposing camera, canvas, screen-record, voice — device-centric, not repo-centric.
4. **Broad modality set:** first-class image/music/video/tts **generation** and transcription, plus a `browser` automation tool and a `wiki` knowledge base with Obsidian/ChatGPT imports.
5. **ClawHub registry** with a `clawhub` publishing CLI and GitHub-gated moderation — a hosted skill/plugin marketplace.
6. **60+ model providers** including routing gateways (ClawRouter/LiteLLM/OpenRouter) and even the **Claude CLI** as a provider backend.
7. **Container + worktree isolation** and an approvals/exec-policy layer, but oriented around delegating to workers rather than sandboxing its own edits.

## Explicit uncertainties to re-verify before the next parity pass

- **(a)** MCP client-vs-server split and config shape (§10) — CLI exposes `mcp serve`/`set` but the tools page omits MCP.
- **(b)** reasoning-effort / thinking controls (§7) — not documented on the providers page.
- **(c)** exact CLI-only vs app/node command boundaries (the CLI surface is very large and spans device/channel operations).
- **(d)** no single release version was surfaced; pin one on the next capture for a firmer currency marker.

---

## Source pages (fetched 2026-07-18)

Canonical docs at `https://docs.openclaw.ai/<slug>`. Repo: `github.com/openclaw/openclaw`. Marketing: `https://openclaw.ai/`.

| Page | Slug | Notes |
|---|---|---|
| Getting started | `/start/getting-started` | onboarding |
| Install | `/install` | npm + daemon |
| Architecture & agents | `/concepts/architecture` | Gateway, nodes, protocol |
| Tools & capabilities | `/tools` | built-in + plugin tools |
| Channels | `/channels` | 20+ platforms |
| Model providers | `/providers` | 60+ providers |
| ClawHub marketplace | `/clawhub` | registry + `clawhub` CLI |
| Platforms | `/platforms` | OS/node support |
| Gateway & operations | `/gateway` | daemon ops |
| CLI reference | `/cli` | full subcommand tree |
| Help / troubleshooting | `/help` | doctor, diagnostics |
| Coding-agent skill | repo `skills/coding-agent/SKILL.md` | worker delegation (Codex/Claude Code/OpenCode) |
| Repo README | `github.com/openclaw/openclaw` | positioning, MIT, Node/TS |
