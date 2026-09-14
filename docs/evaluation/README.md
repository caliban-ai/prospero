# Evaluation

Home for how we measure Prospero against comparable systems — chiefly other
**agent-orchestration control planes**.

This mirrors the convention established in the caliban repo
(`caliban/docs/evaluation/`), scoped to Prospero's layer: Prospero is a control
plane over fleets of coding agents, so its competitors are other control
planes / gateways that launch, route to, and observe agent workers — not the
terminal coding agents themselves (those live in caliban's evaluation tree).

## Layout

| Directory | Contents |
|-----------|----------|
| [`competitors/`](competitors/) | Per-competitor capability inventories and parity analysis. One subdirectory per competitor, each with a documented-capability inventory + a Prospero ↔ competitor parity gap matrix. Currently: [`openclaw/`](competitors/openclaw/) — OpenClaw, a multi-channel assistant gateway whose control-plane core is the same species as Prospero; [`openhands/`](competitors/openhands/) — OpenHands Agent Canvas + Agent Server, the closest open-source self-hostable analogue (a control surface dispatching OpenHands and ACP agents to agent-server backends); [`coder/`](competitors/coder/) — Coder Agents, a self-hosted Go control plane with Kubernetes workspaces and Postgres HA (Prospero's deployment shape); [`github-agent-hq/`](competitors/github-agent-hq/) — GitHub Agent HQ / mission control, the hosted multi-vendor fleet product (Copilot, Claude, Codex); [`vibe-kanban/`](competitors/vibe-kanban/) — Vibe Kanban, the most-adopted open-source single-host orchestrator (10+ agent CLIs in worktrees; sunsetting, community-maintained). |

### Where Prospero stands (2026-09-13)

Counts are capability-table rows in each matrix's lettered sections (A–J);
"in-scope ✅" is ✅ ÷ (✅ + 🟡 + 🔴), n/a rows excluded. Matrices differ in row
count and granularity, so compare the gap themes more than the percentages.

| Competitor | Shape | ✅ | 🟡 | 🔴 | n/a | In-scope ✅ |
|---|---|---|---|---|---|---|
| [OpenClaw](competitors/openclaw/parity-gap-matrix.md) | self-hosted assistant gateway | 23 | 9 | 3 | 8 | 66% |
| [GitHub Agent HQ](competitors/github-agent-hq/parity-gap-matrix.md) | hosted multi-vendor fleet | 18 | 10 | 8 | 11 | 50% |
| [OpenHands](competitors/openhands/parity-gap-matrix.md) | OSS self-hostable platform | 16 | 6 | 13 | 7 | 46% |
| [Vibe Kanban](competitors/vibe-kanban/parity-gap-matrix.md) | OSS local orchestrator | 9 | 6 | 6 | 5 | 43% |
| [Coder](competitors/coder/parity-gap-matrix.md) | self-hosted k8s/HA control plane | 16 | 9 | 13 | 8 | 42% |

**Gaps that recur across competitors** (the strongest prioritization signal):

- **Control-plane API auth / identity** — token auth shipped (#2): ✅ against
  GitHub Agent HQ and OpenHands (comparable token/API-key granularity); 🟡
  against OpenClaw (no device pairing / Tailscale identity) and Coder (no
  per-user identity or RBAC); Vibe Kanban documents no auth story of its own
  either. Per-user identity, SSO and RBAC remain open (gonzalo#277).
- **Heterogeneous worker backends** — OpenClaw, OpenHands (ACP), Agent HQ and Vibe Kanban all drive several agent products; Prospero drives only caliban (ADR-0003 makes this a wire-adapter extension).
- **MCP-server exposure of the fleet** — OpenClaw and Vibe Kanban.
- **Automations** — scheduled / webhook / event-triggered spawns (OpenHands, Agent HQ).
- **Steering beyond interactive agents** — follow-up input works only for `interactive: true` spawns; no queue, interrupt, or edit (Coder, Vibe Kanban).

**Where Prospero is ahead:** a Kubernetes fleet backend and clustered Postgres
HA in a self-hosted OSS control plane (only Coder matches the deployment
shape); a durable, resumable event stream (`?from=<seq>` replay-then-live);
and a REST API that can stream, steer, stop and respawn a running agent (Agent
HQ's preview API can only create and read tasks).

### Candidates considered and not tracked (2026-09-13)

Recorded so the next sweep doesn't re-litigate them:

- **Google Antigravity Agent Manager** — folded into the Antigravity 2.0 desktop app; no documented API, not self-hostable, single-vendor harness.
- **Claude Squad** — active tmux + worktree TUI with no API, persistence or web UI; the replacement pick if Vibe Kanban goes dormant.
- **Terragon Labs** — shut down (~2026-02). **Sculptor** (Imbue) — experimental research preview, no API. **Crystal / Nimbalyst**, **Conductor** — desktop GUIs without a server/API.
- **Nora** — close shape (self-hosted, k3s, REST/MCP, Postgres) but fleets assistant agents rather than coding agents.
- **Single-vendor hosted agents** (Claude Code cloud agents, Codex cloud, Cursor background agents, Jules, Devin, Factory, Warp, Ona) — their fleet views are features of one agent; GitHub Agent HQ covers the hosted multi-vendor slot.
- **container-use, uzi, Goose subagents** — isolation primitives or in-agent parallelism, not control planes.

## Conventions

- **Competitors** each get their own directory under `competitors/<name>/` with
  a static, dated `capability-inventory.md` (a snapshot of the competitor's
  documented surface) and a living `parity-gap-matrix.md` (Prospero ↔ competitor,
  ✅/🟡/🔴/n·a). Re-baseline the inventory manually before a parity-prioritization
  pass; tick matrix rows in the same PR that ships the feature.
- **Scoring** follows the caliban tree's rule (`caliban/docs/evaluation/README.md`,
  "Scoring rule for parity matrices"): a row is ✅ only when a production call
  path from the shipped binaries reaches it; machinery with no non-test caller is
  🟡 at most; a design or ADR alone is 🔴. Cite the evidence (file path, ADR,
  PR/issue) in the Notes column of every row you change, and prefer 🟡 at merge
  time over a ✅ that has to be undone.
- **Chat / messaging surfaces are Ariel's, not Prospero's.**
  [Ariel](https://github.com/caliban-ai/ariel) is the fleet's chat bridge
  (Discord → Slack → Teams), a sibling service that talks to prosperod over its
  HTTP + SSE API. Competitor channel features score **n/a (Ariel)** here and must
  not become Prospero backlog items; compare them in Ariel's own tree once it
  ships.

## Relationship to the caliban evaluation tree

caliban's `docs/evaluation/competitors/` tracks **terminal coding agents**
(Claude Code, Codex, OpenCode, Grok Build, Pi, Antigravity) — things caliban
competes with head to head. Orchestration-layer competitors that *drive* such agents belong here
instead. OpenClaw is the first: it was originally captured in caliban's tree,
then re-homed here because its comparison is with a control plane, not a coding
engine. caliban keeps only the thin "caliban as a worker backend" note under its
own `competitors/openclaw/`.
