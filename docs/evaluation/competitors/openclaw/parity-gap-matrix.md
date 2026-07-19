# Prospero ↔ OpenClaw parity gap matrix

> **What this is:** a living comparison between **Prospero** (this project — the
> agent-orchestration control plane over caliband) and **OpenClaw**, a
> self-hosted multi-channel gateway whose core is *also* an agent control
> plane. Refresh it whenever a major feature lands or OpenClaw ships a new
> capability.
>
> **Why here (and not in caliban).** OpenClaw was first captured in the caliban
> repo, but against a *single* terminal agent most of its surface had to be
> marked out-of-scope: OpenClaw doesn't compete with a coding *engine*, it
> competes with a control plane that *drives* engines. That is Prospero's
> category. Its coding path delegates to background workers (Codex / Claude
> Code / OpenCode) in isolated worktrees; Prospero launches and supervises
> caliban agents in isolated worktrees. Same species. The caliban repo keeps
> only the thin **worker-backend** angle (caliban as a thing OpenClaw could
> drive); the orchestration comparison lives here.
>
> **Companion document:** [`capability-inventory.md`](capability-inventory.md)
> — a dated snapshot of OpenClaw's documented surface; refresh both together.

**Legend:** ✅ Prospero has an equivalent · 🟡 partial · 🔴 gap · **n/a** =
OpenClaw-surface concept with no intended Prospero analogue (Prospero is a
dev-fleet control plane, not a chat/assistant gateway). A ✅ means "Prospero
does the equivalent thing," not byte-identical.

**Last refreshed:** 2026-07-19 (initial capture — re-homed from the caliban
repo's OpenClaw area. OpenClaw surface from
[`capability-inventory.md`](capability-inventory.md) snapshot 2026-07-18;
Prospero state cross-referenced from its README + ADRs 0002–0009).

> **Caveat:** rows tagged **⚠** depend on an OpenClaw fact still flagged
> uncertain in the inventory or a Prospero detail inferred from the README/ADRs
> rather than re-verified against the code.

---

## A. Control-plane architecture

| Capability (OpenClaw) | Prospero | Notes |
|---|---|---|
| Single self-hosted control-plane daemon | ✅ | `prosperod` control plane (ADR-0002 — a control plane over caliband, *not* a re-implementation) |
| Couples to workers through a thin wire adapter | ✅ | couples to caliban **only** via its NDJSON wire format (ADR-0003); no dependency on caliban crates |
| Typed event protocol, schema-validated | ✅ | normalizes caliban stream-json into a stable `FleetEvent` type; OpenClaw uses TypeBox/JSON-Schema frames |
| Control clients over a network API | ✅ | CLI + dashboard talk to the same HTTP API (OpenClaw uses a WebSocket control plane) |

## B. Agent launch & fleet lifecycle

| Capability (OpenClaw) | Prospero | Notes |
|---|---|---|
| Launch/spawn agent workers | ✅ | `prospero spawn <repo> "<task>"` |
| List the fleet | ✅ | `prospero ls` (repos + agents) |
| Kill / stop an agent | ✅ | `prospero kill <agent-id>` |
| Respawn / remove fleet-wide | ✅ | list / kill / respawn / remove across the fleet |
| Parallel agents on one codebase | ✅ | multiple agents per repo, isolated by worktree |
| Bind agents to identities/channels | 🟡 | Prospero binds agents to repos + ids; OpenClaw binds to channel identities (different axis) |

## C. Isolation

| Capability (OpenClaw) | Prospero | Notes |
|---|---|---|
| Git-worktree isolation for workers | ✅ | worktree isolation **by default** (ADR-0005); `--shared-tree` to opt out |
| Container isolation | 🟡 | `docs/container.md` + a `K8sFleet` Kubernetes provider (ADR-0008); no single `--container` run flag like OpenClaw |
| Kubernetes fleet backend | ✅ | `K8sFleet` `FleetProvider` (ADR-0008) — Prospero can place agents on K8s; OpenClaw has no K8s placement |

## D. Observability

| Capability (OpenClaw) | Prospero | Notes |
|---|---|---|
| Live event stream (fan-out) | ✅ | live SSE fan-out of `FleetEvent`; `prospero follow <agent-id>` |
| Durable history that survives the agent | ✅ | hybrid live + durable model behind a `Store` trait (ADR-0004); JSONL event store — caliban itself is live-only |
| Web control UI / dashboard | ✅ | embedded dashboard on `127.0.0.1:7878` |
| Per-agent status polling + stream attach | ✅ | polls each caliband for status, attaches to per-agent streams while active |
| Audit / usage-cost views | 🟡 | event log supports it; no dedicated `usage-cost`/`audit` command surface like OpenClaw |

## E. Interfaces

| Capability (OpenClaw) | Prospero | Notes |
|---|---|---|
| Operator CLI over the API | ✅ | `prospero` (thin HTTP client over `prosperod`) |
| REST API | ✅ | `prospero-api` (axum REST) |
| Server-push events (SSE / WS) | ✅ | SSE (OpenClaw uses WebSocket) |
| Embedded web UI | ✅ | dashboard served by `prospero-api` |
| MCP-server exposure of the control plane | 🔴 | Prospero exposes REST/SSE, not MCP; OpenClaw can `mcp serve` |

## F. Registry, discovery & multi-backend

| Capability (OpenClaw) | Prospero | Notes |
|---|---|---|
| Register managed workspaces/repos | ✅ | `prospero repo add <name> <path>` |
| Discovery of running supervisors | ✅ | `prospero-core` discovery of calibands |
| Heterogeneous worker backends | 🟡 | Prospero drives **caliban** agents (any model caliban supports); OpenClaw drives *multiple agent products* (Codex/Claude Code/OpenCode). Non-caliban backends 🔴 |
| Multi-host fleets | 🔴 | deferred (spec non-goal); OpenClaw spans nodes/devices |

## G. Model / provider handling

| Capability (OpenClaw) | Prospero | Notes |
|---|---|---|
| Model/provider selection | n/a (delegated) | Prospero delegates all model choice to caliban (Anthropic/OpenAI/Ollama/Google/Bedrock/Vertex); OpenClaw carries its own 60+ provider layer |
| Routing / fallback | n/a (delegated) | lives in caliban's router (ADR-0038 there), not the control plane |

## H. Extensibility

| Capability (OpenClaw) | Prospero | Notes |
|---|---|---|
| Skills / plugins at the control-plane level | 🔴 | skills/plugins live in the *agent* (caliban), not in Prospero; Prospero has no plugin surface |
| Hosted marketplace/registry (ClawHub) | n/a | not a control-plane concern; caliban owns the plugin marketplace |
| Lifecycle hooks | 🟡 | caliban hooks fire inside agents; Prospero has no control-plane hook surface |

## I. Auth / security / deployment / scale

| Capability (OpenClaw) | Prospero | Notes |
|---|---|---|
| Local-first bind | ✅ | binds `127.0.0.1:7878` by default |
| Control-plane auth (tokens / device pairing / Tailscale) | 🔴 | API auth is a deferred non-goal; OpenClaw has device pairing + shared-secret + Tailscale identity |
| Durable-log retention / rotation | 🔴 | deferred; OpenClaw persists transcripts |
| Pluggable store backend | 🟡 | `Store` trait exists (ADR-0004); a sqlite backend is deferred (JSONL today) |
| Supervised service (launchd/systemd) | ✅ | `prosperod` runs supervised; OpenClaw daemon likewise |

## J. Out of scope for Prospero (OpenClaw-distinctive)

All **n/a** — OpenClaw is a personal-assistant gateway; Prospero is a dev-fleet
control plane. Listed only to mark the boundary.

| Capability (OpenClaw) | Prospero | Notes |
|---|---|---|
| Multi-channel messaging (Discord/Slack/WhatsApp/Signal/…) | n/a | not a messaging product |
| Mobile/desktop nodes (canvas, camera, voice, screen) | n/a | no device-node mesh |
| Media generation (image/music/video/tts) | n/a | not a media tool |
| Personal-assistant memory / wiki / knowledge base | n/a | orchestrates dev agents, doesn't hold assistant memory |
| Browser-automation tool | n/a | that's an agent-level tool (caliban's concern) |

---

## Read: same species, different reach

- **Prospero is narrower and deeper.** It is a caliban-native dev-fleet control
  plane: worktree-by-default, a durable `FleetEvent` store, a Kubernetes fleet
  provider, and an in-process fake-caliband test harness. It does one thing —
  run and observe fleets of coding agents — thoroughly.
- **OpenClaw is broader and shallower on orchestration.** Its control plane is
  wrapped in a multi-channel personal assistant; coding is one delegated skill
  among messaging, media, and device control.
- **They could interoperate rather than only compete.** OpenClaw drives
  heterogeneous coding-agent workers; Prospero already drives caliban. If
  caliban grows a server/ACP/MCP-server surface (its top cross-competitor gap —
  see the caliban repo's `docs/evaluation/`), caliban becomes drivable by
  *both* — and Prospero
  itself could, in principle, be one of the backends a gateway like OpenClaw
  delegates to.

## Prospero-distinctive gaps worth a ticket

Capabilities OpenClaw has that Prospero lacks and that are *in scope* for a
control plane (the assistant/channel surface is deliberately excluded):

1. **Heterogeneous worker backends** (F) — drive non-caliban agents behind the
   same fleet model. Prospero's wire-only coupling (ADR-0003) makes this a
   natural extension.
2. **Control-plane API auth** (I) — already a known deferred non-goal; OpenClaw's
   token/device/Tailscale model is a reference.
3. **MCP-server exposure of the fleet** (E) — let other tools drive Prospero over
   MCP, not just REST/SSE.
4. **Durable-log retention / rotation + a sqlite `Store`** (I) — both deferred;
   OpenClaw persists transcripts durably.

---

## Refresh process

1. When a Prospero feature lands: tick the relevant row(s) in the same PR
   (🔴 → 🟡 → ✅).
2. When OpenClaw ships something new: refresh
   [`capability-inventory.md`](capability-inventory.md) first, then propagate here.
3. Resolve **⚠** rows against OpenClaw's live docs / Prospero code when you touch them.
4. Keep Section J as a boundary marker — don't let assistant/channel rows creep into the Prospero backlog.
5. Bump the **Last refreshed** date at the top.
