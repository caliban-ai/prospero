# Prospero ↔ Coder parity gap matrix

> **What this is:** a living comparison between **Prospero** (this project — the
> agent-orchestration control plane over caliband) and **Coder**, whose **Coder
> Agents** surface runs coding agents from a self-hosted control plane against
> template-provisioned workspaces. Refresh it whenever a major feature lands or
> Coder ships a new capability.
>
> **Why here.** Coder is the closest *deployment-shape* analogue on the board:
> self-hosted, Kubernetes-native, Postgres-backed, multi-replica HA, with a REST
> API and live event streams over agents. That is Prospero's posture. The agent
> model differs — Coder now runs **its own** agent (the loop lives in `coderd`),
> while Prospero supervises caliban workers — so rows about *driving* agents
> compare Prospero against Coder Agents, with the retired **Tasks / AgentAPI**
> (which drove third-party agent CLIs) noted where it is the closer analogue.
>
> **Companion document:** [`capability-inventory.md`](capability-inventory.md)
> — a dated snapshot of Coder's documented surface; refresh both together.

**Legend:** ✅ Prospero has an equivalent · 🟡 partial · 🔴 gap · **n/a** =
Coder-surface concept with no intended Prospero analogue. **n/a (Ariel)** = in
scope for the caliban-ai stack, but owned by
[Ariel](https://github.com/caliban-ai/ariel), the chat-bridge service in front
of prosperod — not by Prospero. A ✅ means "Prospero does the equivalent thing,"
not byte-identical. Rows are scored by the caliban evaluation tree's rule: ✅
only when a production call path reaches the capability.

**Last refreshed:** 2026-09-13 (initial capture. Coder surface from
[`capability-inventory.md`](capability-inventory.md) snapshot 2026-09-13,
v2.37.0; Prospero state verified against the code on the same date). Auth row
refreshed 2026-09-13 for #2 — token auth shipped, `crates/api/src/auth/`,
ADR-0010.

> **Caveat:** rows tagged **⚠** depend on a Coder fact flagged uncertain in the
> inventory (Premium gating, hooks, cost reporting) or a Prospero detail inferred
> rather than traced to a call path.

---

## A. Control-plane architecture

| Capability (Coder) | Prospero | Notes |
|---|---|---|
| Single self-hosted control-plane daemon | ✅ | `prosperod` (ADR-0002); Coder runs `coderd` |
| Durable state in PostgreSQL | ✅ | `PostgresStore` + `PostgresConfigStore` in clustered mode (`crates/core/src/postgres_store.rs`); SQLite / JSONL standalone |
| Agent loop runs in the control plane | n/a | architectural choice, not a gap: Prospero deliberately supervises caliban workers that run their own loop (ADR-0002, ADR-0003) |
| Provider credentials held only by the control plane | 🔴 | ⚠ inferred: caliban workers hold their own provider keys (k8s agents get env / Secret material from the operator; `crates/core/src/k8s/fleet.rs` `SessionPlane`). Coder keeps credentials in `coderd` and out of workspaces |

## B. Agent launch & fleet lifecycle

| Capability (Coder) | Prospero | Notes |
|---|---|---|
| Launch an agent on a task | ✅ | `POST /api/workspaces/{workspace}/agents` (`crates/api/src/lib.rs`), `prospero spawn` |
| List the fleet | ✅ | `GET /api/fleet`, `prospero ls` |
| Send follow-up messages to a running agent | 🟡 | `POST /api/agents/{id}/input` + `/end-input` steer an **interactive** agent (`FleetProvider::send_input`); interactive agents are not representable on the k8s backend |
| Queue / edit messages | 🔴 | no queue or edit verbs on the API |
| Interrupt without killing | 🔴 | only `kill` / `respawn` / `DELETE` (`crates/api/src/lib.rs`); no interrupt-and-continue |
| Archive / remove | ✅ | `DELETE /api/agents/{id}` (`rm_agent`); history survives in the store |
| Child agents in parallel | n/a (delegated) | sub-agents are a caliban concern; Prospero's parallelism is many top-level agents per workspace |
| Plan mode | n/a (delegated) | agent-level behaviour, owned by caliban |

## C. Isolation

| Capability (Coder) | Prospero | Notes |
|---|---|---|
| Per-agent isolated workspace | ✅ | git worktree per spawn by default (ADR-0005); per-agent pods on `K8sFleet` (ADR-0008) |
| Kubernetes backend | ✅ | `K8sFleet` `FleetProvider` (ADR-0008) |
| Docker / VM backends | 🟡 | prosperod itself ships as a container (`docs/container.md`); no Docker-per-agent or VM provider |
| Template-provisioned environments (Terraform) | 🔴 | no template layer; a per-workspace config (`PUT /api/workspaces/{name}/config`) is the closest analogue |
| Network-isolated workspaces | 🟡 | ⚠ the caliban-operator's default-deny NetworkPolicy isolates agent pods to same-namespace ingress (`docs/superpowers/specs/2026-07-15-live-cluster-spawn-fixes-design.md`); lives in the operator, not Prospero, and local fleets have none |

## D. Observability

| Capability (Coder) | Prospero | Notes |
|---|---|---|
| Live per-agent event stream | ✅ | SSE `GET /api/agents/{id}/stream` (`crates/api/src/sse.rs`); Coder uses WebSocket |
| Resume a stream from a cursor | ✅ | `GET /api/agents/{id}/stream?from=<seq>` replays persisted history from that seq, then tails the live bus with seq dedup — no gap or dup (`crates/api/src/sse.rs`); equivalent to Coder's `after_id` |
| Fleet-wide watch stream | 🔴 | `GET /api/fleet` is a snapshot poll; no fleet-wide push stream like `chats/watch` |
| Durable history | ✅ | `Store` trait (ADR-0004) with JSONL / SQLite / Postgres |
| Web UI | ✅ | embedded Dioxus/WASM dashboard |
| Full-text search over history | 🔴 | no search endpoint or index |
| Usage / cost reporting | 🟡 | ⚠ `GET /api/usage` rendered by the dashboard; no CLI command. Coder's equivalent sits behind Premium AI Gateway / Governance |

## E. Interfaces

| Capability (Coder) | Prospero | Notes |
|---|---|---|
| REST API (GA, versioned) | 🟡 | `/api/*` REST ships and is used by CLI + dashboard, but is unversioned; Coder moved Chats to `/api/v2` |
| Push event streams | ✅ | SSE |
| Web UI | ✅ | dashboard |
| Operator CLI | ✅ | `prospero` |
| MCP server configuration | 🔴 | Prospero neither consumes nor exposes MCP (MCP servers an agent uses are configured in caliban) |

## F. Registry, discovery & multi-backend

| Capability (Coder) | Prospero | Notes |
|---|---|---|
| Workspace registry | ✅ | `GET/POST /api/workspaces`, `DELETE /api/workspaces/{name}` |
| Heterogeneous agent products | 🟡 | Prospero drives caliban only (ADR-0003). Current Coder is **also** single-agent; legacy AgentAPI drove 11 CLIs — the capability Coder retired |
| Connect cloud-hosted agents to self-hosted workspaces | 🔴 | ⚠ Coder's Agent Relay is **Early Access**; Prospero has no remote/multi-host transport (#1) |
| IDE integrations (Cursor / Zed / Devin) | 🔴 | none |

## G. Model / provider handling

| Capability (Coder) | Prospero | Notes |
|---|---|---|
| Deployment-wide provider configuration | n/a (delegated) | model / provider choice is caliban's (Anthropic / OpenAI / Google / Bedrock / Vertex; local runners via the OpenAI adapter + `base_url`) |
| Models scoped to organizations | n/a | no org model in Prospero; see I |

## H. Extensibility

| Capability (Coder) | Prospero | Notes |
|---|---|---|
| Skills | n/a (delegated) | skills live in caliban |
| Chat lifecycle hooks | 🔴 | ⚠ Coder hook docs unverified; Prospero has no control-plane hook surface (caliban hooks fire inside agents) |

## I. Auth / security / deployment / scale

| Capability (Coder) | Prospero | Notes |
|---|---|---|
| Authenticated API | 🟡 | named bearer tokens with `read`/`operate`/`admin` scopes guard every inbound route (`crates/api/src/auth/`, ADR-0010, #2); still no per-user identity or RBAC — see the rows below |
| User identity on every agent action | 🔴 | no user model; agents are owned by workspaces, not people |
| RBAC / per-template access control | 🔴 | none |
| Multi-replica HA on shared Postgres | ✅ | clustered mode: N `prosperod` replicas, Postgres store + LISTEN/NOTIFY bus + leased single-writer ownership (`crates/core/src/leased_ownership.rs`); soak/failover test still open (#65) |
| Helm-deployable | 🟡 | deployed with Helm from `johnford2002/helm-charts` via Argo; no chart ships in this repo |
| Retention policy | 🟡 | age-based retention (`--retention-days`); JSONL rotation open (#4). ⚠ Coder's policy undocumented |
| Egress / agent firewall | 🔴 | ⚠ Coder's Agent Firewall is **Premium**; nothing equivalent in Prospero |

## J. Out of scope for Prospero

| Capability (Coder) | Prospero | Notes |
|---|---|---|
| Cloud development environments for humans (IDE-in-browser, SSH workspaces) | n/a | Prospero runs agent fleets, not developer workstations |
| Chat-platform integrations for agent notifications | n/a (Ariel) | owned by [Ariel](https://github.com/caliban-ai/ariel) |

---

## Read: same deployment shape, opposite agent placement

- **Coder is a multi-user platform; Prospero is a single-operator control
  plane.** The widest remaining gap is identity: per-user ownership, RBAC, and
  a credential boundary. Coder attaches a user to every agent action; Prospero
  now authenticates callers with named tokens (#2) but still has no per-user
  identity.
- **Coder moved the agent into the control plane; Prospero keeps it in the
  worker.** Coder's loop runs in `coderd` so credentials never reach a
  workspace. Prospero's wire-only coupling to caliban (ADR-0003) is what lets it
  stay agent-agnostic — the property Coder gave up when it retired AgentAPI.
- **Prospero leads where it is agent-native:** worktree-by-default isolation,
  durable replay behind a pluggable store, and a fleet provider that treats
  agents as first-class Kubernetes objects rather than chats inside a workspace.
- **Steering is Coder's other edge:** queue / edit / interrupt and a fleet-wide
  watch stream make a running agent conversational; Prospero offers input on
  interactive agents and kill / respawn.

## Prospero-distinctive gaps worth a ticket

1. **User identity + RBAC on agent actions** (I) — #2 shipped token auth; the
   remaining prerequisite for multi-user is attaching a person (not just a
   token) to every action, plus RBAC/per-template access control.
2. **Fleet-wide watch stream** (D) — one SSE stream of status changes across the
   fleet, replacing snapshot polling in the dashboard and future Ariel
   notifications.
3. **Interrupt without kill** (B) — stop the current turn and keep the session,
   matching Coder's interrupt.

---

## Refresh process

1. When a Prospero feature lands: tick the relevant row(s) in the same PR
   (🔴 → 🟡 → ✅).
2. When Coder ships something new: refresh
   [`capability-inventory.md`](capability-inventory.md) first, then propagate here.
3. Resolve **⚠** rows against Coder's live docs / Prospero code when you touch them.
4. Keep Section J as a boundary marker — workstation and chat rows are not Prospero backlog.
5. Bump the **Last refreshed** date at the top.
