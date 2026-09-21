# HTTP API & events

`prosperod` serves one HTTP surface. The CLI, the dashboard and other clients such
as [ariel](./introduction.md#the-caliban-ai-ecosystem) all use it. Requests
and responses are JSON. The request and response types live in the
`prospero-types` crate, which the WASM dashboard also uses, so these shapes are
the ones the server actually serializes.

## The OpenAPI document

Everything this page describes in prose is also published as an
[OpenAPI 3.1](https://spec.openapis.org/oas/v3.1.0.html) document at
`GET /api/openapi.json`, so a client can generate its request and response types
instead of hand-writing them:

```sh
curl -s http://127.0.0.1:7777/api/openapi.json | jq .
```

The route is open — you need the document to work out how to authenticate, and
it describes the API's shape rather than any fleet data. Each operation records
the scope it requires in an `x-prospero-scope` field, so the table below and the
document never disagree about who may call what.

For code generation in CI, where no daemon is running, the same bytes come from
the repository:

```sh
cargo run -p prospero-api --example openapi > openapi.json
```

The schemas are derived from the same `prospero-types` structs the server
serializes, so they cannot drift from the wire. Where a type's read shape
differs from its write shape — a field with a serde default is optional when
sent but always present when returned — the request form is published under a
`…Request` name, and the request body points at that one.

## Authentication

When `prosperod` runs with a tokens file, every route except the open ones
requires a credential: an `Authorization: Bearer pspo_…` header, or the
dashboard's session cookie. Each route below lists the minimum scope it needs.
Routes not in the table fail closed to `admin`. See [Securing the API](./api-auth.md).

## Errors

Errors come back as `{"error": "<message>", "kind": "<kind>"}`:

| Status | `kind` | When |
|---|---|---|
| 400 | `provider_misconfigured`, `invalid_config` | Bad provider or workspace configuration |
| 401 | `unauthorized` | Missing, invalid or expired credential (with `WWW-Authenticate: Bearer realm="prospero"`) |
| 403 | `forbidden` | Scope too low, or a cross-origin mutation made with a session cookie |
| 404 | `not_found` | Unknown agent or workspace |
| 405 | `method_not_allowed` | The active fleet backend has no workspace admin plane wired |
| 409 | `invalid_state`, `conflict` | For example, registering a workspace name that already exists |
| 502 | `protocol` | caliband answered with something unexpected |
| 503 | `unreachable` | caliband (or the fleet backend) could not be reached |
| 500 | `internal` | Store or I/O failure |

## Routes

| Method | Path | Scope | Purpose |
|---|---|---|---|
| GET | `/` , `/assets/{*path}` | open | The dashboard |
| GET | `/healthz` | open | Liveness: always `200 ok` while the process is up |
| GET | `/readyz` | open | Readiness: `200` when the event store is writable, else `503` |
| GET / POST / DELETE | `/api/session` | open | Dashboard sign-in, whoami, sign-out |
| GET | `/api/openapi.json` | open | This API as an OpenAPI 3.1 document |
| GET | `/api/capabilities` | read | What the active backend supports |
| GET | `/api/metrics` | read | Operational counters |
| GET | `/api/fleet` | read | Fleet snapshot: every workspace and its agents |
| GET | `/api/fleet/stream?from=N\|now` | read | Every stream's events, replay then tail (SSE) |
| POST/GET | `/mcp` | operate | Drive the fleet over the Model Context Protocol |
| GET | `/api/usage` | read | Cost, turns and outcomes per workspace per day |
| GET | `/api/workspaces` | read | Workspaces with health, sources, agent count and config |
| POST | `/api/workspaces` | admin | Register a workspace |
| PUT | `/api/workspaces/{name}/config` | admin | Replace a workspace's configuration |
| DELETE | `/api/workspaces/{name}` | admin | Unregister a workspace |
| GET | `/api/workspaces/{workspace}/agents` | read | Agents under one workspace |
| POST | `/api/workspaces/{workspace}/agents` | operate | Spawn an agent |
| GET | `/api/agents/{id}` | read | One agent's current state |
| DELETE | `/api/agents/{id}` | operate | Remove an agent |
| GET | `/api/agents/{id}/events?from=N` | read | Durable event history (JSON array) |
| GET | `/api/agents/{id}/stream?from=N` | read | Replay history, then tail live (SSE) |
| POST | `/api/agents/{id}/kill` | operate | Kill a running agent (`202`) |
| POST | `/api/agents/{id}/respawn` | operate | Replace an agent with a fresh one using the same spec |
| POST | `/api/agents/{id}/input` | operate | Send a user message to an interactive agent (`202`) |
| POST | `/api/agents/{id}/end-input` | operate | Signal end-of-input to an interactive agent (`202`) |

`HEAD` is accepted wherever `GET` is.

### Workspaces

`POST /api/workspaces` takes:

```json
{ "name": "myproj", "root": "/home/me/src/myproj", "config": { "provider": "anthropic" } }
```

`root` is the workspace directory on the local backend. It is ignored under k8s,
where sources come from `config`. `config` is optional. It is the same object
`PUT /api/workspaces/{name}/config` takes as its whole body. Each backend reads
only the fields it uses:

| Field | Backend | Meaning |
|---|---|---|
| `provider` | local | Provider id, exported to caliband as `CALIBAN_PROVIDER` |
| `base_url` | local | Provider base URL, for example `ANTHROPIC_BASE_URL` or `OPENAI_BASE_URL` |
| `api_key_from_env` | local | **Name** of an env var in prosperod's environment whose value becomes the provider's API key |
| `env` | local | Raw `KEY: value` overrides. Stored and returned verbatim, so never put secrets here |
| `display_name` | k8s | Dashboard label |
| `sources` | k8s | `[{name, repo, ref?, path}]` git checkouts |
| `providers` | k8s | `[{name, kind, base_url?, model?, credentials_ref?: {secret_name, key}}]` |
| `default_provider` | k8s | Provider bound when a spawn names none |
| `isolation` | k8s | `{runtime_class?, worktrees?}` |

On the local backend, `POST` returns `201` and `PUT` returns `204`. Both apply
immediately; setting the config restarts the workspace's caliband. Under k8s the
change is written to a `Workspace` custom resource and reconciled
asynchronously, so both return `202`. `GET /api/capabilities` reports which
applies:

```json
{ "admin": true, "async_workspace_ops": false }
```

### Spawning

`POST /api/workspaces/{workspace}/agents`:

```json
{
  "prompt": "refactor the parser",
  "label": "parser",
  "model": "…",
  "isolation": "worktree",
  "interactive": false,
  "tool_allowlist": ["read", "edit"],
  "frontmatter_path": "/path/to/agent-template.md",
  "provider_ref": "planner",
  "timeout_secs": 3600,
  "permission_posture": "supervised"
}
```

Only `prompt` is required. `isolation` defaults to a git worktree; only the exact
string `"shared"` opts out. `provider_ref` picks a named provider under k8s and is
ignored by the local backend, which uses the workspace's stored config.

`timeout_secs` caps the agent's wall-clock life. Prospero — not the agent —
enforces it: the deadline is written to the event log at spawn, so it survives
the replica that set it, and any replica holding the workspace's lifecycle lease
kills the agent once it passes. The log records an `agent_timed_out` event
before the `killed` transition, so history says *why* the agent stopped, and
`/api/usage` counts those runs separately as `timed_out` (they are also counted
in `killed`, because the kill is real). Absent means no limit.

`permission_posture` is `"supervised"` (the default, and what an absent field
means) or `"unattended"`. Supervised keeps the agent's permission gate: a tool
call that needs approval is refused unless a human answers it. Unattended turns
the gate off, so every tool runs without asking — it needs an **`admin`** token,
and a request with a lesser scope is refused with `403` and
`{"error": "unattended requires admin scope"}`. Each granted unattended spawn is
recorded in the audit log with the token's name.

Under k8s the field is written to the `CalibanTask` as `spec.task.permissionPosture`
and is a *request*: the operator admits `unattended` only when the Workspace sets
`spec.agentPolicy.allowUnattended: true`, and otherwise fails the task with
`PostureNotPermitted` before any sandbox starts. prospero runs the agent with the
posture the operator admitted (`status.permissionPosture`), so a cluster whose
operator predates that field (caliban-operator ≤ v0.5.0) runs every session
supervised. It needs caliban ≥ v0.14.0 in the sandbox.

The response is `201`:

```json
{ "agent_id": "…", "workspace": "myproj", "isolated": true, "created": true }
```

`created` is `false` when the request matched an agent that was already running
instead of starting a new one. Spawning is idempotent under k8s.

`POST /api/agents/{id}/respawn` returns `{"agent_id": "<new id>"}`. The old id
leaves `/api/fleet` and `/api/agents/{id}`, but its history is still available
from `/api/agents/{old id}/events`.

### MCP

`/mcp` exposes the fleet as an **MCP server** (streamable HTTP), so an agentic
client can drive prospero with tools instead of hand-written HTTP calls. It is a
thin adapter over the same seam the REST handlers use, so it works on both
backends.

| Tool | Does |
|---|---|
| `prospero_list_workspaces` | Workspaces, health, agent counts |
| `prospero_list_agents` | Every agent with status and workspace |
| `prospero_spawn_agent` | Launch an agent (`workspace`, `prompt`, optional `label`, `model`, `interactive`, `timeout_secs`) |
| `prospero_agent_status` | One agent's current state |
| `prospero_agent_events` | Recorded events from `from` onward, capped (100 default, 500 max) with `truncated` + `next_from` |
| `prospero_send_input` / `prospero_end_input` | Steer an interactive agent |
| `prospero_kill_agent` / `prospero_respawn_agent` | Stop or restart an agent |

The whole endpoint requires the **`operate`** scope — its tools spawn, steer and
kill, and one scope for the surface is simpler to reason about than a second,
per-tool authorization model inside the handler. Workspace administration
(add/remove/configure) is deliberately *not* exposed; use the REST API with an
`admin` token.

A spawn is visible to `prospero_list_agents` from the next poll, exactly as it is
on `GET /api/fleet` — the fleet view is the poll snapshot.

### The fleet stream

`GET /api/fleet/stream` is one SSE connection carrying **every** agent's events,
for a consumer that wants the whole fleet rather than one agent — a notifier, a
dashboard, an external bridge. Without it, watching a fleet meant one connection
per agent, and no way to learn about an agent you had not seen yet.

Each event arrives as JSON with its **fleet cursor** in the SSE `id:` field:

    id: 4821
    data: {"seq":7,"ts":"…","repo":"myproj","agent_id":"a1","kind":{…}}

- `?from=<cursor>` resumes **after** that cursor: no gap, no repeat.
- `?from=now` skips history and delivers only what happens next — what a
  notifier wants on restart, so it does not re-announce everything it already
  handled.
- Omitted replays from the beginning of the store.

The cursor is durable insertion order across all streams, which is the only
total order a fleet has: a per-agent `seq` cannot order two different agents,
since both start at 1.

Events are read from the durable store rather than from the live bus, so a
clustered deployment delivers each event exactly once no matter which replica
produced it, and a slow client cannot be skipped past — the per-agent stream's
`gap` signal has no counterpart here because there is nothing to miss.

### Why an agent is in its state

An agent in `/api/fleet` and `/api/agents/{id}` may carry `reason` and
`detail` — present only when something *outside* the agent decided its state,
and omitted entirely otherwise.

Under k8s they come from the operator's `Ready` condition on the `CalibanTask`.
The case worth knowing: a task that asks for `permission_posture: "unattended"`
under a Workspace that does not set `spec.agentPolicy.allowUnattended: true` is
failed by the operator with `reason: "PostureNotPermitted"` **before any sandbox
is created** — so there is no pod, no caliband and no agent-side error, and this
field is the only account of it. The spawn itself still returns `201`: the
refusal happens during reconciliation, after the API has answered.

Local (non-k8s) agents never carry these: caliband reports their state directly,
with no admission step above it.

### Usage

`GET /api/usage` aggregates `agent_finished` accounting and terminal outcomes
from the event store. Query parameters, all optional:

- `until`: exclusive end, RFC 3339. Defaults to now.
- `since`: inclusive start, RFC 3339. Takes precedence over `days`.
- `days`: window length counted back from `until`. Defaults to 7, minimum 1.

The response echoes the window it used and returns one group per workspace. Each
group has totals and a per-UTC-day `series`:

```json
{
  "since": "…", "until": "…",
  "groups": [{
    "workspace": "myproj", "cost_usd": 1.75, "turns": 6,
    "outcomes": { "done": 2, "failed": 0, "killed": 1, "crashed": 0 },
    "series": [{ "day": "2026-09-01", "cost_usd": 0.75, "turns": 4, "outcomes": { "…": 0 } }]
  }]
}
```

## Events

Every observation becomes a `FleetEvent`:

```json
{
  "seq": 42,
  "ts": "2026-09-13T12:00:00Z",
  "repo": "myproj",
  "agent_id": "…",
  "kind": { "kind": "tool_started", "id": "tu_1", "name": "Read", "input": {} },
  "actor": "alice"
}
```

`seq` increases monotonically within an agent's stream. `repo` holds the owning
workspace name; the field keeps its original name for wire compatibility.
`actor` appears only on events a token-authenticated request caused directly,
which today means local-fleet spawn and remove.

| `kind` | Fields | Meaning |
|---|---|---|
| `agent_spawned` | — | Prospero asked for this agent |
| `agent_discovered` | — | A poll found an agent Prospero had not seen |
| `agent_init` | `model`, `tools`, `session_id` | The agent's run started |
| `status_changed` | `from`, `to` | Lifecycle transition (`spawning`, `running`, `idle`, `killed`, `done`, `failed`, `crashed`) |
| `output` | `stream` (`stdout` or `thinking`), `chunk` | Streamed text |
| `tool_started` | `id`, `name`, `input` | A tool call began |
| `tool_finished` | `id`, `name`, `ok`, `result`?, `truncated`? | A tool call ended. Pair it with its start by `id`; `name` is usually empty. `result` is what the tool returned as text, capped at 8,192 characters; `truncated: true` means it is only the start of a longer result. Both are omitted when absent (events recorded before results were captured, or no result sent) |
| `agent_finished` | `outcome`, `cost_usd`, `turns` | Terminal accounting |
| `agent_gone` | — | The agent left caliband's registry |
| `repo_health` | `state` | A workspace's caliband became `healthy` or `unreachable` |
| `store_persist_failed` | `lost_seq`, `detail` | Event `lost_seq` went out live but is missing from durable history |

Model reasoning (`stream: "thinking"`) is dropped by default. The k8s backend
includes it when `PROSPERO_INCLUDE_THINKING=1`.

Clients should ignore `kind` values they don't recognize.

### The SSE stream

`GET /api/agents/{id}/stream?from=N` first replays stored events with `seq >= N`,
then tails live events, with no gap or duplicate between the two phases. Each
event is an unnamed SSE message whose `data` is one `FleetEvent`. The stream
**closes after `agent_finished`**, so a finished run reads like `tail` of a file
and does not hang. Periodic keep-alive comments are sent while idle.

A consumer that falls far enough behind to overflow its live buffer does not
silently lose events. The server sends a named `gap` event,
`{"skipped": <n>, "last_seq": <seq>}`, then replays the missed events from the
durable store. To resume after a disconnect, reconnect with
`from = last seen seq + 1`.

```sh
curl -N -H "Authorization: Bearer $PROSPERO_TOKEN" \
  "http://127.0.0.1:7878/api/agents/$AGENT/stream?from=0"
```
