# HTTP API & events

`prosperod` serves one HTTP surface. The CLI, the dashboard and other clients such
as [ariel](./introduction.md#the-caliban-ai-ecosystem) all use it. Requests
and responses are JSON. The request and response types live in the
`prospero-types` crate, which the WASM dashboard also uses, so these shapes are
the ones the server actually serializes.

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
| GET | `/api/capabilities` | read | What the active backend supports |
| GET | `/api/metrics` | read | Operational counters |
| GET | `/api/fleet` | read | Fleet snapshot: every workspace and its agents |
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
  "provider_ref": "planner"
}
```

Only `prompt` is required. `isolation` defaults to a git worktree; only the exact
string `"shared"` opts out. `provider_ref` picks a named provider under k8s and is
ignored by the local backend, which uses the workspace's stored config. The
response is `201`:

```json
{ "agent_id": "…", "workspace": "myproj", "isolated": true, "created": true }
```

`created` is `false` when the request matched an agent that was already running
instead of starting a new one. Spawning is idempotent under k8s.

`POST /api/agents/{id}/respawn` returns `{"agent_id": "<new id>"}`. The old id
leaves `/api/fleet` and `/api/agents/{id}`, but its history is still available
from `/api/agents/{old id}/events`.

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
