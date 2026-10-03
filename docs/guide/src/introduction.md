# prospero

Prospero is the **control plane for fleets of [caliban](https://github.com/caliban-ai/caliban)
agents**. You use it to launch, manage and observe many agents across many
workspaces, including several agents working in parallel on the same codebase.

It has three parts:

- **`prosperod`**, a long-running daemon that serves an HTTP/JSON API, Server-Sent
  Events streams and an embedded web dashboard on `127.0.0.1:7878` by default.
- **`prospero`**, an operator CLI. It is a thin HTTP client over that API.
- **The dashboard**, a Rust → WASM app served by `prosperod` at `/`.

The CLI, the dashboard and any other client all use the same public API. That API
is published as an [OpenAPI 3.1 document](./api.md#the-openapi-document), and
`prosperod` also exposes the fleet as an [MCP server](./api.md#mcp) at `/mcp`, so
an agentic client can drive it with tools instead of hand-written HTTP calls.

## How it works

Caliban already ships a per-workspace supervisor, `caliband`, which spawns and
manages background agents over a Unix-socket NDJSON protocol. Prospero sits
*above* many calibands:

```text
prospero CLI ─┐
dashboard ────┼─ HTTP/JSON + SSE ─▶ prosperod ─┬─ local backend: caliband per workspace (Unix sockets)
other clients ┘                                └─ k8s backend:   CalibanTask / Workspace custom resources
```

- **Launch.** Spawn agents under any registered workspace. Each spawn runs in an
  isolated git worktree by default. Sharing the working tree is an explicit
  opt-out.
- **Manage.** List, kill, respawn and remove agents, and send input to interactive
  agents. A spawn may carry a wall-clock timeout that Prospero — not the agent —
  enforces, and a permission posture that decides whether tool calls needing
  approval wait for a human.
- **Observe.** Prospero turns caliban's stream output into a stable `FleetEvent`
  type. Events go out live over SSE — per agent, or as one
  [fleet-wide stream](./api.md#the-fleet-stream) — and are also written to a
  durable store (sqlite standalone, Postgres clustered), so an agent's history
  survives after it finishes. `/api/usage` aggregates that history into cost,
  turns and outcomes per workspace.
- **Automate.** An [automation](./api.md#automations) spawns an agent on a cron
  schedule or a signed webhook, so recurring work needs nobody at a keyboard.

Prospero talks to caliban only through its wire format. It depends on no caliban
crate ([ADR 0003](./adr/0003-couple-to-caliban-via-ndjson-wire-format.md)).

## The caliban-ai ecosystem

| Project | Role |
|---|---|
| [caliban](https://github.com/caliban-ai/caliban) | The agent harness, plus `caliband`, the supervisor Prospero drives on the local backend. |
| **prospero** | This project: the fleet control plane. |
| [gonzalo](https://github.com/caliban-ai/gonzalo) | A shareable persistence layer for caliban. |
| [ariel](https://github.com/caliban-ai/ariel) | A chat bridge for the fleet: Discord today, Slack and Teams planned. |
| [caliban-operator](https://github.com/caliban-ai/caliban-operator) | Reconciles `CalibanTask` and `Workspace` resources into caliband pods for Prospero's k8s backend ([ADR 0008](./adr/0008-k8s-fleet-backend.md)). |

**Ariel** is a separate service that brings fleet notifications, commands and
conversation into chat. It uses Prospero only through the public HTTP + SSE API
described in this guide, typically with an `operate`-scoped
[API token](./api-auth.md). It does not depend on any Prospero crate. Ariel keeps
its own identity, channel configuration and audit data in gonzalo.

Ariel is released and running: the current release ships `/ariel status`,
`/ariel spawn`, `/ariel kill` and `/ariel respawn` for driving the fleet from
Discord, plus `/ariel channel`, `/ariel configure` and `/ariel invite` for
channel administration and onboarding, with every command authorized against the
person's role and the channel's ceiling and audited in gonzalo. It is distributed
as the container image `ghcr.io/caliban-ai/ariel` rather than on crates.io, and
it can reach a prosperod over `https` through an ingress as well as in-cluster.
See [Ariel's guide](https://caliban-ai.github.io/ariel/) and its
[releases](https://github.com/caliban-ai/ariel/releases) for the current state.

## Where to go next

- [Getting started](./getting-started.md): build and run `prosperod`, register a
  workspace, spawn an agent.
- [The `prospero` CLI](./cli.md): every command and flag.
- [Configuring prosperod](./configuration.md): flags, environment, storage
  topology, fleet backends.
- [HTTP API & events](./api.md): routes, payloads, the SSE stream and event
  kinds.
- [Securing the API](./api-auth.md): tokens, scopes, dashboard sessions.
- [Deploying the container](./deployment.md): the `ghcr.io/caliban-ai/prospero`
  image, including the k8s backend.
- [Guiding principles](./principles.md) and the [ADRs](./adr/index.md): the
  reasoning behind the design.
- The [API reference](./api/index.html): rustdoc for the crates.
