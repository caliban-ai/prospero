# prospero

Prospero is the **control plane for fleets of [caliban](https://github.com/caliban-ai/caliban)
agents**. You use it to launch, manage and observe many agents across many
workspaces, including several agents working in parallel on the same codebase.

It has three parts:

- **`prosperod`**, a long-running daemon that serves an HTTP/JSON API, Server-Sent
  Events streams and an embedded web dashboard on `127.0.0.1:7878` by default.
- **`prospero`**, an operator CLI. It is a thin HTTP client over that API.
- **The dashboard**, a Rust → WASM app served by `prosperod` at `/`.

The CLI, the dashboard and any other client all use the same public API.

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
  agents.
- **Observe.** Prospero turns caliban's stream output into a stable `FleetEvent`
  type. Events go out live over SSE and are also written to a durable store
  (sqlite standalone, Postgres clustered), so an agent's history survives after
  it finishes.

Prospero talks to caliban only through its wire format. It depends on no caliban
crate ([ADR 0003](./adr/0003-couple-to-caliban-via-ndjson-wire-format.md)).

## The caliban-ai ecosystem

| Project | Role |
|---|---|
| [caliban](https://github.com/caliban-ai/caliban) | The agent harness, plus `caliband`, the supervisor Prospero drives on the local backend. |
| **prospero** | This project: the fleet control plane. |
| [gonzalo](https://github.com/caliban-ai/gonzalo) | A shareable persistence layer for caliban. |
| [ariel](https://github.com/caliban-ai/ariel) | A chat bridge for the fleet: Discord first, then Slack and Teams. |
| caliban-operator | Reconciles `CalibanTask` resources into caliband pods for Prospero's k8s backend ([ADR 0008](./adr/0008-k8s-fleet-backend.md)). |

**Ariel** is a separate service that brings fleet notifications, commands and
conversation into chat. It uses Prospero only through the public HTTP + SSE API
described in this guide, typically with an `operate`-scoped
[API token](./api-auth.md). It does not depend on any Prospero crate. Ariel keeps
its own identity, channel configuration and audit data in gonzalo. Its design is
tracked in [prospero#67](https://github.com/caliban-ai/prospero/issues/67).

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
