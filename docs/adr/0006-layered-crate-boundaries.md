# ADR 0006 · Layered crate boundaries: cli/daemon → api → core

- **Status:** accepted
- **Date:** 2026-06-05
- **Source:** [`docs/superpowers/specs/2026-06-05-prospero-framework-design.md`](../superpowers/specs/2026-06-05-prospero-framework-design.md) §4

## Context

Prospero is a CLI, a long-running daemon, an HTTP/SSE API, and an orchestration engine. Put
in one crate, the web framework, transport, and process concerns would bleed into the domain
logic, and tests of the core engine would drag in axum and a running server.

We need crate boundaries that keep the orchestration brain independent of how it is exposed.

## Decision

Split the workspace into four crates with **one-directional** dependencies:

```
prospero-cli (prospero)  ─┐
                          ├─▶ prospero-api ─▶ prospero-core
prospero-daemon (prosperod)┘
```

- **`prospero-core`** — the orchestration brain: domain model, `CalibandClient`, discovery,
  registry, `Store`, `FleetManager`. **No web framework in its public API.**
- **`prospero-api`** — an axum adapter (REST + SSE + dashboard assets) over `FleetManager`;
  depends only on `core`.
- **`prospero-daemon`** (`prosperod`) — process entry: owns the tokio runtime, config,
  logging, shutdown; wires `core` + `api` into a server.
- **`prospero-cli`** (`prospero`) — a thin client that talks to `prosperod` over **HTTP**,
  not a second protocol. Nothing depends on the daemon.

## Consequences

- **Positive:** the core engine is testable with no HTTP server and no web types in scope,
  and `api` is testable in-process over a fake-backed `FleetManager`. One control surface, not
  two: the CLI and the dashboard both go through the HTTP API, so there is a single place where
  control/observe semantics live. The acyclic, one-way dependency graph keeps responsibilities
  from leaking upward (e.g. transport concerns can't seep into the domain model) and makes the
  boundaries easy to reason about as the system grows.
- **Negative:** four crates plus the one-way rule impose a structure cost — types that span
  layers must be placed deliberately, and an in-process call becomes an HTTP round-trip for the
  CLI rather than a direct function call.
- **Revisit if:** the crate split adds more ceremony than it prevents leakage (e.g. constant
  re-exports across boundaries), or a client genuinely needs a control path that HTTP can't
  serve well — either would pressure the layering.
