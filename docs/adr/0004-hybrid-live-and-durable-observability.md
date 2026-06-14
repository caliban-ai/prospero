# 0004. Hybrid live + durable observability behind a `Store` trait

- **Status:** Accepted
- **Date:** 2026-06-05
- **Deciders:** Prospero maintainers
- **Source:** [`docs/superpowers/specs/2026-06-05-prospero-framework-design.md`](../superpowers/specs/2026-06-05-prospero-framework-design.md) §1, §4, §5

## Context

Caliban exposes only **live** state: it can `List` agents and stream a `stream-json` tail
while an agent is active, but it keeps no history — once an agent finishes, its story is
gone. Prospero must show both a cheap fleet-wide status overview and a per-agent detail
stream, and that detail must survive after the agent (and caliband's memory of it) is gone.

Options ranged from pure polling (cheap, but no streaming detail and no history) to
attaching to every agent's stream continuously (rich, but expensive and still no history).

## Decision

Adopt a **hybrid** model:

- **Poll** each caliband's `List` on an interval for cheap, fleet-wide status reconciliation.
- **Attach** to a per-agent stream **on demand** — while the agent is active or a client is
  watching — and stop when it is terminal and unwatched (work stays proportional to
  active + watched agents).
- Normalize both onto an in-memory `FleetSnapshot` and a normalized `FleetEvent` type, and
  also **append every event to durable storage** behind a `Store` trait. The first
  implementation is `JsonlStore` (append-only JSONL log + registry persistence).

A client that starts watching gets **replay** from the `Store` then a **live tail** from the
broadcast bus, joined on a monotonic `seq` — so "observe" means live + history, unified,
and the full story persists on disk even after caliban forgets the agent.

## Consequences

- Prospero fills caliban's history gap: runs are durable and replayable; `seq` survives
  prosperod restarts.
- Streaming cost is bounded to active/watched agents rather than the whole fleet.
- Putting persistence behind a `Store` trait keeps the door open for a sqlite backend later
  without touching the rest of the system; `JsonlStore` is deliberately the simple first step.
- Durability is best-effort relative to liveness: a failed `Store.append` is logged and
  metered but does not stop live SSE — we favor a never-down fleet view over guaranteed
  persistence (first stab). Log retention/rotation is deferred.
