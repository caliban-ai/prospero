# ADR 0002 · Prospero is a control plane over caliband, not a re-implementation

- **Status:** accepted
- **Date:** 2026-06-05
- **Source:** [`docs/superpowers/specs/2026-06-05-prospero-framework-design.md`](../superpowers/specs/2026-06-05-prospero-framework-design.md) §1

## Context

Caliban already ships `caliband`, a per-repo supervisor daemon that spawns, lists, kills,
respawns, and attaches to background agents over a Unix-socket NDJSON protocol. Prospero's
job is to launch, manage, and observe **many** agents across **many** repositories at once.

We could either (a) re-implement process supervision ourselves to own the full stack, or
(b) build a layer *above* the existing calibands that delegates supervision to them and
adds only the fleet-wide concerns caliban lacks.

## Decision

Prospero is a **control plane**. It discovers and drives the existing per-repo `caliband`
daemons and does **not** re-implement process supervision. Prospero sits above many
calibands and adds what they individually lack:

- a fleet-wide model aggregating agents across repos and hosts,
- durable run **history** (caliband exposes only live state),
- a **normalized** event type independent of caliban's wire frames,
- the observability/control surfaces: CLI, HTTP/JSON API, SSE, and a minimal dashboard.

## Consequences

- **Positive:** we avoid duplicating — and diverging from — caliban's supervision logic;
  spawn/kill/respawn/attach semantics stay defined in one place. Prospero's value is
  concentrated on the genuinely new concerns (fleet aggregation, history, normalization)
  rather than re-litigating process management.
- **Negative:** Prospero depends on a running `caliband` per managed repo and inherits its
  behavior and limitations (Discovery can autostart a caliband on demand to soften this),
  and the boundary between the two systems must be defined precisely — see
  [0003](0003-couple-to-caliban-via-ndjson-wire-format.md).
- **Revisit if:** caliban's supervision proves too limited for fleet needs, or a concern we
  treat as fleet-wide turns out to belong inside the per-repo daemon — either would move the
  control-plane / supervisor boundary.
