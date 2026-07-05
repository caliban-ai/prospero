# Architecture Decision Records

This directory holds Prospero's **Architecture Decision Records (ADRs)** — short
documents that capture a single significant architectural decision, the context that
forced it, and the consequences we accepted by making it. ADRs use the
[MADR-lite](https://adr.github.io/madr/) format, matching the sibling caliban and gonzalo
repos so the records read the same across all three.

An ADR is not a design doc. Design docs (under `docs/superpowers/`) explore a whole
feature; an ADR records *one decision* and the trade-off behind it, so that months
later we can see **why** a choice was made without reverse-engineering it from the code.

## When to write one

Write an ADR when a decision is **architecturally significant** — it is costly to
reverse, it constrains future work, or a newcomer would reasonably ask "why is it done
this way?". Examples: choosing a coupling boundary, a persistence model, a concurrency
strategy, a public crate boundary, or a testing approach the rest of the code leans on.

Routine choices (naming, a local refactor, picking an obvious library) do **not** need
an ADR.

## File convention

```
docs/adr/####-topic.md
```

- `####` — a zero-padded, **monotonically increasing** sequence number (`0001`, `0002`, …).
  The number is permanent; never renumber an existing ADR.
- `topic` — a short kebab-case slug describing the decision
  (e.g. `couple-to-caliban-via-ndjson-wire-format`).

Numbers are assigned in order; to find the next one, look at the highest existing file.

## Lifecycle

Every ADR carries a lowercase **Status**:

- **proposed** — under discussion, not yet adopted.
- **accepted** — the decision is in force.
- **deprecated** — no longer recommended, but not actively replaced.
- **superseded by [####](####-topic.md)** — replaced by a later ADR.

ADRs are **immutable once accepted**. To change a decision, write a *new* ADR that
supersedes the old one, and update the old one's status to
`superseded by ####`. This preserves the decision history rather than rewriting it.

## How to add one

1. Copy [`template.md`](template.md) to `docs/adr/####-topic.md` using the next number
   (or copy an existing ADR — they all follow the same MADR-lite shape).
2. Fill in Context, Decision, and Consequences. Keep it short — one screen is ideal.
   Structure Consequences as **Positive** / **Negative** / **Revisit if** bullets — the
   `Revisit if` line names what would make the decision worth reopening.
3. Set Status to `proposed` (if still under discussion) or `accepted` (if already in force).
4. If it replaces an earlier ADR, mark that one `superseded by ####`.

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | accepted |
| [0002](0002-control-plane-over-caliband.md) | Prospero is a control plane over caliband, not a re-implementation | accepted |
| [0003](0003-couple-to-caliban-via-ndjson-wire-format.md) | Couple to caliban only through its NDJSON wire format | accepted |
| [0004](0004-hybrid-live-and-durable-observability.md) | Hybrid live + durable observability behind a `Store` trait | accepted |
| [0005](0005-worktree-isolation-by-default-for-spawns.md) | Worktree isolation by default for agent spawns | accepted |
| [0006](0006-layered-crate-boundaries.md) | Layered crate boundaries: cli/daemon → api → core | accepted |
| [0007](0007-fake-caliban-test-harness.md) | Test the control plane against an in-process fake caliban | accepted |
| [0008](0008-k8s-fleet-backend.md) | `K8sFleet` — a Kubernetes `FleetProvider` backend (CalibanTask CRs; session plane over #71/#75 transport) | accepted |
| [0009](0009-agpl-3.0-only-license.md) | License prospero under AGPL-3.0-only | accepted |
