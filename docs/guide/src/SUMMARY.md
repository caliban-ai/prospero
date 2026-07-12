# Summary

[prospero](./introduction.md)

# Changelog

- [Changelog](./changelog.md)

# Design

- [Guiding Principles & Invariants](./principles.md)

# Architecture Decisions

- [ADR Index](./adr/index.md)
<!-- adrs -->
  - [ADR 0001 · Record architecture decisions](./adr/0001-record-architecture-decisions.md)
  - [ADR 0002 · Prospero is a control plane over caliband, not a re-implementation](./adr/0002-control-plane-over-caliband.md)
  - [ADR 0003 · Couple to caliban only through its NDJSON wire format](./adr/0003-couple-to-caliban-via-ndjson-wire-format.md)
  - [ADR 0004 · Hybrid live + durable observability behind a `Store` trait](./adr/0004-hybrid-live-and-durable-observability.md)
  - [ADR 0005 · Worktree isolation by default for agent spawns](./adr/0005-worktree-isolation-by-default-for-spawns.md)
  - [ADR 0006 · Layered crate boundaries: cli/daemon → api → core](./adr/0006-layered-crate-boundaries.md)
  - [ADR 0007 · Test the control plane against an in-process fake caliban](./adr/0007-fake-caliban-test-harness.md)
