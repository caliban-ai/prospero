# 0007. Test the control plane against an in-process fake caliban

- **Status:** Accepted
- **Date:** 2026-06-05
- **Deciders:** Prospero maintainers
- **Source:** [`docs/superpowers/specs/2026-06-05-prospero-framework-design.md`](../superpowers/specs/2026-06-05-prospero-framework-design.md) §7

## Context

End-to-end testing of Prospero would normally require a real `caliband`, real agents, API
keys, and live LLM calls — slow, non-deterministic, expensive, and awkward in CI. Yet the
behavior worth testing (spawn defaults, poll reconciliation, attach/normalize, replay-then-
tail, resilience to dropped streams and refused sockets) is exactly the control-plane logic
that sits between Prospero and caliban.

Because the **only coupling to caliban is the NDJSON wire format**
([0003](0003-couple-to-caliban-via-ndjson-wire-format.md)), we can substitute anything that
speaks that protocol.

## Decision

Build an **in-process fake caliban** as the cornerstone of the test strategy: a harness
(shipped in `prospero-core` behind a `testkit` feature) that listens on a real Unix socket
and speaks the same NDJSON control + per-agent stream protocol as the real daemon. Tests
drive the **real** `FleetManager` / `CalibandClient` against this fake.

This enables deterministic, end-to-end testing of the whole control plane — including the
CLI-through-HTTP path — with **no real caliban, no API keys, and no LLM calls**.

## Consequences

- The full stack is tested fast and deterministically in CI; scripted frames make event
  sequences, resilience cases, and `seq` recovery reproducible.
- The fake must track the wire protocol; if caliban's protocol drifts, the fake and the
  mirrored client must be updated together — the same explicit seam noted in
  [0003](0003-couple-to-caliban-via-ndjson-wire-format.md).
- Tests against a *real* caliban binary + live model remain **out of scope** for the first
  stab (manual / CI-gated later); the fake covers the wire contract, not caliban's own
  correctness.
- Pure units (framing, normalizer, discovery resolution, store, reconciliation) are tested
  directly; the fake is reserved for integration-level behavior that needs the protocol.
