# ADR 0003 · Couple to caliban only through its NDJSON wire format

- **Status:** accepted
- **Date:** 2026-06-05
- **Source:** [`docs/superpowers/specs/2026-06-05-prospero-framework-design.md`](../superpowers/specs/2026-06-05-prospero-framework-design.md) §3, §4

## Context

Prospero needs to talk to `caliband`: send control requests (`List`, `Spawn`, `Attach`,
`Kill`, …) and read per-agent `stream-json` frames. Caliban implements this in Rust crates
(e.g. `caliban-supervisor`) that Prospero could depend on directly to reuse the request,
reply, and `SpawnSpec` types.

Depending on caliban's crates would tie Prospero to caliban's internal Rust API, version
cadence, and transitive dependencies — coupling far wider than the bytes actually exchanged
on the socket.

## Decision

The **caliband wire format is the only contract**. Prospero owns a **thin NDJSON client**
in `prospero-core` (`CalibandClient`) with its own mirrored serde types
(`CtlRequest` / `CtlReply` / `AgentRecord` / `SpawnSpec`) and newline-delimited framing over
`tokio::net::UnixStream`. Prospero does **not** depend on `caliban-supervisor` or any other
caliban crate.

## Consequences

- **Positive:** Prospero and caliban evolve independently; the only thing that must stay
  compatible is the bytes on the wire, which is also the surface real deployments depend on.
  Because the coupling is just a socket protocol, the entire control plane can be tested
  against a fake that speaks the same protocol — see
  [0007](0007-fake-caliban-test-harness.md). Unknown/forward-compatible frames are tolerated
  by the normalizer (skip-and-log), so caliban can add frame types without breaking Prospero.
- **Negative:** the wire types are mirrored, so a protocol change in caliban requires a
  corresponding edit in Prospero's client — an intentional, explicit seam rather than a
  silent transitive break, but a seam someone must remember to keep in sync.
- **Revisit if:** the wire protocol churns fast enough that hand-mirroring the types becomes
  a recurring source of drift — a generated client or a shared schema crate might then earn
  its coupling cost.
