# ADR 0008 · `K8sFleet` — a Kubernetes `FleetProvider` backend

- **Status:** accepted
- **Date:** 2026-07-04
- **Source:** k8s system-design spec (§"prospero changes", §"The two planes") in the caliban-ai docs hub · prospero [#64](https://github.com/caliban-ai/prospero/issues/64) · epic [caliban#274](https://github.com/caliban-ai/caliban/issues/274) · builds on prospero [#71/#75](https://github.com/caliban-ai/prospero/pull/75) (caliband network transport) · relates to [0003](0003-couple-to-caliban-via-ndjson-wire-format.md), [0006](0006-layered-crate-boundaries.md), [0007](0007-fake-caliban-test-harness.md)

## Context

[ADR 0006](0006-layered-crate-boundaries.md) put fleet control behind the
`FleetProvider` trait (prospero #63); `LocalFleet` (caliband-over-Unix) is the only
backend. The k8s epic needs a second backend, **`K8sFleet`**, that drives a fleet by
CRUD + watch on **`CalibanTask`** custom resources — the caliban-operator (caliban
#283) reconciles each `CalibanTask` into a sandboxed caliband pod exposing a stable
DNS endpoint — and connects the live session plane to that pod over the network.

The **network transport this needs already landed** in prospero #71/#75
(`caliband/transport.rs`): `CalibandClient` can now dial a caliband over TCP + rustls
TLS + a bearer-token preamble (`connect_tcp`), `spawn`/`attach` return an
`Endpoint`, and `AgentHandle.endpoint: Endpoint` carries a Unix path or a
`host:port`. So `K8sFleet` composes an existing transport; it does not build one.

What remains for `K8sFleet`:

- a **client-side `CalibanTask` type** and a `kube` client to CRUD/watch it;
- the four `FleetProvider` methods mapped onto CR operations;
- a **session-plane bridge** that dials each agent's `Endpoint::Tcp` (Sandbox DNS)
  over #75's transport and feeds prospero's existing event bus + store, so the
  dashboard/SSE work unchanged;
- reuse of the [0007](0007-fake-caliban-test-harness.md) conformance suite, which is
  Unix-`FakeCaliband`-coupled and must be generalized to a fake backend.

## Decision

1. **Mirror a minimal `CalibanTask` type; do not depend on the caliban-operator crate.** Per [ADR 0003](0003-couple-to-caliban-via-ndjson-wire-format.md)'s "couple only through the wire" principle (here, the CRD's serialized form), declare a minimal `kube::CustomResource` (`caliban.caliban-ai.dev/v1alpha1`) in `prospero-core` carrying only the fields `K8sFleet` sets (`workspace.sources`, `task.prompt`, optional `isolation`) and reads (`status.phase`, `status.calibandEndpoint`, `status.sandboxRef`). A golden test pins it against a sample CR. The operator's CRD is the source of truth; the mirror is kept minimal to limit drift.

2. **`K8sFleet` implements `FleetProvider` over `CalibanTask` CRs.** New `prospero-core` module behind a `k8s` cargo feature (so `LocalFleet`-only builds pull no `kube`).
   - `ensure_agent(spec)` → server-side-apply a `CalibanTask` (deterministic name from a hash of the spec, so it is idempotent); await `status.phase = Running` + `status.calibandEndpoint`; return `AgentHandle { endpoint: Endpoint::Tcp(calibandEndpoint) }`.
   - `watch_fleet()` → a `kube::runtime::watcher` on `CalibanTask` → translate applied/deleted + `phase` transitions into `FleetChange::{Discovered,StatusChanged,Gone}` (map `Phase` → `AgentStatus`), seeded by an initial list.
   - `stop_agent(id, drain)` → delete the `CalibanTask` (the operator's owner-ref GC tears down the Sandbox); `Graceful` best-effort awaits deletion within the timeout.
   - `restart_agent(id)` → delete + re-apply (fresh name → fresh id).

3. **The session plane dials the agent `Endpoint` over #75's transport and feeds the existing bus + store.** `K8sFleet` carries its own attach task built on `CalibandClient::connect_tcp` + the shared `stream` normalizer + `Emitter`, so `/stream` SSE and history work unchanged (they read the bus/store, never a socket — [ADR 0004](0004-hybrid-live-and-durable-observability.md)). TLS root + bearer token come from operator-injected config (env/Secret; Sandbox DNS is the host). The attach-loop core is refactored out of `FleetManager` into a provider-agnostic helper if cheaper than duplicating.

4. **Generalize the conformance suite behind a `FakeBackend` trait.** Replace `fleet_provider_conformance(provider, fake: &FakeCaliband)` with `(provider, backend: &dyn FakeBackend)` where `FakeBackend { received_any_spec(); simulate_reap(id) }`. `FakeCaliband` implements it trivially (its existing `received_specs`/`remove_agent`); a new in-memory `FakeK8s` implements it for `K8sFleet`. `LocalFleet`'s existing conformance run is unchanged. This keeps [ADR 0007](0007-fake-caliban-test-harness.md)'s "test the control plane against a fake" property for both backends.

5. **Backend selection at the daemon edge.** `prosperod` chooses `LocalFleet` vs `K8sFleet` by config/env (mirroring caliban's `--database-url` topology switch — e.g. `PROSPERO_FLEET=local|k8s` + namespace/kubeconfig). The API layer's remaining direct `FleetManager` calls (kill/respawn/steer/snapshot) are an [ADR 0006](0006-layered-crate-boundaries.md) P1 limitation tracked separately; `K8sFleet` MVP wires the four provider methods + the session plane.

## Consequences

- **prospero gains a Kubernetes fleet backend** — `kubectl`-less fleet control via `CalibanTask` CRs, with live streaming over the pod network. This completes the epic's "two planes" for the k8s path (declarative CRs + real-time session over #75's transport).
- **prospero takes its first `kube`/`k8s-openapi` dependency**, scoped to `prospero-core` behind the `k8s` feature; `LocalFleet` builds and runs with no cluster.
- **A second mirrored seam** (the `CalibanTask` type vs the operator's CRD) joins the wire mirror of ADR 0003 — the same manual-sync tradeoff, kept minimal and golden-pinned.
- **The conformance suite becomes backend-agnostic**, so future backends (remote — prospero #1) reuse it for free.
- **Deferred:** gRPC (caliban #314); rerouting the API's direct `FleetManager` calls through the provider seam; warm pools / multi-tenant (epic P4). The finalizer-drain / checkpoint pairing waits on caliban checkpoint gRPC.
