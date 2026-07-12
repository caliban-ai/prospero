# Guiding Principles & Invariants

Prospero's design philosophy is recorded in the [architecture decisions](./adr/index.md).
This page **synthesizes** those decisions into the guiding principles, the
inviolable invariants, and the scale-out roadmap — the *why* behind the code, in
one place. It complements the ADR log; it does not replace it. Every item cites
the ADR(s) it derives from; on supersession, keep this page in sync.

> **Scoping note.** The unit a `caliband` manages is a **workspace of 1..N repo
> sources**, not a single repo. *Per-repo is only today's implementation*
> (caliband identity = `hash(repo_root)`), being generalized in lockstep by
> caliban [#281](https://github.com/caliban-ai/caliban/issues/281) (supervisor)
> and prospero [#72](https://github.com/caliban-ai/prospero/issues/72)
> (discovery). Read "per-repo" phrasings below as transitional.

## Guiding principles

1. **Control plane, not re-implementation.** Prospero owns fleet *lifecycle*
   (spawn / kill / respawn / attach) and adds only the fleet-wide concerns
   caliban lacks — it never re-implements the agent runtime. The *mechanism*
   sits behind the `FleetProvider` trait: **Local** drives an existing caliband
   over the wire; **K8s** realizes the same verbs declaratively as CRUD on
   `CalibanTask` custom resources.
   ([ADR 0002](./adr/0002-control-plane-over-caliband.md),
   [ADR 0008](./adr/0008-k8s-fleet-backend.md))

2. **Couple through the wire, nothing else.** Caliban's NDJSON wire format (and,
   for k8s, the `CalibanTask` CRD's serialized form) is the *only* contract.
   Prospero mirrors the serde types and depends on **no caliban crate** — so the
   two projects evolve independently and integration breakage surfaces as a
   data-shape test, not a compile error.
   ([ADR 0003](./adr/0003-couple-to-caliban-via-ndjson-wire-format.md))

3. **Hybrid observability: live + durable, unified.** Status comes from polling
   `List`; detail comes from attach-on-demand; and every normalized event is
   appended to durable storage, so an agent's history survives after it finishes.
   Live and durable views are one read path.
   ([ADR 0004](./adr/0004-hybrid-live-and-durable-observability.md))

4. **Normalize caliban's frames into a stable internal type.** Raw `stream-json`
   frames become `FleetEvent` / `FleetSnapshot`; consumers never see raw frames.
   The normalizer is forward-compatible — an unknown frame is skipped and logged,
   never fatal.
   ([ADR 0003](./adr/0003-couple-to-caliban-via-ndjson-wire-format.md),
   [ADR 0004](./adr/0004-hybrid-live-and-durable-observability.md))

5. **Safe-by-default isolation.** Worktree isolation is the default for *every*
   spawn; sharing the working tree is an explicit opt-out (`--shared-tree`).
   Isolation stays per-source even as scoping moves to the workspace.
   ([ADR 0005](./adr/0005-worktree-isolation-by-default-for-spawns.md))

6. **Enforce policy at the boundary, in one place.** Defaults like worktree
   isolation are set at the API boundary, so every client (CLI, dashboard,
   future callers) inherits the same policy from a single control surface.
   ([ADR 0005](./adr/0005-worktree-isolation-by-default-for-spawns.md),
   [ADR 0006](./adr/0006-layered-crate-boundaries.md))

7. **Layered, one-directional crate boundaries.** `cli` / `daemon` → `api` →
   `core`, acyclic; no web framework leaks into `core`'s public API. Shared
   read-model DTOs live in a wasm-compatible leaf crate so the (native) server
   and a (WASM) dashboard cannot drift.
   ([ADR 0006](./adr/0006-layered-crate-boundaries.md))

8. **Abstraction behind traits for deferred evolution.** Persistence sits behind
   a `Store` trait (realized as jsonl / sqlite / Postgres); fleet control sits
   behind `FleetProvider` (Local / K8s). Traits are the vehicle for scale-out and
   workspace scoping without touching call sites.
   ([ADR 0004](./adr/0004-hybrid-live-and-durable-observability.md),
   [ADR 0008](./adr/0008-k8s-fleet-backend.md))

## Inviolable invariants

These hold across every backend and topology; a change that breaks one is a
design change, not a refactor.

- **No caliban crate dependency.** The wire format / CRD serialized form is the
  sole coupling. ([ADR 0003](./adr/0003-couple-to-caliban-via-ndjson-wire-format.md))
- **Acyclic layers.** Dependencies flow one way — `cli`/`daemon` → `api` →
  `core` — and `core`'s public API names no web framework.
  ([ADR 0006](./adr/0006-layered-crate-boundaries.md))
- **Durability before divergence.** A normalized event reaches durable history;
  if an append fails, that gap is itself recorded so live and durable views never
  silently disagree. ([ADR 0004](./adr/0004-hybrid-live-and-durable-observability.md))
- **Unknown frames never crash.** The normalizer tolerates unrecognized frames by
  skip-and-log. ([ADR 0003](./adr/0003-couple-to-caliban-via-ndjson-wire-format.md))
- **Isolation is the default, opt-out is explicit.** No spawn shares the working
  tree unless a caller says so. ([ADR 0005](./adr/0005-worktree-isolation-by-default-for-spawns.md))
- **Backends are interchangeable behind the trait.** Local and K8s implement the
  same `FleetProvider` verbs and emit to the same observability plane; the API
  request path is backend-agnostic.
  ([ADR 0002](./adr/0002-control-plane-over-caliband.md),
  [ADR 0008](./adr/0008-k8s-fleet-backend.md))
- **The fake is a faithful double.** The control plane is testable end-to-end
  against an in-process fake caliban, so backends are correct by construction,
  not by hope. ([ADR 0007](./adr/0007-fake-caliban-test-harness.md))

## Scale-out roadmap

The trait seams above exist so prospero can grow along three independent axes
without disturbing the request path.

- **Backend: Local → K8s.** `LocalFleet` drives a caliband over Unix sockets;
  `K8sFleet` realizes the same `FleetProvider` verbs as CRUD + watch on
  `CalibanTask` CRs, which the caliban-operator reconciles into sandboxed
  caliband pods. Both emit to the shared `Store`/`EventBus`.
  ([ADR 0008](./adr/0008-k8s-fleet-backend.md))
- **Scope: per-repo → workspace.** A caliband manages a *workspace* of 1..N
  source checkouts; today's per-repo identity (`hash(repo_root)`) is being
  generalized in lockstep with caliban #281 and prospero #72.
- **Topology: standalone → clustered.** Standalone runs sqlite + an in-process
  bus + self-owned streams; clustered runs a Postgres store/config, a
  LISTEN/NOTIFY event bus, and leased stream ownership so replicas fail over
  without double-writing. Both sit behind the `Store` / `EventBus` / `Ownership`
  seams. ([ADR 0004](./adr/0004-hybrid-live-and-durable-observability.md))

## License

Prospero is licensed AGPL-3.0-only.
([ADR 0009](./adr/0009-agpl-3.0-only-license.md))
