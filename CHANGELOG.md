# Changelog

All notable changes to prospero are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the project is pre-1.0, the minor version is bumped for new features and
the patch version for fixes.

## [Unreleased]

## [0.3.2] - 2026-07-18

Two fixes to the `PROSPERO_FLEET=k8s` control plane found in live-cluster use:
spawning an agent no longer blocks the dashboard on the operator's reconcile,
and an agent's interactive reply box now appears under the k8s backend. Local
behavior is unchanged.

### Fixed

- **Spawning an agent no longer hangs the dashboard on reconcile.** Under the
  k8s backend, `K8sFleet::ensure_agent` applied the `CalibanTask` CR and then
  synchronously polled (up to ~30s) for `status.phase == "Running"` before
  returning, coupling the HTTP response to the full `CR → operator reconcile →
  pod schedule → Running` chain (and blocking the entire budget when the pod
  never started). It now returns as soon as the CR is admitted; the background
  watch loop surfaces the agent and attaches its session when it reaches
  `Running` — the synchronous poll was redundant with that path
  ([#157](https://github.com/caliban-ai/prospero/pull/157)).
- **The interactive reply box now appears for k8s agents.** The dashboard shows
  it only when an agent is `interactive` and `idle`, and under `PROSPERO_FLEET=k8s`
  neither was sourced correctly — the status/interactive fields are now read from
  the pod's caliband rather than the `CalibanTask` CR alone
  ([#130](https://github.com/caliban-ai/prospero/issues/130))
  ([#156](https://github.com/caliban-ai/prospero/pull/156)).

## [0.3.1] - 2026-07-14

Bug-fix follow-up to the 0.3.0 Kubernetes config plane, closing the four issues
surfaced in k8s smoke testing on a fresh 0.3.0 deploy. The `PROSPERO_FLEET=k8s`
fleet now stays `Ready` through a schema-skewed custom resource, surfaces the
registered `Workspace` CRs it manages (instead of a synthetic phantom), and
rejects an unregisterable workspace up front. Local behavior is unchanged.

### Fixed

- **A single un-deserializable `CalibanTask` no longer wedges the whole fleet.**
  `K8sFleet`'s watch/readiness path listed `CalibanTask`s strictly, so one CR
  that failed to deserialize (e.g. a stale task predating the now-required
  `workspaceRef` field) failed the entire poll — the fleet never populated,
  `/readyz` stuck at `503`, and the pod never became `Ready`. The list is now
  decoded per-item, skipping and logging the bad CRs
  ([#148](https://github.com/caliban-ai/prospero/issues/148))
  ([#152](https://github.com/caliban-ai/prospero/pull/152)).
- **The k8s fleet snapshot reconciles with the `Workspace` registry.**
  `GET /api/fleet` synthesized a single phantom `k8s` workspace and never read
  the registered `Workspace` CRs, so a registered workspace was invisible in the
  dashboard while the synthetic `k8s` entry reported `workspace not registered`.
  The snapshot now surfaces the registered `Workspace` CRs (agents grouped by the
  workspace they reference), so `/api/fleet` and `/api/workspaces` agree and a
  fresh deploy shows no phantom
  ([#149](https://github.com/caliban-ai/prospero/issues/149),
  [#151](https://github.com/caliban-ai/prospero/issues/151))
  ([#153](https://github.com/caliban-ai/prospero/pull/153)).
- **Add-workspace rejects an invalid workspace as `400`, not a raw apiserver
  `422`.** The dashboard's `+ add workspace` posted a `Workspace` with empty
  `providers`/`sources`, which the CRD (`minItems: 1` on both) rejected — so a
  workspace could never be registered from the dashboard. The config plane now
  validates at least one well-formed source and provider before apply (add and
  edit paths), and the form validates the same client-side
  ([#150](https://github.com/caliban-ai/prospero/issues/150))
  ([#154](https://github.com/caliban-ai/prospero/pull/154)).

## [0.3.0] - 2026-07-12

The **Kubernetes config plane**: deploying with `PROSPERO_FLEET=k8s` is now a
real control plane — create and configure workspaces, and launch provider-bound
agents, from the dashboard — instead of a read-only viewer that returned
`405 Method Not Allowed` on Save. Workspaces are first-class `Workspace` custom
resources reconciled by `caliban-operator`, and the dashboard is backend-aware.
Local behavior is unchanged.

### Added

- **Kubernetes config plane (core + API).** Under `PROSPERO_FLEET=k8s`,
  `K8sFleet` now wires a `FleetAdmin` over operator-owned `Workspace` custom
  resources, so `POST` / `PUT` / `DELETE` on `/api/workspaces` persist and
  manage real configuration — multi-source workspaces, a named-provider list,
  and per-provider credentials referenced by Kubernetes `Secret` name (prospero
  never reads the Secret) — instead of returning `405`. A backend-neutral
  `WorkspaceConfig` DTO lets one API serve both backends (local projects its
  single-provider subset, unchanged); `GET /api/workspaces` returns the real
  `Workspace` CRs with reconciliation status; async workspace writes answer
  `202 Accepted`; and a spawned agent binds a named provider via `providerRef`
  ([#142](https://github.com/caliban-ai/prospero/issues/142))
  ([#144](https://github.com/caliban-ai/prospero/pull/144),
  [#145](https://github.com/caliban-ai/prospero/pull/145)).
- **Backend-aware dashboard.** The dashboard fetches `GET /api/capabilities` and
  adapts. On k8s it renders a workspace editor (git sources + a named-provider
  list with `secretName` / `key` Secret references and a default marker),
  reconciliation status pills (`pending` / `reconciling` / `ready` / `failed`
  with the failure message on hover), and a launch-modal provider picker; on
  local it is byte-for-byte unchanged
  ([#143](https://github.com/caliban-ai/prospero/issues/143))
  ([#146](https://github.com/caliban-ai/prospero/pull/146)).
- **`GET /api/capabilities`** — a backend capability seam the dashboard gates its
  controls on ([#99](https://github.com/caliban-ai/prospero/issues/99))
  ([#101](https://github.com/caliban-ai/prospero/pull/101)).
- **Frontmatter / agent-template support through spawn** — a spawn can forward an
  agent-template markdown file to caliband's `SpawnSpec.frontmatter_path`
  ([#6](https://github.com/caliban-ai/prospero/issues/6))
  ([#102](https://github.com/caliban-ai/prospero/pull/102)).
- **Guiding Principles & Invariants** guide page synthesizing ADRs 0002–0009
  ([#74](https://github.com/caliban-ai/prospero/issues/74))
  ([#104](https://github.com/caliban-ai/prospero/pull/104)).

### Changed

- The `CalibanTask` CRD mirror moved from an inline `workspace` to a
  `workspaceRef` (plus an operator-pinned `status.resolvedWorkspace`), matching
  caliban-operator's frozen `v1alpha1` contract. Pre-v1; existing cluster CRs
  are recreated under the new schema.

## [0.2.0] - 2026-07-11

Kubernetes high-availability, a reworked dashboard, and a full QA sweep. A
second QA pass over the real `prospero`/`caliband` stack filed 23 findings; all
are fixed here, alongside first-class leader election for the k8s fleet backend
and a new agent-timeline dashboard.

### Added

- **Leader election + attach lifecycle for the `K8sFleet` backend.** The
  session-plane attach — the one path that writes an agent's events to the shared
  store/bus — is now gated on a per-agent ownership lease, so with 2+ `prosperod`
  replicas exactly one replica owns, attaches to, and emits each agent (no more
  duplicate SSE events or racing per-stream `seq` allocation). Standalone is
  unchanged (`SelfOwnsAll`); a clustered deploy builds a `LeasedOwnership` lease
  plus heartbeat. Attach tasks are now promptly torn down on stop/remove/restart,
  and any agent observed `Running` — including operator- or peer-created ones —
  is streamed by the lease owner
  ([#108](https://github.com/caliban-ai/prospero/issues/108),
  [#112](https://github.com/caliban-ai/prospero/issues/112),
  [#113](https://github.com/caliban-ai/prospero/issues/113))
  ([#138](https://github.com/caliban-ai/prospero/pull/138)).
- **Dashboard agent timeline, tool-call inspector, and run header** — a folded
  event timeline with expandable tool-call segments and a per-run turns/outcome
  header ([#5](https://github.com/caliban-ai/prospero/issues/5))
  ([#96](https://github.com/caliban-ai/prospero/pull/96)).
- **`prospero-types` crate** — the normalized `FleetEvent`/model DTOs extracted
  into a small, wasm-compatible serde-only crate the WASM dashboard can share
  ([#98](https://github.com/caliban-ai/prospero/issues/98))
  ([#100](https://github.com/caliban-ai/prospero/pull/100)).

### Changed

- Under `PROSPERO_FLEET=k8s`, `prosperod` no longer builds a local
  `FleetManager`/poll loop; the k8s backend serves directly over the shared
  store/bus ([#83](https://github.com/caliban-ai/prospero/issues/83))
  ([#92](https://github.com/caliban-ai/prospero/pull/92)).
- `/readyz` now reports `workspaces_total`/`workspaces_healthy`/
  `workspaces_unreachable` (was `repos_*`), and user-facing error wording says
  "workspace" not "repo", matching the vocabulary used everywhere else
  ([#116](https://github.com/caliban-ai/prospero/issues/116),
  [#117](https://github.com/caliban-ai/prospero/issues/117))
  ([#135](https://github.com/caliban-ai/prospero/pull/135)).

### Fixed

- **Dashboard.** Terminal-agent SSE streams no longer reconnect-storm into an
  unbounded, duplicated timeline with runaway memory
  ([#105](https://github.com/caliban-ai/prospero/issues/105))
  ([#128](https://github.com/caliban-ai/prospero/pull/128)); tool calls resolve
  `ok`/`fail` instead of showing "running" forever (paired by `tool_use_id`)
  ([#106](https://github.com/caliban-ai/prospero/issues/106))
  ([#131](https://github.com/caliban-ai/prospero/pull/131)); the fleet summary
  shows the workspace count, the misleading `$0.0000` cost is gone, and a favicon
  is served ([#115](https://github.com/caliban-ai/prospero/issues/115),
  [#109](https://github.com/caliban-ai/prospero/issues/109),
  [#119](https://github.com/caliban-ai/prospero/issues/119))
  ([#134](https://github.com/caliban-ai/prospero/pull/134)).
- **API.** Duplicate workspace registration returns `409 Conflict`, not a
  misleading `503` ([#111](https://github.com/caliban-ai/prospero/issues/111))
  ([#139](https://github.com/caliban-ai/prospero/pull/139)); an unknown agent's
  events endpoint returns `404` instead of `200 []`
  ([#118](https://github.com/caliban-ai/prospero/issues/118))
  ([#135](https://github.com/caliban-ai/prospero/pull/135)); `api_key_from_env`
  on a keyless provider is rejected at config-set time, and `rm` no longer races
  a just-spawned agent or lags the fleet view
  ([#120](https://github.com/caliban-ai/prospero/issues/120),
  [#122](https://github.com/caliban-ai/prospero/issues/122),
  [#123](https://github.com/caliban-ai/prospero/issues/123))
  ([#137](https://github.com/caliban-ai/prospero/pull/137)).
- **k8s hardening.** The session-plane bearer token is never sent over plaintext
  ([#107](https://github.com/caliban-ai/prospero/issues/107))
  ([#133](https://github.com/caliban-ai/prospero/pull/133)); unrecognized
  `CalibanTask` phases map to a terminal state, `calibandEndpoint` is validated,
  lock poisoning can't wedge the fleet view, the token compare is constant-time,
  and `--fleet-backend k8s` on a non-k8s build fails before any side effects
  ([#114](https://github.com/caliban-ai/prospero/issues/114),
  [#121](https://github.com/caliban-ai/prospero/issues/121),
  [#125](https://github.com/caliban-ai/prospero/issues/125),
  [#126](https://github.com/caliban-ai/prospero/issues/126),
  [#127](https://github.com/caliban-ai/prospero/issues/127))
  ([#136](https://github.com/caliban-ai/prospero/pull/136)).
- **Tests.** De-flaked the `distributed_bus` PG suite under parallel shared-DB
  load ([#110](https://github.com/caliban-ai/prospero/issues/110))
  ([#129](https://github.com/caliban-ai/prospero/pull/129)) and
  `cli_drives_the_full_stack` ([#85](https://github.com/caliban-ai/prospero/issues/85))
  ([#94](https://github.com/caliban-ai/prospero/pull/94)).

## [0.1.1] - 2026-07-05

### Fixed

- The released image now builds `prosperod` with `--features k8s`, so the
  `K8sFleet` backend is compiled in and `PROSPERO_FLEET=k8s` works. Previously the
  image only ran the local backend, so an in-cluster deploy showed an empty fleet
  ([#90](https://github.com/caliban-ai/prospero/issues/90)). Unblocks the
  k8s-fleet-backend support in the prospero Helm chart.

## [0.1.0] - 2026-07-04

Initial containerized and licensed release of the **prospero** control plane —
the agent orchestration layer that sits above many `caliband` supervisors — as
part of the P0 Kubernetes deployment (epic
[caliban-ai/caliban#274](https://github.com/caliban-ai/caliban/issues/274)).

### Added

- `ghcr.io/caliban-ai/prospero:0.1.0` — multi-arch (linux/amd64 + linux/arm64),
  non-root container image running `prosperod` (REST + SSE + dashboard on 7878);
  also tagged `:latest` and `:sha-<commit>`.
- Helm chart `charts/prospero` in
  [caliban-ai/helm-charts](https://github.com/caliban-ai/helm-charts), rendering
  **standalone** (SQLite + PVC) or **clustered** (external Postgres, N replicas)
  from one `topology` value.

### Changed

- Repository relicensed to **AGPL-3.0-only**, matching its sibling projects.

[Unreleased]: https://github.com/caliban-ai/prospero/compare/v0.3.2...HEAD
[0.3.2]: https://github.com/caliban-ai/prospero/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/caliban-ai/prospero/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/caliban-ai/prospero/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/caliban-ai/prospero/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/caliban-ai/prospero/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/caliban-ai/prospero/releases/tag/v0.1.0
