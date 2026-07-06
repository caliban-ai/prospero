# Changelog

All notable changes to prospero are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the project is pre-1.0, the minor version is bumped for new features and
the patch version for fixes.

## [Unreleased]

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

[Unreleased]: https://github.com/caliban-ai/prospero/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/caliban-ai/prospero/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/caliban-ai/prospero/releases/tag/v0.1.0
