//! Kubernetes `FleetProvider` backend (`K8sFleet`), gated behind the `k8s`
//! cargo feature so `LocalFleet`-only builds pull no `kube`/`k8s-openapi`.
//!
//! See [ADR 0008](../../../../docs/adr/0008-k8s-fleet-backend.md).

pub mod crd;
/// In-memory `CalibanTaskApi` + `FakeBackend` double for `K8sFleet`, used by
/// Task B5's `k8s_fleet_satisfies_conformance` test (and available to other
/// crates via `testkit`). No real apiserver involved.
#[cfg(any(test, feature = "testkit"))]
pub mod fake;
pub mod fleet;
