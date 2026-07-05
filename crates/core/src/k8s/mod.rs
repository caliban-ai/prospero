//! Kubernetes `FleetProvider` backend (`K8sFleet`), gated behind the `k8s`
//! cargo feature so `LocalFleet`-only builds pull no `kube`/`k8s-openapi`.
//!
//! See [ADR 0008](../../../../docs/adr/0008-k8s-fleet-backend.md).

pub mod crd;
pub mod fleet;
