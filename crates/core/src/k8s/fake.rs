//! `FakeK8s` — an in-memory `CalibanTask` store standing in for a real
//! apiserver, so `K8sFleet` can be proven against the same backend-agnostic
//! [`crate::testkit::fleet_provider_conformance`] suite `LocalFleet` runs
//! (ADR 0008 §4). Mirrors `testkit::FakeCaliband`'s role: a faithful-enough
//! double that a `FleetProvider` backend drives through its real seam
//! ([`super::fleet::CalibanTaskApi`]) with no cluster involved.
//!
//! ## Why reads are instant but writes aren't
//!
//! `K8sFleet` runs a *single shared* poll-diff loop that maintains a canonical
//! `known` map (prospero #77 M2); each `watch_fleet()` subscriber seeds from
//! that shared state then tails a broadcast, so every subscriber sees the same
//! diff stream and `Gone` exactly once. The conformance suite's `stop_agent`
//! step subscribes and then, with no
//! synchronizing wait in between, immediately deletes — so this fake's
//! *scheduling* behavior, not just its data, has to make that first `list()`
//! win the race deterministically instead of by luck.
//!
//! If every method here resolved with equal latency, it wouldn't: the
//! `delete` call (already running on the caller's task) always arms its
//! timer *before* the freshly spawned watch task gets its first turn, so an
//! equal-duration `list()` timer armed afterwards always fires later, every
//! time — the freshly-spawned watch task would never see the agent before it
//! vanished. Keeping `get`/`list` latency-free (they resolve on their very
//! first poll, no timer at all) instead guarantees the watch task's first
//! iteration runs to completion — reading pre-deletion state — during the
//! very same scheduling turn the caller's task yields to `delete`'s write
//! latency, before that latency's timer can fire and mutate the store.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::error::{CoreError, Result};
use crate::k8s::crd::{CalibanTask, CalibanTaskStatus, NamedRef};
use crate::k8s::fleet::CalibanTaskApi;
use crate::testkit::FakeBackend;

/// A nominal round-trip delay `apply`/`delete` incur, standing in for real
/// apiserver write latency. `get`/`list` deliberately do **not** get this
/// delay — see the module doc's "why reads are instant but writes aren't"
/// note for why that asymmetry (not just "some" latency) is what makes the
/// conformance suite's timing deterministic rather than a coin flip.
const FAKE_API_WRITE_LATENCY: Duration = Duration::from_millis(5);

async fn simulate_write_latency() {
    tokio::time::sleep(FAKE_API_WRITE_LATENCY).await;
}

/// An `Arc`-shared, in-memory `CalibanTask` store. Clone freely — every clone
/// is a handle onto the *same* underlying store, which is what lets a
/// `K8sFleet` built over one clone and a `&dyn FakeBackend` built over
/// another observe each other's writes (in particular: a `simulate_reap`
/// delete becomes visible to `K8sFleet::watch_fleet`'s poll loop).
///
/// `apply` immediately simulates the operator's reconcile — setting
/// `status.phase = "Running"` plus a `calibandEndpoint`/`sandboxRef` — rather
/// than leaving the CR pending the way a real apiserver would. A real cluster
/// needs the separate caliban-operator process to drive that transition;
/// this fake plays both parts so `K8sFleet::ensure_agent`'s wait-for-Running
/// converges immediately instead of the test needing a second actor to push
/// the fake forward in the background.
#[derive(Clone)]
pub struct FakeK8s {
    store: Arc<Mutex<HashMap<String, CalibanTask>>>,
    applied_any: Arc<AtomicBool>,
}

impl Default for FakeK8s {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeK8s {
    /// A fresh, empty fake with no `CalibanTask`s yet.
    #[must_use]
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
            applied_any: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl CalibanTaskApi for FakeK8s {
    async fn apply(&self, ct: &CalibanTask) -> Result<()> {
        simulate_write_latency().await;
        let name = ct
            .metadata
            .name
            .clone()
            .ok_or_else(|| CoreError::Fleet("CalibanTask missing metadata.name".to_string()))?;

        let mut reconciled = ct.clone();
        reconciled.status = Some(CalibanTaskStatus {
            phase: "Running".to_string(),
            caliband_endpoint: Some(format!("{name}.fake.svc:8443")),
            sandbox_ref: Some(NamedRef {
                name: format!("{name}-sandbox"),
            }),
            // The fake plays operator but doesn't resolve a Workspace CR; leave
            // the pinned resolvedWorkspace unset. `agent_from_task` falls back to
            // `spec.workspaceRef.name` for the workspace label, which suffices.
            resolved_workspace: None,
        });

        self.store.lock().unwrap().insert(name, reconciled);
        self.applied_any.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Option<CalibanTask>> {
        Ok(self.store.lock().unwrap().get(name).cloned())
    }

    async fn delete(&self, name: &str) -> Result<()> {
        simulate_write_latency().await;
        // Idempotent: `K8sFleet::stop_agent(Kill)` already deletes on this
        // same seam, and the conformance suite's `simulate_reap` calls
        // `delete` again on a name that may already be gone — a double
        // delete must stay `Ok(())`, matching `KubeTaskApi::delete`'s
        // documented contract.
        self.store.lock().unwrap().remove(name);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<CalibanTask>> {
        Ok(self.store.lock().unwrap().values().cloned().collect())
    }
}

impl FakeBackend for FakeK8s {
    fn received_any_spec(&self) -> bool {
        self.applied_any.load(Ordering::SeqCst)
    }

    fn simulate_reap(&self, id: &str) {
        self.store.lock().unwrap().remove(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::fleet::{K8sFleet, PollConfig};
    use std::time::Duration;

    /// ADR 0008 §4 / Task B5 acceptance: `K8sFleet` satisfies the same
    /// backend-agnostic conformance suite `LocalFleet` does
    /// (`fleet_provider.rs::local_fleet_satisfies_conformance`), driven here
    /// against `FakeK8s` instead of a real cluster.
    ///
    /// `K8sFleet` runs one shared poll-diff loop off the `CalibanTaskApi::list()`
    /// seam (prospero #77 M2) — unlike `LocalFleet`, which needs a *separate*
    /// background `FleetManager::run` task started before the suite runs, there's
    /// no extra reconciliation loop to wire up here: every `watch_fleet()` call
    /// the suite makes (including its repeated re-subscriptions in
    /// `wait_for_discovered`) starts its own loop against the live store.
    #[tokio::test]
    async fn k8s_fleet_satisfies_conformance() {
        let fake = FakeK8s::new();
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn crate::store::Store> =
            Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let bus: Arc<dyn crate::bus::EventBus> = Arc::new(crate::bus::InProcessBus::new(64));
        let fleet = K8sFleet::with_poll_config(
            fake.clone(),
            PollConfig {
                // `FakeK8s::apply` sets `Running` synchronously, so
                // `ensure_agent`'s first poll already succeeds; a short
                // deadline just bounds the "never happens" failure mode.
                deadline: Duration::from_secs(2),
                interval: Duration::from_millis(10),
            },
            bus,
            store,
        )
        .with_watch_poll_interval(Duration::from_millis(20));

        crate::testkit::fleet_provider_conformance(&fleet, &fake).await;
    }
}
