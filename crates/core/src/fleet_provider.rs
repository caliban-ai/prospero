//! The `FleetProvider` seam: an ensure-desired-state + observe abstraction over
//! a fleet of caliband-supervised agents. `LocalFleet` is the caliband-over-Unix
//! -sockets backend (today's behavior); future backends (K8sFleet — epic #274 P2;
//! remote — prospero #1) implement the same trait. The live session plane
//! (attach/stream/steer) is deliberately NOT part of this trait — it stays on
//! `CalibandClient` and is shared across backends.

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::error::Result;
use crate::fleet::FleetManager;
use crate::model::{AgentHandle, AgentId, DrainPolicy, FleetChange, TaskSpec};

#[async_trait]
pub trait FleetProvider: Send + Sync {
    /// Ensure an agent for `spec` exists and is attachable. Idempotent.
    async fn ensure_agent(&self, spec: TaskSpec) -> Result<AgentHandle>;

    /// Observe the fleet: an initial listing followed by live change events.
    fn watch_fleet(&self) -> BoxStream<'static, FleetChange>;

    /// Stop an agent per `drain` policy.
    async fn stop_agent(&self, id: &AgentId, drain: DrainPolicy) -> Result<()>;

    /// Restart an agent; returns the (possibly new) id.
    async fn restart_agent(&self, id: &AgentId) -> Result<AgentId>;
}

/// caliband-over-Unix-sockets backend — wraps today's `FleetManager` verbatim.
#[derive(Clone)]
pub struct LocalFleet {
    inner: FleetManager,
}

impl LocalFleet {
    #[must_use]
    pub fn new(inner: FleetManager) -> Self {
        Self { inner }
    }

    /// Access the underlying manager (session plane, API handlers still use it
    /// directly in P1).
    #[must_use]
    pub fn manager(&self) -> &FleetManager {
        &self.inner
    }
}

#[async_trait]
impl FleetProvider for LocalFleet {
    async fn ensure_agent(&self, spec: TaskSpec) -> Result<AgentHandle> {
        let id = self.inner.spawn_agent(&spec.repo, spec.request).await?;
        // Resolve the per-agent socket for the returned id (attach path, no
        // stream opened).
        let socket = self.inner.agent_socket(&spec.repo, &id).await?;
        Ok(AgentHandle {
            id: AgentId::from(id),
            repo: spec.repo,
            socket,
        })
    }

    fn watch_fleet(&self) -> BoxStream<'static, FleetChange> {
        // TODO(T3): real stream — Task 3 replaces this placeholder with the
        // live poll-diff feed (fleet.rs:811).
        self.inner.watch_changes()
    }

    async fn stop_agent(&self, id: &AgentId, drain: DrainPolicy) -> Result<()> {
        match drain {
            DrainPolicy::Kill => self.inner.kill_agent(id.as_str()).await,
            DrainPolicy::Graceful { timeout_ms } => {
                self.inner
                    .drain_agent(id.as_str(), std::time::Duration::from_millis(timeout_ms))
                    .await
            }
        }
    }

    async fn restart_agent(&self, id: &AgentId) -> Result<AgentId> {
        let new_id = self.inner.respawn_agent(id.as_str()).await?;
        Ok(AgentId::from(new_id))
    }
}

#[cfg(all(test, feature = "testkit"))]
mod local_fleet_tests {
    use super::*;
    use crate::fleet::{FleetConfig, SpawnRequest};
    use crate::store::JsonlStore;
    use crate::testkit::FakeCaliband;
    use std::sync::Arc;

    /// Wire a `FleetManager` over a `FakeCaliband` control socket, following the
    /// same discovery-derived path used across `fleet.rs`'s own inline tests
    /// (e.g. `restart_caliband_shuts_down_and_clears_client`), then wrap it in
    /// `LocalFleet`.
    async fn setup() -> (LocalFleet, FakeCaliband, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
        config.ensure.autostart = false; // no real caliband to spawn in tests
        let root = dir.path().join("repo-a");
        std::fs::create_dir_all(&root).unwrap();
        let socket = crate::discovery::resolve_socket(&root, &config.discovery_env).unwrap();

        let fake = FakeCaliband::start_at(&socket).await.unwrap();
        let store = Arc::new(JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).await.unwrap();
        mgr.add_repo("repo-a", &root).await.unwrap();

        (LocalFleet::new(mgr), fake, dir)
    }

    #[tokio::test]
    async fn ensure_then_stop_agent_via_provider() {
        let (provider, fake, _dir) = setup().await;

        let handle = provider
            .ensure_agent(TaskSpec {
                repo: "repo-a".into(),
                request: SpawnRequest::new("task"),
            })
            .await
            .expect("ensure_agent");
        assert_eq!(handle.repo, "repo-a");
        assert!(!fake.received_specs().is_empty());

        // Populate the manager's snapshot so `stop_agent` (via `kill_agent` ->
        // `repo_of`) can resolve the agent's repo, mirroring how `fleet.rs`'s own
        // tests poll once after a spawn before acting on the agent id.
        provider.manager().poll_repo_once("repo-a").await;

        provider
            .stop_agent(&handle.id, DrainPolicy::Kill)
            .await
            .expect("stop");

        // Confirm the kill actually reached the fake: re-poll and check the
        // manager's own view of the agent's status.
        provider.manager().poll_repo_once("repo-a").await;
        let snap = provider.manager().snapshot().await;
        let (_, agent) = snap.find_agent(handle.id.as_str()).expect("agent still known");
        assert_eq!(agent.status, crate::model::AgentStatus::Killed);
    }

    #[tokio::test]
    async fn restart_agent_returns_new_id() {
        let (provider, _fake, _dir) = setup().await;

        let handle = provider
            .ensure_agent(TaskSpec {
                repo: "repo-a".into(),
                request: SpawnRequest::new("task"),
            })
            .await
            .expect("ensure_agent");
        provider.manager().poll_repo_once("repo-a").await;

        let new_id = provider
            .restart_agent(&handle.id)
            .await
            .expect("restart_agent");
        assert_ne!(new_id, handle.id);
    }

    #[tokio::test]
    async fn watch_fleet_placeholder_is_empty() {
        use futures::StreamExt;

        let (provider, _fake, _dir) = setup().await;
        let mut stream = provider.watch_fleet();
        assert!(stream.next().await.is_none());
    }
}
