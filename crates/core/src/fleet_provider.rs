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
        // `spawn_agent_with_socket` already returns the per-agent socket
        // `client.spawn` produced, so no follow-up `Attach` round-trip is
        // needed to resolve it (and no new failure mode on the success path).
        let (id, endpoint) = self
            .inner
            .spawn_agent_with_socket(&spec.repo, spec.request)
            .await?;
        Ok(AgentHandle {
            id: AgentId::from(id),
            repo: spec.repo,
            endpoint,
        })
    }

    fn watch_fleet(&self) -> BoxStream<'static, FleetChange> {
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

    /// Regression for the whole-branch-review finding: `ensure_agent` used to
    /// resolve the spawned agent's socket via a second `Attach` round-trip
    /// (`FleetManager::agent_socket`) even though `client.spawn` already
    /// returns the socket. That extra round-trip both duplicated a request
    /// and introduced a new failure mode (a successful spawn could still fail
    /// `ensure_agent` if the follow-up attach errored). Assert the fake sees a
    /// `Spawn` but no `Attach`, and that the handle's socket is exactly the one
    /// the fake's `Spawn` reply advertised.
    #[tokio::test]
    async fn ensure_agent_does_not_issue_a_second_attach() {
        let (provider, fake, _dir) = setup().await;

        let handle = provider
            .ensure_agent(TaskSpec {
                repo: "repo-a".into(),
                request: SpawnRequest::new("task"),
            })
            .await
            .expect("ensure_agent");

        assert!(!fake.received_specs().is_empty(), "spawn reached the fake");
        assert!(
            fake.received_attach_ids().is_empty(),
            "ensure_agent must not issue an Attach to resolve the socket it already has, but saw: {:?}",
            fake.received_attach_ids()
        );

        // The endpoint on the handle must be the one caliband's `Spawned` reply
        // advertised for this id, proving it came straight from `spawn`'s
        // return value rather than a (now-absent) follow-up attach.
        let expected = crate::caliband::wire::Endpoint::Unix {
            path: _dir.path().join(format!("{}.sock", handle.id.as_str())),
        };
        assert_eq!(handle.endpoint, expected);
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
        let (_, agent) = snap
            .find_agent(handle.id.as_str())
            .expect("agent still known");
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

    /// Task 3: `watch_fleet` seeds from the current snapshot (here, just
    /// `repo-a`'s `RepoHealth`, since no agent exists yet) and then surfaces
    /// live `FleetChange`s translated from the poll-diff events `reconcile`
    /// emits — driven here by a `FakeCaliband` spawn + one `poll_repo_once`.
    #[tokio::test]
    async fn watch_fleet_reports_discovered() {
        use crate::model::FleetChange;
        use crate::testkit::test_record;
        use futures::StreamExt;
        use std::time::Duration;

        let (provider, mut fake, dir) = setup().await;

        // Subscribe before the agent exists, exactly like a real watcher would:
        // `watch_fleet` must pick up `a1`'s `Discovered` change even though its
        // stream key (its own id) is unknowable until after the event fires.
        let mut changes = provider.watch_fleet();

        fake.add_agent(
            test_record("a1", dir.path(), crate::model::AgentStatus::Running, false),
            Vec::new(),
        )
        .await;
        provider.manager().poll_repo_once("repo-a").await;

        // The initial burst carries `repo-a`'s `RepoHealth` (seeded from
        // `setup()`'s own `add_repo`-triggered poll) ahead of the post-seed
        // `Discovered` diff; drain up to a few items for it, bounded so a
        // regression fails fast instead of hanging.
        let mut discovered = None;
        for _ in 0..5 {
            let item = tokio::time::timeout(Duration::from_secs(1), changes.next())
                .await
                .expect("timed out waiting for a FleetChange")
                .expect("watch_fleet stream ended unexpectedly");
            if matches!(item, FleetChange::Discovered { .. }) {
                discovered = Some(item);
                break;
            }
        }
        let ev = discovered.expect("did not observe a Discovered change in time");
        assert!(
            matches!(ev, FleetChange::Discovered { ref id, ref repo, .. }
            if id.as_str() == "a1" && repo == "repo-a")
        );
    }

    /// Like [`setup`], but also runs `FleetManager::run`'s background poll
    /// loop on a fast interval. `testkit::fleet_provider_conformance` is
    /// deliberately generic over `&dyn FleetProvider` and never reaches for
    /// `LocalFleet`-internal methods like `poll_repo_once`, so it needs a real
    /// (if accelerated) reconciliation loop driving state forward underneath
    /// it, the same way production does.
    async fn setup_with_background_poll() -> (LocalFleet, FakeCaliband, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
        config.ensure.autostart = false; // no real caliband to spawn in tests
        config.poll_interval = std::time::Duration::from_millis(20);
        let root = dir.path().join("repo-a");
        std::fs::create_dir_all(&root).unwrap();
        let socket = crate::discovery::resolve_socket(&root, &config.discovery_env).unwrap();

        let fake = FakeCaliband::start_at(&socket).await.unwrap();
        let store = Arc::new(JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).await.unwrap();
        mgr.add_repo("repo-a", &root).await.unwrap();

        tokio::spawn(mgr.clone().run());

        (LocalFleet::new(mgr), fake, dir)
    }

    /// Task 4: `LocalFleet` satisfies the `FleetProvider` conformance suite.
    #[tokio::test]
    async fn local_fleet_satisfies_conformance() {
        let (provider, fake, _dir) = setup_with_background_poll().await;
        crate::testkit::fleet_provider_conformance(&provider, &fake).await;
        // Tidy: stop the background poll loop before `_dir` (and its sockets)
        // get removed on drop.
        provider.manager().begin_shutdown();
    }
}
