//! Integration tests for `FleetManager` driven by the `FakeCaliband` harness.
//!
//! These exercise the real poll + reconcile + attach + normalize + store path
//! end-to-end, with no real caliban.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use prospero_core::discovery::{DiscoveryEnv, EnsureConfig, control_socket_path};
use prospero_core::event::EventKind;
use prospero_core::fleet::{AttachBackoff, FleetConfig, FleetManager, SpawnRequest};
use prospero_core::model::{AgentStatus, RepoHealth};
use prospero_core::store::JsonlStore;
use prospero_core::testkit::{FakeCaliband, test_record};
use prospero_core::{CoreError, RepoProviderConfig};

/// Keeps the manager, fake, and all backing temp dirs alive for a test.
struct Harness {
    manager: FleetManager,
    fake: FakeCaliband,
    _repo: tempfile::TempDir,
    _runtime: tempfile::TempDir,
    _data: tempfile::TempDir,
}

impl Harness {
    /// Directory holding the control + per-agent stream sockets.
    fn socket_dir(&self) -> PathBuf {
        self.fake.control_socket().parent().unwrap().to_path_buf()
    }
}

/// Build a manager + fake whose socket lives at the discovery-derived path for
/// `repo_root`, so the production discovery path is exercised unchanged.
async fn setup() -> Harness {
    let repo_dir = tempfile::tempdir().unwrap();
    let runtime_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let repo_root = repo_dir.path().canonicalize().unwrap();

    let env = DiscoveryEnv {
        caliban_daemon_runtime_dir: Some(runtime_dir.path().to_path_buf()),
        xdg_runtime_dir: None,
        tmpdir: None,
    };
    let socket = control_socket_path(&repo_root, &env);
    let fake = FakeCaliband::start_at(&socket).await.unwrap();

    let mut config = FleetConfig::new("test-host", data_dir.path());
    config.discovery_env = env;
    config.ensure = EnsureConfig {
        autostart: false,
        ..EnsureConfig::default()
    };
    config.poll_interval = Duration::from_millis(20);
    // Tiny backoff so reconnection tests don't sleep for real.
    config.attach_backoff = AttachBackoff {
        base: Duration::from_millis(1),
        max: Duration::from_millis(5),
        max_retries: 5,
    };

    let store = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let manager = FleetManager::new(config, store).unwrap();
    manager.add_repo("repo", repo_root).await.unwrap();

    Harness {
        manager,
        fake,
        _repo: repo_dir,
        _runtime: runtime_dir,
        _data: data_dir,
    }
}

/// Drain events from a receiver into a Vec of kinds until quiet for `max_wait`.
async fn collect_kinds(
    rx: &mut tokio::sync::broadcast::Receiver<prospero_core::FleetEvent>,
    max_wait: Duration,
) -> Vec<EventKind> {
    let mut kinds = Vec::new();
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(ev)) => kinds.push(ev.kind),
            _ => break,
        }
    }
    kinds
}

#[tokio::test]
async fn spawn_defaults_to_worktree_isolation() {
    let h = setup().await;
    h.manager
        .spawn_agent("repo", SpawnRequest::new("do the thing"))
        .await
        .unwrap();

    let specs = h.fake.received_specs();
    assert_eq!(specs.len(), 1);
    assert!(
        specs[0].isolation_worktree,
        "worktree isolation must be the default"
    );
    assert_eq!(specs[0].initial_prompt, "do the thing");
}

#[tokio::test]
async fn shared_tree_opt_out_is_respected() {
    let h = setup().await;
    let mut req = SpawnRequest::new("shared work");
    req.isolation_worktree = false;
    h.manager.spawn_agent("repo", req).await.unwrap();

    let specs = h.fake.received_specs();
    assert!(!specs[0].isolation_worktree);
}

#[tokio::test]
async fn poll_discovers_preexisting_agents_and_streams_them() {
    let mut h = setup().await;
    let dir = h.socket_dir();

    // Pre-seed an active agent whose stream replays an init then a result.
    let rec = test_record("agent001", &dir, AgentStatus::Running, true);
    h.fake
        .add_agent(
            rec,
            vec![
                serde_json::json!({"type":"system","subtype":"init","model":"m","tools":["Read"],"session_id":"s"}),
                serde_json::json!({"type":"text","delta":"hello"}),
                serde_json::json!({"type":"result","subtype":"success","total_cost_usd":0.5,"turns":2}),
            ],
        )
        .await;

    let mut rx = h.manager.subscribe();
    h.manager.poll_repo_once("repo").await;
    let kinds = collect_kinds(&mut rx, Duration::from_secs(2)).await;

    assert!(
        kinds
            .iter()
            .any(|k| matches!(k, EventKind::AgentDiscovered))
    );
    assert!(
        kinds
            .iter()
            .any(|k| matches!(k, EventKind::AgentInit { .. }))
    );
    assert!(
        kinds
            .iter()
            .any(|k| matches!(k, EventKind::Output { chunk, .. } if chunk == "hello"))
    );
    assert!(kinds.iter().any(
        |k| matches!(k, EventKind::AgentFinished { cost_usd, turns, .. } if *cost_usd == 0.5 && *turns == 2)
    ));

    let snap = h.manager.snapshot().await;
    let (repo, agent) = snap.find_agent("agent001").unwrap();
    assert_eq!(repo, "repo");
    assert!(agent.isolated);
}

#[tokio::test]
async fn attach_reconnects_after_drop_without_dup_or_loss() {
    let mut h = setup().await;
    let dir = h.socket_dir();
    let rec = test_record("agent001", &dir, AgentStatus::Running, true);

    let init = serde_json::json!({"type":"system","subtype":"init","model":"m","tools":["Read"],"session_id":"s"});
    let hello = serde_json::json!({"type":"text","delta":"hello"});
    let result =
        serde_json::json!({"type":"result","subtype":"success","total_cost_usd":0.5,"turns":2});

    // Connection 1 drops mid-stream after init+hello (no terminal result);
    // the reconnect replays the full stream including the result.
    h.fake
        .add_agent_with_scripts(
            rec,
            vec![
                vec![init.clone(), hello.clone()],
                vec![init.clone(), hello.clone(), result.clone()],
            ],
        )
        .await;

    let mut rx = h.manager.subscribe();
    h.manager.poll_repo_once("repo").await;
    let kinds = collect_kinds(&mut rx, Duration::from_secs(2)).await;

    // No loss: the terminal event arrives despite the mid-stream drop.
    assert_eq!(
        kinds
            .iter()
            .filter(|k| matches!(k, EventKind::AgentFinished { .. }))
            .count(),
        1,
        "the post-drop terminal event must not be lost: {kinds:?}"
    );
    // No duplicates: the replayed prefix (init, hello) is emitted exactly once.
    assert_eq!(
        kinds
            .iter()
            .filter(|k| matches!(k, EventKind::AgentInit { .. }))
            .count(),
        1,
        "init must not be duplicated across the reconnect: {kinds:?}"
    );
    assert_eq!(
        kinds
            .iter()
            .filter(|k| matches!(k, EventKind::Output { chunk, .. } if chunk == "hello"))
            .count(),
        1,
        "output must not be duplicated across the reconnect: {kinds:?}"
    );

    // Durable history is likewise free of duplicates.
    let history = h.manager.history("agent001", 0).await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|e| matches!(&e.kind, EventKind::Output { chunk, .. } if chunk == "hello"))
            .count(),
        1,
        "durable log must not double-persist replayed frames"
    );
}

#[tokio::test]
async fn unknown_frame_advances_the_metrics_counter() {
    let mut h = setup().await;
    let dir = h.socket_dir();
    let rec = test_record("agent001", &dir, AgentStatus::Running, false);
    // A frame caliban has moved ahead to that prospero's wire client doesn't know.
    h.fake
        .add_agent(
            rec,
            vec![
                serde_json::json!({"type":"future_thing","data":1}),
                serde_json::json!({"type":"result","subtype":"success","total_cost_usd":0.0,"turns":1}),
            ],
        )
        .await;

    let mut rx = h.manager.subscribe();
    h.manager.poll_repo_once("repo").await;
    let _ = collect_kinds(&mut rx, Duration::from_secs(1)).await;

    let m = h.manager.metrics();
    assert_eq!(
        m.unknown_frames, 1,
        "the future_thing frame must be counted"
    );
    assert!(m.repos_polled >= 1, "poll must be counted: {m:?}");
    assert!(
        m.events_appended >= 1,
        "appended events must be counted: {m:?}"
    );
}

#[tokio::test]
async fn history_is_persisted_and_replayable() {
    let mut h = setup().await;
    let dir = h.socket_dir();
    let rec = test_record("agent001", &dir, AgentStatus::Running, false);
    h.fake
        .add_agent(
            rec,
            vec![serde_json::json!({"type":"text","delta":"persisted"})],
        )
        .await;

    let mut rx = h.manager.subscribe();
    h.manager.poll_repo_once("repo").await;
    let _ = collect_kinds(&mut rx, Duration::from_secs(1)).await;

    let history = h.manager.history("agent001", 0).await.unwrap();
    assert!(
        history
            .iter()
            .any(|e| matches!(&e.kind, EventKind::Output { chunk, .. } if chunk == "persisted"))
    );
    let seqs: Vec<u64> = history.iter().map(|e| e.seq).collect();
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    assert_eq!(seqs, sorted);
}

#[tokio::test]
async fn status_change_emits_event_across_polls() {
    let mut h = setup().await;
    let dir = h.socket_dir();
    // Idle agent (not active → no attach), so we isolate the status-change path.
    let rec = test_record("agent001", &dir, AgentStatus::Idle, false);
    h.fake.add_agent(rec, vec![]).await;

    h.manager.poll_repo_once("repo").await; // discover as Idle
    let mut rx = h.manager.subscribe();
    h.fake.set_status("agent001", AgentStatus::Done);
    h.manager.poll_repo_once("repo").await; // observe transition

    let kinds = collect_kinds(&mut rx, Duration::from_secs(1)).await;
    assert!(kinds.iter().any(|k| matches!(
        k,
        EventKind::StatusChanged {
            from: AgentStatus::Idle,
            to: AgentStatus::Done
        }
    )));
}

#[tokio::test]
async fn unreachable_repo_degrades_without_failing() {
    let repo_dir = tempfile::tempdir().unwrap();
    let runtime_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let repo_root = repo_dir.path().canonicalize().unwrap();

    // No fake started → socket is absent.
    let env = DiscoveryEnv {
        caliban_daemon_runtime_dir: Some(runtime_dir.path().to_path_buf()),
        xdg_runtime_dir: None,
        tmpdir: None,
    };
    let mut config = FleetConfig::new("h", data_dir.path());
    config.discovery_env = env;
    config.ensure = EnsureConfig {
        autostart: false,
        ..EnsureConfig::default()
    };
    let store = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let manager = FleetManager::new(config, store).unwrap();
    manager.add_repo("repo", repo_root).await.unwrap();

    manager.poll_repo_once("repo").await;
    let snap = manager.snapshot().await;
    let repo = snap.repos.iter().find(|r| r.name == "repo").unwrap();
    assert!(matches!(repo.health, RepoHealth::Unreachable { .. }));
}

#[tokio::test]
async fn spawn_rejects_provider_with_unset_api_key() {
    let h = setup().await;
    h.manager
        .set_repo_config_registry_only(
            "repo",
            RepoProviderConfig {
                provider: Some("anthropic".into()),
                ..RepoProviderConfig::default()
            },
        )
        .await
        .unwrap();

    let err = h
        .manager
        .spawn_agent("repo", SpawnRequest::new("doomed"))
        .await
        .unwrap_err();

    assert!(
        matches!(err, CoreError::ProviderMisconfigured(_)),
        "unset provider key must surface as ProviderMisconfigured, got: {err:?}"
    );
    assert!(
        h.fake.received_specs().is_empty(),
        "validation must reject before a doomed agent reaches caliban"
    );
}

#[tokio::test]
async fn agent_gone_emitted_when_it_disappears() {
    let mut h = setup().await;
    let dir = h.socket_dir();
    let rec = test_record("agent001", &dir, AgentStatus::Idle, false);
    h.fake.add_agent(rec, vec![]).await;

    h.manager.poll_repo_once("repo").await; // discover
    let mut rx = h.manager.subscribe();
    h.fake.remove_agent("agent001");
    h.manager.poll_repo_once("repo").await; // observe removal

    let kinds = collect_kinds(&mut rx, Duration::from_secs(1)).await;
    assert!(kinds.iter().any(|k| matches!(k, EventKind::AgentGone)));
}
