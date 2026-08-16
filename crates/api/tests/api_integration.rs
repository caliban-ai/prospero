//! In-process API tests: drive the axum `Router` with `oneshot` (no real port)
//! over a `FakeCaliband`-backed `FleetManager`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use prospero_api::router;
use prospero_core::discovery::{DiscoveryEnv, EnsureConfig, control_socket_path};
use prospero_core::fleet::{FleetConfig, FleetManager};
use prospero_core::model::AgentStatus;
use prospero_core::store::{JsonlStore, Store};
use prospero_core::testkit::{FakeCaliband, test_record};
use prospero_core::{FleetEvent, LocalFleet, Result};
use tower::ServiceExt;

/// A store that persists normally but reports itself non-writable, to drive the
/// readiness endpoint's degraded (503) path.
struct UnwritableStore(JsonlStore);

#[async_trait]
impl Store for UnwritableStore {
    async fn append(&self, event: &FleetEvent) -> Result<()> {
        self.0.append(event).await
    }
    async fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
        self.0.replay(stream_key, from_seq).await
    }
    async fn high_water(&self, stream_key: &str) -> Result<u64> {
        self.0.high_water(stream_key).await
    }
    async fn writable(&self) -> bool {
        false
    }
    async fn prune(&self, before_ts: &str) -> Result<u64> {
        self.0.prune(before_ts).await
    }
    async fn usage(&self, since: &str, until: &str) -> Result<Vec<prospero_core::store::UsageRow>> {
        self.0.usage(since, until).await
    }
}

struct Harness {
    router: Router,
    manager: FleetManager,
    fake: FakeCaliband,
    _repo: tempfile::TempDir,
    _runtime: tempfile::TempDir,
    _data: tempfile::TempDir,
}

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

    let store = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let manager = FleetManager::new(config, store).await.unwrap();
    manager.add_repo("repo", repo_root).await.unwrap();

    Harness {
        router: router(
            Arc::new(LocalFleet::new(manager.clone())),
            Some(Arc::new(LocalFleet::new(manager.clone()))),
            manager.store(),
            manager.bus(),
        ),
        manager,
        fake,
        _repo: repo_dir,
        _runtime: runtime_dir,
        _data: data_dir,
    }
}

async fn json_body(resp: axum::response::Response) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

/// Poll `/api/agents/<id>/events` until some persisted event satisfies `pred`,
/// returning the full events array.
///
/// `poll_repo_once` only kicks off the attach task; that task streams and
/// persists the agent's frames in the background. Waiting a fixed number of
/// milliseconds for it guesses at how long that takes and flakes on loaded CI
/// runners, so tests wait on the condition itself. (#103)
async fn wait_for_event(
    router: &Router,
    agent_id: &str,
    what: &str,
    pred: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/agents/{agent_id}/events"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v = json_body(resp).await;
        if v.as_array().is_some_and(|arr| arr.iter().any(&pred)) {
            return v;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what} on {agent_id}: {v}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn healthz_ok() {
    let h = setup().await;
    let resp = h
        .router
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn get_metrics_returns_operational_counters() {
    let h = setup().await;
    // add_repo triggers a poll, so repos_polled should be non-zero.
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    for key in [
        "events_appended",
        "append_failures",
        "unknown_frames",
        "repos_polled",
        "active_attaches",
    ] {
        assert!(
            v.get(key).and_then(|x| x.as_u64()).is_some(),
            "missing {key}: {v}"
        );
    }
    assert!(
        v["repos_polled"].as_u64().unwrap() >= 1,
        "the registration poll must be counted: {v}"
    );
}

#[tokio::test]
async fn capabilities_reports_admin_true_with_local_admin() {
    // The `setup()` harness builds the router with a `Some(admin)` (LocalFleet).
    let h = setup().await;
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/capabilities")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    assert_eq!(v["admin"], true, "local backend has an admin plane: {v}");
    assert_eq!(
        v["async_workspace_ops"], false,
        "local config applies synchronously: {v}"
    );
}

#[tokio::test]
async fn capabilities_reports_admin_false_without_admin() {
    // A router built with `admin: None`, mirroring the k8s composition (#76).
    let data_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let config = FleetConfig::new("test-host", data_dir.path());
    let manager = FleetManager::new(config, store).await.unwrap();
    let app = router(
        Arc::new(LocalFleet::new(manager.clone())),
        None,
        manager.store(),
        manager.bus(),
    );
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/capabilities")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    assert_eq!(v["admin"], false, "no admin plane ⇒ admin=false: {v}");
}

/// The k8s config plane (#142): configuring a workspace persists a `Workspace`
/// CR (no 405), the create is async (`202`), and `GET /api/workspaces` surfaces
/// the real CR — providers + reconciliation status — even with no agents yet.
#[cfg(feature = "k8s")]
#[tokio::test]
async fn k8s_config_plane_creates_and_surfaces_workspace() {
    use prospero_core::k8s::fake::{FakeK8s, FakeWorkspaceApi};
    use prospero_core::{K8sFleet, K8sWorkspaceAdmin};

    let data_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let bus: Arc<dyn prospero_core::bus::EventBus> =
        Arc::new(prospero_core::bus::InProcessBus::new(64));
    let ws_api = Arc::new(FakeWorkspaceApi::new());
    let admin = Arc::new(K8sWorkspaceAdmin::new(Arc::clone(&ws_api)));
    let fleet = Arc::new(K8sFleet::new(FakeK8s::new(), bus.clone(), store.clone()));
    let app = router(
        fleet,
        Some(admin as Arc<dyn prospero_core::FleetAdmin>),
        store,
        bus,
    );

    // Capabilities advertise the async config plane, so the dashboard renders
    // the k8s config UI + reconciling save semantics.
    let caps = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/capabilities")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let cv = json_body(caps).await;
    assert_eq!(cv["admin"], true);
    assert_eq!(cv["async_workspace_ops"], true, "k8s config is async: {cv}");

    // Configuring a workspace on k8s is accepted asynchronously (202), not 405.
    let body = r#"{"name":"team-a-ws","config":{
        "display_name":"Team A",
        "sources":[{"name":"caliban","repo":"git@x:caliban","path":"/work/caliban"}],
        "providers":[
            {"name":"planner","kind":"anthropic","model":"claude-opus-4-8","credentials_ref":{"secret_name":"anthropic-key","key":"api-key"}},
            {"name":"workers","kind":"ollama","base_url":"http://h:11434"}
        ],
        "default_provider":"planner"}}"#;
    let post = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/workspaces")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(post.status(), StatusCode::ACCEPTED, "async create ⇒ 202");

    // The operator reconciles the CR (simulated) → status surfaces on the read side.
    ws_api.set_status("team-a-ws", "Ready", None);

    let get = app
        .oneshot(
            Request::builder()
                .uri("/api/workspaces")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let v = json_body(get).await;
    let ws = v
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["name"] == "team-a-ws")
        .expect("configured workspace is visible even with no agents");
    assert_eq!(ws["display_name"], "Team A");
    assert_eq!(ws["default_provider"], "planner");
    assert_eq!(ws["providers"].as_array().unwrap().len(), 2);
    assert_eq!(ws["providers"][0]["has_credentials"], true);
    assert_eq!(ws["providers"][1]["has_credentials"], false);
    assert_eq!(ws["agent_count"], 0);
    assert_eq!(ws["status"]["phase"], "Ready");
}

#[tokio::test]
async fn readyz_returns_200_when_store_writable() {
    let h = setup().await;
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    assert_eq!(v["ready"], true);
    assert_eq!(v["store_writable"], true);
    assert_eq!(v["workspaces_total"], 1);
}

#[tokio::test]
async fn readyz_returns_503_when_store_unwritable() {
    let data_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(UnwritableStore(JsonlStore::open(data_dir.path()).unwrap()));
    let config = FleetConfig::new("test-host", data_dir.path());
    let manager = FleetManager::new(config, store).await.unwrap();
    let app = router(
        Arc::new(LocalFleet::new(manager.clone())),
        Some(Arc::new(LocalFleet::new(manager.clone()))),
        manager.store(),
        manager.bus(),
    );

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let v = json_body(resp).await;
    assert_eq!(v["ready"], false);
    assert_eq!(v["store_writable"], false);
}

#[tokio::test]
async fn get_fleet_returns_registered_repo() {
    let h = setup().await;
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/fleet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    assert_eq!(v["host"], "test-host");
    assert_eq!(v["workspaces"][0]["name"], "repo");
}

#[tokio::test]
async fn spawn_defaults_to_worktree_and_returns_isolated_true() {
    let h = setup().await;
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/workspaces/repo/agents")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"prompt":"do it"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let v = json_body(resp).await;
    assert_eq!(v["isolated"], true);
    assert_eq!(v["workspace"], "repo");
    // And caliban actually received a worktree-isolated spec.
    assert!(h.fake.received_specs()[0].isolation_worktree);
}

#[tokio::test]
async fn spawn_with_unset_provider_key_returns_400() {
    let h = setup().await;
    h.manager
        .set_repo_config_registry_only(
            "repo",
            prospero_core::RepoProviderConfig {
                provider: Some("anthropic".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/workspaces/repo/agents")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"prompt":"doomed"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let v = json_body(resp).await;
    assert_eq!(v["kind"], "provider_misconfigured");
    assert!(
        v["error"].as_str().unwrap().contains("ANTHROPIC_API_KEY"),
        "actionable error names the missing var: {v}"
    );
    // No doomed agent reached caliban.
    assert!(h.fake.received_specs().is_empty());
}

#[tokio::test]
async fn spawn_shared_opt_out_returns_isolated_false() {
    let h = setup().await;
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/workspaces/repo/agents")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"prompt":"x","isolation":"shared"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let v = json_body(resp).await;
    assert_eq!(v["isolated"], false);
    assert!(!h.fake.received_specs()[0].isolation_worktree);
}

#[tokio::test]
async fn get_unknown_agent_is_404_not_500() {
    let h = setup().await;
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/agents/nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let v = json_body(resp).await;
    assert_eq!(v["kind"], "not_found");
}

#[tokio::test]
async fn get_events_for_unknown_agent_is_404() {
    // Mirrors the projection endpoint: an unknown id → 404, not `200 []` (#118).
    let h = setup().await;
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/agents/nope/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let v = json_body(resp).await;
    assert_eq!(v["kind"], "not_found");
}

#[tokio::test]
async fn kill_unknown_agent_is_404() {
    let h = setup().await;
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/agents/nope/kill")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn events_endpoint_returns_history_after_poll() {
    let mut h = setup().await;
    let dir = h.fake.control_socket().parent().unwrap().to_path_buf();
    let rec = test_record("agent001", &dir, AgentStatus::Running, true);
    h.fake
        .add_agent(
            rec,
            vec![serde_json::json!({"type":"AssistantTextDelta","turn_index":0,"content_block_index":0,"text":"hi from api"})],
        )
        .await;
    h.manager.poll_repo_once("repo").await;

    wait_for_event(&h.router, "agent001", "the streamed output chunk", |e| {
        e["kind"]["kind"] == "output" && e["kind"]["chunk"] == "hi from api"
    })
    .await;
}

/// Locks the exact `EventKind` JSON shapes the dashboard timeline
/// (`groupEvents`/`renderTimeline` in `dashboard/app.js`) reads. Seeds the store
/// directly so the assertion is on the wire contract, not on caliban frame
/// normalization. (#5)
#[tokio::test]
async fn events_endpoint_exposes_tool_and_cost_shapes_for_the_timeline() {
    use prospero_core::event::EventKind;
    let h = setup().await;
    let store = h.manager.store();
    let ev = |seq, kind| FleetEvent {
        seq,
        ts: "2026-07-05T00:00:00Z".to_string(),
        repo: "repo".to_string(),
        agent_id: "agent001".to_string(),
        kind,
    };
    store
        .append(&ev(
            1,
            EventKind::ToolStarted {
                id: "tu_1".to_string(),
                name: "Read".to_string(),
                input: serde_json::json!({ "path": "/x.rs" }),
            },
        ))
        .await
        .unwrap();
    store
        .append(&ev(
            2,
            EventKind::ToolFinished {
                id: "tu_1".to_string(),
                name: "Read".to_string(),
                ok: true,
            },
        ))
        .await
        .unwrap();
    store
        .append(&ev(
            3,
            EventKind::AgentFinished {
                outcome: "success".to_string(),
                cost_usd: 0.12,
                turns: 4,
            },
        ))
        .await
        .unwrap();

    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/agents/agent001/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    let arr = v.as_array().unwrap();
    assert!(
        arr.iter().any(|e| e["kind"]["kind"] == "tool_started"
            && e["kind"]["name"] == "Read"
            && e["kind"]["input"]["path"] == "/x.rs"),
        "tool_started shape: {v}"
    );
    assert!(
        arr.iter()
            .any(|e| e["kind"]["kind"] == "tool_finished" && e["kind"]["ok"] == true),
        "tool_finished shape: {v}"
    );
    assert!(
        arr.iter().any(|e| e["kind"]["kind"] == "agent_finished"
            && e["kind"]["cost_usd"] == 0.12
            && e["kind"]["turns"] == 4),
        "agent_finished shape: {v}"
    );
}

/// `GET /api/usage` aggregates spend and outcomes per workspace over a window
/// (#180). Seeds the store directly so the assertion is on the aggregate the
/// charts in #181 consume, not on caliban normalization.
#[tokio::test]
async fn usage_endpoint_aggregates_cost_and_outcomes_by_workspace() {
    use prospero_core::event::EventKind;
    let h = setup().await;
    let store = h.manager.store();
    let ev = |seq, ts: &str, agent: &str, kind| FleetEvent {
        seq,
        ts: ts.to_string(),
        repo: "repo".to_string(),
        agent_id: agent.to_string(),
        kind,
    };

    store
        .append(&ev(
            1,
            "2026-08-01T10:00:00+00:00",
            "a1",
            EventKind::AgentFinished {
                outcome: "success".to_string(),
                cost_usd: 0.50,
                turns: 3,
            },
        ))
        .await
        .unwrap();
    store
        .append(&ev(
            2,
            "2026-08-01T10:00:01+00:00",
            "a1",
            EventKind::StatusChanged {
                from: AgentStatus::Running,
                to: AgentStatus::Done,
            },
        ))
        .await
        .unwrap();
    // Killed without ever finishing: an outcome carrying no cost.
    store
        .append(&ev(
            1,
            "2026-08-02T10:00:00+00:00",
            "a2",
            EventKind::StatusChanged {
                from: AgentStatus::Running,
                to: AgentStatus::Killed,
            },
        ))
        .await
        .unwrap();

    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/usage?since=2026-08-01T00:00:00%2B00:00&until=2026-08-03T00:00:00%2B00:00")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;

    let g = &v["groups"][0];
    assert_eq!(g["workspace"], "repo", "payload: {v}");
    assert_eq!(g["cost_usd"], 0.50);
    assert_eq!(g["turns"], 3);
    assert_eq!(g["outcomes"]["done"], 1);
    assert_eq!(g["outcomes"]["killed"], 1);
    assert_eq!(g["outcomes"]["failed"], 0);

    // Two active days, ascending, so #181 can plot a series without re-sorting.
    let series = g["series"].as_array().unwrap();
    assert_eq!(series.len(), 2, "series: {v}");
    assert_eq!(series[0]["day"], "2026-08-01");
    assert_eq!(series[0]["cost_usd"], 0.50);
    assert_eq!(series[1]["day"], "2026-08-02");
    assert_eq!(series[1]["cost_usd"], 0.0);
    assert_eq!(series[1]["outcomes"]["killed"], 1);
}

/// The overview's window control sends a day count and lets the server resolve
/// `since` against its own clock — a browser-computed bound would silently clip
/// or pad the window whenever the client clock drifts (#181).
#[tokio::test]
async fn usage_endpoint_accepts_a_day_count_window() {
    let h = setup().await;
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/usage?days=30")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;

    let since = v["since"].as_str().unwrap();
    let until = v["until"].as_str().unwrap();
    let since = chrono::DateTime::parse_from_rfc3339(since).unwrap();
    let until = chrono::DateTime::parse_from_rfc3339(until).unwrap();
    let span_days = (until - since).num_days();
    assert_eq!(
        span_days, 30,
        "days=30 must widen the window to 30 days, got {span_days} ({since} → {until})"
    );
}

/// With no query params the endpoint must still answer, echoing back the window
/// it chose so a client can label an axis without re-deriving the default.
#[tokio::test]
async fn usage_endpoint_defaults_its_window() {
    let h = setup().await;
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/usage")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    assert!(
        v["since"].as_str().is_some_and(|s| !s.is_empty()),
        "since must be echoed: {v}"
    );
    assert!(
        v["until"].as_str().is_some_and(|s| !s.is_empty()),
        "until must be echoed: {v}"
    );
    assert!(v["groups"].as_array().unwrap().is_empty(), "payload: {v}");
}

#[tokio::test]
async fn sse_stream_closes_after_agent_finished() {
    let mut h = setup().await;
    let dir = h.fake.control_socket().parent().unwrap().to_path_buf();
    let rec = test_record("agent001", &dir, AgentStatus::Running, true);
    h.fake
        .add_agent(
            rec,
            vec![
                serde_json::json!({"type":"TurnStart","turn_index":0,"message_id":"s","model":"m"}),
                serde_json::json!({"type":"RunEnd","final_messages":[],"total_usage":{},"turn_count":1,"stopped_for":"EndOfTurn"}),
            ],
        )
        .await;
    h.manager.poll_repo_once("repo").await;

    // Wait for the terminal event to be persisted before opening the stream, so
    // the close-on-`AgentFinished` assertion isn't racing the attach task.
    wait_for_event(&h.router, "agent001", "agent_finished", |e| {
        e["kind"]["kind"] == "agent_finished"
    })
    .await;

    // Collecting the whole body must terminate (stream closes on AgentFinished).
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/agents/agent001/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let collected = tokio::time::timeout(Duration::from_secs(5), resp.into_body().collect())
        .await
        .expect("SSE stream should close, not hang")
        .unwrap()
        .to_bytes();
    let text = String::from_utf8_lossy(&collected);
    assert!(text.contains("agent_finished"), "stream body: {text}");
}

#[tokio::test]
async fn add_repo_with_config_persists_and_get_repos_returns_it() {
    // A fresh harness without any pre-registered repo so we can add one with config.
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
    let _fake = FakeCaliband::start_at(&socket).await.unwrap();

    let mut config = FleetConfig::new("test-host", data_dir.path());
    config.discovery_env = env;
    config.ensure = EnsureConfig {
        autostart: false,
        ..EnsureConfig::default()
    };

    let store = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let manager = FleetManager::new(config, store).await.unwrap();
    let app = router(
        Arc::new(LocalFleet::new(manager.clone())),
        Some(Arc::new(LocalFleet::new(manager.clone()))),
        manager.store(),
        manager.bus(),
    );

    // POST /api/workspaces with a config object.
    let post_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/workspaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"p","root":"/tmp/p","config":{"provider":"ollama","base_url":"http://h:11434"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(post_resp.status(), StatusCode::CREATED);

    // A second workspace on the SAME root (a permanent conflict, not a transient
    // reachability failure) → 409 Conflict, not 503 unreachable (#111).
    let dup = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/workspaces")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"q","root":"/tmp/p"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(dup.status(), StatusCode::CONFLICT);
    assert_eq!(json_body(dup).await["kind"], "conflict");

    // GET /api/workspaces should include the config fields.
    let get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/workspaces")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let v = json_body(get_resp).await;
    let repos = v.as_array().unwrap();
    let p = repos
        .iter()
        .find(|r| r["name"] == "p")
        .expect("repo 'p' not found");
    assert_eq!(p["config"]["provider"], "ollama");
    assert_eq!(p["config"]["base_url"], "http://h:11434");

    // The fleet snapshot must surface the same config (#48).
    let fleet_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/fleet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fleet_resp.status(), StatusCode::OK);
    let fleet = json_body(fleet_resp).await;
    let fp = fleet["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "p")
        .expect("repo 'p' not in fleet snapshot");
    assert_eq!(fp["config"]["provider"], "ollama");
    assert_eq!(fp["config"]["base_url"], "http://h:11434");
}

#[tokio::test]
async fn put_config_updates_and_returns_204() {
    // `setup()` registers "repo" with a FakeCaliband listening. PUT triggers a
    // restart (Shutdown → drain → re-ensure); with autostart=false the repo
    // simply degrades, but the config is persisted and the handler returns 204.
    let h = setup().await;
    let put_resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/workspaces/repo/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"provider":"ollama","base_url":"http://h:11434"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_resp.status(), StatusCode::NO_CONTENT);

    let get_resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/workspaces")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let v = json_body(get_resp).await;
    let repo = v
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "repo")
        .expect("repo not found");
    assert_eq!(repo["config"]["provider"], "ollama");
    assert_eq!(repo["config"]["base_url"], "http://h:11434");
}

#[tokio::test]
async fn put_config_unknown_repo_returns_404() {
    let h = setup().await;
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/workspaces/nope/config")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"provider":"ollama"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn put_config_api_key_on_keyless_provider_returns_400() {
    // #120: `api_key_from_env` on ollama (no api-key env var) would be silently
    // ignored at spawn time. It must be rejected at config-set with a clear 400.
    let h = setup().await;
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/workspaces/repo/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"provider":"ollama","api_key_from_env":"SOME_VAR"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let v = json_body(resp).await;
    assert_eq!(v["kind"], "provider_misconfigured");
    assert!(
        v["error"].as_str().unwrap().contains("api_key_from_env"),
        "error explains the offending field: {v}"
    );
}

#[tokio::test]
async fn rm_immediately_after_spawn_does_not_404() {
    // #122: a DELETE issued right after spawn — before any poll lands the agent
    // in the snapshot — must resolve via the spawn-tracking fallback, not 404.
    let h = setup().await;
    let spawn_resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/workspaces/repo/agents")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"prompt":"x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(spawn_resp.status(), StatusCode::CREATED);
    let id = json_body(spawn_resp).await["agent_id"]
        .as_str()
        .unwrap()
        .to_string();

    // No poll in between: the snapshot has not observed the agent yet.
    let rm_resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/agents/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        rm_resp.status(),
        StatusCode::NO_CONTENT,
        "rm right after spawn must not 404 on the registration race"
    );
}

#[tokio::test]
async fn rm_drops_agent_from_fleet_immediately() {
    // #123: after a 204 rm, `/api/fleet` must not still list the agent while
    // waiting for the next poll — the served snapshot is pruned optimistically.
    let h = setup().await;
    let spawn_resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/workspaces/repo/agents")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"prompt":"x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let id = json_body(spawn_resp).await["agent_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Poll so the agent is in the served snapshot (sanity-checked below).
    h.manager.poll_repo_once("repo").await;
    let fleet = json_body(
        h.router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/fleet")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;
    let agents_listed = |fleet: &serde_json::Value| -> Vec<String> {
        fleet["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|w| w["agents"].as_array().unwrap().clone())
            .map(|a| a["id"].as_str().unwrap().to_string())
            .collect()
    };
    assert!(
        agents_listed(&fleet).contains(&id),
        "agent should be listed after a poll: {fleet}"
    );

    // Remove it, then re-read the fleet WITHOUT polling again.
    let rm_resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/agents/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rm_resp.status(), StatusCode::NO_CONTENT);

    let fleet_after = json_body(
        h.router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/fleet")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;
    assert!(
        !agents_listed(&fleet_after).contains(&id),
        "removed agent must be gone from /api/fleet immediately, not after the next poll: {fleet_after}"
    );
}

#[tokio::test]
async fn agent_input_and_end_input_and_404() {
    use prospero_core::model::AgentStatus;
    use prospero_core::testkit::test_record;

    let mut h = setup().await; // registers "repo" with a FakeCaliband, autostart off
    // An interactive, idle agent with a reachable per-agent socket.
    let mut rec = test_record("ag1", h._runtime.path(), AgentStatus::Idle, false);
    rec.spec.interactive = true;
    h.fake.add_agent(rec, vec![]).await;
    // A non-interactive idle agent — input must be rejected (409).
    let ag2 = test_record("ag2", h._runtime.path(), AgentStatus::Idle, false);
    h.fake.add_agent(ag2, vec![]).await;
    h.manager.poll_repo_once("repo").await;

    // Happy path: POST /input → 202
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/agents/ag1/input")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"text":"also check the tests"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Happy path: POST /end-input (no body) → 202
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/agents/ag1/end-input")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Unknown id → 404
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/agents/nope/input")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"text":"x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Non-interactive agent → 409 (InvalidState).
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/agents/ag2/input")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"text":"x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn serves_dashboard_index() {
    let h = setup().await;
    let resp = h
        .router
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8_lossy(&bytes);
    assert!(html.contains("Prospero"));
}

#[tokio::test]
async fn dashboard_app_js_has_javascript_content_type() {
    let h = setup().await;
    let resp = h
        .router
        .oneshot(
            Request::builder()
                .uri("/app.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(ct.contains("javascript"));
}

// --- Dashboard v2 (Dioxus/WASM bundle, #97) ---------------------------------

/// Fetch `uri` through the real router and return (status, content-type, csp).
async fn head_of(h: &Harness, uri: &str) -> (StatusCode, String, String) {
    let resp = h
        .router
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let get = |name: &str| {
        resp.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string()
    };
    (
        resp.status(),
        get("content-type"),
        get("content-security-policy"),
    )
}

#[tokio::test]
async fn v2_index_serves_html_with_locked_down_csp() {
    let h = setup().await;
    let (status, ct, csp) = head_of(&h, "/v2").await;
    assert_eq!(status, StatusCode::OK);
    assert!(ct.starts_with("text/html"), "content-type was {ct}");
    // The bundle is fully self-contained, so everything is denied by default;
    // 'wasm-unsafe-eval' is the one grant WebAssembly instantiation requires.
    assert!(csp.contains("default-src 'none'"), "csp was: {csp}");
    assert!(csp.contains("'wasm-unsafe-eval'"), "csp was: {csp}");
    assert!(csp.contains("connect-src 'self'"), "csp was: {csp}");
}

#[tokio::test]
async fn v2_serves_wasm_with_the_correct_mime() {
    let h = setup().await;
    // WebAssembly.instantiateStreaming rejects anything but application/wasm.
    let (status, ct, _) = head_of(&h, "/v2/prospero-dashboard_bg.wasm").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ct, "application/wasm");
}

#[tokio::test]
async fn v2_serves_js_glue_and_stylesheet() {
    let h = setup().await;
    let (status, ct, _) = head_of(&h, "/v2/prospero-dashboard.js").await;
    assert_eq!(status, StatusCode::OK);
    assert!(ct.contains("javascript"), "content-type was {ct}");

    let (status, ct, _) = head_of(&h, "/v2/app.css").await;
    assert_eq!(status, StatusCode::OK);
    assert!(ct.starts_with("text/css"), "content-type was {ct}");
}

/// wasm-bindgen emits per-dependency JS snippets under `snippets/`, imported
/// relatively by the glue. Their directory names carry content hashes that
/// change whenever a dependency does, so the bundle is served from a generated
/// asset table rather than a hardcoded file list — if these 404 the app never
/// boots.
#[tokio::test]
async fn v2_serves_the_wasm_bindgen_snippets() {
    let h = setup().await;
    let glue = h
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/prospero-dashboard.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = glue.into_body().collect().await.unwrap().to_bytes();
    let js = String::from_utf8(body.to_vec()).unwrap();

    // Pull every `./snippets/...` import out of the glue and demand each one.
    let mut checked = 0;
    for line in js.lines().filter(|l| l.contains("./snippets/")) {
        let Some(start) = line.find("./snippets/") else {
            continue;
        };
        let rest = &line[start + 2..];
        let Some(end) = rest.find(['\'', '"']) else {
            continue;
        };
        let (status, ct, _) = head_of(&h, &format!("/v2/{}", &rest[..end])).await;
        assert_eq!(status, StatusCode::OK, "missing snippet {}", &rest[..end]);
        assert!(ct.contains("javascript"), "snippet content-type was {ct}");
        checked += 1;
    }
    assert!(
        checked > 0,
        "expected the glue to import at least one snippet"
    );
}

#[tokio::test]
async fn v2_unknown_asset_is_404_not_a_panic() {
    let h = setup().await;
    let (status, _, _) = head_of(&h, "/v2/does-not-exist.js").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // Path traversal cannot escape a static table, but prove it 404s.
    let (status, _, _) = head_of(&h, "/v2/../Cargo.toml").await;
    assert_ne!(status, StatusCode::OK);
}

/// v2 is served alongside v1 during the transition; `/` must be untouched.
#[tokio::test]
async fn v1_dashboard_is_unaffected_by_v2() {
    let h = setup().await;
    let (status, ct, _) = head_of(&h, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(ct.starts_with("text/html"), "content-type was {ct}");
    let (status, ct, _) = head_of(&h, "/app.js").await;
    assert_eq!(status, StatusCode::OK);
    assert!(ct.contains("javascript"), "content-type was {ct}");
}
