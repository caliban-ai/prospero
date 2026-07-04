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
        router: router(manager.clone(), LocalFleet::new(manager.clone())),
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
    assert_eq!(v["repos_total"], 1);
}

#[tokio::test]
async fn readyz_returns_503_when_store_unwritable() {
    let data_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(UnwritableStore(JsonlStore::open(data_dir.path()).unwrap()));
    let config = FleetConfig::new("test-host", data_dir.path());
    let manager = FleetManager::new(config, store).await.unwrap();
    let app = router(manager.clone(), LocalFleet::new(manager));

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
    assert_eq!(v["repos"][0]["name"], "repo");
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
                .uri("/api/repos/repo/agents")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"prompt":"do it"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let v = json_body(resp).await;
    assert_eq!(v["isolated"], true);
    assert_eq!(v["repo"], "repo");
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
                .uri("/api/repos/repo/agents")
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
                .uri("/api/repos/repo/agents")
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
    // Give the attach task a moment to stream + persist.
    tokio::time::sleep(Duration::from_millis(200)).await;

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
        arr.iter()
            .any(|e| e["kind"]["kind"] == "output" && e["kind"]["chunk"] == "hi from api")
    );
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
    tokio::time::sleep(Duration::from_millis(200)).await;

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
    let app = router(manager.clone(), LocalFleet::new(manager));

    // POST /api/repos with a config object.
    let post_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"p","root":"/tmp/p","config":{"provider":"ollama","base_url":"http://h:11434"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(post_resp.status(), StatusCode::CREATED);

    // GET /api/repos should include the config fields.
    let get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/repos")
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
    let fp = fleet["repos"]
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
                .uri("/api/repos/repo/config")
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
                .uri("/api/repos")
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
                .uri("/api/repos/nope/config")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"provider":"ollama"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
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
