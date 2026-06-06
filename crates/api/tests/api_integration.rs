//! In-process API tests: drive the axum `Router` with `oneshot` (no real port)
//! over a `FakeCaliband`-backed `FleetManager`.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use prospero_api::router;
use prospero_core::discovery::{DiscoveryEnv, EnsureConfig, control_socket_path};
use prospero_core::fleet::{FleetConfig, FleetManager};
use prospero_core::model::AgentStatus;
use prospero_core::store::JsonlStore;
use prospero_core::testkit::{FakeCaliband, test_record};
use tower::ServiceExt;

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
    let manager = FleetManager::new(config, store).unwrap();
    manager.add_repo("repo", repo_root).await.unwrap();

    Harness {
        router: router(manager.clone()),
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
            vec![serde_json::json!({"type":"text","delta":"hi from api"})],
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
