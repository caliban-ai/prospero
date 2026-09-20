//! Automations over the real router (#220).
//!
//! Drives the HTTP surface a client actually meets — routing, extractors,
//! status codes and the signature header — against a real `LocalFleet` over
//! `FakeCaliband`. The scheduling rules themselves are covered by unit tests
//! in `prospero-core`; what is proved here is that the wiring between them
//! and the wire is intact.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use prospero_core::LocalFleet;
use prospero_core::automation::AutomationEngine;
use prospero_core::config_store::{ConfigStore, SqliteConfigStore};
use prospero_core::discovery::{DiscoveryEnv, EnsureConfig, control_socket_path};
use prospero_core::fleet::{FleetConfig, FleetManager};
use prospero_core::store::JsonlStore;
use prospero_core::testkit::FakeCaliband;
use tower::ServiceExt;

struct Harness {
    app: Router,
    fake: FakeCaliband,
    _dirs: [tempfile::TempDir; 3],
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
    let fake = FakeCaliband::start_at(&control_socket_path(&repo_root, &env))
        .await
        .unwrap();
    let mut config = FleetConfig::new("automation-host", data_dir.path());
    config.discovery_env = env;
    config.ensure = EnsureConfig {
        autostart: false,
        ..EnsureConfig::default()
    };
    config.poll_interval = Duration::from_millis(20);
    let store = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let config_store: Arc<dyn ConfigStore> =
        Arc::new(SqliteConfigStore::open(data_dir.path()).await.unwrap());
    let manager = FleetManager::with_config_store(config, store, config_store.clone())
        .await
        .unwrap();
    manager.add_repo("repo", repo_root).await.unwrap();

    let local = LocalFleet::new(manager.clone());
    let fleet: Arc<dyn prospero_core::FleetProvider> = Arc::new(local.clone());
    let engine = Arc::new(AutomationEngine::new(config_store, fleet.clone()));
    let app = prospero_api::router_with_automations(
        fleet,
        Some(Arc::new(local)),
        manager.store(),
        manager.bus(),
        engine,
    );
    Harness {
        app,
        fake,
        _dirs: [repo_dir, runtime_dir, data_dir],
    }
}

async fn send(app: &Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, body)
}

fn post(path: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn get(path: &str) -> Request<Body> {
    Request::builder().uri(path).body(Body::empty()).unwrap()
}

fn sign(secret: &str, body: &[u8]) -> String {
    use hmac::Mac as _;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

fn webhook_automation(task: &str) -> serde_json::Value {
    serde_json::json!({
        "id": "on-issue",
        "workspace": "repo",
        "trigger": { "kind": "webhook" },
        "template": { "task": task },
    })
}

#[tokio::test]
async fn a_created_automation_is_listed_without_its_secret() {
    let h = setup().await;

    let (status, created) = send(&h.app, post("/api/automations", webhook_automation("go"))).await;
    assert_eq!(status, StatusCode::CREATED);
    let secret = created["webhook_secret"]
        .as_str()
        .expect("creation returns the signing key once");
    assert!(!secret.is_empty());

    // The listing is built from the wire type, which has no secret field at
    // all — so this is a structural guarantee, not a redaction step.
    let (status, listed) = send(&h.app, get("/api/automations")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["id"], "on-issue");
    assert!(
        listed[0].get("webhook_secret").is_none(),
        "a listing must never carry the signing key: {listed}"
    );
    assert!(
        !serde_json::to_string(&listed).unwrap().contains(secret),
        "the key leaked into the listing"
    );
}

#[tokio::test]
async fn a_duplicate_id_is_refused_rather_than_rotating_the_key() {
    let h = setup().await;
    let (status, _) = send(&h.app, post("/api/automations", webhook_automation("go"))).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _) = send(&h.app, post("/api/automations", webhook_automation("go"))).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "re-creating an id would silently invalidate the live signing key"
    );
}

#[tokio::test]
async fn an_unparseable_schedule_is_rejected_at_creation() {
    let h = setup().await;
    let (status, body) = send(
        &h.app,
        post(
            "/api/automations",
            serde_json::json!({
                "id": "bad",
                "workspace": "repo",
                "trigger": { "kind": "cron", "schedule": "not a schedule" },
                "template": { "task": "go" },
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("not a schedule"),
        "the error should quote what was rejected: {body}"
    );
}

#[tokio::test]
async fn a_signed_webhook_spawns_and_an_unsigned_one_does_not() {
    let h = setup().await;
    let (_, created) = send(
        &h.app,
        post(
            "/api/automations",
            webhook_automation("Fix {{ issue.title }}"),
        ),
    )
    .await;
    let secret = created["webhook_secret"].as_str().unwrap().to_string();
    let body = br#"{"issue":{"title":"the bug"}}"#;

    // Unsigned first, so a later success cannot be mistaken for this one.
    let (status, _) = send(
        &h.app,
        post(
            "/api/automations/on-issue/trigger",
            serde_json::json!({ "issue": { "title": "the bug" } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(h.fake.received_specs().is_empty());

    let signed = Request::builder()
        .method("POST")
        .uri("/api/automations/on-issue/trigger")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-prospero-signature", sign(&secret, body))
        .body(Body::from(body.to_vec()))
        .unwrap();
    let (status, fired) = send(&h.app, signed).await;
    assert_eq!(status, StatusCode::OK);
    assert!(fired["run"]["agent_id"].is_string(), "{fired}");

    let specs = h.fake.received_specs();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].initial_prompt, "Fix the bug");

    // The run is visible in history.
    let (status, runs) = send(&h.app, get("/api/automations/on-issue/runs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(runs.as_array().unwrap().len(), 1);
    assert_eq!(runs[0]["source"], "webhook");
}

#[tokio::test]
async fn a_disabled_automation_refuses_to_fire() {
    let h = setup().await;
    let (_, created) = send(&h.app, post("/api/automations", webhook_automation("go"))).await;
    let secret = created["webhook_secret"].as_str().unwrap().to_string();

    let disable = Request::builder()
        .method("PUT")
        .uri("/api/automations/on-issue/enabled")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"enabled":false}"#))
        .unwrap();
    let (status, _) = send(&h.app, disable).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let body = b"{}";
    let signed = Request::builder()
        .method("POST")
        .uri("/api/automations/on-issue/trigger")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-prospero-signature", sign(&secret, body))
        .body(Body::from(body.to_vec()))
        .unwrap();
    let (status, _) = send(&h.app, signed).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "a correctly signed request must still not fire a disabled automation"
    );
    assert!(h.fake.received_specs().is_empty());
}

#[tokio::test]
async fn run_now_fires_a_cron_automation_off_schedule() {
    let h = setup().await;
    let (status, _) = send(
        &h.app,
        post(
            "/api/automations",
            serde_json::json!({
                "id": "nightly",
                "workspace": "repo",
                "trigger": { "kind": "cron", "schedule": "0 3 * * *" },
                "template": { "task": "sweep the logs" },
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, fired) = send(
        &h.app,
        post("/api/automations/nightly/run", serde_json::json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fired["run"]["source"], "manual");
    assert_eq!(h.fake.received_specs().len(), 1);
}

#[tokio::test]
async fn deleting_an_automation_removes_it_and_its_runs() {
    let h = setup().await;
    send(&h.app, post("/api/automations", webhook_automation("go"))).await;
    send(
        &h.app,
        post("/api/automations/on-issue/run", serde_json::json!({})),
    )
    .await;
    assert_eq!(
        send(&h.app, get("/api/automations/on-issue/runs"))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let del = Request::builder()
        .method("DELETE")
        .uri("/api/automations/on-issue")
        .body(Body::empty())
        .unwrap();
    let (status, _) = send(&h.app, del).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    assert!(
        send(&h.app, get("/api/automations"))
            .await
            .1
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        send(&h.app, get("/api/automations/on-issue/runs"))
            .await
            .1
            .as_array()
            .unwrap()
            .is_empty(),
        "run history must not outlive the automation it belongs to"
    );
}

#[tokio::test]
async fn an_unknown_automation_is_a_404_not_a_500() {
    let h = setup().await;
    let (status, _) = send(&h.app, get("/api/automations/ghost/runs")).await;
    assert_eq!(status, StatusCode::OK, "history of an unknown id is empty");

    let (status, _) = send(
        &h.app,
        post("/api/automations/ghost/run", serde_json::json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let del = Request::builder()
        .method("DELETE")
        .uri("/api/automations/ghost")
        .body(Body::empty())
        .unwrap();
    assert_eq!(send(&h.app, del).await.0, StatusCode::NOT_FOUND);
}
