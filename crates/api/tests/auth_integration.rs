//! Inbound API auth over the real router (#2): every route's scope, 401/403
//! shapes, open probes, and actor attribution on spawn.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use prospero_api::auth::{AuthState, SessionKey};
use prospero_api::router_with_auth;
use prospero_core::auth::{TokenSet, generate_token, tokens_file_line};
use prospero_core::discovery::{DiscoveryEnv, EnsureConfig, control_socket_path};
use prospero_core::fleet::{FleetConfig, FleetManager};
use prospero_core::store::JsonlStore;
use prospero_core::testkit::FakeCaliband;
use prospero_core::{LocalFleet, Scope};
use tower::ServiceExt;

pub struct Harness {
    pub app: Router,
    pub manager: FleetManager,
    pub read: String,
    pub operate: String,
    pub admin: String,
    _fake: FakeCaliband,
    _dirs: [tempfile::TempDir; 3],
}

pub async fn setup() -> Harness {
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
    let mut config = FleetConfig::new("auth-host", data_dir.path());
    config.discovery_env = env;
    config.ensure = EnsureConfig {
        autostart: false,
        ..EnsureConfig::default()
    };
    config.poll_interval = Duration::from_millis(20);
    let store = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let manager = FleetManager::new(config, store).await.unwrap();
    manager.add_repo("repo", repo_root).await.unwrap();

    let (read, operate, admin) = (generate_token(), generate_token(), generate_token());
    let file = [
        tokens_file_line("viewer", Scope::Read, &read),
        tokens_file_line("ops", Scope::Operate, &operate),
        tokens_file_line("root", Scope::Admin, &admin),
    ]
    .join("\n");
    let auth = AuthState::enabled(TokenSet::parse(&file).unwrap(), SessionKey::random(), false);
    let local = LocalFleet::new(manager.clone());
    let app = router_with_auth(
        Arc::new(local.clone()),
        Some(Arc::new(local)),
        manager.store(),
        manager.bus(),
        auth,
    );
    Harness {
        app,
        manager,
        read,
        operate,
        admin,
        _fake: fake,
        _dirs: [repo_dir, runtime_dir, data_dir],
    }
}

pub fn request(method: &str, uri: &str, bearer: Option<&str>, body: &str) -> Request<Body> {
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(t) = bearer {
        b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    b.body(Body::from(body.to_string())).unwrap()
}

async fn status(app: &Router, req: Request<Body>) -> StatusCode {
    app.clone().oneshot(req).await.unwrap().status()
}

/// (method, uri, body, minimum scope)
const PROTECTED: &[(&str, &str, &str, Scope)] = &[
    ("GET", "/api/metrics", "", Scope::Read),
    ("GET", "/api/capabilities", "", Scope::Read),
    ("GET", "/api/fleet", "", Scope::Read),
    ("GET", "/api/usage", "", Scope::Read),
    ("GET", "/api/workspaces", "", Scope::Read),
    ("GET", "/api/workspaces/repo/agents", "", Scope::Read),
    ("GET", "/api/agents/nope", "", Scope::Read),
    ("GET", "/api/agents/nope/events", "", Scope::Read),
    ("GET", "/api/agents/nope/stream", "", Scope::Read),
    (
        "POST",
        "/api/workspaces/repo/agents",
        r#"{"prompt":"p"}"#,
        Scope::Operate,
    ),
    ("POST", "/api/agents/nope/kill", "", Scope::Operate),
    ("POST", "/api/agents/nope/respawn", "", Scope::Operate),
    (
        "POST",
        "/api/agents/nope/input",
        r#"{"text":"hi"}"#,
        Scope::Operate,
    ),
    ("POST", "/api/agents/nope/end-input", "", Scope::Operate),
    ("DELETE", "/api/agents/nope", "", Scope::Operate),
    (
        "POST",
        "/api/workspaces",
        r#"{"name":"w2","root":"/nonexistent"}"#,
        Scope::Admin,
    ),
    ("DELETE", "/api/workspaces/nope", "", Scope::Admin),
    ("PUT", "/api/workspaces/nope/config", "{}", Scope::Admin),
];

#[tokio::test]
async fn every_protected_route_enforces_its_scope() {
    let h = setup().await;
    let token_for = |s: Scope| match s {
        Scope::Read => h.read.as_str(),
        Scope::Operate => h.operate.as_str(),
        Scope::Admin => h.admin.as_str(),
    };
    for &(method, uri, body, need) in PROTECTED {
        assert_eq!(
            status(&h.app, request(method, uri, None, body)).await,
            StatusCode::UNAUTHORIZED,
            "anonymous {method} {uri}"
        );
        assert_eq!(
            status(&h.app, request(method, uri, Some("pspo_wrong"), body)).await,
            StatusCode::UNAUTHORIZED,
            "bad token {method} {uri}"
        );
        for lower in [Scope::Read, Scope::Operate]
            .into_iter()
            .filter(|s| *s < need)
        {
            assert_eq!(
                status(&h.app, request(method, uri, Some(token_for(lower)), body)).await,
                StatusCode::FORBIDDEN,
                "{lower:?} on {method} {uri}"
            );
        }
        let ok = status(&h.app, request(method, uri, Some(token_for(need)), body)).await;
        assert!(
            ok != StatusCode::UNAUTHORIZED && ok != StatusCode::FORBIDDEN,
            "{need:?} on {method} {uri} got {ok}"
        );
    }
}

#[tokio::test]
async fn probes_and_dashboard_shell_are_open() {
    let h = setup().await;
    for uri in ["/healthz", "/readyz", "/"] {
        let s = status(&h.app, request("GET", uri, None, "")).await;
        assert!(
            s != StatusCode::UNAUTHORIZED && s != StatusCode::FORBIDDEN,
            "{uri}: {s}"
        );
    }
}

#[tokio::test]
async fn unauthorized_and_forbidden_bodies_follow_the_spec() {
    let h = setup().await;
    let resp = h
        .app
        .clone()
        .oneshot(request("GET", "/api/fleet", None, ""))
        .await
        .unwrap();
    assert_eq!(
        resp.headers().get(header::WWW_AUTHENTICATE).unwrap(),
        "Bearer realm=\"prospero\""
    );
    let v: serde_json::Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"error": "unauthorized", "kind": "unauthorized"})
    );

    let resp = h
        .app
        .clone()
        .oneshot(request("POST", "/api/workspaces", Some(&h.operate), "{}"))
        .await
        .unwrap();
    let v: serde_json::Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"error": "requires scope admin", "kind": "forbidden"})
    );
}

#[tokio::test]
async fn spawn_over_http_records_the_token_name_as_actor() {
    let h = setup().await;
    let resp = h
        .app
        .clone()
        .oneshot(request(
            "POST",
            "/api/workspaces/repo/agents",
            Some(&h.operate),
            r#"{"prompt":"p"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let v: serde_json::Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let id = v["agent_id"].as_str().unwrap();
    let key = prospero_core::event::stream_key_for("repo", id);
    let events = h.manager.store().replay(&key, 0).await.unwrap();
    let spawned = events
        .iter()
        .find(|e| matches!(e.kind, prospero_core::EventKind::AgentSpawned))
        .unwrap();
    assert_eq!(spawned.actor.as_deref(), Some("ops"));
}
