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
        None,
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
    // #219: the fleet-wide stream is a read of the same fleet.
    ("GET", "/api/fleet/stream?from=now", "", Scope::Read),
    // #218: MCP drives the fleet (spawn/steer/kill), so it sits at `operate`.
    ("POST", "/mcp", "", Scope::Operate),
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

/// #222: a client needs the spec *before* it has a credential — it is how the
/// client learns how to authenticate. The document describes the API's shape
/// and carries no fleet data, so serving it open costs nothing the published
/// guide does not already give away.
#[tokio::test]
async fn the_openapi_document_is_served_without_a_credential() {
    let h = setup().await;
    let resp = h
        .app
        .clone()
        .oneshot(request("GET", "/api/openapi.json", None, ""))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let doc: serde_json::Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(doc["openapi"], "3.1.0");
    assert!(
        doc["paths"]["/api/fleet"]["get"].is_object(),
        "the served document should describe the fleet route"
    );
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

/// #251: a client that serves many people (Ariel holds one `operate` token for
/// a whole Discord) can say who a spawn is for. Both identities are recorded —
/// the token prosperod authenticated, and the person the token claimed.
#[tokio::test]
async fn a_spawn_can_name_the_person_it_is_for_beside_the_token() {
    let h = setup().await;
    let req = Request::builder()
        .method("POST")
        .uri("/api/workspaces/repo/agents")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {}", h.operate))
        .header("x-prospero-on-behalf-of", "discord:U123")
        .body(Body::from(r#"{"prompt":"p"}"#.to_string()))
        .unwrap();
    let resp = h.app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let spawned = spawned_event(&h, resp).await;
    assert_eq!(
        spawned.actor.as_deref(),
        Some("ops"),
        "the authenticated token must still be recorded, unchanged"
    );
    assert_eq!(spawned.on_behalf_of.as_deref(), Some("discord:U123"));
}

/// A client that sends nothing must behave exactly as it did before #251.
#[tokio::test]
async fn a_spawn_without_the_header_records_no_subject() {
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
    let spawned = spawned_event(&h, resp).await;
    assert_eq!(spawned.actor.as_deref(), Some("ops"));
    assert_eq!(spawned.on_behalf_of, None);
}

/// The value reaches the event log and the audit log. A newline cannot even be
/// put into an HTTP header — that protection belongs to the transport — but
/// blank, whitespace, tab and over-long values all can be, and prosperod
/// refuses them rather than quietly dropping an attribution the caller
/// believed it had set.
#[tokio::test]
async fn an_unusable_on_behalf_of_is_refused_and_spawns_nothing() {
    let h = setup().await;
    let over_long = "x".repeat(200);
    // An interior tab, not a leading one: surrounding whitespace is trimmed
    // (harmless), but a control character *inside* the value is refused.
    for bad in ["", "   ", "alice\tbob", over_long.as_str()] {
        let req = Request::builder()
            .method("POST")
            .uri("/api/workspaces/repo/agents")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {}", h.operate))
            .header("x-prospero-on-behalf-of", bad)
            .body(Body::from(r#"{"prompt":"p"}"#.to_string()))
            .unwrap();
        let status = h.app.clone().oneshot(req).await.unwrap().status();
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "on-behalf-of {bad:?} should be refused"
        );
    }
}

/// Resolve the `AgentSpawned` event for the agent a spawn response names.
async fn spawned_event(
    h: &Harness,
    resp: axum::response::Response,
) -> prospero_core::event::FleetEvent {
    let v: serde_json::Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let id = v["agent_id"].as_str().unwrap();
    let key = prospero_core::event::stream_key_for("repo", id);
    h.manager
        .store()
        .replay(&key, 0)
        .await
        .unwrap()
        .into_iter()
        .find(|e| matches!(e.kind, prospero_core::EventKind::AgentSpawned))
        .expect("the spawn must have been recorded")
}

fn cookie_from(resp: &axum::response::Response) -> String {
    let set = resp
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    set.split(';').next().unwrap().to_string() // "prospero_session=<value>"
}

async fn json(resp: axum::response::Response) -> serde_json::Value {
    serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

/// #238: choosing `unattended` turns off the permission gate for that session,
/// so it takes more than the `operate` scope an ordinary spawn needs.
#[tokio::test]
async fn unattended_spawn_requires_the_admin_scope() {
    let h = setup().await;

    let resp = h
        .app
        .clone()
        .oneshot(request(
            "POST",
            "/api/workspaces/repo/agents",
            Some(&h.operate),
            r#"{"prompt":"hi","permission_posture":"unattended"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(json(resp).await["error"], "unattended requires admin scope");
}

/// The same request is accepted for an `admin` token, and a supervised spawn
/// still needs only `operate` — the gate is on the posture, not the route.
#[tokio::test]
async fn supervised_spawn_stays_at_operate_and_admin_may_go_unattended() {
    let h = setup().await;

    let s = status(
        &h.app,
        request(
            "POST",
            "/api/workspaces/repo/agents",
            Some(&h.operate),
            r#"{"prompt":"hi"}"#,
        ),
    )
    .await;
    assert!(
        s != StatusCode::UNAUTHORIZED && s != StatusCode::FORBIDDEN,
        "supervised spawn must stay at operate, got {s}"
    );

    let s = status(
        &h.app,
        request(
            "POST",
            "/api/workspaces/repo/agents",
            Some(&h.admin),
            r#"{"prompt":"hi","permission_posture":"unattended"}"#,
        ),
    )
    .await;
    assert!(
        s != StatusCode::UNAUTHORIZED && s != StatusCode::FORBIDDEN,
        "admin must be allowed unattended, got {s}"
    );
}

#[tokio::test]
async fn sign_in_cookie_reads_streams_and_signs_out() {
    let h = setup().await;
    let body = format!(r#"{{"token":"{}"}}"#, h.admin);
    let resp = h
        .app
        .clone()
        .oneshot(request("POST", "/api/session", None, &body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let set = resp
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(set.contains("HttpOnly") && set.contains("SameSite=Strict") && !set.contains("Secure"));
    let cookie = cookie_from(&resp);
    let info = json(resp).await;
    assert_eq!(info["auth"], "token");
    assert_eq!(info["token_name"], "root");
    assert_eq!(info["scope"], "admin");
    assert!(info["expires_at"].is_string());

    let with_cookie = |method: &str, uri: &str| {
        let mut r = request(method, uri, None, "");
        r.headers_mut()
            .insert(header::COOKIE, cookie.parse().unwrap());
        r
    };
    assert_eq!(
        status(&h.app, with_cookie("GET", "/api/fleet")).await,
        StatusCode::OK
    );
    assert_eq!(
        status(&h.app, with_cookie("GET", "/api/agents/nope/stream")).await,
        StatusCode::OK
    );
    let who = json(
        h.app
            .clone()
            .oneshot(with_cookie("GET", "/api/session"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(who["token_name"], "root");

    let out = h
        .app
        .clone()
        .oneshot(with_cookie("DELETE", "/api/session"))
        .await
        .unwrap();
    assert_eq!(out.status(), StatusCode::NO_CONTENT);
    assert!(
        out.headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
}

#[tokio::test]
async fn bad_sign_in_is_401_and_https_proxy_sets_secure() {
    let h = setup().await;
    assert_eq!(
        status(
            &h.app,
            request("POST", "/api/session", None, r#"{"token":"pspo_nope"}"#)
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    let mut r = request(
        "POST",
        "/api/session",
        None,
        &format!(r#"{{"token":"{}"}}"#, h.read),
    );
    r.headers_mut()
        .insert("x-forwarded-proto", "https".parse().unwrap());
    let resp = h.app.clone().oneshot(r).await.unwrap();
    assert!(
        resp.headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("; Secure")
    );
}

#[tokio::test]
async fn cookie_mutations_must_be_same_origin_but_bearer_need_not_be() {
    let h = setup().await;
    let resp = h
        .app
        .clone()
        .oneshot(request(
            "POST",
            "/api/session",
            None,
            &format!(r#"{{"token":"{}"}}"#, h.operate),
        ))
        .await
        .unwrap();
    let cookie = cookie_from(&resp);

    let mut cross = request("POST", "/api/agents/nope/kill", None, "");
    cross
        .headers_mut()
        .insert(header::COOKIE, cookie.parse().unwrap());
    cross
        .headers_mut()
        .insert(header::ORIGIN, "https://evil.example".parse().unwrap());
    cross
        .headers_mut()
        .insert(header::HOST, "prospero.example".parse().unwrap());
    let resp = h.app.clone().oneshot(cross).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(json(resp).await["error"], "cross-origin request");

    let mut same = request("POST", "/api/agents/nope/kill", None, "");
    same.headers_mut()
        .insert(header::COOKIE, cookie.parse().unwrap());
    same.headers_mut()
        .insert("sec-fetch-site", "same-origin".parse().unwrap());
    let s = status(&h.app, same).await;
    assert!(
        s != StatusCode::UNAUTHORIZED && s != StatusCode::FORBIDDEN,
        "{s}"
    );

    let s = status(
        &h.app,
        request("POST", "/api/agents/nope/kill", Some(&h.operate), ""),
    )
    .await;
    assert!(
        s != StatusCode::UNAUTHORIZED && s != StatusCode::FORBIDDEN,
        "{s}"
    );
}

#[tokio::test]
async fn session_endpoints_when_auth_is_disabled() {
    let h = setup().await;
    let local = LocalFleet::new(h.manager.clone());
    let open = prospero_api::router(
        Arc::new(local.clone()),
        Some(Arc::new(local)),
        h.manager.store(),
        h.manager.bus(),
    );
    let v = json(
        open.clone()
            .oneshot(request("GET", "/api/session", None, ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(v, serde_json::json!({"auth": "disabled"}));
    assert_eq!(
        status(
            &open,
            request("POST", "/api/session", None, r#"{"token":"x"}"#)
        )
        .await,
        StatusCode::NOT_FOUND
    );
}
