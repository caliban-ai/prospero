//! #76: the API served over a `K8sFleet` backend (no `FleetManager`). Proves a
//! non-Unix backend serves the read path via the `FleetProvider` seam and that
//! the workspace-registry routes (`FleetAdmin`, absent under k8s) return 405.
//!
//! Gated on the `prospero-api/k8s` feature (which pulls in `prospero-core/k8s`),
//! so the testkit-only coverage build compiles without it. CI's test gate
//! enables it via `TESTKIT`.
#![cfg(feature = "k8s")]

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use prospero_api::router;
use prospero_core::bus::{EventBus, InProcessBus};
use prospero_core::k8s::fake::FakeK8s;
use prospero_core::store::{JsonlStore, Store};
use prospero_core::{FleetProvider, K8sFleet};
use tower::ServiceExt;

/// A router whose control plane is a `K8sFleet` over an in-memory fake api, with
/// no `FleetAdmin` (as a real k8s daemon wires it: `admin = None`).
fn k8s_router() -> Router {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(JsonlStore::open(dir.path()).unwrap());
    std::mem::forget(dir); // keep the store's backing file alive for the test
    let bus: Arc<dyn EventBus> = Arc::new(InProcessBus::new(64));
    let fleet: Arc<dyn FleetProvider> =
        Arc::new(K8sFleet::new(FakeK8s::new(), bus.clone(), store.clone()));
    router(fleet, None, store, bus)
}

async fn status_of(app: Router, method: &str, uri: &str, body: &str) -> StatusCode {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    app.oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn k8s_backend_serves_the_fleet_snapshot() {
    let app = k8s_router();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fleet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    // The synthetic namespace workspace is present even with no agents.
    assert_eq!(v["workspaces"][0]["name"], "k8s");
}

#[tokio::test]
async fn workspace_registry_routes_return_405_under_k8s() {
    // No `FleetAdmin` on a k8s backend → the registry/config plane is 405.
    assert_eq!(
        status_of(
            k8s_router(),
            "POST",
            "/api/workspaces",
            r#"{"name":"x","root":"/x"}"#
        )
        .await,
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        status_of(k8s_router(), "DELETE", "/api/workspaces/x", "").await,
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        status_of(k8s_router(), "PUT", "/api/workspaces/x/config", "{}").await,
        StatusCode::METHOD_NOT_ALLOWED
    );
}

/// A k8s router with the config plane wired (`FleetAdmin` over a `Workspace`
/// registry), sharing that same registry with `K8sFleet` — mirroring the real
/// daemon wiring. Seeds one registered `Workspace` CR named `ws_name`.
async fn k8s_router_with_registry(ws_name: &str) -> Router {
    use prospero_core::k8s::crd::{Provider, Workspace, WorkspaceSpec};
    use prospero_core::k8s::fake::FakeWorkspaceApi;
    use prospero_core::{FleetAdmin, K8sWorkspaceAdmin, WorkspaceApi};

    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(JsonlStore::open(dir.path()).unwrap());
    std::mem::forget(dir);
    let bus: Arc<dyn EventBus> = Arc::new(InProcessBus::new(64));

    let ws_api = Arc::new(FakeWorkspaceApi::new());
    ws_api
        .apply(&Workspace::new(
            ws_name,
            WorkspaceSpec {
                display_name: format!("{ws_name} display"),
                sources: Vec::new(),
                providers: vec![Provider {
                    name: "ollama".into(),
                    kind: "ollama".into(),
                    base_url: None,
                    model: None,
                    credentials_ref: None,
                }],
                default_provider: None,
                env: Vec::new(),
                isolation: None,
            },
        ))
        .await
        .unwrap();

    let fleet: Arc<dyn FleetProvider> = Arc::new(
        K8sFleet::new(FakeK8s::new(), bus.clone(), store.clone()).with_workspaces(ws_api.clone()),
    );
    let admin: Arc<dyn FleetAdmin> = Arc::new(K8sWorkspaceAdmin::new(ws_api));
    router(fleet, Some(admin), store, bus)
}

async fn json_of(app: Router, uri: &str) -> serde_json::Value {
    let resp = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn fleet_and_workspaces_agree_on_registered_workspace_set() {
    // #149/#151: with the registry wired, `GET /api/fleet` surfaces the
    // registered `Workspace` CR (no synthetic 'k8s' phantom), and it agrees with
    // `GET /api/workspaces` on the workspace set.
    let fleet = json_of(k8s_router_with_registry("team-a").await, "/api/fleet").await;
    let workspaces = json_of(k8s_router_with_registry("team-a").await, "/api/workspaces").await;

    let fleet_names: Vec<&str> = fleet["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["name"].as_str().unwrap())
        .collect();
    let ws_names: Vec<&str> = workspaces
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["name"].as_str().unwrap())
        .collect();

    assert_eq!(
        fleet_names,
        ["team-a"],
        "GET /api/fleet surfaces the registered workspace, not synthetic 'k8s'"
    );
    assert!(
        !fleet_names.contains(&"k8s"),
        "no synthetic 'k8s' phantom in /api/fleet"
    );
    assert_eq!(
        fleet_names, ws_names,
        "/api/fleet and /api/workspaces must agree on the workspace set"
    );
}
