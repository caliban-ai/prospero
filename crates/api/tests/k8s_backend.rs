//! #76: the API served over a `K8sFleet` backend (no `FleetManager`). Proves a
//! non-Unix backend serves the read path via the `FleetProvider` seam and that
//! the workspace-registry routes (`FleetAdmin`, absent under k8s) return 405.
//!
//! Requires the `prospero-core/testkit` + `prospero-core/k8s` features (CI
//! passes both), same as `api_integration.rs` requires `testkit`.

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
