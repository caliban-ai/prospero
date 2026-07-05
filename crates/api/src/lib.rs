//! HTTP API and embedded web dashboard for Prospero.
//!
//! Turns a [`FleetManager`] into a control surface: REST endpoints for the
//! fleet, repos, and agents; a Server-Sent-Events stream per agent
//! (replay-then-tail); and a static dashboard. The CLI and the browser both
//! talk to this one surface.

pub mod dashboard;
pub mod dto;
pub mod error;
pub mod handlers;
pub mod sse;

use std::sync::Arc;

use axum::Router;
use axum::routing::{delete, get, post, put};
use prospero_core::bus::EventBus;
use prospero_core::store::Store;
use prospero_core::{FleetAdmin, FleetProvider};

/// Shared application state handed to every handler. Backend-agnostic (#76):
/// the control plane is a `FleetProvider`, the workspace-registry plane an
/// optional `FleetAdmin` (`None` under k8s → those routes 405), and
/// observability (history/SSE) reads the shared `Store`/`EventBus` directly.
#[derive(Clone)]
pub struct AppState {
    /// The fleet control plane (ensure/stop/restart/snapshot/readiness/metrics/
    /// remove_agent/send_input). `LocalFleet` or `K8sFleet`.
    pub fleet: Arc<dyn FleetProvider>,
    /// The workspace-registry/config plane. `Some` for local; `None` for k8s.
    pub admin: Option<Arc<dyn FleetAdmin>>,
    /// Shared durable event store — agent history reads route here.
    pub store: Arc<dyn Store>,
    /// Shared event bus — SSE subscribe routes here.
    pub bus: Arc<dyn EventBus>,
}

/// Build the application router over the backend seams (constructed once, at the
/// daemon's composition edge — see `prospero-daemon`'s `main.rs`).
pub fn router(
    fleet: Arc<dyn FleetProvider>,
    admin: Option<Arc<dyn FleetAdmin>>,
    store: Arc<dyn Store>,
    bus: Arc<dyn EventBus>,
) -> Router {
    let state = AppState {
        fleet,
        admin,
        store,
        bus,
    };
    Router::new()
        // Dashboard.
        .route("/", get(dashboard::index))
        .route("/app.js", get(dashboard::app_js))
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/api/metrics", get(handlers::get_metrics))
        // Fleet + workspaces.
        .route("/api/fleet", get(handlers::get_fleet))
        .route(
            "/api/workspaces",
            get(handlers::get_workspaces).post(handlers::add_workspace),
        )
        .route("/api/workspaces/{name}", delete(handlers::delete_workspace))
        .route(
            "/api/workspaces/{name}/config",
            put(handlers::set_workspace_config),
        )
        .route(
            "/api/workspaces/{workspace}/agents",
            get(handlers::get_workspace_agents).post(handlers::spawn_agent),
        )
        // Agents.
        .route(
            "/api/agents/{id}",
            get(handlers::get_agent).delete(handlers::rm_agent),
        )
        .route("/api/agents/{id}/events", get(handlers::get_agent_events))
        .route("/api/agents/{id}/stream", get(sse::agent_stream))
        .route("/api/agents/{id}/kill", post(handlers::kill_agent))
        .route("/api/agents/{id}/respawn", post(handlers::respawn_agent))
        .route("/api/agents/{id}/input", post(handlers::agent_input))
        .route(
            "/api/agents/{id}/end-input",
            post(handlers::agent_end_input),
        )
        .with_state(state)
}
