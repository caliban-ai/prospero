//! HTTP API and embedded web dashboard for Prospero.
//!
//! Turns a [`FleetManager`] into a control surface: REST endpoints for the
//! fleet, repos, and agents; a Server-Sent-Events stream per agent
//! (replay-then-tail); and a static dashboard. The CLI and the browser both
//! talk to this one surface.

pub mod auth;
pub mod automations;
pub mod dashboard;
pub mod dto;
pub mod error;
pub mod handlers;
pub mod mcp;
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
    /// Inbound auth configuration (#2).
    pub auth: Arc<auth::AuthState>,
    /// The automation runtime (#220). `None` when the backend has no shared
    /// config store to hold automations in — those routes then 405 rather
    /// than pretending to schedule something nothing will fire.
    pub automations: Option<Arc<prospero_core::automation::AutomationEngine>>,
}

/// Build the application router over the backend seams (constructed once, at the
/// daemon's composition edge — see `prospero-daemon`'s `main.rs`).
pub fn router_with_auth(
    fleet: Arc<dyn FleetProvider>,
    admin: Option<Arc<dyn FleetAdmin>>,
    store: Arc<dyn Store>,
    bus: Arc<dyn EventBus>,
    automations: Option<Arc<prospero_core::automation::AutomationEngine>>,
    auth: auth::AuthState,
) -> Router {
    let auth = Arc::new(auth);
    // #218: the MCP service holds its own handles to the same seams — the
    // state below moves them.
    let mcp_service = mcp::service(fleet.clone(), store.clone());
    let state = AppState {
        fleet,
        admin,
        store,
        bus,
        auth: auth.clone(),
        automations,
    };
    Router::new()
        // The dashboard (Dioxus/WASM, #97) is the only UI: the document at
        // `/`, its files under `/assets/`. It shared the server with a
        // vanilla-JS page until #197 removed that, and the version-suffixed
        // `/v1` and `/v2` paths went with it.
        //
        // Assets sit under a prefix rather than at the root so the bundle
        // cannot shadow the API namespace. The catch-all covers the JS glue,
        // the .wasm, the stylesheet, and wasm-bindgen's hashed `snippets/`
        // tree, whose filenames change with every dependency bump.
        .route("/", get(dashboard::index))
        .route("/assets/{*path}", get(dashboard::asset))
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route(
            "/api/session",
            get(auth::handlers::get_session)
                .post(auth::handlers::post_session)
                .delete(auth::handlers::delete_session),
        )
        .route("/api/metrics", get(handlers::get_metrics))
        .route("/api/capabilities", get(handlers::get_capabilities))
        // Fleet + workspaces.
        .route("/api/fleet", get(handlers::get_fleet))
        .route("/api/fleet/stream", get(sse::fleet_stream))
        // #218: the fleet as an MCP server, behind the same scope check as the
        // REST routes (see `auth::required_access`).
        .nest_service("/mcp", mcp_service)
        .route("/api/usage", get(handlers::get_usage))
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
        // Automations (#220).
        .route(
            "/api/automations",
            get(automations::list_automations).post(automations::create_automation),
        )
        .route(
            "/api/automations/{id}",
            delete(automations::delete_automation),
        )
        .route(
            "/api/automations/{id}/enabled",
            put(automations::set_enabled),
        )
        .route("/api/automations/{id}/runs", get(automations::list_runs))
        .route("/api/automations/{id}/run", post(automations::run_now))
        .route("/api/automations/{id}/trigger", post(automations::trigger))
        .route_layer(axum::middleware::from_fn_with_state(auth, auth::middleware))
        .with_state(state)
}

/// Build the router with authentication disabled (tests, loopback dev).
pub fn router(
    fleet: Arc<dyn FleetProvider>,
    admin: Option<Arc<dyn FleetAdmin>>,
    store: Arc<dyn Store>,
    bus: Arc<dyn EventBus>,
) -> Router {
    router_with_auth(fleet, admin, store, bus, None, auth::AuthState::disabled())
}

/// Build the router with automations wired and authentication disabled.
pub fn router_with_automations(
    fleet: Arc<dyn FleetProvider>,
    admin: Option<Arc<dyn FleetAdmin>>,
    store: Arc<dyn Store>,
    bus: Arc<dyn EventBus>,
    automations: Arc<prospero_core::automation::AutomationEngine>,
) -> Router {
    router_with_auth(
        fleet,
        admin,
        store,
        bus,
        Some(automations),
        auth::AuthState::disabled(),
    )
}
