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

use axum::Router;
use axum::routing::{delete, get, post, put};
use prospero_core::{FleetManager, LocalFleet};

/// Shared application state handed to every handler.
#[derive(Clone)]
pub struct AppState {
    /// The fleet control plane. Almost every handler (fleet/repo listing,
    /// agent lookup, kill/respawn, the session plane) still talks to this
    /// directly — unchanged in P1.
    pub manager: FleetManager,
    /// The `FleetProvider` seam, wrapping the same `manager`. Only the
    /// spawn/ensure path is routed through it today; see
    /// `handlers::spawn_agent`.
    pub fleet: LocalFleet,
}

/// Build the application router over the given fleet manager and its
/// `FleetProvider` wrapper (constructed once, at the daemon's composition
/// edge — see `prospero-daemon`'s `main.rs`).
pub fn router(manager: FleetManager, fleet: LocalFleet) -> Router {
    let state = AppState { manager, fleet };
    Router::new()
        // Dashboard.
        .route("/", get(dashboard::index))
        .route("/app.js", get(dashboard::app_js))
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/api/metrics", get(handlers::get_metrics))
        // Fleet + repos.
        .route("/api/fleet", get(handlers::get_fleet))
        .route(
            "/api/repos",
            get(handlers::get_repos).post(handlers::add_repo),
        )
        .route("/api/repos/{name}", delete(handlers::delete_repo))
        .route("/api/repos/{name}/config", put(handlers::set_repo_config))
        .route(
            "/api/repos/{repo}/agents",
            get(handlers::get_repo_agents).post(handlers::spawn_agent),
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
