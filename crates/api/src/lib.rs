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
use prospero_core::FleetManager;

/// Shared application state handed to every handler.
#[derive(Clone)]
pub struct AppState {
    /// The fleet control plane.
    pub manager: FleetManager,
}

/// Build the application router over the given fleet manager.
pub fn router(manager: FleetManager) -> Router {
    let state = AppState { manager };
    Router::new()
        // Dashboard.
        .route("/", get(dashboard::index))
        .route("/app.js", get(dashboard::app_js))
        .route("/healthz", get(handlers::healthz))
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
