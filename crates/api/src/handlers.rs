//! REST endpoint handlers over the `FleetManager`.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use prospero_core::AttachInbound;
use prospero_core::FleetProvider;
use prospero_core::model::{Agent, FleetSnapshot, TaskSpec};

use crate::AppState;
use crate::dto::{
    AddWorkspaceBody, AgentInputBody, FromSeq, RespawnedResponse, SetConfigBody, SpawnBody,
    SpawnedResponse, WorkspaceSummary,
};
use crate::error::ApiError;

/// `GET /api/fleet` — the whole fleet snapshot.
pub async fn get_fleet(State(st): State<AppState>) -> Json<FleetSnapshot> {
    Json(st.manager.snapshot().await)
}

/// `GET /api/workspaces` — managed workspaces with health, sources, agent counts.
pub async fn get_workspaces(State(st): State<AppState>) -> Json<Vec<WorkspaceSummary>> {
    let snap = st.manager.snapshot().await;
    let out = snap
        .workspaces
        .into_iter()
        .map(|r| WorkspaceSummary {
            name: r.name,
            root: r.root.display().to_string(),
            sources: r.sources,
            health: r.health,
            agent_count: r.agents.len(),
            config: r.config,
        })
        .collect();
    Json(out)
}

/// `POST /api/workspaces` — register a workspace.
pub async fn add_workspace(
    State(st): State<AppState>,
    Json(body): Json<AddWorkspaceBody>,
) -> Result<StatusCode, ApiError> {
    st.manager
        .add_workspace_with_config(body.name, body.root, body.config)
        .await?;
    Ok(StatusCode::CREATED)
}

/// `PUT /api/workspaces/{name}/config` — set provider config and restart caliband.
pub async fn set_workspace_config(
    State(st): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<SetConfigBody>,
) -> Result<StatusCode, ApiError> {
    st.manager.set_repo_config(&name, body.0).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /api/workspaces/{name}` — unregister a workspace.
pub async fn delete_workspace(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    if st.manager.remove_repo(&name).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(prospero_core::CoreError::WorkspaceNotFound(name).into())
    }
}

/// `GET /api/workspaces/{workspace}/agents` — agents under one workspace.
pub async fn get_workspace_agents(
    State(st): State<AppState>,
    Path(workspace): Path<String>,
) -> Result<Json<Vec<Agent>>, ApiError> {
    let snap = st.manager.snapshot().await;
    match snap.workspaces.into_iter().find(|r| r.name == workspace) {
        Some(r) => Ok(Json(r.agents)),
        None => Err(prospero_core::CoreError::WorkspaceNotFound(workspace).into()),
    }
}

/// `POST /api/workspaces/{workspace}/agents` — spawn an agent (worktree by default).
///
/// Routed through the `FleetProvider` seam: `LocalFleet::ensure_agent`
/// delegates to the same `FleetManager::spawn_agent` this handler called
/// directly before, so behavior is unchanged.
pub async fn spawn_agent(
    State(st): State<AppState>,
    Path(workspace): Path<String>,
    Json(body): Json<SpawnBody>,
) -> Result<(StatusCode, Json<SpawnedResponse>), ApiError> {
    let req = body.into_request();
    let isolated = req.isolation_worktree;
    let handle = st
        .fleet
        .ensure_agent(TaskSpec {
            workspace: workspace.clone(),
            request: req,
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(SpawnedResponse {
            agent_id: handle.id.to_string(),
            workspace,
            isolated,
        }),
    ))
}

/// `GET /api/agents/{id}` — one agent's current projection.
pub async fn get_agent(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Agent>, ApiError> {
    let snap = st.manager.snapshot().await;
    match snap.find_agent(&id) {
        Some((_, agent)) => Ok(Json(agent.clone())),
        None => Err(prospero_core::CoreError::AgentNotFound(id).into()),
    }
}

/// `GET /api/agents/{id}/events?from=N` — replay history from the store.
pub async fn get_agent_events(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<FromSeq>,
) -> Result<Json<Vec<prospero_core::FleetEvent>>, ApiError> {
    Ok(Json(st.manager.history(&id, q.from).await?))
}

/// `POST /api/agents/{id}/kill`.
pub async fn kill_agent(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    st.manager.kill_agent(&id).await?;
    Ok(StatusCode::ACCEPTED)
}

/// `POST /api/agents/{id}/respawn`.
pub async fn respawn_agent(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RespawnedResponse>, ApiError> {
    let agent_id = st.manager.respawn_agent(&id).await?;
    Ok(Json(RespawnedResponse { agent_id }))
}

/// `POST /api/agents/{id}/input` — inject a user message into an interactive agent.
pub async fn agent_input(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<AgentInputBody>,
) -> Result<StatusCode, ApiError> {
    st.manager
        .send_agent_input(&id, AttachInbound::UserMessage { text: body.text })
        .await?;
    Ok(StatusCode::ACCEPTED)
}

/// `POST /api/agents/{id}/end-input` — signal end-of-input to an interactive agent.
pub async fn agent_end_input(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    st.manager
        .send_agent_input(&id, AttachInbound::EndInput)
        .await?;
    Ok(StatusCode::ACCEPTED)
}

/// `DELETE /api/agents/{id}` — remove from caliban's registry.
pub async fn rm_agent(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    st.manager.rm_agent(&id, false).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/metrics` — prosperod's operational counters.
pub async fn get_metrics(State(st): State<AppState>) -> Json<prospero_core::MetricsSnapshot> {
    Json(st.manager.metrics())
}

/// `GET /healthz` — daemon liveness (always 200 while the process is up).
pub async fn healthz() -> &'static str {
    "ok"
}

/// `GET /readyz` — readiness: 200 only when the store is writable, otherwise
/// 503 so an orchestrator can gate traffic/restarts. The body carries the
/// store-writability flag and an aggregate repo-health summary.
pub async fn readyz(State(st): State<AppState>) -> (StatusCode, Json<prospero_core::Readiness>) {
    let readiness = st.manager.readiness().await;
    let code = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(readiness))
}
