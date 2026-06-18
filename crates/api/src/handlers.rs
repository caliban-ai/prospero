//! REST endpoint handlers over the `FleetManager`.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use prospero_core::AttachInbound;
use prospero_core::model::{Agent, FleetSnapshot};

use crate::AppState;
use crate::dto::{
    AddRepoBody, AgentInputBody, FromSeq, RepoSummary, RespawnedResponse, SetConfigBody, SpawnBody,
    SpawnedResponse,
};
use crate::error::ApiError;

/// `GET /api/fleet` — the whole fleet snapshot.
pub async fn get_fleet(State(st): State<AppState>) -> Json<FleetSnapshot> {
    Json(st.manager.snapshot().await)
}

/// `GET /api/repos` — managed repos with health and agent counts.
pub async fn get_repos(State(st): State<AppState>) -> Json<Vec<RepoSummary>> {
    let snap = st.manager.snapshot().await;
    let mut out = Vec::with_capacity(snap.repos.len());
    for r in snap.repos {
        let config = st.manager.repo_config(&r.name).await.unwrap_or_default();
        out.push(RepoSummary {
            name: r.name,
            root: r.root.display().to_string(),
            health: r.health,
            agent_count: r.agents.len(),
            config,
        });
    }
    Json(out)
}

/// `POST /api/repos` — register a repo.
pub async fn add_repo(
    State(st): State<AppState>,
    Json(body): Json<AddRepoBody>,
) -> Result<StatusCode, ApiError> {
    st.manager
        .add_repo_with_config(body.name, body.root, body.config)
        .await?;
    Ok(StatusCode::CREATED)
}

/// `PUT /api/repos/{name}/config` — set provider config and restart caliband.
pub async fn set_repo_config(
    State(st): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<SetConfigBody>,
) -> Result<StatusCode, ApiError> {
    st.manager.set_repo_config(&name, body.0).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /api/repos/{name}` — unregister a repo.
pub async fn delete_repo(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    if st.manager.remove_repo(&name).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(prospero_core::CoreError::RepoNotFound(name).into())
    }
}

/// `GET /api/repos/{repo}/agents` — agents under one repo.
pub async fn get_repo_agents(
    State(st): State<AppState>,
    Path(repo): Path<String>,
) -> Result<Json<Vec<Agent>>, ApiError> {
    let snap = st.manager.snapshot().await;
    match snap.repos.into_iter().find(|r| r.name == repo) {
        Some(r) => Ok(Json(r.agents)),
        None => Err(prospero_core::CoreError::RepoNotFound(repo).into()),
    }
}

/// `POST /api/repos/{repo}/agents` — spawn an agent (worktree by default).
pub async fn spawn_agent(
    State(st): State<AppState>,
    Path(repo): Path<String>,
    Json(body): Json<SpawnBody>,
) -> Result<(StatusCode, Json<SpawnedResponse>), ApiError> {
    let req = body.into_request();
    let isolated = req.isolation_worktree;
    let agent_id = st.manager.spawn_agent(&repo, req).await?;
    Ok((
        StatusCode::CREATED,
        Json(SpawnedResponse {
            agent_id,
            repo,
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
