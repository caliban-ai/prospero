//! REST endpoint handlers over the `FleetManager`.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use prospero_core::AttachInbound;
use prospero_core::model::{Agent, AgentId, DrainPolicy, FleetSnapshot, TaskSpec};

use crate::AppState;
use crate::dto::{
    AddWorkspaceBody, AgentInputBody, FromSeq, RespawnedResponse, SetConfigBody, SpawnBody,
    SpawnedResponse, WorkspaceSummary,
};
use crate::error::ApiError;

/// `GET /api/fleet` — the whole fleet snapshot.
pub async fn get_fleet(State(st): State<AppState>) -> Json<FleetSnapshot> {
    Json(st.fleet.snapshot().await)
}

/// `GET /api/workspaces` — managed workspaces with health, sources, agent counts.
pub async fn get_workspaces(State(st): State<AppState>) -> Json<Vec<WorkspaceSummary>> {
    let snap = st.fleet.snapshot().await;

    // The k8s config plane surfaces real `Workspace` CRs (config + reconciliation
    // status), including configured-but-agentless ones the fleet snapshot can't
    // see. `list_workspaces` is empty for the local backend, so local falls
    // through to the snapshot projection unchanged. (#142)
    let records = match st.admin.as_ref() {
        Some(admin) => admin.list_workspaces().await.unwrap_or_default(),
        None => Vec::new(),
    };

    if records.is_empty() {
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
                display_name: None,
                providers: Vec::new(),
                default_provider: None,
                status: None,
            })
            .collect();
        return Json(out);
    }

    // Regroup the snapshot's agents by the workspace they belong to
    // (`agent.workspace` == the `Workspace` object name), so each config-plane
    // workspace reports its own agent count.
    let mut agent_counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for w in &snap.workspaces {
        for a in &w.agents {
            *agent_counts.entry(a.workspace.as_str()).or_default() += 1;
        }
    }

    let out = records
        .into_iter()
        .map(|wi| {
            let agent_count = agent_counts.get(wi.name.as_str()).copied().unwrap_or(0);
            let sources = wi
                .sources
                .iter()
                .map(|s| prospero_core::Source {
                    name: s.name.clone(),
                    path: std::path::PathBuf::from(&s.path),
                })
                .collect();
            WorkspaceSummary {
                name: wi.name,
                root: String::new(),
                sources,
                // Reconciliation status (below) is the real health signal under
                // k8s; the poll-based `health` field doesn't apply.
                health: prospero_core::WorkspaceHealth::Healthy,
                agent_count,
                config: prospero_core::registry::RepoProviderConfig::default(),
                display_name: wi.display_name,
                providers: wi.providers,
                default_provider: wi.default_provider,
                status: wi.status,
            }
        })
        .collect();
    Json(out)
}

/// `POST /api/workspaces` — register a workspace.
pub async fn add_workspace(
    State(st): State<AppState>,
    Json(body): Json<AddWorkspaceBody>,
) -> Result<StatusCode, ApiError> {
    let admin = st
        .admin
        .as_ref()
        .ok_or_else(ApiError::unsupported_on_backend)?;
    let async_ops = admin.workspace_ops_are_async();
    admin
        .add_workspace(body.name, body.root.into(), body.config)
        .await?;
    // Async backends (k8s) only enqueued a reconcile → 202 Accepted; local
    // applied synchronously → 201 Created.
    Ok(if async_ops {
        StatusCode::ACCEPTED
    } else {
        StatusCode::CREATED
    })
}

/// `PUT /api/workspaces/{name}/config` — set provider config and restart caliband.
pub async fn set_workspace_config(
    State(st): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<SetConfigBody>,
) -> Result<StatusCode, ApiError> {
    let admin = st
        .admin
        .as_ref()
        .ok_or_else(ApiError::unsupported_on_backend)?;
    let async_ops = admin.workspace_ops_are_async();
    admin.set_workspace_config(&name, body.0).await?;
    // k8s: the operator re-reconciles → 202 Accepted; local: applied now → 204.
    Ok(if async_ops {
        StatusCode::ACCEPTED
    } else {
        StatusCode::NO_CONTENT
    })
}

/// `DELETE /api/workspaces/{name}` — unregister a workspace.
pub async fn delete_workspace(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let admin = st
        .admin
        .as_ref()
        .ok_or_else(ApiError::unsupported_on_backend)?;
    if admin.remove_workspace(&name).await? {
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
    let snap = st.fleet.snapshot().await;
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

/// `GET /api/agents/{id}` — one agent's current state.
///
/// This reads the **live fleet snapshot** — the exact same source as
/// `GET /api/fleet` — so the two are always consistent: an agent that has been
/// `rm`'d, or whose id was replaced by `respawn`, disappears from *both* the
/// fleet listing and this endpoint (404) as soon as the registry no longer
/// tracks it (immediately for `rm` via the optimistic snapshot prune, or at the
/// next poll for a respawn's old id). It is deliberately **not** served from the
/// event-sourced projection: the immutable per-agent history of a replaced or
/// removed agent remains reachable via `GET /api/agents/{id}/events`, which
/// replays from the durable store and therefore intentionally outlives the
/// agent's presence in the live registry (#124).
pub async fn get_agent(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Agent>, ApiError> {
    let snap = st.fleet.snapshot().await;
    match snap.find_agent(&id) {
        Some((_, agent)) => Ok(Json(agent.clone())),
        None => Err(prospero_core::CoreError::AgentNotFound(id).into()),
    }
}

/// `GET /api/agents/{id}/events?from=N` — replay history from the shared store.
pub async fn get_agent_events(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<FromSeq>,
) -> Result<Json<Vec<prospero_core::FleetEvent>>, ApiError> {
    // An agent's stream key is its own id (see `event::stream_key_for`).
    let events = st.store.replay(&id, q.from).await?;
    // A truly unknown agent id → 404, mirroring `GET /api/agents/{id}`. But a
    // *known* agent with an empty replay (spawned-but-no-events-yet, or a
    // `from` past its last seq) still returns `200 []`. Distinguish them: a
    // known agent has durable history (high_water > 0) or is live in the fleet.
    if events.is_empty()
        && st.store.high_water(&id).await? == 0
        && st.fleet.snapshot().await.find_agent(&id).is_none()
    {
        return Err(prospero_core::CoreError::AgentNotFound(id).into());
    }
    Ok(Json(events))
}

/// `POST /api/agents/{id}/kill`.
pub async fn kill_agent(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    st.fleet
        .stop_agent(&AgentId::from(id.as_str()), DrainPolicy::Kill)
        .await?;
    Ok(StatusCode::ACCEPTED)
}

/// `POST /api/agents/{id}/respawn` — replace an agent with a fresh one.
///
/// Returns the **new** agent id. The old id is retired from caliban's registry,
/// so once the next poll reconciles it disappears from both `GET /api/fleet` and
/// `GET /api/agents/{id}` (they share the live snapshot). Its history is not
/// destroyed: `GET /api/agents/{old_id}/events` still replays the retired
/// agent's immutable event stream from the durable store (#124).
pub async fn respawn_agent(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RespawnedResponse>, ApiError> {
    let new_id = st.fleet.restart_agent(&AgentId::from(id.as_str())).await?;
    Ok(Json(RespawnedResponse {
        agent_id: new_id.to_string(),
    }))
}

/// `POST /api/agents/{id}/input` — inject a user message into an interactive agent.
pub async fn agent_input(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<AgentInputBody>,
) -> Result<StatusCode, ApiError> {
    st.fleet
        .send_input(
            &AgentId::from(id.as_str()),
            AttachInbound::UserMessage { text: body.text },
        )
        .await?;
    Ok(StatusCode::ACCEPTED)
}

/// `POST /api/agents/{id}/end-input` — signal end-of-input to an interactive agent.
pub async fn agent_end_input(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    st.fleet
        .send_input(&AgentId::from(id.as_str()), AttachInbound::EndInput)
        .await?;
    Ok(StatusCode::ACCEPTED)
}

/// `DELETE /api/agents/{id}` — forget the agent (local: caliban registry; k8s: CR).
pub async fn rm_agent(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    st.fleet
        .remove_agent(&AgentId::from(id.as_str()), false)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/metrics` — prosperod's operational counters.
pub async fn get_metrics(State(st): State<AppState>) -> Json<prospero_core::MetricsSnapshot> {
    Json(st.fleet.metrics())
}

/// `GET /api/capabilities` — what the active fleet backend supports, so the
/// dashboard can render backend-aware controls (#99). `admin` mirrors whether an
/// admin/registry plane is wired (`Some` for local, `None` for k8s).
pub async fn get_capabilities(State(st): State<AppState>) -> Json<crate::dto::Capabilities> {
    Json(crate::dto::Capabilities {
        admin: st.admin.is_some(),
        async_workspace_ops: st
            .admin
            .as_ref()
            .is_some_and(|a| a.workspace_ops_are_async()),
    })
}

/// `GET /healthz` — daemon liveness (always 200 while the process is up).
pub async fn healthz() -> &'static str {
    "ok"
}

/// `GET /readyz` — readiness: 200 only when the store is writable, otherwise
/// 503 so an orchestrator can gate traffic/restarts. The body carries the
/// store-writability flag and an aggregate repo-health summary.
pub async fn readyz(State(st): State<AppState>) -> (StatusCode, Json<prospero_core::Readiness>) {
    let readiness = st.fleet.readiness().await;
    let code = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(readiness))
}
