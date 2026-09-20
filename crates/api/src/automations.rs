//! REST endpoints for automations (#220).
//!
//! Scheduling and signature checking belong to `prospero-core`'s
//! `AutomationEngine`; these handlers only resolve the route, hand over the
//! request, and shape the reply — the one-directional `api → core` rule of
//! ADR 0006.
//!
//! One thing is deliberately *not* delegated: the trigger endpoint reads the
//! **raw** request body. Axum's `Json` extractor would give us a parsed value,
//! and re-serializing that produces different bytes than the sender signed, so
//! every honest caller's signature would fail to verify.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use prospero_core::automation::{
    Automation, AutomationRun, CreateAutomationBody, CreatedAutomationResponse, FiredResponse,
    RunSource, SetEnabledBody,
};
use serde::Deserialize;

use crate::AppState;
use crate::error::ApiError;

/// Header carrying a webhook's HMAC signature, `sha256=<hex>`.
pub const SIGNATURE_HEADER: &str = "x-prospero-signature";

/// Runs returned by `GET /api/automations/{id}/runs` when none is asked for.
const RUNS_DEFAULT: usize = 50;

/// Query for the run-history endpoint.
#[derive(Debug, Default, Deserialize)]
pub struct RunsQuery {
    /// How many runs to return, newest first.
    pub limit: Option<usize>,
}

/// The engine, or a `405` explaining that this backend has no shared config
/// store to hold automations in.
fn engine(
    st: &AppState,
) -> Result<&std::sync::Arc<prospero_core::automation::AutomationEngine>, ApiError> {
    st.automations.as_ref().ok_or_else(|| {
        ApiError::MethodNotAllowed(
            "automations need a shared config store; run with --database-url \
             (or the local backend) to enable them"
                .to_string(),
        )
    })
}

/// `GET /api/automations` — every configured automation.
pub async fn list_automations(
    State(st): State<AppState>,
) -> Result<Json<Vec<Automation>>, ApiError> {
    let stored = engine(&st)?.list_automations().await?;
    // Project to the wire type, which structurally has no secret to leak.
    Ok(Json(stored.into_iter().map(|s| s.automation).collect()))
}

/// `POST /api/automations` — create one, returning a webhook key if it has a
/// webhook trigger.
pub async fn create_automation(
    State(st): State<AppState>,
    Json(body): Json<CreateAutomationBody>,
) -> Result<(StatusCode, Json<CreatedAutomationResponse>), ApiError> {
    let created = engine(&st)?.create_automation(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

/// `DELETE /api/automations/{id}` — remove it and its run history.
pub async fn delete_automation(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    if engine(&st)?.delete_automation(&id).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!("automation not found: {id}")))
    }
}

/// `PUT /api/automations/{id}/enabled` — enable or disable without deleting.
pub async fn set_enabled(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<SetEnabledBody>,
) -> Result<StatusCode, ApiError> {
    engine(&st)?
        .set_automation_enabled(&id, body.enabled)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/automations/{id}/runs` — recent runs, newest first.
pub async fn list_runs(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<RunsQuery>,
) -> Result<Json<Vec<AutomationRun>>, ApiError> {
    let runs = engine(&st)?
        .automation_runs(&id, q.limit.unwrap_or(RUNS_DEFAULT))
        .await?;
    Ok(Json(runs))
}

/// `POST /api/automations/{id}/run` — fire it now, whatever its trigger.
pub async fn run_now(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<FiredResponse>, ApiError> {
    let run = engine(&st)?
        .fire_automation(&id, RunSource::Manual, None)
        .await?;
    Ok(Json(FiredResponse { run }))
}

/// `POST /api/automations/{id}/trigger` — an inbound signed webhook.
///
/// Open in the scope table on purpose: the signature over the body *is* the
/// credential. A sender holding the automation's key can fire that one
/// automation and nothing else, which is why this needs no bearer token and
/// must never accept one in place of a signature.
pub async fn trigger(
    State(st): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<FiredResponse>, ApiError> {
    let signature = headers
        .get(SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let run = engine(&st)?
        .trigger_webhook(&id, &body, signature.as_deref())
        .await?;
    Ok(Json(FiredResponse { run }))
}
