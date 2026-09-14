//! `/api/session` — dashboard sign-in, whoami and sign-out (#2).

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use prospero_types::{SessionInfo, SignInBody};

use super::session::{self, SESSION_TTL_SECS};
use super::{Principal, Via, resolve_principal};
use crate::AppState;
use crate::error::ApiError;

fn info(p: &Principal) -> SessionInfo {
    SessionInfo::Token {
        token_name: p.token_name.clone(),
        scope: p.scope,
        expires_at: p
            .expires_unix
            .and_then(|s| chrono::DateTime::from_timestamp(s, 0))
            .map(|d| d.to_rfc3339()),
    }
}

/// `GET /api/session` — the current credential, or `{"auth":"disabled"}`.
pub async fn get_session(
    State(st): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<SessionInfo>, ApiError> {
    if !st.auth.is_enabled() {
        return Ok(Json(SessionInfo::Disabled));
    }
    let p = resolve_principal(&st.auth, &headers).ok_or(ApiError::Unauthorized)?;
    Ok(Json(info(&p)))
}

/// `POST /api/session` — exchange a token for a session cookie.
pub async fn post_session(
    State(st): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SignInBody>,
) -> Result<Response, ApiError> {
    let Some(tokens) = st.auth.tokens() else {
        return Err(ApiError::NotFound("authentication is disabled".into()));
    };
    let entry = tokens
        .authenticate(&body.token)
        .ok_or(ApiError::Unauthorized)?;
    let expires = chrono::Utc::now().timestamp() + SESSION_TTL_SECS;
    let value = session::sign(st.auth.session_key(), entry, expires);
    let secure = st.auth.cookie_secure() || session::is_https(&headers);
    let principal = Principal {
        token_name: entry.name.clone(),
        scope: entry.scope,
        via: Via::Cookie,
        expires_unix: Some(expires),
    };
    tracing::info!(target: "prospero_audit", actor = %entry.name, "dashboard sign-in");
    let mut response = Json(info(&principal)).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        session::set_cookie(&value, SESSION_TTL_SECS, secure),
    );
    Ok(response)
}

/// `DELETE /api/session` — clear the cookie.
pub async fn delete_session(State(st): State<AppState>, headers: HeaderMap) -> Response {
    let secure = st.auth.cookie_secure() || session::is_https(&headers);
    (
        StatusCode::NO_CONTENT,
        [(header::SET_COOKIE, session::clear_cookie(secure))],
    )
        .into_response()
}
