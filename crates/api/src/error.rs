//! Mapping `CoreError` to HTTP responses. Typed errors become precise status
//! codes (404/409/503) — never an opaque 500 for an expected condition.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use prospero_core::CoreError;
use serde::Serialize;

/// An API error that renders as `{ "error": "...", "kind": "..." }`.
pub struct ApiError(pub CoreError);

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    kind: &'static str,
}

impl From<CoreError> for ApiError {
    fn from(e: CoreError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, kind) = match &self.0 {
            CoreError::AgentNotFound(_) | CoreError::RepoNotFound(_) => {
                (StatusCode::NOT_FOUND, "not_found")
            }
            CoreError::InvalidState { .. } => (StatusCode::CONFLICT, "invalid_state"),
            CoreError::ProviderMisconfigured(_) => {
                (StatusCode::BAD_REQUEST, "provider_misconfigured")
            }
            CoreError::CalibandUnreachable { .. } | CoreError::Discovery(_) => {
                (StatusCode::SERVICE_UNAVAILABLE, "unreachable")
            }
            CoreError::Protocol(_) => (StatusCode::BAD_GATEWAY, "protocol"),
            CoreError::Store(_) | CoreError::Io(_) | CoreError::Json(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal")
            }
        };
        let body = ErrorBody {
            error: self.0.to_string(),
            kind,
        };
        (status, Json(body)).into_response()
    }
}
