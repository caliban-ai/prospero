//! Mapping `CoreError` to HTTP responses. Typed errors become precise status
//! codes (404/409/503) — never an opaque 500 for an expected condition.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use prospero_core::CoreError;
use serde::Serialize;

/// An API error that renders as `{ "error": "...", "kind": "..." }`.
pub enum ApiError {
    /// A typed core error mapped to a precise status code.
    Core(CoreError),
    /// The operation exists but the active fleet backend does not support it
    /// (e.g. workspace-registry ops under k8s) → 405. (#76)
    MethodNotAllowed(String),
}

impl ApiError {
    /// The requested operation is real but not served by the active backend —
    /// e.g. the workspace registry/config plane under k8s, where workspaces are
    /// `CalibanTask`/namespace-driven rather than a prospero registry. (#76)
    #[must_use]
    pub fn unsupported_on_backend() -> Self {
        ApiError::MethodNotAllowed(
            "not supported by the active fleet backend (k8s workspaces are \
             CalibanTask/namespace-driven, not a prospero registry)"
                .to_string(),
        )
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    kind: &'static str,
}

impl From<CoreError> for ApiError {
    fn from(e: CoreError) -> Self {
        ApiError::Core(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, kind, error) = match self {
            ApiError::MethodNotAllowed(msg) => {
                (StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed", msg)
            }
            ApiError::Core(e) => {
                let (status, kind) = match &e {
                    CoreError::AgentNotFound(_) | CoreError::WorkspaceNotFound(_) => {
                        (StatusCode::NOT_FOUND, "not_found")
                    }
                    CoreError::InvalidState { .. } => (StatusCode::CONFLICT, "invalid_state"),
                    CoreError::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
                    CoreError::ProviderMisconfigured(_) => {
                        (StatusCode::BAD_REQUEST, "provider_misconfigured")
                    }
                    CoreError::InvalidConfig(_) => (StatusCode::BAD_REQUEST, "invalid_config"),
                    CoreError::CalibandUnreachable { .. }
                    | CoreError::Discovery(_)
                    | CoreError::Fleet(_) => (StatusCode::SERVICE_UNAVAILABLE, "unreachable"),
                    CoreError::Protocol(_) => (StatusCode::BAD_GATEWAY, "protocol"),
                    CoreError::Store(_)
                    | CoreError::SeqConflict
                    | CoreError::Io(_)
                    | CoreError::Json(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
                };
                (status, kind, e.to_string())
            }
        };
        let body = ErrorBody { error, kind };
        (status, Json(body)).into_response()
    }
}
