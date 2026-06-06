//! Error types for the orchestration core.
//!
//! A failure in one repo or agent must never abort the daemon, so most
//! call sites surface these as state rather than propagating panics.

use crate::caliband::wire::SupervisorError;

/// The result type used throughout `prospero-core`.
pub type Result<T> = std::result::Result<T, CoreError>;

/// Errors produced by the orchestration core.
#[derive(thiserror::Error, Debug)]
pub enum CoreError {
    /// A caliban supervisor socket could not be reached.
    #[error("caliband unreachable at {path}: {source}")]
    CalibandUnreachable {
        /// The control socket path we tried to connect to.
        path: String,
        /// Underlying I/O error.
        source: std::io::Error,
    },

    /// A reply could not be parsed, or violated the protocol.
    #[error("caliband protocol error: {0}")]
    Protocol(String),

    /// The supervisor reported there is no such agent.
    #[error("agent not found: {0}")]
    AgentNotFound(String),

    /// The agent was in the wrong state for the requested operation.
    #[error("invalid state for {op}: agent {id} is {status}")]
    InvalidState {
        /// The operation that was attempted.
        op: String,
        /// The target agent id.
        id: String,
        /// The agent's actual status (rendered).
        status: String,
    },

    /// Repo discovery (socket resolution / daemon autostart) failed.
    #[error("discovery error: {0}")]
    Discovery(String),

    /// The durable event store failed.
    #[error("store error: {0}")]
    Store(String),

    /// A repo name was not registered.
    #[error("repo not registered: {0}")]
    RepoNotFound(String),

    /// Generic I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON (de)serialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<SupervisorError> for CoreError {
    fn from(e: SupervisorError) -> Self {
        match e {
            SupervisorError::NotFound { id } => CoreError::AgentNotFound(id),
            SupervisorError::InvalidState { op, id, status } => CoreError::InvalidState {
                op,
                id,
                status: format!("{status:?}"),
            },
            SupervisorError::Internal { message } => CoreError::Protocol(message),
        }
    }
}
