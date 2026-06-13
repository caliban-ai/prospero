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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AgentStatus;

    #[test]
    fn display_messages() {
        let e = CoreError::CalibandUnreachable {
            path: "/tmp/x.sock".into(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "nope"),
        };
        assert!(
            e.to_string()
                .starts_with("caliband unreachable at /tmp/x.sock:")
        );
        assert_eq!(
            CoreError::Protocol("bad".into()).to_string(),
            "caliband protocol error: bad"
        );
        assert_eq!(
            CoreError::AgentNotFound("a1".into()).to_string(),
            "agent not found: a1"
        );
        assert_eq!(
            CoreError::InvalidState {
                op: "kill".into(),
                id: "a1".into(),
                status: "Done".into(),
            }
            .to_string(),
            "invalid state for kill: agent a1 is Done"
        );
        assert_eq!(
            CoreError::Discovery("d".into()).to_string(),
            "discovery error: d"
        );
        assert_eq!(CoreError::Store("s".into()).to_string(), "store error: s");
        assert_eq!(
            CoreError::RepoNotFound("r".into()).to_string(),
            "repo not registered: r"
        );
    }

    #[test]
    fn from_io_and_json() {
        let io: CoreError = std::io::Error::other("boom").into();
        assert!(matches!(io, CoreError::Io(_)));
        assert!(io.to_string().starts_with("io error:"));
        let json: CoreError = serde_json::from_str::<i32>("not json").unwrap_err().into();
        assert!(matches!(json, CoreError::Json(_)));
        assert!(json.to_string().starts_with("json error:"));
    }

    #[test]
    fn from_supervisor_error_maps_all_arms() {
        let nf: CoreError = SupervisorError::NotFound { id: "a1".into() }.into();
        assert!(matches!(nf, CoreError::AgentNotFound(id) if id == "a1"));

        let inv: CoreError = SupervisorError::InvalidState {
            op: "respawn".into(),
            id: "a2".into(),
            status: AgentStatus::Done,
        }
        .into();
        match inv {
            CoreError::InvalidState { op, id, status } => {
                assert_eq!(
                    (op.as_str(), id.as_str(), status.as_str()),
                    ("respawn", "a2", "Done")
                );
            }
            other => panic!("expected InvalidState, got {other:?}"),
        }

        let internal: CoreError = SupervisorError::Internal {
            message: "kaboom".into(),
        }
        .into();
        assert!(matches!(internal, CoreError::Protocol(m) if m == "kaboom"));
    }
}
