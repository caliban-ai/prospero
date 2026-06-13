//! Mirrored caliband IPC wire types.
//!
//! These mirror `caliban-supervisor`'s `proto.rs`. The **wire format is the
//! only contract** between Prospero and caliban — we deliberately do not
//! depend on the caliban crate. If caliban's protocol changes, these types
//! (and the golden tests) are where the drift surfaces.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub use crate::model::AgentStatus;

/// Snapshot describing a registered sub-agent (caliband `AgentRecord`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRecord {
    /// Opaque id.
    pub id: String,
    /// Human-readable label.
    pub name: String,
    /// Current lifecycle state.
    pub status: AgentStatus,
    /// RFC-3339 registration timestamp.
    pub started_at: String,
    /// Path to the agent's session directory.
    pub session_dir: PathBuf,
    /// Path to the agent's per-agent socket (for attach).
    pub socket_path: PathBuf,
    /// Original spawn spec.
    pub spec: SpawnSpec,
}

/// Daemon status (caliband `DaemonStatus`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonStatus {
    /// Daemon PID.
    pub pid: u32,
    /// Number of registered agents.
    pub agents: u32,
    /// Seconds since the daemon started.
    pub uptime_secs: u64,
    /// Path to the control socket.
    pub socket_path: PathBuf,
}

/// Parameters for a new sub-agent spawn (caliband `SpawnSpec`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpawnSpec {
    /// Optional human-readable name.
    #[serde(default)]
    pub label: Option<String>,
    /// Path to a frontmatter markdown file, if any.
    #[serde(default)]
    pub frontmatter_path: Option<PathBuf>,
    /// Initial prompt handed to the agent.
    pub initial_prompt: String,
    /// Optional model override.
    #[serde(default)]
    pub model: Option<String>,
    /// Optional tool allowlist.
    #[serde(default)]
    pub tool_allowlist: Option<Vec<String>>,
    /// True iff the agent runs in an isolated worktree.
    #[serde(default)]
    pub isolation_worktree: bool,
    /// Whether to inherit parent hooks.
    #[serde(default = "true_default")]
    pub inherit_hooks: bool,
    /// When true, the worker runs in interactive mode: at each end-of-run
    /// boundary it awaits inbound operator messages over the per-agent socket
    /// instead of finishing. Mirrors caliban `SpawnSpec.interactive`.
    #[serde(default)]
    pub interactive: bool,
}

fn true_default() -> bool {
    true
}

/// Inbound control frames written to an interactive agent's per-agent socket.
/// Mirrors caliban `AttachInbound` (`caliban/src/attach.rs`); the outbound
/// stream stays caliban stream-json, so the two never share a direction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AttachInbound {
    /// Inject a user message and resume the run.
    UserMessage {
        /// Message text.
        text: String,
    },
    /// Signal end-of-input: the agent finishes after this.
    EndInput,
}

/// Control-plane requests sent to the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CtlRequest {
    /// List all registered agents.
    List,
    /// Register and start a new agent.
    Spawn {
        /// Spec describing the agent.
        spec: SpawnSpec,
    },
    /// Return the dedicated socket for an agent.
    Attach {
        /// Target agent.
        id: String,
    },
    /// Terminate an agent.
    Kill {
        /// Target agent.
        id: String,
    },
    /// Kill + respawn with the same spec.
    Respawn {
        /// Target agent.
        id: String,
    },
    /// Remove an agent from the registry.
    Rm {
        /// Target agent.
        id: String,
        /// Force-remove even if running.
        #[serde(default)]
        force: bool,
    },
    /// Daemon health probe.
    Status,
    /// Ask the daemon to drain and shut down.
    Shutdown,
}

/// Control-plane replies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CtlReply {
    /// Successful list.
    Listed {
        /// Registered agents.
        agents: Vec<AgentRecord>,
    },
    /// Successful spawn.
    Spawned {
        /// New id.
        id: String,
        /// Per-agent socket path.
        socket_path: PathBuf,
    },
    /// Successful attach handshake.
    AttachAck {
        /// Per-agent socket path.
        socket_path: PathBuf,
    },
    /// Successful kill.
    Killed,
    /// Successful respawn.
    Respawned {
        /// New id (old id removed).
        id: String,
    },
    /// Successful rm.
    Removed,
    /// Daemon status snapshot.
    Status(DaemonStatus),
    /// Daemon will shut down once drained.
    ShutdownAck,
    /// An error occurred.
    Error {
        /// Structured error.
        error: SupervisorError,
    },
}

/// Errors the supervisor reports to clients.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SupervisorError {
    /// No such agent.
    #[error("agent not found: {id}")]
    NotFound {
        /// Missing id.
        id: String,
    },
    /// Agent is in the wrong state for the operation.
    #[error("invalid state for {op}: agent {id} is {status:?}")]
    InvalidState {
        /// Operation attempted.
        op: String,
        /// Target id.
        id: String,
        /// Actual status.
        status: AgentStatus,
    },
    /// Generic internal daemon error.
    #[error("internal supervisor error: {message}")]
    Internal {
        /// Free-form message.
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ctl_request_list_is_tagged() {
        assert_eq!(
            serde_json::to_string(&CtlRequest::List).unwrap(),
            "{\"kind\":\"list\"}"
        );
    }

    #[test]
    fn ctl_request_rm_force_defaults_false() {
        let r: CtlRequest = serde_json::from_str("{\"kind\":\"rm\",\"id\":\"a1\"}").unwrap();
        assert_eq!(
            r,
            CtlRequest::Rm {
                id: "a1".into(),
                force: false
            }
        );
    }

    #[test]
    fn spawn_spec_defaults_inherit_hooks_true() {
        let s: SpawnSpec = serde_json::from_str("{\"initial_prompt\":\"hi\"}").unwrap();
        assert!(s.inherit_hooks);
        assert!(!s.isolation_worktree);
        assert!(s.model.is_none());
    }

    #[test]
    fn spawn_spec_is_wire_compatible_with_caliban_interactive() {
        // Golden JSON in caliban's serialized SpawnSpec form (proto.rs). Pinned
        // so upstream protocol drift on `interactive` fails loudly here.
        let golden = r#"{"label":null,"frontmatter_path":null,"initial_prompt":"hi","model":null,"tool_allowlist":null,"isolation_worktree":false,"inherit_hooks":true,"interactive":true}"#;
        let spec: SpawnSpec = serde_json::from_str(golden).expect("deserialize caliban spec");
        assert!(
            spec.interactive,
            "interactive must round-trip from caliban's wire form"
        );
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["interactive"], serde_json::json!(true));
        // Bidirectional pin: our serialized form must match caliban's exact wire
        // shape (field set + order), so adding/dropping a field drifts loudly.
        assert_eq!(
            serde_json::to_string(&spec).unwrap(),
            golden,
            "re-serialised SpawnSpec must match caliban's golden wire form"
        );
    }

    #[test]
    fn spawn_spec_without_interactive_defaults_false() {
        // Back-compat: a pre-interactive spec (field absent) still deserializes.
        let old = r#"{"initial_prompt":"hi"}"#;
        let spec: SpawnSpec = serde_json::from_str(old).unwrap();
        assert!(!spec.interactive);
    }

    #[test]
    fn attach_inbound_user_message_serializes() {
        let j = serde_json::to_string(&AttachInbound::UserMessage {
            text: "hi there".into(),
        })
        .unwrap();
        assert_eq!(j, r#"{"type":"UserMessage","text":"hi there"}"#);
    }

    #[test]
    fn attach_inbound_end_input_serializes() {
        let j = serde_json::to_string(&AttachInbound::EndInput).unwrap();
        assert_eq!(j, r#"{"type":"EndInput"}"#);
    }

    #[test]
    fn attach_inbound_round_trips() {
        // Symmetric drift guard: the tagged shape must survive a serialize →
        // deserialize round-trip for both variants.
        for frame in [
            AttachInbound::UserMessage { text: "hi".into() },
            AttachInbound::EndInput,
        ] {
            let s = serde_json::to_string(&frame).unwrap();
            let back: AttachInbound = serde_json::from_str(&s).unwrap();
            assert_eq!(frame, back);
        }
    }

    #[test]
    fn ctl_reply_error_round_trips() {
        let reply = CtlReply::Error {
            error: SupervisorError::NotFound { id: "x".into() },
        };
        let s = serde_json::to_string(&reply).unwrap();
        let back: CtlReply = serde_json::from_str(&s).unwrap();
        assert_eq!(reply, back);
    }

    #[test]
    fn spawned_reply_parses() {
        let json = "{\"kind\":\"spawned\",\"id\":\"a1\",\"socket_path\":\"/tmp/a1.sock\"}";
        let r: CtlReply = serde_json::from_str(json).unwrap();
        assert_eq!(
            r,
            CtlReply::Spawned {
                id: "a1".into(),
                socket_path: "/tmp/a1.sock".into()
            }
        );
    }
}
