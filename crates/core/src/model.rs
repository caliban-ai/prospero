//! Prospero's fleet domain model: `Host -> Repo -> [Agent]`.
//!
//! `Agent` is the primary unit; `Repo` is a grouping that can host many
//! concurrent agents (parallel streams of work on one codebase).

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Lifecycle state of an agent. Mirrors caliban's `AgentStatus` wire enum
/// exactly so the same value round-trips through both protocols.
/// Aggregate readiness of prosperod, distinct from mere liveness.
///
/// `ready` gates traffic/restarts: it is `true` only when the durable store can
/// accept writes. The repo-health counts are an informational summary
/// (per-repo reachability is already surfaced in `/api/repos` and `/api/fleet`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Readiness {
    /// Overall ready signal — currently equivalent to `store_writable`.
    pub ready: bool,
    /// Whether the durable event store can accept writes.
    pub store_writable: bool,
    /// Total managed repos.
    pub repos_total: usize,
    /// Repos whose caliband responded to the last poll.
    pub repos_healthy: usize,
    /// Repos whose caliband was unreachable at the last poll.
    pub repos_unreachable: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    /// Registered, not yet executing.
    Spawning,
    /// Actively running (or attached).
    Running,
    /// Awaiting input; no compute pending.
    Idle,
    /// Stopped via kill.
    Killed,
    /// Finished successfully.
    Done,
    /// Finished with an error.
    Failed,
    /// Supervisor restarted while active; needs recovery.
    Crashed,
}

impl AgentStatus {
    /// True for states where the agent will produce no further work.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            AgentStatus::Killed | AgentStatus::Done | AgentStatus::Failed | AgentStatus::Crashed
        )
    }

    /// True while the agent may still be streaming output worth attaching to.
    pub fn is_active(self) -> bool {
        matches!(self, AgentStatus::Spawning | AgentStatus::Running)
    }
}

/// Connectivity of a managed repo's caliband daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RepoHealth {
    /// The control socket responded to the last poll.
    Healthy,
    /// The control socket could not be reached; carries the reason.
    Unreachable {
        /// Human-readable reason from the last failed poll.
        reason: String,
    },
}

/// Prospero's view of a single agent (projected from a caliban `AgentRecord`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Agent {
    /// Opaque caliban agent id.
    pub id: String,
    /// Human-readable label.
    pub name: String,
    /// Owning repo name (Prospero registry key).
    pub repo: String,
    /// Current lifecycle state.
    pub status: AgentStatus,
    /// RFC-3339 timestamp when the agent was registered.
    pub started_at: String,
    /// True if the agent runs in an isolated git worktree.
    pub isolated: bool,
    /// True if the agent was spawned in interactive mode (accepts operator input).
    pub interactive: bool,
    /// Path to the agent's session directory on disk.
    pub session_dir: PathBuf,
}

/// A managed repository and the agents running under its caliband.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Repo {
    /// Registry key (operator-chosen short name).
    pub name: String,
    /// Canonical repo root path.
    pub root: PathBuf,
    /// Health of the repo's caliband daemon.
    pub health: RepoHealth,
    /// Agents currently known under this repo.
    pub agents: Vec<Agent>,
}

/// A point-in-time view of the whole fleet on one host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FleetSnapshot {
    /// Host identity (single host in the first stab).
    pub host: String,
    /// Managed repos and their agents.
    pub repos: Vec<Repo>,
}

impl FleetSnapshot {
    /// Find an agent by id across all repos, returning `(repo_name, &Agent)`.
    pub fn find_agent(&self, id: &str) -> Option<(&str, &Agent)> {
        self.repos.iter().find_map(|r| {
            r.agents
                .iter()
                .find(|a| a.id == id)
                .map(|a| (r.name.as_str(), a))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_serializes_snake_case() {
        let j = serde_json::to_string(&AgentStatus::Running).unwrap();
        assert_eq!(j, "\"running\"");
    }

    #[test]
    fn status_terminal_and_active_partition_correctly() {
        for s in [AgentStatus::Spawning, AgentStatus::Running] {
            assert!(s.is_active() && !s.is_terminal());
        }
        for s in [
            AgentStatus::Killed,
            AgentStatus::Done,
            AgentStatus::Failed,
            AgentStatus::Crashed,
        ] {
            assert!(s.is_terminal() && !s.is_active());
        }
        // Idle is neither active nor terminal: awaiting input.
        assert!(!AgentStatus::Idle.is_active() && !AgentStatus::Idle.is_terminal());
    }

    #[test]
    fn repo_health_tags_state() {
        let j = serde_json::to_string(&RepoHealth::Healthy).unwrap();
        assert_eq!(j, "{\"state\":\"healthy\"}");
        let j = serde_json::to_string(&RepoHealth::Unreachable {
            reason: "no socket".into(),
        })
        .unwrap();
        assert_eq!(j, "{\"state\":\"unreachable\",\"reason\":\"no socket\"}");
    }

    #[test]
    fn find_agent_searches_across_repos() {
        let snap = FleetSnapshot {
            host: "local".into(),
            repos: vec![Repo {
                name: "prospero".into(),
                root: "/r".into(),
                health: RepoHealth::Healthy,
                agents: vec![Agent {
                    id: "a1".into(),
                    name: "x".into(),
                    repo: "prospero".into(),
                    status: AgentStatus::Running,
                    started_at: "t".into(),
                    isolated: true,
                    interactive: false,
                    session_dir: "/s".into(),
                }],
            }],
        };
        let (repo, agent) = snap.find_agent("a1").unwrap();
        assert_eq!(repo, "prospero");
        assert_eq!(agent.id, "a1");
        assert!(snap.find_agent("nope").is_none());
    }
}
