//! Derived read-model for the dashboard: everything the views display but the
//! API doesn't send directly — rollups, labels, tone tokens, formatting.
//!
//! This module deliberately contains **no Dioxus** and compiles for the host
//! target as well as wasm, so it is unit-testable with a plain `cargo test`.
//! The rendering layer stays a thin projection over these functions, which is
//! what keeps the parts where bugs actually hide under test without a headless
//! browser.

use prospero_types::{Agent, AgentStatus, FleetSnapshot, WorkspaceHealth};

/// How a set of agents is distributed across lifecycle states.
///
/// Finished agents are split into `done` and `bad` rather than one `terminal`
/// bucket: an operator scanning a fleet needs "three finished" and "three
/// crashed" to look different, and collapsing them would throw away the only
/// distinction that prompts action.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatusCounts {
    /// Actively executing.
    pub running: usize,
    /// Awaiting operator input.
    pub idle: usize,
    /// Registered but not yet executing.
    pub spawning: usize,
    /// Finished successfully.
    pub done: usize,
    /// Finished badly — killed, failed, or crashed.
    pub bad: usize,
}

impl StatusCounts {
    /// Total agents counted.
    pub fn total(&self) -> usize {
        self.running + self.idle + self.spawning + self.done + self.bad
    }
}

/// Partition agents by lifecycle state.
pub fn count_statuses(agents: &[Agent]) -> StatusCounts {
    let mut c = StatusCounts::default();
    for a in agents {
        match a.status {
            AgentStatus::Running => c.running += 1,
            AgentStatus::Idle => c.idle += 1,
            AgentStatus::Spawning => c.spawning += 1,
            AgentStatus::Done => c.done += 1,
            AgentStatus::Killed | AgentStatus::Failed | AgentStatus::Crashed => c.bad += 1,
        }
    }
    c
}

/// Fleet-wide rollup for the stat row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FleetTotals {
    /// Managed workspaces.
    pub workspaces: usize,
    /// Agents across all workspaces.
    pub agents: usize,
    /// Workspaces whose caliband answered the last poll.
    pub healthy: usize,
    /// Workspaces whose caliband was unreachable.
    pub unreachable: usize,
    /// Agent status distribution across the whole fleet.
    pub statuses: StatusCounts,
}

/// Aggregate a snapshot into the numbers the overview displays.
pub fn totals(snap: &FleetSnapshot) -> FleetTotals {
    let mut t = FleetTotals {
        workspaces: snap.workspaces.len(),
        ..Default::default()
    };
    for ws in &snap.workspaces {
        if is_healthy(&ws.health) {
            t.healthy += 1;
        } else {
            t.unreachable += 1;
        }
        t.agents += ws.agents.len();
        let c = count_statuses(&ws.agents);
        t.statuses.running += c.running;
        t.statuses.idle += c.idle;
        t.statuses.spawning += c.spawning;
        t.statuses.done += c.done;
        t.statuses.bad += c.bad;
    }
    t
}

/// Whether a workspace's caliband answered the last poll.
pub fn is_healthy(health: &WorkspaceHealth) -> bool {
    matches!(health, WorkspaceHealth::Healthy)
}

/// The reason a workspace is unreachable, if it is.
pub fn health_reason(health: &WorkspaceHealth) -> Option<&str> {
    match health {
        WorkspaceHealth::Healthy => None,
        WorkspaceHealth::Unreachable { reason } => Some(reason),
    }
}

/// Human-facing label for a lifecycle state.
pub fn status_label(status: AgentStatus) -> &'static str {
    match status {
        AgentStatus::Spawning => "spawning",
        AgentStatus::Running => "running",
        AgentStatus::Idle => "idle",
        AgentStatus::Killed => "killed",
        AgentStatus::Done => "done",
        AgentStatus::Failed => "failed",
        AgentStatus::Crashed => "crashed",
    }
}

/// CSS tone token for a lifecycle state.
///
/// Tones are deliberately coarse (four, not seven): status is conveyed by the
/// label and pill shape as much as by colour, so the palette stays legible in
/// greyscale and to colourblind operators.
pub fn status_tone(status: AgentStatus) -> &'static str {
    match status {
        AgentStatus::Running | AgentStatus::Spawning => "live",
        AgentStatus::Idle => "wait",
        AgentStatus::Done => "done",
        AgentStatus::Killed | AgentStatus::Failed | AgentStatus::Crashed => "bad",
    }
}

/// First 8 characters of an agent id, for dense display.
///
/// Truncates on a char boundary — slicing bytes would panic on multi-byte
/// input, and ids are opaque strings from caliband, not guaranteed ASCII.
pub fn short_id(id: &str) -> &str {
    match id.char_indices().nth(8) {
        Some((byte_idx, _)) => &id[..byte_idx],
        None => id,
    }
}

/// Trailing path component, for showing a source or session directory compactly.
pub fn basename(path: &str) -> &str {
    path.trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prospero_types::{Agent, AgentStatus, FleetSnapshot, Workspace, WorkspaceHealth};

    fn agent(id: &str, status: AgentStatus) -> Agent {
        Agent {
            id: id.into(),
            name: "a".into(),
            workspace: "ws".into(),
            status,
            started_at: "2026-08-01T00:00:00Z".into(),
            isolated: true,
            interactive: false,
            session_dir: "/s".into(),
        }
    }

    fn workspace(name: &str, health: WorkspaceHealth, agents: Vec<Agent>) -> Workspace {
        Workspace {
            name: name.into(),
            root: "/r".into(),
            sources: vec![],
            health,
            config: Default::default(),
            agents,
        }
    }

    #[test]
    fn counts_partition_agents_by_status() {
        let c = count_statuses(&[
            agent("a", AgentStatus::Running),
            agent("b", AgentStatus::Running),
            agent("c", AgentStatus::Idle),
            agent("d", AgentStatus::Spawning),
            agent("e", AgentStatus::Done),
            agent("f", AgentStatus::Failed),
        ]);
        assert_eq!(c.running, 2);
        assert_eq!(c.idle, 1);
        assert_eq!(c.spawning, 1);
        assert_eq!(c.done, 1);
        assert_eq!(c.bad, 1);
        assert_eq!(c.total(), 6);
    }

    #[test]
    fn a_clean_finish_and_a_bad_one_are_counted_separately() {
        let done = count_statuses(&[agent("x", AgentStatus::Done)]);
        assert_eq!((done.done, done.bad), (1, 0));

        for s in [
            AgentStatus::Killed,
            AgentStatus::Failed,
            AgentStatus::Crashed,
        ] {
            let c = count_statuses(&[agent("x", s)]);
            assert_eq!((c.done, c.bad), (0, 1), "{s:?} should count as bad");
        }
    }

    /// Guards against a new `AgentStatus` variant being silently dropped from
    /// every bucket: whatever the state, an agent must be counted exactly once.
    #[test]
    fn every_status_lands_in_exactly_one_bucket() {
        for s in [
            AgentStatus::Spawning,
            AgentStatus::Running,
            AgentStatus::Idle,
            AgentStatus::Killed,
            AgentStatus::Done,
            AgentStatus::Failed,
            AgentStatus::Crashed,
        ] {
            assert_eq!(count_statuses(&[agent("x", s)]).total(), 1, "{s:?} lost");
        }
    }

    #[test]
    fn totals_aggregate_across_workspaces_and_health() {
        let snap = FleetSnapshot {
            host: "local".into(),
            workspaces: vec![
                workspace(
                    "a",
                    WorkspaceHealth::Healthy,
                    vec![agent("1", AgentStatus::Running)],
                ),
                workspace(
                    "b",
                    WorkspaceHealth::Unreachable {
                        reason: "no socket".into(),
                    },
                    vec![agent("2", AgentStatus::Idle), agent("3", AgentStatus::Done)],
                ),
            ],
        };
        let t = totals(&snap);
        assert_eq!(t.workspaces, 2);
        assert_eq!(t.agents, 3);
        assert_eq!(t.healthy, 1);
        assert_eq!(t.unreachable, 1);
        assert_eq!(t.statuses.running, 1);
        assert_eq!(t.statuses.idle, 1);
        assert_eq!(t.statuses.done, 1);
        assert_eq!(t.statuses.bad, 0);
    }

    #[test]
    fn totals_of_an_empty_fleet_are_all_zero() {
        let t = totals(&FleetSnapshot {
            host: "local".into(),
            workspaces: vec![],
        });
        assert_eq!(t, FleetTotals::default());
    }

    #[test]
    fn health_reason_is_present_only_when_unreachable() {
        assert_eq!(health_reason(&WorkspaceHealth::Healthy), None);
        assert_eq!(
            health_reason(&WorkspaceHealth::Unreachable {
                reason: "no socket".into()
            }),
            Some("no socket")
        );
    }

    #[test]
    fn every_status_has_a_label_and_a_known_tone() {
        for s in [
            AgentStatus::Spawning,
            AgentStatus::Running,
            AgentStatus::Idle,
            AgentStatus::Killed,
            AgentStatus::Done,
            AgentStatus::Failed,
            AgentStatus::Crashed,
        ] {
            assert!(!status_label(s).is_empty());
            assert!(
                matches!(status_tone(s), "live" | "wait" | "done" | "bad"),
                "{s:?} produced an unknown tone"
            );
        }
    }

    #[test]
    fn short_id_truncates_and_never_splits_a_char() {
        assert_eq!(short_id("abcdefghijkl"), "abcdefgh");
        assert_eq!(short_id("abc"), "abc");
        assert_eq!(short_id(""), "");
        // Multi-byte input must not panic on a non-boundary slice.
        let s = "ααααααααα";
        assert!(s.starts_with(short_id(s)));
        assert_eq!(short_id(s).chars().count(), 8);
    }

    #[test]
    fn basename_takes_the_trailing_component() {
        assert_eq!(basename("/work/caliban"), "caliban");
        assert_eq!(basename("/work/caliban/"), "caliban");
        assert_eq!(basename("caliban"), "caliban");
        assert_eq!(basename("/"), "/");
    }
}
