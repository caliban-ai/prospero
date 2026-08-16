//! Derived read-model for the dashboard: everything the views display but the
//! API doesn't send directly — rollups, labels, tone tokens, formatting.
//!
//! This module deliberately contains **no Dioxus** and compiles for the host
//! target as well as wasm, so it is unit-testable with a plain `cargo test`.
//! The rendering layer stays a thin projection over these functions, which is
//! what keeps the parts where bugs actually hide under test without a headless
//! browser.

use prospero_types::{Agent, AgentStatus, FleetSnapshot, Workspace, WorkspaceHealth};

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

/// A digest of everything in the snapshot that could change the usage
/// aggregate: each agent's identity and lifecycle state (#190).
///
/// The usage panel must not ride the 5-second fleet poll — re-running a 30-day
/// store aggregate that often would be wasteful, which is why #181 fetched only
/// on a window change. But never refetching left the panel reading `TURNS 0`
/// while the API already reported `1`. This key is the middle ground: the poll
/// loop compares it across snapshots and asks for a refetch only when it moves,
/// so the cost is one aggregate per *change* rather than one per poll.
///
/// A spawn, a terminal transition, and a reap all move it; streaming output —
/// which changes no agent's identity or status — does not.
///
/// Order-independent (the snapshot's workspace and agent order is not
/// guaranteed stable) via a commutative fold, and `DefaultHasher`'s keys are
/// fixed, so the same fleet always digests to the same value.
pub fn activity_key(snap: &FleetSnapshot) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut key: u64 = 0;
    for ws in &snap.workspaces {
        for agent in &ws.agents {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            agent.id.hash(&mut h);
            agent.workspace.hash(&mut h);
            // `AgentStatus` is a wire type and derives no `Hash`; its
            // discriminant is enough here and keeps the shared DTO untouched.
            std::mem::discriminant(&agent.status).hash(&mut h);
            // Commutative, so agent/workspace ordering can't alter the digest.
            key = key.wrapping_add(h.finish());
        }
    }
    key
}

/// What to tell the operator after a spawn request succeeds (#190).
///
/// Spawning is idempotent, and under k8s the `CalibanTask` name is derived from
/// the spec — so submitting the same prompt twice resolves to the run already in
/// flight. The dashboard used to report "Launched …" either way, which claimed
/// something that had not happened. When the server reports `created: false`,
/// say what actually occurred instead.
pub fn launch_note(created: bool, agent_id: &str, workspace: &str) -> String {
    let id = short_id(agent_id);
    if created {
        format!("Launched {id} in {workspace}.")
    } else {
        format!("Attached to the existing run {id} in {workspace} — an identical prompt was already in flight.")
    }
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

/// Which control an agent's row should offer.
///
/// This partition is **deliberately not** `AgentStatus::is_active()`. That one
/// is stream-oriented (`Spawning | Running`) and answers "might this still emit
/// output?". The operator question is different: an `Idle` agent is awaiting
/// input and is very much still killable, so it belongs with the live ones. The
/// remove path is kill → terminal → remove, which is why removing is only
/// offered once an agent has actually finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentControls {
    /// Still going: offer kill.
    Killable,
    /// Finished: offer respawn and remove.
    Finished,
}

/// Choose the control set for an agent's lifecycle state.
pub fn controls_for(status: AgentStatus) -> AgentControls {
    match status {
        AgentStatus::Spawning | AgentStatus::Running | AgentStatus::Idle => AgentControls::Killable,
        AgentStatus::Killed | AgentStatus::Done | AgentStatus::Failed | AgentStatus::Crashed => {
            AgentControls::Finished
        }
    }
}

/// Whether an interactive agent is currently waiting for operator input.
pub fn awaits_input(agent: &Agent) -> bool {
    agent.interactive && agent.status == AgentStatus::Idle
}

/// Whether a workspace can accept new agents.
///
/// Backend-dependent: a k8s workspace must have reconciled to `Ready`, while a
/// local one just needs its caliband reachable. Asking the wrong question would
/// either offer a launch that is certain to fail or hide one that would work.
pub fn is_launchable(workspace: &Workspace) -> bool {
    is_healthy(&workspace.health)
}

/// Compact elapsed string ("45s", "12m", "3h") from an RFC-3339 timestamp and
/// the current time in milliseconds since the epoch.
///
/// Takes `now_ms` rather than reading the clock so it stays pure and testable —
/// the caller supplies the browser's clock.
pub fn elapsed(started_at: &str, now_ms: f64) -> Option<String> {
    let started_ms = rfc3339_to_millis(started_at)?;
    let secs = ((now_ms - started_ms) / 1000.0).floor();
    if !secs.is_finite() || secs < 0.0 {
        return None;
    }
    let secs = secs as u64;
    Some(if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else {
        format!("{}h", secs / 3600)
    })
}

/// Parse the RFC-3339 timestamps caliband emits into epoch milliseconds.
///
/// Hand-rolled rather than pulling `chrono`: this crate is compiled to wasm and
/// ships to a browser, and one timestamp format does not justify the bytes.
/// Only the shape prospero actually emits is accepted — `YYYY-MM-DDTHH:MM:SS`
/// with an optional fractional part and an optional `Z`/offset.
pub(crate) fn rfc3339_to_millis(ts: &str) -> Option<f64> {
    let bytes = ts.as_bytes();
    if bytes.len() < 19 {
        return None;
    }
    let num = |a: usize, b: usize| ts.get(a..b)?.parse::<i64>().ok();
    let (y, mo, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (h, mi, s) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }

    // Days from the civil epoch (Howard Hinnant's days_from_civil).
    let y_adj = if mo <= 2 { y - 1 } else { y };
    let era = if y_adj >= 0 { y_adj } else { y_adj - 399 } / 400;
    let yoe = y_adj - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;

    Some(((days * 86_400 + h * 3600 + mi * 60 + s) * 1000) as f64)
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
    fn idle_agents_are_killable_not_removable() {
        // The operator partition is broader than AgentStatus::is_active():
        // an idle agent is awaiting input but still very much killable.
        for s in [
            AgentStatus::Spawning,
            AgentStatus::Running,
            AgentStatus::Idle,
        ] {
            assert_eq!(controls_for(s), AgentControls::Killable, "{s:?}");
        }
        for s in [
            AgentStatus::Killed,
            AgentStatus::Done,
            AgentStatus::Failed,
            AgentStatus::Crashed,
        ] {
            assert_eq!(controls_for(s), AgentControls::Finished, "{s:?}");
        }
        // Guard the divergence explicitly: Idle is NOT active by the stream
        // definition, but IS killable by the operator one.
        assert!(!AgentStatus::Idle.is_active());
        assert_eq!(controls_for(AgentStatus::Idle), AgentControls::Killable);
    }

    #[test]
    fn only_an_interactive_idle_agent_awaits_input() {
        let mut a = agent("x", AgentStatus::Idle);
        a.interactive = true;
        assert!(awaits_input(&a));

        a.interactive = false;
        assert!(
            !awaits_input(&a),
            "non-interactive idle must not offer input"
        );

        let mut running = agent("y", AgentStatus::Running);
        running.interactive = true;
        assert!(
            !awaits_input(&running),
            "a busy agent is not awaiting input"
        );
    }

    #[test]
    fn elapsed_renders_seconds_minutes_and_hours() {
        let t0 = rfc3339_to_millis("2026-08-02T12:00:00Z").unwrap();
        assert_eq!(
            elapsed("2026-08-02T12:00:00Z", t0 + 45_000.0).as_deref(),
            Some("45s")
        );
        assert_eq!(
            elapsed("2026-08-02T12:00:00Z", t0 + 12.0 * 60_000.0).as_deref(),
            Some("12m")
        );
        assert_eq!(
            elapsed("2026-08-02T12:00:00Z", t0 + 3.0 * 3_600_000.0).as_deref(),
            Some("3h")
        );
        // Boundaries.
        assert_eq!(
            elapsed("2026-08-02T12:00:00Z", t0 + 59_999.0).as_deref(),
            Some("59s")
        );
        assert_eq!(
            elapsed("2026-08-02T12:00:00Z", t0 + 60_000.0).as_deref(),
            Some("1m")
        );
    }

    #[test]
    fn elapsed_rejects_clock_skew_and_garbage_instead_of_rendering_nonsense() {
        let t0 = rfc3339_to_millis("2026-08-02T12:00:00Z").unwrap();
        // A timestamp in the future (client clock behind the server) must not
        // render a huge or negative duration.
        assert_eq!(elapsed("2026-08-02T12:00:00Z", t0 - 5_000.0), None);
        assert_eq!(elapsed("not-a-timestamp", t0), None);
        assert_eq!(elapsed("", t0), None);
        assert_eq!(elapsed("2026-13-45T99:99:99Z", t0), None);
    }

    /// The parser is hand-rolled to keep chrono out of a wasm bundle, so pin it
    /// against known epoch values.
    #[test]
    fn rfc3339_parses_against_known_epoch_values() {
        assert_eq!(rfc3339_to_millis("1970-01-01T00:00:00Z"), Some(0.0));
        assert_eq!(
            rfc3339_to_millis("2000-01-01T00:00:00Z"),
            Some(946_684_800_000.0)
        );
        assert_eq!(
            rfc3339_to_millis("2026-08-02T12:00:00Z"),
            Some(1_785_672_000_000.0)
        );
        // A leap day must not shift the date.
        assert_eq!(
            rfc3339_to_millis("2024-02-29T00:00:00Z"),
            Some(1_709_164_800_000.0)
        );
        // Fractional seconds and offsets are tolerated (the tail is ignored).
        assert_eq!(
            rfc3339_to_millis("2026-08-02T12:00:00.123456Z"),
            Some(1_785_672_000_000.0)
        );
    }

    #[test]
    fn basename_takes_the_trailing_component() {
        assert_eq!(basename("/work/caliban"), "caliban");
        assert_eq!(basename("/work/caliban/"), "caliban");
        assert_eq!(basename("caliban"), "caliban");
        assert_eq!(basename("/"), "/");
    }

    /// #190: the note must not claim a launch that did not happen.
    #[test]
    fn launch_note_distinguishes_a_real_launch_from_an_attach() {
        let launched = launch_note(true, "ct-6e9c59b774f34db2", "v2-eval");
        assert!(
            launched.starts_with("Launched "),
            "a real launch reads as one: {launched}"
        );

        let attached = launch_note(false, "ct-6e9c59b774f34db2", "v2-eval");
        assert!(
            !attached.contains("Launched"),
            "an attach must never claim a launch: {attached}"
        );
        assert!(attached.contains("existing run"), "{attached}");
        // Both name the same agent and workspace, so the operator can find it.
        for note in [&launched, &attached] {
            assert!(note.contains(short_id("ct-6e9c59b774f34db2")), "{note}");
            assert!(note.contains("v2-eval"), "{note}");
        }
    }

    fn snap(workspaces: Vec<Workspace>) -> FleetSnapshot {
        FleetSnapshot {
            host: "h".into(),
            workspaces,
        }
    }

    /// #190: the same fleet must digest identically, or the usage panel would
    /// refetch on every poll — exactly the cost #181 avoided.
    #[test]
    fn activity_key_is_stable_for_an_unchanged_fleet() {
        let build = || {
            snap(vec![workspace(
                "ws",
                WorkspaceHealth::Healthy,
                vec![agent("a1", AgentStatus::Running), agent("a2", AgentStatus::Idle)],
            )])
        };
        assert_eq!(activity_key(&build()), activity_key(&build()));
    }

    /// The case the ticket was filed for: an agent reaching a terminal state
    /// changes the aggregate, so it must move the key.
    #[test]
    fn activity_key_moves_when_an_agent_reaches_a_terminal_state() {
        let before = snap(vec![workspace(
            "ws",
            WorkspaceHealth::Healthy,
            vec![agent("a1", AgentStatus::Running)],
        )]);
        let after = snap(vec![workspace(
            "ws",
            WorkspaceHealth::Healthy,
            vec![agent("a1", AgentStatus::Done)],
        )]);
        assert_ne!(activity_key(&before), activity_key(&after));
    }

    #[test]
    fn activity_key_moves_when_an_agent_is_added_or_removed() {
        let one = snap(vec![workspace(
            "ws",
            WorkspaceHealth::Healthy,
            vec![agent("a1", AgentStatus::Running)],
        )]);
        let two = snap(vec![workspace(
            "ws",
            WorkspaceHealth::Healthy,
            vec![agent("a1", AgentStatus::Running), agent("a2", AgentStatus::Running)],
        )]);
        assert_ne!(activity_key(&one), activity_key(&two), "an added agent");

        let none = snap(vec![workspace("ws", WorkspaceHealth::Healthy, vec![])]);
        assert_ne!(activity_key(&one), activity_key(&none), "a reaped agent");
    }

    /// The API does not promise a stable agent or workspace order, so an
    /// ordering-sensitive digest would refetch on noise.
    #[test]
    fn activity_key_ignores_ordering() {
        let a1 = agent("a1", AgentStatus::Running);
        let a2 = agent("a2", AgentStatus::Done);
        let forward = snap(vec![workspace(
            "ws",
            WorkspaceHealth::Healthy,
            vec![a1.clone(), a2.clone()],
        )]);
        let reversed = snap(vec![workspace(
            "ws",
            WorkspaceHealth::Healthy,
            vec![a2, a1],
        )]);
        assert_eq!(activity_key(&forward), activity_key(&reversed));
    }

    /// Health is reconciliation noise, not spend: flapping it must not trigger
    /// a store aggregate.
    #[test]
    fn activity_key_ignores_workspace_health() {
        let healthy = snap(vec![workspace(
            "ws",
            WorkspaceHealth::Healthy,
            vec![agent("a1", AgentStatus::Running)],
        )]);
        let sick = snap(vec![workspace(
            "ws",
            WorkspaceHealth::Unreachable {
                reason: "boom".into(),
            },
            vec![agent("a1", AgentStatus::Running)],
        )]);
        assert_eq!(activity_key(&healthy), activity_key(&sick));
    }
}
