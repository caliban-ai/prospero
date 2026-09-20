//! Per-spawn wall-clock deadlines (#221).
//!
//! A runaway agent otherwise runs until someone notices and kills it. A spawn
//! may carry a timeout; prospero records the resulting deadline in the **event
//! log** and enforces it from the poll loop.
//!
//! The log is what makes the deadline survive a replica failover: a local timer
//! dies with the replica that set it, while `DeadlineSet` is in the shared store
//! and any replica that takes the agent over can read it back. Enforcement runs
//! under the same lifecycle lease as the loop's other emissions (#59), so
//! exactly one replica acts on an expiry.

use std::collections::HashMap;

use crate::model::Agent;

/// Which agents have passed their deadline and should be killed now.
///
/// Pure so the decision is testable without sleeping: callers pass `now` and
/// the deadlines they know about.
///
/// - A terminal agent is never selected: it has already stopped, and emitting a
///   timeout for it would add a lie to the log.
/// - An agent with no deadline is never selected — a timeout is opt-in.
/// - An unparseable deadline is ignored rather than treated as expired. Failing
///   the other way would kill a healthy agent because of a bad string.
#[must_use]
pub fn expired<'a>(
    now: chrono::DateTime<chrono::Utc>,
    deadlines: &HashMap<String, String>,
    agents: impl IntoIterator<Item = &'a Agent>,
) -> Vec<(String, String)> {
    agents
        .into_iter()
        .filter(|a| !a.status.is_terminal())
        .filter_map(|a| {
            let raw = deadlines.get(&a.id)?;
            let at = chrono::DateTime::parse_from_rfc3339(raw).ok()?;
            (now >= at.with_timezone(&chrono::Utc)).then(|| (a.id.clone(), raw.clone()))
        })
        .collect()
}

/// The deadline a spawn's timeout implies, as RFC-3339, or `None` when the
/// spawn asked for no timeout.
#[must_use]
pub fn deadline_from(
    started: chrono::DateTime<chrono::Utc>,
    timeout_secs: Option<u64>,
) -> Option<String> {
    let secs = timeout_secs?;
    let delta = chrono::Duration::try_seconds(secs as i64)?;
    Some((started + delta).to_rfc3339())
}

/// The newest deadline in an agent's stored history, if it ever had one.
///
/// This is the failover path: a replica that did not spawn the agent has no
/// in-memory deadline for it, and reads it back from the log instead.
#[must_use]
pub fn deadline_in(events: &[crate::event::FleetEvent]) -> Option<String> {
    events.iter().rev().find_map(|e| match &e.kind {
        crate::event::EventKind::DeadlineSet { deadline } => Some(deadline.clone()),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{EventKind, FleetEvent};
    use crate::model::AgentStatus;

    fn agent(id: &str, status: AgentStatus) -> Agent {
        Agent {
            id: id.into(),
            name: id.into(),
            workspace: "ws".into(),
            status,
            started_at: "2026-09-20T00:00:00Z".into(),
            isolated: false,
            interactive: false,
            session_dir: "/s".into(),
            permission_posture: crate::model::PermissionPosture::Supervised,
            reason: None,
            detail: None,
        }
    }

    fn at(s: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    #[test]
    fn an_agent_past_its_deadline_is_selected() {
        let deadlines = HashMap::from([("a".to_string(), "2026-09-20T00:00:10+00:00".to_string())]);
        let agents = [agent("a", AgentStatus::Running)];

        assert!(
            expired(at("2026-09-20T00:00:09Z"), &deadlines, &agents).is_empty(),
            "not yet due"
        );
        assert_eq!(
            expired(at("2026-09-20T00:00:10Z"), &deadlines, &agents),
            vec![("a".to_string(), "2026-09-20T00:00:10+00:00".to_string())],
            "due exactly on the deadline"
        );
    }

    /// Killing something that already stopped would write a timeout into the
    /// log for a run that ended on its own.
    #[test]
    fn a_terminal_agent_is_never_timed_out() {
        let deadlines = HashMap::from([("a".to_string(), "2026-09-20T00:00:00+00:00".to_string())]);
        for status in [
            AgentStatus::Done,
            AgentStatus::Failed,
            AgentStatus::Killed,
            AgentStatus::Crashed,
        ] {
            let agents = [agent("a", status)];
            assert!(
                expired(at("2026-09-21T00:00:00Z"), &deadlines, &agents).is_empty(),
                "{status:?} must not be timed out"
            );
        }
    }

    #[test]
    fn an_agent_without_a_deadline_is_left_alone() {
        let agents = [agent("a", AgentStatus::Running)];
        assert!(expired(at("2100-01-01T00:00:00Z"), &HashMap::new(), &agents).is_empty());
    }

    /// Fail safe: a malformed deadline must not read as "infinitely expired".
    #[test]
    fn an_unparseable_deadline_never_kills() {
        let deadlines = HashMap::from([("a".to_string(), "not-a-timestamp".to_string())]);
        let agents = [agent("a", AgentStatus::Running)];
        assert!(expired(at("2100-01-01T00:00:00Z"), &deadlines, &agents).is_empty());
    }

    #[test]
    fn deadline_from_adds_the_timeout_to_the_start() {
        let start = at("2026-09-20T00:00:00Z");
        assert_eq!(deadline_from(start, None), None, "no timeout ⇒ no deadline");
        let d = deadline_from(start, Some(90)).expect("a timeout yields a deadline");
        assert_eq!(at(&d), at("2026-09-20T00:01:30Z"));
    }

    /// The failover path: the deadline is recovered from the log, newest wins.
    #[test]
    fn the_newest_deadline_in_history_wins() {
        let ev = |seq: u64, deadline: &str| FleetEvent {
            seq,
            ts: "t".into(),
            repo: "ws".into(),
            agent_id: "a".into(),
            kind: EventKind::DeadlineSet {
                deadline: deadline.into(),
            },
            actor: None,
        };
        let events = vec![
            ev(1, "2026-09-20T00:00:10+00:00"),
            FleetEvent {
                seq: 2,
                ts: "t".into(),
                repo: "ws".into(),
                agent_id: "a".into(),
                kind: EventKind::AgentSpawned,
                actor: None,
            },
            ev(3, "2026-09-20T00:00:30+00:00"),
        ];
        assert_eq!(
            deadline_in(&events).as_deref(),
            Some("2026-09-20T00:00:30+00:00")
        );
        assert_eq!(deadline_in(&[]), None);
    }
}
