//! Presentation components. Thin projections over [`crate::view_model`] — all
//! derived numbers and labels come from there, so this module stays declarative
//! and the logic stays testable on the host target.

use dioxus::prelude::*;
use prospero_types::{Agent, FleetSnapshot, Workspace};

use crate::view_model::{
    FleetTotals, StatusCounts, basename, count_statuses, health_reason, is_healthy, short_id,
    status_label, status_tone, totals,
};

/// How many segments the status meter draws.
const METER_SEGMENTS: usize = 10;

/// Freshness of the data on screen, shown in the header.
#[derive(Debug, Clone, PartialEq)]
pub enum Freshness {
    /// The last refresh succeeded.
    Live,
    /// A refresh failed; what is rendered is the last good snapshot.
    Stale(String),
}

/// The application chrome: brand block, header, nav rail, content region.
#[component]
pub fn Shell(host: String, freshness: Freshness, children: Element) -> Element {
    rsx! {
        div { class: "shell",
            div { class: "brand",
                span { class: "brand-mark" }
                h1 { class: "brand-name", "Prospero" }
            }
            header { class: "topbar",
                div { class: "topbar-context",
                    span { class: "topbar-label", "fleet" }
                    span { class: "topbar-host", "{host}" }
                }
                div { class: "topbar-right", ConnectionState { freshness } }
            }
            nav { class: "nav", aria_label: "Sections",
                span { class: "nav-heading", "Fleet" }
                button { class: "nav-item", aria_current: "page", "Overview" }
                span { class: "nav-foot", "v2 · wasm" }
            }
            main { class: "main", {children} }
        }
    }
}

/// Header freshness indicator. Carries a glyph and a word, not colour alone.
#[component]
fn ConnectionState(freshness: Freshness) -> Element {
    match freshness {
        Freshness::Live => rsx! {
            span { class: "conn", title: "Fleet data is current",
                span { class: "glyph tone-live" }
                "live"
            }
        },
        Freshness::Stale(reason) => rsx! {
            span { class: "conn is-stale", title: "{reason}",
                span { class: "glyph tone-wait" }
                "stale"
            }
        },
    }
}

/// The whole overview: stat row plus a card per workspace.
#[component]
pub fn Overview(snapshot: FleetSnapshot) -> Element {
    let t = totals(&snapshot);
    rsx! {
        StatRow { totals: t }
        div { class: "section-head",
            h2 { class: "section-title", "Workspaces" }
            span { class: "section-rule" }
        }
        if snapshot.workspaces.is_empty() {
            EmptyState {}
        } else {
            div { class: "cards",
                for ws in snapshot.workspaces.iter() {
                    WorkspaceCard { key: "{ws.name}", workspace: ws.clone() }
                }
            }
        }
    }
}

/// Fleet-wide numbers.
#[component]
fn StatRow(totals: FleetTotals) -> Element {
    let unreachable_note = if totals.unreachable > 0 {
        format!("{} unreachable", totals.unreachable)
    } else {
        "all reachable".to_string()
    };
    rsx! {
        div { class: "stats",
            Stat {
                label: "Workspaces",
                value: totals.workspaces,
                note: Some(unreachable_note),
            }
            Stat { label: "Agents", value: totals.agents, note: None }
            Stat { label: "Running", value: totals.statuses.running, note: None }
            Stat { label: "Awaiting input", value: totals.statuses.idle, note: None }
        }
    }
}

#[component]
fn Stat(label: String, value: usize, note: Option<String>) -> Element {
    // A zero reads as absence, not as data — dim it so the eye skips to the
    // numbers that carry information.
    let value_class = if value == 0 {
        "stat-value is-zero"
    } else {
        "stat-value"
    };
    rsx! {
        div { class: "stat reveal",
            span { class: "{value_class}", "{value}" }
            span { class: "stat-label", "{label}" }
            if let Some(note) = note {
                span { class: "stat-note", "{note}" }
            }
        }
    }
}

/// One workspace: identity, health, its agents' status distribution.
#[component]
fn WorkspaceCard(workspace: Workspace) -> Element {
    let healthy = is_healthy(&workspace.health);
    let counts = count_statuses(&workspace.agents);
    let card_class = if healthy {
        "card reveal is-healthy"
    } else {
        "card reveal is-unreachable"
    };
    let sources = if workspace.sources.is_empty() {
        basename(&workspace.root.to_string_lossy()).to_string()
    } else {
        workspace
            .sources
            .iter()
            .map(|s| s.name.clone())
            .collect::<Vec<_>>()
            .join(" · ")
    };

    rsx! {
        article { class: "{card_class}",
            div { class: "card-head",
                div { class: "card-ident",
                    h3 { class: "card-title", "{workspace.name}" }
                    div { class: "card-sub", "{sources}" }
                }
                HealthPill { workspace: workspace.clone() }
            }
            div { class: "card-body",
                if workspace.agents.is_empty() {
                    div { class: "card-empty", "No agents." }
                } else {
                    StatusMeter { counts }
                    div { class: "agents",
                        for agent in workspace.agents.iter() {
                            AgentRow { key: "{agent.id}", agent: agent.clone() }
                        }
                    }
                }
            }
        }
    }
}

/// Health of a workspace's caliband, with the failure reason as a tooltip.
#[component]
fn HealthPill(workspace: Workspace) -> Element {
    match health_reason(&workspace.health) {
        None => rsx! {
            span { class: "pill tone-live",
                span { class: "glyph tone-live" }
                "healthy"
            }
        },
        Some(reason) => rsx! {
            span { class: "pill tone-bad", title: "{reason}",
                span { class: "glyph tone-bad" }
                "unreachable"
            }
        },
    }
}

/// Segmented meter of a workspace's agent status distribution, plus a legend.
///
/// Segments are allocated proportionally but every non-zero bucket is
/// guaranteed at least one segment — a single failing agent among fifty must
/// not round away to invisible.
#[component]
fn StatusMeter(counts: StatusCounts) -> Element {
    let segments = allocate_segments(counts);
    rsx! {
        div { class: "meter", role: "img", aria_label: "{meter_summary(counts)}",
            for (i , tone) in segments.iter().enumerate() {
                span { key: "{i}", class: "meter-seg is-{tone}" }
            }
        }
        div { class: "meter-legend",
            LegendItem { tone: "live", label: "running", count: counts.running }
            LegendItem { tone: "live", label: "spawning", count: counts.spawning }
            LegendItem { tone: "wait", label: "idle", count: counts.idle }
            LegendItem { tone: "done", label: "finished", count: counts.done }
            LegendItem { tone: "bad", label: "failed", count: counts.bad }
        }
    }
}

#[component]
fn LegendItem(tone: String, label: String, count: usize) -> Element {
    if count == 0 {
        return rsx! {};
    }
    rsx! {
        span { class: "meter-legend-item",
            span { class: "glyph tone-{tone}" }
            "{count} {label}"
        }
    }
}

#[component]
fn AgentRow(agent: Agent) -> Element {
    let tone = status_tone(agent.status);
    rsx! {
        div { class: "agent",
            span { class: "agent-id", "{short_id(&agent.id)}" }
            span { class: "agent-name", "{agent.name}" }
            span { class: "agent-tags",
                if agent.interactive {
                    span { class: "tag", title: "Accepts operator input", "int" }
                }
                if agent.isolated {
                    span { class: "tag", title: "Runs in an isolated git worktree", "wt" }
                }
                span { class: "pill tone-{tone}",
                    span { class: "glyph tone-{tone}" }
                    "{status_label(agent.status)}"
                }
            }
        }
    }
}

/// No workspaces registered yet.
#[component]
fn EmptyState() -> Element {
    rsx! {
        div { class: "state",
            h2 { class: "state-title", "No workspaces registered" }
            p { class: "state-detail",
                "Register one with "
                code { "prospero repo add <name> <path>" }
                " and it will appear here."
            }
        }
    }
}

/// First load failed — there is nothing to show but the reason.
#[component]
pub fn ErrorState(message: String, on_retry: EventHandler<()>) -> Element {
    rsx! {
        div { class: "state",
            span { class: "glyph glyph-lg tone-bad" }
            h2 { class: "state-title", "Could not load the fleet" }
            p { class: "state-detail",
                code { "{message}" }
            }
            button { class: "btn", onclick: move |_| on_retry.call(()), "Retry" }
        }
    }
}

/// First load in flight. Placeholder cards keep the layout from jumping.
#[component]
pub fn LoadingState() -> Element {
    rsx! {
        div { class: "cards cards-skeleton",
            for i in 0..3 {
                div { key: "{i}", class: "skeleton" }
            }
        }
    }
}

/// Assign each status bucket a share of the meter's segments.
///
/// Every non-zero bucket is guaranteed one segment before anything is shared
/// out proportionally. Rounding each bucket independently does *not* work: with
/// 49 running and 1 idle, the running bucket alone rounds to all ten segments
/// and the idle one is truncated away — losing exactly the signal an operator
/// is scanning for.
///
/// Kept free of Dioxus so it can be unit-tested on the host target.
fn allocate_segments(counts: StatusCounts) -> Vec<&'static str> {
    let total = counts.total();
    if total == 0 {
        return Vec::new();
    }

    // Order matters: the meter reads left-to-right as work in flight → waiting
    // → finished → broken.
    let present: Vec<(&'static str, usize)> = [
        ("live", counts.running + counts.spawning),
        ("wait", counts.idle),
        ("done", counts.done),
        ("bad", counts.bad),
    ]
    .into_iter()
    .filter(|(_, n)| *n > 0)
    .collect();

    // One segment floor per present bucket, then largest-remainder over what's
    // left so the proportions still read true.
    let mut alloc = vec![1usize; present.len()];
    let spare = METER_SEGMENTS.saturating_sub(present.len());
    if spare > 0 {
        let mut remainders: Vec<(f64, usize)> = Vec::with_capacity(present.len());
        let mut handed = 0usize;
        for (i, (_, n)) in present.iter().enumerate() {
            let exact = (*n as f64) * (spare as f64) / (total as f64);
            let whole = exact.floor() as usize;
            alloc[i] += whole;
            handed += whole;
            remainders.push((exact - whole as f64, i));
        }
        remainders.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        for (_, i) in remainders.into_iter().take(spare - handed) {
            alloc[i] += 1;
        }
    }

    let mut out = Vec::with_capacity(METER_SEGMENTS);
    for (i, (tone, _)) in present.iter().enumerate() {
        for _ in 0..alloc[i] {
            out.push(*tone);
        }
    }
    out.truncate(METER_SEGMENTS);
    out
}

/// Screen-reader description of the meter. The meter itself is decorative
/// colour; this is the accessible equivalent, so it must name every bucket.
fn meter_summary(c: StatusCounts) -> String {
    format!(
        "{} running, {} spawning, {} idle, {} finished, {} failed",
        c.running, c.spawning, c.idle, c.done, c.bad
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(running: usize, idle: usize, spawning: usize, done: usize) -> StatusCounts {
        StatusCounts {
            running,
            idle,
            spawning,
            done,
            bad: 0,
        }
    }

    #[test]
    fn an_empty_workspace_draws_no_segments() {
        assert!(allocate_segments(StatusCounts::default()).is_empty());
    }

    #[test]
    fn segments_never_exceed_the_meter_width() {
        for running in 1..40usize {
            let c = counts(running, 0, 0, 0);
            assert!(allocate_segments(c).len() <= METER_SEGMENTS);
        }
        // A four-way split is the worst case for the at-least-one-segment rule.
        let c = counts(3, 3, 2, 2);
        assert!(allocate_segments(c).len() <= METER_SEGMENTS);
    }

    #[test]
    fn a_single_agent_among_many_still_gets_a_segment() {
        // 1 idle out of 50 rounds to 0.2 segments — it must not disappear.
        let c = counts(49, 1, 0, 0);
        let segs = allocate_segments(c);
        assert!(segs.contains(&"wait"), "lone idle agent vanished: {segs:?}");
    }

    #[test]
    fn buckets_render_in_flight_before_finished() {
        let c = counts(1, 1, 0, 1);
        let segs = allocate_segments(c);
        let live = segs.iter().position(|t| *t == "live").unwrap();
        let wait = segs.iter().position(|t| *t == "wait").unwrap();
        let done = segs.iter().position(|t| *t == "done").unwrap();
        assert!(live < wait && wait < done, "wrong order: {segs:?}");
    }

    #[test]
    fn a_lone_failure_among_many_still_gets_a_segment() {
        // The whole point of the "bad" bucket: one crash in fifty must be
        // visible on the meter, not rounded away.
        let c = StatusCounts {
            running: 49,
            bad: 1,
            ..Default::default()
        };
        let segs = allocate_segments(c);
        assert!(segs.contains(&"bad"), "lone failure vanished: {segs:?}");
    }

    #[test]
    fn meter_summary_names_every_bucket() {
        let s = meter_summary(StatusCounts {
            running: 1,
            idle: 2,
            spawning: 3,
            done: 4,
            bad: 5,
        });
        for word in ["running", "spawning", "idle", "finished", "failed"] {
            assert!(s.contains(word), "summary missing {word}: {s}");
        }
    }
}
