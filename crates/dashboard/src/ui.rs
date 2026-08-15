//! Presentation components. Thin projections over [`crate::view_model`] — all
//! derived numbers and labels come from there, so this module stays declarative
//! and the logic stays testable on the host target.

use dioxus::prelude::*;
use prospero_types::{
    AddWorkspaceBody, Agent, Capabilities, EventKind, FleetEvent, FleetSnapshot, SpawnBody,
    Workspace, WorkspaceSummary,
};

use crate::actions::Action;
use crate::config_form::{K8sForm, LocalForm, PROVIDER_KINDS, ProviderRow, SourceRow};
use crate::stream::{Entry, StreamSession, StreamState};
use crate::theme::{STORAGE_KEY, Theme};
use crate::view_model::{
    AgentControls, FleetTotals, StatusCounts, awaits_input, basename, controls_for, count_statuses,
    elapsed, health_reason, is_healthy, is_launchable, short_id, status_label, status_tone, totals,
};

/// Shared UI state, provided once by `App` and read by any component that needs
/// to raise a dialog, report a failure, or ask for a refresh.
///
/// Dioxus signals are `Copy`, so this whole struct is `Copy` and can be pulled
/// out of context by value wherever it is needed — no cloning, no prop drilling
/// through every intermediate component.
#[derive(Clone, Copy)]
pub struct Ui {
    /// The dialog currently open, if any.
    pub modal: Signal<Modal>,
    /// A failure to surface to the operator.
    pub banner: Signal<Option<String>>,
    /// A transient success note.
    pub note: Signal<Option<String>>,
    /// What the active backend supports; gates the admin controls.
    pub caps: Signal<Capabilities>,
    /// Bumped after a successful mutation to force an immediate refetch.
    pub refresh: Signal<u32>,
    /// The agent whose stream is open, if any.
    pub selected: Signal<Option<Agent>>,
    /// `GET /api/workspaces` summaries, keyed by name. `FleetSnapshot` carries
    /// neither the named providers nor the k8s reconciliation status, so the
    /// config editor and the provider picker need this second read.
    pub workspaces: Signal<Vec<WorkspaceSummary>>,
    /// Browser clock, in epoch milliseconds, sampled once per render pass.
    pub now_ms: Signal<f64>,
}

impl Ui {
    /// Request a fleet refetch on the next tick.
    pub fn request_refresh(&mut self) {
        self.refresh += 1;
    }
}

/// Which dialog is open.
#[derive(Clone, PartialEq)]
pub enum Modal {
    /// None.
    Closed,
    /// Confirm a destructive or irreversible [`Action`].
    Confirm(Action),
    /// The launch-an-agent form, pre-targeted at a workspace.
    Launch {
        /// Workspace the launch button belonged to.
        workspace: String,
    },
    /// Register a new workspace.
    AddWorkspace,
    /// Edit an existing workspace's configuration.
    EditWorkspace {
        /// Which workspace to edit.
        name: String,
    },
}

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
                div { class: "topbar-right",
                    ConnectionState { freshness }
                    ThemeToggle {}
                }
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

/// Theme control: cycles System → Light → Dark.
///
/// A cycling button rather than a three-way picker: the header is dense, the
/// set is tiny, and the current state is always named on the control itself.
#[component]
fn ThemeToggle() -> Element {
    let mut theme = use_signal(|| Theme::parse(stored_theme().as_deref()));

    // Reflect the choice onto the document and persist it.
    use_effect(move || {
        apply_theme(theme());
    });

    let current = theme();
    let icon = match current {
        Theme::System => "theme-icon is-system",
        Theme::Light => "theme-icon is-light",
        Theme::Dark => "theme-icon is-dark",
    };

    rsx! {
        button {
            class: "theme-toggle",
            // Named for screen readers, and says what pressing it will do —
            // a bare icon would leave the state and the action both implicit.
            aria_label: "Theme: {current.label()}. Switch to {current.next().label()}.",
            title: "Theme: {current.label()}",
            onclick: move |_| {
                let next = theme().next();
                theme.set(next);
            },
            span { class: "{icon}" }
            "{current.label()}"
        }
    }
}

/// Read the persisted preference. `None` when storage is unavailable.
fn stored_theme() -> Option<String> {
    web_sys::window()?
        .local_storage()
        .ok()
        .flatten()?
        .get_item(STORAGE_KEY)
        .ok()
        .flatten()
}

/// Stamp the theme onto the document root and persist it.
///
/// `System` removes the attribute rather than setting a third value, so the
/// stylesheet's `prefers-color-scheme` query takes over again.
fn apply_theme(theme: Theme) {
    let Some(window) = web_sys::window() else {
        return;
    };
    if let Some(root) = window.document().and_then(|d| d.document_element()) {
        match theme.attribute() {
            Some(value) => {
                let _ = root.set_attribute("data-theme", value);
            }
            None => {
                let _ = root.remove_attribute("data-theme");
            }
        }
    }
    // Storage can be unavailable (private browsing); the choice simply won't
    // survive a reload, which is better than failing to apply it at all.
    if let Ok(Some(storage)) = window.local_storage() {
        let _ = storage.set_item(STORAGE_KEY, theme.as_str());
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
    let mut ui = use_context::<Ui>();
    let admin = ui.caps.read().admin;
    let t = totals(&snapshot);
    rsx! {
        StatRow { totals: t }
        div { class: "section-head",
            h2 { class: "section-title", "Workspaces" }
            span { class: "section-rule" }
            if admin {
                button {
                    class: "btn btn-sm",
                    onclick: move |_| ui.modal.set(Modal::AddWorkspace),
                    "+ Add workspace"
                }
            }
        }
        if let Some(agent) = ui.selected.read().clone() {
            StreamPane { key: "{agent.id}", agent }
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
    let mut ui = use_context::<Ui>();
    let healthy = is_healthy(&workspace.health);
    let launchable = is_launchable(&workspace);
    let admin = ui.caps.read().admin;
    let name = workspace.name.clone();
    let remove_name = workspace.name.clone();
    let counts = count_statuses(&workspace.agents);
    // Reconciliation status and named providers live only on /api/workspaces —
    // FleetSnapshot carries neither.
    let summary = ui
        .workspaces
        .read()
        .iter()
        .find(|w| w.name == workspace.name)
        .cloned();
    let status = summary.as_ref().and_then(|s| s.status.clone());
    let providers: Vec<String> = summary
        .as_ref()
        .map(|s| {
            s.providers
                .iter()
                .map(|p| {
                    if Some(p.name.as_str()) == s.default_provider.as_deref() {
                        format!("{}*", p.name)
                    } else {
                        p.name.clone()
                    }
                })
                .collect()
        })
        .unwrap_or_default();
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
                    if !providers.is_empty() {
                        div { class: "card-sub", title: "* is the default provider",
                            "providers: {providers.join(\" · \")}"
                        }
                    }
                }
                match status {
                    Some(s) => rsx! { StatusPill { status: s } },
                    None => rsx! { HealthPill { workspace: workspace.clone() } },
                }
            }
            div { class: "card-controls",
                if launchable {
                    button {
                        class: "btn btn-sm btn-primary",
                        onclick: move |_| ui.modal.set(Modal::Launch { workspace: name.clone() }),
                        "Launch agent"
                    }
                }
                // Registry controls only exist when the backend wired an admin
                // plane; under a backend without one they would 405.
                if admin {
                    WorkspaceAdminControls { name: remove_name.clone() }
                }
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
    let mut ui = use_context::<Ui>();
    let tone = status_tone(agent.status);
    let is_open = ui
        .selected
        .read()
        .as_ref()
        .is_some_and(|a| a.id == agent.id);
    let row_class = if is_open { "agent is-open" } else { "agent" };
    let age = elapsed(&agent.started_at, *ui.now_ms.read());
    let controls = controls_for(agent.status);
    let wants_input = awaits_input(&agent);

    let open = agent.clone();
    rsx! {
        div { class: "{row_class}",
            div {
                class: "agent-line",
                onclick: move |_| ui.selected.set(Some(open.clone())),
                span { class: "agent-id", "{short_id(&agent.id)}" }
                span { class: "agent-name", "{agent.name}" }
                span { class: "agent-tags",
                    if let Some(age) = age {
                        span { class: "agent-age", title: "Started {agent.started_at}", "{age}" }
                    }
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
                div { class: "acts",
                    match controls {
                        AgentControls::Killable => rsx! {
                            ControlButton {
                                action: Action::KillAgent {
                                    id: agent.id.clone(),
                                    name: agent.name.clone(),
                                },
                                label: "Kill".to_string(),
                                danger: true,
                            }
                        },
                        AgentControls::Finished => rsx! {
                            ControlButton {
                                action: Action::RespawnAgent {
                                    id: agent.id.clone(),
                                    name: agent.name.clone(),
                                },
                                label: "Respawn".to_string(),
                                danger: false,
                            }
                            ControlButton {
                                action: Action::RemoveAgent {
                                    id: agent.id.clone(),
                                    name: agent.name.clone(),
                                },
                                label: "Remove".to_string(),
                                danger: true,
                            }
                        },
                    }
                }
            }
            if wants_input {
                AgentInput { agent: agent.clone() }
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

// --- Controls ---------------------------------------------------------------

/// A small inline control on an agent or workspace row.
///
/// Raises the confirmation dialog for anything irreversible; fires immediately
/// otherwise. Disables itself while a request is in flight so a double-click
/// can't issue the operation twice.
#[component]
fn ControlButton(action: Action, label: String, danger: bool) -> Element {
    let mut ui = use_context::<Ui>();
    let mut busy = use_signal(|| false);
    let class = if danger { "act-btn danger" } else { "act-btn" };

    rsx! {
        button {
            class: "{class}",
            disabled: busy(),
            onclick: move |evt| {
                evt.stop_propagation();
                let action = action.clone();
                if action.needs_confirmation() {
                    ui.modal.set(Modal::Confirm(action));
                } else {
                    busy.set(true);
                    spawn(async move {
                        run_action(ui, action).await;
                        busy.set(false);
                    });
                }
            },
            "{label}"
        }
    }
}

/// Execute an action, then route the outcome: refresh on success, banner on
/// failure. Central so every control reports consistently.
async fn run_action(mut ui: Ui, action: Action) {
    match action.run().await {
        Ok(()) => {
            ui.note.set(action.success_note());
            ui.banner.set(None);
            ui.request_refresh();
        }
        Err(e) => ui.banner.set(Some(e)),
    }
}

/// Confirmation dialog for an irreversible action.
#[component]
fn ConfirmDialog(action: Action) -> Element {
    let mut ui = use_context::<Ui>();
    let mut busy = use_signal(|| false);
    let confirm_class = if action.is_destructive() {
        "btn btn-danger"
    } else {
        "btn btn-primary"
    };

    let run = {
        let action = action.clone();
        move |_| {
            let action = action.clone();
            busy.set(true);
            spawn(async move {
                run_action(ui, action).await;
                busy.set(false);
                ui.modal.set(Modal::Closed);
            });
        }
    };

    rsx! {
        Scrim {
            div { class: "modal", role: "dialog", aria_modal: "true",
                h2 { class: "modal-title", "{action.title()}" }
                p { class: "modal-detail", "{action.detail()}" }
                div { class: "modal-actions",
                    button {
                        class: "btn",
                        disabled: busy(),
                        onclick: move |_| ui.modal.set(Modal::Closed),
                        "Cancel"
                    }
                    button {
                        class: "{confirm_class}",
                        disabled: busy(),
                        autofocus: true,
                        onclick: run,
                        if busy() { "Working…" } else { "{action.confirm_label()}" }
                    }
                }
            }
        }
    }
}

/// Modal backdrop. Clicking it or pressing Escape closes the dialog — an
/// operator who opened the wrong one should not have to hunt for Cancel.
#[component]
fn Scrim(children: Element) -> Element {
    let mut ui = use_context::<Ui>();
    rsx! {
        div {
            class: "scrim",
            tabindex: "-1",
            onclick: move |_| ui.modal.set(Modal::Closed),
            onkeydown: move |evt| {
                if evt.key() == Key::Escape {
                    ui.modal.set(Modal::Closed);
                }
            },
            div {
                // Swallow clicks inside the panel so they don't reach the
                // backdrop's close handler.
                onclick: move |evt| evt.stop_propagation(),
                {children}
            }
        }
    }
}

/// The launch-an-agent form.
#[component]
fn LaunchModal(workspace: String, snapshot: FleetSnapshot) -> Element {
    let mut ui = use_context::<Ui>();

    let launchable: Vec<String> = snapshot
        .workspaces
        .iter()
        .filter(|w| is_launchable(w))
        .map(|w| w.name.clone())
        .collect();

    let mut target = use_signal(|| workspace.clone());
    let mut provider_ref = use_signal(String::new);
    let mut prompt = use_signal(String::new);
    let mut label = use_signal(String::new);
    let mut model = use_signal(String::new);
    let mut tools = use_signal(String::new);
    let mut worktree = use_signal(|| true);
    let mut interactive = use_signal(|| false);
    let mut advanced = use_signal(|| false);
    let mut error = use_signal(|| None::<String>);
    let mut busy = use_signal(|| false);

    // (value, display) for the currently selected workspace's providers.
    let providers_for_target: Vec<(String, String)> = ui
        .workspaces
        .read()
        .iter()
        .find(|w| w.name == target())
        .map(|w| {
            w.providers
                .iter()
                .map(|p| {
                    let display = match &p.model {
                        Some(m) => format!("{} · {} · {m}", p.name, p.kind),
                        None => format!("{} · {}", p.name, p.kind),
                    };
                    (p.name.clone(), display)
                })
                .collect()
        })
        .unwrap_or_default();

    let submit = move |_| {
        let ws = target().trim().to_string();
        let task = prompt().trim().to_string();
        if ws.is_empty() || task.is_empty() {
            error.set(Some("A workspace and a task are both required.".into()));
            return;
        }
        let allowlist: Vec<String> = tools()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let body = SpawnBody {
            prompt: task,
            label: non_empty(label()),
            model: non_empty(model()),
            // Only the literal "shared" opts out of worktree isolation, so send
            // that exact token rather than a bool the server would misread.
            isolation: if worktree() {
                None
            } else {
                Some("shared".into())
            },
            tool_allowlist: if allowlist.is_empty() {
                None
            } else {
                Some(allowlist)
            },
            interactive: interactive(),
            frontmatter_path: None,
            provider_ref: non_empty(provider_ref()),
        };
        busy.set(true);
        error.set(None);
        spawn(async move {
            match crate::api::spawn_agent(&ws, &body).await {
                Ok(spawned) => {
                    ui.note.set(Some(format!(
                        "Launched {} in {}.",
                        short_id(&spawned.agent_id),
                        spawned.workspace
                    )));
                    ui.banner.set(None);
                    ui.request_refresh();
                    ui.modal.set(Modal::Closed);
                }
                Err(e) => error.set(Some(e)),
            }
            busy.set(false);
        });
    };

    rsx! {
        Scrim {
            div { class: "modal modal-wide", role: "dialog", aria_modal: "true",
                h2 { class: "modal-title", "Launch an agent" }

                label { class: "field",
                    span { class: "field-label", "Workspace" }
                    select {
                        class: "input",
                        onchange: move |e| target.set(e.value()),
                        for name in launchable.iter() {
                            option {
                                key: "{name}",
                                value: "{name}",
                                selected: target() == *name,
                                "{name}"
                            }
                        }
                    }
                }

                // k8s only: an agent binds one of the workspace's named
                // providers. Repopulates when the workspace changes, since the
                // provider list belongs to the workspace.
                if !providers_for_target.is_empty() {
                    label { class: "field",
                        span { class: "field-label", "Provider" }
                        select {
                            class: "input",
                            onchange: move |e| provider_ref.set(e.value()),
                            option {
                                value: "",
                                selected: provider_ref().is_empty(),
                                "(workspace default)"
                            }
                            for p in providers_for_target.iter() {
                                option {
                                    key: "{p.0}",
                                    value: "{p.0}",
                                    selected: provider_ref() == p.0,
                                    "{p.1}"
                                }
                            }
                        }
                    }
                }

                label { class: "field",
                    span { class: "field-label", "Task" }
                    textarea {
                        class: "input",
                        rows: "4",
                        placeholder: "Describe what the agent should do",
                        value: "{prompt}",
                        oninput: move |e| prompt.set(e.value()),
                    }
                }

                label { class: "check",
                    input {
                        r#type: "checkbox",
                        checked: worktree(),
                        onchange: move |e| worktree.set(e.checked()),
                    }
                    span { "Worktree isolation" }
                }
                label { class: "check",
                    input {
                        r#type: "checkbox",
                        checked: interactive(),
                        onchange: move |e| interactive.set(e.checked()),
                    }
                    span { "Interactive — the agent will wait for your input" }
                }

                button {
                    class: "disclosure",
                    onclick: move |_| advanced.toggle(),
                    if advanced() { "▾ Advanced" } else { "▸ Advanced" }
                }
                if advanced() {
                    div { class: "advanced",
                        label { class: "field",
                            span { class: "field-label", "Label" }
                            input {
                                class: "input",
                                value: "{label}",
                                oninput: move |e| label.set(e.value()),
                            }
                        }
                        label { class: "field",
                            span { class: "field-label", "Model" }
                            input {
                                class: "input",
                                placeholder: "workspace default",
                                value: "{model}",
                                oninput: move |e| model.set(e.value()),
                            }
                        }
                        label { class: "field",
                            span { class: "field-label", "Tool allowlist" }
                            input {
                                class: "input",
                                placeholder: "comma, separated",
                                value: "{tools}",
                                oninput: move |e| tools.set(e.value()),
                            }
                        }
                    }
                }

                if let Some(e) = error() {
                    p { class: "form-error", "{e}" }
                }

                div { class: "modal-actions",
                    button {
                        class: "btn",
                        disabled: busy(),
                        onclick: move |_| ui.modal.set(Modal::Closed),
                        "Cancel"
                    }
                    button {
                        class: "btn btn-primary",
                        disabled: busy(),
                        onclick: submit,
                        if busy() { "Launching…" } else { "Launch" }
                    }
                }
            }
        }
    }
}

/// Trim a form field, treating blank as absent.
fn non_empty(s: String) -> Option<String> {
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

/// Send-a-message row for an interactive agent that is awaiting input.
#[component]
fn AgentInput(agent: Agent) -> Element {
    let mut ui = use_context::<Ui>();
    let mut text = use_signal(String::new);
    let mut busy = use_signal(|| false);
    let id = agent.id.clone();

    // `use_callback` so the same handler can be shared by the button and the
    // Enter key without being moved into the first closure that uses it.
    let send = use_callback(move |_: ()| {
        let body = text().trim().to_string();
        if body.is_empty() {
            return;
        }
        let id = id.clone();
        busy.set(true);
        spawn(async move {
            match crate::api::send_input(&id, &body).await {
                Ok(()) => {
                    text.set(String::new());
                    ui.banner.set(None);
                    ui.request_refresh();
                }
                Err(e) => ui.banner.set(Some(e)),
            }
            busy.set(false);
        });
    });

    rsx! {
        div { class: "agent-input", onclick: move |e| e.stop_propagation(),
            input {
                class: "input",
                placeholder: "Send a message…",
                value: "{text}",
                disabled: busy(),
                oninput: move |e| text.set(e.value()),
                onkeydown: move |e| {
                    if e.key() == Key::Enter {
                        e.stop_propagation();
                        send.call(());
                    }
                },
            }
            button {
                class: "btn btn-primary btn-sm",
                disabled: busy(),
                onclick: move |e| {
                    e.stop_propagation();
                    send.call(());
                },
                "Send"
            }
            ControlButton {
                action: Action::EndInput {
                    id: agent.id.clone(),
                    name: agent.name.clone(),
                },
                label: "End".to_string(),
                danger: false,
            }
        }
    }
}

/// Transient failure banner. Click to dismiss.
#[component]
pub fn Banner() -> Element {
    let mut ui = use_context::<Ui>();
    rsx! {
        if let Some(msg) = ui.banner.read().clone() {
            div {
                class: "banner",
                role: "alert",
                onclick: move |_| ui.banner.set(None),
                span { class: "glyph tone-bad" }
                span { class: "banner-text", "{msg}" }
                span { class: "banner-dismiss", "dismiss" }
            }
        }
        if let Some(msg) = ui.note.read().clone() {
            div {
                class: "note",
                role: "status",
                onclick: move |_| ui.note.set(None),
                span { class: "glyph tone-live" }
                span { class: "banner-text", "{msg}" }
            }
        }
    }
}

/// Render whichever dialog is currently open.
#[component]
pub fn ModalHost(snapshot: FleetSnapshot) -> Element {
    let ui = use_context::<Ui>();
    let current = ui.modal.read().clone();
    match current {
        Modal::Closed => rsx! {},
        Modal::Confirm(action) => rsx! { ConfirmDialog { action } },
        Modal::Launch { workspace } => rsx! { LaunchModal { workspace, snapshot } },
        Modal::AddWorkspace => rsx! { WorkspaceModal { existing: None } },
        Modal::EditWorkspace { name } => rsx! { WorkspaceModal { existing: Some(name) } },
    }
}

// --- Workspace configuration ------------------------------------------------

/// Reconciliation status of a k8s workspace. Local workspaces report none and
/// show caliband reachability instead.
#[component]
fn StatusPill(status: prospero_types::WorkspaceStatusInfo) -> Element {
    let phase = status.phase.to_lowercase();
    let tone = match phase.as_str() {
        "ready" => "live",
        "failed" => "bad",
        _ => "wait",
    };
    let title = status.message.clone().unwrap_or_else(|| phase.clone());
    rsx! {
        span { class: "pill tone-{tone}", title: "{title}",
            span { class: "glyph tone-{tone}" }
            "{phase}"
        }
    }
}

/// The registry controls on a workspace card: configure and remove.
#[component]
fn WorkspaceAdminControls(name: String) -> Element {
    let mut ui = use_context::<Ui>();
    let edit_name = name.clone();
    rsx! {
        button {
            class: "act-btn",
            title: "Configure this workspace",
            onclick: move |evt| {
                evt.stop_propagation();
                ui.modal.set(Modal::EditWorkspace { name: edit_name.clone() });
            },
            "Configure"
        }
        ControlButton {
            action: Action::RemoveWorkspace { name: name.clone() },
            label: "Remove".to_string(),
            danger: true,
        }
    }
}

/// Add-a-workspace and edit-configuration share one form; only the identity
/// fields and the submit call differ.
#[component]
fn WorkspaceModal(existing: Option<String>) -> Element {
    let mut ui = use_context::<Ui>();
    let is_k8s = ui.caps.read().async_workspace_ops;
    let editing = existing.clone();

    // Prefill from the read side when editing.
    let summary = existing
        .as_ref()
        .and_then(|n| ui.workspaces.read().iter().find(|w| &w.name == n).cloned());

    let mut name = use_signal(|| existing.clone().unwrap_or_default());
    let mut root = use_signal(|| summary.as_ref().map(|s| s.root.clone()).unwrap_or_default());

    let local = use_signal(|| match &summary {
        Some(s) => LocalForm::from_config(&s.config),
        None => LocalForm::default(),
    });
    let k8s = use_signal(|| match &summary {
        Some(s) => K8sForm::from_summary(
            s.display_name.as_deref(),
            &s.source_specs,
            &s.providers,
            s.default_provider.as_deref(),
        ),
        None => K8sForm::blank(),
    });

    // Which providers the server says already hold credentials. The API never
    // returns the Secret reference itself, so this is the only way to warn that
    // saving without re-entering it would strip the credential.
    let had_credentials: Vec<String> = summary
        .as_ref()
        .map(|s| {
            s.providers
                .iter()
                .filter(|p| p.has_credentials)
                .map(|p| p.name.clone())
                .collect()
        })
        .unwrap_or_default();

    // Env rows live outside the two form structs so one component can edit
    // them for either shape; they are folded back in at submit.
    let env = use_signal(|| match &summary {
        Some(s) => s
            .config
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>(),
        None => Vec::new(),
    });

    let mut error = use_signal(|| None::<String>);
    let mut busy = use_signal(|| false);

    let title = if editing.is_some() {
        "Configure workspace"
    } else {
        "Add workspace"
    };

    let warning = if is_k8s {
        let refs: Vec<&str> = had_credentials.iter().map(String::as_str).collect();
        k8s.read().credentials_warning(&refs)
    } else {
        None
    };

    let submit = move |_| {
        let ws_name = name().trim().to_string();
        if ws_name.is_empty() {
            error.set(Some("A name is required.".into()));
            return;
        }
        let checkout = root().trim().to_string();
        if editing.is_none() && !is_k8s && checkout.is_empty() {
            error.set(Some("A checkout path is required.".into()));
            return;
        }

        let validated = if is_k8s {
            let mut f = k8s.read().clone();
            f.env = env();
            f.validate().map(|()| f.to_config())
        } else {
            let mut f = local.read().clone();
            f.env = env();
            f.validate().map(|()| f.to_config())
        };
        let config = match validated {
            Ok(c) => c,
            Err(e) => {
                error.set(Some(e));
                return;
            }
        };

        let editing = editing.clone();
        busy.set(true);
        error.set(None);
        spawn(async move {
            let result = match &editing {
                Some(existing) => crate::api::set_workspace_config(existing, &config).await,
                None => {
                    let body = AddWorkspaceBody {
                        name: ws_name.clone(),
                        root: checkout,
                        config,
                    };
                    crate::api::add_workspace(&body).await
                }
            };
            match result {
                Ok(()) => {
                    // Under k8s the write is accepted, not applied — the
                    // operator should expect a reconcile, not a finished change.
                    ui.note.set(Some(if is_k8s {
                        format!("{ws_name} accepted — reconciling.")
                    } else if editing.is_some() {
                        format!("{ws_name} updated.")
                    } else {
                        format!("{ws_name} registered.")
                    }));
                    ui.banner.set(None);
                    ui.request_refresh();
                    ui.modal.set(Modal::Closed);
                }
                Err(e) => error.set(Some(e)),
            }
            busy.set(false);
        });
    };

    rsx! {
        Scrim {
            div { class: "modal modal-wide", role: "dialog", aria_modal: "true",
                h2 { class: "modal-title", "{title}" }

                label { class: "field",
                    span { class: "field-label", "Name" }
                    input {
                        class: "input",
                        value: "{name}",
                        disabled: existing.is_some(),
                        placeholder: "my-workspace",
                        oninput: move |e| name.set(e.value()),
                    }
                }

                // The local backend needs a checkout path; k8s derives its
                // sources from the config below instead.
                if existing.is_none() && !is_k8s {
                    label { class: "field",
                        span { class: "field-label", "Checkout path" }
                        input {
                            class: "input",
                            placeholder: "/path/to/workspace",
                            value: "{root}",
                            oninput: move |e| root.set(e.value()),
                        }
                    }
                }

                if is_k8s {
                    K8sFields { form: k8s, env }
                } else {
                    LocalFields { form: local, env }
                }

                if let Some(w) = warning {
                    p { class: "form-warning",
                        span { class: "glyph tone-wait" }
                        "{w}"
                    }
                }
                if let Some(e) = error() {
                    p { class: "form-error", "{e}" }
                }

                div { class: "modal-actions",
                    button {
                        class: "btn",
                        disabled: busy(),
                        onclick: move |_| ui.modal.set(Modal::Closed),
                        "Cancel"
                    }
                    button {
                        class: "btn btn-primary",
                        disabled: busy(),
                        onclick: submit,
                        if busy() { "Saving…" } else { "Save" }
                    }
                }
            }
        }
    }
}

/// The local backend's single-provider fields.
#[component]
fn LocalFields(form: Signal<LocalForm>, env: Signal<Vec<(String, String)>>) -> Element {
    let mut form = form;
    let mut show_env = use_signal(|| !env.read().is_empty());

    rsx! {
        label { class: "field",
            span { class: "field-label", "Provider" }
            select {
                class: "input",
                onchange: move |e| form.write().provider = e.value(),
                // `selected` on the option, not `value` on the select: a
                // select's value is a DOM property, and the attribute does not
                // pre-select anything — the prefill silently did nothing.
                option {
                    value: "",
                    selected: form.read().provider.is_empty(),
                    "(backend default)"
                }
                for kind in PROVIDER_KINDS.iter() {
                    option {
                        key: "{kind}",
                        value: "{kind}",
                        selected: form.read().provider == *kind,
                        "{kind}"
                    }
                }
            }
        }
        label { class: "field",
            span { class: "field-label", "Base URL" }
            input {
                class: "input",
                placeholder: "http://host:11434",
                value: "{form.read().base_url}",
                oninput: move |e| form.write().base_url = e.value(),
            }
        }
        label { class: "field",
            span { class: "field-label", "API key from env var" }
            input {
                class: "input",
                placeholder: "e.g. ANTHROPIC_API_KEY",
                value: "{form.read().api_key_from_env}",
                oninput: move |e| form.write().api_key_from_env = e.value(),
            }
            // The distinction that keeps secrets out of the config store.
            span { class: "field-hint",
                "The NAME of a variable in prosperod's environment — never the key itself."
            }
        }

        button { class: "disclosure", onclick: move |_| show_env.toggle(),
            if show_env() { "▾ Environment overrides" } else { "▸ Environment overrides" }
        }
        if show_env() {
            EnvRows { rows: env }
        }
    }
}

/// The k8s `Workspace`-CR fields: sources, named providers, env.
#[component]
fn K8sFields(form: Signal<K8sForm>, env: Signal<Vec<(String, String)>>) -> Element {
    let mut form = form;
    let mut show_env = use_signal(|| !env.read().is_empty());

    rsx! {
        label { class: "field",
            span { class: "field-label", "Display name" }
            input {
                class: "input",
                placeholder: "Team A",
                value: "{form.read().display_name}",
                oninput: move |e| form.write().display_name = e.value(),
            }
        }

        div { class: "section-label", "Sources" }
        div { class: "cfg-rows",
            for i in 0..form.read().sources.len() {
                div { key: "src-{i}", class: "cfg-row",
                    input {
                        class: "input",
                        placeholder: "name",
                        value: "{form.read().sources[i].name}",
                        oninput: move |e| form.write().sources[i].name = e.value(),
                    }
                    input {
                        class: "input cfg-grow",
                        placeholder: "git remote",
                        value: "{form.read().sources[i].repo}",
                        oninput: move |e| form.write().sources[i].repo = e.value(),
                    }
                    input {
                        class: "input cfg-narrow",
                        placeholder: "ref (main)",
                        value: "{form.read().sources[i].r#ref}",
                        oninput: move |e| form.write().sources[i].r#ref = e.value(),
                    }
                    input {
                        class: "input",
                        placeholder: "/work/name",
                        value: "{form.read().sources[i].path}",
                        oninput: move |e| form.write().sources[i].path = e.value(),
                    }
                    button {
                        class: "act-btn danger",
                        onclick: move |_| { form.write().sources.remove(i); },
                        "×"
                    }
                }
            }
        }
        button {
            class: "row-add",
            onclick: move |_| form.write().sources.push(SourceRow::default()),
            "+ add source"
        }

        div { class: "section-label", "Providers" }
        div { class: "cfg-rows",
            for i in 0..form.read().providers.len() {
                div { key: "prov-{i}", class: "cfg-row",
                    input {
                        class: "input",
                        placeholder: "name",
                        value: "{form.read().providers[i].name}",
                        oninput: move |e| form.write().providers[i].name = e.value(),
                    }
                    select {
                        class: "input cfg-narrow",
                        onchange: move |e| form.write().providers[i].kind = e.value(),
                        for kind in PROVIDER_KINDS.iter() {
                            option {
                                key: "{kind}",
                                value: "{kind}",
                                selected: form.read().providers[i].kind == *kind,
                                "{kind}"
                            }
                        }
                    }
                    input {
                        class: "input",
                        placeholder: "model",
                        value: "{form.read().providers[i].model}",
                        oninput: move |e| form.write().providers[i].model = e.value(),
                    }
                    input {
                        class: "input",
                        placeholder: "secret name",
                        value: "{form.read().providers[i].secret_name}",
                        oninput: move |e| form.write().providers[i].secret_name = e.value(),
                    }
                    input {
                        class: "input cfg-narrow",
                        placeholder: "key",
                        value: "{form.read().providers[i].secret_key}",
                        oninput: move |e| form.write().providers[i].secret_key = e.value(),
                    }
                    label { class: "cfg-default", title: "Bound when an agent requests no provider",
                        input {
                            r#type: "radio",
                            checked: form.read().default_provider.as_deref()
                                == Some(form.read().providers[i].name.as_str()),
                            onchange: move |_| {
                                let n = form.read().providers[i].name.clone();
                                form.write().default_provider = Some(n);
                            },
                        }
                        span { "default" }
                    }
                    button {
                        class: "act-btn danger",
                        onclick: move |_| { form.write().providers.remove(i); },
                        "×"
                    }
                }
            }
        }
        button {
            class: "row-add",
            onclick: move |_| {
                form.write().providers.push(ProviderRow {
                    kind: PROVIDER_KINDS[0].to_string(),
                    ..Default::default()
                });
            },
            "+ add provider"
        }
        span { class: "field-hint",
            "Credentials are referenced by Secret name and key — the value is never sent or shown."
        }

        button { class: "disclosure", onclick: move |_| show_env.toggle(),
            if show_env() { "▾ Environment overrides" } else { "▸ Environment overrides" }
        }
        if show_env() {
            EnvRows { rows: env }
        }
    }
}

/// Repeatable KEY/VALUE rows, shared by both form shapes.
#[component]
fn EnvRows(rows: Signal<Vec<(String, String)>>) -> Element {
    let mut rows = rows;
    rsx! {
        div { class: "cfg-rows",
            for i in 0..rows.read().len() {
                div { key: "env-{i}", class: "cfg-row",
                    input {
                        class: "input",
                        placeholder: "KEY",
                        value: "{rows.read()[i].0}",
                        oninput: move |e| rows.write()[i].0 = e.value(),
                    }
                    input {
                        class: "input cfg-grow",
                        placeholder: "VALUE",
                        value: "{rows.read()[i].1}",
                        oninput: move |e| rows.write()[i].1 = e.value(),
                    }
                    button {
                        class: "act-btn danger",
                        onclick: move |_| { rows.write().remove(i); },
                        "×"
                    }
                }
            }
        }
        button {
            class: "row-add",
            onclick: move |_| rows.write().push((String::new(), String::new())),
            "+ add variable"
        }
    }
}

// --- Agent stream -----------------------------------------------------------

/// The live event stream for one agent: replay history, then tail.
#[component]
pub fn StreamPane(agent: Agent) -> Element {
    let mut ui = use_context::<Ui>();
    let id = agent.id.clone();
    let mut session = use_signal(|| StreamSession::new(id.clone()));

    // Re-open whenever the selected agent changes. The loop owns reconnection:
    // `follow` returns when the connection ends, and the session decides
    // whether that was a finish or a failure.
    use_future(move || {
        let id = id.clone();
        async move {
            // A different agent may have been selected while this was starting.
            if session.peek().agent_id != id {
                session.set(StreamSession::new(id.clone()));
            }
            loop {
                let from = session.peek().resume_from();
                crate::sse::follow(&id, from, |incoming| {
                    let mut s = session.write();
                    crate::sse::apply(&mut s, incoming);
                })
                .await;

                if !session.peek().should_retry() {
                    break;
                }
                let wait = session.peek().backoff();
                gloo_timers::future::sleep(wait).await;
            }
        }
    });

    let current = session.read().clone();
    let tone = match &current.state {
        StreamState::Live | StreamState::Connecting => "live",
        StreamState::Reconnecting { .. } => "wait",
        StreamState::Closed => "done",
    };
    let label = match &current.state {
        StreamState::Connecting => "connecting".to_string(),
        StreamState::Live => "live".to_string(),
        // Say which attempt, so a slow recovery doesn't look like a hang.
        StreamState::Reconnecting { attempt } => format!("reconnecting ({attempt})"),
        StreamState::Closed => "finished".to_string(),
    };

    rsx! {
        section { class: "stream",
            div { class: "stream-head",
                div { class: "card-ident",
                    h3 { class: "card-title", "{agent.name}" }
                    div { class: "card-sub", "{agent.id}" }
                }
                span { class: "pill tone-{tone}",
                    span { class: "glyph tone-{tone}" }
                    "{label}"
                }
                button {
                    class: "act-btn",
                    onclick: move |_| ui.selected.set(None),
                    "Close"
                }
            }
            div { class: "stream-body",
                if current.entries.is_empty() {
                    div { class: "stream-empty",
                        if current.state.is_problem() {
                            "Waiting to reconnect…"
                        } else {
                            "No output yet."
                        }
                    }
                }
                for (i , entry) in current.entries.iter().enumerate() {
                    match entry {
                        Entry::Event(ev) => rsx! {
                            StreamEvent { key: "e{i}", event: (**ev).clone() }
                        },
                        // Rendered inline so the timeline is honest about being
                        // discontinuous rather than quietly skipping.
                        Entry::Gap { skipped } => rsx! {
                            div { key: "g{i}", class: "stream-gap",
                                span { class: "glyph tone-wait" }
                                "{skipped} events were dropped and replayed from the store"
                            }
                        },
                        Entry::Undecodable { why } => rsx! {
                            div { key: "u{i}", class: "stream-gap is-bad",
                                span { class: "glyph tone-bad" }
                                "could not decode a frame — {why}"
                            }
                        },
                    }
                }
            }
        }
    }
}

/// One event line.
#[component]
fn StreamEvent(event: FleetEvent) -> Element {
    let seq = event.seq;
    match &event.kind {
        EventKind::Output { chunk, .. } => rsx! {
            pre { class: "ev-out", "{chunk}" }
        },
        EventKind::ToolStarted { name, .. } => rsx! {
            div { class: "ev-line",
                span { class: "ev-seq", "{seq}" }
                span { class: "ev-tool", "{name}" }
                span { class: "ev-kind", "started" }
            }
        },
        EventKind::ToolFinished { id, name, ok } => rsx! {
            div { class: "ev-line",
                span { class: "ev-seq", "{seq}" }
                // Caliban's ToolCallEnd omits the name (#106); fall back to the
                // correlation id so the row is never blank.
                span { class: "ev-tool",
                    if name.is_empty() { "{id}" } else { "{name}" }
                }
                span { class: if *ok { "pill tone-done" } else { "pill tone-bad" },
                    if *ok { "ok" } else { "failed" }
                }
            }
        },
        EventKind::AgentFinished {
            outcome,
            turns,
            cost_usd,
        } => rsx! {
            div { class: "ev-finish",
                span { class: "glyph tone-done" }
                "finished · {outcome} · {turns} turns · ${cost_usd:.4}"
            }
        },
        other => rsx! {
            div { class: "ev-line",
                span { class: "ev-seq", "{seq}" }
                span { class: "ev-kind", "{kind_label(other)}" }
            }
        },
    }
}

/// Short label for the event kinds without a dedicated row.
fn kind_label(kind: &EventKind) -> String {
    match kind {
        EventKind::AgentInit { model, .. } => format!("init · {model}"),
        EventKind::StatusChanged { from, to } => {
            format!("{} → {}", status_label(*from), status_label(*to))
        }
        EventKind::StorePersistFailed { lost_seq, .. } => {
            format!("store append failed at seq {lost_seq}")
        }
        EventKind::RepoHealth { .. } => "workspace health".into(),
        // Every other kind has its own row; this is the catch-all.
        _ => "event".into(),
    }
}
