//! Request/response payloads for the HTTP API.
//!
//! The shared contract types now live in `prospero-types` (#172) so the WASM
//! dashboard can use the exact same definitions — see that crate's `api` module
//! for why. They are re-exported here from their original paths, so every import
//! site and all serde output are unchanged.
//!
//! What stays in this crate is what does *not* belong in a neutral DTO crate:
//! the axum query extractor, and the mapping from a wire body onto
//! `prospero-core`'s domain types. ADR 0006 makes `api → core` one-directional,
//! so that mapping is the adapter's job — pushing it into `core` (or into
//! `prospero-types`) would drag a transport concept downward.

use prospero_core::fleet::SpawnRequest;
use prospero_core::store::UsageRow;
use serde::Deserialize;

pub use prospero_types::{
    AddWorkspaceBody, AgentInputBody, Capabilities, OutcomeCounts, RespawnedResponse,
    SetConfigBody, SpawnBody, SpawnedResponse, UsageBucket, UsageGroup, UsageReport,
    WorkspaceSummary,
};

/// Query params for `GET /api/agents/{id}/events` and `/stream`.
///
/// Stays here: this is an axum extractor for a URL query string, not part of the
/// client/server body contract.
#[derive(Debug, Deserialize)]
pub struct FromSeq {
    /// Return events with `seq >= from` (default 0).
    #[serde(default)]
    pub from: u64,
}

/// Map a spawn request body onto the core [`SpawnRequest`].
///
/// A free function rather than a method because [`SpawnBody`] is defined in
/// `prospero-types` and an inherent impl can only be written by the defining
/// crate. Keeping the conversion here — rather than adding a `From` impl in
/// `prospero-core` — preserves ADR 0006's one-way `api → core` dependency.
pub fn spawn_request(body: SpawnBody) -> SpawnRequest {
    let isolation_worktree = body.isolation_worktree();
    SpawnRequest {
        prompt: body.prompt,
        label: body.label,
        model: body.model,
        isolation_worktree,
        tool_allowlist: body.tool_allowlist,
        interactive: body.interactive,
        frontmatter_path: body.frontmatter_path.map(std::path::PathBuf::from),
        provider_ref: body.provider_ref,
    }
}

/// Query params for `GET /api/usage`.
///
/// Both bounds are optional; the handler fills in a default window and echoes
/// back what it used. Like [`FromSeq`], this is a URL-query extractor rather
/// than part of the body contract, so it stays in this crate.
#[derive(Debug, Default, Deserialize)]
pub struct UsageQuery {
    /// Inclusive window start (RFC-3339). Defaults to 7 days before `until`.
    pub since: Option<String>,
    /// Exclusive window end (RFC-3339). Defaults to now.
    pub until: Option<String>,
    /// How many days back to look, as an alternative to `since`.
    ///
    /// The dashboard's window control sends this rather than a computed
    /// timestamp so the server resolves the bound against its own clock; a
    /// browser whose clock has drifted would otherwise silently clip or pad the
    /// window. Ignored when `since` is given explicitly.
    pub days: Option<i64>,
}

/// Fold the store's flat (workspace, day) rows into the per-workspace report.
///
/// The store already did the aggregation; this only reshapes. Another adapter
/// mapping in the ADR 0006 sense — [`UsageRow`] is a `prospero-core` type and
/// [`UsageReport`] a `prospero-types` one, and neither crate should know about
/// the other.
///
/// Rows are expected in (workspace, day) order — both SQL backends `ORDER BY`
/// that way and the in-memory fold uses a `BTreeMap` — but this does not rely on
/// it: groups are collected by name and each series sorted before returning, so
/// a backend that ordered differently still produces the same report.
pub fn usage_report(rows: Vec<UsageRow>, since: &str, until: &str) -> UsageReport {
    use std::collections::BTreeMap;

    let mut groups: BTreeMap<String, UsageGroup> = BTreeMap::new();
    for r in rows {
        let g = groups
            .entry(r.workspace.clone())
            .or_insert_with(|| UsageGroup {
                workspace: r.workspace.clone(),
                ..UsageGroup::default()
            });
        g.cost_usd += r.cost_usd;
        g.turns += r.turns;
        g.outcomes.done += r.done;
        g.outcomes.failed += r.failed;
        g.outcomes.killed += r.killed;
        g.outcomes.crashed += r.crashed;
        g.series.push(UsageBucket {
            day: r.day,
            cost_usd: r.cost_usd,
            turns: r.turns,
            outcomes: OutcomeCounts {
                done: r.done,
                failed: r.failed,
                killed: r.killed,
                crashed: r.crashed,
            },
        });
    }

    let mut groups: Vec<UsageGroup> = groups.into_values().collect();
    for g in &mut groups {
        g.series.sort_by(|a, b| a.day.cmp(&b.day));
    }

    UsageReport {
        since: since.to_string(),
        until: until.to_string(),
        groups,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(workspace: &str, day: &str, cost: f64, turns: u64) -> UsageRow {
        UsageRow {
            workspace: workspace.into(),
            day: day.into(),
            cost_usd: cost,
            turns,
            done: 0,
            failed: 0,
            killed: 0,
            crashed: 0,
        }
    }

    #[test]
    fn usage_report_folds_days_into_per_workspace_totals() {
        let rows = vec![
            row("alpha", "2026-08-01", 0.75, 4),
            row("alpha", "2026-08-02", 1.00, 2),
            row("beta", "2026-08-01", 0.10, 1),
        ];

        let report = usage_report(
            rows,
            "2026-08-01T00:00:00+00:00",
            "2026-08-03T00:00:00+00:00",
        );

        assert_eq!(report.since, "2026-08-01T00:00:00+00:00");
        assert_eq!(report.until, "2026-08-03T00:00:00+00:00");
        assert_eq!(report.groups.len(), 2);

        let alpha = &report.groups[0];
        assert_eq!(alpha.workspace, "alpha");
        assert!((alpha.cost_usd - 1.75).abs() < 1e-9);
        assert_eq!(alpha.turns, 6);
        assert_eq!(
            alpha
                .series
                .iter()
                .map(|b| b.day.as_str())
                .collect::<Vec<_>>(),
            vec!["2026-08-01", "2026-08-02"],
            "the series must stay ascending by day"
        );

        let beta = &report.groups[1];
        assert_eq!(beta.workspace, "beta");
        assert_eq!(beta.series.len(), 1);
    }

    #[test]
    fn usage_report_sums_outcomes_across_the_window() {
        let mut a = row("alpha", "2026-08-01", 0.0, 0);
        a.done = 2;
        a.killed = 1;
        let mut b = row("alpha", "2026-08-02", 0.0, 0);
        b.failed = 3;

        let report = usage_report(vec![a, b], "s", "u");

        let g = &report.groups[0];
        assert_eq!(g.outcomes.done, 2);
        assert_eq!(g.outcomes.killed, 1);
        assert_eq!(g.outcomes.failed, 3);
        assert_eq!(g.outcomes.total(), 6);
    }

    /// A killed agent never reports cost, so a workspace can show outcomes
    /// against zero spend. The fold must preserve that rather than dropping the
    /// group as empty.
    #[test]
    fn usage_report_keeps_a_workspace_with_outcomes_but_no_cost() {
        let mut killed = row("beta", "2026-08-01", 0.0, 0);
        killed.killed = 1;

        let report = usage_report(vec![killed], "s", "u");

        assert_eq!(report.groups.len(), 1);
        assert_eq!(report.groups[0].cost_usd, 0.0);
        assert_eq!(report.groups[0].outcomes.killed, 1);
    }

    #[test]
    fn usage_report_over_an_empty_window_has_no_groups() {
        let report = usage_report(Vec::new(), "s", "u");
        assert!(report.groups.is_empty());
    }

    #[test]
    fn spawn_body_interactive_round_trips_and_defaults_false() {
        let with: SpawnBody = serde_json::from_str(r#"{"prompt":"p","interactive":true}"#).unwrap();
        assert!(spawn_request(with).interactive);
        let without: SpawnBody = serde_json::from_str(r#"{"prompt":"p"}"#).unwrap();
        assert!(!spawn_request(without).interactive);
    }

    #[test]
    fn spawn_body_carries_frontmatter_path() {
        let with: SpawnBody =
            serde_json::from_str(r#"{"prompt":"p","frontmatter_path":"/tpl.md"}"#).unwrap();
        assert_eq!(
            spawn_request(with).frontmatter_path,
            Some(std::path::PathBuf::from("/tpl.md"))
        );
        let without: SpawnBody = serde_json::from_str(r#"{"prompt":"p"}"#).unwrap();
        assert_eq!(spawn_request(without).frontmatter_path, None);
    }

    #[test]
    fn spawn_defaults_to_worktree_and_only_shared_opts_out() {
        let default: SpawnBody = serde_json::from_str(r#"{"prompt":"p"}"#).unwrap();
        assert!(spawn_request(default).isolation_worktree);
        let shared: SpawnBody =
            serde_json::from_str(r#"{"prompt":"p","isolation":"shared"}"#).unwrap();
        assert!(!spawn_request(shared).isolation_worktree);
    }

    #[test]
    fn workspace_summary_exposes_sources() {
        let s = WorkspaceSummary {
            name: "ws".into(),
            root: "/ws".into(),
            sources: vec![prospero_core::Source {
                name: "a".into(),
                path: "/ws/a".into(),
            }],
            health: prospero_core::WorkspaceHealth::Healthy,
            agent_count: 0,
            config: prospero_core::registry::RepoProviderConfig::default(),
            source_specs: Vec::new(),
            display_name: None,
            providers: Vec::new(),
            default_provider: None,
            status: None,
        };
        let j = serde_json::to_value(&s).unwrap();
        assert_eq!(j["sources"][0]["name"], "a");
        // A local workspace has no CR specs, so the key must not appear at all.
        assert!(
            j.get("source_specs").is_none(),
            "local payload gained a k8s key: {j}"
        );
    }

    /// `sources` is the *discovered* view (name + path) — all a local checkout
    /// has. A k8s workspace additionally carries the git remote and ref, and
    /// the v2 config editor needs those to round-trip an edit: without them an
    /// operator would face blank remote fields and have to retype every one
    /// from memory. (#175)
    #[test]
    fn source_specs_carry_the_remote_and_ref_that_sources_loses() {
        let s = WorkspaceSummary {
            name: "ws".into(),
            root: String::new(),
            sources: vec![prospero_core::Source {
                name: "caliban".into(),
                path: "/work/caliban".into(),
            }],
            health: prospero_core::WorkspaceHealth::Healthy,
            agent_count: 0,
            config: prospero_core::registry::RepoProviderConfig::default(),
            source_specs: vec![prospero_types::WorkspaceSourceSpec {
                name: "caliban".into(),
                repo: "git@github.com:caliban-ai/caliban.git".into(),
                r#ref: Some("main".into()),
                path: "/work/caliban".into(),
            }],
            display_name: None,
            providers: Vec::new(),
            default_provider: None,
            status: None,
        };
        let j = serde_json::to_value(&s).unwrap();
        assert_eq!(
            j["source_specs"][0]["repo"],
            "git@github.com:caliban-ai/caliban.git"
        );
        assert_eq!(j["source_specs"][0]["ref"], "main");
        // And it round-trips back, which is what the dashboard depends on.
        let back: WorkspaceSummary = serde_json::from_value(j).unwrap();
        assert_eq!(
            back.source_specs[0].repo,
            "git@github.com:caliban-ai/caliban.git"
        );
    }
}
