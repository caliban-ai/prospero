//! `prospero usage` (#223): cost, turns and outcomes per workspace.
//!
//! This module is strict about the window it sends: anything it cannot turn
//! into a well-formed bound is refused here, with a message naming the flag.
//! Since #255 the server validates the window too, but a daemon older than that
//! compares `since` as a string and feeds `days` to date arithmetic unchecked —
//! so the CLI does not rely on it, and a bad value never leaves the machine.

use anyhow::{Result, bail};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use prospero_types::{OutcomeCounts, UsageGroup, UsageReport};

/// Largest `--since` day count accepted: the server's own limit, shared through
/// `prospero-types` so the two cannot drift.
const MAX_DAYS: u64 = prospero_types::MAX_USAGE_WINDOW_DAYS as u64;

const SINCE_FORMS: &str =
    "expected a day count like 7d or 2w, a date like 2026-09-01, or an RFC-3339 timestamp";

/// The `/api/usage` request for a `--since` value.
///
/// - absent → the server's default window;
/// - `7d` / `2w` → `days=`, resolved against the *server's* clock, as the
///   dashboard does, so a drifted client clock cannot clip the window;
/// - `2026-09-01` → that day's UTC midnight, since usage is bucketed by UTC day;
/// - an RFC-3339 timestamp → the same instant in UTC.
pub fn usage_path(since: Option<&str>) -> Result<String> {
    let Some(raw) = since else {
        return Ok("/api/usage".to_string());
    };
    let since = raw.trim();

    if let Some(days) = day_count(since)? {
        return Ok(format!("/api/usage?days={days}"));
    }
    if let Ok(date) = NaiveDate::parse_from_str(since, "%Y-%m-%d") {
        let midnight = date
            .and_hms_opt(0, 0, 0)
            .expect("midnight exists on every date")
            .and_utc();
        return Ok(format!("/api/usage?since={}", stamp(midnight)));
    }
    if let Ok(at) = DateTime::parse_from_rfc3339(since) {
        return Ok(format!(
            "/api/usage?since={}",
            stamp(at.with_timezone(&Utc))
        ));
    }
    bail!("--since {raw:?}: {SINCE_FORMS}")
}

/// `Nd` / `Nw` as a number of days, `None` if `s` is not a duration at all.
fn day_count(s: &str) -> Result<Option<u64>> {
    let Some(unit) = s.chars().last() else {
        return Ok(None);
    };
    let Ok(n) = s[..s.len() - unit.len_utf8()].parse::<u64>() else {
        return Ok(None);
    };

    let days = match unit {
        'd' => Some(n),
        'w' => n.checked_mul(7),
        'h' | 'm' | 's' => bail!(
            "--since {s}: usage is recorded per UTC day, so it can only look back \
             whole days (try 1d)"
        ),
        _ => return Ok(None),
    };

    match days {
        Some(0) => bail!("--since {s}: the window must be at least one day"),
        Some(d) if d <= MAX_DAYS => Ok(Some(d)),
        _ => bail!("--since {s}: at most {MAX_DAYS} days"),
    }
}

/// A window bound, spelled the way the store spells its own timestamps and
/// escaped for a query string.
///
/// The store compares event timestamps to the bound as *strings*, and events
/// are stamped with `Utc::now().to_rfc3339()`: a `+00:00` suffix, with any
/// fractional seconds before it. Only a bound in that same spelling sorts
/// correctly against them — a `Z` suffix sorts after the `.` of a fractional
/// second, so an event half a second into the window would compare as before
/// it and drop out. A current server re-renders the bound itself (#255); an
/// older one does not, so the CLI sends the safe spelling either way. That `+`
/// then has to be percent-encoded, because a bare `+` in a query string decodes
/// to a space.
fn stamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, false)
        .replace('+', "%2B")
}

/// Narrow a raw report to one workspace.
///
/// The server has no workspace filter, so this is done client-side — and on
/// the raw JSON rather than the typed report, so that `--json` stays the
/// server's own bytes: a field this CLI build does not know about survives.
/// Both output modes filter here, so they cannot disagree about which rows
/// they show.
pub fn only_workspace(mut report: serde_json::Value, name: &str) -> serde_json::Value {
    if let Some(groups) = report
        .get_mut("groups")
        .and_then(serde_json::Value::as_array_mut)
    {
        groups.retain(|g| g["workspace"].as_str() == Some(name));
    }
    report
}

/// The report as a table: one row per workspace, a total when there is more
/// than one, and the window it covers stated up front.
pub fn render(report: &UsageReport, workspace: Option<&str>) -> String {
    let (since, until) = (shown(&report.since), shown(&report.until));

    if report.groups.is_empty() {
        return match workspace {
            // The server lists only workspaces with activity, so this cannot
            // tell an idle workspace from a misspelled one; it says only what
            // it knows.
            Some(w) => {
                format!("no usage recorded for workspace '{w}' between {since} and {until}\n")
            }
            None => format!("no usage recorded between {since} and {until}\n"),
        };
    }

    let total = Row::total(&report.groups);
    let mut rows: Vec<Row> = report.groups.iter().map(Row::from).collect();
    if rows.len() > 1 {
        rows.push(total.clone());
    }

    let name_width = rows
        .iter()
        .map(|r| r.name.len())
        .chain(["WORKSPACE".len()])
        .max()
        .unwrap_or_default();

    let mut out = format!("usage {since} → {until}\n\n");
    out.push_str(&format!(
        "{:<name_width$}  {:>10}  {:>7}  {:>6}  {:>6}  {:>6}  {:>7}\n",
        "WORKSPACE", "COST", "TURNS", "DONE", "FAILED", "KILLED", "CRASHED"
    ));
    for r in &rows {
        out.push_str(&format!(
            "{:<name_width$}  {:>10}  {:>7}  {:>6}  {:>6}  {:>6}  {:>7}\n",
            r.name,
            format!("${:.4}", r.cost_usd),
            r.turns,
            r.outcomes.done,
            r.outcomes.failed,
            r.outcomes.killed,
            r.outcomes.crashed,
        ));
    }

    // `timed_out` is a subset of `killed` (#221): the kill was real, but it was
    // policy rather than a person. Say so instead of adding a column that
    // would double-count.
    match total.outcomes.timed_out {
        0 => {}
        1 => out.push_str("\nkilled includes 1 run stopped by its wall-clock time limit\n"),
        n => out.push_str(&format!(
            "\nkilled includes {n} runs stopped by their wall-clock time limit\n"
        )),
    }
    out
}

/// A window bound for a person: the minute, in UTC.
///
/// The server's default window ends at `now.to_rfc3339()`, nanoseconds and all.
/// A bound that does not parse is shown as sent rather than hidden.
fn shown(ts: &str) -> String {
    DateTime::parse_from_rfc3339(ts)
        .map(|t| {
            t.with_timezone(&Utc)
                .format("%Y-%m-%d %H:%M UTC")
                .to_string()
        })
        .unwrap_or_else(|_| ts.to_string())
}

/// One table row: a workspace, or the total across all of them.
#[derive(Clone)]
struct Row {
    name: String,
    cost_usd: f64,
    turns: u64,
    outcomes: OutcomeCounts,
}

impl From<&UsageGroup> for Row {
    fn from(g: &UsageGroup) -> Self {
        Row {
            name: g.workspace.clone(),
            cost_usd: g.cost_usd,
            turns: g.turns,
            outcomes: g.outcomes,
        }
    }
}

impl Row {
    fn total(groups: &[UsageGroup]) -> Row {
        let mut total = Row {
            name: "TOTAL".to_string(),
            cost_usd: 0.0,
            turns: 0,
            outcomes: OutcomeCounts::default(),
        };
        for g in groups {
            total.cost_usd += g.cost_usd;
            total.turns += g.turns;
            total.outcomes.done += g.outcomes.done;
            total.outcomes.failed += g.outcomes.failed;
            total.outcomes.killed += g.outcomes.killed;
            total.outcomes.crashed += g.outcomes.crashed;
            total.outcomes.timed_out += g.outcomes.timed_out;
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn group(workspace: &str, cost_usd: f64, turns: u64, done: u64) -> UsageGroup {
        UsageGroup {
            workspace: workspace.to_string(),
            cost_usd,
            turns,
            outcomes: OutcomeCounts {
                done,
                failed: 1,
                killed: 0,
                crashed: 0,
                timed_out: 0,
            },
            series: Vec::new(),
        }
    }

    fn report(groups: Vec<UsageGroup>) -> UsageReport {
        UsageReport {
            since: "2026-09-13T00:00:00Z".to_string(),
            until: "2026-09-20T00:00:00Z".to_string(),
            groups,
        }
    }

    #[test]
    fn the_table_states_its_window_and_one_row_per_workspace() {
        let out = render(
            &report(vec![group("alpha", 1.25, 40, 3), group("beta", 0.5, 2, 1)]),
            None,
        );
        assert!(
            out.contains("2026-09-13 00:00 UTC") && out.contains("2026-09-20 00:00 UTC"),
            "the window should be stated:\n{out}"
        );
        let alpha = out.lines().find(|l| l.starts_with("alpha")).expect(&out);
        assert!(alpha.contains("$1.2500"), "{alpha}");
        assert!(alpha.contains(" 40 "), "{alpha}");
        assert!(out.lines().any(|l| l.starts_with("beta")), "{out}");
    }

    /// The server's default window ends at `now.to_rfc3339()` — nanoseconds
    /// and an offset. A person reading the table needs the minute, not that.
    #[test]
    fn the_window_is_shown_to_the_minute_in_utc() {
        let mut r = report(vec![group("alpha", 1.0, 1, 1)]);
        r.since = "2026-09-13T20:50:07.123456789+00:00".to_string();
        r.until = "2026-09-20T22:50:07.123456789+02:00".to_string();
        let out = render(&r, None);
        let header = out.lines().next().unwrap();
        assert_eq!(header, "usage 2026-09-13 20:50 UTC → 2026-09-20 20:50 UTC");
    }

    /// Anything unparseable is shown as sent rather than hidden.
    #[test]
    fn an_unparseable_bound_is_shown_verbatim() {
        let mut r = report(vec![group("alpha", 1.0, 1, 1)]);
        r.since = "whenever".to_string();
        assert!(render(&r, None).contains("whenever"));
    }

    #[test]
    fn several_workspaces_get_a_total_row() {
        let out = render(
            &report(vec![group("alpha", 1.25, 40, 3), group("beta", 0.5, 2, 1)]),
            None,
        );
        let total = out.lines().find(|l| l.starts_with("TOTAL")).expect(&out);
        assert!(total.contains("$1.7500"), "{total}");
        assert!(total.contains(" 42 "), "{total}");
    }

    /// A total of one row only repeats it.
    #[test]
    fn a_single_workspace_has_no_total_row() {
        let out = render(&report(vec![group("alpha", 1.25, 40, 3)]), None);
        assert!(!out.lines().any(|l| l.starts_with("TOTAL")), "{out}");
    }

    #[test]
    fn an_empty_window_says_so() {
        let out = render(&report(Vec::new()), None);
        assert!(out.contains("no usage recorded"), "{out}");
    }

    /// The server lists only workspaces with activity, so an empty result
    /// for a named workspace cannot tell "idle" from "no such workspace" —
    /// the message names it and claims no more than it knows.
    #[test]
    fn an_empty_filtered_window_names_the_workspace() {
        let out = render(&report(Vec::new()), Some("gamma"));
        assert!(out.contains("gamma"), "{out}");
    }

    /// A timed-out run is also counted as killed; say so rather than let the
    /// reader assume every kill was a person.
    #[test]
    fn timed_out_runs_are_explained_under_the_table() {
        let mut g = group("alpha", 1.0, 1, 0);
        g.outcomes.killed = 2;
        g.outcomes.timed_out = 1;
        let out = render(&report(vec![g]), None);
        assert!(out.contains("1 run"), "{out}");
        assert!(out.to_lowercase().contains("time"), "{out}");

        let quiet = render(&report(vec![group("alpha", 1.0, 1, 0)]), None);
        assert!(!quiet.to_lowercase().contains("time limit"), "{quiet}");
    }

    #[test]
    fn filtering_keeps_one_workspace_and_the_window() {
        let raw = json!({
            "since": "a", "until": "b",
            "groups": [{"workspace": "alpha"}, {"workspace": "beta"}],
        });
        let narrowed = only_workspace(raw, "beta");
        assert_eq!(narrowed["since"], "a");
        assert_eq!(narrowed["until"], "b");
        assert_eq!(narrowed["groups"], json!([{"workspace": "beta"}]));
    }

    /// `--json` is the raw report; a field this CLI build does not know about
    /// must survive filtering, or a newer server's data silently vanishes.
    #[test]
    fn filtering_preserves_fields_this_build_does_not_know() {
        let raw = json!({
            "since": "a", "until": "b", "from_the_future": 1,
            "groups": [{"workspace": "alpha", "also_new": true}],
        });
        let narrowed = only_workspace(raw, "alpha");
        assert_eq!(narrowed["from_the_future"], 1);
        assert_eq!(narrowed["groups"][0]["also_new"], true);
    }

    #[test]
    fn no_since_leaves_the_window_to_the_server() {
        assert_eq!(usage_path(None).unwrap(), "/api/usage");
    }

    #[test]
    fn a_day_count_is_resolved_on_the_servers_clock() {
        assert_eq!(usage_path(Some("7d")).unwrap(), "/api/usage?days=7");
        assert_eq!(usage_path(Some("30d")).unwrap(), "/api/usage?days=30");
    }

    #[test]
    fn weeks_become_days() {
        assert_eq!(usage_path(Some("2w")).unwrap(), "/api/usage?days=14");
    }

    #[test]
    fn a_bare_date_starts_at_utc_midnight() {
        let path = usage_path(Some("2026-09-01")).unwrap();
        assert_eq!(since_sent(&path), "2026-09-01T00:00:00+00:00");
    }

    #[test]
    fn a_timestamp_is_normalized_to_utc() {
        let path = usage_path(Some("2026-09-01T12:30:00Z")).unwrap();
        assert_eq!(since_sent(&path), "2026-09-01T12:30:00+00:00");
    }

    /// A `+` in a query string decodes to a space, so a bound carrying an
    /// offset must be percent-encoded or it reaches the server mangled.
    #[test]
    fn the_bound_survives_the_query_string() {
        let path = usage_path(Some("2026-09-01T02:00:00+02:00")).unwrap();
        assert!(!path.contains('+'), "a raw + decodes to a space: {path}");
        assert_eq!(since_sent(&path), "2026-09-01T00:00:00+00:00");
    }

    /// The store compares event timestamps to the bound **as strings**, and
    /// events are stamped with `Utc::now().to_rfc3339()` — fractional seconds
    /// and a `+00:00` suffix. A bound in any other spelling mis-sorts: with a
    /// `Z`, an event half a second after midnight compares *before* a midnight
    /// bound (`'.'` < `'Z'`) and silently drops out of the window.
    #[test]
    fn the_bound_sorts_correctly_against_the_stores_own_timestamps() {
        let bound = since_sent(&usage_path(Some("2026-09-01")).unwrap());

        let midnight = chrono::NaiveDate::from_ymd_opt(2026, 9, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let just_after = (midnight + chrono::Duration::milliseconds(500)).to_rfc3339();
        let exactly = midnight.to_rfc3339();
        let just_before = (midnight - chrono::Duration::milliseconds(500)).to_rfc3339();

        assert!(
            just_after.as_str() >= bound.as_str(),
            "{just_after} is inside the window but sorts before {bound}"
        );
        assert!(
            exactly.as_str() >= bound.as_str(),
            "{exactly} is the inclusive start but sorts before {bound}"
        );
        assert!(
            just_before.as_str() < bound.as_str(),
            "{just_before} is outside the window but sorts after {bound}"
        );
    }

    /// The `since` value the server will decode from a request path.
    fn since_sent(path: &str) -> String {
        path.split_once("since=")
            .expect("path should carry since")
            .1
            .replace("%2B", "+")
    }

    /// Usage is bucketed by UTC day, so an hour-level window would promise a
    /// precision the report does not have.
    #[test]
    fn sub_day_durations_are_refused_with_a_reason() {
        let err = usage_path(Some("24h")).unwrap_err().to_string();
        assert!(err.contains("day"), "{err}");
    }

    #[test]
    fn a_zero_day_window_is_refused() {
        assert!(usage_path(Some("0d")).is_err());
    }

    /// A daemon older than #255 subtracts `days` from a `DateTime` unchecked,
    /// which panics once the result leaves chrono's range — so an enormous
    /// count must never leave the CLI.
    #[test]
    fn a_day_count_beyond_any_real_history_is_refused() {
        assert_eq!(usage_path(Some("36500d")).unwrap(), "/api/usage?days=36500");
        for huge in ["36501d", "200000000d", "99999999999w"] {
            assert!(usage_path(Some(huge)).is_err(), "{huge} should be refused");
        }
    }

    /// The server does not validate `since` — it compares it as a string — so
    /// anything unrecognized must stop here rather than yield a silently wrong
    /// report.
    #[test]
    fn anything_else_is_refused_rather_than_sent() {
        for bad in ["yesterday", "7", "d", "2026-13-40", "-3d", ""] {
            assert!(usage_path(Some(bad)).is_err(), "{bad:?} should be refused");
        }
    }
}
