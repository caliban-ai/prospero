//! Chart geometry and formatting for the fleet overview (#181).
//!
//! Hand-rolled SVG rather than a plotting crate. The page is served under
//! `default-src 'none'` with no external anything, and the bundle is a committed
//! artifact — a few hundred lines of geometry costs less than a plotting
//! dependency's wasm footprint, and stays legible.
//!
//! No Dioxus and no `web-sys`, so every scale, bucket, and label decision below
//! is unit-tested on the host target.
//!
//! **Outcomes are faceted, never stacked.** In light mode the crashed (amber)
//! and failed (red) tokens sit at ΔE 3.5 for deuteranopia — and crashed vs
//! killed at 13.2 even with normal vision — so no ordering of a four-way stack
//! is separable. Faceting into one single-hue chart per outcome means no two
//! outcome colours are ever adjacent, which is the real fix.

use prospero_types::{UsageBucket, UsageReport};

/// The window the overview charts cover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Window {
    /// Last 24 hours.
    Day,
    /// Last 7 days.
    Week,
    /// Last 30 days.
    Month,
}

impl Window {
    /// Every window, in the order the control offers them.
    pub fn all() -> [Window; 3] {
        [Window::Day, Window::Week, Window::Month]
    }

    /// How many days back this window reaches.
    pub fn days(self) -> i64 {
        match self {
            Window::Day => 1,
            Window::Week => 7,
            Window::Month => 30,
        }
    }

    /// Control label.
    pub fn label(self) -> &'static str {
        match self {
            Window::Day => "24h",
            Window::Week => "7d",
            Window::Month => "30d",
        }
    }
}

/// One plotted point: a day and its value.
#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    /// UTC day, `YYYY-MM-DD`.
    pub day: String,
    /// The value on that day.
    pub value: f64,
}

/// Which number a series pulls out of a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Measure {
    /// Spend in USD.
    Cost,
    /// Turns taken.
    Turns,
    /// Agents that reached `done`.
    Done,
    /// Agents that reached `killed`.
    Killed,
    /// Agents that reached `failed`.
    Failed,
    /// Agents that reached `crashed`.
    Crashed,
}

impl Measure {
    /// Pull this measure out of one bucket.
    fn of(self, b: &UsageBucket) -> f64 {
        match self {
            Measure::Cost => b.cost_usd,
            Measure::Turns => b.turns as f64,
            Measure::Done => b.outcomes.done as f64,
            Measure::Killed => b.outcomes.killed as f64,
            Measure::Failed => b.outcomes.failed as f64,
            Measure::Crashed => b.outcomes.crashed as f64,
        }
    }

    /// Human label for the facet or axis.
    pub fn label(self) -> &'static str {
        match self {
            Measure::Cost => "Spend",
            Measure::Turns => "Turns",
            Measure::Done => "Done",
            Measure::Killed => "Killed",
            Measure::Failed => "Failed",
            Measure::Crashed => "Crashed",
        }
    }

    /// Token suffix for this measure's hue, matching the status tones already
    /// used by the pills. Cost and turns are neutral accent, not a status.
    pub fn tone(self) -> &'static str {
        match self {
            Measure::Cost | Measure::Turns => "accent",
            Measure::Done => "live",
            Measure::Killed => "done",
            Measure::Failed => "bad",
            Measure::Crashed => "wait",
        }
    }
}

/// Flatten every workspace's daily series into one fleet-wide series for
/// `measure`, summed per day and ordered ascending.
///
/// Workspaces are summed rather than plotted separately: the overview answers
/// "where is the fleet's spend and failure going", and a per-workspace split at
/// fleet scale would exceed the categorical cap the palette can separate.
pub fn fleet_series(report: &UsageReport, measure: Measure) -> Vec<Point> {
    use std::collections::BTreeMap;

    let mut by_day: BTreeMap<&str, f64> = BTreeMap::new();
    for group in &report.groups {
        for bucket in &group.series {
            *by_day.entry(bucket.day.as_str()).or_insert(0.0) += measure.of(bucket);
        }
    }
    by_day
        .into_iter()
        .map(|(day, value)| Point {
            day: day.to_string(),
            value,
        })
        .collect()
}

/// The largest value in a series, or 0 when empty.
pub fn peak(points: &[Point]) -> f64 {
    points.iter().map(|p| p.value).fold(0.0, f64::max)
}

/// Round an axis maximum up to something a human reads cleanly (1, 2, 5 × 10ⁿ).
///
/// A bare data maximum makes the top gridline an arbitrary number like 0.4213;
/// rounding up gives the axis a legible ceiling and leaves headroom above the
/// tallest mark.
pub fn nice_max(raw: f64) -> f64 {
    if !raw.is_finite() || raw <= 0.0 {
        return 1.0;
    }
    let exp = raw.log10().floor();
    let pow = 10f64.powf(exp);
    let norm = raw / pow;
    let step = if norm <= 1.0 {
        1.0
    } else if norm <= 2.0 {
        2.0
    } else if norm <= 5.0 {
        5.0
    } else {
        10.0
    };
    step * pow
}

/// One bar's rectangle in user units, ready for an SVG `rect`.
#[derive(Debug, Clone, PartialEq)]
pub struct Bar {
    /// Left edge.
    pub x: f64,
    /// Top edge.
    pub y: f64,
    /// Bar width.
    pub w: f64,
    /// Bar height.
    pub h: f64,
    /// The day this bar covers, for the tooltip.
    pub day: String,
    /// The raw value, for the tooltip.
    pub value: f64,
}

/// Lay out `points` as bars inside a `width` × `height` plot box.
///
/// A 2px gap is left between adjacent bars (the design system's spacer), and a
/// zero value still gets a hairline so an empty day reads as "nothing happened"
/// rather than as a missing bar.
pub fn bars(points: &[Point], width: f64, height: f64, max: f64) -> Vec<Bar> {
    if points.is_empty() || max <= 0.0 {
        return Vec::new();
    }
    let slot = width / points.len() as f64;
    let gap = if slot > 6.0 { 2.0 } else { 0.0 };
    let w = (slot - gap).max(1.0);
    points
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let ratio = (p.value / max).clamp(0.0, 1.0);
            // Floor at 1px so a zero day is drawn rather than absent.
            let h = (ratio * height).max(1.0);
            Bar {
                x: i as f64 * slot + gap / 2.0,
                y: height - h,
                w,
                h,
                day: p.day.clone(),
                value: p.value,
            }
        })
        .collect()
}

/// An SVG path for the area under `points`, closed to the baseline.
///
/// Returns `None` for an empty series so the caller renders an empty state
/// rather than a degenerate path.
pub fn area_path(points: &[Point], width: f64, height: f64, max: f64) -> Option<String> {
    if points.is_empty() || max <= 0.0 {
        return None;
    }
    let x_of = |i: usize| {
        if points.len() == 1 {
            width / 2.0
        } else {
            i as f64 * width / (points.len() - 1) as f64
        }
    };
    let y_of = |v: f64| height - (v / max).clamp(0.0, 1.0) * height;

    let mut d = String::new();
    for (i, p) in points.iter().enumerate() {
        let cmd = if i == 0 { 'M' } else { 'L' };
        d.push_str(&format!("{cmd}{:.2} {:.2}", x_of(i), y_of(p.value)));
        d.push(' ');
    }
    // Close down to the baseline and back, so the fill has a floor.
    d.push_str(&format!("L{:.2} {:.2} ", x_of(points.len() - 1), height));
    d.push_str(&format!("L{:.2} {:.2} Z", x_of(0), height));
    Some(d)
}

/// The line along the top of the area, without the baseline closure.
pub fn line_path(points: &[Point], width: f64, height: f64, max: f64) -> Option<String> {
    if points.is_empty() || max <= 0.0 {
        return None;
    }
    let x_of = |i: usize| {
        if points.len() == 1 {
            width / 2.0
        } else {
            i as f64 * width / (points.len() - 1) as f64
        }
    };
    let y_of = |v: f64| height - (v / max).clamp(0.0, 1.0) * height;
    let mut d = String::new();
    for (i, p) in points.iter().enumerate() {
        let cmd = if i == 0 { 'M' } else { 'L' };
        d.push_str(&format!("{cmd}{:.2} {:.2} ", x_of(i), y_of(p.value)));
    }
    Some(d.trim_end().to_string())
}

/// Format a USD amount the way an operator scans a bill.
///
/// Sub-cent spend still reads as a number rather than collapsing to `$0.00`,
/// because "we spent something small" and "we spent nothing" are different
/// facts.
pub fn format_usd(v: f64) -> String {
    if v <= 0.0 {
        "$0".to_string()
    } else if v < 0.01 {
        format!("${v:.4}")
    } else if v < 100.0 {
        format!("${v:.2}")
    } else {
        // `{:.0}` rounds half-to-even ($1234.5 → "$1234"), which reads as a
        // typo on a bill. Round half away from zero instead.
        format!("${}", v.round())
    }
}

/// Format a whole-number count with a thousands separator.
pub fn format_count(v: f64) -> String {
    let n = v.round() as i64;
    let s = n.abs().to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 { format!("-{out}") } else { out }
}

/// Shorten `YYYY-MM-DD` to the `MM-DD` an axis tick shows.
///
/// Anything not shaped like a date is returned whole: `"short".get(5..)` is
/// `Some("")`, not `None`, so a naive slice renders a blank tick rather than
/// falling back.
pub fn tick_label(day: &str) -> &str {
    if day.len() >= 10 && day.as_bytes().get(4) == Some(&b'-') {
        &day[5..]
    } else {
        day
    }
}

/// Total of a series.
pub fn total(points: &[Point]) -> f64 {
    points.iter().map(|p| p.value).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use prospero_types::{OutcomeCounts, UsageGroup};

    fn bucket(day: &str, cost: f64, turns: u64, o: OutcomeCounts) -> UsageBucket {
        UsageBucket {
            day: day.into(),
            cost_usd: cost,
            turns,
            outcomes: o,
        }
    }

    fn report(groups: Vec<UsageGroup>) -> UsageReport {
        UsageReport {
            since: "s".into(),
            until: "u".into(),
            groups,
        }
    }

    fn group(name: &str, series: Vec<UsageBucket>) -> UsageGroup {
        UsageGroup {
            workspace: name.into(),
            cost_usd: series.iter().map(|b| b.cost_usd).sum(),
            turns: series.iter().map(|b| b.turns).sum(),
            outcomes: OutcomeCounts::default(),
            series,
        }
    }

    #[test]
    fn fleet_series_sums_workspaces_per_day_in_date_order() {
        let r = report(vec![
            group(
                "alpha",
                vec![
                    bucket("2026-08-02", 1.0, 2, OutcomeCounts::default()),
                    bucket("2026-08-01", 0.5, 1, OutcomeCounts::default()),
                ],
            ),
            group(
                "beta",
                vec![bucket("2026-08-01", 0.25, 3, OutcomeCounts::default())],
            ),
        ]);

        let s = fleet_series(&r, Measure::Cost);
        assert_eq!(
            s.iter().map(|p| p.day.as_str()).collect::<Vec<_>>(),
            vec!["2026-08-01", "2026-08-02"],
            "days must come back ascending regardless of input order"
        );
        assert!((s[0].value - 0.75).abs() < 1e-9, "both workspaces summed");
        assert!((s[1].value - 1.0).abs() < 1e-9);
    }

    #[test]
    fn fleet_series_reads_each_outcome_measure() {
        let o = OutcomeCounts {
            done: 3,
            failed: 2,
            killed: 1,
            crashed: 4,
        };
        let r = report(vec![group("a", vec![bucket("2026-08-01", 0.0, 0, o)])]);

        assert_eq!(fleet_series(&r, Measure::Done)[0].value, 3.0);
        assert_eq!(fleet_series(&r, Measure::Failed)[0].value, 2.0);
        assert_eq!(fleet_series(&r, Measure::Killed)[0].value, 1.0);
        assert_eq!(fleet_series(&r, Measure::Crashed)[0].value, 4.0);
    }

    #[test]
    fn an_empty_report_yields_an_empty_series() {
        assert!(fleet_series(&report(Vec::new()), Measure::Cost).is_empty());
    }

    #[test]
    fn nice_max_rounds_up_to_a_readable_ceiling() {
        assert_eq!(nice_max(0.4213), 0.5);
        assert_eq!(nice_max(1.0), 1.0);
        assert_eq!(nice_max(3.0), 5.0);
        assert_eq!(nice_max(7.0), 10.0);
        assert_eq!(nice_max(42.0), 50.0);
        assert_eq!(nice_max(230.0), 500.0);
    }

    /// An all-zero window must not divide by zero or produce an infinite scale.
    #[test]
    fn nice_max_of_nothing_is_one() {
        assert_eq!(nice_max(0.0), 1.0);
        assert_eq!(nice_max(-5.0), 1.0);
        assert_eq!(nice_max(f64::NAN), 1.0);
    }

    #[test]
    fn bars_fill_the_box_and_scale_to_the_max() {
        let pts = vec![
            Point {
                day: "2026-08-01".into(),
                value: 5.0,
            },
            Point {
                day: "2026-08-02".into(),
                value: 10.0,
            },
        ];
        let b = bars(&pts, 100.0, 50.0, 10.0);
        assert_eq!(b.len(), 2);
        assert!((b[1].h - 50.0).abs() < 1e-9, "the peak fills the height");
        assert!((b[0].h - 25.0).abs() < 1e-9, "half the peak is half height");
        assert!(b[0].x < b[1].x, "bars advance left to right");
        // y + h == baseline, so bars are anchored to the floor.
        assert!((b[0].y + b[0].h - 50.0).abs() < 1e-9);
    }

    /// A day with no activity still gets a hairline — an absent bar reads as
    /// missing data, which is a different claim from "nothing happened".
    #[test]
    fn a_zero_day_still_draws_a_hairline() {
        let pts = vec![Point {
            day: "2026-08-01".into(),
            value: 0.0,
        }];
        let b = bars(&pts, 100.0, 50.0, 10.0);
        assert_eq!(b[0].h, 1.0);
    }

    #[test]
    fn bars_of_an_empty_series_are_empty() {
        assert!(bars(&[], 100.0, 50.0, 10.0).is_empty());
    }

    #[test]
    fn area_path_closes_to_the_baseline() {
        let pts = vec![
            Point {
                day: "2026-08-01".into(),
                value: 0.0,
            },
            Point {
                day: "2026-08-02".into(),
                value: 10.0,
            },
        ];
        let d = area_path(&pts, 100.0, 50.0, 10.0).unwrap();
        assert!(d.starts_with("M0.00 50.00"), "starts at the floor: {d}");
        assert!(d.contains("L100.00 0.00"), "peak touches the top: {d}");
        assert!(d.ends_with('Z'), "path must close: {d}");
    }

    /// A one-point series is a real case on a 24h window; it must centre rather
    /// than divide by zero.
    #[test]
    fn a_single_point_area_centres_without_dividing_by_zero() {
        let pts = vec![Point {
            day: "2026-08-01".into(),
            value: 4.0,
        }];
        let d = area_path(&pts, 100.0, 50.0, 10.0).unwrap();
        assert!(d.starts_with("M50.00"), "single point centres: {d}");
    }

    #[test]
    fn an_empty_series_has_no_path() {
        assert!(area_path(&[], 100.0, 50.0, 10.0).is_none());
        assert!(line_path(&[], 100.0, 50.0, 10.0).is_none());
    }

    /// "Spent a fraction of a cent" and "spent nothing" are different facts and
    /// must not both render as $0.00.
    #[test]
    fn sub_cent_spend_does_not_collapse_to_zero() {
        assert_eq!(format_usd(0.0), "$0");
        assert_eq!(format_usd(0.0042), "$0.0042");
        assert_eq!(format_usd(4.2), "$4.20");
        assert_eq!(format_usd(1234.5), "$1235");
    }

    #[test]
    fn counts_get_thousands_separators() {
        assert_eq!(format_count(0.0), "0");
        assert_eq!(format_count(42.0), "42");
        assert_eq!(format_count(1234.0), "1,234");
        assert_eq!(format_count(1234567.0), "1,234,567");
    }

    #[test]
    fn tick_labels_drop_the_year() {
        assert_eq!(tick_label("2026-08-01"), "08-01");
        assert_eq!(tick_label("short"), "short");
    }

    #[test]
    fn outcome_measures_use_distinct_status_tones() {
        let tones: Vec<&str> = [
            Measure::Done,
            Measure::Killed,
            Measure::Failed,
            Measure::Crashed,
        ]
        .iter()
        .map(|m| m.tone())
        .collect();
        let mut sorted = tones.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            tones.len(),
            "each outcome facet needs its own hue: {tones:?}"
        );
    }

    #[test]
    fn windows_offer_the_three_documented_ranges() {
        let labels: Vec<&str> = Window::all().iter().map(|w| w.label()).collect();
        assert_eq!(labels, vec!["24h", "7d", "30d"]);
        assert_eq!(Window::Month.days(), 30);
    }
}
