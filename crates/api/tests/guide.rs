//! The guide's routes table must list what the router serves (#256).
//!
//! Automations (#220) shipped seven routes and none of them reached
//! `docs/guide/src/api.md` — nothing compared the guide to the router, so it fell
//! behind without anyone noticing. These tests hold the guide to the same
//! standard #222 set for the OpenAPI document: the paths and verbs come from
//! `lib.rs` itself (see `common`), so a new route, or a new verb on an existing
//! one, fails the build until the table lists it.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{VERBS, registered};

const GUIDE: &str = include_str!("../../../docs/guide/src/api.md");

/// The `## Routes` table as path → the methods its rows list for it.
///
/// A row reads `| GET / POST | `/a`, `/b` | scope | purpose |`. A path may
/// carry an example query (`/api/fleet/stream?from=N\|now`), which is dropped,
/// and may appear on several rows (one per method), which are merged.
fn routes_table() -> BTreeMap<String, BTreeSet<String>> {
    let section = GUIDE
        .split_once("\n## Routes\n")
        .expect("api.md should have a `## Routes` section")
        .1;
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    let rows = section
        .lines()
        .skip_while(|l| !l.starts_with('|'))
        .take_while(|l| l.starts_with('|'))
        // The header row and the `|---|` separator.
        .skip(2);

    for row in rows {
        // `\|` is a literal pipe inside a cell, not a column break.
        let row = row.replace("\\|", "\u{0}");
        let cells: Vec<&str> = row.split('|').map(str::trim).collect();
        let (methods, paths) = (cells[1], cells[2]);

        let methods: BTreeSet<String> = methods
            .split('/')
            .map(|m| m.trim().to_lowercase())
            .filter(|m| !m.is_empty())
            .collect();
        for m in &methods {
            assert!(
                VERBS.contains(&m.as_str()),
                "routes table row lists {m:?}, which is not an HTTP method: {row}"
            );
        }

        for path in paths.split('`').skip(1).step_by(2) {
            let path = path.split('?').next().unwrap_or(path).to_string();
            out.entry(path).or_default().extend(methods.iter().cloned());
        }
    }

    assert!(
        out.len() >= 20,
        "parsed only {} paths from the routes table — the parser is broken, not the guide",
        out.len()
    );
    out
}

#[test]
fn every_route_the_router_serves_is_in_the_routes_table() {
    let table = routes_table();
    let missing: Vec<String> = registered()
        .into_keys()
        .filter(|p| !table.contains_key(p))
        .collect();

    assert!(
        missing.is_empty(),
        "these routes are served but missing from the routes table in \
         docs/guide/src/api.md: {missing:#?}"
    );
}

#[test]
fn every_method_the_router_serves_is_in_the_routes_table() {
    let table = routes_table();
    let mut missing = Vec::new();

    for (path, served) in registered() {
        let Some(listed) = table.get(&path) else {
            continue; // the path-level test owns this failure
        };
        for verb in served.difference(listed) {
            missing.push(format!("{} {path}", verb.to_uppercase()));
        }
    }

    assert!(
        missing.is_empty(),
        "these methods are served but missing from the routes table: {missing:#?}"
    );
}

#[test]
fn the_routes_table_lists_nothing_the_router_does_not_serve() {
    let registered = registered();
    let mut phantom = Vec::new();

    for (path, listed) in routes_table() {
        let Some(served) = registered.get(&path) else {
            phantom.push(path);
            continue;
        };
        // A mounted service (`nest_service`, e.g. MCP) answers its own methods,
        // so the router source declares none to compare against.
        if served.is_empty() {
            continue;
        }
        for verb in listed.difference(served) {
            phantom.push(format!("{} {path}", verb.to_uppercase()));
        }
    }

    assert!(
        phantom.is_empty(),
        "the routes table lists routes or methods that are not served: {phantom:#?}"
    );
}
