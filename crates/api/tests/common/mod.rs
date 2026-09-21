//! The router's own route table, read from its source.
//!
//! Shared by every test that checks a published description of the API against
//! what the server actually serves: the OpenAPI document (#222) and the guide's
//! routes table (#256). Comparing two hand-written lists would never fail, so
//! one side always comes from here. `lib.rs` *is* the route table, and reading
//! it is reading the source of truth.
//!
//! This is deliberately literal rather than clever. axum's `Router` cannot be
//! enumerated at runtime, and the alternative (probing a live router for 405s)
//! needs a whole fleet harness to prove the same thing about the same file.

use std::collections::{BTreeMap, BTreeSet};

/// HTTP verbs axum's method routers expose, as they appear in source.
pub const VERBS: &[&str] = &["get", "post", "put", "delete", "patch", "head", "options"];

/// Every path `router_with_auth` registers and the verbs it serves on each,
/// scraped from the router's own source.
///
/// A path mounted with `nest_service` comes back with **no** verbs: the mounted
/// service answers its own methods, so the router source does not declare them.
pub fn registered() -> BTreeMap<String, BTreeSet<String>> {
    let src = include_str!("../../src/lib.rs");
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for call in [".route(", ".nest_service("] {
        let mut rest = src;
        while let Some(at) = rest.find(call) {
            let after = &rest[at + call.len()..];
            let span = balanced_span(after);
            let path = first_literal(span);
            out.entry(path).or_default().extend(verbs_in(span));
            rest = after;
        }
    }

    // If the scraper ever stops matching, it must fail loudly rather than
    // silently reporting that everything is covered.
    assert!(
        out.len() >= 25,
        "scraped only {} routes from lib.rs — the scraper is broken, not the description",
        out.len()
    );
    let verbs: usize = out.values().map(BTreeSet::len).sum();
    assert!(
        verbs >= 30,
        "scraped only {verbs} verbs from lib.rs — the scraper is broken, not the description"
    );
    out
}

/// The text of a call's arguments, from just after its `(` to the matching `)`.
fn balanced_span(after_open: &str) -> &str {
    let mut depth = 1usize;
    for (i, ch) in after_open.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return &after_open[..i];
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced route call");
}

/// The first string literal in a call's arguments — the path template.
fn first_literal(span: &str) -> String {
    let open = span.find('"').expect("route call without a path literal");
    let rest = &span[open + 1..];
    let end = rest.find('"').expect("unterminated route literal");
    rest[..end].to_string()
}

/// The method-router verbs invoked within a route call.
fn verbs_in(span: &str) -> BTreeSet<String> {
    let bytes = span.as_bytes();
    let mut found = BTreeSet::new();

    for verb in VERBS {
        let needle = format!("{verb}(");
        let mut from = 0;
        while let Some(at) = span[from..].find(&needle) {
            let start = from + at;
            // Reject a match inside a longer identifier, so the handler name in
            // `get(handlers::get_metrics)` is not read as a second verb.
            let preceded_by_ident = start > 0 && {
                let prev = bytes[start - 1];
                prev.is_ascii_alphanumeric() || prev == b'_'
            };
            if !preceded_by_ident {
                found.insert((*verb).to_string());
            }
            from = start + needle.len();
        }
    }
    found
}
