//! The published OpenAPI document must describe the API this server serves.
//!
//! A spec is only worth publishing if it cannot quietly fall behind the router.
//! Two hand-written lists compared against each other would never fail, so the
//! coverage tests here derive one side from the server itself: they read
//! `lib.rs`, which *is* the route table, and extract both the paths it registers
//! and the verbs it answers on each. Adding a route — or a verb to an existing
//! route — therefore fails the build until the spec describes it.
//!
//! This is deliberately literal rather than clever. axum's `Router` cannot be
//! enumerated at runtime, and the alternative (probing a live router for 405s)
//! needs a whole fleet harness to prove the same thing about the same file.

use std::collections::{BTreeMap, BTreeSet};

/// Surfaces the spec deliberately leaves out, each for a reason that is not
/// "nobody got round to it".
const NOT_DOCUMENTED: &[&str] = &[
    // The dashboard document and its bundle: a UI, not an API.
    "/",
    "/assets/{*path}",
    // #218: MCP speaks its own protocol and publishes its own tool schemas
    // through it. Describing it as REST would misrepresent it.
    "/mcp",
];

/// HTTP verbs axum's method routers expose, as they appear in source.
const VERBS: &[&str] = &["get", "post", "put", "delete", "patch", "head", "options"];

/// Every path `router_with_auth` registers and the verbs it serves on each,
/// scraped from the router's own source.
fn registered() -> BTreeMap<String, BTreeSet<String>> {
    let src = include_str!("../src/lib.rs");
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
        "scraped only {} routes from lib.rs — the scraper is broken, not the spec",
        out.len()
    );
    let verbs: usize = out.values().map(BTreeSet::len).sum();
    assert!(
        verbs >= 30,
        "scraped only {verbs} verbs from lib.rs — the scraper is broken, not the spec"
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

/// The paths the document describes, and the verbs documented on each.
fn documented(doc: &serde_json::Value) -> BTreeMap<String, BTreeSet<String>> {
    doc["paths"]
        .as_object()
        .expect("paths must be an object")
        .iter()
        .map(|(path, item)| {
            let verbs = item
                .as_object()
                .expect("a path item must be an object")
                .keys()
                // `parameters` sits beside the verbs and is not one.
                .filter(|k| VERBS.contains(&k.as_str()))
                .cloned()
                .collect();
            (path.clone(), verbs)
        })
        .collect()
}

#[test]
fn the_document_declares_openapi_3_1_and_identifies_the_server() {
    let doc = prospero_api::openapi::document();

    assert_eq!(doc["openapi"], "3.1.0");
    assert_eq!(doc["info"]["title"], "prosperod REST API");
    assert!(
        doc["info"]["version"].is_string(),
        "info.version must be set, got {:?}",
        doc["info"]["version"]
    );
}

#[test]
fn every_route_the_router_registers_is_documented() {
    let doc = prospero_api::openapi::document();
    let documented = documented(&doc);

    let undocumented: Vec<String> = registered()
        .into_keys()
        .filter(|p| !NOT_DOCUMENTED.contains(&p.as_str()))
        .filter(|p| !documented.contains_key(p))
        .collect();

    assert!(
        undocumented.is_empty(),
        "these routes are served but absent from the OpenAPI document: {undocumented:#?}\n\
         Add them to `openapi::ROUTES`, or to this test's NOT_DOCUMENTED list with a reason."
    );
}

#[test]
fn every_method_the_router_serves_is_documented() {
    let doc = prospero_api::openapi::document();
    let documented = documented(&doc);

    let mut missing = Vec::new();
    for (path, served) in registered() {
        if NOT_DOCUMENTED.contains(&path.as_str()) {
            continue;
        }
        let Some(described) = documented.get(&path) else {
            continue; // the path-level test owns this failure
        };
        for verb in served.difference(described) {
            missing.push(format!("{} {path}", verb.to_uppercase()));
        }
    }

    assert!(
        missing.is_empty(),
        "these methods are served but absent from the OpenAPI document: {missing:#?}"
    );
}

#[test]
fn the_document_describes_nothing_the_router_does_not_serve() {
    let doc = prospero_api::openapi::document();
    let registered = registered();

    let mut phantom = Vec::new();
    for (path, described) in documented(&doc) {
        let Some(served) = registered.get(&path) else {
            phantom.push(path);
            continue;
        };
        for verb in described.difference(served) {
            phantom.push(format!("{} {path}", verb.to_uppercase()));
        }
    }

    assert!(
        phantom.is_empty(),
        "the OpenAPI document describes routes or methods that are not served: {phantom:#?}"
    );
}

#[test]
fn every_schema_reference_resolves() {
    let doc = prospero_api::openapi::document();
    let schemas = doc["components"]["schemas"]
        .as_object()
        .expect("components.schemas must be an object");

    let mut refs = Vec::new();
    collect_refs(&doc, &mut refs);
    assert!(!refs.is_empty(), "the document should reference schemas");

    let dangling: Vec<&String> = refs
        .iter()
        .filter(|r| {
            r.strip_prefix("#/components/schemas/")
                .is_none_or(|name| !schemas.contains_key(name))
        })
        .collect();

    assert!(
        dangling.is_empty(),
        "these $refs do not resolve to a component schema: {dangling:#?}"
    );
}

fn collect_refs(value: &serde_json::Value, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map {
                if key == "$ref" {
                    if let Some(s) = val.as_str() {
                        out.push(s.to_string());
                    }
                } else {
                    collect_refs(val, out);
                }
            }
        }
        serde_json::Value::Array(items) => items.iter().for_each(|v| collect_refs(v, out)),
        _ => {}
    }
}

#[test]
fn every_operation_records_the_scope_it_needs() {
    let doc = prospero_api::openapi::document();

    for (path, item) in doc["paths"].as_object().unwrap() {
        for (verb, op) in item.as_object().unwrap() {
            if !VERBS.contains(&verb.as_str()) {
                continue;
            }
            assert!(
                op["x-prospero-scope"].is_string(),
                "{} {path} does not say which scope it needs",
                verb.to_uppercase()
            );
            assert!(
                op["operationId"].is_string(),
                "{} {path} has no operationId, so clients cannot name it",
                verb.to_uppercase()
            );
        }
    }
}
