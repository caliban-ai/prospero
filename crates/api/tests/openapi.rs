//! The published OpenAPI document must describe the API this server serves.
//!
//! A spec is only worth publishing if it cannot quietly fall behind the router.
//! The coverage tests here take one side from the server itself — the paths and
//! verbs `lib.rs` registers (see `common`) — so adding a route, or a verb to an
//! existing route, fails the build until the spec describes it.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{VERBS, registered};

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
