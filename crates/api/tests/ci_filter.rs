//! CI must not skip the cargo gate on a docs edit that is a test input (#264).
//!
//! `ci.yml`'s `changes` step skips every cargo step when a diff touches only
//! `docs/`, `*.md` or `LICENSE`. But `guide.rs` `include_str!`s
//! `docs/guide/src/api.md`, so a docs-only edit to the routes table is the single
//! change most likely to fail the build — and the one CI would not run for. The
//! route-coverage test #256 added to stop the guide drifting was defeated on
//! exactly the pull requests that drift it.
//!
//! The workflow therefore declares the docs that are compiled into tests, and
//! this test proves that declaration is complete: add another `include_str!` of a
//! doc and the build fails until `TEST_INPUT_DOCS` lists it. That ordering
//! matters — a list nothing checks is a list that goes stale.
//!
//! The same invariant applies to `docs.yml` and is checked here too (#270): the
//! guide ingests files from outside `docs/guide/`, so every path a `sync-*.sh`
//! script reads must appear in the workflow's trigger paths, or editing that file
//! publishes nothing. `CHANGELOG.md` was ingested but untriggered, so changelog
//! entries reached GitHub and not the site.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/api.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root should resolve")
}

/// Every `.rs` file under `dir`, skipping generated and nested-checkout trees.
fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        // `target/` holds generated sources; `.claude/` holds worktrees, which
        // are nested checkouts of this same repo and would double-count hits.
        if name == "target" || name == ".claude" || name == ".git" {
            continue;
        }
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Collapse `a/b/../c` to `a/c` so the path compares cleanly against a
/// `git diff --name-only` line.
///
/// `Path::canonicalize` would also resolve symlinks and require the target to
/// exist; the point here is only to express the include target the way `ci.yml`
/// and git do.
fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for part in path.components() {
        match part {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other),
        }
    }
    out
}

/// Docs pulled into a compiled test or binary via `include_str!`, as
/// repo-relative paths. These are source as far as CI is concerned, even though
/// they live under `docs/` and end in `.md`.
fn doc_test_inputs() -> BTreeSet<String> {
    let root = repo_root();
    let mut sources = Vec::new();
    rust_sources(&root.join("crates"), &mut sources);

    let mut out = BTreeSet::new();
    for file in sources {
        let text = fs::read_to_string(&file).unwrap_or_default();
        for (at, _) in text.match_indices("include_str!") {
            // Take the first string literal after the macro name.
            let rest = &text[at..];
            let Some(open) = rest.find('"') else { continue };
            let Some(len) = rest[open + 1..].find('"') else {
                continue;
            };
            let target = &rest[open + 1..open + 1 + len];

            let resolved = normalize(&file.parent().expect("file has a parent").join(target));
            let Ok(rel) = resolved.strip_prefix(&root) else {
                continue;
            };
            let rel = rel.to_string_lossy().replace('\\', "/");

            // Only the ones ci.yml's docs filter would swallow.
            if rel.starts_with("docs/") || rel.ends_with(".md") || rel == "LICENSE" {
                out.insert(rel);
            }
        }
    }
    out
}

/// The paths listed in `ci.yml`'s `TEST_INPUT_DOCS`.
fn declared_in_ci() -> BTreeSet<String> {
    let ci = fs::read_to_string(repo_root().join(".github/workflows/ci.yml"))
        .expect("ci.yml should exist");
    let line = ci
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with("TEST_INPUT_DOCS="))
        .unwrap_or_else(|| {
            panic!(
                "ci.yml should declare TEST_INPUT_DOCS so a docs edit that is a \
                 test input still runs the cargo gate (#264)"
            )
        });

    line.trim_start_matches("TEST_INPUT_DOCS=")
        .trim_matches('\'')
        .trim_matches('"')
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

#[test]
fn ci_treats_every_doc_test_input_as_code() {
    let found = doc_test_inputs();
    assert!(
        !found.is_empty(),
        "expected to find at least docs/guide/src/api.md as a test input; the \
         include_str! scan is probably broken"
    );

    let declared = declared_in_ci();
    let missing: Vec<_> = found.difference(&declared).collect();
    assert!(
        missing.is_empty(),
        "these docs are compiled into tests but ci.yml's TEST_INPUT_DOCS does not \
         list them, so a PR editing only them would skip the cargo gate they can \
         break (#264): {missing:?}"
    );
}

#[test]
fn ci_declares_no_doc_that_is_not_a_test_input() {
    // The other direction: a stale entry makes CI run the full gate on a docs
    // edit for no reason, which is the cost the skip exists to avoid.
    let stale: Vec<_> = declared_in_ci()
        .difference(&doc_test_inputs())
        .cloned()
        .collect();
    assert!(
        stale.is_empty(),
        "ci.yml's TEST_INPUT_DOCS lists docs that no test includes any more; drop \
         them so docs-only PRs still skip the gate: {stale:?}"
    );
}

/// The repo-relative paths the guide's `sync-*.sh` scripts read.
///
/// Each script assigns its source to a shell variable, e.g. `SRC="CHANGELOG.md"`
/// and `ADR_SRC="docs/adr"`. Rather than hard-code them — which is the staleness
/// this test exists to prevent — take every quoted assignment whose value names a
/// path that exists, and drop the ones under `docs/guide/src`, which are the
/// destinations the scripts write to.
fn guide_ingest_sources() -> BTreeSet<String> {
    let root = repo_root();
    let mut out = BTreeSet::new();

    let Ok(entries) = fs::read_dir(root.join("docs/guide")) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "sh") {
            let text = fs::read_to_string(&path).unwrap_or_default();
            for line in text.lines() {
                let line = line.trim();
                if line.starts_with('#') {
                    continue;
                }
                // `VAR="value"` — take the value.
                let Some((lhs, rest)) = line.split_once("=\"") else {
                    continue;
                };
                if lhs.contains(' ') || lhs.is_empty() {
                    continue;
                }
                let Some((value, _)) = rest.split_once('"') else {
                    continue;
                };

                // A source is something that exists and is not a destination.
                // An empty value must be rejected explicitly: `sync-adrs.sh` has
                // `entries=""` as an accumulator, and `root.join("")` is the repo
                // root, which of course exists.
                if value.is_empty()
                    || value.starts_with("docs/guide/src")
                    || !root.join(value).exists()
                {
                    continue;
                }
                out.insert(value.to_owned());
            }
        }
    }
    out
}

/// The trigger paths of `docs.yml`, as `(trigger name, paths)`.
///
/// Parsed textually, like `declared_in_ci` above: the workflow is the artefact
/// under test, so reading it as text keeps the test honest about what is written
/// there rather than what a parser infers.
fn docs_trigger_paths() -> Vec<(String, Vec<String>)> {
    let text = fs::read_to_string(repo_root().join(".github/workflows/docs.yml"))
        .expect("docs.yml should exist");

    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    let mut trigger = String::new();
    let mut collecting = false;

    for line in text.lines() {
        let indent = line.len() - line.trim_start().len();
        let trimmed = line.trim();

        // `  push:` / `  pull_request:` sit at one indent inside `on:`.
        if indent == 2 && trimmed.ends_with(':') && !trimmed.starts_with('-') {
            trigger = trimmed.trim_end_matches(':').to_owned();
            collecting = false;
            continue;
        }
        if trimmed == "paths:" {
            collecting = true;
            out.push((trigger.clone(), Vec::new()));
            continue;
        }
        if collecting {
            if let Some(item) = trimmed.strip_prefix("- ") {
                let item = item.trim().trim_matches('"').trim_matches('\'');
                if let Some((_, paths)) = out.last_mut() {
                    paths.push(item.to_owned());
                }
            } else {
                collecting = false;
            }
        }
    }
    out
}

/// Would a change to `source` match `pattern` as GitHub evaluates a paths filter?
///
/// Only the two forms the workflow actually uses are handled — an exact path and a
/// `dir/**` prefix — so an unrecognised pattern reads as "no match" rather than
/// being waved through.
fn pattern_covers(pattern: &str, source: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix("/**") {
        // A directory source is covered when the glob is rooted at or above it.
        return source == prefix || source.starts_with(&format!("{prefix}/"));
    }
    pattern == source
}

#[test]
fn docs_workflow_triggers_on_everything_the_guide_ingests() {
    let sources = guide_ingest_sources();
    assert!(
        sources.contains("CHANGELOG.md") && sources.contains("docs/adr"),
        "expected to discover CHANGELOG.md and docs/adr as ingest sources; the \
         sync-script scan is probably broken, found: {sources:?}"
    );

    let triggers = docs_trigger_paths();
    assert!(
        triggers.len() >= 2,
        "expected docs.yml to filter paths on both push and pull_request, parsed: \
         {triggers:?}"
    );

    let mut gaps = Vec::new();
    for (trigger, paths) in &triggers {
        for source in &sources {
            if !paths.iter().any(|p| pattern_covers(p, source)) {
                gaps.push(format!("{trigger} does not trigger on {source}"));
            }
        }
    }
    assert!(
        gaps.is_empty(),
        "the guide ingests these paths but docs.yml is not triggered by them, so \
         editing one publishes nothing (#270): {gaps:?}"
    );
}
