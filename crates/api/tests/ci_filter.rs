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
