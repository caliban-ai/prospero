//! Workspace sources: the 1..N git checkouts a workspace root holds. Mirrors
//! caliban's `caliban-supervisor::sources` so both sides agree on source
//! identity (name = directory basename). See caliban #281 / ADR 0052.

use std::path::Path;

// The `Source` struct now lives in `prospero-types` (shared with the WASM
// dashboard, prospero #98); re-exported here. `discover_sources` (filesystem
// logic) stays in `prospero-core`.
pub use prospero_types::Source;

/// Is `p` a git checkout (has a `.git` entry)?
fn is_checkout(p: &Path) -> bool {
    p.join(".git").exists()
}

/// Enumerate the sources under `workspace_root`, matching caliban's rule:
/// if the root is itself a checkout it is the single source; otherwise each
/// immediate child directory that is a checkout is a source. Sorted by name.
#[must_use]
pub fn discover_sources(workspace_root: &Path) -> Vec<Source> {
    let mut out = Vec::new();
    if is_checkout(workspace_root)
        && let Some(name) = workspace_root.file_name().and_then(|n| n.to_str())
    {
        out.push(Source {
            name: name.to_string(),
            path: workspace_root.to_path_buf(),
        });
        return out;
    }
    if let Ok(entries) = std::fs::read_dir(workspace_root) {
        for e in entries.flatten() {
            let path = e.path();
            if path.is_dir()
                && is_checkout(&path)
                && let Some(name) = path.file_name().and_then(|n| n.to_str())
            {
                out.push(Source {
                    name: name.to_string(),
                    path,
                });
            }
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_checkout(dir: &Path) {
        std::fs::create_dir_all(dir.join(".git")).unwrap();
    }

    #[test]
    fn root_is_the_single_source_when_a_checkout() {
        let d = tempfile::tempdir().unwrap();
        mk_checkout(d.path());
        let s = discover_sources(d.path());
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].path, d.path());
    }

    #[test]
    fn immediate_child_checkouts_are_sources_sorted() {
        let d = tempfile::tempdir().unwrap();
        mk_checkout(&d.path().join("beta"));
        mk_checkout(&d.path().join("alpha"));
        std::fs::create_dir_all(d.path().join("not-a-repo")).unwrap();
        let s = discover_sources(d.path());
        assert_eq!(
            s.iter().map(|x| x.name.as_str()).collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
    }

    #[test]
    fn empty_when_no_checkouts() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("plain")).unwrap();
        assert!(discover_sources(d.path()).is_empty());
    }

    #[test]
    fn missing_root_is_empty_not_panic() {
        assert!(discover_sources(Path::new("/no/such/path/here")).is_empty());
    }
}
