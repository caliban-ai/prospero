# Workspace-Scoped Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make prospero's fleet unit a first-class **Workspace** (a root + 1..N source checkouts) keyed on the workspace root, matching caliban #281, renaming `Repo`→`Workspace` across core/api/cli/dashboard.

**Architecture:** Add `Source`/`discover_sources` (mirrors caliban `sources.rs`), rename the core `Repo`/`RegisteredRepo` model to `Workspace` carrying its `sources`, key discovery on the workspace root (`hash16` unchanged — already equals caliban's `workspace_hash`), and drive one caliband per workspace. The API/CLI/dashboard surface renames `repos`→`workspaces`. Back-compat: legacy on-disk registries (`{"repos":[...]}`) load as single-source workspaces; the internal event stream-key prefix `repo:` is **kept** to avoid orphaning persisted history.

**Tech Stack:** Rust (edition 2024), tokio, serde/serde_json, axum, clap; static JS dashboard.

## Global Constraints

- **`hash16` does not change.** Workspace identity = `hex(sha256(workspace_root.to_string_lossy()))[..16]`, byte-identical to caliban `runtime.rs::workspace_hash`. A single-source workspace root hashes exactly as the old per-repo root (back-compat).
- **`discover_sources` mirrors caliban's rule** (`caliban/crates/caliban-supervisor/src/sources.rs`): immediate-child directories that are git checkouts are sources; if the workspace root is itself a checkout it is the single source; names are directory basenames; deterministic (sorted) order.
- **Rename map (apply consistently):** `RegisteredRepo`→`Workspace` (registry), `model::Repo`→`Workspace`, `RepoHealth`→`WorkspaceHealth`, `Registry.repos`→`Registry.workspaces`, `FleetSnapshot.repos`→`FleetSnapshot.workspaces`, `Agent.repo`→`Agent.workspace`, `CoreError::RepoNotFound`→`WorkspaceNotFound`, `FleetManager::add_repo`→`add_workspace` (+ kept `add_repo` shim), `add_repo_with_config`→`add_workspace_with_config`, `remove_repo`→`remove_workspace`, `set_repo_config`→`set_workspace_config`. API: `RepoSummary`→`WorkspaceSummary`, `AddRepoBody`→`AddWorkspaceBody`, `SpawnedResponse.repo`→`workspace`, routes `/api/repos*`→`/api/workspaces*`. CLI: `Repo`(cmd)→`Workspace`, `repo`→`workspace`. Dashboard: `repos`→`workspaces`, `/api/repos`→`/api/workspaces`.
- **Do NOT rename** `event.rs::stream_key_for`'s `repo:` prefix or `FleetEvent.repo` (persistence-compat; documented). It semantically holds the workspace name.
- **Verification gate (CI mirror):** from repo root — `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings`; `cargo build --workspace --all-targets`; `cargo test --workspace --features prospero-core/testkit`.
- **Every commit subject ends with `(#72)`.**

---

### Task 1: `Source` + `discover_sources`

**Files:**
- Create: `crates/core/src/caliband/sources.rs`
- Modify: `crates/core/src/caliband/mod.rs` (add `pub mod sources;`)

**Interfaces:**
- Produces: `pub struct Source { pub name: String, pub path: PathBuf }`; `pub fn discover_sources(workspace_root: &Path) -> Vec<Source>`.

- [ ] **Step 1: Write failing tests.** Create `crates/core/src/caliband/sources.rs`:

```rust
//! Workspace sources: the 1..N git checkouts a workspace root holds. Mirrors
//! caliban's `caliban-supervisor::sources` so both sides agree on source
//! identity (name = directory basename). See caliban #281 / ADR 0052.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// One source checkout within a workspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Source {
    /// Directory basename (unique within a workspace).
    pub name: String,
    /// Absolute path to the source checkout.
    pub path: PathBuf,
}

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
```

- [ ] **Step 2: Register module.** In `crates/core/src/caliband/mod.rs` add `pub mod sources;` (beside `pub mod stream;`).

- [ ] **Step 3: Re-export.** In `crates/core/src/lib.rs`, add `Source` to the public re-exports next to the existing caliband/model exports (grep `pub use` there; follow the pattern, e.g. `pub use caliband::sources::{discover_sources, Source};`).

- [ ] **Step 4: Run tests.** `cargo test -p prospero-core --lib caliband::sources` → PASS (4 tests).

- [ ] **Step 5: Commit.**
```bash
git add crates/core/src/caliband/sources.rs crates/core/src/caliband/mod.rs crates/core/src/lib.rs
git commit -m "feat(core): add Source + discover_sources (mirrors caliban sources) (#72)"
```

---

### Task 2: Core rename to `Workspace` + sources + back-compat (atomic)

One task: Rust type-checks the whole crate, so the `Repo`→`Workspace` rename + `sources` field ripples must land together. Behavior is preserved; single-source workspaces are byte-identical to today.

**Files:**
- Modify: `crates/core/src/registry.rs`, `crates/core/src/model.rs`, `crates/core/src/discovery.rs`, `crates/core/src/fleet.rs`, `crates/core/src/error.rs`, `crates/core/src/lib.rs`

**Interfaces:**
- Consumes: `Source`, `discover_sources` (Task 1).
- Produces:
  - `registry::Workspace { name: String, root: PathBuf, sources: Vec<Source>, config: RepoProviderConfig }`; `Registry { workspaces: Vec<Workspace> }` with `get/add/remove/set_config` (same signatures, `add` now runs `discover_sources`); legacy `{"repos":[...]}` load.
  - `model::Workspace { name, root, sources: Vec<Source>, health: WorkspaceHealth, config, agents: Vec<Agent> }`; `model::WorkspaceHealth` (was `RepoHealth`); `FleetSnapshot { host, workspaces: Vec<Workspace> }`; `Agent { workspace: String, .. }`.
  - `FleetManager::add_workspace(name, root)`, `add_workspace_with_config(name, root, config)`, `remove_workspace(name)`, `set_workspace_config(name, cfg)`, and a kept `add_repo`/`add_repo_with_config` shim delegating to the workspace methods (single-source back-compat).
  - `CoreError::WorkspaceNotFound(String)`.

- [ ] **Step 1: Registry — write the back-compat + sources tests first.** In `crates/core/src/registry.rs` tests, add:

```rust
#[test]
fn add_discovers_sources_for_a_workspace() {
    let d = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(d.path().join("a/.git")).unwrap();
    std::fs::create_dir_all(d.path().join("b/.git")).unwrap();
    let mut reg = Registry::default();
    reg.add("ws", d.path()).unwrap();
    let ws = reg.get("ws").unwrap();
    assert_eq!(ws.sources.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(), vec!["a", "b"]);
}

#[test]
fn legacy_repos_json_loads_as_single_source_workspaces() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("registry.json");
    // Old on-disk shape used the "repos" key and no "sources".
    std::fs::write(&path, r#"{"repos":[{"name":"p","root":"/r"}]}"#).unwrap();
    let reg = Registry::load(&path).unwrap();
    let ws = reg.get("p").expect("legacy entry migrates");
    assert_eq!(ws.root, std::path::PathBuf::from("/r"));
    assert_eq!(ws.config, RepoProviderConfig::default());
}
```

- [ ] **Step 2: Registry — implement.** Rewrite `registry.rs`:
  - Rename `RegisteredRepo`→`Workspace`, add `#[serde(default)] pub sources: Vec<Source>` after `root`.
  - `Registry { #[serde(alias = "repos")] pub workspaces: Vec<Workspace> }` — the `alias = "repos"` makes legacy `{"repos":[...]}` deserialize into `workspaces` (serde reads either key). Legacy entries lack `sources` → `#[serde(default)]` gives `[]`.
  - `add(name, root)`: after the existing duplicate-name/root guards, set `sources: crate::caliband::sources::discover_sources(&root)` on the pushed `Workspace`. (For a not-yet-existing root, `discover_sources` returns `[]` — fine.)
  - `get/remove/set_config`: same bodies, `self.repos`→`self.workspaces`, `RegisteredRepo`→`Workspace`.
  - Update the existing registry tests' field refs (`reg.repos`→`reg.workspaces`).
  - `use crate::caliband::sources::{discover_sources, Source};`

- [ ] **Step 3: model.rs — rename + sources.** In `crates/core/src/model.rs`:
  - `RepoHealth`→`WorkspaceHealth` (rename the enum + all refs).
  - `Repo`→`Workspace`; add `#[serde(default)] pub sources: Vec<crate::caliband::sources::Source>` after `root`.
  - `Agent.repo`→`Agent.workspace` (field + doc).
  - `FleetSnapshot.repos`→`workspaces`; update `find_agent` body (`self.repos`→`self.workspaces`, `r.agents`... unchanged).

- [ ] **Step 4: discovery.rs — workspace vocabulary.** Rename `repo_root`→`workspace_root` in `control_socket_path`, `canonical_root`, `resolve_socket`, `ensure_caliband` (signatures + bodies + docs). `hash16` unchanged. In `ensure_caliband`, change the spawn arg `.arg("--repo-root")` → `.arg("--workspace-root")`. Update the module doc comment to say "workspace root" and cite caliban #281 / ADR 0052. Update the discovery tests' local names (mechanical).

- [ ] **Step 5: error.rs — rename.** `CoreError::RepoNotFound`→`WorkspaceNotFound` (variant + its `#[error("...")]` message `"workspace not found: {0}"`); update the `From<SupervisorError>` mapping (`NotFound` still maps to `AgentNotFound`; the repo-not-found sites are in fleet/api). Update the one error.rs test referencing it, if any.

- [ ] **Step 6: fleet.rs — rename + add_workspace + snapshot sources.** Apply the rename map across `fleet.rs` (`Repo`→`Workspace`, `RepoHealth`→`WorkspaceHealth`, `.repos`→`.workspaces`, `RepoNotFound`→`WorkspaceNotFound`). Then:
  - Both snapshot-build sites (the `FleetManager::new` seed ~line 433 and the `add_*` push ~line 573) construct `Workspace { name, root, sources: <from registry entry>, health: WorkspaceHealth::Healthy, config, agents: Vec::new() }`. Seed site: map from `registry.workspaces` carrying each entry's `sources`. Push site: use the just-registered workspace's `sources`.
  - Rename `add_repo`/`add_repo_with_config`→`add_workspace`/`add_workspace_with_config`; canonicalize root (existing logic), and the registry `add` now discovers sources. Keep thin shims:
    ```rust
    /// Back-compat: register a single-source workspace at `root`.
    pub async fn add_repo(&self, name: impl Into<String>, root: impl Into<PathBuf>) -> Result<()> {
        self.add_workspace(name, root).await
    }
    pub async fn add_repo_with_config(&self, name: impl Into<String>, root: impl Into<PathBuf>, config: crate::registry::RepoProviderConfig) -> Result<()> {
        self.add_workspace_with_config(name, root, config).await
    }
    ```
  - Rename `remove_repo`→`remove_workspace`, `set_repo_config`→`set_workspace_config` (+ keep `remove_repo`/`set_repo_config` shims if the API layer still calls them — simpler to rename the API calls in Task 3 and drop shims; keep only `add_repo` shims for existing tests).
  - `Agent { workspace: .. }` at every construction site (the reconcile that projects `AgentRecord`→`Agent`): set `workspace: <owning workspace name>` (was `repo`).
  - The `config_store.upsert_repo`/`RepoProviderConfig` store calls: keep method names as-is (store layer not renamed) — only the in-memory model renames. (Note in a comment.)

- [ ] **Step 7: lib.rs re-exports.** Update `crates/core/src/lib.rs` `pub use` lines: `Repo`→`Workspace`, `RepoHealth`→`WorkspaceHealth` (keep `Source`/`discover_sources` from Task 1).

- [ ] **Step 8: Build the whole crate, fix rename misses.**
Run: `cargo build -p prospero-core --all-features --all-targets 2>&1 | rg "error\[|error:" | head`
Expected: no errors (Rust flags every missed rename; fix mechanically).

- [ ] **Step 9: Run core tests.**
Run: `cargo test -p prospero-core --features testkit 2>&1 | tail -15`
Expected: PASS (behavior unchanged; new registry tests green). Update any test still using old field/variant names.

- [ ] **Step 10: Commit.**
```bash
git add crates/core/src
git commit -m "refactor(core): Repo -> first-class Workspace with sources + back-compat (#72)"
```

---

### Task 3: API layer — routes + DTO sources

**Files:** Modify `crates/api/src/dto.rs`, `crates/api/src/handlers.rs`, `crates/api/src/lib.rs`, `crates/api/src/error.rs`.

**Interfaces:**
- Produces: routes `/api/workspaces`, `/api/workspaces/{name}`, `/api/workspaces/{name}/config`, `/api/workspaces/{workspace}/agents`; `WorkspaceSummary { name, root, sources: Vec<Source>, health, agent_count, config }`; `AddWorkspaceBody`; `SpawnedResponse.workspace`.

- [ ] **Step 1: DTO — write the failing test.** In `crates/api/src/dto.rs` tests (add a `#[cfg(test)]` mod if none), assert a `WorkspaceSummary` serializes with a `sources` array:
```rust
#[test]
fn workspace_summary_exposes_sources() {
    let s = WorkspaceSummary {
        name: "ws".into(), root: "/ws".into(),
        sources: vec![prospero_core::Source { name: "a".into(), path: "/ws/a".into() }],
        health: prospero_core::WorkspaceHealth::Healthy, agent_count: 0,
        config: Default::default(),
    };
    let j = serde_json::to_value(&s).unwrap();
    assert_eq!(j["sources"][0]["name"], "a");
}
```

- [ ] **Step 2: DTO — implement.** In `dto.rs`: rename `RepoSummary`→`WorkspaceSummary`, add `pub sources: Vec<prospero_core::Source>`; rename `AddRepoBody`→`AddWorkspaceBody`; `SpawnedResponse.repo`→`workspace`. Update `use` of `RepoProviderConfig`/`Source` (`prospero_core::Source`).

- [ ] **Step 3: handlers.rs — rename.** `get_repos`→`get_workspaces` (build `WorkspaceSummary` with `sources: w.sources.clone()`), `add_repo`→`add_workspace` (calls `st.manager.add_workspace_with_config`), `set_repo_config`→`set_workspace_config`, `delete_repo`→`delete_workspace` (calls `remove_workspace`, maps `WorkspaceNotFound`), `get_repo_agents`→`get_workspace_agents` (`snap.workspaces`), `spawn_agent` (`SpawnedResponse { workspace, .. }`). Path params `repo`→`workspace`.

- [ ] **Step 4: lib.rs — routes.** Rename the five `/api/repos*` routes to `/api/workspaces*` and their handler idents (per Step 3). Update the module doc comment.

- [ ] **Step 5: api/error.rs — map.** Wherever `CoreError::RepoNotFound` was matched/converted, use `WorkspaceNotFound` (grep `RepoNotFound` in `crates/api`).

- [ ] **Step 6: Build + test the api crate.**
Run: `cargo test -p prospero-api --features prospero-core/testkit 2>&1 | tail -12` → PASS.

- [ ] **Step 7: Commit.**
```bash
git add crates/api/src
git commit -m "refactor(api): /api/workspaces + WorkspaceSummary.sources (#72)"
```

---

### Task 4: CLI — workspace commands

**Files:** Modify `crates/cli/src/main.rs`.

- [ ] **Step 1: Rename the command surface.** `Command::Repo(RepoCmd)`→`Command::Workspace(WorkspaceCmd)` (clap: the subcommand becomes `prospero workspace ...`); `RepoCmd`→`WorkspaceCmd` (`Add`/`List`/`Config`/`Rm`); `RepoConfigArgs`→`WorkspaceConfigArgs`; `SpawnArgs.repo`→`SpawnArgs.workspace`. Update all `/api/repos*` client calls to `/api/workspaces*`. Update user-facing strings ("registered repo"→"registered workspace", etc.).

- [ ] **Step 2: `print_repos`→`print_workspaces` + show sources.** Rename the helper; when listing, print each workspace's source names, e.g. `  sources: a, b`. Read them from the `WorkspaceSummary.sources` JSON.

- [ ] **Step 3: Build + run.**
Run: `cargo build -p prospero-cli 2>&1 | tail -3` → success.
Run: `cargo run -p prospero-cli -- workspace --help 2>&1 | tail -8` → shows the `workspace` subcommands.

- [ ] **Step 4: Commit.**
```bash
git add crates/cli/src/main.rs
git commit -m "refactor(cli): workspace subcommands + source listing (#72)"
```

---

### Task 5: Dashboard — workspaces + sources

**Files:** Modify `crates/api/dashboard/app.js`, `crates/api/dashboard/index.html`.

- [ ] **Step 1: app.js — rename data + endpoints.** `lastFleet.repos`→`lastFleet.workspaces`, `fleet.repos`→`fleet.workspaces`, `healthyRepos`→`healthyWorkspaces`, every `/api/repos`→`/api/workspaces` (list, add, config, delete, spawn `/api/workspaces/{name}/agents`). Update user-facing labels ("add repo"→"add workspace", "Remove repo"→"Remove workspace", settings copy).

- [ ] **Step 2: app.js — render sources.** In the per-workspace render (the `for (const repo of fleet.workspaces)` loop), after the name, render its sources: `(w.sources || []).map(s => s.name).join(', ')` in a small `.sources` line.

- [ ] **Step 3: index.html — labels.** Rename the visible "add n/repo" button text and any `repo` copy to "workspace"; add a `.sources` CSS line style (small, muted) to match existing `.health` styling.

- [ ] **Step 4: Verify served.** `cargo build -p prospero-api 2>&1 | tail -2` (dashboard is embedded via `include_str!`/route — confirm it still compiles). Manually eyeball `app.js` for any missed `/api/repos`.
Run: `rg -n "/api/repos|\.repos\b|healthyRepos" crates/api/dashboard` → **no matches**.

- [ ] **Step 5: Commit.**
```bash
git add crates/api/dashboard
git commit -m "refactor(dashboard): workspaces + source listing (#72)"
```

---

### Task 6: Integration — ≥2-source workspace + full gate

**Files:** Modify `crates/core/src/fleet_provider.rs` (or `crates/core/src/fleet.rs` tests) — a test driving a 2-source workspace through `FakeCaliband`.

- [ ] **Step 1: Write the integration test.** A workspace whose root holds two checkouts registers both sources and observes agents from both through one caliband:
```rust
#[tokio::test]
async fn workspace_with_two_sources_registers_both_and_drives_one_caliband() {
    use crate::testkit::{FakeCaliband, test_record};
    let dir = tempfile::tempdir().unwrap();
    // Workspace root holding two source checkouts.
    std::fs::create_dir_all(dir.path().join("alpha/.git")).unwrap();
    std::fs::create_dir_all(dir.path().join("beta/.git")).unwrap();

    let mut config = FleetConfig::new("local", dir.path());
    config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
    config.ensure.autostart = false;
    // The one caliband is keyed on the workspace root.
    let socket = crate::discovery::resolve_socket(dir.path(), &config.discovery_env).unwrap();
    let mut fake = FakeCaliband::start_at(&socket).await.unwrap();
    let store = std::sync::Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
    let mgr = FleetManager::new(config, store).await.unwrap();

    mgr.add_workspace("ws", dir.path()).await.unwrap();
    // Two sources discovered.
    { let snap = mgr.snapshot().await;
      let ws = snap.workspaces.iter().find(|w| w.name == "ws").unwrap();
      assert_eq!(ws.sources.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(), vec!["alpha", "beta"]); }

    // Agents from both sources surface through the one control socket.
    fake.add_agent(test_record("a1", dir.path(), crate::model::AgentStatus::Running, false), Vec::new()).await;
    mgr.poll_repo_once("ws").await;
    let snap = mgr.snapshot().await;
    assert!(snap.find_agent("a1").is_some(), "agent observed via the workspace caliband");
}
```
(Adjust `poll_repo_once`'s name if it was renamed; if it becomes `poll_workspace_once`, use that. Confirm `test_record`'s `endpoint` is Unix under the workspace runtime dir.)

- [ ] **Step 2: Run it.** `cargo test -p prospero-core --features testkit workspace_with_two_sources 2>&1 | tail` → PASS.

- [ ] **Step 3: Full gate (CI mirror).**
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo build --workspace --all-targets
cargo test --workspace --features prospero-core/testkit
```
Expected: all green.

- [ ] **Step 4: Commit.**
```bash
git add crates/core/src
git commit -m "test(core): 2-source workspace drives one caliband end-to-end (#72)"
```

---

## Self-Review

**Spec coverage:**
- `Source`/`discover_sources` (spec §1) → Task 1. ✓
- `Workspace` registry unit + back-compat load (spec §2) → Task 2 Steps 1–2. ✓
- Workspace-keyed discovery, `--workspace-root` spawn (spec §3) → Task 2 Step 4. ✓
- Fleet one-caliband-per-workspace, `add_workspace` + `add_repo` shim, source enumeration (spec §4) → Task 2 Step 6 + Task 6. ✓
- DTO exposes sources; API/CLI/dashboard workspace-oriented (spec §5 + the chosen full-rename scope) → Tasks 3–5. ✓
- Back-compat single-source = identical hash/behavior → Task 2 (shim + `hash16` unchanged) + covered by unchanged existing tests. ✓
- Deferred: per-agent source attribution (no `working_dir` wire field), rich dashboard UI (#5), source-aware spawn (#324) — none in the tasks. ✓

**Placeholder scan:** Rename steps specify exact symbol/route/string maps (the Global-Constraints rename map is the source of truth) and rely on the Rust compiler (Task 2 Step 8, Task 3/4/6 builds) to enforce completeness — this is a mechanical-rename plan, not a vague one. New logic (Source, discover_sources, back-compat serde `alias`, sources-in-DTO, integration test) has full code. No TBD/TODO.

**Type consistency:** `Workspace`/`WorkspaceHealth`/`Source`/`Agent.workspace`/`FleetSnapshot.workspaces`/`add_workspace`/`WorkspaceNotFound`/`WorkspaceSummary`/`/api/workspaces` used consistently across Tasks 2–6. The event-layer `repo:` stream key + `FleetEvent.repo` are the one deliberate non-rename (persistence-compat), stated in Global Constraints and Task 2 scope.
