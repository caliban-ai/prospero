# Per-repo Provider/Environment Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator configure each repo's caliband environment (provider, base-URL, API-key reference, raw env) from Prospero, with a prosperod-level global default merged underneath, so agents reach the intended model backend.

**Architecture:** A pure `provider_env::resolve_env` resolver turns a repo's config + the global default + prosperod's environment into an env overlay; `discovery::ensure_caliband` applies that overlay when spawning caliband; the registry persists per-repo config; the API + dashboard expose it; editing config restarts the repo's caliband via the existing `Shutdown` control request.

**Tech Stack:** Rust (axum, tokio, serde), vanilla-JS dashboard (no build step). Tests via `cargo test` with the `FakeCaliband` harness (`prospero-core/testkit` feature).

**Design spec:** `docs/superpowers/specs/2026-06-08-per-repo-provider-config-design.md`
**Branch:** `dashboard-control-plane` (same PR as the dashboard controls — do NOT create a new branch).

---

## Working notes (read first)

- **Run all tests with the testkit feature:** `cargo test --workspace --features prospero-core/testkit`.
- **Clippy gate:** `cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings` must pass before each commit.
- Tasks 1–6 are Rust with real TDD (failing test first). Task 7 is the vanilla-JS dashboard (no JS test runner — verified by `cargo build` + `curl` of the served asset, per the dashboard-controls precedent; assets are embedded via `include_str!`, so a browser check needs a rebuild+restart).
- The daemon currently running for manual checks can be restarted with:
  ```bash
  pkill -f 'target/debug/prosperod'; sleep 1
  OLLAMA_BASE_URL=http://192.168.1.240:11434 cargo run --bin prosperod -- \
    --caliband-bin "$HOME/dev/caliban-ai/caliban/target/release/caliband" > /tmp/prosperod.log 2>&1 &
  until grep -q listening /tmp/prosperod.log; do sleep 1; done
  ```

## File structure

| File | Change | Responsibility |
|------|--------|----------------|
| `crates/core/src/registry.rs` | Modify | `RepoProviderConfig` type; `config` field on `RegisteredRepo`; `set_config`; backward-compat load. |
| `crates/core/src/provider_env.rs` | Create | Pure `resolve_env` + provider→env-var mapping. |
| `crates/core/src/lib.rs` | Modify | Register `provider_env` module; re-export `RepoProviderConfig`. |
| `crates/core/src/discovery.rs` | Modify | `env` field on `EnsureConfig`; apply `.envs()` in `ensure_caliband`. |
| `crates/core/src/fleet.rs` | Modify | `default_env` on `FleetConfig`; `ensure_config_for`; `set_repo_config`; `restart_caliband`; `add_repo_with_config`. |
| `crates/core/src/testkit.rs` | Modify | Count shutdowns + stop listening on `Shutdown` (for the restart test). |
| `crates/api/src/dto.rs` | Modify | `config` on `AddRepoBody`; `config` on `RepoSummary`; `SetConfigBody`. |
| `crates/api/src/handlers.rs` | Modify | Pass config on add; `set_repo_config` handler; include config in `get_repos`. |
| `crates/api/src/lib.rs` | Modify | `PUT /api/repos/{name}/config` route. |
| `crates/daemon/src/main.rs` | Modify | `--default-env KEY=VAL` flag → `FleetConfig.default_env`. |
| `crates/api/dashboard/index.html` | Modify | CSS for the settings button + env-row editor. |
| `crates/api/dashboard/app.js` | Modify | Config fields in add-repo modal; repo-settings modal; `PUT config`. |

---

## Task 1: `RepoProviderConfig` type + registry storage

**Files:** Modify `crates/core/src/registry.rs`

- [ ] **Step 1: Write failing tests.** Add to the `tests` module at the bottom of `crates/core/src/registry.rs`:

```rust
    #[test]
    fn repo_config_defaults_empty() {
        let c = RepoProviderConfig::default();
        assert!(c.provider.is_none() && c.base_url.is_none()
            && c.api_key_from_env.is_none() && c.env.is_empty());
    }

    #[test]
    fn old_registry_json_without_config_loads_with_default() {
        // Backward compat: a registry.json written before this field existed.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registry.json");
        std::fs::write(&path, r#"{"repos":[{"name":"p","root":"/r"}]}"#).unwrap();
        let reg = Registry::load(&path).unwrap();
        assert_eq!(reg.get("p").unwrap().config, RepoProviderConfig::default());
    }

    #[test]
    fn set_config_updates_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registry.json");
        let mut reg = Registry::default();
        reg.add("p", "/r").unwrap();
        let mut cfg = RepoProviderConfig::default();
        cfg.provider = Some("ollama".into());
        cfg.base_url = Some("http://host:11434".into());
        assert!(reg.set_config("p", cfg.clone()));
        assert!(!reg.set_config("missing", cfg.clone()));
        reg.save(&path).unwrap();
        let loaded = Registry::load(&path).unwrap();
        assert_eq!(loaded.get("p").unwrap().config, cfg);
    }
```

- [ ] **Step 2: Run tests, verify they fail.** Run: `cargo test -p prospero-core registry:: 2>&1 | tail -20`. Expected: compile error (`RepoProviderConfig` / `config` / `set_config` undefined).

- [ ] **Step 3: Implement.** In `crates/core/src/registry.rs`, add the import at the top (after the existing `use` lines):

```rust
use std::collections::BTreeMap;
```

Add the new type above `RegisteredRepo`:

```rust
/// Per-repo provider/environment configuration applied to its caliband daemon.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoProviderConfig {
    /// Selected provider → `CALIBAN_PROVIDER`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Provider base URL / host → `{PROVIDER}_BASE_URL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// NAME of an env var in prosperod's environment whose value is injected as
    /// `{PROVIDER}_API_KEY` at spawn time. Never the literal secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_from_env: Option<String>,
    /// Raw escape-hatch env overrides (highest precedence within a repo).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}
```

Add the `config` field to `RegisteredRepo` (keep `name`/`root`):

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredRepo {
    /// Operator-chosen short name (registry key).
    pub name: String,
    /// Canonical repo root path.
    pub root: PathBuf,
    /// Provider/environment config for this repo's caliband daemon.
    #[serde(default)]
    pub config: RepoProviderConfig,
}
```

In `Registry::add`, set the new field when pushing (replace the `self.repos.push(...)` line):

```rust
        self.repos.push(RegisteredRepo {
            name,
            root,
            config: RepoProviderConfig::default(),
        });
```

Add a `set_config` method inside `impl Registry` (after `remove`):

```rust
    /// Replace a repo's provider config. Returns whether the repo existed.
    pub fn set_config(&mut self, name: &str, config: RepoProviderConfig) -> bool {
        if let Some(r) = self.repos.iter_mut().find(|r| r.name == name) {
            r.config = config;
            true
        } else {
            false
        }
    }
```

- [ ] **Step 4: Run tests, verify they pass.** Run: `cargo test -p prospero-core registry:: 2>&1 | tail -20`. Expected: all pass (including the pre-existing registry tests — note `add_get_remove` etc. still pass since `config` defaults).

- [ ] **Step 5: Commit.**
```bash
git add crates/core/src/registry.rs
git commit -m "feat(core): RepoProviderConfig + per-repo config in registry"
```

---

## Task 2: `provider_env::resolve_env` pure resolver

**Files:** Create `crates/core/src/provider_env.rs`; modify `crates/core/src/lib.rs`

- [ ] **Step 1: Write failing tests.** Create `crates/core/src/provider_env.rs` with the test module only first (implementation in step 3):

```rust
//! Pure resolution of a repo's caliband environment overlay.
//!
//! Combines the prosperod-level default env, a repo's curated provider fields,
//! and its raw env map into one overlay applied to the caliband process.

use std::collections::BTreeMap;

use crate::registry::RepoProviderConfig;

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RepoProviderConfig {
        RepoProviderConfig::default()
    }
    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn provider_and_base_url_map_to_env_vars() {
        let mut c = cfg();
        c.provider = Some("ollama".into());
        c.base_url = Some("http://h:11434".into());
        let out = resolve_env(&BTreeMap::new(), &c, &no_env);
        assert_eq!(out.get("CALIBAN_PROVIDER").unwrap(), "ollama");
        assert_eq!(out.get("OLLAMA_BASE_URL").unwrap(), "http://h:11434");
    }

    #[test]
    fn api_key_from_env_is_resolved_from_process_env() {
        let mut c = cfg();
        c.provider = Some("anthropic".into());
        c.api_key_from_env = Some("MY_KEY".into());
        let proc = |k: &str| (k == "MY_KEY").then(|| "secret-value".to_string());
        let out = resolve_env(&BTreeMap::new(), &c, &proc);
        assert_eq!(out.get("ANTHROPIC_API_KEY").unwrap(), "secret-value");
    }

    #[test]
    fn dangling_api_key_reference_is_skipped() {
        let mut c = cfg();
        c.provider = Some("anthropic".into());
        c.api_key_from_env = Some("UNSET_VAR".into());
        let out = resolve_env(&BTreeMap::new(), &c, &no_env);
        assert!(!out.contains_key("ANTHROPIC_API_KEY"));
    }

    #[test]
    fn precedence_is_global_then_curated_then_raw() {
        let mut default_env = BTreeMap::new();
        default_env.insert("CALIBAN_PROVIDER".into(), "openai".into());
        default_env.insert("KEEP".into(), "from-global".into());
        let mut c = cfg();
        c.provider = Some("ollama".into()); // curated overrides global
        c.env.insert("CALIBAN_PROVIDER".into(), "raw-wins".into()); // raw overrides curated
        let out = resolve_env(&default_env, &c, &no_env);
        assert_eq!(out.get("CALIBAN_PROVIDER").unwrap(), "raw-wins");
        assert_eq!(out.get("KEEP").unwrap(), "from-global");
    }

    #[test]
    fn provider_only_backend_ignores_base_url() {
        let mut c = cfg();
        c.provider = Some("bedrock".into());
        c.base_url = Some("http://ignored".into());
        let out = resolve_env(&BTreeMap::new(), &c, &no_env);
        assert_eq!(out.get("CALIBAN_PROVIDER").unwrap(), "bedrock");
        assert!(out.keys().all(|k| k == "CALIBAN_PROVIDER"));
    }
}
```

- [ ] **Step 2: Run tests, verify they fail.** First register the module so it compiles: in `crates/core/src/lib.rs`, add `pub mod provider_env;` alongside the other `pub mod` lines, and add `pub use registry::RepoProviderConfig;` next to existing re-exports. Then run: `cargo test -p prospero-core provider_env:: 2>&1 | tail -20`. Expected: compile error (`resolve_env` not found).

- [ ] **Step 3: Implement.** Insert the implementation in `crates/core/src/provider_env.rs` above the `#[cfg(test)]` module:

```rust
/// `(base_url_var, api_key_var)` for a provider, or `(None, None)` for
/// provider-only backends (bedrock/vertex use ambient cloud credentials).
fn provider_vars(provider: &str) -> (Option<&'static str>, Option<&'static str>) {
    match provider {
        "ollama" => (Some("OLLAMA_BASE_URL"), None),
        "anthropic" => (Some("ANTHROPIC_BASE_URL"), Some("ANTHROPIC_API_KEY")),
        "openai" => (Some("OPENAI_BASE_URL"), Some("OPENAI_API_KEY")),
        "google" => (Some("GEMINI_BASE_URL"), Some("GEMINI_API_KEY")),
        _ => (None, None), // bedrock, vertex, unknown
    }
}

/// Resolve the environment overlay for a repo's caliband daemon.
///
/// Layered lowest → highest: `default_env` (global) → curated provider fields →
/// `cfg.env` (raw). `process_env` looks up prosperod's own environment for
/// `api_key_from_env` references.
pub fn resolve_env(
    default_env: &BTreeMap<String, String>,
    cfg: &RepoProviderConfig,
    process_env: &dyn Fn(&str) -> Option<String>,
) -> BTreeMap<String, String> {
    let mut out = default_env.clone();

    if let Some(provider) = &cfg.provider {
        out.insert("CALIBAN_PROVIDER".to_string(), provider.clone());
        let (base_var, key_var) = provider_vars(provider);
        if let Some(base_url) = &cfg.base_url {
            match base_var {
                Some(var) => {
                    out.insert(var.to_string(), base_url.clone());
                }
                None => tracing::warn!(
                    target: "prospero_provider_env",
                    provider, "base_url set but provider has no base-URL env var; ignored"
                ),
            }
        }
        if let Some(name) = &cfg.api_key_from_env {
            match key_var {
                Some(var) => match process_env(name) {
                    Some(value) => {
                        out.insert(var.to_string(), value);
                    }
                    None => tracing::warn!(
                        target: "prospero_provider_env",
                        env_var = %name,
                        "api_key_from_env references an unset variable; skipped"
                    ),
                },
                None => tracing::warn!(
                    target: "prospero_provider_env",
                    provider, "api_key_from_env set but provider has no api-key env var; ignored"
                ),
            }
        }
    }

    for (k, v) in &cfg.env {
        out.insert(k.clone(), v.clone());
    }
    out
}
```

- [ ] **Step 4: Run tests, verify they pass.** Run: `cargo test -p prospero-core provider_env:: 2>&1 | tail -20`. Expected: 5 tests pass.

- [ ] **Step 5: Commit.**
```bash
git add crates/core/src/provider_env.rs crates/core/src/lib.rs
git commit -m "feat(core): provider_env::resolve_env env overlay resolver"
```

---

## Task 3: Per-repo `EnsureConfig.env` + global `default_env` + `ensure_config_for`

**Files:** Modify `crates/core/src/discovery.rs`, `crates/core/src/fleet.rs`

- [ ] **Step 1: Write failing test.** Add to the `tests` module at the bottom of `crates/core/src/fleet.rs` (it already has integration-style helpers; this test builds a manager over a temp dir). First confirm the module's existing imports include what you need; add this test:

```rust
    #[tokio::test]
    async fn ensure_config_for_merges_default_and_repo_config() {
        use crate::registry::RepoProviderConfig;
        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.default_env.insert("KEEP".into(), "global".into());
        let store = std::sync::Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).unwrap();

        mgr.add_repo("p", "/tmp/p").await.ok(); // discovery may fail; registry write is what matters
        let mut cfg = RepoProviderConfig::default();
        cfg.provider = Some("ollama".into());
        cfg.base_url = Some("http://h:11434".into());
        cfg.env.insert("EXTRA".into(), "1".into());
        mgr.set_repo_config_registry_only("p", cfg).await.unwrap();

        let ec = mgr.ensure_config_for("p").await.unwrap();
        assert_eq!(ec.env.get("KEEP").unwrap(), "global");
        assert_eq!(ec.env.get("CALIBAN_PROVIDER").unwrap(), "ollama");
        assert_eq!(ec.env.get("OLLAMA_BASE_URL").unwrap(), "http://h:11434");
        assert_eq!(ec.env.get("EXTRA").unwrap(), "1");
    }
```

> Note: this test uses a registry-only config setter (`set_repo_config_registry_only`) so it does not trigger a daemon restart. `set_repo_config` (Task 4) wraps it + restart.

- [ ] **Step 2: Run test, verify it fails.** Run: `cargo test -p prospero-core ensure_config_for 2>&1 | tail -20`. Expected: compile error (`default_env`, `ensure_config_for`, `set_repo_config_registry_only` undefined).

- [ ] **Step 3a: Implement `EnsureConfig.env` (discovery.rs).** Add the field to `EnsureConfig` (after `startup_timeout`):

```rust
    /// Extra environment variables layered onto the caliband process.
    pub env: std::collections::BTreeMap<String, String>,
```

In `impl Default for EnsureConfig`, add `env: std::collections::BTreeMap::new(),` to the struct literal. In `ensure_caliband`, change the spawn to apply the env (insert `.envs(...)` before `.spawn()`):

```rust
    tokio::process::Command::new(&cfg.caliband_bin)
        .arg("--repo-root")
        .arg(repo_root)
        .envs(&cfg.env)
        .spawn()
        .map_err(|e| CoreError::Discovery(format!("failed to spawn {} : {e}", cfg.caliband_bin)))?;
```

- [ ] **Step 3b: Implement `default_env` + `ensure_config_for` + registry-only setter (fleet.rs).** Add the field to `FleetConfig` (after `event_buffer`):

```rust
    /// Global default env merged under each repo's resolved overlay.
    pub default_env: std::collections::BTreeMap<String, String>,
```

In `FleetConfig::new`, add `default_env: std::collections::BTreeMap::new(),` to the struct literal.

Add these methods inside `impl FleetManager` (near `client_for`):

```rust
    /// Build the `EnsureConfig` for a repo, resolving its env overlay from the
    /// global default + the repo's stored provider config + prosperod's env.
    pub async fn ensure_config_for(&self, repo: &str) -> Result<EnsureConfig> {
        let cfg = {
            let reg = self.inner.registry.read().await;
            reg.get(repo)
                .map(|r| r.config.clone())
                .ok_or_else(|| CoreError::RepoNotFound(repo.to_string()))?
        };
        let env = crate::provider_env::resolve_env(
            &self.inner.config.default_env,
            &cfg,
            &|k| std::env::var(k).ok(),
        );
        let mut ensure = self.inner.config.ensure.clone();
        ensure.env = env;
        Ok(ensure)
    }

    /// Update a repo's provider config in the registry only (no restart).
    pub async fn set_repo_config_registry_only(
        &self,
        repo: &str,
        config: crate::registry::RepoProviderConfig,
    ) -> Result<()> {
        let mut reg = self.inner.registry.write().await;
        if !reg.set_config(repo, config) {
            return Err(CoreError::RepoNotFound(repo.to_string()));
        }
        reg.save(&self.inner.config.registry_path())?;
        Ok(())
    }
```

Change `client_for` to use the per-repo ensure config — replace its `ensure_caliband(&root, &self.inner.config.discovery_env, &self.inner.config.ensure)` call with:

```rust
        let ensure = self.ensure_config_for(repo).await?;
        let client = ensure_caliband(&root, &self.inner.config.discovery_env, &ensure).await?;
```

(Remove the now-unused `root` lookup duplication only if it becomes unused; `root` is still needed for `ensure_caliband`, so keep it.)

- [ ] **Step 4: Run test, verify it passes.** Run: `cargo test -p prospero-core ensure_config_for 2>&1 | tail -20`. Expected: pass. Then `cargo build -p prospero-core` to confirm `client_for` still compiles.

- [ ] **Step 5: Commit.**
```bash
git add crates/core/src/discovery.rs crates/core/src/fleet.rs
git commit -m "feat(core): per-repo caliband env overlay + global default_env"
```

---

## Task 4: `restart_caliband` + `set_repo_config` (with testkit shutdown support)

**Files:** Modify `crates/core/src/testkit.rs`, `crates/core/src/fleet.rs`

- [ ] **Step 1: Write failing test.** Add to the `tests` module at the bottom of `crates/core/src/fleet.rs`:

```rust
    #[tokio::test]
    async fn restart_caliband_shuts_down_and_clears_client() {
        use crate::registry::RepoProviderConfig;
        use crate::testkit::FakeCaliband;

        let dir = tempfile::tempdir().unwrap();
        // Pin discovery to a known socket dir so the fake and the manager agree.
        let mut config = FleetConfig::new("local", dir.path());
        config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
        config.ensure.autostart = false; // no real caliband to spawn in tests
        let root = dir.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        let socket = crate::discovery::resolve_socket(&root, &config.discovery_env).unwrap();

        let mut fake = FakeCaliband::start_at(&socket).await.unwrap();
        let _ = &mut fake;
        let store = std::sync::Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).unwrap();
        mgr.add_repo("p", &root).await.unwrap();

        // Cache a client by talking to the repo once.
        mgr.poll_repo_once("p").await;

        mgr.set_repo_config("p", RepoProviderConfig::default()).await.unwrap();

        assert_eq!(fake.shutdowns(), 1, "restart should send one Shutdown");
        assert!(
            mgr.cached_client_names().await.iter().all(|n| n != "p"),
            "cached client for the repo should be cleared after restart"
        );
    }
```

- [ ] **Step 2: Run test, verify it fails.** Run: `cargo test -p prospero-core --features testkit restart_caliband 2>&1 | tail -20`. Expected: compile error (`shutdowns`, `set_repo_config`, `cached_client_names` undefined).

- [ ] **Step 3a: Testkit — count + honor Shutdown.** In `crates/core/src/testkit.rs`, the `FakeState`/`FakeCaliband` need a shutdown counter, and the control loop should stop the listener on `Shutdown`. Find the `FakeState` struct and add a field `shutdowns: u32` (initialize to 0 wherever `FakeState` is constructed). In `FakeCaliband`, add an `Arc<Mutex<FakeState>>`-backed accessor:

```rust
    /// Number of `Shutdown` requests the fake has received.
    pub fn shutdowns(&self) -> u32 {
        self.state.lock().unwrap().shutdowns
    }
```

(If `FakeCaliband` does not already hold the `Arc<Mutex<FakeState>>` as a field named `state`, add `state: Arc<Mutex<FakeState>>` to the struct and store a clone of the same `Arc` passed to the control loop in `start_at`.)

In `handle_control_conn`, change the `Shutdown` arm to count and signal stop:

```rust
            CtlRequest::Shutdown => {
                st.shutdowns += 1;
                (CtlReply::ShutdownAck, None)
            }
```

Then, after the reply is written, if the request was `Shutdown`, the accept loop should stop and the socket file be removed. The simplest robust approach: have `start_at`'s accept loop check a `should_stop` flag set on shutdown. Add `should_stop: bool` to `FakeState` (default false), set it `true` in the `Shutdown` arm, and in the accept loop (the `tokio::spawn` that loops on `listener.accept()`), break when `state.lock().unwrap().should_stop` is true after handling a connection, then `let _ = std::fs::remove_file(&control_socket);`.

> Implementation note: keep changes minimal and localized; the existing accept loop already holds `control_socket` and the shared state. If the loop structure makes a clean break hard, an acceptable alternative is for the `Shutdown` arm to `std::fs::remove_file` the control socket immediately so the next `connect` fails — the manager's restart wait only needs the socket to become unreachable.

- [ ] **Step 3b: `restart_caliband` + `set_repo_config` (fleet.rs).** Add inside `impl FleetManager`:

```rust
    /// Names of repos with a cached control client (test/observability helper).
    pub async fn cached_client_names(&self) -> Vec<String> {
        self.inner.clients.lock().unwrap().keys().cloned().collect()
    }

    /// Gracefully shut down a repo's caliband daemon and drop its cached client
    /// so the next access re-runs discovery (respawning with the current env).
    pub async fn restart_caliband(&self, repo: &str) -> Result<()> {
        let client = self.inner.clients.lock().unwrap().get(repo).cloned();
        if let Some(client) = client {
            if let Err(e) = client.shutdown().await {
                tracing::warn!(target: "prospero_fleet", repo, error = %e,
                    "shutdown request to caliband failed (continuing)");
            }
        }
        self.inner.clients.lock().unwrap().remove(repo);

        // Wait (bounded) for the old socket to go away so the next ensure spawns
        // fresh rather than reusing the draining daemon.
        let root = {
            let reg = self.inner.registry.read().await;
            reg.get(repo).map(|r| r.root.clone())
        };
        if let Some(root) = root {
            if let Ok(socket) = crate::discovery::resolve_socket(&root, &self.inner.config.discovery_env) {
                let deadline = tokio::time::Instant::now() + self.inner.config.ensure.startup_timeout;
                while tokio::net::UnixStream::connect(&socket).await.is_ok() {
                    if tokio::time::Instant::now() >= deadline {
                        tracing::warn!(target: "prospero_fleet", repo,
                            "old caliband socket still reachable after shutdown; proceeding");
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
        self.poll_repo_once(repo).await;
        Ok(())
    }

    /// Persist a repo's provider config and restart its caliband to apply it.
    pub async fn set_repo_config(
        &self,
        repo: &str,
        config: crate::registry::RepoProviderConfig,
    ) -> Result<()> {
        self.set_repo_config_registry_only(repo, config).await?;
        self.restart_caliband(repo).await
    }
```

- [ ] **Step 4: Run test, verify it passes.** Run: `cargo test -p prospero-core --features testkit restart_caliband 2>&1 | tail -20`. Expected: pass. Then run the full core suite to catch testkit regressions: `cargo test -p prospero-core --features testkit 2>&1 | tail -20`. Expected: all pass.

- [ ] **Step 5: Commit.**
```bash
git add crates/core/src/testkit.rs crates/core/src/fleet.rs
git commit -m "feat(core): restart_caliband + set_repo_config; testkit shutdown support"
```

---

## Task 5: API — config on add, `PUT /api/repos/{name}/config`, config in repo responses

**Files:** Modify `crates/api/src/dto.rs`, `crates/api/src/handlers.rs`, `crates/api/src/lib.rs`, and `crates/core/src/fleet.rs` (add `add_repo_with_config`)

- [ ] **Step 1: Write failing test.** Add to `crates/api/tests/api_integration.rs` (mirror the existing harness setup in that file for building the router over a `FleetManager` + `FakeCaliband`; reuse its helper if present). Append:

```rust
#[tokio::test]
async fn add_repo_with_config_then_read_it_back() {
    // Build the test app exactly like the other tests in this file
    // (FakeCaliband + FleetManager + prospero_api::router). See existing setup.
    let app = test_app().await; // <-- use this file's existing harness/helper

    let body = serde_json::json!({
        "name": "p", "root": "/tmp/p",
        "config": { "provider": "ollama", "base_url": "http://h:11434" }
    });
    let res = app.post_json("/api/repos", &body).await;
    assert_eq!(res.status, 201);

    let repos = app.get_json("/api/repos").await;
    let p = repos.as_array().unwrap().iter().find(|r| r["name"] == "p").unwrap();
    assert_eq!(p["config"]["provider"], "ollama");
    assert_eq!(p["config"]["base_url"], "http://h:11434");
}
```

> If `api_integration.rs` does not already expose `test_app()`/`post_json`/`get_json` helpers, use the same request-construction style the existing tests in that file use (tower `oneshot` against `prospero_api::router(manager)`), and assert on the decoded JSON. Do not invent helpers that aren't there — match the file's actual pattern.

- [ ] **Step 2: Run test, verify it fails.** Run: `cargo test -p prospero-api --features prospero-core/testkit add_repo_with_config 2>&1 | tail -20`. Expected: fail (config not accepted/returned).

- [ ] **Step 3a: Core — `add_repo_with_config` (fleet.rs).** Change `add_repo` to delegate. Replace the body of `add_repo` so it calls a new config-aware method:

```rust
    /// Register a repo and persist the registry. Triggers an immediate poll.
    pub async fn add_repo(&self, name: impl Into<String>, root: impl Into<PathBuf>) -> Result<()> {
        self.add_repo_with_config(name, root, Default::default()).await
    }

    /// Register a repo with an initial provider config.
    pub async fn add_repo_with_config(
        &self,
        name: impl Into<String>,
        root: impl Into<PathBuf>,
        config: crate::registry::RepoProviderConfig,
    ) -> Result<()> {
        let name = name.into();
        let root = root.into();
        {
            let mut reg = self.inner.registry.write().await;
            reg.add(name.clone(), root.clone())?;
            reg.set_config(&name, config);
            reg.save(&self.inner.config.registry_path())?;
        }
        {
            let mut snap = self.inner.snapshot.write().await;
            if !snap.repos.iter().any(|r| r.name == name) {
                snap.repos.push(Repo {
                    name: name.clone(),
                    root: root.clone(),
                    health: RepoHealth::Healthy,
                    agents: Vec::new(),
                });
            }
        }
        self.poll_repo_once(&name).await;
        Ok(())
    }
```

- [ ] **Step 3b: DTOs (dto.rs).** Add the import and extend the bodies. At the top:

```rust
use prospero_core::registry::RepoProviderConfig;
```

Extend `AddRepoBody`:

```rust
#[derive(Debug, Deserialize)]
pub struct AddRepoBody {
    pub name: String,
    pub root: String,
    /// Optional initial provider config.
    #[serde(default)]
    pub config: RepoProviderConfig,
}
```

Add a body type for the PUT and extend `RepoSummary` (add `config`):

```rust
/// Body for `PUT /api/repos/{name}/config`.
#[derive(Debug, Deserialize)]
pub struct SetConfigBody(pub RepoProviderConfig);

#[derive(Debug, Serialize)]
pub struct RepoSummary {
    pub name: String,
    pub root: String,
    pub health: prospero_core::RepoHealth,
    pub agent_count: usize,
    /// Provider/environment config for this repo.
    pub config: RepoProviderConfig,
}
```

- [ ] **Step 3c: Handlers (handlers.rs).** Update `add_repo`, `get_repos`, and add `set_repo_config`. The `add_repo` handler:

```rust
pub async fn add_repo(
    State(st): State<AppState>,
    Json(body): Json<AddRepoBody>,
) -> Result<StatusCode, ApiError> {
    st.manager
        .add_repo_with_config(body.name, body.root, body.config)
        .await?;
    Ok(StatusCode::CREATED)
}
```

To include `config` in `get_repos`, the handler must read it from the registry. Add a manager accessor in `fleet.rs` inside `impl FleetManager`:

```rust
    /// The stored provider config for a repo, if registered.
    pub async fn repo_config(&self, repo: &str) -> Option<crate::registry::RepoProviderConfig> {
        self.inner.registry.read().await.get(repo).map(|r| r.config.clone())
    }
```

Then in `get_repos` (handlers.rs), set `config` per repo (it currently maps the snapshot into `RepoSummary`):

```rust
pub async fn get_repos(State(st): State<AppState>) -> Json<Vec<RepoSummary>> {
    let snap = st.manager.snapshot().await;
    let mut out = Vec::with_capacity(snap.repos.len());
    for r in snap.repos {
        let config = st.manager.repo_config(&r.name).await.unwrap_or_default();
        out.push(RepoSummary {
            name: r.name,
            root: r.root.display().to_string(),
            health: r.health,
            agent_count: r.agents.len(),
            config,
        });
    }
    Json(out)
}
```

Add the new handler:

```rust
/// `PUT /api/repos/{name}/config` — set provider config and restart caliband.
pub async fn set_repo_config(
    State(st): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<SetConfigBody>,
) -> Result<StatusCode, ApiError> {
    st.manager.set_repo_config(&name, body.0).await?;
    Ok(StatusCode::NO_CONTENT)
}
```

Ensure `SetConfigBody` and `RepoProviderConfig` are imported in handlers.rs (`use crate::dto::{..., SetConfigBody};`).

- [ ] **Step 3d: Route (lib.rs).** Add after the `/api/repos/{name}` delete route, and make sure `put` is imported (`use axum::routing::{delete, get, post, put};`):

```rust
        .route("/api/repos/{name}/config", put(handlers::set_repo_config))
```

- [ ] **Step 4: Run test, verify it passes.** Run: `cargo test -p prospero-api --features prospero-core/testkit add_repo_with_config 2>&1 | tail -20`. Expected: pass. Then `cargo test --workspace --features prospero-core/testkit 2>&1 | tail -15` — all green.

- [ ] **Step 5: Commit.**
```bash
git add crates/api/src/dto.rs crates/api/src/handlers.rs crates/api/src/lib.rs crates/core/src/fleet.rs crates/api/tests/api_integration.rs
git commit -m "feat(api): repo provider config on add + PUT config + config in repo list"
```

---

## Task 6: daemon `--default-env KEY=VAL` flag

**Files:** Modify `crates/daemon/src/main.rs`

- [ ] **Step 1: Write failing test.** Add a `#[cfg(test)]` module at the bottom of `crates/daemon/src/main.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::parse_key_val;

    #[test]
    fn parses_key_value() {
        assert_eq!(parse_key_val("A=b").unwrap(), ("A".to_string(), "b".to_string()));
        // Values may contain '='.
        assert_eq!(
            parse_key_val("URL=http://h:1?x=1").unwrap(),
            ("URL".to_string(), "http://h:1?x=1".to_string())
        );
        assert!(parse_key_val("noequals").is_err());
    }
}
```

- [ ] **Step 2: Run test, verify it fails.** Run: `cargo test -p prospero-daemon parses_key_value 2>&1 | tail -20`. Expected: compile error (`parse_key_val` undefined).

- [ ] **Step 3: Implement.** In `crates/daemon/src/main.rs`, add the parser helper (above `main`):

```rust
/// Parse a `KEY=VALUE` pair (value may contain further `=`).
fn parse_key_val(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(format!("expected KEY=VALUE, got '{s}'")),
    }
}
```

Add the flag to `Args` (after `caliband_bin`):

```rust
    /// Default env var applied under every repo's resolved config (repeatable).
    #[arg(long = "default-env", value_parser = parse_key_val)]
    default_env: Vec<(String, String)>,
```

In `main`, after `config.ensure = EnsureConfig { ... }`, set the default env (convert the Vec to a map):

```rust
    config.default_env = args.default_env.iter().cloned().collect();
```

(Confirm `FleetConfig` is in scope — it already is via the existing `use prospero_core::fleet::{FleetConfig, FleetManager};`.)

- [ ] **Step 4: Run test, verify it passes.** Run: `cargo test -p prospero-daemon parses_key_value 2>&1 | tail -20`. Expected: pass. Then verify the flag wires up: `cargo run --bin prosperod -- --help 2>&1 | grep default-env`. Expected: the flag appears.

- [ ] **Step 5: Commit.**
```bash
git add crates/daemon/src/main.rs
git commit -m "feat(daemon): --default-env KEY=VAL global default for repo configs"
```

---

## Task 7: Dashboard — provider config in add-repo modal + repo-settings modal

**Files:** Modify `crates/api/dashboard/index.html`, `crates/api/dashboard/app.js`

No JS test runner; verify by `cargo build`, a `curl` of the served asset, and (optionally) a browser check after rebuild+restart.

- [ ] **Step 1: Add CSS.** In `crates/api/dashboard/index.html`, before `</style>`, add:

```css
      .repo-head .gear { font: 11px ui-monospace, monospace; padding: 1px 6px; border-radius: 5px;
                         border: 1px solid #2d3340; background: #1b1f27; color: #9aa0aa; cursor: pointer; }
      .repo-head .gear:hover { border-color: #3a4256; }
      .repo-head-actions { display: flex; gap: 4px; }
      .env-row { display: flex; gap: 6px; margin-bottom: 6px; }
      .env-row .in { margin-top: 0; }
      .env-row button { font: 11px ui-monospace, monospace; border: 1px solid #3a2424; background: #1b1f27;
                        color: #ef9a9a; border-radius: 5px; cursor: pointer; padding: 0 8px; }
      .env-add { font: 11px ui-monospace, monospace; color: #8ab4f8; cursor: pointer; }
```

- [ ] **Step 2: Add a shared provider-fields builder + config reader to `app.js`.** After `openAddRepoModal()` (or near the modal helpers), add:

```js
// --- Provider-config form fields (shared by add-repo + repo-settings) --------

const PROVIDERS = ["", "ollama", "anthropic", "openai", "google", "bedrock", "vertex"];

// Append provider/base_url/api-key/raw-env fields to `form`, prefilled from `cfg`.
function appendProviderFields(form, cfg) {
  cfg = cfg || {};
  const opts = PROVIDERS.map((p) =>
    `<option value="${p}"${p === (cfg.provider || "") ? " selected" : ""}>${p || "(default)"}</option>`
  ).join("");
  const wrap = document.createElement("div");
  wrap.innerHTML =
    `<label class="fl">provider<select class="in" id="pc-provider">${opts}</select></label>` +
    `<label class="fl">base URL<input class="in" id="pc-baseurl" placeholder="http://host:11434"></label>` +
    `<label class="fl">API key from env var<input class="in" id="pc-keyenv" placeholder="e.g. ANTHROPIC_API_KEY"></label>` +
    `<div class="adv-toggle" id="pc-adv-toggle">▸ advanced env</div>` +
    `<div class="hidden" id="pc-adv"><div id="pc-env-rows"></div>` +
      `<span class="env-add" id="pc-env-add">+ add env var</span></div>`;
  form.appendChild(wrap);
  form.querySelector("#pc-baseurl").value = cfg.base_url || "";
  form.querySelector("#pc-keyenv").value = cfg.api_key_from_env || "";

  const rows = form.querySelector("#pc-env-rows");
  const addRow = (k, v) => {
    const row = document.createElement("div");
    row.className = "env-row";
    row.innerHTML = `<input class="in pc-k" placeholder="KEY"><input class="in pc-v" placeholder="VALUE"><button type="button">×</button>`;
    row.querySelector(".pc-k").value = k || "";
    row.querySelector(".pc-v").value = v || "";
    row.querySelector("button").onclick = () => row.remove();
    rows.appendChild(row);
  };
  for (const [k, v] of Object.entries(cfg.env || {})) addRow(k, v);
  const adv = form.querySelector("#pc-adv");
  const advToggle = form.querySelector("#pc-adv-toggle");
  advToggle.onclick = () => {
    adv.classList.toggle("hidden");
    advToggle.textContent = adv.classList.contains("hidden") ? "▸ advanced env" : "▾ advanced env";
  };
  form.querySelector("#pc-env-add").onclick = () => addRow("", "");
  if (Object.keys(cfg.env || {}).length) adv.classList.remove("hidden");
}

// Read the provider config object back out of the fields appendProviderFields added.
function readProviderConfig(form) {
  const cfg = {};
  const provider = form.querySelector("#pc-provider").value.trim();
  const baseUrl = form.querySelector("#pc-baseurl").value.trim();
  const keyEnv = form.querySelector("#pc-keyenv").value.trim();
  if (provider) cfg.provider = provider;
  if (baseUrl) cfg.base_url = baseUrl;
  if (keyEnv) cfg.api_key_from_env = keyEnv;
  const env = {};
  for (const row of form.querySelectorAll(".env-row")) {
    const k = row.querySelector(".pc-k").value.trim();
    const v = row.querySelector(".pc-v").value.trim();
    if (k) env[k] = v;
  }
  if (Object.keys(env).length) cfg.env = env;
  return cfg;
}
```

- [ ] **Step 3: Wire config into the add-repo modal.** In `openAddRepoModal()`, after `openModal(form);` and before wiring cancel/submit, append the provider fields, and include the config in the POST body. Insert after `openModal(form);`:

```js
  appendProviderFields(form, {});
```

And change the submit handler's `api(...)` call to include config:

```js
      await api("POST", "/api/repos", { name, root, config: readProviderConfig(form) });
```

> Note: `appendProviderFields` appends after the form-actions row in the current markup. To keep buttons last, build the form so the title/name/path come first, then call `appendProviderFields(form, {})` BEFORE appending the `form-err` + `form-actions`. Restructure `openAddRepoModal` so it: sets name/path inputs → `openModal(form)` → `appendProviderFields(form, {})` → append `#ar-err` and the `.form-actions` row (move those two nodes to be created and appended after the provider fields). Verify visually that Add/cancel render at the bottom.

- [ ] **Step 4: Add the repo-settings (gear) modal.** In `renderFleet`'s repo loop, replace the single `head.appendChild(actionBtn("remove", ...))` with a small actions group holding a gear + remove:

```js
    const acts = document.createElement("div");
    acts.className = "repo-head-actions";
    const gear = document.createElement("button");
    gear.className = "gear";
    gear.textContent = "⚙";
    gear.onclick = () => openRepoSettings(repo);
    acts.appendChild(gear);
    acts.appendChild(actionBtn("remove", "danger", (b) =>
      rowAction("DELETE", `/api/repos/${encodeURIComponent(repo.name)}`,
                `Remove repo ${repo.name}?`, b)));
    head.appendChild(acts);
```

Add `openRepoSettings`, which reads the repo's current `config` (already present on each repo object from `/api/fleet`? — note `/api/fleet` returns the snapshot WITHOUT config; the config comes from `/api/repos`). Fetch the repo's config from `/api/repos`:

```js
async function openRepoSettings(repo) {
  let cfg = {};
  try {
    const repos = await api("GET", "/api/repos");
    const found = (repos || []).find((r) => r.name === repo.name);
    cfg = (found && found.config) || {};
  } catch (e) { showBanner(String(e.message || e)); return; }

  const runningCount = (repo.agents || []).filter((a) => isActive(a.status)).length;
  const form = document.createElement("div");
  form.innerHTML = `<div class="form-title">settings — ${escapeHtml(repo.name)}</div>`;
  openModal(form);
  appendProviderFields(form, cfg);
  const err = document.createElement("div");
  err.className = "form-err";
  form.appendChild(err);
  const actions = document.createElement("div");
  actions.className = "form-actions";
  actions.innerHTML = `<button class="ctl-btn" id="rs-cancel">cancel</button><button class="ctl-btn primary" id="rs-save">save</button>`;
  form.appendChild(actions);
  form.querySelector("#rs-cancel").onclick = closeModal;
  const save = form.querySelector("#rs-save");
  save.onclick = async () => {
    if (runningCount > 0 &&
        !window.confirm(`Restart caliban for ${repo.name}? This stops ${runningCount} running agent(s).`)) {
      return;
    }
    save.disabled = true;
    try {
      await api("PUT", `/api/repos/${encodeURIComponent(repo.name)}/config`, readProviderConfig(form));
      closeModal();
      refreshFleet();
    } catch (e) {
      err.textContent = String(e.message || e);
      save.disabled = false;
    }
  };
}
```

- [ ] **Step 5: Build, rebuild-and-restart, verify served + behavior.**

```bash
cargo build --bin prosperod
node --check crates/api/dashboard/app.js && echo "js ok"
```
Expected: both clean. Rebuild-and-restart the daemon (Working notes), then:
```bash
curl -s http://127.0.0.1:7878/app.js | grep -c "function appendProviderFields"   # expect 1
curl -s http://127.0.0.1:7878/app.js | grep -c "function openRepoSettings"        # expect 1
# Backend round-trip the dashboard relies on:
curl -s -X POST http://127.0.0.1:7878/api/repos -H 'Content-Type: application/json' \
  -d '{"name":"cfgtest","root":"/Users/johnford2002/dev/caliban-ai/prospero","config":{"provider":"ollama","base_url":"http://192.168.1.240:11434"}}' -o /dev/null -w "add:[%{http_code}]\n"
curl -s http://127.0.0.1:7878/api/repos | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const r=JSON.parse(s).find(x=>x.name==="cfgtest");console.log("config:",JSON.stringify(r&&r.config))})'
curl -s -X PUT http://127.0.0.1:7878/api/repos/cfgtest/config -H 'Content-Type: application/json' \
  -d '{"provider":"ollama","base_url":"http://localhost:11434"}' -o /dev/null -w "put:[%{http_code}]\n"
curl -s -X DELETE http://127.0.0.1:7878/api/repos/cfgtest -o /dev/null -w "cleanup:[%{http_code}]\n"
```
Expected: `add:[201]`, config shows the provider/base_url, `put:[204]`, `cleanup:[204]`. (Browser: open the add-repo modal and a repo's ⚙ — confirm the provider dropdown, base-URL, API-key-env, and advanced env rows render and round-trip.)

- [ ] **Step 6: Commit.**
```bash
git add crates/api/dashboard/index.html crates/api/dashboard/app.js
git commit -m "feat(dashboard): provider config in add-repo + repo-settings modals"
```

---

## Task 8: Final verification

**Files:** none (verification only).

- [ ] **Step 1: Full workspace gates.**
```bash
cargo test --workspace --features prospero-core/testkit 2>&1 | tail -20
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings 2>&1 | tail -5
cargo fmt --all --check
```
Expected: tests green, clippy clean, fmt clean. (If `fmt --check` fails, run `cargo fmt --all` and amend the relevant commit.)

- [ ] **Step 2: Backward-compat smoke.** Confirm an existing `registry.json` (no `config`) still loads — covered by Task 1's test, but verify the live daemon started clean against the real `~/.local/share/prospero/registry.json` (Working notes restart, then `curl -s localhost:7878/api/repos`).

- [ ] **Step 3: Finish the branch.** Use **superpowers:finishing-a-development-branch** (both this feature and the dashboard controls land in the one PR).

---

## Self-review notes

- **Spec coverage:** data model (Task 1), `resolve_env` + mapping + precedence + api-key-ref + provider-only (Task 2), `EnsureConfig.env` + `default_env` + `ensure_config_for` (Task 3), `restart_caliband` via `Shutdown` + `set_repo_config` (Task 4), `AddRepoBody.config` + `PUT …/config` + config in repo responses (Task 5), `--default-env` (Task 6), add-repo + ⚙ settings UI with confirm-on-running-agents (Task 7), gates + back-compat (Task 8). All spec sections map to a task.
- **Type/name consistency:** `RepoProviderConfig` (provider/base_url/api_key_from_env/env) used identically across registry, `resolve_env`, DTOs, and JS (`provider`/`base_url`/`api_key_from_env`/`env`). `resolve_env(default_env, cfg, process_env)`, `ensure_config_for`, `set_repo_config_registry_only`, `set_repo_config`, `restart_caliband`, `add_repo_with_config`, `repo_config`, `cached_client_names`, `shutdowns` are each defined once and referenced consistently. Precedence (global < curated < raw) matches the spec.
- **Known limitation (documented):** `restart_caliband` waits (bounded) for the old socket to release before re-discovering; on timeout it proceeds with a warning. Acceptable for a single-operator localhost control plane.
