# De-flake `cli_drives_the_full_stack` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Close the registry read-after-write race (B) and make the e2e assertion resilient (A) so `cli_drives_the_full_stack` is deterministic under CI load.

**Architecture:** B — hold the async registry write lock across the config-store I/O in `set_repo_config_registry_only` (persist) and `refresh_registry_from_store` (list+replace), serializing them. A — the e2e polls `/api/workspaces` for eventual consistency.

**Tech Stack:** Rust 2024, tokio async `RwLock`, `#[async_trait]` ConfigStore, ureq (e2e).

## Global Constraints

- Gate `$TESTKIT` = `--features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s`.
- No behavior change beyond closing the race; `refresh` still converges from durable.
- No registry versioning (YAGNI).

---

### Task 1: B — close the registry race (TDD, deterministic test)

**Files:**
- Modify: `crates/core/src/fleet.rs` — `set_repo_config_registry_only`, `refresh_registry_from_store`, + test module.

**Interfaces:** no signature changes; internal locking only.

- [ ] **Step 1: Add the `SlowListConfigStore` double + failing regression test**

In `crates/core/src/fleet.rs` `#[cfg(test)] mod tests`, add:

```rust
// A ConfigStore whose `list_repos` snapshots state, THEN sleeps (opening the
// exact race window), returning the pre-sleep snapshot. Lets a test deterministically
// interleave a `set_config` inside a `refresh_registry_from_store`'s durable read.
struct SlowListConfigStore {
    repos: std::sync::Mutex<Vec<crate::registry::RegisteredWorkspace>>,
    read_delay: Duration,
}
impl SlowListConfigStore {
    fn new(read_delay: Duration) -> Self {
        Self { repos: std::sync::Mutex::new(Vec::new()), read_delay }
    }
}
#[async_trait::async_trait]
impl crate::config_store::ConfigStore for SlowListConfigStore {
    async fn list_repos(&self) -> Result<Vec<crate::registry::RegisteredWorkspace>> {
        let snapshot = self.repos.lock().unwrap().clone(); // read BEFORE the delay
        tokio::time::sleep(self.read_delay).await;         // window for a concurrent set_config
        Ok(snapshot)
    }
    async fn upsert_repo(&self, repo: &crate::registry::RegisteredWorkspace) -> Result<()> {
        let mut v = self.repos.lock().unwrap();
        if let Some(e) = v.iter_mut().find(|e| e.name == repo.name) { *e = repo.clone(); }
        else { v.push(repo.clone()); }
        Ok(())
    }
    async fn delete_repo(&self, name: &str) -> Result<bool> {
        let mut v = self.repos.lock().unwrap();
        let before = v.len();
        v.retain(|e| e.name != name);
        Ok(v.len() != before)
    }
}

#[tokio::test]
async fn concurrent_refresh_does_not_clobber_a_just_set_config() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = FleetConfig::new("local", dir.path());
    config.ensure.autostart = false;
    let root = dir.path().join("r");
    std::fs::create_dir_all(&root).unwrap();

    let store: Arc<dyn crate::store::Store> =
        Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
    let cfg_store: Arc<dyn crate::config_store::ConfigStore> =
        Arc::new(SlowListConfigStore::new(Duration::from_millis(150)));
    let mgr = FleetManager::with_config_store(config, store, cfg_store)
        .await
        .unwrap();
    mgr.add_repo("r", &root).await.unwrap(); // registry + durable now hold r with config {}

    // Kick off a poll-style refresh; with the fix it holds the registry lock
    // across the slow list_repos, so the set_config below serializes after it.
    let m = mgr.clone();
    let refresh = tokio::spawn(async move { m.refresh_registry_from_store().await });

    // Let refresh get into list_repos, then set the config mid-flight.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let cfg = crate::registry::RepoProviderConfig {
        provider: Some("ollama".to_string()),
        ..Default::default()
    };
    mgr.set_repo_config_registry_only("r", cfg).await.unwrap();
    refresh.await.unwrap();

    // The just-set config must survive the concurrent refresh.
    let snap = mgr.snapshot().await;
    let repo = snap.workspaces.iter().find(|w| w.name == "r").unwrap();
    assert_eq!(
        repo.config.provider.as_deref(),
        Some("ollama"),
        "refresh clobbered a concurrent set_config back to durable; got {:?}",
        repo.config
    );
}
```

- [ ] **Step 2: Run to verify it FAILS (proves the race + the test's teeth)**

Run: `cargo test -p prospero-core --features testkit concurrent_refresh_does_not_clobber -- --nocapture`
Expected: FAIL — `provider` is `None` (clobbered). If it *passes* pre-fix, increase `read_delay` / adjust the 30ms so `set_config` lands inside the read window.

- [ ] **Step 3: Fix `set_repo_config_registry_only` — persist under the lock**

Replace its body so the registry lock is held across the durable upsert:

```rust
    pub async fn set_repo_config_registry_only(
        &self,
        repo: &str,
        config: crate::registry::RepoProviderConfig,
    ) -> Result<()> {
        // Hold the registry write lock across the durable upsert so a concurrent
        // `refresh_registry_from_store` (poll loop) cannot read stale durable
        // state and clobber this write. The registry RwLock is async, so the
        // config-store await under the guard is sound (prospero #85).
        let mut reg = self.inner.registry.write().await;
        if !reg.set_config(repo, config) {
            return Err(CoreError::WorkspaceNotFound(repo.to_string()));
        }
        let record = reg
            .get(repo)
            .cloned()
            .expect("repo exists after successful set_config");
        self.inner.config_store.upsert_repo(&record).await?;
        Ok(())
    }
```

- [ ] **Step 4: Fix `refresh_registry_from_store` — read under the lock**

Move `list_repos()` inside the registry write guard:

```rust
    async fn refresh_registry_from_store(&self) {
        // Read durable state and wholesale-replace the in-memory registry
        // atomically under the write lock, so a concurrent
        // `set_repo_config_registry_only` can't interleave and get clobbered by a
        // stale durable read (prospero #85). Costs one config-store read per poll
        // held under the registry lock — sub-ms standalone, a few ms clustered.
        let durable = {
            let mut reg = self.inner.registry.write().await;
            let durable = match self.inner.config_store.list_repos().await {
                Ok(repos) => repos,
                Err(e) => {
                    tracing::warn!(
                        target: "prospero_fleet", error = %e,
                        "registry refresh from config store failed; keeping cached view"
                    );
                    return;
                }
            };
            reg.workspaces = durable.clone();
            durable
        };
        let mut snap = self.inner.snapshot.write().await;
        snap.workspaces
            .retain(|r| durable.iter().any(|d| d.name == r.name));
        for d in &durable {
            if !snap.workspaces.iter().any(|r| r.name == d.name) {
                snap.workspaces.push(Workspace {
                    name: d.name.clone(),
                    root: d.root.clone(),
                    sources: crate::caliband::sources::discover_sources(&d.root),
                    health: WorkspaceHealth::Healthy,
                    config: d.config.clone(),
                    agents: Vec::new(),
                });
            }
        }
    }
```

- [ ] **Step 5: Run to verify it PASSES + no regressions**

Run: `cargo test -p prospero-core --features testkit concurrent_refresh_does_not_clobber` → PASS.
Then `cargo test -p prospero-core --features testkit` → whole core suite green (esp. any existing config/refresh tests).

- [ ] **Step 6: Commit**

```bash
git add crates/core/src/fleet.rs
git commit -m "fix(core): serialize set_config vs poll registry refresh to close config clobber (#85)"
```

---

### Task 2: A — resilient e2e assertion

**Files:**
- Modify: `crates/cli/tests/e2e_smoke.rs` (config-read assertion, ~line 162-175).

- [ ] **Step 1: Replace the single read with a bounded poll**

Swap the immediate `GET /api/workspaces` + assert for a retry loop that polls
until `config.provider == "ollama"` (deadline ~5 s, ~100 ms interval), then
assert both `provider` and `base_url`. Keep using `ureq` via
`spawn_blocking`. Concretely, replace the block that binds `repos` and the two
`assert_eq!`s with:

```rust
    let repos_url = format!("{base}/api/workspaces");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let cfg = loop {
        let url = repos_url.clone();
        let repos: serde_json::Value =
            tokio::task::spawn_blocking(move || ureq::get(&url).call().unwrap().into_json())
                .await
                .unwrap()
                .unwrap();
        let cfg = repos.as_array().unwrap()[0]["config"].clone();
        if cfg["provider"].as_str() == Some("ollama") {
            break cfg;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "provider config never became visible via /api/workspaces: {repos}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    assert_eq!(cfg["provider"].as_str(), Some("ollama"));
    assert_eq!(cfg["base_url"].as_str(), Some("http://h:11434"));
```

- [ ] **Step 2: Run the e2e**

Run: `cargo test -p prospero-cli --test e2e_smoke --features prospero-core/testkit cli_drives_the_full_stack` → PASS.

- [ ] **Step 3: Stress it under load (confirm deterministic)**

Build once, then run the test binary ~20× under CPU saturation (`yes` background procs); expect 0 failures.

- [ ] **Step 4: Commit**

```bash
git add crates/cli/tests/e2e_smoke.rs
git commit -m "test(cli): poll /api/workspaces for eventual consistency in full-stack e2e (#85)"
```

---

## Self-Review

- **Spec coverage:** B lock-serialization (Task 1.3/1.4) + deterministic test (1.1), A poll (Task 2). Covered.
- **Placeholders:** none — full code for the double, test, both fixes, and the e2e loop.
- **Type consistency:** `RegisteredWorkspace{name,root,config}`, `RepoProviderConfig{provider,...}`, `with_config_store(config, store, Arc<dyn ConfigStore>)`, `refresh_registry_from_store(&self)` (private, reachable in-module), `snapshot().workspaces[].config.provider` all match the code read during design.
- **Risk:** holding the registry lock across config-store I/O — checked for deadlock (config stores never re-enter the registry lock) and contention (bounded, documented). Standalone path (the flake's context) is sub-ms.
