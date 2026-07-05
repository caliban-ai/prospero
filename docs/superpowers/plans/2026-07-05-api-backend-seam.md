# Backend-Agnostic API Seam Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reroute prosperod's API off `FleetManager` onto layered backend-agnostic seams so `K8sFleet` can serve `PROSPERO_FLEET=k8s` end-to-end.

**Architecture:** Extend `FleetProvider` with `snapshot`/`readiness`/`metrics`/`remove_agent` (control plane); add an optional `FleetAdmin` trait for the workspace-registry plane (LocalFleet-only); read observability (`history`/`stream`) from the shared `Store`+`EventBus` in `AppState`. Wire the prosperod k8s arm and drop the fail-fast guard.

**Tech Stack:** Rust (edition 2024), axum, tokio, async-trait, kube (k8s feature).

## Global Constraints

- **`hash16`/wire/persistence unchanged** — this is an API-wiring refactor, no protocol or storage change.
- **LocalFleet behavior is byte-identical** — existing `api_integration` + `e2e_smoke` tests must stay green; local still implements every plane.
- **k8s config/registry routes return `405`** when `admin: None` (not 500/501) — the operation is real but absent on this backend.
- **The k8s code is behind `feature = "k8s"`** (prospero-core/k8s); k8s-specific tasks/tests compile only with it.
- **Verification gate (CI mirror):** from repo root — `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets --features prospero-core/testkit,prospero-core/k8s -- -D warnings`; `cargo build --workspace --all-targets`; `cargo test --workspace --features prospero-core/testkit,prospero-core/k8s`.
- **Every commit subject ends with `(#76)`.**

---

### Task 1: Extend `FleetProvider` + `FleetManager` accessors + LocalFleet impl

**Files:**
- Modify: `crates/core/src/fleet_provider.rs` (trait + `LocalFleet` impl + tests)
- Modify: `crates/core/src/fleet.rs` (add `store()`/`bus()` accessors)

**Interfaces:**
- Consumes: `FleetManager::{snapshot, readiness, metrics, rm_agent, subscribe, history}` (existing), `crate::model::{FleetSnapshot, Readiness, AgentId, DrainPolicy}`, `crate::metrics::MetricsSnapshot`, `crate::store::Store`, `crate::bus::EventBus`.
- Produces:
  - `FleetProvider` gains: `async fn remove_agent(&self, id: &AgentId, force: bool) -> Result<()>`, `async fn snapshot(&self) -> FleetSnapshot`, `async fn readiness(&self) -> Readiness`, `fn metrics(&self) -> MetricsSnapshot`.
  - `FleetManager::store(&self) -> Arc<dyn Store>` and `FleetManager::bus(&self) -> Arc<dyn EventBus>`.

- [ ] **Step 1: Add `store()`/`bus()` to `FleetManager`.** In `fleet.rs`, near `subscribe` (~line 474), add:
```rust
/// The shared event store (observability reads route here, not through the
/// fleet backend — see prospero #76).
#[must_use]
pub fn store(&self) -> std::sync::Arc<dyn crate::store::Store> {
    self.inner.emitter.store.clone()
}

/// The shared event bus (SSE subscribe routes here).
#[must_use]
pub fn bus(&self) -> std::sync::Arc<dyn crate::bus::EventBus> {
    self.inner.emitter.bus.clone()
}
```
(Confirm `emitter.store`/`emitter.bus` are `Arc<dyn Store>`/`Arc<dyn EventBus>`; they are — the `Emitter` struct holds them.)

- [ ] **Step 2: Write the failing LocalFleet trait test.** In `fleet_provider.rs`'s `local_fleet_tests`:
```rust
#[tokio::test]
async fn local_fleet_snapshot_readiness_metrics_via_trait() {
    let (provider, _fake, _dir) = setup().await;
    let p: &dyn FleetProvider = &provider;
    // snapshot() matches the manager's own snapshot (same underlying state).
    let snap = p.snapshot().await;
    assert!(snap.workspaces.iter().any(|w| w.name == "repo-a"));
    // readiness() and metrics() are delegations that don't panic and are shaped.
    let _ = p.readiness().await;
    let _ = p.metrics();
}
```

- [ ] **Step 3: Extend the trait.** In `fleet_provider.rs`, add the four methods to `trait FleetProvider` (signatures above), with doc comments explaining local vs k8s semantics.

- [ ] **Step 4: Implement for `LocalFleet`.** In the `impl FleetProvider for LocalFleet` block:
```rust
async fn remove_agent(&self, id: &AgentId, force: bool) -> Result<()> {
    self.inner.rm_agent(id.as_str(), force).await
}
async fn snapshot(&self) -> crate::model::FleetSnapshot {
    self.inner.snapshot().await
}
async fn readiness(&self) -> crate::model::Readiness {
    self.inner.readiness().await
}
fn metrics(&self) -> crate::metrics::MetricsSnapshot {
    self.inner.metrics()
}
```

- [ ] **Step 5: Run tests.** `cargo test -p prospero-core --features testkit fleet_provider 2>&1 | tail` → PASS (existing + new).

- [ ] **Step 6: Commit.**
```bash
git add crates/core/src/fleet_provider.rs crates/core/src/fleet.rs
git commit -m "feat(core): extend FleetProvider (snapshot/readiness/metrics/remove_agent) + manager store/bus accessors (#76)"
```

---

### Task 2: `FleetAdmin` trait + LocalFleet impl

**Files:**
- Modify: `crates/core/src/fleet_provider.rs` (trait + `LocalFleet` impl + test)
- Modify: `crates/core/src/lib.rs` (re-export `FleetAdmin`)

**Interfaces:**
- Consumes: `FleetManager::{add_workspace_with_config, remove_workspace? , set_repo_config}` — NOTE the manager's methods are `add_workspace_with_config`, `remove_repo`, `set_repo_config` (kept names from #72). Use those.
- Produces: `pub trait FleetAdmin: Send + Sync` with `add_workspace(name: String, root: PathBuf, config: RepoProviderConfig)`, `remove_workspace(name: &str) -> Result<bool>`, `set_workspace_config(name: &str, config: RepoProviderConfig) -> Result<bool>`. `LocalFleet: FleetAdmin`.

- [ ] **Step 1: Write the failing test.** In `local_fleet_tests`:
```rust
#[tokio::test]
async fn local_fleet_admin_add_and_remove_workspace() {
    let (provider, _fake, dir) = setup().await;
    let admin: &dyn FleetAdmin = &provider;
    let root = dir.path().join("repo-b");
    std::fs::create_dir_all(&root).unwrap();
    admin.add_workspace("repo-b".into(), root, Default::default()).await.unwrap();
    assert!(provider.snapshot().await.workspaces.iter().any(|w| w.name == "repo-b"));
    assert!(admin.remove_workspace("repo-b").await.unwrap());
}
```

- [ ] **Step 2: Add the trait + impl.** In `fleet_provider.rs`:
```rust
use std::path::PathBuf;
use crate::registry::RepoProviderConfig;

/// The workspace-registry / provider-config plane. A prospero concept
/// (`Registry` of managed workspaces) — `LocalFleet` implements it; k8s has no
/// analogue (workspaces are `CalibanTask`/namespace-driven), so `K8sFleet` does
/// NOT implement it and the API returns 405 for these routes under k8s. (#76)
#[async_trait]
pub trait FleetAdmin: Send + Sync {
    async fn add_workspace(&self, name: String, root: PathBuf, config: RepoProviderConfig) -> Result<()>;
    async fn remove_workspace(&self, name: &str) -> Result<bool>;
    async fn set_workspace_config(&self, name: &str, config: RepoProviderConfig) -> Result<bool>;
}

#[async_trait]
impl FleetAdmin for LocalFleet {
    async fn add_workspace(&self, name: String, root: PathBuf, config: RepoProviderConfig) -> Result<()> {
        self.inner.add_workspace_with_config(name, root, config).await
    }
    async fn remove_workspace(&self, name: &str) -> Result<bool> {
        self.inner.remove_repo(name).await
    }
    async fn set_workspace_config(&self, name: &str, config: RepoProviderConfig) -> Result<bool> {
        Ok(self.inner.set_repo_config(name, config).await)
    }
}
```
(Confirm signatures: `remove_repo` returns `Result<bool>`; `set_repo_config` returns `bool` (wrap in `Ok`). Adjust to the actual return types found in `fleet.rs`.)

- [ ] **Step 3: Re-export.** In `lib.rs`, add `FleetAdmin` to the `fleet_provider` re-export line: `pub use fleet_provider::{FleetAdmin, FleetProvider, LocalFleet};`.

- [ ] **Step 4: Run tests.** `cargo test -p prospero-core --features testkit fleet_provider 2>&1 | tail` → PASS.

- [ ] **Step 5: Commit.**
```bash
git add crates/core/src/fleet_provider.rs crates/core/src/lib.rs
git commit -m "feat(core): FleetAdmin seam (workspace registry) + LocalFleet impl (#76)"
```

---

### Task 3: Implement the new `FleetProvider` methods for `K8sFleet`

**Files:**
- Modify: `crates/core/src/k8s/fleet.rs` (impl + tests)

**Interfaces:**
- Consumes: `CalibanTaskApi::{list, delete}`, `agent_from_task`, `phase_to_status`; the `store`/`bus` the fleet already holds (`self.emitter`).
- Produces: `K8sFleet` implements the four new `FleetProvider` methods.

- [ ] **Step 1: Write failing tests (over `FakeApi`).** In `k8s/fleet.rs` tests:
```rust
#[tokio::test]
async fn k8s_snapshot_lists_calibantasks_as_agents() {
    let fleet = /* build K8sFleet over FakeApi with 2 applied CalibanTasks, one Running */;
    let snap = fleet.snapshot().await;
    let agents: Vec<_> = snap.workspaces.iter().flat_map(|w| &w.agents).collect();
    assert_eq!(agents.len(), 2);
}

#[tokio::test]
async fn k8s_remove_agent_deletes_the_cr() {
    let fleet = /* build over FakeApi with one CalibanTask "a1" */;
    fleet.remove_agent(&AgentId::from("a1"), true).await.unwrap();
    assert!(fleet.snapshot().await.workspaces.iter().all(|w| w.agents.is_empty()));
}

#[tokio::test]
async fn k8s_readiness_true_when_api_and_store_healthy() {
    let fleet = /* build over FakeApi + JsonlStore */;
    assert!(fleet.readiness().await.store_writable);
}
```
(Reuse the existing test-construction helpers in `k8s/fleet.rs` tests — grep for how the other tests build a `K8sFleet` over `FakeApi` + bus + store, and mirror it.)

- [ ] **Step 2: Implement.** In the `impl<A: CalibanTaskApi> FleetProvider for K8sFleet<A>` block (add to it), matching the exact generic bounds already on the block:
```rust
async fn remove_agent(&self, id: &AgentId, _force: bool) -> Result<()> {
    self.api.delete(id.as_str()).await
}

async fn snapshot(&self) -> crate::model::FleetSnapshot {
    let tasks = self.api.list().await.unwrap_or_default();
    let agents: Vec<crate::model::Agent> = tasks.iter().map(agent_from_task).collect();
    // One synthetic workspace for the namespace; k8s has no prospero registry.
    let ws = crate::model::Workspace {
        name: self.namespace_label(),   // see helper below
        root: std::path::PathBuf::new(),
        sources: Vec::new(),
        health: crate::model::WorkspaceHealth::Healthy,
        config: crate::registry::RepoProviderConfig::default(),
        agents,
    };
    crate::model::FleetSnapshot { host: self.namespace_label(), workspaces: vec![ws] }
}

async fn readiness(&self) -> crate::model::Readiness {
    let api_ok = self.api.list().await.is_ok();
    let store_writable = self.emitter.store_writable().await;   // or a store health probe
    crate::model::Readiness {
        store_writable,
        // k8s has no per-workspace poll health; report the single namespace ws.
        workspaces_total: 1,
        workspaces_healthy: if api_ok { 1 } else { 0 },
        workspaces_unreachable: if api_ok { 0 } else { 1 },
    }
}

fn metrics(&self) -> crate::metrics::MetricsSnapshot {
    self.emitter.metrics_snapshot()   // or construct a MetricsSnapshot from the fleet's counters
}
```
**Field/method reconciliation (do at impl time):**
- `Readiness`'s real fields — grep `struct Readiness` in `model.rs`; #72 renamed `repos_*`→`workspaces_*`? Confirm the exact names and use them (the plan assumes `workspaces_total/healthy/unreachable` + `store_writable`; adjust if they're still `repos_*`).
- `namespace_label()` — a small helper returning the fleet's namespace (grep how `K8sFleet` stores its namespace; `KubeTaskApi::new(client, namespace)` holds it — expose it, or store the namespace on `K8sFleet`). If not readily available, use a constant `"k8s"`.
- `store_writable`/`metrics_snapshot` — the `Emitter` (shared with `FleetManager`) likely already has these; grep `impl Emitter` in `fleet.rs`. If `metrics` isn't tracked in K8sFleet's emitter, return `MetricsSnapshot::default()`-shaped values (grep its fields).

- [ ] **Step 3: Run tests.** `cargo test -p prospero-core --features testkit,k8s k8s::fleet 2>&1 | tail` → PASS.

- [ ] **Step 4: Commit.**
```bash
git add crates/core/src/k8s/fleet.rs
git commit -m "feat(core): K8sFleet snapshot/readiness/metrics/remove_agent (list/delete CRs) (#76)"
```

---

### Task 4: Rewire the API onto the seams

**Files:**
- Modify: `crates/api/src/lib.rs` (`AppState`, `router`), `crates/api/src/handlers.rs`, `crates/api/src/sse.rs`, `crates/api/src/error.rs`

**Interfaces:**
- Consumes: `Arc<dyn FleetProvider>`, `Option<Arc<dyn FleetAdmin>>`, `Arc<dyn Store>`, `Arc<dyn EventBus>`, `AgentId`, `DrainPolicy`.
- Produces: `pub fn router(fleet: Arc<dyn FleetProvider>, admin: Option<Arc<dyn FleetAdmin>>, store: Arc<dyn Store>, bus: Arc<dyn EventBus>) -> Router`; `AppState { fleet, admin, store, bus }`.

- [ ] **Step 1: Restructure `AppState` + `router`.** In `lib.rs`:
```rust
#[derive(Clone)]
pub struct AppState {
    pub fleet: std::sync::Arc<dyn prospero_core::FleetProvider>,
    pub admin: Option<std::sync::Arc<dyn prospero_core::FleetAdmin>>,
    pub store: std::sync::Arc<dyn prospero_core::store::Store>,
    pub bus:   std::sync::Arc<dyn prospero_core::bus::EventBus>,
}

pub fn router(
    fleet: std::sync::Arc<dyn prospero_core::FleetProvider>,
    admin: Option<std::sync::Arc<dyn prospero_core::FleetAdmin>>,
    store: std::sync::Arc<dyn prospero_core::store::Store>,
    bus: std::sync::Arc<dyn prospero_core::bus::EventBus>,
) -> Router {
    let state = AppState { fleet, admin, store, bus };
    Router::new() /* ...unchanged routes... */
}
```

- [ ] **Step 2: Reroute handlers.** In `handlers.rs`, per the spec's routing table:
  - `get_fleet`/`get_workspaces`/`get_workspace_agents`/`get_agent`: `st.fleet.snapshot().await`.
  - `get_metrics`: `st.fleet.metrics()`. `readyz`: `st.fleet.readiness().await`.
  - `kill_agent`: `st.fleet.stop_agent(&AgentId::from(id.as_str()), DrainPolicy::Kill).await?`.
  - `respawn_agent`: `let new = st.fleet.restart_agent(&AgentId::from(id.as_str())).await?;` (RespawnedResponse { agent_id: new.to_string() }).
  - `delete_agent` (rm): `st.fleet.remove_agent(&AgentId::from(id.as_str()), false).await?`.
  - `get_agent_events`: `st.store.replay(&stream_key_for(id), q.from).await` — use `prospero_core::event::stream_key_for(&"", &id)` (agent key = the id). Confirm the key rule: agent events key on the agent id (see `stream_key_for`).
  - The 3 admin routes (`add_workspace`, `set_workspace_config`, `delete_workspace`):
    ```rust
    let admin = st.admin.as_ref().ok_or(ApiError::unsupported_on_backend())?;
    admin.add_workspace(body.name, body.root.into(), body.config).await?;
    ```

- [ ] **Step 3: `ApiError` 405 mapping.** In `error.rs`, add:
```rust
impl ApiError {
    /// The requested operation exists but is not supported by the active fleet
    /// backend (e.g. workspace registry ops under k8s). Maps to 405. (#76)
    pub fn unsupported_on_backend() -> Self { /* construct an ApiError whose IntoResponse is 405 with a JSON `error` body */ }
}
```
(Follow the existing `ApiError` shape — grep `enum ApiError`/`impl IntoResponse for ApiError`; add a `MethodNotAllowed(String)` variant mapping to `StatusCode::METHOD_NOT_ALLOWED`.)

- [ ] **Step 4: SSE over shared store/bus.** In `sse.rs`: `st.manager.subscribe(&id)` → `st.bus.subscribe(&id)`; `st.manager.history(...)` → `st.store.replay(&id, q.from)` (agent key = id); `Tailer::new(id, last_delivered, st.manager.clone())` → pass a `HistorySource` backed by `st.store.clone()`. **Check `HistorySource`** (`sse/tail.rs`): it's already a trait; either `Arc<dyn Store>` implements it or add a thin `StoreHistory(Arc<dyn Store>)` adapter implementing `HistorySource` via `store.replay`. Implement whichever the existing `HistorySource` contract wants (grep `trait HistorySource`).

- [ ] **Step 5: Build the api crate + fix call sites.**
Run: `cargo build -p prospero-api --features prospero-core/testkit,prospero-core/k8s 2>&1 | rg "error" | head` → fix until clean.

- [ ] **Step 6: Update api tests' `router(...)` calls.** `api_integration.rs` + any `router(` callers now pass `(Arc::new(LocalFleet), Some(Arc::new(LocalFleet)), store, bus)`. Add a small test helper. Run `cargo test -p prospero-api --features prospero-core/testkit,prospero-core/k8s` → PASS (local behavior unchanged).

- [ ] **Step 7: Commit.**
```bash
git add crates/api/src
git commit -m "refactor(api): route handlers through FleetProvider/FleetAdmin + shared store/bus (#76)"
```

---

### Task 5: Wire prosperod arms + drop fail-fast

**Files:**
- Modify: `crates/daemon/src/main.rs`

- [ ] **Step 1: local arm.** Where `let app = prospero_api::router(manager.clone(), fleet);` is built, replace with:
```rust
let local = LocalFleet::new(manager.clone());
let fleet: Arc<dyn prospero_core::FleetProvider> = Arc::new(local.clone());
let admin: Option<Arc<dyn prospero_core::FleetAdmin>> = Some(Arc::new(local));
let app = prospero_api::router(fleet, admin, manager.store(), manager.bus());
```
(For the `local` backend. The poll loop `manager.clone().run()` and retention stay.)

- [ ] **Step 2: k8s arm (behind `feature = "k8s"`).** Add a branch on `args.fleet_backend`: build a `kube::Client` (`kube::Client::try_default().await?`), a namespace (env `PROSPERO_K8S_NAMESPACE` default `"default"`), a `KubeTaskApi::new(client, &ns)`, and `K8sFleet::new(api, bus, store)` where `store`/`bus` are the same ones built for the topology. Then:
```rust
let k8s = K8sFleet::new(api, bus.clone(), store.clone());
tokio::spawn(/* k8s watch/reconcile loop if K8sFleet exposes one; else none */);
let fleet: Arc<dyn prospero_core::FleetProvider> = Arc::new(k8s);
let app = prospero_api::router(fleet, None, store, bus);
```
(Grep `K8sFleet` for any background loop to spawn — mirror how the local arm spawns `manager.run()`. If K8sFleet needs no background loop, skip it.)

- [ ] **Step 3: Drop the fail-fast.** Remove the `check_fleet_backend_servable(args.fleet_backend)?;` call for the k8s-servable case. **Keep** the "not built with k8s feature" arm: under `#[cfg(not(feature = "k8s"))]`, `PROSPERO_FLEET=k8s` must still error clearly ("rebuild with --features k8s"). Update the doc comments that describe the fail-fast.

- [ ] **Step 4: Build both feature sets.**
```bash
cargo build -p prospero-daemon 2>&1 | tail -1                       # local-only, no k8s
cargo build -p prospero-daemon --features k8s 2>&1 | tail -1        # k8s arm compiles
```
Both succeed.

- [ ] **Step 5: Commit.**
```bash
git add crates/daemon/src/main.rs
git commit -m "feat(daemon): wire K8sFleet into the k8s arm; drop the serve fail-fast (#76)"
```

---

### Task 6: k8s-shaped API integration test + full gate

**Files:**
- Create/modify: `crates/api/tests/k8s_backend.rs` (new integration test, `#[cfg(feature = "prospero-core/k8s")]`-gated via a crate feature or `cfg`)

**Interfaces:** Consumes `router(...)`, a `K8sFleet` over `k8s::fake::FakeApi`, `tower::ServiceExt::oneshot`.

- [ ] **Step 1: Write the test.** Build a k8s-shaped `AppState` and assert it serves + 405s:
```rust
// fleet = K8sFleet over FakeApi (2 CalibanTasks); admin = None; store+bus shared.
let app = prospero_api::router(fleet, None, store.clone(), bus.clone());

// GET /api/fleet serves the CR-derived snapshot.
let res = app.clone().oneshot(get("/api/fleet")).await.unwrap();
assert_eq!(res.status(), 200);
let body = /* parse */;
assert!(body["workspaces"][0]["agents"].as_array().unwrap().len() >= 1);

// POST /api/workspaces -> 405 (no registry under k8s).
let res = app.clone().oneshot(post_json("/api/workspaces", json!({"name":"x","root":"/x"}))).await.unwrap();
assert_eq!(res.status(), 405);

// DELETE /api/workspaces/{n} and PUT .../config also 405.
```
(Mirror `api_integration.rs`'s request helpers. Build the `FakeApi` + `K8sFleet` the way `k8s/fleet.rs` tests do.)

- [ ] **Step 2: Run it.** `cargo test -p prospero-api --features prospero-core/testkit,prospero-core/k8s k8s_backend 2>&1 | tail` → PASS.

- [ ] **Step 3: Full gate (CI mirror).**
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit,prospero-core/k8s -- -D warnings
cargo build --workspace --all-targets
cargo test --workspace --features prospero-core/testkit,prospero-core/k8s
```
All green.

- [ ] **Step 4: Commit.**
```bash
git add crates/api/tests
git commit -m "test(api): K8sFleet serves the API; workspace-registry routes 405 (#76)"
```

---

## Self-Review

**Spec coverage:**
- Control plane extension (snapshot/readiness/metrics/remove_agent) → Task 1 (LocalFleet) + Task 3 (K8sFleet). ✓
- Observability from shared Store+EventBus → Task 1 (accessors) + Task 4 Step 4 (sse) + Step 2 (events). ✓
- `FleetAdmin` optional seam → Task 2 (trait+LocalFleet), Task 4 (Option in AppState + 405), Task 5 (admin: None for k8s). ✓
- API rewiring table (all ~12 sites) → Task 4 Step 2. ✓
- prosperod k8s arm + drop fail-fast → Task 5. ✓
- Error handling (405) → Task 4 Step 3. ✓
- Testing (trait conformance, k8s-shaped API, local unchanged, daemon) → Tasks 1/3/4/6. ✓

**Placeholder scan:** Tasks 3 & 4 carry explicit "reconcile at impl time" notes (exact `Readiness`/`MetricsSnapshot` field names, `HistorySource` contract, `namespace_label`) rather than guessed signatures — these are grep-and-match instructions with a concrete fallback stated, not vague TODOs; every method body and the fallback are given. All other steps have complete code.

**Type consistency:** `FleetProvider::{snapshot→FleetSnapshot, readiness→Readiness, metrics→MetricsSnapshot, remove_agent}`, `FleetAdmin::{add_workspace, remove_workspace, set_workspace_config}`, `AppState{fleet,admin,store,bus}`, `router(fleet,admin,store,bus)`, `ApiError::unsupported_on_backend`→405 used consistently across Tasks 1–6. The manager's kept method names from #72 (`add_workspace_with_config`, `remove_repo`, `set_repo_config`) are called as-is in Task 2.

**Known reconciliations (do at impl, not guesses):** `Readiness` field names (`repos_*` vs `workspaces_*` after #72), `MetricsSnapshot` fields, `HistorySource` trait shape, K8sFleet namespace accessor, `Emitter` store-writable/metrics helpers. Each task step names the grep to run and a concrete fallback.
