# Design: backend-agnostic API seam — wire K8sFleet into prosperod — prospero #76

- **Date:** 2026-07-05
- **Issue:** caliban-ai/prospero#76 · follow-up to #64 (K8sFleet) · epic #274 (k8s) · relates ADR 0006 (layered crates), ADR 0008 (K8sFleet)

## Problem

`K8sFleet` (#64) is a complete `FleetProvider` library — ensure/watch/stop/restart
over `CalibanTask` CRs, conformance-passing, session-plane over the #71 network
transport — but it cannot **serve** through `prosperod`. The API's `AppState`/
`router` are hardcoded to `FleetManager` + `LocalFleet` (`crates/api/src/lib.rs`),
and ~12 handler sites call `st.manager.*` directly: `snapshot`, `history`,
`subscribe`/stream, `kill_agent`, `respawn_agent`, `rm_agent`, `metrics`,
`readiness`, `set_repo_config`, `remove_repo`. `PROSPERO_FLEET=k8s` fails fast by
design (`check_fleet_backend_servable`, ADR 0008 §5).

## Decision: layered seams

Route the API through three backend-agnostic planes, not one god-trait:

1. **Control plane → `FleetProvider`** (extend the existing trait).
2. **Observability plane → shared `Store` + `EventBus`**, read directly from
   `AppState` (both backends already emit there — no backend method needed).
3. **Registry/config plane → optional `FleetAdmin`** companion trait
   (LocalFleet-only; the prospero-registry concept has no k8s analogue).

Rejected — **Fat `FleetProvider`** (all ops on one trait): forces K8sFleet into
runtime `Err("unsupported")` stubs for config/registry ops it can't honor, and
duplicates the shared-store observability across backends. **Minimal serve-path**
(defer the whole admin plane): smaller, but leaves `AppState` coupled to a
concrete `FleetManager` and pushes local↔k8s parity into a follow-up.

## Architecture

### 1. Control — `FleetProvider` (`crates/core/src/fleet_provider.rs`)

```rust
#[async_trait]
pub trait FleetProvider: Send + Sync {
    // existing
    async fn ensure_agent(&self, spec: TaskSpec) -> Result<AgentHandle>;
    fn watch_fleet(&self) -> BoxStream<'static, FleetChange>;
    async fn stop_agent(&self, id: &AgentId, drain: DrainPolicy) -> Result<()>;   // ← kill
    async fn restart_agent(&self, id: &AgentId) -> Result<AgentId>;               // ← respawn
    // new
    async fn remove_agent(&self, id: &AgentId, force: bool) -> Result<()>;        // local: rm; k8s: delete CR
    async fn snapshot(&self) -> FleetSnapshot;                                    // local: in-mem; k8s: from watch state
    async fn readiness(&self) -> Readiness;                                       // local: store-writable+health; k8s: store+kube reachable
    fn metrics(&self) -> MetricsSnapshot;                                         // per-backend counters
}
```

- **LocalFleet** — `remove_agent`/`snapshot`/`readiness`/`metrics` delegate to its
  `FleetManager` (`rm_agent`/`snapshot`/`readiness`/`metrics`).
- **K8sFleet** —
  - `snapshot()`: **list the `CalibanTask`s** via its `CalibanTaskApi` and project
    each through the existing `agent_from_task` into a `FleetSnapshot` (list-on-
    demand, so it does not depend on internal watch state). Agents group under a
    single synthetic workspace named for the namespace (k8s has no prospero
    registry); `sources` empty.
  - `readiness()`: `store` writable **and** a light kube reachability probe (list
    `CalibanTask` with `limit=1`, or the api-server `/healthz` via the client).
    No per-workspace poll health.
  - `metrics()`: its own counters (agents observed, watch restarts) shaped as
    `MetricsSnapshot` so the `/api/metrics` DTO is unchanged.
  - `remove_agent(id, force)`: delete the `CalibanTask` CR named for the agent.

### 2. Observability — shared `Store` + `EventBus` (no backend method)

`history(id, from)` → `store.replay(stream_key, from)`; `sse` → `bus.subscribe` +
`Tailer`. `AppState` holds `Arc<dyn Store>` + `Arc<dyn EventBus>`. Both backends
already write events to the same store/bus (LocalFleet via `FleetManager`'s
emitter; K8sFleet via the `bus`+`store` it's constructed with), so the read path
is backend-independent. `Tailer` already takes what it needs; adjust its
construction to use the shared handles rather than `FleetManager`.

### 3. Registry/config — optional `FleetAdmin` (`crates/core/src/fleet_provider.rs`)

```rust
#[async_trait]
pub trait FleetAdmin: Send + Sync {
    async fn add_workspace(&self, name: String, root: PathBuf, config: RepoProviderConfig) -> Result<()>;
    async fn remove_workspace(&self, name: &str) -> Result<bool>;
    async fn set_workspace_config(&self, name: &str, config: RepoProviderConfig) -> Result<bool>;
}
```

`LocalFleet` implements it (delegates to `FleetManager`). `K8sFleet` does **not**.

### 4. API rewiring (`crates/api`)

```rust
pub struct AppState {
    pub fleet: Arc<dyn FleetProvider>,
    pub admin: Option<Arc<dyn FleetAdmin>>,
    pub store: Arc<dyn Store>,
    pub bus:   Arc<dyn EventBus>,
}
pub fn router(
    fleet: Arc<dyn FleetProvider>,
    admin: Option<Arc<dyn FleetAdmin>>,
    store: Arc<dyn Store>,
    bus:   Arc<dyn EventBus>,
) -> Router { ... }
```

Handler routing:

| Route / handler | Was | Now |
|---|---|---|
| `GET /api/fleet`, `/api/workspaces`, `/api/workspaces/{n}/agents`, `GET /api/agents/{id}` | `manager.snapshot()` | `fleet.snapshot()` |
| `GET /api/agents/{id}/events` | `manager.history()` | `store.replay()` |
| `GET /api/agents/{id}/stream` (sse) | `manager.subscribe()/history()` | `bus`+`store` |
| `POST /api/agents/{id}/kill` | `manager.kill_agent()` | `fleet.stop_agent(Kill)` |
| `POST /api/agents/{id}/respawn` | `manager.respawn_agent()` | `fleet.restart_agent()` |
| `DELETE /api/agents/{id}` | `manager.rm_agent()` | `fleet.remove_agent()` |
| `POST /api/workspaces/{w}/agents` (spawn) | `fleet.ensure_agent()` | unchanged |
| `GET /api/metrics` | `manager.metrics()` | `fleet.metrics()` |
| `GET /readyz` | `manager.readiness()` | `fleet.readiness()` |
| `POST /api/workspaces` (add) | `manager.add_*` | `admin` or **405** |
| `DELETE /api/workspaces/{n}` (remove) | `manager.remove_repo()` | `admin` or **405** |
| `PUT /api/workspaces/{n}/config` | `manager.set_repo_config()` | `admin` or **405** |

### 5. prosperod (`crates/daemon/src/main.rs`)

- `local` arm: build `LocalFleet` (over `FleetManager`); pass it as
  `fleet: Arc::new(local.clone())` **and** `admin: Some(Arc::new(local))`; `store`/
  `bus` from the manager.
- `k8s` arm (behind `feature = "k8s"`): build `K8sFleet` from a `kube::Client` +
  namespace (env/kubeconfig config), a `store` + `bus` (Postgres/Jsonl store + the
  in-process or distributed bus, same as local); `admin: None`. **Remove**
  `check_fleet_backend_servable`'s k8s fail-fast (keep the "not built with k8s
  feature" arm).

## Error handling

- `admin: None` on a config/registry route → **`405 Method Not Allowed`** with a
  body explaining the backend has no workspace registry (k8s workspaces are
  CR/namespace-driven). Add `CoreError`/`ApiError` mapping or handle at the
  handler.
- Backend/store errors surface through the existing `ApiError` conversion
  unchanged.

## Testing strategy (TDD)

1. **Trait conformance** — both `LocalFleet` and `K8sFleet` (over `k8s::fake::FakeApi`)
   satisfy the extended `FleetProvider`: `snapshot`/`readiness`/`metrics`/
   `remove_agent` behave (K8sFleet snapshot reflects applied `CalibanTask`s;
   `remove_agent` deletes the CR; readiness true when fake api + store healthy).
2. **API over a k8s-shaped `AppState`** — `router(fleet = K8sFleet-over-fake,
   admin = None, store, bus)`:
   - `GET /api/fleet` and `GET /api/workspaces` serve the CR-derived snapshot;
   - spawn → kill → the agent disappears; `GET /api/agents/{id}/stream` tails the
     bus;
   - `POST /api/workspaces`, `DELETE /api/workspaces/{n}`, `PUT .../config` return
     **405**.
3. **Local unchanged** — existing `api_integration` + `e2e_smoke` stay green
   (LocalFleet implements both traits; `admin: Some`).
4. **prosperod** — a unit test that the `k8s` arm builds an `AppState` with
   `admin: None` and no longer fails fast (gated on `feature = "k8s"`).

## Consequences

- **Positive:** `PROSPERO_FLEET=k8s prosperod` serves the dashboard/API against a
  cluster end-to-end (the acceptance); the API depends on abstractions, not
  `FleetManager`; local↔k8s divergence (no k8s registry) is a type-level fact
  (`admin: Option`), surfaced as a clean 405, not a runtime backend error; the
  seam is ready for the remote backend (#1).
- **Negative:** `FleetProvider` grows by four methods and `AppState` carries four
  fields; K8sFleet must synthesize `snapshot`/`readiness`/`metrics` it didn't
  before. The dashboard's workspace-registration controls are inert (405) under
  k8s — correct, but a UI affordance for that is future work.
- **Deferred:** richer k8s-native config surface and session-plane hardening →
  **#77**; k8s-aware dashboard affordances → **#5**.
- **Revisit if:** a third backend (remote, #1) needs a different split, or k8s
  gains a real per-workspace config model.
