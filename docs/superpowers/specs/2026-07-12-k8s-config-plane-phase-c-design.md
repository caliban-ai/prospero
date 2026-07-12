# K8s Config Plane — Phase C (core + API) Design

**Ticket:** [#142](https://github.com/caliban-ai/prospero/issues/142) · parent epic [#141](https://github.com/caliban-ai/prospero/issues/141)
**Umbrella design:** SilverBullet `Agent/Memory/Long-Term/References/Prospero-K8s-Config-Plane-Design` (change set **C**), brainstormed & approved with the maintainer 2026-07-11.
**Frozen contract:** caliban-operator **#11** (merged 2026-07-12) — `deploy/crd/{workspace,calibantask}.yaml` + `deploy/samples/{workspace,calibantask}.yaml`.

**Goal:** Make prospero's k8s backend a real *editor* of the operator-owned `Workspace`/`CalibanTask` CRDs, removing the `405 Method Not Allowed` the dashboard hits when configuring a workspace under `PROSPERO_FLEET=k8s`.

---

## Global constraints (verbatim from the frozen contract / ADRs)

- **Couple only through the CRD's serialized form** (ADR 0008 §1, invariant "no caliban crate dependency"). Prospero holds a *minimal client-side mirror*; golden tests pin it against the operator's committed sample CRs.
- **CRD group/version:** `caliban.caliban-ai.dev/v1alpha1`, namespaced, status subresource.
- **camelCase serde** on every spec/status field (matches the operator's `#[serde(rename_all = "camelCase")]`).
- **No `secrets` RBAC for prospero.** Prospero references Secrets by name only (`credentialsRef {secretName, key}`); the operator is the sole Secret reader/validator.
- **Pre-v1, no back-compat** (decision 7): inline `CalibanTaskSpec.workspace` is *removed*, not deprecated.
- **Layered crates** (ADR 0006): CRD mirrors + `WorkspaceApi` live in `core`; endpoints in `api`; no web framework in `core`'s public API.

---

## The breaking change (Task 1, lands first)

The operator replaced inline `CalibanTaskSpec.workspace` with `workspaceRef`. Prospero's current mirror (`crates/core/src/k8s/crd.rs`) and `build_calibantask`/`agent_from_task` (`crates/core/src/k8s/fleet.rs`) still use the inline form and will **fail to deserialize a real CR** post-#11. This migration is the foundation for everything else and lands as the first task.

| Prospero today | Frozen contract |
|---|---|
| `CalibanTaskSpec { workspace: Workspace, task, … }` | `CalibanTaskSpec { workspace_ref: WorkspaceRef, provider_ref: Option<String>, task, model?, isolation?, tools?, … }` |
| `agent_from_task` reads `task.spec.workspace.sources.first()` | reads `task.status.resolved_workspace.sources` (pinned at admission), falling back to the CR name |
| `build_calibantask` sets inline `workspace: Workspace { sources }` | sets `workspace_ref: WorkspaceRef { name }` + optional `provider_ref` |

`agent_from_task`'s `repo`/`workspace` label now derives from `status.resolvedWorkspace.sources` (the operator pins it at admission) when present, else the CR name — the inline `spec.workspace.sources` it reads today no longer exists.

---

## Components & file structure

### 1. CRD mirrors — `crates/core/src/k8s/crd.rs`

Minimal mirrors (only the fields prospero reads/writes), camelCase, `#[serde(default, skip_serializing_if…)]` on every optional, matching the operator's serde shape exactly.

- **`Workspace`** (new): `WorkspaceSpec { display_name, sources: Vec<Source>, providers: Vec<Provider>, default_provider: Option<String>, env: Vec<EnvEntry>, isolation: Option<IsolationSpec> }`; `WorkspaceStatus { phase: WorkspacePhase, conditions, observed_generation, message }`; `WorkspacePhase { Pending, Reconciling, Ready, Failed }`.
- **`Provider`** `{ name, kind, base_url?, model?, credentials_ref? }`; **`CredentialsRef` `{ secret_name, key }`**; **`EnvEntry` `{ name, value }`**.
- **`CalibanTask`** (grow): add `workspace_ref: WorkspaceRef { name }`, `provider_ref: Option<String>`, `status.resolved_workspace: Option<ResolvedWorkspace>`; **drop `workspace`**. `ResolvedWorkspace { sources, provider: ResolvedProvider, env, isolation }` (the read path for `agent_from_task`).

Reuse the existing `Source`/`IsolationSpec` mirror types where prospero already has them; add only what's new.

### 2. Golden fixtures — `crates/core/tests/fixtures/` + a golden test

Vendor the operator's committed samples verbatim:
- `crates/core/tests/fixtures/operator-workspace.yaml` ← `deploy/samples/workspace.yaml`
- `crates/core/tests/fixtures/operator-calibantask.yaml` ← `deploy/samples/calibantask.yaml`

A golden test deserializes each into the prospero mirror, asserts the key fields, **re-serializes to JSON and asserts camelCase keys survive** — the drift guard for the ADR 0008 §1 boundary. (A follow-up CI job may re-fetch the operator samples to detect upstream drift; out of scope here — the vendored copy + a comment naming the source commit is the contract snapshot.)

### 3. `WorkspaceApi` seam — `crates/core/src/k8s/workspace_api.rs` (new)

Mirrors `CalibanTaskApi`/`MemTaskApi`. Trait + real kube impl + in-memory fake.

```rust
#[async_trait]
pub trait WorkspaceApi: Send + Sync {
    async fn apply(&self, ws: &Workspace) -> Result<()>;      // server-side-apply (create-or-update)
    async fn get(&self, name: &str) -> Result<Option<Workspace>>;
    async fn list(&self) -> Result<Vec<Workspace>>;           // list-with-status
    async fn delete(&self, name: &str) -> Result<bool>;       // false if absent
}
```

- **`KubeWorkspaceApi`** — `kube::Api::<Workspace>::namespaced`, same namespace/client construction as the CalibanTask api.
- **`FakeWorkspaceApi`** (testkit) — `Arc<Mutex<BTreeMap<String, Workspace>>>`; lets tests drive the admin seam without an apiserver (ADR 0007). Supports seeding a `status.phase` so list-with-status is exercised.

### 4. Implement the `admin` seam under k8s — `crates/core/src/k8s/fleet.rs`

Today `K8sFleet` does **not** implement `FleetAdmin`, so `AppState.admin` is `None` → 405. Implement `FleetAdmin` for `K8sFleet` (or a small `K8sWorkspaceAdmin` holding the `WorkspaceApi`), backed by `WorkspaceApi`:

| `FleetAdmin` method | k8s realization |
|---|---|
| `add_workspace(cfg)` | build a `Workspace` CR from cfg → `WorkspaceApi::apply` (create) |
| `set_workspace_config(name, cfg)` | build/patch → `WorkspaceApi::apply` |
| `remove_workspace(name)` | `WorkspaceApi::delete` |
| (list) | `WorkspaceApi::list` → map to the API's workspace-config DTO incl. `status.phase` |

The daemon wires `AppState.admin = Some(k8s_admin)` under the k8s backend arm. Run the new impl through the existing **admin-seam conformance suite** (the one `LocalFleet` already passes), extended to construct a k8s admin over `FakeWorkspaceApi`.

**Config DTO ↔ CR mapping:** the existing `RepoProviderConfig`/workspace-config DTO carries a *single* env-var-style provider today. The frozen `Workspace` has a *named provider list* + `credentialsRef`. Phase C maps what the current DTO can express and **carries the provider list through additively** (the dashboard's provider-list editor is Phase D). The precise DTO shape is settled in the plan against the current `RepoProviderConfig` definition; the invariant is: a round-trip `add_workspace` → `list` returns the providers and reconciliation status.

### 5. `GET /api/workspaces` returns real `Workspace` CRs — `crates/api/src/handlers.rs`

Under k8s, replace the read-only `CalibanTask`-projection with `WorkspaceApi::list` → config + `status.phase`/`message`. The agent list (from `snapshot()`) is unchanged. Local backend path unchanged.

### 6. `build_calibantask` / `spawn_agent` emit refs — `crates/core/src/k8s/fleet.rs`

`build_calibantask` sets `workspace_ref` + optional `provider_ref` + per-run overrides (`model`/`tools`/`isolation`) from the `TaskSpec`, instead of inline `workspace`. Requires threading the target workspace name + optional provider through the spawn path (the `TaskSpec` already carries repo/prompt; add the workspace/provider selection — defaulting to a workspace named for the source when unset, preserving today's implicit-workspace behavior until Phase D's launch modal supplies them explicitly).

### 7. Async create → `202`; retire the workspace-op 405 — `crates/api/src/{handlers,error}.rs`

Workspace create/patch under k8s is async (operator reconciles). Return `202 Accepted` for those ops. `ApiError::method_not_allowed` stays only for anything *genuinely* unsupported — it's no longer the response for workspace config under k8s.

### 8. Capabilities surface — **coordination seam with #101, not a new endpoint**

`GET /api/capabilities` + the `Capabilities` DTO are introduced by **#101** (open, being deepened in another session; lands ahead of Phase D). Phase C must **not** define its own — that would collide.

- Phase C's contribution to capability signalling is *structural*: implementing the admin seam under k8s makes `AppState.admin` **`Some`** on k8s, so #101's existing `Capabilities { admin }` automatically reports config-plane availability there (today it's hard-`false` under k8s only because the seam is `None`).
- Any *added* capability fields the design calls for — `backend_kind`, async semantics — are **additive extensions to #101's `Capabilities` DTO**, applied after Phase C rebases onto merged #101 (or handed to the #101/Phase-D track that owns the DTO). Phase C does not fork the DTO.

This keeps Phase C's diff off the files #101 is actively changing (`crates/api/dashboard/app.js`, the `Capabilities` DTO/handler), so the two land without conflict.

---

## Testing

- **Golden mirror tests** (Component 2): operator samples → prospero mirror → camelCase round-trip.
- **`FakeWorkspaceApi` conformance:** apply/get/list/delete semantics.
- **Admin-seam conformance:** the k8s admin (over `FakeWorkspaceApi`) passes the same suite `LocalFleet` does — add/list/config/remove round-trips, incl. reconciliation-status surfacing.
- **`GET /api/workspaces` handler test:** k8s path returns the seeded `Workspace` CRs with status; local path unchanged.
- **Migration tests:** `build_calibantask` emits `workspace_ref`/`provider_ref` (not inline `workspace`); `agent_from_task` reads `status.resolvedWorkspace`.
- **Full gate under CI `TESTKIT` feature set + `wasm-types`.**
- **Reality caveat:** k8s paths are fakes+golden+compile only — a live-cluster smoke (new QA-runbook row) is the real gate.

## Out of scope (Phase D / other tracks)

- Dashboard UI (provider-list editor, workspace modal, launch modal, status pills) — **#143**, which *extends* #101.
- `GET /api/capabilities` endpoint + `Capabilities` DTO shape — **owned by #101**.
- Operator-side reconcile, RBAC/CRD install (helm) — caliban-operator #11 (done) / helm-charts #30.

## Task order (each an independently testable deliverable)

1. CRD mirror migration (inline `workspace` → `workspaceRef` + `Workspace` mirror + `resolvedWorkspace`) + golden fixtures/tests.
2. `WorkspaceApi` trait + `KubeWorkspaceApi` + `FakeWorkspaceApi`.
3. `FleetAdmin` for k8s over `WorkspaceApi` + admin-seam conformance.
4. Daemon wiring: `AppState.admin = Some(..)` under k8s; `GET /api/workspaces` real CRs; `202` for async create.
5. `build_calibantask`/`spawn` emit `workspaceRef`/`providerRef`/overrides.
6. Rebase on merged #101; additive `Capabilities` extension (only if needed) — coordinate, don't fork.
