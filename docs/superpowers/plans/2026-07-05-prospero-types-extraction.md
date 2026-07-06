# Extract `prospero-types` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax. This is a **pure relocation + re-export** refactor — the guiding invariant is *zero behavior change*: the full existing gate must stay green at every commit, and serde output must be byte-for-byte identical.

**Goal:** Move the shared read-model serde DTOs out of `prospero-core` into a new wasm-compatible `prospero-types` leaf crate, re-exported from their old paths, so the WASM dashboard (#97) can depend on them without pulling in tokio/sqlx/kube.

**Architecture:** New `crates/types` (serde + serde_json only). Move the types + their impls + round-trip tests. `prospero-core` depends on `prospero-types` and re-exports each moved type from its original module path (`event.rs`, `model.rs`, `registry.rs`, `caliband/sources.rs`). A CI step builds `prospero-types` for `wasm32`.

## Global Constraints

- **Zero behavior change / zero import churn** — verified by the unchanged gate.
- `prospero-types` deps: **only** `serde` (derive) + `serde_json`. Never add a native dep.
- Gate `$TESTKIT` = `--features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s`.
- Types that move: `OutputStream, EventKind, FleetEvent (+stream_key_for), Readiness, AgentStatus, WorkspaceHealth, Agent, Workspace, FleetSnapshot, AgentId, RepoProviderConfig, Source (struct only)`.
- Stays in core: `TaskSpec, AgentHandle, DrainPolicy, FleetChange, discover_sources(), Registry/RegisteredWorkspace, ...`.

---

### Task 1: Create the empty `prospero-types` crate

**Files:** Create `crates/types/Cargo.toml`, `crates/types/src/lib.rs`.

- [ ] **Step 1:** `crates/types/Cargo.toml`:

```toml
[package]
name = "prospero-types"
version.workspace = true
edition.workspace = true

[dependencies]
serde = { workspace = true, features = ["derive"] }
serde_json.workspace = true
```

- [ ] **Step 2:** `crates/types/src/lib.rs`:
```rust
//! Shared, wasm-compatible serde DTOs for the prospero API surface. Depends only
//! on `serde`/`serde_json` so both the native server (prospero-core/api) and the
//! WASM dashboard (#97) can share these exact types — no client/server drift.
//! Behavior-bearing types stay in `prospero-core`.
```

- [ ] **Step 3:** Confirm it joins the workspace (glob member) and builds:
  `cargo build -p prospero-types` → PASS (empty crate).

- [ ] **Step 4: Commit** — `git commit -m "feat(types): add empty prospero-types crate (#98)"`

---

### Task 2: Move the `event.rs` DTOs

**Files:** `crates/types/src/lib.rs` (or `event.rs` module in it), `crates/core/src/event.rs`, `crates/core/Cargo.toml`.

- [ ] **Step 1:** Add `prospero-types.workspace = true` to `[dependencies]` in `crates/core/Cargo.toml`, and a workspace dep entry in root `Cargo.toml` if the workspace pins deps there (check `[workspace.dependencies]`; add `prospero-types = { path = "crates/types" }`).
- [ ] **Step 2:** Cut `OutputStream`, `EventKind`, `FleetEvent`, the free fn `stream_key_for`, and `impl FleetEvent` from `core/src/event.rs` into `prospero-types` (e.g. a `types::event` module, re-exported at the types crate root). Bring the `#[cfg(test)]` round-trip tests for these along.
- [ ] **Step 3:** Replace them in `core/src/event.rs` with `pub use prospero_types::{EventKind, FleetEvent, OutputStream, stream_key_for};` (keep the module's other contents).
- [ ] **Step 4:** Build core: `cargo build -p prospero-core --features testkit` → fix any now-`crate::`-internal paths the moved code used (it should reference only serde/serde_json; if a moved item used `crate::model::X`, that `X` must also be moved or the reference re-pathed to `prospero_types::X`).
- [ ] **Step 5:** `cargo test -p prospero-types` (moved round-trip tests) + `cargo test -p prospero-core --features testkit` (unchanged) → PASS.
- [ ] **Step 6: Commit** — `"refactor(core): move event DTOs to prospero-types, re-export (#98)"`

---

### Task 3: Move the `model.rs` DTOs + `Source` + `RepoProviderConfig`

**Files:** `crates/types/src/…`, `crates/core/src/model.rs`, `crates/core/src/registry.rs`, `crates/core/src/caliband/sources.rs`.

> Order matters — `EventKind` (Task 2) already references `AgentStatus`/`WorkspaceHealth` via re-export from core; once those move here too, keep the re-exports consistent. Move the whole cluster in one commit so the crate is internally consistent.

- [ ] **Step 1:** Move into `prospero-types`: `Readiness`, `AgentStatus` (+impl), `WorkspaceHealth`, `Agent`, `Workspace`, `FleetSnapshot` (+`impl find_agent`), `AgentId` (+`From<&str>`, `From<String>`, `Display`). Also move `RepoProviderConfig` (from `registry.rs`) and the **`Source` struct** (from `caliband/sources.rs`), since `Workspace` embeds both. Bring their round-trip tests.
  - `Workspace.root: PathBuf` — add `use std::path::PathBuf;` in types (std, wasm-ok).
  - `EventKind`/`FleetChange` etc. that reference these now resolve within `prospero-types` (for the movers) or via core's dep on types (for `FleetChange`, which stays).
- [ ] **Step 2:** Re-export from old paths:
  - `core/src/model.rs`: `pub use prospero_types::{Agent, AgentId, AgentStatus, FleetSnapshot, Readiness, Workspace, WorkspaceHealth};`
  - `core/src/registry.rs`: `pub use prospero_types::RepoProviderConfig;`
  - `core/src/caliband/sources.rs`: `pub use prospero_types::Source;` (keep `discover_sources()` here, operating on the re-exported `Source`).
- [ ] **Step 3:** Build + fix paths: `cargo build -p prospero-core --features testkit`. Watch for: code that constructed `Workspace{…}`/`Agent{…}` (still works via re-export), and any `crate::caliband::sources::Source` / `crate::registry::RepoProviderConfig` references (still resolve via re-export).
- [ ] **Step 4:** `cargo test -p prospero-types` + `cargo test -p prospero-core --features testkit` → PASS.
- [ ] **Step 5: Commit** — `"refactor(core): move fleet/agent/config DTOs to prospero-types (#98)"`

---

### Task 4: Workspace-wide green + api/daemon unchanged

- [ ] **Step 1:** Full gate: `cargo fmt --all && cargo clippy --workspace --all-targets $TESTKIT -- -D warnings && cargo build --workspace --all-targets $TESTKIT && cargo test --workspace $TESTKIT`. `prospero-api`/`prospero-daemon` should compile untouched (they use `prospero_core::WorkspaceHealth` etc., still re-exported). Fix only if a path genuinely moved.
- [ ] **Step 2:** Confirm serde parity: the moved round-trip tests already assert wire shape; spot-check one API integration test (`api_integration.rs` fleet/events) still passes.
- [ ] **Step 3: Commit** if any fmt/fixups — else skip.

---

### Task 5: `wasm32` CI check (the durability guard)

**Files:** `.github/workflows/ci.yml`.

- [ ] **Step 1:** Add a lightweight job/step:
```yaml
  wasm-types:
    name: prospero-types builds for wasm32
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: rustup target add wasm32-unknown-unknown
      - run: cargo build -p prospero-types --target wasm32-unknown-unknown
```
(Match the repo's existing job style/toolchain setup.)
- [ ] **Step 2:** Verify locally: `rustup target add wasm32-unknown-unknown && cargo build -p prospero-types --target wasm32-unknown-unknown` → PASS. This is the proof the crate is genuinely wasm-clean (no transitive native dep sneaks in).
- [ ] **Step 3: Commit** — `"ci: build prospero-types for wasm32 (#98)"`

---

## Self-Review

- **Spec coverage:** new crate (T1), event DTOs (T2), model+Source+config DTOs (T3), workspace-green/no-behavior-change (T4), wasm32 guard (T5). Covered.
- **Risk — orphan rule:** all impls on moved types live in `model.rs`/`event.rs` (scan was empty) and move with them; no core-local trait is impl'd for a moved type, so nothing is stranded.
- **Risk — cyclic move:** `EventKind`→`AgentStatus`/`WorkspaceHealth` and `Workspace`→`Source`/`RepoProviderConfig`/`Agent` all move into the *same* crate, so the cluster is self-contained; `FleetChange` (stays) references moved types via core's dep on `prospero-types`.
- **Verification is the gate:** because it's pure relocation, an unchanged green `TESTKIT` gate + the moved round-trip tests + the wasm32 build *are* the proof. No new behavioral tests needed.
- **Type consistency:** re-export list matches the moved-types list exactly (cross-checked against the spec table).
