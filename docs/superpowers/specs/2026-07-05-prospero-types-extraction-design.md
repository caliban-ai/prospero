# Extract `prospero-types` — wasm-compatible serde DTO crate — Design

**Ticket:** caliban-ai/prospero#98 (`kind/cleanup`). Prerequisite for #97 (Dioxus
scaffold); part of epic **#95** (Dashboard v2, Rust/WASM). Approved approach:
extract types first, then #97 depends on it.

## Problem

Dashboard v2 is a Rust → WASM Dioxus SPA whose headline advantage is **sharing
the API's serde DTOs** with the server (no client/server drift). But the crates
holding those DTOs are native-only and **do not compile to `wasm32`**:

- `prospero-core` → `tokio`, `sqlx`, `tokio-rustls`, `kube`
- `prospero-api` → `prospero-core` + `axum`

The read-model DTOs are pure serde types but are trapped inside these crates, so
a WASM frontend cannot depend on them. Hand-duplicating them in the frontend
would reintroduce exactly the drift Rust/WASM was chosen to avoid.

## Goal

A new leaf crate **`prospero-types`** (`crates/types/`) that:
- depends only on wasm-compatible crates: **`serde` + `serde_json`** (no tokio/
  sqlx/kube/chrono/schemars),
- holds the shared read-model DTOs,
- **compiles for `wasm32-unknown-unknown`** (CI-checked),
- and is re-exported from the DTOs' **current paths** in `prospero-core` so there
  is **zero import churn and no behavior change** anywhere downstream.

## What moves (verified pure serde; no orphan-rule impls; deps = serde+serde_json)

| From | Types |
|---|---|
| `core/src/event.rs` | `OutputStream`, `EventKind`, `FleetEvent` (+ the free fn `stream_key_for` and `impl FleetEvent::stream_key`) |
| `core/src/model.rs` | `Readiness`, `AgentStatus` (+impl), `WorkspaceHealth`, `Agent`, `Workspace`, `FleetSnapshot` (+impl `find_agent`), `AgentId` (+`From`/`Display` impls) |
| `core/src/registry.rs` | `RepoProviderConfig` (referenced by `Workspace.config` + api `WorkspaceSummary`) |
| `core/src/caliband/sources.rs` | the **`Source` struct only** (referenced by `Workspace.sources`) |

`serde_json` is required because `EventKind::ToolStarted.input: serde_json::Value`.
`PathBuf` (in `Workspace.root`) is std — wasm-ok.

## What stays in core (behavior-bearing / control-plane)

- `model.rs`: `TaskSpec`, `AgentHandle`, `DrainPolicy`, `FleetChange` (control plane;
  may *reference* moved types like `AgentId` via the re-export — fine, core depends
  on `prospero-types`).
- `caliband/sources.rs`: `discover_sources()` (filesystem logic) — stays, now
  operating on the moved `Source` struct.
- Everything else (`FleetManager`, `Store`, `Registry`/`RegisteredWorkspace`, k8s,
  etc.).

`WorkspaceSummary` (in `prospero-api/dto.rs`) is **not** moved in this ticket — the
scaffold's first render targets `GET /api/fleet` → `FleetSnapshot`, which is fully
inside the moved set. Moving api-side response DTOs can happen in a later slice if
a view needs them.

## Re-export strategy (zero churn)

Each moved type is re-exported from its old module path, so every existing
reference keeps resolving unchanged:

```rust
// core/src/event.rs
pub use prospero_types::{EventKind, FleetEvent, OutputStream, stream_key_for};
// core/src/model.rs
pub use prospero_types::{Agent, AgentId, AgentStatus, FleetSnapshot, Readiness, Workspace, WorkspaceHealth};
// core/src/registry.rs   → pub use prospero_types::RepoProviderConfig;
// core/src/caliband/sources.rs → pub use prospero_types::Source;
```

`prospero-core`'s crate-root re-exports (`pub use model::…`, `pub use event::…`)
then continue to surface the same names, so `prospero_core::WorkspaceHealth` etc.
are unchanged for `prospero-api` and the daemon.

`crates/*` is a glob workspace member, so the new crate auto-joins — no root
`Cargo.toml` members edit.

## Testing

- **Move the round-trip serde unit tests** for the moved types into
  `prospero-types` (they exercise `Serialize`/`Deserialize` shape — they belong
  with the types). Core keeps tests for the types that stay (`FleetChange`, etc.).
- **wasm target check (CI):** add a step that builds `prospero-types` for
  `wasm32-unknown-unknown` (`rustup target add wasm32-unknown-unknown` +
  `cargo build -p prospero-types --target wasm32-unknown-unknown`), proving the
  crate stays wasm-clean as it grows. This is the guard that makes the whole v2
  DTO-sharing story durable.
- **No behavior change:** the full existing gate must stay green with the CI
  `TESTKIT` feature set — the extraction is pure relocation + re-export.

## Scope (YAGNI)

- Only the core read-model DTOs above; no api-side response DTOs (`WorkspaceSummary`)
  this pass.
- No new fields/renames — a straight move; serde output is byte-for-byte identical.
- No frontend code (that's #97).

## Acceptance

- `crates/types/` exists, deps limited to `serde`/`serde_json`, builds for
  `wasm32-unknown-unknown` (CI-checked).
- The listed types live in `prospero-types`; `prospero-core` re-exports them from
  their old paths; no downstream import changes.
- Full gate green; serde wire shapes unchanged. Unblocks #97.
