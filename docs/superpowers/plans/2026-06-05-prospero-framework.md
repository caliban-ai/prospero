# Prospero Orchestration Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first-stab Prospero control plane that launches, manages, and observes multiple Caliban agents across repos.

**Architecture:** A control plane over Caliban's per-repo `caliband` daemons. `prospero-core` holds the domain model, a thin NDJSON caliband client, discovery, a registry, a durable JSONL event store, and a `FleetManager` (poll + on-demand attach + broadcast bus). `prospero-api` exposes it over axum (REST + SSE + dashboard). `prosperod` is the long-running daemon; `prospero` is a thin HTTP CLI.

**Tech Stack:** Rust (edition 2024), tokio, serde/serde_json, axum + tower-http, clap, ureq, sha2, chrono, thiserror/anyhow, tracing.

**Reference spec:** `docs/superpowers/specs/2026-06-05-prospero-framework-design.md`

---

## File structure

```
Cargo.toml                              # workspace.dependencies (shared versions)
crates/core/Cargo.toml                  # prospero-core deps + `testkit` feature
crates/core/src/
  lib.rs                                # re-exports; pub mod ...
  error.rs                              # CoreError
  model.rs                              # Host/Repo/Agent/AgentStatus/RepoHealth/FleetSnapshot
  event.rs                              # FleetEvent/EventKind/OutputStream
  caliband/mod.rs                       # pub mod wire; pub mod client; ndjson framing
  caliband/wire.rs                      # mirrored CtlRequest/CtlReply/SpawnSpec/AgentRecord/...
  caliband/client.rs                    # CalibandClient (UnixStream NDJSON)
  caliband/stream.rs                    # normalize_frame(): caliban stream-json -> EventKind
  discovery.rs                          # socket-path resolution + ensure_caliband
  registry.rs                           # RegisteredRepo + Registry (JSON persistence)
  store.rs                              # Store trait + JsonlStore
  fleet.rs                              # FleetManager (poll loop, attach tasks, bus, seq)
  testkit.rs                            # FakeCaliband (feature = "testkit")
crates/api/Cargo.toml
crates/api/src/
  lib.rs                                # router(state) + AppState
  dto.rs                               # request/response DTOs
  handlers.rs                          # endpoint handlers
  sse.rs                               # SSE stream handler
  dashboard.rs                         # embedded index.html + app.js routes
crates/api/dashboard/index.html
crates/api/dashboard/app.js
crates/daemon/Cargo.toml
crates/daemon/src/main.rs               # prosperod: config, wiring, serve, shutdown
crates/cli/Cargo.toml
crates/cli/src/main.rs                  # prospero: clap + ureq client
crates/cli/src/client.rs                # DaemonClient (ureq)
tests/ (in cli crate)                   # e2e smoke (ignored)
```

---

## Phase 0 — Workspace dependencies

### Task 0: Declare shared dependencies

**Files:**
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Add `[workspace.dependencies]` entries**

Append to the existing `[workspace.dependencies]` table in root `Cargo.toml`:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "io-util", "sync", "time", "process", "signal"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
anyhow = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
sha2 = "0.10"
hex = "0.4"
chrono = { version = "0.4", default-features = false, features = ["clock", "serde"] }
axum = "0.8"
tower-http = { version = "0.6", features = ["trace"] }
clap = { version = "4", features = ["derive", "env"] }
ureq = { version = "2", features = ["json"] }
tempfile = "3"
```

- [ ] **Step 2: Verify the workspace still resolves**

Run: `cargo metadata --no-deps --format-version 1 >/dev/null && echo OK`
Expected: `OK` (no manifest parse error).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "build: declare shared workspace dependencies"
```

---

This plan is split into per-module task files for manageable review. Subsequent phases live alongside this file:

- `2026-06-05-prospero-framework-phase1-core-foundations.md` — error, model, event, wire types, NDJSON framing
- `2026-06-05-prospero-framework-phase2-core-client-normalize.md` — CalibandClient, stream normalizer
- `2026-06-05-prospero-framework-phase3-core-discovery-registry-store.md` — discovery, registry, JsonlStore
- `2026-06-05-prospero-framework-phase4-core-fleet-testkit.md` — FleetManager, FakeCaliband
- `2026-06-05-prospero-framework-phase5-api.md` — DTOs, router, handlers, SSE, dashboard
- `2026-06-05-prospero-framework-phase6-daemon-cli.md` — prosperod, prospero CLI
- `2026-06-05-prospero-framework-phase7-e2e.md` — end-to-end smoke test

Each phase file is self-contained with full code and tests, and must be implemented in order (later phases depend on earlier types).
