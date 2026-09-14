# Prospero

[![release](https://img.shields.io/github/v/release/caliban-ai/prospero?logo=github&logoColor=white&label=release&color=blue)](https://github.com/caliban-ai/prospero/releases/latest)
[![license](https://img.shields.io/badge/license-AGPL--3.0--only-blue)](LICENSE)
[![ci](https://img.shields.io/github/actions/workflow/status/caliban-ai/prospero/ci.yml?branch=main&logo=githubactions&logoColor=white&label=ci)](https://github.com/caliban-ai/prospero/actions/workflows/ci.yml?query=branch%3Amain)

Prospero is the **agent orchestration layer** for the [Caliban](https://github.com/caliban-ai/caliban)
agent harness. It is a control plane for launching, managing, and observing
**multiple Caliban agents** across workspaces, including several agents working
in parallel on the same codebase.

**Guide:** <https://caliban-ai.github.io/prospero/>. It covers getting started,
the CLI, configuration, the HTTP API and events, auth, and deployment.

## How it works

Caliban already ships a per-workspace supervisor daemon, `caliband`, that spawns
and manages background agents over a Unix-socket NDJSON protocol. Prospero sits
*above* many calibands as a single control plane:

```
prospero (CLI) ─┐
dashboard ──────┼─ HTTP/JSON + SSE ─▶ prosperod ─┬─ prospero-api (axum: REST + SSE + dashboard)
other clients ──┘                                └─ prospero-core (fleet model, caliband client,
                                                                   discovery, registry, stores)
                                                        │ NDJSON over Unix sockets   (local backend)
                                                        ▼
                                  caliband(workspace A)  caliband(workspace B)  …
                                          │                     │
                                     agents…                agents…
```

With `PROSPERO_FLEET=k8s`, the same API is served over `CalibanTask`/`Workspace`
custom resources instead of local calibands (ADR 0008).

- **Launch.** Spawn agents under any registered workspace. Parallel work on one
  codebase runs in isolated git worktrees by default; pass `--shared-tree` to opt
  out.
- **Manage.** List, kill, respawn and remove agents fleet-wide, and send input to
  interactive agents.
- **Observe.** Prospero uses a hybrid model. It polls each caliband for live
  status and attaches to per-agent streams while they're active, converting
  caliban's stream-json into a stable `FleetEvent` type. Events go out live over
  SSE **and** are written to a durable store (sqlite standalone, Postgres
  clustered), so history survives after an agent finishes. Caliban itself only
  exposes live state.

The only coupling to caliban is its **wire format**. Prospero owns a thin NDJSON
client and does not depend on the caliban crates.

## Ecosystem

- [caliban](https://github.com/caliban-ai/caliban) is the agent harness and
  `caliband` supervisor that Prospero drives.
- [gonzalo](https://github.com/caliban-ai/gonzalo) is a shareable persistence
  layer for caliban.
- [ariel](https://github.com/caliban-ai/ariel) is a chat bridge for the fleet:
  Discord first, then Slack and Teams. It uses Prospero only through the public
  HTTP + SSE API (no crate dependency) and keeps identity, channel config and
  audit data in gonzalo. Tracking issue:
  [#67](https://github.com/caliban-ai/prospero/issues/67).

## Crates

| Crate | Binary | Responsibility |
|-------|--------|----------------|
| `prospero-types` | — | Shared, wasm-compatible DTOs: fleet model, `FleetEvent`, API and auth payloads |
| `prospero-core` | — | Fleet model, caliband client, discovery, workspace registry, event stores (sqlite / Postgres), event bus, `FleetManager`, `K8sFleet` (feature `k8s`) |
| `prospero-api` | — | axum REST + SSE + auth + embedded dashboard over the `FleetProvider` seam |
| `prospero-daemon` | `prosperod` | Long-running control-plane daemon |
| `prospero-cli` | `prospero` | Operator CLI (thin HTTP client over `prosperod`) |
| `prospero-dashboard` | — | Dashboard: Dioxus → WASM SPA (outside the workspace; see below) |

## Usage

Start the daemon. It serves the API and dashboard on `127.0.0.1:7878` by default:

```bash
cargo run --bin prosperod
# dashboard:  http://127.0.0.1:7878      (Dioxus/WASM)
```

Drive it with the CLI:

```bash
prospero workspace add prospero /path/to/prospero      # register a workspace
prospero workspace config prospero --provider anthropic --api-key-env ANTHROPIC_API_KEY
prospero spawn prospero "refactor the parser"           # launch a worktree-isolated agent
prospero spawn prospero "add tests" --shared-tree       # ...or in the shared tree
prospero ls                                             # list the fleet (workspaces + agents)
prospero follow <agent-id>                              # stream an agent's events live
prospero kill <agent-id>
```

The CLI and the dashboard use the same HTTP API. For the container image, see
[`docs/container.md`](docs/container.md).

## Development

These mirror `.github/workflows/ci.yml`:

```bash
FEATURES="--features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s"
cargo fmt --all -- --check
cargo clippy --workspace --all-targets $FEATURES -- -D warnings
cargo build --workspace --all-targets $FEATURES
cargo test --workspace $FEATURES      # Postgres tests run when DATABASE_URL is set
scripts/coverage.sh                   # line-coverage report + gate
```

CI runs fmt/clippy/build/test plus a line-coverage gate on every PR.
`scripts/coverage.sh` is the single coverage entrypoint for both local and CI
runs (cargo-llvm-cov; line-coverage floor 85% in the script). On a Homebrew Rust
toolchain, point it at Homebrew's LLVM:
`export LLVM_COV=/opt/homebrew/opt/llvm/bin/llvm-cov LLVM_PROFDATA=/opt/homebrew/opt/llvm/bin/llvm-profdata`.

The test suite runs entirely against an in-process `FakeCaliband` harness (in
`prospero-core`'s `testkit` feature) that speaks the real wire protocol over Unix
sockets. The whole control plane, including the end-to-end CLI path, is tested
with no real caliban and no LLM calls.

### Dashboard (Dioxus/WASM)

`crates/dashboard` is a Rust → WASM SPA and **the dashboard**: the document is
served at `/` and its files under `/assets/`. It is **excluded from the cargo
workspace** on purpose. The workspace gates and the 85% coverage floor run over
members, and a wasm-only UI crate would either fail the host-target build or
sink measured coverage. It has its own `Cargo.lock` and its own CI job.

Its built bundle is **committed** to `crates/api/dashboard/` and
`include_bytes!`'d into prosperod, so an ordinary `cargo build` needs no wasm
toolchain and one binary still ships the UI. Rebuild it after changing anything
under `crates/dashboard/`:

```bash
scripts/build-dashboard.sh          # rebuild the bundle (then commit it)
scripts/build-dashboard.sh --check  # is the committed bundle current?
```

Requirements:

- the `wasm32-unknown-unknown` target;
- a `wasm-bindgen` CLI **at the same version as the `wasm-bindgen` crate** (the
  script checks and tells you if they differ);
- optionally, `binaryen` for the `wasm-opt` size pass.

Where the default toolchain has no wasm32 std (a Homebrew Rust, say), the script
finds a rustup toolchain automatically, or you can set `CARGO_WASM` to one.

CI enforces freshness with `--check`, which compares a recorded hash of the
build *inputs* rather than diffing rebuilt bytes. Wasm output is not
reproducible across toolchains, so a byte diff would fail whenever CI's
`rustc`/`wasm-opt` differed from yours.

```bash
cd crates/dashboard && cargo test    # view-model + meter logic, host target
```

## Design docs

- The guide's [Guiding Principles & Invariants](https://caliban-ai.github.io/prospero/principles.html)
- Architecture Decision Records: [`docs/adr/`](docs/adr/), the *why* behind
  significant decisions (control-plane role, caliban coupling, observability
  model, crate boundaries, k8s backend, API auth, …)
- Design specs and implementation plans: [`docs/superpowers/`](docs/superpowers/)
- Releasing: [`docs/releasing.md`](docs/releasing.md)

## Securing the API

On loopback, prosperod runs without auth. Anywhere else, give it a tokens file:

    prospero token new alice --scope admin   # read | operate | admin
    prosperod --addr 0.0.0.0:7878 --api-tokens-file ./tokens
    PROSPERO_TOKEN=pspo_… prospero ls

The dashboard asks for a token and keeps a 12-hour session cookie. Details are in
the guide's [Securing the API](https://caliban-ai.github.io/prospero/api-auth.html)
page and ADR-0010.

## Status

Pre-1.0; see [`CHANGELOG.md`](CHANGELOG.md). Shipped so far:

- local (caliband) and k8s (`CalibanTask`) fleet backends;
- standalone (sqlite) and clustered (Postgres, leased stream ownership)
  topologies;
- age-based event retention;
- scoped API tokens;
- the WASM dashboard.

Not yet covered: automated tests against a real caliban binary and a live model.
The suite runs against the in-process fake.
