# Prospero

[![release](https://img.shields.io/github/v/release/caliban-ai/prospero?logo=github&logoColor=white&label=release&color=blue)](https://github.com/caliban-ai/prospero/releases/latest)
[![license](https://img.shields.io/badge/license-AGPL--3.0--only-blue)](LICENSE)
[![ci](https://img.shields.io/github/actions/workflow/status/caliban-ai/prospero/ci.yml?branch=main&logo=githubactions&logoColor=white&label=ci)](https://github.com/caliban-ai/prospero/actions/workflows/ci.yml?query=branch%3Amain)

Prospero is the **agent orchestration layer** for the [Caliban](https://github.com/caliban-ai/caliban)
agent harness. It is a control plane for launching, managing, and observing
**multiple Caliban agents** across repositories — including several parallel
agents working on the same codebase.

## How it works

Caliban already ships a per-repo supervisor daemon (`caliband`) that spawns and
manages background agents over a Unix-socket NDJSON protocol. Prospero sits
*above* many calibands as a single control plane:

```
prospero (CLI) ── HTTP/JSON ─▶ prosperod ─┬─ prospero-api (axum: REST + SSE + dashboard)
                                          └─ prospero-core (fleet model, caliband client,
                                                            discovery, registry, store)
                                                     │ NDJSON over Unix sockets
                                                     ▼
                                   caliband(repo A)  caliband(repo B)  …
                                          │                 │
                                     agents…            agents…
```

- **Launch** — spawn agents under any managed repo. Parallel work on one
  codebase runs in isolated git worktrees by default (`--shared-tree` to opt out).
- **Manage** — list, kill, respawn, and remove agents fleet-wide.
- **Observe** — a hybrid model: Prospero polls each caliband for live status and
  attaches to per-agent streams while they're active, normalizing caliban's
  stream-json into a stable `FleetEvent` type. Events are fanned out live (SSE)
  **and** persisted to a durable log, so history survives after an agent finishes
  (caliban itself only exposes live state).

The only coupling to caliban is its **wire format** — Prospero owns a thin
NDJSON client and does not depend on the caliban crates.

## Crates

| Crate | Binary | Responsibility |
|-------|--------|----------------|
| `prospero-core` | — | Fleet model, caliband client, discovery, registry, JSONL event store, `FleetManager` |
| `prospero-api` | — | axum REST + SSE + embedded dashboard over `FleetManager` |
| `prospero-daemon` | `prosperod` | Long-running control-plane daemon |
| `prospero-cli` | `prospero` | Operator CLI (thin HTTP client over `prosperod`) |
| `prospero-dashboard` | — | Dashboard v2: Dioxus → WASM SPA (outside the workspace — see below) |

## Usage

Start the daemon (serves the API + dashboard on `127.0.0.1:7878` by default):

```bash
cargo run --bin prosperod
# dashboard:    http://127.0.0.1:7878
# dashboard v2: http://127.0.0.1:7878/v2   (Dioxus/WASM — in development, #95)
```

Drive it with the CLI:

```bash
prospero repo add prospero /path/to/prospero      # register a repo
prospero spawn prospero "refactor the parser"      # launch a worktree-isolated agent
prospero spawn prospero "add tests" --shared-tree  # ...or in the shared tree
prospero ls                                        # list the fleet (repos + agents)
prospero follow <agent-id>                         # stream an agent's events live
prospero kill <agent-id>
```

The CLI and the dashboard talk to the same HTTP API.

## Development

```bash
cargo test --workspace --features prospero-core/testkit   # all tests
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings
cargo fmt --all --check
scripts/coverage.sh                                        # line-coverage report + gate
```

CI (`.github/workflows/ci.yml`) runs fmt/clippy/build/test plus a line-coverage
gate on every PR. `scripts/coverage.sh` is the single coverage entrypoint for
both local and CI runs (cargo-llvm-cov; line-coverage floor 85% in the script).
On a Homebrew Rust toolchain, point it at Homebrew's LLVM:
`export LLVM_COV=/opt/homebrew/opt/llvm/bin/llvm-cov LLVM_PROFDATA=/opt/homebrew/opt/llvm/bin/llvm-profdata`.

The test suite runs entirely against an in-process `FakeCaliband` harness (in
`prospero-core`'s `testkit` feature) that speaks the real wire protocol over Unix
sockets — so the whole control plane, including the end-to-end CLI path, is
tested with no real caliban and no LLM calls.

### Dashboard v2 (Dioxus/WASM)

`crates/dashboard` is a Rust → WASM SPA served at `/v2`. It is **excluded from
the cargo workspace** on purpose: the workspace gates and the 85% coverage floor
run over members, and a wasm-only UI crate would either fail the host-target
build or sink measured coverage. It has its own `Cargo.lock` and its own CI job.

Its built bundle is **committed** to `crates/api/dashboard-v2/` and
`include_bytes!`'d into prosperod, so an ordinary `cargo build` needs no wasm
toolchain and one binary still ships the UI. Rebuild it after changing anything
under `crates/dashboard/`:

```bash
scripts/build-dashboard.sh          # rebuild the bundle (then commit it)
scripts/build-dashboard.sh --check  # is the committed bundle current?
```

Requirements: the `wasm32-unknown-unknown` target, a `wasm-bindgen` CLI **at the
same version as the `wasm-bindgen` crate** (the script checks and tells you if
they differ), and optionally `binaryen` for the `wasm-opt` size pass. Where the
default toolchain has no wasm32 std — a Homebrew Rust, say — the script finds a
rustup toolchain automatically, or set `CARGO_WASM` to one.

CI enforces freshness with `--check`, which compares a recorded hash of the
build *inputs* rather than diffing rebuilt bytes: wasm output is not
reproducible across toolchains, so a byte diff would fail whenever CI's
`rustc`/`wasm-opt` differed from yours.

```bash
cd crates/dashboard && cargo test    # view-model + meter logic, host target
```

## Design docs

- Design spec: [`docs/superpowers/specs/2026-06-05-prospero-framework-design.md`](docs/superpowers/specs/2026-06-05-prospero-framework-design.md)
- Implementation plan: [`docs/superpowers/plans/2026-06-05-prospero-framework.md`](docs/superpowers/plans/2026-06-05-prospero-framework.md)
- Architecture Decision Records: [`docs/adr/`](docs/adr/) — the *why* behind significant
  decisions (control-plane role, caliban coupling, observability model, crate boundaries, …)

## Status

First-stab framework: complete and tested. Deferred (see the spec's non-goals):
multi-host fleets, API auth, log retention/rotation, a sqlite `Store` backend,
and automated tests against a real caliban binary + live model.
