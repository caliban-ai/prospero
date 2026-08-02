# Dashboard v2 — Dioxus/WASM scaffold — Design

**Ticket:** caliban-ai/prospero#97 (`kind/feature`). First sub-ticket of epic
**#95** (Dashboard v2, Rust/WASM). Depends on **#98** (`prospero-types`
extraction — landed).

## Problem

Today's dashboard is a single hand-written vanilla-JS file (`crates/api/dashboard/app.js`,
~1050 lines) plus `index.html`, embedded with `include_str!` and served from
axum. It has no component model, no type-safety against the API, and every
timeline or chart has to be hand-rolled in imperative DOM code.

Epic #95 re-platforms it as a Rust → WASM Dioxus SPA. Before any feature port,
the foundation has to be proven end-to-end: a crate that compiles to `wasm32`, a
build that yields a bundle, that bundle embedded in prosperod and served from
axum, rendering live data through a DTO **shared verbatim** with the server.
This slice de-risks the four real unknowns — build toolchain, embedding, CSP,
DTO sharing — and lands the design system the follow-on tickets build on.

## Goal

`/v2` serves a Dioxus/WASM dashboard that renders live fleet data fetched from
`/api/fleet` and deserialized into `prospero_types::FleetSnapshot`, with a green
CI wasm build and today's `/` dashboard byte-for-byte unaffected.

## Decisions (2026-08-01)

These were open in the ticket ("decide in brainstorm") and are settled here.

| Decision | Choice | Why |
|---|---|---|
| Build tool | **`cargo` + `wasm-bindgen-cli`** (not `dx`, not `trunk`) | Fewest moving parts; one pinnable binary; no opinionated project layout. We hand-write `index.html`, so trunk's templating/hashing buys nothing. |
| Bundle delivery | **Commit the built bundle**, CI proves freshness | Any `cargo build` works with zero extra toolchain; the single-binary story stays airtight. |
| Workspace membership | **Excluded** from the cargo workspace | Keeps `--workspace` gates and the 85% coverage floor exactly as they are. |
| Serving route | **`/v2`**, absolute asset URLs | `/` stays untouched during the transition. |
| Read endpoint | **`GET /api/fleet` → `FleetSnapshot`** | Already in `prospero-types` with `Deserialize`; proves DTO parity with no new type-plumbing. |
| Visual direction | **Modern control plane** | Sans chrome, monospace for data, light+dark tokens. |

## Architecture

### Crate layout and the workspace boundary

New crate `crates/dashboard` (package **`prospero-dashboard`**), **excluded from
the cargo workspace**:

```toml
# root Cargo.toml
[workspace]
members = ["crates/*"]
exclude = ["crates/dashboard"]
```

`crates/dashboard/Cargo.toml` carries its own empty `[workspace]` table, making
it a standalone workspace root with its own `Cargo.lock`.

This is the load-bearing structural decision. The root workspace's gates —
`cargo clippy --workspace --all-targets`, `cargo build --workspace --all-targets`,
`cargo test --workspace`, and `scripts/coverage.sh` with its **85% line floor** —
all run over workspace *members*. A wasm-targeted UI crate inside that set would
be built for `aarch64`/`x86_64` on every gate, and its view code would drag
measured coverage under the floor. Excluding it leaves all four gates numerically
unchanged; the crate earns its own dedicated CI job instead (see Testing).

Dependencies: `dioxus` (web renderer), `prospero-types` (path dep `../types`),
`gloo-net` (fetch + serde), `wasm-bindgen`, `console_error_panic_hook`.

### Build pipeline

`scripts/build-dashboard.sh` — the single entrypoint for humans and CI, matching
how `scripts/coverage.sh` is already structured:

1. `cargo build -p prospero-dashboard --target wasm32-unknown-unknown --release`
2. `wasm-bindgen --target web --no-typescript --out-dir crates/api/dashboard-v2`
3. `wasm-opt -Oz` when available; skipped with a printed notice when not
4. copy `index.html` and `app.css` into the output dir

**Version-skew guard.** The script parses the resolved `wasm-bindgen` version out
of `crates/dashboard/Cargo.lock` and fails with an actionable message when the
installed `wasm-bindgen` CLI reports a different version. Crate/CLI skew is the
most common failure mode in this toolchain and it surfaces as opaque runtime
errors in the browser, so it is caught at build time instead.

**Local toolchain.** The repo's default toolchain need not have `wasm32`. The
script resolves a wasm-capable cargo in this order: `$CARGO_WASM` if set, then a
rustup toolchain under `~/.rustup/toolchains/`, then plain `cargo`. It exits with
setup instructions if none can target `wasm32-unknown-unknown`.

### Embedding and serving

The built bundle is committed to `crates/api/dashboard-v2/` and pulled in with
`include_bytes!`/`include_str!` from a new `crates/api/src/dashboard_v2.rs` — the
same pattern as the existing `dashboard.rs`, adding no dependency.

| Route | Content-Type |
|---|---|
| `GET /v2` | `text/html; charset=utf-8` |
| `GET /v2/app.css` | `text/css; charset=utf-8` |
| `GET /v2/prospero-dashboard.js` | `application/javascript; charset=utf-8` |
| `GET /v2/prospero-dashboard_bg.wasm` | `application/wasm` |

Asset URLs inside the HTML are **absolute** (`/v2/app.css`), so serving at `/v2`
without a trailing slash cannot mis-resolve them.

`GET /` and `GET /app.js` are not modified.

**CSP.** `/v2` responds with:

```
default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self';
connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'
```

`'wasm-unsafe-eval'` is required for the module to instantiate. Everything else
stays denied because the bundle is fully self-contained — no CDN, no external
font, no remote image.

### Data flow

On mount the app fetches `GET /api/fleet` and deserializes the body into
`prospero_types::FleetSnapshot` — the exact type `prospero-api` serializes. That
is the shared-DTO claim proven end-to-end. It refreshes by polling on an interval.

`WorkspaceSummary` (`GET /api/workspaces`) is deliberately **not** used: it lives
in `prospero-api`, is serialize-only, and depends on `prospero-core`, so it
cannot compile to wasm today. Moving it into `prospero-types` is real work
belonging to the follow-on port ticket. `FleetSnapshot` already carries
everything this slice renders: `host`, and per workspace `name` / `root` /
`sources` / `health` / `config` / `agents`, with each agent's `id`, `name`,
`status`, `started_at`, `isolated`, and `interactive`.

Per-agent SSE is out of scope here.

### UI/UX

Direction: **modern control plane**, built with the `frontend-design` skill.

- A design-token layer in `app.css` (color, space, type scale, radius,
  elevation) that everything else derives from; light **and** dark via
  `prefers-color-scheme`.
- Sans-serif for chrome; monospace confined to data — ids, paths, counts,
  timestamps. Precision where it carries meaning, not as costume.
- App shell: header, sidebar nav, a stat row, then workspace cards carrying
  health, sources, and an agent status distribution.
- Status is encoded by **shape and label as well as hue**, so it survives
  colorblindness and greyscale rendering.

### Error handling

Explicit app state: `Loading | Ready(FleetSnapshot) | Error(String)`.

- A failed initial load renders the error text plus a retry action, never a
  blank page.
- A failed *refresh* after a successful load keeps rendering last-known data
  with a staleness indicator, rather than blanking a working screen.
- `console_error_panic_hook` is installed at startup so a panic produces a
  readable trace instead of bare `unreachable`.

## Testing

**Unit (host target).** Derived logic — status rollups, counts, duration and
path formatting — lives in a `view_model` module that compiles for the host
target too, exercised by plain `cargo test -p prospero-dashboard`. This puts the
parts where bugs actually hide under test without a headless browser.

**API-side (workspace).** Tests in `prospero-api` assert that `/v2` returns 200
with the expected content type and CSP header, that the wasm route serves
`application/wasm`, and that `/` still serves the v1 dashboard unchanged.

**CI — new `dashboard-wasm` job.** Installs the `wasm32-unknown-unknown` target
and a pinned `wasm-bindgen-cli`, then:

1. runs `scripts/build-dashboard.sh`
2. `git diff --exit-code crates/api/dashboard-v2/` — **proves the committed
   bundle matches its sources**
3. `cargo test -p prospero-dashboard`
4. `cargo clippy -p prospero-dashboard --all-targets -- -D warnings`

Step 2 is what makes committing a build artifact safe instead of a rot vector: a
stale bundle fails the build.

## Out of scope

No feature port (fleet controls, agent stream viewer, spawn and config modals),
no charts, no per-agent timeline, no tool-call inspector, no SSE. Those are the
remaining sub-tickets of #95. This slice delivers foundation plus design system.

## Known trade-off

The committed `.wasm` is roughly 1–3 MB and re-churns on every UI change, which
compounds across the follow-on tickets. Accepted deliberately in exchange for
`cargo build` working with no wasm toolchain. The escape hatch, if the git churn
becomes painful, is to move the build to release time — a change confined to the
build script, CI, and `dashboard_v2.rs`, touching no application code.

## Acceptance

- A Dioxus/WASM bundle builds via `scripts/build-dashboard.sh`.
- It is embedded in prosperod and served over HTTP at `/v2`.
- It renders live fleet data fetched from `/api/fleet` using
  `prospero_types::FleetSnapshot` — the same type the server sends.
- CI builds the wasm bundle green and fails on a stale committed bundle.
- Today's `/` dashboard is unaffected.
