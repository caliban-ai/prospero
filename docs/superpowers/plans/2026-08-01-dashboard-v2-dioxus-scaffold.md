# Dashboard v2 — Dioxus/WASM Scaffold Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve a Dioxus/WASM dashboard at `/v2` that renders live fleet data fetched from `/api/fleet` using `prospero_types::FleetSnapshot`, without disturbing today's `/` dashboard.

**Architecture:** A new `crates/dashboard` crate, *excluded* from the cargo workspace so the existing `--workspace` gates and 85% coverage floor are untouched, compiled to `wasm32-unknown-unknown` and post-processed by `wasm-bindgen`. The resulting bundle is committed to `crates/api/dashboard-v2/` and embedded into prosperod with `include_bytes!`, served from new axum routes under `/v2`. A CI job rebuilds the bundle and diffs it to prove freshness.

**Tech Stack:** Rust (edition 2024), Dioxus 0.7, `gloo-net` for fetch, `wasm-bindgen` + `wasm-bindgen-cli`, `wasm-opt` (optional), axum 0.8, plain CSS with a design-token layer.

**Spec:** `docs/superpowers/specs/2026-08-01-dashboard-v2-dioxus-scaffold-design.md`

## Global Constraints

- Rust edition **2024**; workspace version **0.4.0**; license **AGPL-3.0-only**.
- The dashboard crate is **excluded** from the root workspace and has its own `[workspace]` table and `Cargo.lock`.
- **No npm / Node toolchain** anywhere in the build.
- `GET /` and `GET /app.js` must remain byte-for-byte unchanged in behaviour.
- All `/v2` asset URLs referenced from HTML are **absolute** (`/v2/app.css`, not `./app.css`).
- CSP served on `/v2` is exactly: `default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'`
- The bundle is committed at `crates/api/dashboard-v2/` with these four filenames: `index.html`, `app.css`, `prospero-dashboard.js`, `prospero-dashboard_bg.wasm`.
- Local wasm builds use a rustup toolchain (`~/.rustup/toolchains/stable-aarch64-apple-darwin/bin/cargo`); the default `cargo` on PATH is Homebrew's and has no wasm32 std.
- Full local gate before pushing: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets $TESTKIT -- -D warnings`, `cargo build --workspace --all-targets $TESTKIT`, `cargo test --workspace $TESTKIT`, where `TESTKIT="--features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s"`.

---

### Task 1: Scaffold the excluded crate and prove a wasm build

**Files:**
- Modify: `Cargo.toml` (add `exclude`)
- Create: `crates/dashboard/Cargo.toml`
- Create: `crates/dashboard/src/main.rs`
- Create: `crates/dashboard/index.html`
- Create: `crates/dashboard/app.css` (placeholder; real design lands in Task 4)
- Create: `scripts/build-dashboard.sh`
- Create: `crates/dashboard/.gitignore` (ignore `target/`)

**Interfaces:**
- Consumes: nothing.
- Produces: a runnable `scripts/build-dashboard.sh` that writes `crates/api/dashboard-v2/{index.html,app.css,prospero-dashboard.js,prospero-dashboard_bg.wasm}`.

- [ ] **Step 1: Exclude the crate from the workspace**

In root `Cargo.toml`, under `[workspace]`:

```toml
[workspace]
resolver = "3"
members = ["crates/*"]
# The Dioxus dashboard (#97) targets wasm32 only and is intentionally NOT a
# workspace member: `cargo {build,clippy,test} --workspace` and the 85% coverage
# gate (scripts/coverage.sh) all run over members, and a wasm-only UI crate would
# either fail the host-target build or sink measured coverage. It builds via
# scripts/build-dashboard.sh and has its own CI job (.github/workflows/ci.yml).
exclude = ["crates/dashboard"]
```

- [ ] **Step 2: Create the crate manifest**

`crates/dashboard/Cargo.toml` — note the empty `[workspace]` table, which makes this a standalone workspace root:

```toml
[package]
name = "prospero-dashboard"
version = "0.4.0"
edition = "2024"
license = "AGPL-3.0-only"
publish = false

# Standalone workspace root: this crate is excluded from the repo workspace
# (see the root Cargo.toml) and resolves its own Cargo.lock.
[workspace]

[dependencies]
prospero-types = { path = "../types" }
dioxus = { version = "0.7", features = ["web"] }
gloo-net = { version = "0.7", features = ["json"] }
serde_json = "1"
console_error_panic_hook = "0.1"

[profile.release]
opt-level = "z"
lto = true
codegen-units = 1
panic = "abort"
```

- [ ] **Step 3: Minimal app that proves the DTO round-trip compiles**

`crates/dashboard/src/main.rs`:

```rust
//! Prospero Dashboard v2 — Dioxus/WASM single-page app (#97, epic #95).

use dioxus::prelude::*;

fn main() {
    console_error_panic_hook::set_once();
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    rsx! { main { "prospero dashboard v2" } }
}
```

- [ ] **Step 4: Write the build script**

`scripts/build-dashboard.sh` (chmod +x). It resolves a wasm-capable cargo, guards
crate/CLI version skew, and writes the bundle:

```bash
#!/usr/bin/env bash
# Build the Dioxus/WASM dashboard (#97) and write the embeddable bundle to
# crates/api/dashboard-v2/. Single entrypoint for humans and CI, matching how
# scripts/coverage.sh works.
set -euo pipefail
cd "$(dirname "$0")/.."

CRATE_DIR="crates/dashboard"
OUT_DIR="crates/api/dashboard-v2"
TARGET="wasm32-unknown-unknown"

# 1. Resolve a cargo that can target wasm32. The repo's default toolchain may be
#    a Homebrew rust with only the host std installed.
resolve_cargo() {
  if [ -n "${CARGO_WASM:-}" ]; then echo "$CARGO_WASM"; return; fi
  for c in "$HOME"/.rustup/toolchains/*/bin/cargo; do
    [ -x "$c" ] && echo "$c" && return
  done
  command -v cargo
}
CARGO="$(resolve_cargo)"
RUSTC="$(dirname "$CARGO")/rustc"
if ! "$RUSTC" --print target-libdir --target "$TARGET" >/dev/null 2>&1; then
  echo "error: $CARGO cannot target $TARGET." >&2
  echo "  Install it with:  rustup target add $TARGET" >&2
  echo "  Or point CARGO_WASM at a cargo that can." >&2
  exit 1
fi

# 2. Build.
( cd "$CRATE_DIR" && "$CARGO" build --release --target "$TARGET" )
WASM="$CRATE_DIR/target/$TARGET/release/prospero_dashboard.wasm"

# 3. Guard wasm-bindgen crate/CLI skew — the #1 failure mode in this toolchain,
#    and it surfaces as opaque runtime errors in the browser.
LOCK_VER="$(awk '/^name = "wasm-bindgen"$/{f=1;next} f&&/^version = /{gsub(/[",]/,"");print $3;exit}' "$CRATE_DIR/Cargo.lock")"
if ! command -v wasm-bindgen >/dev/null 2>&1; then
  echo "error: wasm-bindgen CLI not found. Install $LOCK_VER:" >&2
  echo "  cargo install wasm-bindgen-cli --version $LOCK_VER --locked" >&2
  echo "  (or: brew install wasm-bindgen, if the version matches)" >&2
  exit 1
fi
CLI_VER="$(wasm-bindgen --version | awk '{print $2}')"
if [ "$CLI_VER" != "$LOCK_VER" ]; then
  echo "error: wasm-bindgen version skew — crate $LOCK_VER, CLI $CLI_VER." >&2
  echo "  cargo install wasm-bindgen-cli --version $LOCK_VER --locked --force" >&2
  exit 1
fi

# 4. Generate the JS glue + processed wasm.
rm -rf "$OUT_DIR"; mkdir -p "$OUT_DIR"
wasm-bindgen --target web --no-typescript \
  --out-dir "$OUT_DIR" --out-name prospero-dashboard "$WASM"

# 5. Shrink when binaryen is available; never required.
if command -v wasm-opt >/dev/null 2>&1; then
  wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int \
    "$OUT_DIR/prospero-dashboard_bg.wasm" -o "$OUT_DIR/prospero-dashboard_bg.wasm"
else
  echo "note: wasm-opt not found (brew install binaryen) — skipping size pass."
fi

# 6. Static assets.
cp "$CRATE_DIR/index.html" "$CRATE_DIR/app.css" "$OUT_DIR/"
echo "dashboard bundle written to $OUT_DIR ($(du -h "$OUT_DIR/prospero-dashboard_bg.wasm" | cut -f1) wasm)"
```

- [ ] **Step 5: Write the bundle's HTML entrypoint**

`crates/dashboard/index.html` — absolute asset URLs, inline SVG favicon so no
`/favicon.ico` 404 (the bug #119 already fixed for v1):

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Prospero — Fleet</title>
    <link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E%3Ccircle cx='8' cy='8' r='7' fill='%23c792ea'/%3E%3C/svg%3E" />
    <link rel="stylesheet" href="/v2/app.css" />
  </head>
  <body>
    <div id="main"></div>
    <script type="module">
      import init from "/v2/prospero-dashboard.js";
      init();
    </script>
  </body>
</html>
```

- [ ] **Step 6: Run the build and verify all four artifacts exist**

Run: `scripts/build-dashboard.sh && ls -la crates/api/dashboard-v2/`
Expected: `index.html`, `app.css`, `prospero-dashboard.js`, `prospero-dashboard_bg.wasm` all present.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/dashboard scripts/build-dashboard.sh crates/api/dashboard-v2
git commit -m "feat(dashboard): scaffold excluded Dioxus wasm crate + build script (#97)"
```

---

### Task 2: Embed and serve the bundle at `/v2`

**Files:**
- Create: `crates/api/src/dashboard_v2.rs`
- Modify: `crates/api/src/lib.rs` (declare module, add four routes)
- Test: `crates/api/tests/dashboard_v2.rs`

**Interfaces:**
- Consumes: the bundle written by `scripts/build-dashboard.sh` (Task 1).
- Produces: `dashboard_v2::{index, app_css, app_js, app_wasm}` axum handlers; `pub const CSP: &str`.

- [ ] **Step 1: Write the failing route tests**

`crates/api/tests/dashboard_v2.rs`:

```rust
//! Serving tests for the Dashboard v2 bundle (#97): the /v2 routes exist with
//! the right content types and CSP, and the v1 dashboard is untouched.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

mod common;

fn header(res: &axum::response::Response, name: &str) -> String {
    res.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

async fn get(path: &str) -> axum::response::Response {
    let app = common::test_router().await;
    app.oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn v2_index_serves_html_with_locked_down_csp() {
    let res = get("/v2").await;
    assert_eq!(res.status(), StatusCode::OK);
    assert!(header(&res, "content-type").starts_with("text/html"));
    let csp = header(&res, "content-security-policy");
    assert!(csp.contains("default-src 'none'"), "csp was: {csp}");
    assert!(csp.contains("'wasm-unsafe-eval'"), "csp was: {csp}");
}

#[tokio::test]
async fn v2_serves_wasm_with_the_correct_mime() {
    let res = get("/v2/prospero-dashboard_bg.wasm").await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(header(&res, "content-type"), "application/wasm");
}

#[tokio::test]
async fn v2_serves_js_and_css() {
    let js = get("/v2/prospero-dashboard.js").await;
    assert_eq!(js.status(), StatusCode::OK);
    assert!(header(&js, "content-type").starts_with("application/javascript"));
    let css = get("/v2/app.css").await;
    assert_eq!(css.status(), StatusCode::OK);
    assert!(header(&css, "content-type").starts_with("text/css"));
}

#[tokio::test]
async fn v1_dashboard_is_unaffected() {
    let res = get("/").await;
    assert_eq!(res.status(), StatusCode::OK);
    assert!(header(&res, "content-type").starts_with("text/html"));
    let res = get("/app.js").await;
    assert_eq!(res.status(), StatusCode::OK);
}
```

If `crates/api/tests/common/` does not already expose a router builder, reuse the
construction used by the existing api integration tests in `crates/api/tests/`
rather than inventing a second harness.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p prospero-api --test dashboard_v2 --features prospero-core/testkit`
Expected: FAIL — routes 404.

- [ ] **Step 3: Implement the serving module**

`crates/api/src/dashboard_v2.rs`:

```rust
//! Serve the embedded Dashboard v2 bundle (Dioxus/WASM, #97).
//!
//! The bundle under `../dashboard-v2/` is a build artifact committed to the repo
//! and regenerated by `scripts/build-dashboard.sh`. Committing it keeps the
//! "one binary ships the UI" property without requiring a wasm toolchain for an
//! ordinary `cargo build`; CI reruns the build and diffs the tree so a stale
//! bundle fails the build rather than shipping.

use axum::http::header;
use axum::response::{IntoResponse, Response};

const INDEX_HTML: &str = include_str!("../dashboard-v2/index.html");
const APP_CSS: &str = include_str!("../dashboard-v2/app.css");
const APP_JS: &str = include_str!("../dashboard-v2/prospero-dashboard.js");
const APP_WASM: &[u8] = include_bytes!("../dashboard-v2/prospero-dashboard_bg.wasm");

/// Content-Security-Policy for the v2 page. The bundle is fully self-contained
/// (no CDN, font, or remote image), so everything is denied except same-origin
/// script/style/fetch. `'wasm-unsafe-eval'` is what permits WebAssembly
/// instantiation — without it the module will not start.
pub const CSP: &str = "default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; \
style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; \
form-action 'none'";

/// `GET /v2` — the dashboard v2 page.
pub async fn index() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CONTENT_SECURITY_POLICY, CSP),
        ],
        INDEX_HTML,
    )
        .into_response()
}

/// `GET /v2/app.css` — the design-token stylesheet.
pub async fn app_css() -> Response {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        APP_CSS,
    )
        .into_response()
}

/// `GET /v2/prospero-dashboard.js` — the wasm-bindgen JS glue.
pub async fn app_js() -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        APP_JS,
    )
        .into_response()
}

/// `GET /v2/prospero-dashboard_bg.wasm` — the compiled module. The MIME must be
/// exactly `application/wasm` or `WebAssembly.instantiateStreaming` refuses it.
pub async fn app_wasm() -> Response {
    ([(header::CONTENT_TYPE, "application/wasm")], APP_WASM).into_response()
}
```

- [ ] **Step 4: Wire the routes**

In `crates/api/src/lib.rs`, add `pub mod dashboard_v2;` beside `pub mod dashboard;`,
then after the existing `/app.js` route:

```rust
        // Dashboard v2 (Dioxus/WASM, #97) — served alongside v1 during the
        // transition so `/` is unaffected.
        .route("/v2", get(dashboard_v2::index))
        .route("/v2/app.css", get(dashboard_v2::app_css))
        .route("/v2/prospero-dashboard.js", get(dashboard_v2::app_js))
        .route(
            "/v2/prospero-dashboard_bg.wasm",
            get(dashboard_v2::app_wasm),
        )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p prospero-api --test dashboard_v2 --features prospero-core/testkit`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add crates/api/src/dashboard_v2.rs crates/api/src/lib.rs crates/api/tests/dashboard_v2.rs
git commit -m "feat(api): embed + serve the Dashboard v2 bundle at /v2 (#97)"
```

---

### Task 3: View-model layer with host-target unit tests

**Files:**
- Create: `crates/dashboard/src/view_model.rs`
- Modify: `crates/dashboard/src/main.rs` (declare the module)

**Interfaces:**
- Consumes: `prospero_types::{FleetSnapshot, Workspace, Agent, AgentStatus, WorkspaceHealth}`.
- Produces:
  - `pub struct StatusCounts { pub running: usize, pub idle: usize, pub spawning: usize, pub terminal: usize }`
  - `pub fn count_statuses(agents: &[Agent]) -> StatusCounts`
  - `pub struct FleetTotals { pub workspaces: usize, pub agents: usize, pub healthy: usize, pub unreachable: usize, pub statuses: StatusCounts }`
  - `pub fn totals(snap: &FleetSnapshot) -> FleetTotals`
  - `pub fn status_label(s: AgentStatus) -> &'static str`
  - `pub fn status_tone(s: AgentStatus) -> &'static str` (returns a CSS tone token: `"live" | "wait" | "done" | "bad"`)
  - `pub fn short_id(id: &str) -> &str` (first 8 chars, char-boundary safe)
  - `pub fn is_healthy(h: &WorkspaceHealth) -> bool`

Rationale: this module carries every piece of derived logic and compiles for the
**host** target, so it is unit-testable with plain `cargo test` — no headless
browser. The rendering layer stays a thin projection over it.

- [ ] **Step 1: Write the failing tests**

At the bottom of `crates/dashboard/src/view_model.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use prospero_types::{Agent, AgentStatus, FleetSnapshot, Workspace, WorkspaceHealth};

    fn agent(id: &str, status: AgentStatus) -> Agent {
        Agent {
            id: id.into(),
            name: "a".into(),
            workspace: "ws".into(),
            status,
            started_at: "2026-08-01T00:00:00Z".into(),
            isolated: true,
            interactive: false,
            session_dir: "/s".into(),
        }
    }

    fn workspace(name: &str, health: WorkspaceHealth, agents: Vec<Agent>) -> Workspace {
        Workspace {
            name: name.into(),
            root: "/r".into(),
            sources: vec![],
            health,
            config: Default::default(),
            agents,
        }
    }

    #[test]
    fn counts_partition_agents_by_status() {
        let c = count_statuses(&[
            agent("a", AgentStatus::Running),
            agent("b", AgentStatus::Running),
            agent("c", AgentStatus::Idle),
            agent("d", AgentStatus::Spawning),
            agent("e", AgentStatus::Done),
            agent("f", AgentStatus::Failed),
        ]);
        assert_eq!(c.running, 2);
        assert_eq!(c.idle, 1);
        assert_eq!(c.spawning, 1);
        assert_eq!(c.terminal, 2);
    }

    #[test]
    fn totals_aggregate_across_workspaces_and_health() {
        let snap = FleetSnapshot {
            host: "local".into(),
            workspaces: vec![
                workspace("a", WorkspaceHealth::Healthy, vec![agent("1", AgentStatus::Running)]),
                workspace(
                    "b",
                    WorkspaceHealth::Unreachable { reason: "no socket".into() },
                    vec![agent("2", AgentStatus::Idle), agent("3", AgentStatus::Done)],
                ),
            ],
        };
        let t = totals(&snap);
        assert_eq!(t.workspaces, 2);
        assert_eq!(t.agents, 3);
        assert_eq!(t.healthy, 1);
        assert_eq!(t.unreachable, 1);
        assert_eq!(t.statuses.running, 1);
        assert_eq!(t.statuses.idle, 1);
        assert_eq!(t.statuses.terminal, 1);
    }

    #[test]
    fn totals_of_an_empty_fleet_are_all_zero() {
        let t = totals(&FleetSnapshot { host: "local".into(), workspaces: vec![] });
        assert_eq!(t.workspaces, 0);
        assert_eq!(t.agents, 0);
        assert_eq!(t.healthy, 0);
        assert_eq!(t.unreachable, 0);
    }

    #[test]
    fn every_status_has_a_label_and_a_known_tone() {
        for s in [
            AgentStatus::Spawning,
            AgentStatus::Running,
            AgentStatus::Idle,
            AgentStatus::Killed,
            AgentStatus::Done,
            AgentStatus::Failed,
            AgentStatus::Crashed,
        ] {
            assert!(!status_label(s).is_empty());
            assert!(matches!(status_tone(s), "live" | "wait" | "done" | "bad"));
        }
    }

    #[test]
    fn short_id_truncates_and_never_splits_a_char() {
        assert_eq!(short_id("abcdefghijkl"), "abcdefgh");
        assert_eq!(short_id("abc"), "abc");
        // Multi-byte input must not panic on a non-boundary slice.
        let s = "ααααααααα";
        assert!(s.starts_with(short_id(s)));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd crates/dashboard && cargo test`
Expected: FAIL — items not found.

- [ ] **Step 3: Implement `view_model.rs`**

Implement exactly the interface listed above. `count_statuses` maps
`Killed | Done | Failed | Crashed` into `terminal` (reuse
`AgentStatus::is_terminal()` from `prospero-types` rather than rewriting the
match). `short_id` truncates at 8 chars using `char_indices` so multi-byte input
cannot panic. `status_tone` maps `Running | Spawning → "live"`,
`Idle → "wait"`, `Done → "done"`, `Killed | Failed | Crashed → "bad"`.

Add `mod view_model;` to `main.rs`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd crates/dashboard && cargo test`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/dashboard/src
git commit -m "feat(dashboard): view-model layer with host-target unit tests (#97)"
```

---

### Task 4: Design system and app shell

**Files:**
- Rewrite: `crates/dashboard/app.css`
- Create: `crates/dashboard/src/ui.rs`
- Modify: `crates/dashboard/src/main.rs`

**Interfaces:**
- Consumes: `view_model::{FleetTotals, StatusCounts, totals, count_statuses, status_label, status_tone, short_id, is_healthy}`.
- Produces: `ui::{Shell, StatRow, WorkspaceCard, StatusPill, EmptyState, ErrorState}` Dioxus components.

**REQUIRED SUB-SKILL:** Use `frontend-design` for this task.

Design requirements, from the approved spec:

- A design-token layer at the top of `app.css` — color, space, type scale,
  radius, elevation — that every rule below derives from. No ad-hoc hex values
  outside the token block.
- Light **and** dark, via `prefers-color-scheme`. Both must be deliberate, not
  one inverted.
- Sans-serif for chrome; monospace confined to data (ids, paths, counts,
  timestamps).
- App shell: header (product mark, host, live indicator), sidebar nav, content
  region containing a stat row and a workspace card grid.
- Workspace card: name, health, source count, agent count, and a status
  distribution.
- Status is encoded by **shape and text as well as hue**, so it survives
  colorblindness and greyscale.
- Responsive: the sidebar collapses under a narrow viewport; nothing scrolls
  horizontally.
- Self-contained: no external font, script, image, or stylesheet — the CSP
  forbids them and the page must render correctly offline.

- [ ] **Step 1: Invoke the frontend-design skill and write `app.css` + `ui.rs`**

- [ ] **Step 2: Build the bundle and confirm it compiles**

Run: `scripts/build-dashboard.sh`
Expected: bundle written, no compile errors.

- [ ] **Step 3: Commit**

```bash
git add crates/dashboard crates/api/dashboard-v2
git commit -m "feat(dashboard): design-token system + app shell components (#97)"
```

---

### Task 5: Fetch live fleet data and wire the states

**Files:**
- Modify: `crates/dashboard/src/main.rs`
- Create: `crates/dashboard/src/api.rs`

**Interfaces:**
- Consumes: `ui::*`, `view_model::totals`, `prospero_types::FleetSnapshot`.
- Produces: `api::fetch_fleet() -> Result<FleetSnapshot, String>`; `enum Load { Loading, Ready(FleetSnapshot), Error(String) }`.

- [ ] **Step 1: Implement the fetch**

`crates/dashboard/src/api.rs` — `gloo_net::http::Request::get("/api/fleet")`,
`.send().await`, check `res.ok()`, then `res.json::<FleetSnapshot>().await`.
Map every error into a human-readable `String`; include the HTTP status when the
response is not ok. This is the load-bearing shared-DTO proof: the response
deserializes into the exact type `prospero-api` serializes.

- [ ] **Step 2: Wire the three states in `App`**

Use a `use_resource` (or `use_signal` + `spawn`) that fetches on mount and then
polls every 5 seconds.

- `Loading` on first load → skeleton/loading state.
- `Ready(snap)` → the shell with live data.
- `Error(e)` on a *first* load → `ErrorState` with the message and a retry.
- An error on a *refresh* after a successful load must keep rendering the last
  good snapshot with a staleness indicator. Blanking a working screen on one
  failed poll is a regression against v1, which is exactly the kind of thing
  this scaffold must not ship.

- [ ] **Step 3: Build, then verify live against a running daemon**

```bash
scripts/build-dashboard.sh
cargo run -p prospero-daemon &
```

Open `http://127.0.0.1:<port>/v2` and confirm: the page renders, workspace and
agent counts match `curl localhost:<port>/api/fleet`, and the browser console is
free of CSP violations and wasm errors.

- [ ] **Step 4: Commit**

```bash
git add crates/dashboard crates/api/dashboard-v2
git commit -m "feat(dashboard): fetch live fleet data from /api/fleet via shared DTO (#97)"
```

---

### Task 6: CI job, docs, and the freshness guard

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `README.md` (or the mdBook guide page covering the dashboard)

**Interfaces:**
- Consumes: `scripts/build-dashboard.sh`.
- Produces: a `dashboard-wasm` CI job.

- [ ] **Step 1: Add the CI job**

Append to `.github/workflows/ci.yml`:

```yaml
  dashboard-wasm:
    name: dashboard v2 wasm build
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - name: Checkout
        uses: actions/checkout@v5
      - name: Install Rust toolchain (+ wasm32 target)
        uses: dtolnay/rust-toolchain@stable
        with:
          targets: wasm32-unknown-unknown
          components: clippy
      - name: Cache cargo registry & target
        uses: Swatinem/rust-cache@v2
        with:
          workspaces: "crates/dashboard"
      # Version must match the wasm-bindgen crate in crates/dashboard/Cargo.lock;
      # scripts/build-dashboard.sh fails loudly on skew.
      - name: Install wasm-bindgen-cli
        uses: taiki-e/install-action@v2
        with:
          tool: wasm-bindgen-cli
      - name: Install binaryen (wasm-opt)
        run: sudo apt-get update && sudo apt-get install -y binaryen
      - name: cargo test -p prospero-dashboard
        run: cd crates/dashboard && cargo test
      - name: cargo clippy -p prospero-dashboard
        run: cd crates/dashboard && cargo clippy --all-targets -- -D warnings
      - name: Build the wasm bundle
        run: scripts/build-dashboard.sh
      # The bundle under crates/api/dashboard-v2/ is a COMMITTED build artifact
      # (it is include_bytes!'d into prosperod so an ordinary `cargo build` needs
      # no wasm toolchain). Rebuilding and diffing proves the committed bytes
      # match their sources — a stale bundle fails the build instead of shipping.
      - name: Committed bundle is fresh
        run: |
          if ! git diff --exit-code --stat crates/api/dashboard-v2/; then
            echo "::error::crates/api/dashboard-v2/ is stale — run scripts/build-dashboard.sh and commit the result"
            exit 1
          fi
          echo "committed dashboard bundle is fresh ✓"
```

- [ ] **Step 2: Document the build**

Add a short section explaining: `/v2` serves Dashboard v2; the bundle is a
committed artifact; regenerate with `scripts/build-dashboard.sh`; it needs the
`wasm32-unknown-unknown` target and a matching `wasm-bindgen` CLI; CI enforces
freshness.

- [ ] **Step 3: Run the full local gate**

```bash
cargo fmt --all -- --check
TESTKIT="--features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s"
cargo clippy --workspace --all-targets $TESTKIT -- -D warnings
cargo build --workspace --all-targets $TESTKIT
cargo test --workspace $TESTKIT
(cd crates/dashboard && cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test)
```

Expected: all pass.

- [ ] **Step 4: Commit and open the PR**

```bash
git add .github/workflows/ci.yml README.md
git commit -m "ci(dashboard): wasm build job + committed-bundle freshness guard (#97)"
git push -u origin worktree-issue-97-dioxus-wasm-scaffold
gh pr create --draft --title "feat(dashboard): scaffold Dioxus WASM crate + build + embed + serve (#97)" --body "..."
```

---

## Self-review

**Spec coverage.** Crate layout + workspace exclusion → Task 1. Build pipeline
and version-skew guard → Task 1. Embedding, routes, MIME types, CSP → Task 2.
Error-handling states → Task 5. UI/UX direction → Task 4. Data flow / shared DTO
→ Task 5. Host-target unit tests → Task 3. API-side serving tests → Task 2. CI
job + freshness guard → Task 6. Out-of-scope items appear in no task, as
intended.

**Type consistency.** `count_statuses`, `totals`, `status_label`, `status_tone`,
`short_id`, `is_healthy`, `StatusCounts`, `FleetTotals` are declared in Task 3's
Interfaces block and used under those exact names in Tasks 4 and 5. Bundle
filenames (`prospero-dashboard.js`, `prospero-dashboard_bg.wasm`) are identical
in the build script, `index.html`, `dashboard_v2.rs`, and the route table.

**Ordering.** Task 2 `include_bytes!`s the bundle, so Task 1 must run first or
`prospero-api` will not compile. Tasks 4 and 5 rewrite the app but change no
interface Task 2 depends on.
