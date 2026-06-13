# Interactive Sub-Agent Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator, through Prospero, launch an **interactive** agent and **send a message** / **end-input** to a running or idle interactive agent — end to end across core → API → CLI → dashboard.

**Architecture:** Mirror caliban's already-shipped interactive contract (ADR-0047 / caliban#81): add `SpawnSpec.interactive` (with a wire-drift guard), plumb it through the spawn path, add an inbound NDJSON send path (`AttachInbound` frames written to the per-agent socket via a fresh write-only connection), and surface two verbs (`/input`, `/end-input`) through the API, CLI, and dashboard.

**Tech Stack:** Rust (axum, tokio, serde) + vanilla-JS dashboard (no build step). Tests via `cargo test` with the `FakeCaliband` harness (`prospero-core/testkit`).

**Design spec:** `docs/superpowers/specs/2026-06-11-interactive-agent-control-handoff.md`

---

## Working notes (read first)

- **Base branch:** this feature builds on the dashboard/provider-config work that is **not yet on `main`** (it references the dashboard launch modal, `SpawnBody`, etc.). Create the feature branch off the current `dashboard-control-plane` HEAD, not `main`.
- **Upstream contract (verified against caliban source, do NOT change caliban):**
  - `SpawnSpec.interactive: bool`, `#[serde(default)]`, last field (caliban `proto.rs:96`).
  - `AttachInbound` (caliban `src/attach.rs:20`): `#[serde(tag = "type")]`, variants `UserMessage { text: String }` and `EndInput` — **no `rename_all`**, so the tags are literally `"UserMessage"` / `"EndInput"`. Wire: `{"type":"UserMessage","text":"…"}` and `{"type":"EndInput"}`.
  - `AgentStatus::Idle` already exists in our `model.rs` — no model-enum change needed.
- **Run core/api tests with the testkit feature** where the fleet/api harness is involved: `cargo test -p prospero-core --features testkit …` and `cargo test -p prospero-api --features prospero-core/testkit …`. Without it, `fleet_integration` / `api_integration` fail to compile (feature-flag artifact, not a real failure).
- **Verification gate** (run before claiming done): `cargo fmt --all` then
  `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings && cargo build --workspace --all-targets && cargo test --workspace --features prospero-core/testkit`.

## File structure

| File | Change | Responsibility |
|------|--------|----------------|
| `crates/core/src/caliband/wire.rs` | Modify | `SpawnSpec.interactive`; `AttachInbound` enum; wire-drift fixture tests. |
| `crates/core/src/caliband/client.rs` | Modify | `send_inbound(socket_path, &AttachInbound)` (fresh write-only connection). |
| `crates/core/src/lib.rs` | Modify | Re-export `AttachInbound`. |
| `crates/core/src/fleet.rs` | Modify | `SpawnRequest.interactive` + `into_spec` passthrough; `send_agent_input`. |
| `crates/core/src/model.rs` | Modify | `Agent.interactive`. |
| `crates/core/src/testkit.rs` | Modify | `test_record` literal gains `interactive`. |
| `crates/api/src/dto.rs` | Modify | `SpawnBody.interactive`; `AgentInputBody`. |
| `crates/api/src/handlers.rs` | Modify | `agent_input` / `agent_end_input` handlers. |
| `crates/api/src/lib.rs` | Modify | two routes. |
| `crates/cli/src/main.rs` | Modify | `spawn --interactive`; `send` / `end-input` subcommands. |
| `crates/api/dashboard/app.js` | Modify | launch-modal interactive checkbox; idle-agent input box. |

---

## Task 1: Mirror `SpawnSpec.interactive` + wire-drift guard

**Files:** Modify `crates/core/src/caliband/wire.rs`, `crates/core/src/fleet.rs`, `crates/core/src/testkit.rs`

- [ ] **Step 1: Write failing tests.** In `crates/core/src/caliband/wire.rs`, in the existing `#[cfg(test)] mod tests`, add:

```rust
    #[test]
    fn spawn_spec_is_wire_compatible_with_caliban_interactive() {
        // Golden JSON in caliban's serialized SpawnSpec form (proto.rs). Pinned
        // so upstream protocol drift on `interactive` fails loudly here.
        let golden = r#"{"label":null,"frontmatter_path":null,"initial_prompt":"hi","model":null,"tool_allowlist":null,"isolation_worktree":false,"inherit_hooks":true,"interactive":true}"#;
        let spec: SpawnSpec = serde_json::from_str(golden).expect("deserialize caliban spec");
        assert!(spec.interactive, "interactive must round-trip from caliban's wire form");
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["interactive"], serde_json::json!(true));
    }

    #[test]
    fn spawn_spec_without_interactive_defaults_false() {
        // Back-compat: a pre-interactive spec (field absent) still deserializes.
        let old = r#"{"initial_prompt":"hi"}"#;
        let spec: SpawnSpec = serde_json::from_str(old).unwrap();
        assert!(!spec.interactive);
    }
```

- [ ] **Step 2: Run tests, verify they fail.** Run: `cargo test -p prospero-core spawn_spec_ 2>&1 | tail -20`. Expected: compile error (`SpawnSpec` has no field `interactive`).

- [ ] **Step 3: Implement.** In `crates/core/src/caliband/wire.rs`, add the field to `SpawnSpec` (after `inherit_hooks`, matching caliban's order):

```rust
    /// When true, the worker runs in interactive mode: at each end-of-run
    /// boundary it awaits inbound operator messages over the per-agent socket
    /// instead of finishing. Mirrors caliban `SpawnSpec.interactive`.
    #[serde(default)]
    pub interactive: bool,
```

The new field breaks the two `SpawnSpec { … }` literals — fix both to keep compiling:
- `crates/core/src/fleet.rs:58` (`into_spec`): add `interactive: false,` (Task 2 makes it pass through the request).
- `crates/core/src/testkit.rs:309` (`test_record`): add `interactive: false,`.

- [ ] **Step 4: Run tests, verify they pass.** Run: `cargo test -p prospero-core --features testkit spawn_spec_ 2>&1 | tail -10` and `cargo build -p prospero-core --features testkit 2>&1 | tail -2`. Expected: both green.

- [ ] **Step 5: Commit.**
```bash
git add crates/core/src/caliband/wire.rs crates/core/src/fleet.rs crates/core/src/testkit.rs
git commit -m "feat(core): mirror SpawnSpec.interactive + wire-drift guard"
```

---

## Task 2: Plumb `interactive` through the spawn path (core + API + CLI + dashboard)

**Files:** Modify `crates/core/src/fleet.rs`, `crates/api/src/dto.rs`, `crates/cli/src/main.rs`, `crates/api/dashboard/app.js`

- [ ] **Step 1: Write failing tests.**

In `crates/api/src/dto.rs`'s `#[cfg(test)] mod tests` (create one at the end of the file if absent), add:

```rust
    #[test]
    fn spawn_body_interactive_round_trips_and_defaults_false() {
        let with: SpawnBody = serde_json::from_str(r#"{"prompt":"p","interactive":true}"#).unwrap();
        assert!(with.into_request().interactive);
        let without: SpawnBody = serde_json::from_str(r#"{"prompt":"p"}"#).unwrap();
        assert!(!without.into_request().interactive);
    }
```

In `crates/cli/src/main.rs`'s test module, add:

```rust
    #[test]
    fn spawn_interactive_flag_parses() {
        let cli = Cli::parse_from(["prospero", "spawn", "r", "p", "--interactive"]);
        match cli.command {
            Command::Spawn(a) => assert!(a.interactive),
            other => panic!("expected spawn, got {other:?}"),
        }
    }
```

- [ ] **Step 2: Run tests, verify they fail.** Run: `cargo test -p prospero-api spawn_body_interactive 2>&1 | tail -10` and `cargo test -p prospero-cli spawn_interactive_flag 2>&1 | tail -10`. Expected: compile errors (`interactive` missing on `SpawnRequest`/`SpawnBody`/`SpawnArgs`).

- [ ] **Step 3a: Core `SpawnRequest` (fleet.rs).** Add the field to `SpawnRequest` (after `tool_allowlist`):

```rust
    /// Run in interactive mode (the worker awaits operator input instead of
    /// finishing). Defaults to `false` via [`SpawnRequest::new`].
    pub interactive: bool,
```

In `SpawnRequest::new`, add `interactive: false,` to the struct literal. In `into_spec`, change the `interactive: false,` line (added in Task 1) to:

```rust
            interactive: self.interactive,
```

- [ ] **Step 3b: DTO (dto.rs).** Add to `SpawnBody` (after `tool_allowlist`):

```rust
    /// Run the agent in interactive mode (awaits operator input).
    #[serde(default)]
    pub interactive: bool,
```

In `SpawnBody::into_request`, add `interactive: self.interactive,` to the `SpawnRequest { … }` it returns.

- [ ] **Step 3c: CLI (main.rs).** Add to `SpawnArgs` (after `shared_tree`):

```rust
    /// Run the agent in interactive mode (it awaits your input instead of finishing).
    #[arg(long)]
    interactive: bool,
```

In the `Command::Spawn(a)` arm, after the `body["isolation"] = …` line, add:

```rust
            if a.interactive {
                body["interactive"] = true.into();
            }
```

- [ ] **Step 3d: Dashboard launch modal (app.js).** In `openLaunchModal`, the form HTML has the worktree checkbox `<label class="chk"><input type="checkbox" id="la-wt" checked> worktree isolation</label>`. Add an interactive checkbox immediately after it (same template string):

```js
    `<label class="chk"><input type="checkbox" id="la-interactive"> interactive (awaits your input)</label>` +
```

In the submit handler, where `body` is assembled (after `if (!form.querySelector("#la-wt").checked) body.isolation = "shared";`), add:

```js
    if (form.querySelector("#la-interactive").checked) body.interactive = true;
```

- [ ] **Step 4: Run tests, verify they pass.** Run: `cargo test -p prospero-api spawn_body_interactive 2>&1 | tail -6`, `cargo test -p prospero-cli spawn_interactive_flag 2>&1 | tail -6`, `node --check crates/api/dashboard/app.js && echo js-ok`. Expected: tests pass, js-ok.

- [ ] **Step 5: Commit.**
```bash
git add crates/core/src/fleet.rs crates/api/src/dto.rs crates/cli/src/main.rs crates/api/dashboard/app.js
git commit -m "feat: plumb interactive through spawn path (core/api/cli/dashboard)"
```

---

## Task 3: `AttachInbound` wire type + client send path

**Files:** Modify `crates/core/src/caliband/wire.rs`, `crates/core/src/caliband/client.rs`, `crates/core/src/lib.rs`

- [ ] **Step 1: Write failing tests.** In `crates/core/src/caliband/wire.rs` tests module, add:

```rust
    #[test]
    fn attach_inbound_user_message_serializes() {
        let j = serde_json::to_string(&AttachInbound::UserMessage { text: "hi there".into() }).unwrap();
        assert_eq!(j, r#"{"type":"UserMessage","text":"hi there"}"#);
    }

    #[test]
    fn attach_inbound_end_input_serializes() {
        let j = serde_json::to_string(&AttachInbound::EndInput).unwrap();
        assert_eq!(j, r#"{"type":"EndInput"}"#);
    }
```

In `crates/core/src/caliband/client.rs`, add a `#[cfg(test)] mod tests` at the end of the file (if one already exists, add to it):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::caliband::wire::AttachInbound;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn send_inbound_writes_one_ndjson_frame() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("a.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut line = String::new();
            BufReader::new(stream).read_line(&mut line).await.unwrap();
            line
        });
        CalibandClient::send_inbound(&sock, &AttachInbound::UserMessage { text: "go".into() })
            .await
            .unwrap();
        assert_eq!(server.await.unwrap().trim_end(), r#"{"type":"UserMessage","text":"go"}"#);
    }
}
```

- [ ] **Step 2: Run tests, verify they fail.** Run: `cargo test -p prospero-core --features testkit attach_inbound 2>&1 | tail -12` and `cargo test -p prospero-core --features testkit send_inbound_writes 2>&1 | tail -12`. Expected: compile errors (`AttachInbound`, `send_inbound` undefined).

- [ ] **Step 3a: `AttachInbound` (wire.rs).** Add near the other mirrored types:

```rust
/// Inbound control frames written to an interactive agent's per-agent socket.
/// Mirrors caliban `AttachInbound` (`caliban/src/attach.rs`); the outbound
/// stream stays caliban stream-json, so the two never share a direction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AttachInbound {
    /// Inject a user message and resume the run.
    UserMessage {
        /// Message text.
        text: String,
    },
    /// Signal end-of-input: the agent finishes after this.
    EndInput,
}
```

- [ ] **Step 3b: Re-export (lib.rs).** In `crates/core/src/lib.rs`, add `AttachInbound` to the caliband wire re-exports (match the existing `pub use` style; e.g. alongside where `SpawnSpec`/wire types are surfaced). If wire types aren't re-exported at the crate root, add:

```rust
pub use caliband::wire::AttachInbound;
```

- [ ] **Step 3c: `send_inbound` (client.rs).** Ensure the write trait is imported — at the top of `client.rs`, the imports include `tokio::io::BufReader`; add `AsyncWriteExt`:

```rust
use tokio::io::AsyncWriteExt;
```

Add the method to `impl CalibandClient`, right after `open_stream`:

```rust
    /// Write a single inbound control frame to an interactive agent's per-agent
    /// socket (path from [`Self::attach`]). Opens a fresh write-only connection,
    /// matching caliban's "all attach connections feed a shared inbox" model.
    pub async fn send_inbound(socket_path: &Path, frame: &AttachInbound) -> Result<()> {
        let mut stream = UnixStream::connect(socket_path).await.map_err(|source| {
            CoreError::CalibandUnreachable {
                path: socket_path.display().to_string(),
                source,
            }
        })?;
        let mut line = serde_json::to_vec(frame)?;
        line.push(b'\n');
        stream
            .write_all(&line)
            .await
            .map_err(|source| CoreError::CalibandUnreachable {
                path: socket_path.display().to_string(),
                source,
            })?;
        let _ = stream.flush().await;
        Ok(())
    }
```

Add `use crate::caliband::wire::AttachInbound;` to client.rs's imports if `AttachInbound` is not already in scope (the file already imports other `wire` types — extend that `use`).

- [ ] **Step 4: Run tests, verify they pass.** Run: `cargo test -p prospero-core --features testkit attach_inbound 2>&1 | tail -8` and `cargo test -p prospero-core --features testkit send_inbound_writes 2>&1 | tail -8`. Expected: all pass.

- [ ] **Step 5: Commit.**
```bash
git add crates/core/src/caliband/wire.rs crates/core/src/caliband/client.rs crates/core/src/lib.rs
git commit -m "feat(core): AttachInbound wire type + client send_inbound path"
```

---

## Task 4: `Agent.interactive` projection + `FleetManager::send_agent_input`

**Files:** Modify `crates/core/src/model.rs`, `crates/core/src/fleet.rs`

- [ ] **Step 1: Write failing test.** Add to `crates/core/src/fleet.rs`'s test module (mirror the harness setup from the existing `restart_caliband_shuts_down_and_clears_client` test — pin `discovery_env.caliban_daemon_runtime_dir`, `autostart=false`, a `FakeCaliband` at the repo socket):

```rust
    #[tokio::test]
    async fn send_agent_input_rejects_terminal_and_non_interactive() {
        use crate::caliband::wire::AttachInbound;
        use crate::testkit::{test_record, FakeCaliband};
        use crate::model::AgentStatus;

        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
        config.ensure.autostart = false;
        let root = dir.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        let socket = crate::discovery::resolve_socket(&root, &config.discovery_env).unwrap();

        let mut fake = FakeCaliband::start_at(&socket).await.unwrap();
        // Terminal agent (Done), interactive — must reject as terminal.
        let mut done = test_record("ag-done", dir.path(), AgentStatus::Done, false);
        done.spec.interactive = true;
        fake.add_agent(done, vec![]).await;
        // Idle agent, NOT interactive — must reject as non-interactive.
        let idle = test_record("ag-idle", dir.path(), AgentStatus::Idle, false);
        fake.add_agent(idle, vec![]).await;

        let store = std::sync::Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).unwrap();
        mgr.add_repo("repo", &root).await.unwrap();
        mgr.poll_repo_once("repo").await;

        let r1 = mgr.send_agent_input("ag-done", AttachInbound::EndInput).await;
        assert!(matches!(r1, Err(CoreError::InvalidState { .. })), "terminal must reject");
        let r2 = mgr.send_agent_input("ag-idle", AttachInbound::EndInput).await;
        assert!(matches!(r2, Err(CoreError::InvalidState { .. })), "non-interactive must reject");
        let r3 = mgr.send_agent_input("nope", AttachInbound::EndInput).await;
        assert!(matches!(r3, Err(CoreError::AgentNotFound(_))), "unknown id must 404");
    }
```

- [ ] **Step 2: Run test, verify it fails.** Run: `cargo test -p prospero-core --features testkit send_agent_input_rejects 2>&1 | tail -15`. Expected: compile error (`Agent` has no `interactive`; `send_agent_input` undefined).

- [ ] **Step 3a: `Agent.interactive` (model.rs).** Add to `struct Agent` (after `isolated`):

```rust
    /// True if the agent was spawned in interactive mode (accepts operator input).
    pub interactive: bool,
```

In `crates/core/src/fleet.rs`, the snapshot projection builds `Agent { … isolated: rec.spec.isolation_worktree, … }`. Add `interactive: rec.spec.interactive,` to that literal. (If any other `Agent { … }` literal exists — e.g. in a test — add `interactive: false,` to it so it compiles.)

- [ ] **Step 3b: `send_agent_input` (fleet.rs).** Add `use crate::caliband::wire::AttachInbound;` to fleet.rs imports (it already imports `wire::{AgentRecord, SpawnSpec}` — extend that line). Add the method inside `impl FleetManager` (near `kill_agent`):

```rust
    /// Send an inbound control frame to an interactive agent. Rejects if the
    /// agent is unknown (`AgentNotFound`), terminal, or was not spawned
    /// interactive (`InvalidState`).
    pub async fn send_agent_input(&self, agent_id: &str, input: AttachInbound) -> Result<()> {
        let (repo, interactive, terminal) = {
            let snap = self.inner.snapshot.read().await;
            let (repo, agent) = snap
                .find_agent(agent_id)
                .ok_or_else(|| CoreError::AgentNotFound(agent_id.to_string()))?;
            (repo.to_string(), agent.interactive, agent.status.is_terminal())
        };
        if terminal {
            return Err(CoreError::InvalidState {
                op: "send_input".into(),
                id: agent_id.to_string(),
                status: "terminal".into(),
            });
        }
        if !interactive {
            return Err(CoreError::InvalidState {
                op: "send_input".into(),
                id: agent_id.to_string(),
                status: "not interactive".into(),
            });
        }
        let client = self.client_for(&repo).await?;
        let socket = client.attach(agent_id).await?;
        CalibandClient::send_inbound(&socket, &input).await
    }
```

- [ ] **Step 4: Run test, verify it passes.** Run: `cargo test -p prospero-core --features testkit send_agent_input_rejects 2>&1 | tail -8`, then the full core suite `cargo test -p prospero-core --features testkit 2>&1 | tail -12`. Expected: all green (no regression from the new `Agent` field).

- [ ] **Step 5: Commit.**
```bash
git add crates/core/src/model.rs crates/core/src/fleet.rs
git commit -m "feat(core): Agent.interactive projection + send_agent_input (with rejects)"
```

---

## Task 5: API endpoints — `POST /input`, `POST /end-input`

**Files:** Modify `crates/api/src/dto.rs`, `crates/api/src/handlers.rs`, `crates/api/src/lib.rs`, `crates/api/tests/api_integration.rs`

- [ ] **Step 1: Write failing test.** Add to `crates/api/tests/api_integration.rs` (mirror the file's `setup()`/`oneshot`/`json_body` harness; `Harness` exposes `fake` and `manager`). The happy path needs an interactive idle agent whose per-agent socket is reachable, so add it via the harness fake:

```rust
#[tokio::test]
async fn agent_input_and_end_input_and_rejects() {
    use prospero_core::model::AgentStatus;
    use prospero_core::testkit::test_record;

    let mut h = setup().await; // registers "repo" with a FakeCaliband, autostart off
    // Interactive, idle agent with a reachable per-agent socket.
    let mut rec = test_record("ag1", h.runtime_dir_path(), AgentStatus::Idle, false);
    rec.spec.interactive = true;
    h.fake.add_agent(rec, vec![]).await;
    h.manager.poll_repo_once("repo").await;

    // Happy path: POST /input
    let resp = h.router.clone().oneshot(
        axum::http::Request::builder()
            .method("POST")
            .uri("/api/agents/ag1/input")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(r#"{"text":"also check the tests"}"#))
            .unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::ACCEPTED);

    // Happy path: POST /end-input (no body)
    let resp = h.router.clone().oneshot(
        axum::http::Request::builder()
            .method("POST")
            .uri("/api/agents/ag1/end-input")
            .body(axum::body::Body::empty())
            .unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::ACCEPTED);

    // Unknown id → 404
    let resp = h.router.clone().oneshot(
        axum::http::Request::builder()
            .method("POST").uri("/api/agents/nope/input")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(r#"{"text":"x"}"#)).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);
}
```

> Add a tiny accessor to the `Harness` so the test can locate the runtime dir for the agent socket: in `setup()`/`Harness`, expose the runtime dir path. If `Harness` keeps the runtime `TempDir` as `_runtime`, add `fn runtime_dir_path(&self) -> &std::path::Path { self._runtime.path() }` and rename the field to non-`_` if needed, OR construct `rec` with `test_record("ag1", &h._runtime.path().to_path_buf(), …)` directly if the field is accessible. Match the file's actual field names — read `setup()` first.

- [ ] **Step 2: Run test, verify it fails.** Run: `cargo test -p prospero-api --features prospero-core/testkit agent_input_and_end_input 2>&1 | tail -15`. Expected: 404 on the routes (handlers/routes don't exist yet).

- [ ] **Step 3a: DTO (dto.rs).** Add:

```rust
/// Body for `POST /api/agents/{id}/input`.
#[derive(Debug, Deserialize)]
pub struct AgentInputBody {
    /// Message text to inject into the interactive agent.
    pub text: String,
}
```

- [ ] **Step 3b: Handlers (handlers.rs).** Import `AttachInbound` and `AgentInputBody` (extend the existing `use crate::dto::{…}` and add `use prospero_core::AttachInbound;`). Add next to `kill_agent`/`respawn_agent`:

```rust
/// `POST /api/agents/{id}/input` — inject a user message into an interactive agent.
pub async fn agent_input(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<AgentInputBody>,
) -> Result<StatusCode, ApiError> {
    st.manager
        .send_agent_input(&id, AttachInbound::UserMessage { text: body.text })
        .await?;
    Ok(StatusCode::ACCEPTED)
}

/// `POST /api/agents/{id}/end-input` — signal end-of-input to an interactive agent.
pub async fn agent_end_input(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    st.manager
        .send_agent_input(&id, AttachInbound::EndInput)
        .await?;
    Ok(StatusCode::ACCEPTED)
}
```

- [ ] **Step 3c: Routes (lib.rs).** Add after the `/api/agents/{id}/respawn` route:

```rust
        .route("/api/agents/{id}/input", post(handlers::agent_input))
        .route("/api/agents/{id}/end-input", post(handlers::agent_end_input))
```

- [ ] **Step 4: Run test, verify it passes.** Run: `cargo test -p prospero-api --features prospero-core/testkit agent_input_and_end_input 2>&1 | tail -8`, then `cargo test --workspace --features prospero-core/testkit 2>&1 | tail -12`. Expected: all green.

- [ ] **Step 5: Commit.**
```bash
git add crates/api/src/dto.rs crates/api/src/handlers.rs crates/api/src/lib.rs crates/api/tests/api_integration.rs
git commit -m "feat(api): POST /agents/{id}/input + /end-input endpoints"
```

---

## Task 6: CLI verbs — `send` / `end-input`

**Files:** Modify `crates/cli/src/main.rs`

- [ ] **Step 1: Write failing tests.** Add to the test module in `crates/cli/src/main.rs`:

```rust
    #[test]
    fn send_parses_id_and_text() {
        let cli = Cli::parse_from(["prospero", "send", "ag1", "do the thing"]);
        match cli.command {
            Command::Send(a) => {
                assert_eq!(a.id, "ag1");
                assert_eq!(a.text, "do the thing");
            }
            other => panic!("expected send, got {other:?}"),
        }
    }

    #[test]
    fn end_input_parses_id() {
        let cli = Cli::parse_from(["prospero", "end-input", "ag1"]);
        match cli.command {
            Command::EndInput(a) => assert_eq!(a.id, "ag1"),
            other => panic!("expected end-input, got {other:?}"),
        }
    }
```

- [ ] **Step 2: Run tests, verify they fail.** Run: `cargo test -p prospero-cli send_parses 2>&1 | tail -10`. Expected: compile error (no `Send`/`EndInput` variants).

- [ ] **Step 3: Implement.** In the `enum Command`, add (after `Rm(AgentRef)`):

```rust
    /// Send a user message to an interactive agent (resumes the run).
    Send(SendArgs),
    /// Signal end-of-input to an interactive agent (it finishes after).
    EndInput(AgentRef),
```

Add the args struct (near `AgentRef`):

```rust
#[derive(Debug, Args)]
struct SendArgs {
    /// Agent id.
    id: String,
    /// Message text to inject.
    text: String,
}
```

Add match arms in `main` (after the `Command::Rm(a)` arm):

```rust
        Command::Send(a) => {
            client.post_json(
                &format!("/api/agents/{}/input", a.id),
                serde_json::json!({ "text": a.text }),
            )?;
            println!("sent message to {}", a.id);
        }
        Command::EndInput(a) => {
            client.post_json(
                &format!("/api/agents/{}/end-input", a.id),
                serde_json::Value::Null,
            )?;
            println!("end-input sent to {}", a.id);
        }
```

- [ ] **Step 4: Run tests, verify they pass.** Run: `cargo test -p prospero-cli 2>&1 | tail -8`. Expected: all CLI tests pass (incl. `cli_definition_is_valid`).

- [ ] **Step 5: Commit.**
```bash
git add crates/cli/src/main.rs
git commit -m "feat(cli): prospero send / end-input verbs"
```

---

## Task 7: Dashboard input UX for interactive idle agents

**Files:** Modify `crates/api/dashboard/app.js`, `crates/api/dashboard/index.html`

No JS test runner — verify with `cargo build`, `node --check`, `curl` of the served asset, and a `curl` API round-trip.

- [ ] **Step 1: Add CSS.** In `crates/api/dashboard/index.html`, before `</style>`, add:

```css
      .agent-input { display: flex; gap: 6px; margin: 4px 0 8px; }
      .agent-input .in { margin-top: 0; flex: 1; }
      .agent-input button { font: 10px ui-monospace, monospace; padding: 2px 8px; border-radius: 5px;
                            border: 1px solid #2d3a5a; background: #1f2a44; color: #8ab4f8; cursor: pointer; }
      .agent-input button.end { background: #1b1f27; color: #9aa0aa; border-color: #2d3340; }
```

- [ ] **Step 2: Render the input box for interactive idle agents (app.js).** In `renderAgent`, after the row's action buttons are appended (i.e. after `right.appendChild(acts)` / before `return row` — the agent row is built into `row`), add an input affordance when the agent is interactive and idle. Append it to `row` so it sits under the agent line:

```js
  if (agent.interactive && agent.status === "idle") {
    const box = document.createElement("div");
    box.className = "agent-input";
    const input = document.createElement("input");
    input.className = "in";
    input.placeholder = "send a message…";
    const send = document.createElement("button");
    send.textContent = "send";
    const end = document.createElement("button");
    end.className = "end";
    end.textContent = "end input";
    const doSend = async () => {
      const text = input.value.trim();
      if (!text) return;
      send.disabled = true;
      try {
        await api("POST", `/api/agents/${encodeURIComponent(agent.id)}/input`, { text });
        input.value = "";
        selectAgent(agent.id); // (re)open the stream to watch the resumed turn
      } catch (e) {
        showBanner(String(e.message || e));
        send.disabled = false;
      }
    };
    send.onclick = (e) => { e.stopPropagation(); doSend(); };
    input.onclick = (e) => e.stopPropagation();
    input.onkeydown = (e) => { if (e.key === "Enter") { e.stopPropagation(); doSend(); } };
    end.onclick = async (e) => {
      e.stopPropagation();
      end.disabled = true;
      try {
        await api("POST", `/api/agents/${encodeURIComponent(agent.id)}/end-input`);
        refreshFleet();
      } catch (err) {
        showBanner(String(err.message || err));
        end.disabled = false;
      }
    };
    box.appendChild(input);
    box.appendChild(send);
    box.appendChild(end);
    row.appendChild(box);
  }
```

> The input/buttons call `e.stopPropagation()` so interacting with them doesn't trigger the row's `selectAgent` onclick. `api(method, path)` with no body sends no payload, which the `/end-input` endpoint expects.

- [ ] **Step 3: Build, rebuild-and-restart, verify.**
```bash
cargo build --bin prosperod
node --check crates/api/dashboard/app.js && echo js-ok
```
Both clean. Then restart the daemon (per the spec's smoke instructions or the standard restart) and:
```bash
curl -s http://127.0.0.1:7878/app.js | grep -c "agent-input"          # expect >= 1
curl -s http://127.0.0.1:7878/app.js | grep -c "/end-input"            # expect >= 1
# Endpoint shape the dashboard relies on (unknown id → 404 proves wiring):
curl -s -X POST http://127.0.0.1:7878/api/agents/nope/input -H 'content-type: application/json' \
  -d '{"text":"x"}' -o /dev/null -w "input-unknown:[%{http_code}]\n"   # expect 404
curl -s -X POST http://127.0.0.1:7878/api/agents/nope/end-input -o /dev/null -w "endinput-unknown:[%{http_code}]\n"  # expect 404
```

- [ ] **Step 4: Commit.**
```bash
git add crates/api/dashboard/app.js crates/api/dashboard/index.html
git commit -m "feat(dashboard): input box + end-input for interactive idle agents"
```

---

## Task 8: Final verification gate

**Files:** none (verification only).

- [ ] **Step 1: Run the full gate.**
```bash
cargo fmt --all
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit -- -D warnings 2>&1 | tail -3
cargo build --workspace --all-targets 2>&1 | tail -2
cargo test --workspace --features prospero-core/testkit 2>&1 | tail -15
```
Expected: fmt clean, clippy clean, build clean, all tests green. (If `fmt --check` shows a diff, the prior `cargo fmt --all` already fixed it — re-stage and amend the relevant commit.)

- [ ] **Step 2: Manual smoke (optional, requires a working caliban worker — tracked by caliban#71).** Spawn an interactive agent, watch it reach `Idle`, `prospero send <id> "…"`, confirm it resumes via the SSE stream, then `prospero end-input <id>` and confirm `Done`. Note: end-to-end live behavior depends on caliban actually launching workers.

- [ ] **Step 3: Finish the branch.** Use **superpowers:finishing-a-development-branch**.

---

## Self-review notes

- **Spec coverage:** wire mirror + drift guard (Task 1), spawn-path plumbing incl. dashboard checkbox + CLI flag (Task 2), `AttachInbound` + client send path (Task 3), `Agent.interactive` + `send_agent_input` with terminal/non-interactive/unknown rejects (Task 4), API `/input` + `/end-input` (Task 5), CLI `send`/`end-input` (Task 6), dashboard input UX for idle interactive agents (Task 7), final gate (Task 8). `ReportStatus` intentionally omitted (worker→daemon, per spec). All spec task-breakdown items map to a task.
- **Type/name consistency:** `SpawnSpec.interactive`, `SpawnRequest.interactive`, `SpawnBody.interactive`, `Agent.interactive`, `AttachInbound::{UserMessage{text}, EndInput}`, `CalibandClient::send_inbound(socket_path, &AttachInbound)`, `FleetManager::send_agent_input(agent_id, AttachInbound)`, handlers `agent_input`/`agent_end_input`, CLI `Send(SendArgs{id,text})`/`EndInput(AgentRef)` — each defined once and referenced consistently. Rejections use `CoreError::InvalidState { op, id, status }` (→409) and `CoreError::AgentNotFound` (→404), matching the existing `ApiError` mapping.
- **Known external blocker:** live end-to-end requires caliban to actually launch workers (caliban#71). All Prospero-side layers are unit/integration-tested against the wire contract regardless.
