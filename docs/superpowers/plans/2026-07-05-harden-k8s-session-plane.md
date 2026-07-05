# Harden K8sFleet Session Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the K8sFleet per-agent stream leg works over TCP (I1), reconcile restart CR naming to be spec-deterministic (M1), and make `watch_fleet` a single shared poll loop with exactly-once `Gone` (M2).

**Architecture:** Add a TCP+TLS per-agent stream listener to `FakeCaliband` and an end-to-end test dialing it; change `restart_agent` to re-apply `task_name(spec)` after a bounded wait-for-gone (dropping `restart_name`/`restart_nonce`); replace `watch_fleet`'s per-subscription poll task with one construction-time poll loop broadcasting `FleetChange`, where `watch_fleet` seeds from `snapshot()` then tails a `broadcast::Receiver`.

**Tech Stack:** Rust (edition 2024), tokio (`broadcast`, `mpsc`), async-stream, the `crate::caliband::transport` seam (#71), kube (k8s feature).

## Global Constraints

- **All k8s code is behind `feature = "k8s"`; the harness additions behind `feature = "testkit"`.** Tests run with `--features prospero-core/testkit,prospero-core/k8s`.
- **`FleetChange` contract unchanged:** `watch_fleet` still yields `Discovered` (seed + new), `StatusChanged`, `Gone`. The change is *ownership* (one loop), not the event vocabulary.
- **`restart_agent` returns the `task_name(spec)` id** (same as the original spawn's id) — allowed by the trait's "possibly new id".
- **Verification gate (CI mirror):** from repo root — `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets --features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s -- -D warnings`; `cargo build --workspace --all-targets --features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s`; `cargo test --workspace --features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s`. Also `cargo build -p prospero-core` (no features).
- **Every commit subject ends with `(#77)`.**

---

### Task 1 (M1): restart_agent reuses `task_name(spec)`

**Files:**
- Modify: `crates/core/src/k8s/fleet.rs` (`restart_agent`, remove `restart_name` + `restart_nonce`, tests)

**Interfaces:**
- Consumes: `task_name(&TaskSpec)`, `CalibanTaskApi::{get, delete, apply}`, `CalibanTask::new`.
- Produces: `restart_agent` returns `AgentId::from(task_name(&spec))`; `restart_name`/`restart_nonce` removed.

- [ ] **Step 1: Write the failing test.** In `k8s/fleet.rs` tests:
```rust
#[tokio::test]
async fn restart_then_ensure_targets_one_cr_no_duplicate() {
    let api = MemTaskApi::new();
    let s = spec("repo-a", "p", None);
    let name = task_name(&s);
    api.apply(&build_calibantask(&s, &name)).await.unwrap();
    let (bus, store) = test_seams();
    let fleet = K8sFleet::new(api, bus, store);

    let new_id = fleet.restart_agent(&AgentId::from(name.as_str())).await.unwrap();
    // Restart keeps the spec-deterministic name (idempotent identity).
    assert_eq!(new_id, AgentId::from(name.as_str()));
    // Exactly one CR exists (the restart replaced, not duplicated).
    assert_eq!(fleet.snapshot().await.workspaces[0].agents.len(), 1);
}
```

- [ ] **Step 2: Run it, verify it fails.** `cargo test -p prospero-core --features testkit,k8s restart_then_ensure_targets_one_cr` → FAIL (restart returns a nonce-salted name; assert_eq on id fails).

- [ ] **Step 3: Rewrite `restart_agent`.** Replace the body:
```rust
async fn restart_agent(&self, id: &AgentId) -> Result<AgentId> {
    let old_name = id.as_str();
    let old = self
        .api
        .get(old_name)
        .await?
        .ok_or_else(|| CoreError::AgentNotFound(old_name.to_string()))?;

    // The CR name is a pure function of spec (`task_name`), so a restart
    // re-applies the SAME name — keeping identity idempotent for a future
    // declarative `ensure_agent(spec)` reconcile (prospero #77 M1).
    let name = task_name(&old.spec);

    self.api.delete(old_name).await?;
    // Wait for the old CR to actually disappear before re-applying the same
    // name, so we never race a not-yet-finalized delete (FakeK8s deletes
    // synchronously; real kube deletion with finalizers needs this poll).
    let deadline = tokio::time::Instant::now() + self.poll.deadline;
    while self.api.get(&name).await?.is_some() {
        if tokio::time::Instant::now() >= deadline {
            return Err(CoreError::Fleet(format!(
                "restart: CalibanTask {name} did not delete within the budget"
            )));
        }
        tokio::time::sleep(self.poll.interval).await;
    }

    let mut fresh = CalibanTask::new(&name, old.spec.clone());
    fresh.status = None;
    self.api.apply(&fresh).await?;

    Ok(AgentId::from(name))
}
```
(Note: when `old_name == name` — the common case, since the original spawn used `task_name` — the `delete` then `get`→`None` loop still holds because FakeK8s/real-kube remove the entry. If `old_name != name` for a legacy nonce-named CR, the delete targets `old_name` and the wait targets `name`, which is already absent → the loop exits immediately.)

- [ ] **Step 4: Remove `restart_name` + `restart_nonce`.** Delete the `fn restart_name(...)` (around line 64) and the `restart_nonce: AtomicU64` struct field + its initializers in `with_poll_config`. Remove the now-unused `use std::sync::atomic::{AtomicU64, Ordering}` if nothing else uses them (grep first).

- [ ] **Step 5: Update the existing restart test.** The prior test asserted a *different* id; find it (grep `restart_agent` in tests) and update its expectation to the same-name behavior, or fold it into the Step 1 test. Delete any `restart_name`-specific test.

- [ ] **Step 6: Run k8s tests.** `cargo test -p prospero-core --features testkit,k8s k8s:: 2>&1 | tail` → PASS.

- [ ] **Step 7: Commit.**
```bash
git add crates/core/src/k8s/fleet.rs
git commit -m "fix(core): K8sFleet restart reuses task_name(spec) for idempotent CR identity (#77)"
```

---

### Task 2 (M2): `watch_fleet` → one shared broadcast poll loop

**Files:**
- Modify: `crates/core/src/k8s/fleet.rs` (struct field, constructor, `watch_fleet`, Drop, tests)

**Interfaces:**
- Consumes: `snapshot()` (Task from #76), `agent_from_task`, `CalibanTaskApi::list`.
- Produces: `K8sFleet` holds `changes: tokio::sync::broadcast::Sender<FleetChange>` + a poll task aborted on drop; `watch_fleet` seeds from `snapshot()` then tails `changes.subscribe()`.

- [ ] **Step 1: Write the failing test (exactly-once Gone across subscribers).** In `k8s/fleet.rs` tests:
```rust
#[tokio::test]
async fn watch_fleet_shared_loop_seeds_and_gones_once_per_subscriber() {
    use futures::StreamExt;
    use std::time::Duration;
    let api = MemTaskApi::new();
    api.apply(&build_calibantask(&spec("repo-a", "p", None), "a1")).await.unwrap();
    let (bus, store) = test_seams();
    let fleet = K8sFleet::new(api, bus, store).with_watch_poll_interval(Duration::from_millis(20));

    // Two independent subscribers both seed the present agent as Discovered.
    let mut w1 = fleet.watch_fleet();
    let mut w2 = fleet.watch_fleet();
    for w in [&mut w1, &mut w2] {
        let ev = tokio::time::timeout(Duration::from_secs(1), w.next()).await.unwrap().unwrap();
        assert!(matches!(ev, FleetChange::Discovered { ref id, .. } if id.as_str() == "a1"));
    }

    // Delete → each live subscriber gets exactly one Gone for a1.
    fleet.api.delete("a1").await.unwrap();
    for w in [&mut w1, &mut w2] {
        let ev = tokio::time::timeout(Duration::from_secs(1), w.next()).await.unwrap().unwrap();
        assert!(matches!(ev, FleetChange::Gone { ref id, .. } if id.as_str() == "a1"));
    }
}
```

- [ ] **Step 2: Add the `changes` sender + poll task to the struct.** In `struct K8sFleet<A>`, replace `restart_nonce` (already removed in Task 1) region and add:
```rust
/// Broadcast of fleet changes from the single shared poll-diff loop. Each
/// `watch_fleet` subscriber seeds from `snapshot()` then tails this. (#77 M2)
changes: tokio::sync::broadcast::Sender<crate::model::FleetChange>,
/// The shared poll-diff loop's task; aborted on drop so a dropped fleet
/// (e.g. between tests) doesn't leak a forever-polling task.
poll_task: tokio::task::JoinHandle<()>,
```
Add a Drop impl:
```rust
impl<A: CalibanTaskApi> Drop for K8sFleet<A> {
    fn drop(&mut self) {
        self.poll_task.abort();
    }
}
```

- [ ] **Step 3: Start the loop in `with_poll_config`.** In `with_poll_config`, before building `Self`, create the sender and spawn the loop (moving the current `watch_fleet` loop body here, broadcasting instead of `tx.send`):
```rust
let (changes, _) = tokio::sync::broadcast::channel::<crate::model::FleetChange>(256);
let poll_task = {
    let api = Arc::clone(&api_arc);            // the `Arc<A>` you build below
    let tx = changes.clone();
    let interval = watch_poll_interval;        // the value going into the struct
    tokio::spawn(async move {
        let mut known: std::collections::HashMap<String, (crate::model::AgentStatus, String)> =
            std::collections::HashMap::new();
        loop {
            let tasks = match api.list().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(target: "prospero_k8s_fleet", error = %e, "watch loop: list() failed; retry");
                    tokio::time::sleep(interval).await;
                    continue;
                }
            };
            let mut seen = std::collections::HashSet::with_capacity(tasks.len());
            for task in &tasks {
                let Some(name) = task.metadata.name.clone() else { continue };
                seen.insert(name.clone());
                let agent = agent_from_task(task);
                let status = agent.status;
                let ws = agent.workspace.clone();
                let change = match known.get(&name) {
                    None => Some(crate::model::FleetChange::Discovered { id: AgentId::from(name.clone()), workspace: ws.clone(), agent }),
                    Some((prev, _)) if *prev != status => Some(crate::model::FleetChange::StatusChanged { id: AgentId::from(name.clone()), workspace: ws.clone(), from: *prev, to: status }),
                    Some(_) => None,
                };
                known.insert(name.clone(), (status, ws));
                if let Some(c) = change { let _ = tx.send(c); }
            }
            let gone: Vec<(String, String)> = known.iter()
                .filter(|(n, _)| !seen.contains(*n))
                .map(|(n, (_, ws))| (n.clone(), ws.clone()))
                .collect();
            for (name, ws) in gone {
                known.remove(&name);
                let _ = tx.send(crate::model::FleetChange::Gone { id: AgentId::from(name), workspace: ws });
            }
            tokio::time::sleep(interval).await;
        }
    })
};
```
Add `changes` and `poll_task` to the `Self { ... }` initializer. (`api_arc` is the `Arc::new(api)` the constructor already builds for the `api` field — reuse it; if it currently inlines `Arc::new(api)` into the struct literal, hoist it to a `let api_arc = Arc::new(api);` first.)

Note: `with_watch_poll_interval` sets `self.watch_poll_interval` *after* construction, but the loop already captured the default. Change `with_watch_poll_interval` to also **restart** the loop with the new interval, OR (simpler) make `with_poll_config` take the interval. Simplest: have `with_watch_poll_interval` abort `self.poll_task` and re-spawn with the new interval (same closure, factored into a `fn spawn_watch_loop(api, tx, interval) -> JoinHandle` free helper the constructor and this both call). Implement that helper and use it in both places.

- [ ] **Step 4: Rewrite `watch_fleet` as seed + tail.** Replace the whole method:
```rust
fn watch_fleet(&self) -> BoxStream<'static, FleetChange> {
    let mut rx = self.changes.subscribe();
    let api = Arc::clone(&self.api);
    let s = async_stream::stream! {
        // Seed: current agents as Discovered (dedup key for the tail overlap).
        let mut seen = std::collections::HashSet::new();
        for task in api.list().await.unwrap_or_default() {
            if let Some(name) = task.metadata.name.clone() {
                let agent = agent_from_task(&task);
                let ws = agent.workspace.clone();
                seen.insert(name.clone());
                yield FleetChange::Discovered { id: AgentId::from(name), workspace: ws, agent };
            }
        }
        // Tail the shared broadcast; skip a Discovered already in the seed.
        loop {
            match rx.recv().await {
                Ok(change) => {
                    if let FleetChange::Discovered { ref id, .. } = change {
                        if seen.remove(id.as_str()) { continue; }
                    }
                    yield change;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Self-heal: re-seed from a fresh snapshot, then keep tailing.
                    seen.clear();
                    for task in api.list().await.unwrap_or_default() {
                        if let Some(name) = task.metadata.name.clone() {
                            let agent = agent_from_task(&task);
                            let ws = agent.workspace.clone();
                            seen.insert(name.clone());
                            yield FleetChange::Discovered { id: AgentId::from(name), workspace: ws, agent };
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Box::pin(s)
}
```
(`async_stream` is already a dep — the SSE tail uses it; confirm it's in `prospero-core`'s Cargo. If not, add `async-stream.workspace = true`.)

- [ ] **Step 5: Build + fix.** `cargo build -p prospero-core --features testkit,k8s 2>&1 | rg error` → fix any missed `restart_nonce`/imports.

- [ ] **Step 6: Run tests.** `cargo test -p prospero-core --features testkit,k8s k8s:: 2>&1 | tail` → PASS (the two-subscriber test + existing watch tests). Update any existing `watch_fleet` test that assumed per-subscription re-Discovery semantics (the seed still Discovers, so most hold).

- [ ] **Step 7: Commit.**
```bash
git add crates/core/src/k8s/fleet.rs
git commit -m "refactor(core): K8sFleet watch_fleet -> single shared broadcast poll loop (#77 M2)"
```

---

### Task 3 (I1): `FakeCaliband` per-agent stream over TCP+TLS

**Files:**
- Modify: `crates/core/src/testkit.rs` (a TCP+TLS per-agent stream listener + a registration helper)

**Interfaces:**
- Consumes: `crate::caliband::transport::{Listener, BindSpec, tls_server_from_pem}`, `crate::caliband::wire::Endpoint`.
- Produces: `FakeCaliband::add_agent_tcp_stream(script: Vec<serde_json::Value>) -> (Endpoint, Vec<u8>)` — binds a `127.0.0.1:0` TCP+TLS listener that replays `script` as NDJSON on each connection, returning the `Endpoint::Tcp { addr }` to dial and the CA PEM to trust.

- [ ] **Step 1: Write the failing integration test.** Create `crates/core/tests/k8s_session_plane_tcp.rs` (mirror `k8s_session_plane.rs`'s structure), gated `#![cfg(all(feature = "k8s", feature = "testkit"))]`:
```rust
#[tokio::test]
async fn k8s_fleet_streams_a_network_agent_stream_over_tcp() {
    let token = "s3cr3t";
    let (mut fake, _fix) = prospero_core::testkit::FakeCaliband::start_tcp_tls(token).await.unwrap();

    // A per-agent STREAM served over TCP+TLS (the leg #77 I1 proves).
    let script = vec![
        serde_json::json!({"type":"TurnStart","turn_index":0,"message_id":"a1","model":"m"}),
        serde_json::json!({"type":"AssistantTextDelta","turn_index":0,"content_block_index":0,"text":"hello"}),
        serde_json::json!({"type":"RunEnd","final_messages":[],"total_usage":{},"turn_count":1,"stopped_for":"EndOfTurn"}),
    ];
    let (endpoint, ca_pem) = fake.add_agent_tcp_stream(script).await;
    let tls = prospero_core::caliband::transport::tls_client_from_pem(&ca_pem, "localhost").unwrap();

    let (bus, store) = /* test_seams equivalent: JsonlStore + InProcessBus */;
    let fleet = prospero_core::K8sFleet::new(prospero_core::k8s::fake::FakeK8s::new(), bus, store.clone())
        .with_network(Some(tls), Some(token.to_string()));

    // Dial the per-agent stream over TCP+TLS; frames land in the store.
    fleet.start_agent_stream("repo-a", "a1", &endpoint);
    // wait_for_history helper: replay store under stream_key "a1" until >=1 event.
    /* assert an Output "hello" event appears under stream_key_for("", "a1") */
}
```
(Reuse `k8s_session_plane.rs`'s `wait_for_history` + seam-building helpers — copy or extract them; keep the test self-contained.)

- [ ] **Step 2: Run it, verify it fails.** `cargo test -p prospero-core --features testkit,k8s --test k8s_session_plane_tcp` → FAIL (`add_agent_tcp_stream` not found).

- [ ] **Step 3: Add the TCP+TLS stream listener helper.** In `testkit.rs`, add a free fn + a `FakeCaliband` method:
```rust
/// Bind a per-agent stream socket over TCP + TLS that, on each connection,
/// writes the scripted frames as NDJSON then closes. Returns the bound
/// `host:port`. (#77 I1)
#[cfg(any(test, feature = "testkit"))]
async fn spawn_tcp_stream_listener(
    script: Vec<serde_json::Value>,
    cert_pem: &[u8],
    key_pem: &[u8],
) -> (String, JoinHandle<()>) {
    use crate::caliband::transport::{tls_server_from_pem, BindSpec, Listener};
    use crate::caliband::wire::Endpoint;
    use tokio::io::AsyncWriteExt as _;
    let listener = Listener::bind(&BindSpec {
        endpoint: Endpoint::Tcp { addr: "127.0.0.1:0".into() },
        tls: Some(tls_server_from_pem(cert_pem, key_pem).unwrap()),
        token: None,
    })
    .await
    .unwrap();
    let addr = listener.local_addr().expect("tcp addr");
    let task = tokio::spawn(async move {
        while let Ok(mut conn) = listener.accept().await {
            for frame in &script {
                let mut line = serde_json::to_vec(frame).unwrap();
                line.push(b'\n');
                if conn.write_all(&line).await.is_err() { break; }
            }
            let _ = conn.flush().await;
        }
    });
    (addr, task)
}

impl FakeCaliband {
    /// Register a per-agent stream served over TCP+TLS and return the
    /// `Endpoint::Tcp` to dial plus the CA PEM to trust. The fake's own
    /// self-signed "localhost" cert is reused. (#77 I1)
    pub async fn add_agent_tcp_stream(
        &mut self,
        script: Vec<serde_json::Value>,
    ) -> (crate::caliband::wire::Endpoint, Vec<u8>) {
        // Generate (or reuse) a localhost cert; simplest is a fresh one here.
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_pem = cert.cert.pem().into_bytes();
        let key_pem = cert.key_pair.serialize_pem().into_bytes();
        let (addr, task) = spawn_tcp_stream_listener(script, &cert_pem, &key_pem).await;
        self.tasks.push(task);
        (crate::caliband::wire::Endpoint::Tcp { addr }, cert_pem)
    }
}
```
(Confirm `FakeCaliband` has a `tasks: Vec<JoinHandle<()>>` field to push into — it does, from #71's structure. `rcgen` is already a testkit dep.)

- [ ] **Step 4: Run the test.** `cargo test -p prospero-core --features testkit,k8s --test k8s_session_plane_tcp 2>&1 | tail` → PASS (a "hello" `Output` event lands under the agent's stream key over a fully-TCP+TLS stream leg).

- [ ] **Step 5: Commit.**
```bash
git add crates/core/src/testkit.rs crates/core/tests/k8s_session_plane_tcp.rs
git commit -m "test(core): prove K8sFleet per-agent stream over TCP+TLS end-to-end (#77 I1)"
```

---

### Task 4: Full gate

- [ ] **Step 1: Run the CI-mirror gate.**
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s -- -D warnings
cargo build --workspace --all-targets --features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s
cargo build -p prospero-core
cargo test --workspace --features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s
```
Expected: all green.

- [ ] **Step 2: Commit any fmt fixups.**
```bash
git add -A && git commit -m "chore: fmt (#77)" || true
```

---

## Self-Review

**Spec coverage:**
- M1 restart reuses `task_name(spec)` + drop `restart_name`/nonce → Task 1. ✓
- M2 single shared broadcast loop + seed/tail + lagged-reseed + drop-abort → Task 2. ✓
- I1 FakeCaliband TCP+TLS per-agent stream + end-to-end test → Task 3. ✓
- Error handling (list retry, lagged reseed, restart wait-for-gone timeout) → Tasks 1–2. ✓
- Testing strategy (two-subscriber Gone-once, TCP stream, restart-no-duplicate) → Tasks 1–3. ✓

**Placeholder scan:** Task 3 Step 1's test body has two bracketed spots (`/* test_seams equivalent */`, `/* assert Output hello */`) that say "reuse `k8s_session_plane.rs`'s helpers" — that existing test is the concrete template (its `test_seams`/`wait_for_history` are copied verbatim), so this is a "mirror the sibling file" instruction, not an undefined reference. Everything else is complete code.

**Type consistency:** `FleetChange::{Discovered{id,workspace,agent}, StatusChanged{id,workspace,from,to}, Gone{id,workspace}}`, `task_name(&TaskSpec)->String`, `broadcast::Sender<FleetChange>`, `add_agent_tcp_stream(script)->(Endpoint, Vec<u8>)`, `spawn_tcp_stream_listener(script,cert,key)->(String,JoinHandle)`, `with_network(Some(tls),Some(token))` used consistently across tasks and match the current code read during planning.

**Known reconciliations (do at impl):** whether `with_watch_poll_interval` needs the loop-restart helper (Task 2 Step 3 note); whether `async-stream` is already a `prospero-core` dep (Task 2 Step 4 note); the exact `tasks`/cert-reuse shape on `FakeCaliband` (Task 3 Step 3 note). Each names the check + fallback.
