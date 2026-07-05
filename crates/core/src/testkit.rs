//! `FakeCaliband` — an in-test harness that speaks caliban's NDJSON protocol.
//!
//! Because the wire format is the only coupling to caliban, a faithful fake
//! lets us test the whole control plane deterministically with no real caliban,
//! no API keys, and no LLM calls. Available to in-crate tests automatically and
//! to other crates via the `testkit` feature.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::task::JoinHandle;

use crate::caliband::wire::{
    AgentRecord, AgentStatus, CtlReply, CtlRequest, DaemonStatus, Endpoint, SpawnSpec,
    SupervisorError,
};

/// Shared mutable state inside a running fake.
#[derive(Default)]
struct FakeState {
    /// Registered agents by id.
    agents: HashMap<String, AgentRecord>,
    /// Per-agent stream scripts (caliban stream-json frames as JSON values).
    scripts: HashMap<String, Vec<serde_json::Value>>,
    /// Every spawn spec the fake has received (for assertions).
    received_specs: Vec<SpawnSpec>,
    /// Every agent id an `Attach` request has named, in order (for asserting
    /// that a code path did/didn't take the extra attach round-trip).
    received_attach_ids: Vec<String>,
    /// Monotonic id counter for spawns.
    next_id: u64,
    /// How many `Shutdown` requests have been received.
    shutdowns: u32,
    /// Set to `true` after the first `Shutdown`; the accept loop exits.
    should_stop: bool,
}

/// A running fake caliband daemon. Aborts its listener tasks on drop.
pub struct FakeCaliband {
    control_socket: PathBuf,
    state: Arc<Mutex<FakeState>>,
    tasks: Vec<JoinHandle<()>>,
    /// Owns the temp dir backing a TCP fake's per-agent sockets, if any, so it
    /// outlives the fake. `None` for the Unix path (caller owns the dir).
    _tempdir: Option<tempfile::TempDir>,
}

/// Self-signed localhost TLS material + address a matching
/// `CalibandClient::connect_tcp` needs to reach a TCP+TLS [`FakeCaliband`].
pub struct CalibandTlsFixture {
    /// `host:port` the fake bound (resolved from `:0`).
    pub addr: String,
    /// CA/cert PEM the client trusts (self-signed for "localhost").
    pub ca_pem: Vec<u8>,
}

impl FakeCaliband {
    /// Start a fake listening on `control_socket`. Per-agent stream sockets are
    /// created alongside it in the same directory.
    pub async fn start_at(control_socket: impl Into<PathBuf>) -> std::io::Result<Self> {
        let control_socket = control_socket.into();
        if let Some(parent) = control_socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&control_socket);
        let dir = control_socket
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let listener =
            crate::caliband::transport::Listener::bind(&crate::caliband::transport::BindSpec {
                endpoint: Endpoint::Unix {
                    path: control_socket.clone(),
                },
                tls: None,
                token: None,
            })
            .await?;
        let state = Arc::new(Mutex::new(FakeState::default()));
        let accept_task = serve_control(listener, state.clone(), dir, Some(control_socket.clone()));

        Ok(Self {
            control_socket,
            state,
            tasks: vec![accept_task],
            _tempdir: None,
        })
    }

    /// Start a fake serving the control protocol over **TCP + TLS + bearer
    /// token** (ADR 0051). Per-agent stream sockets remain Unix in a temp dir —
    /// the control plane (list/spawn/attach/kill/status/shutdown) is what this
    /// path proves over the network; full per-agent-stream-over-TCP is a
    /// K8sFleet concern (prospero #64). Returns the fixture a matching
    /// [`crate::caliband::client::CalibandClient::connect_tcp`] needs.
    pub async fn start_tcp_tls(token: &str) -> std::io::Result<(Self, CalibandTlsFixture)> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .map_err(std::io::Error::other)?;
        let cert_pem = cert.cert.pem().into_bytes();
        let key_pem = cert.key_pair.serialize_pem().into_bytes();

        let tempdir = tempfile::tempdir()?;
        let dir = tempdir.path().to_path_buf();

        let listener =
            crate::caliband::transport::Listener::bind(&crate::caliband::transport::BindSpec {
                endpoint: Endpoint::Tcp {
                    addr: "127.0.0.1:0".into(),
                },
                tls: Some(crate::caliband::transport::tls_server_from_pem(
                    &cert_pem, &key_pem,
                )?),
                token: Some(token.to_string()),
            })
            .await?;
        let addr = listener.local_addr().expect("tcp listener has an address");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let accept_task = serve_control(listener, state.clone(), dir, None);

        Ok((
            Self {
                // No control socket file for TCP; a non-existent path keeps the
                // Drop cleanup a harmless no-op.
                control_socket: PathBuf::from("<tcp>"),
                state,
                tasks: vec![accept_task],
                _tempdir: Some(tempdir),
            },
            CalibandTlsFixture {
                addr,
                ca_pem: cert_pem,
            },
        ))
    }

    /// The control socket path the fake is listening on.
    pub fn control_socket(&self) -> &Path {
        &self.control_socket
    }

    /// Pre-register an agent with a stream script, and start its per-agent
    /// stream listener so an attach will replay `script` then close.
    pub async fn add_agent(&mut self, record: AgentRecord, script: Vec<serde_json::Value>) {
        let socket_path = record
            .endpoint
            .unix_socket_path()
            .expect("fake uses unix endpoints")
            .to_path_buf();
        {
            let mut st = self.state.lock().unwrap();
            st.scripts.insert(record.id.clone(), script.clone());
            st.agents.insert(record.id.clone(), record);
        }
        let task = spawn_stream_listener(&socket_path, script).await;
        self.tasks.push(task);
    }

    /// Pre-register an agent whose stream serves a **different script on each
    /// successive attach connection** (the last script repeats once exhausted).
    /// Lets a test simulate a mid-stream drop — a first connection that ends
    /// without a terminal `result` frame — followed by a full replay on
    /// reconnect, to exercise reconnection + dedup.
    pub async fn add_agent_with_scripts(
        &mut self,
        record: AgentRecord,
        scripts: Vec<Vec<serde_json::Value>>,
    ) {
        let socket_path = record
            .endpoint
            .unix_socket_path()
            .expect("fake uses unix endpoints")
            .to_path_buf();
        {
            let mut st = self.state.lock().unwrap();
            st.scripts.insert(
                record.id.clone(),
                scripts.last().cloned().unwrap_or_default(),
            );
            st.agents.insert(record.id.clone(), record);
        }
        let task = spawn_multi_script_stream_listener(&socket_path, scripts).await;
        self.tasks.push(task);
    }

    /// All spawn specs received so far (in order).
    pub fn received_specs(&self) -> Vec<SpawnSpec> {
        self.state.lock().unwrap().received_specs.clone()
    }

    /// All agent ids named by an `Attach` request so far (in order). Lets a
    /// test prove a code path did *not* issue a second control round-trip to
    /// resolve a socket it already had.
    pub fn received_attach_ids(&self) -> Vec<String> {
        self.state.lock().unwrap().received_attach_ids.clone()
    }

    /// Number of `Shutdown` requests the fake has received.
    pub fn shutdowns(&self) -> u32 {
        self.state.lock().unwrap().shutdowns
    }

    /// Set an agent's status (to simulate lifecycle transitions across polls).
    pub fn set_status(&self, id: &str, status: AgentStatus) {
        if let Some(a) = self.state.lock().unwrap().agents.get_mut(id) {
            a.status = status;
        }
    }

    /// Remove an agent (to simulate it disappearing from the registry).
    pub fn remove_agent(&self, id: &str) {
        self.state.lock().unwrap().agents.remove(id);
    }
}

/// A backend a `FleetProvider` implementation drives, generalized just enough
/// for [`fleet_provider_conformance`] to (a) assert a provision reached the
/// backend and (b) simulate the eventual reap of a stopped agent so
/// `watch_fleet` observes `Gone`. `FakeCaliband` implements it trivially
/// (below); a `K8sFleet`-side fake (`prospero_core::k8s::fake::FakeK8s`,
/// behind the `k8s` feature) implements it too, which is what makes the
/// conformance suite itself backend-agnostic.
pub trait FakeBackend {
    /// True once the backend received at least one provision request (a
    /// Unix `FakeCaliband`: a `Spawn` spec; a k8s fake: a `CalibanTask`
    /// apply).
    fn received_any_spec(&self) -> bool;

    /// Simulate the eventual reap of a stopped agent (real caliban/k8s both
    /// remove a stopped agent's registry entry/CR sometime after the stop,
    /// not instantly) so a live `watch_fleet` subscription observes `Gone`.
    /// Idempotent — safe to call on an id already reaped.
    fn simulate_reap(&self, id: &str);
}

impl FakeBackend for FakeCaliband {
    fn received_any_spec(&self) -> bool {
        !self.received_specs().is_empty()
    }

    fn simulate_reap(&self, id: &str) {
        self.remove_agent(id);
    }
}

impl Drop for FakeCaliband {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
        let _ = std::fs::remove_file(&self.control_socket);
    }
}

/// Drive one control listener: accept connections, handle each request, and
/// stop once a `Shutdown` set `should_stop`. Works over any transport family
/// (Unix or TCP+TLS). `cleanup_socket`, when set, is removed on stop.
fn serve_control(
    listener: crate::caliband::transport::Listener,
    state: Arc<Mutex<FakeState>>,
    dir: PathBuf,
    cleanup_socket: Option<PathBuf>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok(conn) = listener.accept().await {
            handle_control_conn(conn, state.clone(), dir.clone())
                .await
                .ok();
            if state.lock().unwrap().should_stop {
                if let Some(sock) = &cleanup_socket {
                    let _ = std::fs::remove_file(sock);
                }
                break;
            }
        }
    })
}

async fn handle_control_conn(
    conn: crate::caliband::transport::BoxConn,
    state: Arc<Mutex<FakeState>>,
    dir: PathBuf,
) -> std::io::Result<()> {
    let (read_half, mut write_half) = tokio::io::split(conn);
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(());
    }
    let req: CtlRequest = match serde_json::from_str(line.trim_end()) {
        Ok(r) => r,
        Err(_) => return Ok(()),
    };

    let (reply, new_listener): (CtlReply, Option<(PathBuf, Vec<serde_json::Value>)>) = {
        let mut st = state.lock().unwrap();
        match req {
            CtlRequest::List => (
                CtlReply::Listed {
                    agents: st.agents.values().cloned().collect(),
                },
                None,
            ),
            CtlRequest::Spawn { spec } => {
                st.received_specs.push(spec.clone());
                st.next_id += 1;
                let id = format!("agent{:03}", st.next_id);
                let socket_path = dir.join(format!("{id}.sock"));
                // Default script: a turn-start book-keeping frame then a
                // run-end (caliban's `TurnEvent` vocabulary — see ADR-0003).
                let script = vec![
                    serde_json::json!({
                        "type": "TurnStart",
                        "turn_index": 0,
                        "message_id": id.clone(),
                        "model": spec.model.clone().unwrap_or_else(|| "model".into()),
                    }),
                    serde_json::json!({
                        "type": "RunEnd",
                        "final_messages": [],
                        "total_usage": {},
                        "turn_count": 1,
                        "stopped_for": "EndOfTurn",
                    }),
                ];
                let record = AgentRecord {
                    id: id.clone(),
                    name: spec.label.clone().unwrap_or_else(|| id.clone()),
                    status: AgentStatus::Running,
                    started_at: "1970-01-01T00:00:00Z".into(),
                    session_dir: dir.join(&id),
                    endpoint: Endpoint::Unix {
                        path: socket_path.clone(),
                    },
                    spec: spec.clone(),
                };
                st.scripts.insert(id.clone(), script.clone());
                st.agents.insert(id.clone(), record);
                (
                    CtlReply::Spawned {
                        id,
                        endpoint: Endpoint::Unix {
                            path: socket_path.clone(),
                        },
                    },
                    Some((socket_path, script)),
                )
            }
            CtlRequest::Attach { id } => {
                st.received_attach_ids.push(id.clone());
                match st.agents.get(&id) {
                    Some(a) => (
                        CtlReply::AttachAck {
                            endpoint: a.endpoint.clone(),
                        },
                        None,
                    ),
                    None => (
                        CtlReply::Error {
                            error: SupervisorError::NotFound { id },
                        },
                        None,
                    ),
                }
            }
            CtlRequest::Kill { id } => {
                if let Some(a) = st.agents.get_mut(&id) {
                    a.status = AgentStatus::Killed;
                    (CtlReply::Killed, None)
                } else {
                    (
                        CtlReply::Error {
                            error: SupervisorError::NotFound { id },
                        },
                        None,
                    )
                }
            }
            CtlRequest::Respawn { id } => {
                if st.agents.remove(&id).is_some() {
                    st.next_id += 1;
                    let new_id = format!("agent{:03}", st.next_id);
                    (CtlReply::Respawned { id: new_id }, None)
                } else {
                    (
                        CtlReply::Error {
                            error: SupervisorError::NotFound { id },
                        },
                        None,
                    )
                }
            }
            CtlRequest::Rm { id, force: _ } => {
                if st.agents.remove(&id).is_some() {
                    (CtlReply::Removed, None)
                } else {
                    (
                        CtlReply::Error {
                            error: SupervisorError::NotFound { id },
                        },
                        None,
                    )
                }
            }
            CtlRequest::Status => (
                CtlReply::Status(DaemonStatus {
                    pid: 1234,
                    agents: st.agents.len() as u32,
                    uptime_secs: 0,
                    endpoint: Endpoint::Unix {
                        path: dir.join("control.sock"),
                    },
                }),
                None,
            ),
            CtlRequest::Shutdown => {
                st.shutdowns += 1;
                st.should_stop = true;
                (CtlReply::ShutdownAck, None)
            }
        }
    };

    // For spawns, start the per-agent stream listener before replying so the
    // caller can attach immediately.
    if let Some((socket_path, script)) = new_listener {
        spawn_stream_listener(&socket_path, script).await;
    }

    let mut bytes = serde_json::to_vec(&reply).unwrap();
    bytes.push(b'\n');
    write_half.write_all(&bytes).await?;
    write_half.flush().await?;
    Ok(())
}

/// Bind a per-agent stream socket that, on each connection, writes the scripted
/// frames as NDJSON and then closes.
async fn spawn_stream_listener(
    socket_path: &Path,
    script: Vec<serde_json::Value>,
) -> JoinHandle<()> {
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path).expect("bind per-agent stream socket");
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            for frame in &script {
                let mut line = serde_json::to_vec(frame).unwrap();
                line.push(b'\n');
                if stream.write_all(&line).await.is_err() {
                    break;
                }
            }
            let _ = stream.flush().await;
            // Drop closes the stream, signalling end-of-stream.
        }
    })
}

/// Bind a per-agent stream socket that serves a distinct script per connection
/// (connection `i` gets `scripts[i]`, with the last script repeating). Each
/// connection writes its frames then closes, so a short script simulates a
/// premature drop and a later one a full replay.
async fn spawn_multi_script_stream_listener(
    socket_path: &Path,
    scripts: Vec<Vec<serde_json::Value>>,
) -> JoinHandle<()> {
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path).expect("bind per-agent stream socket");
    tokio::spawn(async move {
        let mut conn = 0usize;
        while let Ok((mut stream, _)) = listener.accept().await {
            let script = scripts
                .get(conn)
                .or_else(|| scripts.last())
                .cloned()
                .unwrap_or_default();
            for frame in &script {
                let mut line = serde_json::to_vec(frame).unwrap();
                line.push(b'\n');
                if stream.write_all(&line).await.is_err() {
                    break;
                }
            }
            let _ = stream.flush().await;
            conn += 1;
            // Drop closes the stream, signalling end-of-stream for this attach.
        }
    })
}

/// The behavioral contract every [`crate::store::Store`] must satisfy. Backends
/// (jsonl, sqlite, Postgres) call this with a freshly-opened, empty store to
/// prove parity — so a new backend is correct by construction, not by hope.
pub async fn store_conformance(store: &dyn crate::store::Store) {
    use crate::event::{EventKind, FleetEvent, OutputStream};

    fn ev(seq: u64, agent: &str, chunk: &str) -> FleetEvent {
        FleetEvent {
            seq,
            ts: "t".into(),
            repo: "r".into(),
            agent_id: agent.into(),
            kind: EventKind::Output {
                stream: OutputStream::Stdout,
                chunk: chunk.into(),
            },
        }
    }

    assert_eq!(store.high_water("a").await.unwrap(), 0);
    assert!(store.replay("a", 0).await.unwrap().is_empty());
    assert!(store.writable().await);

    store.append(&ev(1, "a", "a1")).await.unwrap();
    store.append(&ev(1, "b", "b1")).await.unwrap();
    store.append(&ev(2, "a", "a2")).await.unwrap();

    assert_eq!(store.high_water("a").await.unwrap(), 2);
    assert_eq!(store.high_water("b").await.unwrap(), 1);

    let a = store.replay("a", 0).await.unwrap();
    assert_eq!(a.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2]);
    let a_from2 = store.replay("a", 2).await.unwrap();
    assert_eq!(a_from2.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![2]);
    let b = store.replay("b", 0).await.unwrap();
    assert_eq!(b.len(), 1);
}

/// Retention contract: `prune(before_ts)` deletes events with `ts < before_ts`
/// (RFC-3339, lexically ordered) and returns the count removed, leaving newer
/// events intact. Backends call this to prove identical retention semantics.
pub async fn store_prune_conformance(store: &dyn crate::store::Store) {
    use crate::event::{EventKind, FleetEvent};

    fn ev(seq: u64, ts: &str) -> FleetEvent {
        FleetEvent {
            seq,
            ts: ts.into(),
            repo: "r".into(),
            agent_id: "a".into(),
            kind: EventKind::AgentSpawned,
        }
    }

    // Match the production timestamp format (`chrono::Utc::now().to_rfc3339()`
    // emits `+00:00`, not `Z`); lexical ordering only holds within one offset form.
    store
        .append(&ev(1, "2026-01-01T00:00:00+00:00"))
        .await
        .unwrap();
    store
        .append(&ev(2, "2026-03-01T00:00:00+00:00"))
        .await
        .unwrap();
    store
        .append(&ev(3, "2026-06-01T00:00:00+00:00"))
        .await
        .unwrap();

    let removed = store.prune("2026-03-01T00:00:00+00:00").await.unwrap();
    assert_eq!(removed, 1);

    let remaining = store.replay("a", 0).await.unwrap();
    assert_eq!(
        remaining.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![2, 3]
    );

    assert_eq!(store.prune("2026-03-01T00:00:00+00:00").await.unwrap(), 0);
}

/// Contract every [`crate::config_store::ConfigStore`] must satisfy: upsert is
/// insert-or-update by name, list returns all repos (name-ordered), delete is
/// idempotent. Backends call this to prove identical config semantics.
pub async fn config_store_conformance(store: &dyn crate::config_store::ConfigStore) {
    use crate::registry::{RegisteredWorkspace, RepoProviderConfig};

    assert!(store.list_repos().await.unwrap().is_empty());

    let r = RegisteredWorkspace {
        name: "p".into(),
        root: "/r".into(),
        config: RepoProviderConfig {
            provider: Some("ollama".into()),
            ..Default::default()
        },
    };
    store.upsert_repo(&r).await.unwrap();
    let repos = store.list_repos().await.unwrap();
    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0].name, "p");
    assert_eq!(repos[0].root, std::path::PathBuf::from("/r"));
    assert_eq!(repos[0].config.provider.as_deref(), Some("ollama"));

    let mut r2 = r.clone();
    r2.config.provider = Some("anthropic".into());
    store.upsert_repo(&r2).await.unwrap();
    let repos = store.list_repos().await.unwrap();
    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0].config.provider.as_deref(), Some("anthropic"));

    assert!(store.delete_repo("p").await.unwrap());
    assert!(!store.delete_repo("p").await.unwrap());
    assert!(store.list_repos().await.unwrap().is_empty());

    // `list_repos` is name-ordered: insert out of order, expect sorted output.
    for name in ["z", "a"] {
        store
            .upsert_repo(&RegisteredWorkspace {
                name: name.into(),
                root: format!("/{name}").into(),
                config: RepoProviderConfig::default(),
            })
            .await
            .unwrap();
    }
    let names: Vec<String> = store
        .list_repos()
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.name)
        .collect();
    assert_eq!(names, vec!["a".to_string(), "z".to_string()]);
}

/// Behavioral contract every [`crate::fleet_provider::FleetProvider`] backend
/// must satisfy: `ensure_agent` provisions an attachable handle and its spec
/// reaches the backend; `watch_fleet` observes the new agent (`Discovered`);
/// `stop_agent` stops it and `watch_fleet` observes its departure (`Gone`);
/// `restart_agent` yields a fresh id. Mirrors the store/config conformance
/// style — driven over a [`FakeBackend`] so it needs no real caliban/cluster.
/// Runs against `LocalFleet` (+ `FakeCaliband`) here; `K8sFleet` (+ `FakeK8s`,
/// epic #274, P2) calls this too, so a new backend is correct by
/// construction, not by hope.
///
/// Deliberately drives everything through the `FleetProvider` trait plus
/// [`FakeBackend`]'s two hooks (`received_any_spec`, `simulate_reap`) — never
/// a backend-internal poll method (e.g. `LocalFleet`'s `FleetManager::
/// poll_repo_once`) — so this same function is reusable across backends with
/// their own reconciliation loop. That means the caller (each backend's own
/// test wiring) is responsible for actually running that backend's background
/// reconciliation (for `LocalFleet`, `FleetManager::run` on a fast poll
/// interval; for `K8sFleet`, its own `watch_fleet`-spawned poll loop) so the
/// bounded waits below converge instead of timing out.
pub async fn fleet_provider_conformance(
    provider: &dyn crate::FleetProvider,
    backend: &dyn FakeBackend,
) {
    use crate::fleet::SpawnRequest;
    use crate::model::{AgentId, DrainPolicy, FleetChange, TaskSpec};
    use futures::StreamExt;
    use std::time::Duration;

    const STEP_TIMEOUT: Duration = Duration::from_secs(2);
    const POLL_BACKOFF: Duration = Duration::from_millis(20);

    /// Re-subscribe (bounded by `STEP_TIMEOUT`) until a fresh `watch_fleet`
    /// subscription's *initial listing* carries a `Discovered` for `id`.
    /// Re-subscribing rather than sleeping arbitrarily: each attempt cheaply
    /// re-reads current state instead of guessing how long the backend's own
    /// reconciliation takes to converge.
    async fn wait_for_discovered(provider: &dyn crate::FleetProvider, id: &AgentId) {
        let deadline = tokio::time::Instant::now() + STEP_TIMEOUT;
        loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "watch_fleet never observed a Discovered for {id}"
            );
            let mut changes = provider.watch_fleet();
            while let Ok(Some(item)) =
                tokio::time::timeout(Duration::from_millis(50), changes.next()).await
            {
                if matches!(&item, FleetChange::Discovered { id: i, .. } if i == id) {
                    return;
                }
            }
            tokio::time::sleep(POLL_BACKOFF).await;
        }
    }

    // 1. `ensure_agent` provisions and returns an attachable handle; the spec
    // reached caliband.
    let h = provider
        .ensure_agent(TaskSpec {
            workspace: "repo-a".into(),
            request: SpawnRequest::new("task"),
        })
        .await
        .expect("ensure_agent");
    assert_eq!(h.workspace, "repo-a");
    assert!(backend.received_any_spec(), "provision reached backend");

    // 2. `watch_fleet` observes it. `ensure_agent` attaches the agent as part
    // of spawning it, so `reconcile` treats it as already-known and suppresses
    // a *live* `Discovered` diff for it (fleet.rs's `reconcile`: "Suppress
    // discovered for agents we just spawned"). The half of the contract this
    // exercises is `watch_fleet`'s *initial listing* (its doc comment: "an
    // initial listing followed by live change events").
    wait_for_discovered(provider, &h.id).await;

    // 3. `stop_agent(id, DrainPolicy::Kill)` stops it; `watch_fleet` observes
    // `Gone`. Subscribe *before* triggering the stop so this is a genuine live
    // diff, not another initial-listing read. A backend's `Kill` may only
    // mark the agent stopped rather than removing it immediately (matching
    // real caliban, where a killed agent's registry entry is reaped later,
    // not instantly) — `simulate_reap` simulates that eventual reap
    // deterministically instead of the test waiting on it.
    let mut changes = provider.watch_fleet();
    provider
        .stop_agent(&h.id, DrainPolicy::Kill)
        .await
        .expect("stop_agent");
    backend.simulate_reap(h.id.as_str());

    let deadline = tokio::time::Instant::now() + STEP_TIMEOUT;
    let mut gone = false;
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(item)) = tokio::time::timeout(Duration::from_millis(200), changes.next()).await
        else {
            continue;
        };
        if matches!(&item, FleetChange::Gone { id, .. } if id == &h.id) {
            gone = true;
            break;
        }
    }
    assert!(
        gone,
        "watch_fleet did not observe Gone for the stopped agent"
    );

    // 4. `restart_agent` yields a fresh id. Provision a second, still-live
    // agent for this rather than reusing `h`: the `remove_agent` call above
    // that let us observe `Gone` also means `h.id` is no longer registered at
    // all, so a respawn against it would legitimately fail with `NotFound`
    // rather than exercise the "restart a live agent" contract.
    let h2 = provider
        .ensure_agent(TaskSpec {
            workspace: "repo-a".into(),
            request: SpawnRequest::new("task-2"),
        })
        .await
        .expect("ensure_agent (2nd agent, for restart)");
    wait_for_discovered(provider, &h2.id).await;

    // The trait contract is that `restart_agent` succeeds and returns *an* id
    // for the restarted agent — "the (possibly new) id". Backends differ on
    // whether it changes: `LocalFleet`/`FakeCaliband`'s `Respawn` assigns a
    // fresh caliban id, while `K8sFleet` keeps the spec-deterministic CR name
    // as a stable identity (prospero #77 M1). So the conformance bar is a
    // non-empty id, not a *different* one. (It deliberately does NOT assert the
    // id later shows up in a `watch_fleet` listing — the fake never makes that
    // true for the returned id.)
    let new_id = provider.restart_agent(&h2.id).await.expect("restart_agent");
    assert!(
        !new_id.as_str().is_empty(),
        "restart_agent must return an id for the restarted agent"
    );
}

/// Build a minimal `AgentRecord` for tests.
pub fn test_record(id: &str, dir: &Path, status: AgentStatus, isolated: bool) -> AgentRecord {
    AgentRecord {
        id: id.into(),
        name: id.into(),
        status,
        started_at: "1970-01-01T00:00:00Z".into(),
        session_dir: dir.join(id),
        endpoint: Endpoint::Unix {
            path: dir.join(format!("{id}.sock")),
        },
        spec: SpawnSpec {
            label: Some(id.into()),
            frontmatter_path: None,
            initial_prompt: "task".into(),
            model: None,
            provider: None,
            tool_allowlist: None,
            isolation_worktree: isolated,
            inherit_hooks: true,
            interactive: false,
        },
    }
}
