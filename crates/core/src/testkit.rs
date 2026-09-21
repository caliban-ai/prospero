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
    AgentRecord, CtlReply, CtlRequest, DaemonStatus, Endpoint, EndpointExt, SpawnSpec,
    SupervisorError, WireAgentStatus as AgentStatus, WirePermissionPosture as PermissionPosture,
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
    /// How many `List` requests have been served. Lets a test assert that a
    /// polling caller stops dialling once there is nothing left to learn (#194).
    lists: u32,
    /// Set to `true` after the first `Shutdown`; the accept loop exits.
    should_stop: bool,
    /// Every agent id a `Kill` request has named, in order — so a test can
    /// prove a policy actually killed the agent, not merely logged that it
    /// meant to (#221).
    killed_ids: Vec<String>,
}

/// A running fake caliband daemon. Aborts its listener tasks on drop.
pub struct FakeCaliband {
    control_socket: PathBuf,
    state: Arc<Mutex<FakeState>>,
    tasks: Vec<JoinHandle<()>>,
    /// Owns the temp dir backing a TCP fake's per-agent sockets, if any, so it
    /// outlives the fake. `None` for the Unix path (caller owns the dir).
    _tempdir: Option<tempfile::TempDir>,
    /// `(cert_pem, key_pem)` of a TCP+TLS fake's self-signed cert, so
    /// `add_agent_tcp` can serve per-agent streams over TLS with the *same*
    /// cert the client already trusts (`CalibandTlsFixture::ca_pem`). `None`
    /// for the Unix path. (#77 I1)
    tls_material: Option<(Vec<u8>, Vec<u8>)>,
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
            tls_material: None,
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
        let tls_material = Some((cert_pem.clone(), key_pem.clone()));

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
                tls_material,
            },
            CalibandTlsFixture {
                addr,
                ca_pem: cert_pem,
            },
        ))
    }

    /// Register a per-agent stream served over **TCP + TLS** (the leg #77 I1
    /// proves is network-routable), reusing this TCP fake's own cert so the
    /// client's `CalibandTlsFixture::ca_pem` trust already covers it. The
    /// agent's `AttachAck`/`Spawned` reply then advertises an `Endpoint::Tcp`,
    /// so a client that `attach`es over the network gets a **TCP** stream
    /// endpoint (not a same-process Unix path). Requires a `start_tcp_tls` fake.
    /// (#77 I1)
    pub async fn add_agent_tcp(&mut self, id: &str, script: Vec<serde_json::Value>) {
        let (cert_pem, key_pem) = self
            .tls_material
            .clone()
            .expect("add_agent_tcp requires a TCP+TLS fake (start_tcp_tls)");
        let (addr, task) = spawn_tcp_stream_listener(script, &cert_pem, &key_pem).await;
        let mut record = test_record(
            id,
            Path::new("/tmp"),
            crate::model::AgentStatus::Running,
            false,
        );
        record.endpoint = Endpoint::Tcp { addr };
        self.state
            .lock()
            .unwrap()
            .agents
            .insert(id.to_string(), record);
        self.tasks.push(task);
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

    /// Number of `List` requests the fake has served. Used to prove a polling
    /// caller stops dialling a pod once it has nothing left to learn (#194) —
    /// without which the k8s watch loop would re-dial finished agents forever,
    /// since their CR phase never stops saying `Running`.
    pub fn lists(&self) -> u32 {
        self.state.lock().unwrap().lists
    }

    /// Agent ids this fake has been asked to `Kill`, in order (#221).
    pub fn killed_ids(&self) -> Vec<String> {
        self.state.lock().unwrap().killed_ids.clone()
    }

    /// Set an agent's status (to simulate lifecycle transitions across polls).
    ///
    /// Takes prospero's domain status — what a test is reasoning about — and
    /// converts to the wire status the fake stores, so call sites didn't have
    /// to learn caliban's enum when the contract crate landed (#239).
    pub fn set_status(&self, id: &str, status: crate::model::AgentStatus) {
        if let Some(a) = self.state.lock().unwrap().agents.get_mut(id) {
            a.status = crate::caliband::wire::wire_status(status);
        }
    }

    /// Set an agent's `spec.interactive` flag (to simulate an interactive agent
    /// the `List` control reply advertises). No-op if the id isn't registered.
    pub fn set_interactive(&self, id: &str, interactive: bool) {
        if let Some(a) = self.state.lock().unwrap().agents.get_mut(id) {
            a.spec.interactive = interactive;
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
            CtlRequest::List => {
                st.lists += 1;
                (
                    CtlReply::Listed {
                        agents: st.agents.values().cloned().collect(),
                    },
                    None,
                )
            }
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
                    working_dir: dir.clone(),
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
                st.killed_ids.push(id.clone());
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
            // Control requests the contract carries that prospero never sends
            // (#239). Answered plausibly rather than ignored, so a future
            // prospero that does send one sees a real reply here first. `Drain`
            // checkpoints every live agent (caliban ADR 0057); `ReportStatus`
            // is a worker reporting its own Running/Idle transition.
            CtlRequest::Drain { .. } => {
                let drained: Vec<_> = st
                    .agents
                    .values()
                    .map(|rec| crate::caliband::wire::DrainedAgent {
                        id: rec.id.clone(),
                        session_dir: rec.session_dir.clone(),
                    })
                    .collect();
                for rec in st.agents.values_mut() {
                    rec.status = AgentStatus::Drained;
                }
                (CtlReply::Drained { agents: drained }, None)
            }
            CtlRequest::ReportStatus { id, status } => {
                if let Some(rec) = st.agents.get_mut(&id) {
                    rec.status = status;
                }
                (CtlReply::StatusReported, None)
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

/// Bind a per-agent stream socket over **TCP + TLS** that, on each connection,
/// writes the scripted frames as NDJSON then closes. Returns the bound
/// `host:port`. The TLS cert must be the one the dialing client trusts. (#77 I1)
async fn spawn_tcp_stream_listener(
    script: Vec<serde_json::Value>,
    cert_pem: &[u8],
    key_pem: &[u8],
) -> (String, JoinHandle<()>) {
    use crate::caliband::transport::{Listener, tls_server_from_pem};
    use tokio::io::AsyncWriteExt as _;
    let listener = Listener::bind(&crate::caliband::transport::BindSpec {
        endpoint: Endpoint::Tcp {
            addr: "127.0.0.1:0".into(),
        },
        tls: Some(tls_server_from_pem(cert_pem, key_pem).expect("server tls")),
        // No token check on the stream leg; the client may still send one (its
        // control token). The accept loop below drains those inbound bytes
        // concurrently so they don't linger unread in the receive buffer — an
        // unread buffer would make a Linux `close()` emit a RST that truncates
        // the frames we're writing (see the accept loop's drain comment).
        token: None,
    })
    .await
    .expect("bind per-agent tcp stream listener");
    let addr = listener.local_addr().expect("tcp stream addr");
    let task = tokio::spawn(async move {
        use tokio::io::AsyncReadExt as _;
        while let Ok(conn) = listener.accept().await {
            // Split so we can DRAIN the client's inbound bytes concurrently
            // with writing frames. The client's `open_stream` sends a bearer
            // preamble (`client_send_token`) that this listener declares no
            // token for and so never reads. If those bytes sat unread in the
            // socket's receive buffer when we close, Linux answers `close()`
            // with a TCP **RST** instead of a FIN, which discards the frames
            // still in flight to the client — truncating the stream and
            // hanging the reader (macOS closes gracefully, hiding this). A
            // concurrent drain keeps the receive buffer empty so the close is
            // a clean FIN on every platform.
            let (mut rd, mut wr) = tokio::io::split(conn);
            let drain = tokio::spawn(async move {
                let mut scratch = [0u8; 256];
                while let Ok(n) = rd.read(&mut scratch).await {
                    if n == 0 {
                        break;
                    }
                }
                rd // hold the read half open until writing is done
            });
            for frame in &script {
                let mut line = serde_json::to_vec(frame).unwrap();
                line.push(b'\n');
                if wr.write_all(&line).await.is_err() {
                    break;
                }
            }
            let _ = wr.flush().await;
            // Clean half-close: FIN/close_notify so the client reads a proper
            // end-of-stream, then release the drain so the connection drops
            // with nothing unread.
            let _ = wr.shutdown().await;
            drain.abort();
        }
    });
    (addr, task)
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
            actor: None,
            on_behalf_of: None,
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

    // #2: `actor` round-trips, and `None` stays `None`.
    let attributed = FleetEvent {
        seq: 1,
        ts: "2026-09-13T00:00:00Z".into(),
        repo: "actor-repo".into(),
        agent_id: "actor-agent".into(),
        kind: EventKind::AgentSpawned,
        actor: Some("alice".into()),
        on_behalf_of: None,
    };
    let unattributed = FleetEvent {
        seq: 2,
        actor: None,
        on_behalf_of: None,
        kind: EventKind::AgentGone,
        ..attributed.clone()
    };
    // #251: a client acting for someone else — both identities must survive,
    // because the whole point is telling one token's spawns apart by person.
    let delegated = FleetEvent {
        seq: 3,
        actor: Some("ariel".into()),
        on_behalf_of: Some("discord:U123".into()),
        kind: EventKind::AgentSpawned,
        ..attributed.clone()
    };
    store.append(&attributed).await.unwrap();
    store.append(&unattributed).await.unwrap();
    store.append(&delegated).await.unwrap();
    let back = store.replay(&attributed.stream_key(), 0).await.unwrap();
    assert_eq!(back.len(), 3);
    assert_eq!(back[0].actor.as_deref(), Some("alice"));
    assert_eq!(back[0].on_behalf_of, None);
    assert_eq!(back[1].actor, None);
    assert_eq!(back[2].actor.as_deref(), Some("ariel"));
    assert_eq!(
        back[2].on_behalf_of.as_deref(),
        Some("discord:U123"),
        "the asserted subject must survive a round trip, not just the token"
    );
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
            actor: None,
            on_behalf_of: None,
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

/// Fleet-wide replay contract every [`crate::store::Store`] must satisfy
/// (prospero #219): `replay_fleet(after, limit)` returns events from **every**
/// stream in durable insertion order, each carrying a fleet-wide cursor.
///
/// Per-stream `seq` cannot order a fleet: two agents both have a `seq` 1, and
/// nothing says which happened first. The cursor is the backend's own insertion
/// order, which is the only total order that exists, and it is what a resuming
/// client hands back — so it must be strictly increasing, exclusive on `after`,
/// and stable for an event once written.
pub async fn store_fleet_replay_conformance(store: &dyn crate::store::Store) {
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
            actor: None,
            on_behalf_of: None,
        }
    }

    assert!(
        store.replay_fleet(0, 100).await.unwrap().is_empty(),
        "an empty store replays nothing"
    );
    assert_eq!(
        store.latest_fleet_cursor().await.unwrap(),
        0,
        "an empty store's head is 0, so `from=now` starts at the beginning"
    );

    // Interleaved across two agents: `seq` alone could not order these.
    store.append(&ev(1, "a", "a1")).await.unwrap();
    store.append(&ev(1, "b", "b1")).await.unwrap();
    store.append(&ev(2, "a", "a2")).await.unwrap();

    let all = store.replay_fleet(0, 100).await.unwrap();
    assert_eq!(
        all.iter()
            .map(|c| match &c.event.kind {
                EventKind::Output { chunk, .. } => chunk.clone(),
                other => panic!("unexpected kind {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec!["a1", "b1", "a2"],
        "insertion order across streams, not per-stream seq order"
    );

    let cursors: Vec<u64> = all.iter().map(|c| c.cursor).collect();
    assert!(
        cursors.windows(2).all(|w| w[0] < w[1]),
        "cursors must strictly increase: {cursors:?}"
    );

    // Exclusive on `after`: resuming from a cursor must not repeat that event.
    let resumed = store.replay_fleet(cursors[0], 100).await.unwrap();
    assert_eq!(
        resumed.iter().map(|c| c.cursor).collect::<Vec<_>>(),
        cursors[1..].to_vec(),
        "replay_fleet(after) is exclusive"
    );

    // The cursor of an already-returned event does not move when more arrive.
    store.append(&ev(2, "b", "b2")).await.unwrap();
    let again = store.replay_fleet(0, 100).await.unwrap();
    assert_eq!(
        again.iter().take(3).map(|c| c.cursor).collect::<Vec<_>>(),
        cursors,
        "a written event's cursor is stable"
    );
    assert_eq!(again.len(), 4);

    // `limit` caps the batch, so a long history resumes in bounded chunks.
    let first_two = store.replay_fleet(0, 2).await.unwrap();
    assert_eq!(first_two.len(), 2);
    assert_eq!(
        first_two.iter().map(|c| c.cursor).collect::<Vec<_>>(),
        cursors[..2].to_vec()
    );

    // Past the end is empty, not an error.
    let last = again.last().unwrap().cursor;
    assert!(store.replay_fleet(last, 100).await.unwrap().is_empty());

    // The head is the cursor a `from=now` client starts after: replaying from
    // it yields nothing until something new is written.
    assert_eq!(store.latest_fleet_cursor().await.unwrap(), last);
    store.append(&ev(3, "a", "a3")).await.unwrap();
    let after_head = store.replay_fleet(last, 100).await.unwrap();
    assert_eq!(after_head.len(), 1, "only what arrived after the head");
    assert_eq!(
        store.latest_fleet_cursor().await.unwrap(),
        after_head[0].cursor
    );
}

/// Usage-aggregation contract every [`crate::store::Store`] must satisfy
/// (prospero #180): `usage(since, until)` returns one row per
/// (workspace, UTC day) that saw terminal activity, summing cost and turns from
/// `AgentFinished` and counting outcomes from terminal `StatusChanged`
/// transitions.
///
/// The two come from different events on purpose. `AgentFinished.outcome` is
/// caliban's raw result subtype ("EndOfTurn", "max_turns"), an open vocabulary;
/// the done/failed/killed/crashed breakdown the dashboard charts is the
/// terminal [`crate::model::AgentStatus`] set, which only `StatusChanged`
/// carries. A killed agent therefore contributes an outcome but no cost — that
/// divergence is real, and asserted below.
pub async fn store_usage_conformance(store: &dyn crate::store::Store) {
    use crate::event::{EventKind, FleetEvent};
    use crate::model::AgentStatus;

    fn ev(seq: u64, ts: &str, repo: &str, agent: &str, kind: EventKind) -> FleetEvent {
        FleetEvent {
            seq,
            ts: ts.into(),
            repo: repo.into(),
            agent_id: agent.into(),
            kind,
            actor: None,
            on_behalf_of: None,
        }
    }

    fn finished(cost: f64, turns: u32) -> EventKind {
        EventKind::AgentFinished {
            outcome: "EndOfTurn".into(),
            cost_usd: cost,
            turns,
        }
    }

    fn moved_to(to: AgentStatus) -> EventKind {
        EventKind::StatusChanged {
            from: AgentStatus::Running,
            to,
        }
    }

    // Before the window — must be excluded entirely.
    store
        .append(&ev(
            1,
            "2026-07-31T23:59:59+00:00",
            "alpha",
            "old",
            finished(99.0, 99),
        ))
        .await
        .unwrap();
    store
        .append(&ev(
            2,
            "2026-07-31T23:59:59+00:00",
            "alpha",
            "old",
            moved_to(AgentStatus::Done),
        ))
        .await
        .unwrap();

    // alpha, day 1: one clean finish and one failure.
    store
        .append(&ev(
            1,
            "2026-08-01T10:00:00+00:00",
            "alpha",
            "a1",
            finished(0.50, 3),
        ))
        .await
        .unwrap();
    store
        .append(&ev(
            2,
            "2026-08-01T10:00:01+00:00",
            "alpha",
            "a1",
            moved_to(AgentStatus::Done),
        ))
        .await
        .unwrap();
    store
        .append(&ev(
            1,
            "2026-08-01T11:00:00+00:00",
            "alpha",
            "a2",
            finished(0.25, 1),
        ))
        .await
        .unwrap();
    store
        .append(&ev(
            2,
            "2026-08-01T11:00:01+00:00",
            "alpha",
            "a2",
            moved_to(AgentStatus::Failed),
        ))
        .await
        .unwrap();

    // alpha, day 2: a separate bucket, so the series is per-day not per-window.
    store
        .append(&ev(
            1,
            "2026-08-02T09:00:00+00:00",
            "alpha",
            "a3",
            finished(1.00, 2),
        ))
        .await
        .unwrap();
    store
        .append(&ev(
            2,
            "2026-08-02T09:00:01+00:00",
            "alpha",
            "a3",
            moved_to(AgentStatus::Crashed),
        ))
        .await
        .unwrap();

    // beta: killed before it ever finished — an outcome with no cost at all.
    store
        .append(&ev(
            1,
            "2026-08-01T12:00:00+00:00",
            "beta",
            "b1",
            moved_to(AgentStatus::Killed),
        ))
        .await
        .unwrap();
    // A non-terminal transition must not be counted as an outcome.
    store
        .append(&ev(
            2,
            "2026-08-01T12:00:01+00:00",
            "beta",
            "b1",
            moved_to(AgentStatus::Idle),
        ))
        .await
        .unwrap();

    // #221: a run prospero killed for passing its deadline. It is *also* a
    // `killed` transition — the kill is real — but the timeout is what an
    // operator needs to see separately, since it is policy rather than someone
    // intervening.
    store
        .append(&ev(
            1,
            "2026-08-01T13:00:00+00:00",
            "beta",
            "b2",
            EventKind::AgentTimedOut {
                deadline: "2026-08-01T13:00:00+00:00".into(),
            },
        ))
        .await
        .unwrap();
    store
        .append(&ev(
            2,
            "2026-08-01T13:00:01+00:00",
            "beta",
            "b2",
            moved_to(AgentStatus::Killed),
        ))
        .await
        .unwrap();

    let rows = store
        .usage("2026-08-01T00:00:00+00:00", "2026-08-03T00:00:00+00:00")
        .await
        .unwrap();

    let find = |workspace: &str, day: &str| {
        rows.iter()
            .find(|r| r.workspace == workspace && r.day == day)
            .unwrap_or_else(|| panic!("no row for {workspace}/{day} in {rows:?}"))
    };

    let a1 = find("alpha", "2026-08-01");
    assert!(
        (a1.cost_usd - 0.75).abs() < 1e-9,
        "alpha day 1 cost should sum both finishes, got {}",
        a1.cost_usd
    );
    assert_eq!(a1.turns, 4, "alpha day 1 turns should sum both finishes");
    assert_eq!(a1.done, 1);
    assert_eq!(a1.failed, 1);
    assert_eq!(a1.killed, 0);
    assert_eq!(a1.crashed, 0);

    assert_eq!(a1.timed_out, 0, "a day with no timeout must not report one");

    let a2 = find("alpha", "2026-08-02");
    assert!((a2.cost_usd - 1.00).abs() < 1e-9);
    assert_eq!(a2.turns, 2);
    assert_eq!(a2.crashed, 1);
    assert_eq!(a2.done, 0);

    // The divergence: an outcome with no matching AgentFinished.
    let b = find("beta", "2026-08-01");
    assert_eq!(b.cost_usd, 0.0, "a killed agent never reported cost");
    assert_eq!(b.turns, 0);
    assert_eq!(b.killed, 2, "both the manual kill and the timeout's kill");
    assert_eq!(
        b.timed_out, 1,
        "the timeout is counted separately from the kill it caused (#221)"
    );
    assert_eq!(
        b.done + b.failed + b.crashed,
        0,
        "Idle is not terminal and must not be counted"
    );

    // Exactly the three (workspace, day) pairs above — the pre-window events
    // must not have created a row.
    assert_eq!(rows.len(), 3, "unexpected rows: {rows:?}");

    // An empty window aggregates to nothing rather than erroring.
    let empty = store
        .usage("2027-01-01T00:00:00+00:00", "2027-02-01T00:00:00+00:00")
        .await
        .unwrap();
    assert!(empty.is_empty(), "empty window should yield no rows");
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
            provider: Some("openai".into()),
            ..Default::default()
        },
    };
    store.upsert_repo(&r).await.unwrap();
    let repos = store.list_repos().await.unwrap();
    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0].name, "p");
    assert_eq!(repos[0].root, std::path::PathBuf::from("/r"));
    assert_eq!(repos[0].config.provider.as_deref(), Some("openai"));

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

/// A minimal automation for the conformance battery.
#[cfg(any(test, feature = "testkit"))]
fn sample_automation(id: &str) -> crate::automation::StoredAutomation {
    use crate::automation::{Automation, SpawnTemplate, StoredAutomation, Trigger};
    StoredAutomation {
        automation: Automation {
            id: id.to_string(),
            workspace: "w".into(),
            trigger: Trigger::Cron {
                schedule: "*/5 * * * *".into(),
            },
            template: SpawnTemplate {
                task: "do the thing".into(),
                ..Default::default()
            },
            enabled: true,
            created_at: "2026-09-20T10:00:00Z".into(),
            last_fired_at: None,
        },
        webhook_secret: None,
    }
}

/// Behavioral contract every [`crate::config_store::ConfigStore`] must satisfy
/// for automations (#220): CRUD round-trips, `last_fired_at` stays under the
/// claim's control, run history is newest-first and bounded, and deleting an
/// automation takes its runs with it.
pub async fn automation_store_conformance(store: &dyn crate::config_store::ConfigStore) {
    use crate::automation::{AutomationRun, RUN_HISTORY_LIMIT, RunSource};

    assert!(store.list_automations().await.unwrap().is_empty());

    let a = sample_automation("nightly");
    store.upsert_automation(&a).await.unwrap();
    let stored = store.list_automations().await.unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0], a, "an automation must round-trip unchanged");

    // A webhook secret survives the round trip — it is the credential, so
    // losing it silently would lock every sender out.
    let mut hooked = sample_automation("hooked");
    hooked.automation.trigger = crate::automation::Trigger::Webhook;
    hooked.webhook_secret = Some("s3cret".into());
    store.upsert_automation(&hooked).await.unwrap();
    let got = store.list_automations().await.unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].automation.id, "hooked", "list is ordered by id");
    assert_eq!(got[0].webhook_secret.as_deref(), Some("s3cret"));

    // Editing an automation must not move its fire clock.
    store
        .claim_automation_fire("nightly", "2026-09-20T10:05:00Z")
        .await
        .unwrap();
    let mut edited = a.clone();
    edited.automation.template.task = "do another thing".into();
    store.upsert_automation(&edited).await.unwrap();
    let after = store.list_automations().await.unwrap();
    let nightly = after.iter().find(|s| s.automation.id == "nightly").unwrap();
    assert_eq!(nightly.automation.template.task, "do another thing");
    assert_eq!(
        nightly.automation.last_fired_at.as_deref(),
        Some("2026-09-20T10:05:00Z"),
        "an ordinary edit must not rewind or advance the fire clock"
    );

    // Run history: newest first.
    for (i, at) in ["10:05:00", "10:10:00", "10:15:00"].iter().enumerate() {
        store
            .record_run(&AutomationRun {
                automation_id: "nightly".into(),
                fired_at: format!("2026-09-20T{at}Z"),
                source: RunSource::Schedule,
                agent_id: Some(format!("agent-{i}")),
                error: None,
            })
            .await
            .unwrap();
    }
    let runs = store.list_runs("nightly", 10).await.unwrap();
    assert_eq!(runs.len(), 3);
    assert_eq!(runs[0].fired_at, "2026-09-20T10:15:00Z");
    assert_eq!(runs[0].agent_id.as_deref(), Some("agent-2"));
    assert_eq!(runs[2].fired_at, "2026-09-20T10:05:00Z");
    assert!(store.list_runs("nightly", 2).await.unwrap().len() == 2);
    assert!(store.list_runs("hooked", 10).await.unwrap().is_empty());

    // A failed run is recorded too — an automation that quietly stops working
    // is exactly what this history exists to make visible.
    store
        .record_run(&AutomationRun {
            automation_id: "hooked".into(),
            fired_at: "2026-09-20T11:00:00Z".into(),
            source: RunSource::Webhook,
            agent_id: None,
            error: Some("workspace 'w' is unreachable".into()),
        })
        .await
        .unwrap();
    let failed = store.list_runs("hooked", 10).await.unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].source, RunSource::Webhook);
    assert!(failed[0].agent_id.is_none());
    assert_eq!(
        failed[0].error.as_deref(),
        Some("workspace 'w' is unreachable")
    );

    // Deleting takes the run history with it, rather than orphaning rows that
    // a later automation reusing the id would inherit.
    assert!(store.delete_automation("nightly").await.unwrap());
    assert!(!store.delete_automation("nightly").await.unwrap());
    assert!(store.list_runs("nightly", 10).await.unwrap().is_empty());
    assert_eq!(store.list_automations().await.unwrap().len(), 1);

    // History is bounded: a busy automation must not grow the config DB
    // without limit.
    for i in 0..(RUN_HISTORY_LIMIT + 25) {
        store
            .record_run(&AutomationRun {
                automation_id: "hooked".into(),
                fired_at: format!("2026-09-21T{:02}:{:02}:00Z", i / 60, i % 60),
                source: RunSource::Schedule,
                agent_id: Some(format!("a{i}")),
                error: None,
            })
            .await
            .unwrap();
    }
    let kept = store
        .list_runs("hooked", RUN_HISTORY_LIMIT * 2)
        .await
        .unwrap();
    assert_eq!(kept.len(), RUN_HISTORY_LIMIT);
    assert_eq!(
        kept[0].agent_id.as_deref(),
        Some(format!("a{}", RUN_HISTORY_LIMIT + 24).as_str()),
        "trimming must drop the oldest runs, not the newest"
    );
}

/// The exactly-once contract behind scheduled spawns (#220, acceptance
/// criterion 3): when several replicas see the same due tick, the store hands
/// the fire to exactly one of them.
pub async fn automation_claim_conformance(store: &dyn crate::config_store::ConfigStore) {
    store
        .upsert_automation(&sample_automation("nightly"))
        .await
        .unwrap();

    // Three replicas racing on one tick.
    let tick = "2026-09-20T10:05:00Z";
    let mut wins = 0;
    for _ in 0..3 {
        if store.claim_automation_fire("nightly", tick).await.unwrap() {
            wins += 1;
        }
    }
    assert_eq!(wins, 1, "exactly one replica may fire a given tick");

    // The next tick is claimable exactly once again.
    let next = "2026-09-20T10:10:00Z";
    assert!(store.claim_automation_fire("nightly", next).await.unwrap());
    assert!(!store.claim_automation_fire("nightly", next).await.unwrap());

    // A late replica still holding the *previous* tick must not re-fire it.
    assert!(
        !store.claim_automation_fire("nightly", tick).await.unwrap(),
        "an older tick must never win after a newer one has fired"
    );

    // Claiming an automation that does not exist is a loss, not an error or a
    // phantom row.
    assert!(!store.claim_automation_fire("ghost", next).await.unwrap());
    assert!(
        store
            .list_automations()
            .await
            .unwrap()
            .iter()
            .all(|s| s.automation.id != "ghost")
    );
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
pub fn test_record(
    id: &str,
    dir: &Path,
    status: crate::model::AgentStatus,
    isolated: bool,
) -> AgentRecord {
    AgentRecord {
        id: id.into(),
        name: id.into(),
        // Tests reason in prospero's vocabulary; the fake stores caliban's (#239).
        status: crate::caliband::wire::wire_status(status),
        started_at: "1970-01-01T00:00:00Z".into(),
        session_dir: dir.join(id),
        endpoint: Endpoint::Unix {
            path: dir.join(format!("{id}.sock")),
        },
        working_dir: dir.to_path_buf(),
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
            permission_posture: PermissionPosture::Supervised,
            inherited_hooks_config: None,
            source: None,
            resume_session: None,
            drive_protocol: crate::caliband::wire::DriveProtocol::Ndjson,
        },
    }
}
