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
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;

use crate::caliband::wire::{
    AgentRecord, AgentStatus, CtlReply, CtlRequest, DaemonStatus, SpawnSpec, SupervisorError,
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
        let listener = UnixListener::bind(&control_socket)?;
        let state = Arc::new(Mutex::new(FakeState::default()));

        let st = state.clone();
        let dir2 = dir.clone();
        let socket2 = control_socket.clone();
        let accept_task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let conn_st = st.clone();
                let dir = dir2.clone();
                handle_control_conn(stream, conn_st, dir).await.ok();
                if st.lock().unwrap().should_stop {
                    let _ = std::fs::remove_file(&socket2);
                    break;
                }
            }
        });

        Ok(Self {
            control_socket,
            state,
            tasks: vec![accept_task],
        })
    }

    /// The control socket path the fake is listening on.
    pub fn control_socket(&self) -> &Path {
        &self.control_socket
    }

    /// Pre-register an agent with a stream script, and start its per-agent
    /// stream listener so an attach will replay `script` then close.
    pub async fn add_agent(&mut self, record: AgentRecord, script: Vec<serde_json::Value>) {
        let socket_path = record.socket_path.clone();
        {
            let mut st = self.state.lock().unwrap();
            st.scripts.insert(record.id.clone(), script.clone());
            st.agents.insert(record.id.clone(), record);
        }
        let task = spawn_stream_listener(&socket_path, script).await;
        self.tasks.push(task);
    }

    /// All spawn specs received so far (in order).
    pub fn received_specs(&self) -> Vec<SpawnSpec> {
        self.state.lock().unwrap().received_specs.clone()
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

impl Drop for FakeCaliband {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
        let _ = std::fs::remove_file(&self.control_socket);
    }
}

async fn handle_control_conn(
    stream: UnixStream,
    state: Arc<Mutex<FakeState>>,
    dir: PathBuf,
) -> std::io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
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
                // Default script: an init frame then a success result.
                let script = vec![
                    serde_json::json!({
                        "type": "system", "subtype": "init",
                        "model": spec.model.clone().unwrap_or_else(|| "model".into()),
                        "tools": ["Read"],
                        "session_id": id.clone(),
                    }),
                    serde_json::json!({
                        "type": "result", "subtype": "success",
                        "total_cost_usd": 0.0, "turns": 1,
                    }),
                ];
                let record = AgentRecord {
                    id: id.clone(),
                    name: spec.label.clone().unwrap_or_else(|| id.clone()),
                    status: AgentStatus::Running,
                    started_at: "1970-01-01T00:00:00Z".into(),
                    session_dir: dir.join(&id),
                    socket_path: socket_path.clone(),
                    spec: spec.clone(),
                };
                st.scripts.insert(id.clone(), script.clone());
                st.agents.insert(id.clone(), record);
                (
                    CtlReply::Spawned {
                        id,
                        socket_path: socket_path.clone(),
                    },
                    Some((socket_path, script)),
                )
            }
            CtlRequest::Attach { id } => match st.agents.get(&id) {
                Some(a) => (
                    CtlReply::AttachAck {
                        socket_path: a.socket_path.clone(),
                    },
                    None,
                ),
                None => (
                    CtlReply::Error {
                        error: SupervisorError::NotFound { id },
                    },
                    None,
                ),
            },
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
                    socket_path: dir.join("control.sock"),
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

/// Build a minimal `AgentRecord` for tests.
pub fn test_record(id: &str, dir: &Path, status: AgentStatus, isolated: bool) -> AgentRecord {
    AgentRecord {
        id: id.into(),
        name: id.into(),
        status,
        started_at: "1970-01-01T00:00:00Z".into(),
        session_dir: dir.join(id),
        socket_path: dir.join(format!("{id}.sock")),
        spec: SpawnSpec {
            label: Some(id.into()),
            frontmatter_path: None,
            initial_prompt: "task".into(),
            model: None,
            tool_allowlist: None,
            isolation_worktree: isolated,
            inherit_hooks: true,
            interactive: false,
        },
    }
}
