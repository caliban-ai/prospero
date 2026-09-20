//! The fleet as an MCP server (#218).
//!
//! prospero exposed REST + SSE only, so an agentic tool could not drive the
//! fleet the way OpenClaw (`openclaw mcp serve`) and Vibe Kanban already allow.
//! This is a thin adapter over the same [`FleetProvider`] seam the HTTP handlers
//! use — no second control path, so both backends (`LocalFleet`, `K8sFleet`)
//! work through it unchanged.
//!
//! **Auth.** The transport is mounted inside the same router as the REST API, so
//! the `route_layer` scope check runs first: `/mcp` requires `operate` (#2), the
//! scope a spawn or kill needs. Read-only tools are reachable with that same
//! credential rather than a lesser one — the surface is a driving surface, and
//! splitting per-tool scopes here would put a second, divergent authorization
//! model behind the one the route already enforces.

use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content, ServerCapabilities, ServerInfo};
use rmcp::{ErrorData, ServerHandler, tool, tool_handler, tool_router};
use serde::Deserialize;
use serde_json::{Value, json};

use prospero_core::fleet_provider::FleetProvider;
use prospero_core::model::{AgentId, DrainPolicy, TaskSpec};
use prospero_core::store::Store;

/// Mount the MCP server as an axum service at `/mcp` (#218).
///
/// Stateful sessions (the transport's default): a client initializes once and
/// keeps the session for its subsequent calls, which is what an editor-shaped
/// client expects. The service is built per connection from the shared seams,
/// so every session sees the same fleet.
#[must_use]
pub fn service(
    fleet: Arc<dyn FleetProvider>,
    store: Arc<dyn Store>,
) -> rmcp::transport::streamable_http_server::StreamableHttpService<
    FleetMcp,
    rmcp::transport::streamable_http_server::session::local::LocalSessionManager,
> {
    rmcp::transport::streamable_http_server::StreamableHttpService::new(
        move || Ok(FleetMcp::new(fleet.clone(), store.clone())),
        Arc::new(
            rmcp::transport::streamable_http_server::session::local::LocalSessionManager::default(),
        ),
        rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default(),
    )
}

/// The MCP server over one fleet.
#[derive(Clone)]
pub struct FleetMcp {
    fleet: Arc<dyn FleetProvider>,
    store: Arc<dyn Store>,
    tool_router: ToolRouter<Self>,
}

impl FleetMcp {
    /// Build a server over the same seams the HTTP handlers use.
    #[must_use]
    pub fn new(fleet: Arc<dyn FleetProvider>, store: Arc<dyn Store>) -> Self {
        Self {
            fleet,
            store,
            tool_router: Self::tool_router(),
        }
    }

    /// A tool failure the *model* should see and can act on — a bad agent id,
    /// an unreachable backend — as opposed to a protocol error.
    fn err(msg: impl Into<String>) -> Result<CallToolResult, ErrorData> {
        Ok(CallToolResult::error(vec![Content::text(msg.into())]))
    }

    fn ok(value: Value) -> Result<CallToolResult, ErrorData> {
        Ok(CallToolResult::success(vec![Content::json(value)?]))
    }
}

/// Arguments naming one agent.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AgentArgs {
    /// The agent's id, as returned by `prospero_list_agents`.
    pub agent_id: String,
}

/// Arguments for a spawn.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SpawnArgs {
    /// Workspace to spawn under.
    pub workspace: String,
    /// The task for the agent.
    pub prompt: String,
    /// Optional human-readable label.
    #[serde(default)]
    pub label: Option<String>,
    /// Optional model override.
    #[serde(default)]
    pub model: Option<String>,
    /// Await operator input instead of finishing at the end of a run.
    #[serde(default)]
    pub interactive: bool,
    /// Kill the agent after this many seconds of wall-clock time (#221).
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

/// Arguments for reading an agent's history.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct EventsArgs {
    /// The agent's id.
    pub agent_id: String,
    /// Return events with `seq >= from`. Defaults to 0.
    #[serde(default)]
    pub from: u64,
    /// Cap on how many events to return. Defaults to 100, max 500 — a model
    /// context is finite, and a long run's history is not.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Arguments for sending input to an interactive agent.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct InputArgs {
    /// The agent's id.
    pub agent_id: String,
    /// Message text to deliver.
    pub text: String,
}

/// Default and ceiling for `prospero_agent_events`.
const EVENTS_DEFAULT: usize = 100;
const EVENTS_MAX: usize = 500;

#[tool_router]
impl FleetMcp {
    #[tool(description = "List the workspaces prospero supervises, with health and agent counts.")]
    async fn prospero_list_workspaces(&self) -> Result<CallToolResult, ErrorData> {
        let snap = self.fleet.snapshot().await;
        let workspaces: Vec<Value> = snap
            .workspaces
            .iter()
            .map(|w| {
                json!({
                    "name": w.name,
                    "healthy": matches!(w.health, prospero_core::model::WorkspaceHealth::Healthy),
                    "agents": w.agents.len(),
                })
            })
            .collect();
        Self::ok(json!({ "workspaces": workspaces }))
    }

    #[tool(description = "List every agent in the fleet with its status and workspace.")]
    async fn prospero_list_agents(&self) -> Result<CallToolResult, ErrorData> {
        let snap = self.fleet.snapshot().await;
        let agents: Vec<Value> = snap
            .workspaces
            .iter()
            .flat_map(|w| w.agents.iter())
            .map(agent_json)
            .collect();
        Self::ok(json!({ "agents": agents }))
    }

    #[tool(description = "Launch an agent in a workspace; returns its id.")]
    async fn prospero_spawn_agent(
        &self,
        Parameters(args): Parameters<SpawnArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let mut request = prospero_core::fleet::SpawnRequest::new(args.prompt);
        request.label = args.label;
        request.model = args.model;
        request.interactive = args.interactive;
        request.timeout_secs = args.timeout_secs;

        match self
            .fleet
            .ensure_agent(TaskSpec {
                workspace: args.workspace.clone(),
                request,
            })
            .await
        {
            Ok(handle) => Self::ok(json!({
                "agent_id": handle.id.to_string(),
                "workspace": args.workspace,
                "created": handle.created,
            })),
            Err(e) => Self::err(format!("spawn failed: {e}")),
        }
    }

    #[tool(description = "Read one agent's current state.")]
    async fn prospero_agent_status(
        &self,
        Parameters(args): Parameters<AgentArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let snap = self.fleet.snapshot().await;
        match snap.find_agent(&args.agent_id) {
            Some((_, agent)) => Self::ok(agent_json(agent)),
            None => Self::err(format!("unknown agent: {}", args.agent_id)),
        }
    }

    #[tool(description = "Read an agent's recorded events from `from` onward (bounded).")]
    async fn prospero_agent_events(
        &self,
        Parameters(args): Parameters<EventsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let limit = args.limit.unwrap_or(EVENTS_DEFAULT).min(EVENTS_MAX);
        let events = match self.store.replay(&args.agent_id, args.from).await {
            Ok(events) => events,
            Err(e) => return Self::err(format!("reading history failed: {e}")),
        };
        let total = events.len();
        let page: Vec<&prospero_core::FleetEvent> = events.iter().take(limit).collect();
        let next = page.last().map(|e| e.seq + 1);
        Self::ok(json!({
            "events": page,
            // A model cannot tell a short page from the end of history without
            // being told, and would stop reading early.
            "truncated": total > limit,
            "next_from": next,
        }))
    }

    #[tool(description = "Send a message to an interactive agent, resuming its run.")]
    async fn prospero_send_input(
        &self,
        Parameters(args): Parameters<InputArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let frame = prospero_core::AttachInbound::UserMessage { text: args.text };
        match self
            .fleet
            .send_input(&AgentId::from(args.agent_id.clone()), frame)
            .await
        {
            Ok(()) => Self::ok(json!({ "delivered": true })),
            Err(e) => Self::err(format!("send failed: {e}")),
        }
    }

    #[tool(description = "Signal end-of-input to an interactive agent; it finishes after.")]
    async fn prospero_end_input(
        &self,
        Parameters(args): Parameters<AgentArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        match self
            .fleet
            .send_input(
                &AgentId::from(args.agent_id.clone()),
                prospero_core::AttachInbound::EndInput,
            )
            .await
        {
            Ok(()) => Self::ok(json!({ "delivered": true })),
            Err(e) => Self::err(format!("end-input failed: {e}")),
        }
    }

    #[tool(description = "Kill a running agent.")]
    async fn prospero_kill_agent(
        &self,
        Parameters(args): Parameters<AgentArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        match self
            .fleet
            .stop_agent(&AgentId::from(args.agent_id.clone()), DrainPolicy::Kill)
            .await
        {
            Ok(()) => Self::ok(json!({ "killed": args.agent_id })),
            Err(e) => Self::err(format!("kill failed: {e}")),
        }
    }

    #[tool(description = "Kill and respawn an agent with the same spec; returns the new id.")]
    async fn prospero_respawn_agent(
        &self,
        Parameters(args): Parameters<AgentArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        match self
            .fleet
            .restart_agent(&AgentId::from(args.agent_id.clone()))
            .await
        {
            Ok(id) => Self::ok(json!({ "agent_id": id.to_string() })),
            Err(e) => Self::err(format!("respawn failed: {e}")),
        }
    }
}

/// One agent as the tools report it.
fn agent_json(agent: &prospero_core::model::Agent) -> Value {
    json!({
        "agent_id": agent.id,
        "name": agent.name,
        "workspace": agent.workspace,
        "status": agent.status,
        "started_at": agent.started_at,
        "interactive": agent.interactive,
        "isolated": agent.isolated,
        // #241: why the backend put it in this state, when something else did.
        "reason": agent.reason,
        "detail": agent.detail,
    })
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for FleetMcp {
    fn get_info(&self) -> ServerInfo {
        // `ServerInfo` is `#[non_exhaustive]`, so build from its default and
        // set what we mean rather than naming every field.
        let mut info = ServerInfo::default();
        info.instructions = Some(
            "Drive a prospero fleet: list workspaces and agents, spawn, read history, \
             steer an interactive agent, kill or respawn."
                .into(),
        );
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prospero_core::LocalFleet;
    use prospero_core::discovery::{DiscoveryEnv, EnsureConfig};
    use prospero_core::fleet::{FleetConfig, FleetManager};
    use prospero_core::store::JsonlStore;
    use prospero_core::testkit::FakeCaliband;

    /// A server over a real `LocalFleet` talking to a fake caliband — the same
    /// seam the HTTP handlers use, so a passing tool here works in the daemon.
    async fn harness() -> (FleetMcp, FleetManager, FakeCaliband, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("mcp-host", dir.path());
        config.discovery_env = DiscoveryEnv {
            caliban_daemon_runtime_dir: Some(dir.path().to_path_buf()),
            xdg_runtime_dir: None,
            tmpdir: None,
        };
        config.ensure = EnsureConfig {
            autostart: false,
            ..EnsureConfig::default()
        };
        let root = dir.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        let socket =
            prospero_core::discovery::resolve_socket(&root, &config.discovery_env).unwrap();
        let fake = FakeCaliband::start_at(&socket).await.unwrap();

        let store = Arc::new(JsonlStore::open(dir.path()).unwrap());
        let manager = FleetManager::new(config, store.clone()).await.unwrap();
        manager.add_repo("repo", &root).await.unwrap();

        let fleet: Arc<dyn FleetProvider> = Arc::new(LocalFleet::new(manager.clone()));
        (FleetMcp::new(fleet, store), manager, fake, dir)
    }

    /// The JSON a tool returned, or a panic naming the error the model saw.
    fn payload(result: CallToolResult) -> Value {
        assert!(
            !result.is_error.unwrap_or(false),
            "tool reported an error: {:?}",
            result.content
        );
        let raw = result
            .content
            .first()
            .and_then(|c| c.as_text().map(|t| t.text.clone()))
            .expect("a tool result carries content");
        serde_json::from_str(&raw).expect("tool content is JSON")
    }

    #[tokio::test]
    async fn listing_reports_the_workspaces_prospero_supervises() {
        let (mcp, _manager, _fake, _dir) = harness().await;
        let v = payload(mcp.prospero_list_workspaces().await.unwrap());
        let names: Vec<&str> = v["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .map(|w| w["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["repo"]);
    }

    /// The round trip that matters: a tool spawns an agent, and the fleet tools
    /// then see it — proving the adapter drives the same fleet the REST API does.
    #[tokio::test]
    async fn a_spawned_agent_is_visible_to_the_other_tools() {
        let (mcp, manager, _fake, _dir) = harness().await;

        let spawned = payload(
            mcp.prospero_spawn_agent(Parameters(SpawnArgs {
                workspace: "repo".into(),
                prompt: "do the thing".into(),
                label: Some("mcp-spawn".into()),
                model: None,
                interactive: false,
                timeout_secs: None,
            }))
            .await
            .unwrap(),
        );
        let id = spawned["agent_id"].as_str().expect("an id").to_string();
        assert_eq!(spawned["workspace"], "repo");

        // The fleet view is the poll snapshot, exactly as `/api/fleet` is: a
        // spawn is visible from the next poll, not the instant it returns.
        manager.poll_repo_once("repo").await;

        let listed = payload(mcp.prospero_list_agents().await.unwrap());
        assert!(
            listed["agents"]
                .as_array()
                .unwrap()
                .iter()
                .any(|a| a["agent_id"] == id.as_str()),
            "the spawned agent must appear in the fleet: {listed}"
        );

        let status = payload(
            mcp.prospero_agent_status(Parameters(AgentArgs {
                agent_id: id.clone(),
            }))
            .await
            .unwrap(),
        );
        assert_eq!(status["agent_id"], id.as_str());
    }

    /// An unknown id is a *tool* error the model can read and recover from, not
    /// a protocol failure that kills the session.
    #[tokio::test]
    async fn an_unknown_agent_is_a_readable_tool_error() {
        let (mcp, _manager, _fake, _dir) = harness().await;
        let result = mcp
            .prospero_agent_status(Parameters(AgentArgs {
                agent_id: "nope".into(),
            }))
            .await
            .unwrap();
        assert!(result.is_error.unwrap_or(false), "{result:?}");
        let text = result
            .content
            .first()
            .and_then(|c| c.as_text().map(|t| t.text.clone()))
            .unwrap_or_default();
        assert!(
            text.contains("nope"),
            "the message must name the id: {text}"
        );
    }

    /// History is capped, and says when it was — a model that could not tell a
    /// short page from the end of the log would stop reading early.
    #[tokio::test]
    async fn agent_events_are_bounded_and_say_so() {
        let (mcp, _manager, _fake, dir) = harness().await;
        let store = JsonlStore::open(dir.path()).unwrap();
        for seq in 1..=5u64 {
            store
                .append(&prospero_core::FleetEvent {
                    seq,
                    ts: "2026-09-20T00:00:00Z".into(),
                    repo: "repo".into(),
                    agent_id: "a1".into(),
                    kind: prospero_core::EventKind::Output {
                        stream: prospero_core::event::OutputStream::Stdout,
                        chunk: format!("chunk {seq}"),
                    },
                    actor: None,
                })
                .await
                .unwrap();
        }

        let page = payload(
            mcp.prospero_agent_events(Parameters(EventsArgs {
                agent_id: "a1".into(),
                from: 0,
                limit: Some(2),
            }))
            .await
            .unwrap(),
        );
        assert_eq!(page["events"].as_array().unwrap().len(), 2);
        assert_eq!(page["truncated"], true);
        assert_eq!(page["next_from"], 3, "resume after the last one delivered");

        let rest = payload(
            mcp.prospero_agent_events(Parameters(EventsArgs {
                agent_id: "a1".into(),
                from: 3,
                limit: None,
            }))
            .await
            .unwrap(),
        );
        assert_eq!(rest["events"].as_array().unwrap().len(), 3);
        assert_eq!(rest["truncated"], false);
    }
}
