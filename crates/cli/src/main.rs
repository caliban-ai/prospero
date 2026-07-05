//! `prospero` — the operator CLI for the Prospero control plane.
//!
//! Thin commands over `prosperod`'s HTTP API. Worktree isolation is the default
//! for spawns; `--shared-tree` opts out.

mod client;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use client::DaemonClient;

/// Prospero control-plane CLI.
#[derive(Debug, Parser)]
#[command(name = "prospero", version, about)]
struct Cli {
    /// Base URL of the prosperod daemon.
    #[arg(long, env = "PROSPERO_ADDR", default_value = "http://127.0.0.1:7878")]
    addr: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Manage the set of workspaces Prospero supervises.
    #[command(subcommand)]
    Workspace(WorkspaceCmd),
    /// Launch a new agent under a workspace (worktree-isolated by default).
    Spawn(SpawnArgs),
    /// List the fleet (all workspaces and their agents).
    Ls,
    /// Show daemon + fleet status.
    Status,
    /// Stream an agent's events live (replay history then tail).
    Follow(FollowArgs),
    /// Kill a running agent.
    Kill(AgentRef),
    /// Kill and respawn an agent with the same spec.
    Respawn(AgentRef),
    /// Remove an agent from caliban's registry.
    Rm(AgentRef),
    /// Send a user message to an interactive agent (resumes the run).
    Send(SendArgs),
    /// Signal end-of-input to an interactive agent (it finishes after).
    EndInput(AgentRef),
}

#[derive(Debug, Subcommand)]
enum WorkspaceCmd {
    /// Register a workspace by name and root path.
    Add {
        /// Short name (registry key).
        name: String,
        /// Workspace root path.
        root: String,
    },
    /// List managed workspaces.
    List,
    /// Set a workspace's provider config and restart its caliband.
    Config(WorkspaceConfigArgs),
    /// Unregister a workspace.
    Rm {
        /// Workspace name.
        name: String,
    },
}

#[derive(Debug, Args)]
struct WorkspaceConfigArgs {
    /// Workspace name (registry key).
    name: String,
    /// Provider id (e.g. anthropic, openai, google, ollama). Omit to clear.
    #[arg(long)]
    provider: Option<String>,
    /// Provider base URL / host.
    #[arg(long = "base-url", value_name = "URL")]
    base_url: Option<String>,
    /// NAME of an env var in prosperod's environment to inject as the provider's
    /// API key at spawn time (never the literal secret).
    #[arg(long = "api-key-env", value_name = "VAR")]
    api_key_env: Option<String>,
    /// Raw env override KEY=VALUE (repeatable; do not put secrets here).
    #[arg(long = "env", value_name = "KEY=VALUE", value_parser = parse_key_val)]
    env: Vec<(String, String)>,
}

/// Parse a `KEY=VALUE` pair (value may contain further `=`).
fn parse_key_val(s: &str) -> std::result::Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(format!("expected KEY=VALUE, got '{s}'")),
    }
}

#[derive(Debug, Args)]
struct SpawnArgs {
    /// Workspace to spawn the agent under.
    workspace: String,
    /// The prompt / task for the agent.
    prompt: String,
    /// Optional human-readable label.
    #[arg(long)]
    label: Option<String>,
    /// Optional model override.
    #[arg(long)]
    model: Option<String>,
    /// Run in the shared working tree instead of an isolated worktree.
    #[arg(long)]
    shared_tree: bool,
    /// Run the agent in interactive mode (it awaits your input instead of finishing).
    #[arg(long)]
    interactive: bool,
    /// Restrict the agent to these tools (repeat the flag per tool). Empty = no restriction.
    #[arg(long = "tool-allowlist", value_name = "TOOL")]
    tool_allowlist: Vec<String>,
}

#[derive(Debug, Args)]
struct FollowArgs {
    /// Agent id to follow.
    id: String,
    /// Start from this sequence number (0 = full history).
    #[arg(long, default_value_t = 0)]
    from: u64,
}

#[derive(Debug, Args)]
struct AgentRef {
    /// Agent id.
    id: String,
}

#[derive(Debug, Args)]
struct SendArgs {
    /// Agent id.
    id: String,
    /// Message text to inject.
    text: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = DaemonClient::new(&cli.addr);

    match cli.command {
        Command::Workspace(WorkspaceCmd::Add { name, root }) => {
            let body = serde_json::json!({ "name": name, "root": root });
            client.post_json("/api/workspaces", body)?;
            println!("registered workspace '{name}' at {root}");
        }
        Command::Workspace(WorkspaceCmd::List) => {
            let repos = client.get_json("/api/workspaces")?;
            print_workspaces(&repos);
        }
        Command::Workspace(WorkspaceCmd::Config(a)) => {
            let mut config = serde_json::Map::new();
            if let Some(provider) = &a.provider {
                config.insert("provider".into(), provider.clone().into());
            }
            if let Some(base_url) = &a.base_url {
                config.insert("base_url".into(), base_url.clone().into());
            }
            if let Some(api_key_env) = &a.api_key_env {
                config.insert("api_key_from_env".into(), api_key_env.clone().into());
            }
            if !a.env.is_empty() {
                let env: serde_json::Map<String, serde_json::Value> = a
                    .env
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone().into()))
                    .collect();
                config.insert("env".into(), serde_json::Value::Object(env));
            }
            client.put_json(
                &format!("/api/workspaces/{}/config", a.name),
                serde_json::Value::Object(config),
            )?;
            println!("updated provider config for workspace '{}'", a.name);
        }
        Command::Workspace(WorkspaceCmd::Rm { name }) => {
            client.delete(&format!("/api/workspaces/{name}"))?;
            println!("unregistered workspace '{name}'");
        }
        Command::Spawn(a) => {
            let mut body = serde_json::json!({ "prompt": a.prompt });
            if let Some(label) = a.label {
                body["label"] = label.into();
            }
            if let Some(model) = a.model {
                body["model"] = model.into();
            }
            body["isolation"] = if a.shared_tree { "shared" } else { "worktree" }.into();
            if a.interactive {
                body["interactive"] = true.into();
            }
            if !a.tool_allowlist.is_empty() {
                body["tool_allowlist"] = a.tool_allowlist.into();
            }
            let resp =
                client.post_json(&format!("/api/workspaces/{}/agents", a.workspace), body)?;
            let id = resp.get("agent_id").and_then(|v| v.as_str()).unwrap_or("?");
            let isolated = resp
                .get("isolated")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            println!(
                "spawned agent {id} in workspace '{}' ({})",
                a.workspace,
                if isolated { "worktree" } else { "shared tree" }
            );
        }
        Command::Ls => {
            let fleet = client.get_json("/api/fleet")?;
            print_fleet(&fleet);
        }
        Command::Status => {
            let healthy = client.get_json("/healthz").is_ok()
                || ureq::get(&format!("{}/healthz", cli.addr)).call().is_ok();
            println!("daemon: {}", if healthy { "up" } else { "unreachable" });
            if let Ok(fleet) = client.get_json("/api/fleet") {
                print_fleet(&fleet);
            }
        }
        Command::Follow(a) => {
            println!("— following {} (Ctrl-C to stop) —", a.id);
            client
                .stream_events(
                    &format!("/api/agents/{}/stream?from={}", a.id, a.from),
                    print_event,
                )
                .with_context(|| "streaming agent events")?;
        }
        Command::Kill(a) => {
            client.post_json(
                &format!("/api/agents/{}/kill", a.id),
                serde_json::Value::Null,
            )?;
            println!("kill requested for {}", a.id);
        }
        Command::Respawn(a) => {
            let resp = client.post_json(
                &format!("/api/agents/{}/respawn", a.id),
                serde_json::Value::Null,
            )?;
            let new_id = resp.get("agent_id").and_then(|v| v.as_str()).unwrap_or("?");
            println!("respawned {} as {}", a.id, new_id);
        }
        Command::Rm(a) => {
            client.delete(&format!("/api/agents/{}", a.id))?;
            println!("removed {}", a.id);
        }
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
    }
    Ok(())
}

fn print_workspaces(workspaces: &serde_json::Value) {
    let Some(arr) = workspaces.as_array() else {
        return;
    };
    if arr.is_empty() {
        println!("(no workspaces registered)");
        return;
    }
    for r in arr {
        let name = r["name"].as_str().unwrap_or("?");
        let root = r["root"].as_str().unwrap_or("?");
        let health = r["health"]["state"].as_str().unwrap_or("?");
        let count = r["agent_count"].as_u64().unwrap_or(0);
        let provider = r["config"]["provider"]
            .as_str()
            .map(|p| format!("  {p}"))
            .unwrap_or_default();
        println!("{name:<16} {health:<12} {count} agents   {root}{provider}");
        let sources: Vec<&str> = r["sources"]
            .as_array()
            .map(|a| a.iter().filter_map(|s| s["name"].as_str()).collect())
            .unwrap_or_default();
        if !sources.is_empty() {
            println!("{:<16} sources: {}", "", sources.join(", "));
        }
    }
}

fn print_fleet(fleet: &serde_json::Value) {
    let host = fleet["host"].as_str().unwrap_or("?");
    println!("host: {host}");
    let Some(repos) = fleet["workspaces"].as_array() else {
        return;
    };
    if repos.is_empty() {
        println!("(no workspaces registered)");
        return;
    }
    for repo in repos {
        let name = repo["name"].as_str().unwrap_or("?");
        let health = repo["health"]["state"].as_str().unwrap_or("?");
        let provider = repo["config"]["provider"]
            .as_str()
            .map(|p| format!("  provider={p}"))
            .unwrap_or_default();
        println!("\n{name}  [{health}]{provider}");
        let agents = repo["agents"].as_array().cloned().unwrap_or_default();
        if agents.is_empty() {
            println!("  (no agents)");
        }
        for a in &agents {
            let id = a["id"].as_str().unwrap_or("?");
            let status = a["status"].as_str().unwrap_or("?");
            let wt = if a["isolated"].as_bool().unwrap_or(false) {
                "worktree"
            } else {
                "shared"
            };
            let label = a["name"].as_str().unwrap_or("");
            println!("  {id:<14} {status:<9} {wt:<9} {label}");
        }
    }
}

fn print_event(event_name: &str, ev: serde_json::Value) {
    // Named control events carry no `FleetEvent` payload; handle them first.
    if event_name == "gap" {
        println!(
            "[gap] fell behind — recovered {} dropped event(s) from history",
            ev["skipped"].as_u64().unwrap_or(0)
        );
        return;
    }
    let kind = &ev["kind"];
    match kind["kind"].as_str().unwrap_or("") {
        "output" => print!("{}", kind["chunk"].as_str().unwrap_or("")),
        "tool_started" => println!("⚙ {}", kind["name"].as_str().unwrap_or("?")),
        "tool_finished" => {
            let ok = kind["ok"].as_bool().unwrap_or(false);
            println!(
                "{} {}",
                if ok { "✓" } else { "✗" },
                kind["name"].as_str().unwrap_or("?")
            );
        }
        "agent_init" => println!("[init] model={}", kind["model"].as_str().unwrap_or("?")),
        "agent_finished" => println!(
            "[finished] {} — ${:.4}, {} turns",
            kind["outcome"].as_str().unwrap_or("?"),
            kind["cost_usd"].as_f64().unwrap_or(0.0),
            kind["turns"].as_u64().unwrap_or(0)
        ),
        "status_changed" => println!(
            "[status] {} → {}",
            kind["from"].as_str().unwrap_or("?"),
            kind["to"].as_str().unwrap_or("?")
        ),
        "store_persist_failed" => println!(
            "⚠ [persist-gap] event seq {} was not durably stored: {}",
            kind["lost_seq"].as_u64().unwrap_or(0),
            kind["detail"].as_str().unwrap_or("")
        ),
        other => println!("[{other}]"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn spawn_defaults_to_worktree_shared_tree_off() {
        let cli = Cli::parse_from(["prospero", "spawn", "myrepo", "do the thing"]);
        match cli.command {
            Command::Spawn(a) => {
                assert_eq!(a.workspace, "myrepo");
                assert_eq!(a.prompt, "do the thing");
                assert!(
                    !a.shared_tree,
                    "shared_tree must default to false (worktree on)"
                );
            }
            other => panic!("expected spawn, got {other:?}"),
        }
    }

    #[test]
    fn repo_config_parses_all_flags() {
        let cli = Cli::parse_from([
            "prospero",
            "repo",
            "config",
            "myrepo",
            "--provider",
            "ollama",
            "--base-url",
            "http://h:11434",
            "--api-key-env",
            "OLLAMA_KEY",
            "--env",
            "FOO=bar",
        ]);
        match cli.command {
            Command::Workspace(WorkspaceCmd::Config(a)) => {
                assert_eq!(a.name, "myrepo");
                assert_eq!(a.provider.as_deref(), Some("ollama"));
                assert_eq!(a.base_url.as_deref(), Some("http://h:11434"));
                assert_eq!(a.api_key_env.as_deref(), Some("OLLAMA_KEY"));
                assert_eq!(a.env, vec![("FOO".to_string(), "bar".to_string())]);
            }
            other => panic!("expected repo config, got {other:?}"),
        }
    }

    #[test]
    fn repo_config_rejects_bad_env_pair() {
        let res = Cli::try_parse_from(["prospero", "repo", "config", "r", "--env", "noequals"]);
        assert!(res.is_err(), "KEY=VALUE without '=' must be rejected");
    }

    #[test]
    fn spawn_shared_tree_flag_parses() {
        let cli = Cli::parse_from(["prospero", "spawn", "r", "p", "--shared-tree"]);
        match cli.command {
            Command::Spawn(a) => assert!(a.shared_tree),
            other => panic!("expected spawn, got {other:?}"),
        }
    }

    #[test]
    fn spawn_interactive_flag_parses() {
        let cli = Cli::parse_from(["prospero", "spawn", "r", "p", "--interactive"]);
        match cli.command {
            Command::Spawn(a) => assert!(a.interactive),
            other => panic!("expected spawn, got {other:?}"),
        }
    }

    #[test]
    fn spawn_tool_allowlist_defaults_empty() {
        let cli = Cli::parse_from(["prospero", "spawn", "r", "p"]);
        match cli.command {
            Command::Spawn(a) => assert!(
                a.tool_allowlist.is_empty(),
                "tool_allowlist must default to empty (no restriction)"
            ),
            other => panic!("expected spawn, got {other:?}"),
        }
    }

    #[test]
    fn spawn_tool_allowlist_repeatable_parses() {
        let cli = Cli::parse_from([
            "prospero",
            "spawn",
            "r",
            "p",
            "--tool-allowlist",
            "read",
            "--tool-allowlist",
            "edit",
        ]);
        match cli.command {
            Command::Spawn(a) => assert_eq!(a.tool_allowlist, vec!["read", "edit"]),
            other => panic!("expected spawn, got {other:?}"),
        }
    }

    #[test]
    fn repo_add_parses_name_and_root() {
        let cli = Cli::parse_from(["prospero", "repo", "add", "p", "/dev/p"]);
        match cli.command {
            Command::Workspace(WorkspaceCmd::Add { name, root }) => {
                assert_eq!(name, "p");
                assert_eq!(root, "/dev/p");
            }
            other => panic!("expected repo add, got {other:?}"),
        }
    }

    #[test]
    fn addr_defaults_to_localhost_daemon() {
        let cli = Cli::parse_from(["prospero", "ls"]);
        assert_eq!(cli.addr, "http://127.0.0.1:7878");
    }

    #[test]
    fn follow_from_defaults_to_zero() {
        let cli = Cli::parse_from(["prospero", "follow", "agent001"]);
        match cli.command {
            Command::Follow(a) => {
                assert_eq!(a.id, "agent001");
                assert_eq!(a.from, 0);
            }
            other => panic!("expected follow, got {other:?}"),
        }
    }

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
}
