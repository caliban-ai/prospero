//! `prospero` — the operator CLI for the Prospero control plane.
//!
//! Thin commands over `prosperod`'s HTTP API. Worktree isolation is the default
//! for spawns; `--shared-tree` opts out.

mod client;
mod usage;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use client::DaemonClient;
use prospero_core::model::PermissionPosture;

/// Prospero control-plane CLI.
#[derive(Debug, Parser)]
#[command(name = "prospero", version, about)]
struct Cli {
    /// Base URL of the prosperod daemon.
    #[arg(long, env = "PROSPERO_ADDR", default_value = "http://127.0.0.1:7878")]
    addr: String,

    /// API token for an authenticated prosperod.
    #[arg(long, env = "PROSPERO_TOKEN", hide_env_values = true, global = true)]
    token: Option<String>,

    /// File containing the API token (trailing whitespace trimmed).
    #[arg(long, env = "PROSPERO_TOKEN_FILE", global = true)]
    token_file: Option<std::path::PathBuf>,

    /// Name the person this command is being run for, for a wrapper that
    /// serves several people over one token (#251). Recorded beside the token
    /// on the events the request emits. **Asserted, not verified** —
    /// prosperod cannot authenticate someone else's user.
    #[arg(long = "on-behalf-of", env = "PROSPERO_ON_BEHALF_OF", global = true)]
    on_behalf_of: Option<String>,

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
    /// Manage automations: scheduled and webhook-triggered spawns.
    #[command(subcommand)]
    Automation(AutomationCmd),
    /// Cost, turns and outcomes per workspace over a window.
    Usage(UsageArgs),
    /// Manage API tokens (offline).
    #[command(subcommand)]
    Token(TokenCmd),
    /// Show which token this CLI is using and its scope.
    Whoami,
}

#[derive(Debug, Subcommand)]
enum AutomationCmd {
    /// Create an automation. Exactly one of --schedule or --webhook.
    Add(AutomationAddArgs),
    /// List configured automations.
    Ls,
    /// Fire one now, whatever its trigger.
    Run {
        /// Automation id.
        id: String,
    },
    /// Show an automation's recent runs, newest first.
    Runs {
        /// Automation id.
        id: String,
        /// How many runs to show.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Stop an automation firing, without deleting it.
    Disable {
        /// Automation id.
        id: String,
    },
    /// Let a disabled automation fire again.
    Enable {
        /// Automation id.
        id: String,
    },
    /// Delete an automation and its run history.
    Rm {
        /// Automation id.
        id: String,
    },
}

#[derive(Debug, Args)]
struct AutomationAddArgs {
    /// Automation id (unique).
    id: String,
    /// Workspace to spawn into.
    workspace: String,
    /// The agent's task. With --webhook it may contain `{{ dotted.path }}`
    /// placeholders filled from the request payload.
    task: String,
    /// Cron schedule, e.g. "0 3 * * *" (5-field, UTC).
    #[arg(long, conflicts_with = "webhook")]
    schedule: Option<String>,
    /// Trigger by signed webhook instead of a schedule.
    #[arg(long, conflicts_with = "schedule")]
    webhook: bool,
    /// Optional label for spawned agents.
    #[arg(long)]
    label: Option<String>,
    /// Optional model override.
    #[arg(long)]
    model: Option<String>,
    /// Kill each spawned agent after this many seconds.
    #[arg(long)]
    timeout_secs: Option<u64>,
    /// Run in the shared checkout instead of an isolated worktree.
    #[arg(long)]
    shared_tree: bool,
    /// Create it disabled.
    #[arg(long)]
    disabled: bool,
}

#[derive(Debug, Subcommand)]
enum TokenCmd {
    /// Generate a new API token and its tokens-file line (does not contact prosperod).
    New {
        /// Token name (recorded as the actor on events).
        name: String,
        /// Scope: read, operate or admin.
        #[arg(long, value_parser = parse_scope)]
        scope: prospero_core::Scope,
    },
}

fn parse_scope(s: &str) -> std::result::Result<prospero_core::Scope, String> {
    prospero_core::Scope::parse(s)
        .ok_or_else(|| format!("unknown scope '{s}' (read|operate|admin)"))
}

/// The token to send: `--token-file` wins over `--token`; blank values mean none.
fn resolve_token(
    token: Option<String>,
    token_file: Option<&std::path::Path>,
) -> Result<Option<String>> {
    if let Some(path) = token_file {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading token file {}", path.display()))?;
        let t = raw.trim();
        if t.is_empty() {
            anyhow::bail!("token file {} is empty", path.display());
        }
        return Ok(Some(t.to_string()));
    }
    Ok(token
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty()))
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
    /// Provider id (e.g. anthropic, openai, google). Omit to clear.
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
    /// Path to an agent-template / frontmatter markdown file for the agent.
    #[arg(long = "frontmatter", value_name = "PATH")]
    frontmatter: Option<String>,
    /// Kill the agent after this many seconds of wall-clock time. Omitted, it
    /// runs until it finishes or you stop it.
    #[arg(long = "timeout", value_name = "SECONDS")]
    timeout_secs: Option<u64>,
    /// How tool calls needing permission are handled: `supervised` (a human
    /// approves them; the default) or `unattended` (no permission gate — needs
    /// an admin-scope token, and in-cluster a workspace that allows it).
    #[arg(
        long = "permission-posture",
        value_name = "POSTURE",
        default_value = "supervised",
        value_parser = parse_permission_posture
    )]
    permission_posture: PermissionPosture,
}

/// Parse the `--permission-posture` value. Hand-written rather than clap's
/// `ValueEnum` because the enum lives in `prospero-types`, which carries no
/// clap dependency; the accepted strings are the wire values.
fn parse_permission_posture(s: &str) -> Result<PermissionPosture, String> {
    match s {
        "supervised" => Ok(PermissionPosture::Supervised),
        "unattended" => Ok(PermissionPosture::Unattended),
        other => Err(format!(
            "unknown posture '{other}' (expected 'supervised' or 'unattended')"
        )),
    }
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
struct UsageArgs {
    /// How far back to look: a day count (`7d`, `2w`), a date (`2026-09-01`,
    /// from UTC midnight) or an RFC-3339 timestamp. Defaults to the server's
    /// window (the last 7 days).
    #[arg(long)]
    since: Option<String>,
    /// Only this workspace.
    #[arg(long)]
    workspace: Option<String>,
    /// Print the server's report as JSON instead of a table.
    #[arg(long)]
    json: bool,
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

    // `token new` is fully offline and needs no credential at all, so it must run
    // before --token/--token-file are resolved: a stale or unreadable
    // PROSPERO_TOKEN_FILE must never block it (#2 review finding, round 1).
    if let Command::Token(TokenCmd::New { name, scope }) = &cli.command {
        prospero_core::auth::validate_token_name(name).map_err(anyhow::Error::msg)?;
        let token = prospero_core::auth::generate_token();
        // These two output prefixes are matched exactly by the e2e test in
        // tests/e2e_smoke.rs — keep them stable.
        println!("token (shown once): {token}");
        println!(
            "tokens-file line:  {}",
            prospero_core::auth::tokens_file_line(name, *scope, &token)
        );
        return Ok(());
    }

    let token = resolve_token(cli.token.clone(), cli.token_file.as_deref())?;
    let client = DaemonClient::new(&cli.addr, token).on_behalf_of(cli.on_behalf_of.clone());

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
        Command::Automation(AutomationCmd::Add(a)) => {
            let trigger = match (&a.schedule, a.webhook) {
                (Some(schedule), false) => {
                    serde_json::json!({ "kind": "cron", "schedule": schedule })
                }
                (None, true) => serde_json::json!({ "kind": "webhook" }),
                _ => anyhow::bail!("pass exactly one of --schedule <CRON> or --webhook"),
            };
            let mut template = serde_json::json!({
                "task": a.task,
                "isolation_worktree": !a.shared_tree,
            });
            if let Some(label) = &a.label {
                template["label"] = label.clone().into();
            }
            if let Some(model) = &a.model {
                template["model"] = model.clone().into();
            }
            if let Some(secs) = a.timeout_secs {
                template["timeout_secs"] = secs.into();
            }
            let body = serde_json::json!({
                "id": a.id,
                "workspace": a.workspace,
                "trigger": trigger,
                "template": template,
                "enabled": !a.disabled,
            });
            let created = client.post_json("/api/automations", body)?;
            println!("created automation '{}'", a.id);
            if let Some(secret) = created["webhook_secret"].as_str() {
                println!();
                println!("  webhook URL:  POST /api/automations/{}/trigger", a.id);
                println!("  signing key:  {secret}");
                println!();
                println!("Sign the raw request body with HMAC-SHA256 and send it as");
                println!("  X-Prospero-Signature: sha256=<hex>");
                println!("This key is shown once and is not recoverable — store it now.");
            }
        }
        Command::Automation(AutomationCmd::Ls) => {
            let list = client.get_json("/api/automations")?;
            print_automations(&list);
        }
        Command::Automation(AutomationCmd::Run { id }) => {
            let fired =
                client.post_json(&format!("/api/automations/{id}/run"), serde_json::json!({}))?;
            print_run(&fired["run"]);
        }
        Command::Automation(AutomationCmd::Runs { id, limit }) => {
            let runs = client.get_json(&format!("/api/automations/{id}/runs?limit={limit}"))?;
            match runs.as_array() {
                Some(rows) if rows.is_empty() => println!("no runs recorded for '{id}'"),
                Some(rows) => rows.iter().for_each(print_run),
                None => println!("no runs recorded for '{id}'"),
            }
        }
        Command::Automation(AutomationCmd::Disable { id }) => {
            client.put_json(
                &format!("/api/automations/{id}/enabled"),
                serde_json::json!({ "enabled": false }),
            )?;
            println!("disabled automation '{id}'");
        }
        Command::Automation(AutomationCmd::Enable { id }) => {
            client.put_json(
                &format!("/api/automations/{id}/enabled"),
                serde_json::json!({ "enabled": true }),
            )?;
            println!("enabled automation '{id}'");
        }
        Command::Automation(AutomationCmd::Rm { id }) => {
            client.delete(&format!("/api/automations/{id}"))?;
            println!("removed automation '{id}'");
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
            if let Some(frontmatter) = a.frontmatter {
                body["frontmatter_path"] = frontmatter.into();
            }
            if let Some(secs) = a.timeout_secs {
                body["timeout_secs"] = secs.into();
            }
            if a.permission_posture != PermissionPosture::Supervised {
                body["permission_posture"] = serde_json::to_value(a.permission_posture)?;
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
        Command::Usage(a) => {
            let path = usage::usage_path(a.since.as_deref())?;
            let mut report = client.get_json(&path)?;
            if let Some(w) = &a.workspace {
                report = usage::only_workspace(report, w);
            }
            if a.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                let typed: prospero_types::UsageReport = serde_json::from_value(report)
                    .context("the server's usage report did not match this CLI's version")?;
                print!("{}", usage::render(&typed, a.workspace.as_deref()));
            }
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
        // Handled above, before --token/--token-file resolution, so it stays fully
        // offline regardless of a stale or unreadable token file.
        Command::Token(_) => unreachable!("token new is handled before client construction"),
        Command::Whoami => {
            let v = client.get_json("/api/session")?;
            if v["auth"].as_str() == Some("disabled") {
                println!("authentication is disabled on this prosperod");
            } else {
                println!(
                    "{} ({})",
                    v["token_name"].as_str().unwrap_or("?"),
                    v["scope"].as_str().unwrap_or("?")
                );
            }
        }
    }
    Ok(())
}

/// One line per automation: id, what fires it, where, and its last run.
fn print_automations(list: &serde_json::Value) {
    let Some(arr) = list.as_array() else {
        return;
    };
    if arr.is_empty() {
        println!("(no automations configured)");
        return;
    }
    for a in arr {
        let id = a["id"].as_str().unwrap_or("?");
        let workspace = a["workspace"].as_str().unwrap_or("?");
        let trigger = match a["trigger"]["kind"].as_str() {
            Some("cron") => a["trigger"]["schedule"].as_str().unwrap_or("?").to_string(),
            Some("webhook") => "webhook".to_string(),
            _ => "?".to_string(),
        };
        // Disabled is the exception worth calling out; enabled is the norm.
        let state = if a["enabled"].as_bool().unwrap_or(true) {
            ""
        } else {
            "  (disabled)"
        };
        let last = a["last_fired_at"]
            .as_str()
            .map(|t| format!("  last {t}"))
            .unwrap_or_else(|| "  never fired".to_string());
        println!("{id:<20} {trigger:<16} {workspace}{last}{state}");
    }
}

/// One line for a recorded run, leading with whether it worked.
fn print_run(run: &serde_json::Value) {
    let at = run["fired_at"].as_str().unwrap_or("?");
    let source = run["source"].as_str().unwrap_or("?");
    match (run["agent_id"].as_str(), run["error"].as_str()) {
        (Some(agent), _) => println!("{at}  {source:<9} ok      {agent}"),
        (None, Some(err)) => println!("{at}  {source:<9} FAILED  {err}"),
        (None, None) => println!("{at}  {source:<9} ?"),
    }
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

    /// #220: --schedule and --webhook are alternatives, and clap must enforce
    /// that rather than the daemon discovering a trigger-less automation.
    #[test]
    fn an_automation_takes_exactly_one_trigger() {
        let cli = Cli::parse_from([
            "prospero",
            "automation",
            "add",
            "nightly",
            "myrepo",
            "sweep the logs",
            "--schedule",
            "0 3 * * *",
        ]);
        match cli.command {
            Command::Automation(AutomationCmd::Add(a)) => {
                assert_eq!(a.id, "nightly");
                assert_eq!(a.workspace, "myrepo");
                assert_eq!(a.task, "sweep the logs");
                assert_eq!(a.schedule.as_deref(), Some("0 3 * * *"));
                assert!(!a.webhook);
                assert!(!a.shared_tree, "worktree isolation stays the default");
                assert!(!a.disabled);
            }
            other => panic!("expected automation add, got {other:?}"),
        }

        assert!(
            Cli::try_parse_from([
                "prospero",
                "automation",
                "add",
                "x",
                "r",
                "t",
                "--schedule",
                "0 3 * * *",
                "--webhook",
            ])
            .is_err(),
            "--schedule and --webhook must conflict"
        );
    }

    #[test]
    fn usage_defaults_to_the_servers_window_every_workspace_and_a_table() {
        let cli = Cli::parse_from(["prospero", "usage"]);
        match cli.command {
            Command::Usage(a) => {
                assert_eq!(a.since, None);
                assert_eq!(a.workspace, None);
                assert!(!a.json);
            }
            other => panic!("expected usage, got {other:?}"),
        }

        let cli = Cli::parse_from([
            "prospero",
            "usage",
            "--since",
            "2w",
            "--workspace",
            "repo",
            "--json",
        ]);
        match cli.command {
            Command::Usage(a) => {
                assert_eq!(a.since.as_deref(), Some("2w"));
                assert_eq!(a.workspace.as_deref(), Some("repo"));
                assert!(a.json);
            }
            other => panic!("expected usage, got {other:?}"),
        }
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
    fn spawn_frontmatter_flag_parses() {
        let cli = Cli::parse_from([
            "prospero",
            "spawn",
            "myrepo",
            "do it",
            "--frontmatter",
            "/tpl.md",
        ]);
        match cli.command {
            Command::Spawn(a) => assert_eq!(a.frontmatter.as_deref(), Some("/tpl.md")),
            other => panic!("expected spawn, got {other:?}"),
        }
    }

    #[test]
    fn repo_config_parses_all_flags() {
        let cli = Cli::parse_from([
            "prospero",
            "workspace",
            "config",
            "myrepo",
            "--provider",
            "openai",
            "--base-url",
            "http://h:9292/v1",
            "--api-key-env",
            "OPENAI_API_KEY",
            "--env",
            "FOO=bar",
        ]);
        match cli.command {
            Command::Workspace(WorkspaceCmd::Config(a)) => {
                assert_eq!(a.name, "myrepo");
                assert_eq!(a.provider.as_deref(), Some("openai"));
                assert_eq!(a.base_url.as_deref(), Some("http://h:9292/v1"));
                assert_eq!(a.api_key_env.as_deref(), Some("OPENAI_API_KEY"));
                assert_eq!(a.env, vec![("FOO".to_string(), "bar".to_string())]);
            }
            other => panic!("expected repo config, got {other:?}"),
        }
    }

    #[test]
    fn repo_config_rejects_bad_env_pair() {
        let res =
            Cli::try_parse_from(["prospero", "workspace", "config", "r", "--env", "noequals"]);
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

    /// #221: a wall-clock cap is opt-in and expressed in seconds.
    #[test]
    fn spawn_timeout_parses_and_defaults_to_none() {
        let cli = Cli::parse_from(["prospero", "spawn", "r", "p"]);
        match cli.command {
            Command::Spawn(a) => assert_eq!(a.timeout_secs, None),
            other => panic!("expected spawn, got {other:?}"),
        }

        let cli = Cli::parse_from(["prospero", "spawn", "r", "p", "--timeout", "900"]);
        match cli.command {
            Command::Spawn(a) => assert_eq!(a.timeout_secs, Some(900)),
            other => panic!("expected spawn, got {other:?}"),
        }

        assert!(
            Cli::try_parse_from(["prospero", "spawn", "r", "p", "--timeout", "soon"]).is_err(),
            "a non-numeric timeout must be rejected at parse time"
        );
    }

    /// #238: the posture is a typed choice, and omitting it means supervised —
    /// a typo must fail at parse time rather than silently spawn supervised.
    #[test]
    fn spawn_permission_posture_parses_and_defaults_to_supervised() {
        let cli = Cli::parse_from(["prospero", "spawn", "r", "p"]);
        match cli.command {
            Command::Spawn(a) => assert_eq!(a.permission_posture, PermissionPosture::Supervised),
            other => panic!("expected spawn, got {other:?}"),
        }

        let cli = Cli::parse_from([
            "prospero",
            "spawn",
            "r",
            "p",
            "--permission-posture",
            "unattended",
        ]);
        match cli.command {
            Command::Spawn(a) => assert_eq!(a.permission_posture, PermissionPosture::Unattended),
            other => panic!("expected spawn, got {other:?}"),
        }

        assert!(
            Cli::try_parse_from([
                "prospero",
                "spawn",
                "r",
                "p",
                "--permission-posture",
                "yolo"
            ])
            .is_err(),
            "an unknown posture must be rejected, not defaulted"
        );
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
        let cli = Cli::parse_from(["prospero", "workspace", "add", "p", "/dev/p"]);
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

    #[test]
    fn token_new_parses_scope_and_rejects_unknown() {
        let cli = Cli::parse_from(["prospero", "token", "new", "ci", "--scope", "operate"]);
        match cli.command {
            Command::Token(TokenCmd::New { name, scope }) => {
                assert_eq!(name, "ci");
                assert_eq!(scope, prospero_core::Scope::Operate);
            }
            other => panic!("expected token new, got {other:?}"),
        }
        assert!(
            Cli::try_parse_from(["prospero", "token", "new", "ci", "--scope", "root"]).is_err()
        );
    }

    #[test]
    fn token_flag_is_global() {
        let cli = Cli::parse_from(["prospero", "ls", "--token", "pspo_x"]);
        assert_eq!(cli.token.as_deref(), Some("pspo_x"));
    }

    #[test]
    fn resolve_token_prefers_file_and_rejects_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("t");
        std::fs::write(&f, "pspo_from_file\n").unwrap();
        assert_eq!(
            resolve_token(Some("pspo_flag".into()), Some(&f))
                .unwrap()
                .as_deref(),
            Some("pspo_from_file")
        );
        std::fs::write(&f, "  \n").unwrap();
        assert!(resolve_token(None, Some(&f)).is_err());
        assert_eq!(resolve_token(Some("  ".into()), None).unwrap(), None);
    }
}
