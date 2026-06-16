//! End-to-end smoke test: boot a real HTTP server (router + FleetManager backed
//! by a `FakeCaliband`) on an ephemeral port, then drive the **real `prospero`
//! binary** against it over HTTP. Proves the whole vertical slice wires together
//! without a real caliban or any LLM.

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use prospero_core::discovery::{DiscoveryEnv, EnsureConfig, control_socket_path};
use prospero_core::fleet::{FleetConfig, FleetManager};
use prospero_core::store::JsonlStore;
use prospero_core::testkit::FakeCaliband;

/// Path to the compiled `prospero` binary under test.
const PROSPERO_BIN: &str = env!("CARGO_BIN_EXE_prospero");

fn run_cli(base: &str, args: &[&str]) -> (bool, String) {
    let output = Command::new(PROSPERO_BIN)
        .arg("--addr")
        .arg(base)
        .args(args)
        .output()
        .expect("running prospero binary");
    let mut combined = String::from_utf8_lossy(&output.stdout).to_string();
    combined.push_str(&String::from_utf8_lossy(&output.stderr));
    (output.status.success(), combined)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_drives_the_full_stack() {
    // --- temp dirs (kept alive for the whole test) ---
    let repo_dir = tempfile::tempdir().unwrap();
    let runtime_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let repo_root = repo_dir.path().canonicalize().unwrap();

    // --- fake caliband at the discovery-derived socket ---
    let env = DiscoveryEnv {
        caliban_daemon_runtime_dir: Some(runtime_dir.path().to_path_buf()),
        xdg_runtime_dir: None,
        tmpdir: None,
    };
    let socket = control_socket_path(&repo_root, &env);
    let fake = FakeCaliband::start_at(&socket).await.unwrap();

    // --- manager + background poll loop ---
    let mut config = FleetConfig::new("e2e-host", data_dir.path());
    config.discovery_env = env;
    config.ensure = EnsureConfig {
        autostart: false,
        ..EnsureConfig::default()
    };
    config.poll_interval = Duration::from_millis(100);
    let store = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let manager = FleetManager::new(config, store).unwrap();
    manager.add_repo("repo", repo_root).await.unwrap();
    tokio::spawn(manager.clone().run());

    // --- serve the API on an ephemeral port ---
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    let app = prospero_api::router(manager);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Wait for the server to accept connections.
    wait_for_health(&base).await;

    // --- drive the real CLI binary ---
    let (ok, out) = run_cli(&base, &["ls"]);
    assert!(ok, "ls failed: {out}");
    assert!(out.contains("repo"), "ls output missing repo: {out}");

    let (ok, out) = run_cli(&base, &["spawn", "repo", "do the thing"]);
    assert!(ok, "spawn failed: {out}");
    assert!(out.contains("spawned agent"), "spawn output: {out}");
    assert!(
        out.contains("worktree"),
        "spawn should default to worktree: {out}"
    );

    // Extract the new agent id ("spawned agent <id> in repo ...").
    let agent_id = out
        .split_whitespace()
        .skip_while(|w| *w != "agent")
        .nth(1)
        .expect("agent id in spawn output")
        .to_string();

    // Give the poll loop a moment to surface the agent, then list the fleet.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (ok, out) = run_cli(&base, &["ls"]);
    assert!(ok, "second ls failed: {out}");
    assert!(
        out.contains(&agent_id),
        "fleet should list the agent: {out}"
    );

    // Follow streams replayed history then closes when the agent's stream ends.
    let (ok, out) = run_cli(&base, &["follow", &agent_id, "--from", "0"]);
    assert!(ok, "follow failed: {out}");
    assert!(
        out.contains("finished") || out.contains("init"),
        "follow should show streamed events: {out}"
    );

    // --tool-allowlist reaches caliband as the spawned spec's allowlist.
    let (ok, out) = run_cli(
        &base,
        &[
            "spawn",
            "repo",
            "restricted task",
            "--tool-allowlist",
            "read",
            "--tool-allowlist",
            "edit",
        ],
    );
    assert!(ok, "allowlisted spawn failed: {out}");
    let allowlisted = fake
        .received_specs()
        .into_iter()
        .find(|s| s.initial_prompt == "restricted task")
        .expect("fake caliband received the allowlisted spawn spec");
    assert_eq!(
        allowlisted.tool_allowlist,
        Some(vec!["read".to_string(), "edit".to_string()]),
        "allowlist must reach caliband; got {:?}",
        allowlisted.tool_allowlist
    );

    // `repo config` sets the per-repo provider end-to-end (kept last: it restarts
    // caliband). Verify the daemon persisted it via /api/repos.
    let (ok, out) = run_cli(
        &base,
        &[
            "repo",
            "config",
            "repo",
            "--provider",
            "ollama",
            "--base-url",
            "http://h:11434",
        ],
    );
    assert!(ok, "repo config failed: {out}");
    assert!(
        out.contains("updated provider config"),
        "config output: {out}"
    );

    let repos_url = format!("{base}/api/repos");
    let repos: serde_json::Value =
        tokio::task::spawn_blocking(move || ureq::get(&repos_url).call().unwrap().into_json())
            .await
            .unwrap()
            .unwrap();
    let cfg = &repos.as_array().unwrap()[0]["config"];
    assert_eq!(
        cfg["provider"].as_str(),
        Some("ollama"),
        "repo config must persist the provider end-to-end: {repos}"
    );
    assert_eq!(cfg["base_url"].as_str(), Some("http://h:11434"));
}

async fn wait_for_health(base: &str) {
    let url = format!("{base}/healthz");
    for _ in 0..100 {
        let url2 = url.clone();
        let ok = tokio::task::spawn_blocking(move || ureq::get(&url2).call().is_ok())
            .await
            .unwrap();
        if ok {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not become healthy at {url}");
}
