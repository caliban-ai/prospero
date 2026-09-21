//! End-to-end smoke test: boot a real HTTP server (router + FleetManager backed
//! by a `FakeCaliband`) on an ephemeral port, then drive the **real `prospero`
//! binary** against it over HTTP. Proves the whole vertical slice wires together
//! without a real caliban or any LLM.

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use prospero_core::LocalFleet;
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
    let manager = FleetManager::new(config, store).await.unwrap();
    manager.add_repo("repo", repo_root).await.unwrap();
    tokio::spawn(manager.clone().run());

    // --- serve the API on an ephemeral port ---
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    let local = LocalFleet::new(manager.clone());
    let app = prospero_api::router(
        Arc::new(local.clone()),
        Some(Arc::new(local)),
        manager.store(),
        manager.bus(),
    );
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

    // The background poll loop surfaces the agent asynchronously, so poll `ls`
    // to a deadline instead of guessing how long a cycle takes — a fixed sleep
    // passes locally and flakes on loaded CI runners. (#103)
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let (ok, out) = run_cli(&base, &["ls"]);
        assert!(ok, "second ls failed: {out}");
        if out.contains(&agent_id) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "fleet should list the agent {agent_id}: {out}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Follow streams replayed history then closes when the agent's stream ends.
    let (ok, out) = run_cli(&base, &["follow", &agent_id, "--from", "0"]);
    assert!(ok, "follow failed: {out}");
    assert!(
        out.contains("finished") || out.contains("init"),
        "follow should show streamed events: {out}"
    );

    // --- `prospero usage` (#223) ---
    //
    // Seed one known finish into the real store so the numbers are exact
    // rather than whatever the fake happens to report. `follow` above only
    // returns once the spawned agent's stream has ended, so nothing else is
    // writing usage while these assertions run.
    let seeded = |seq, kind| prospero_core::FleetEvent {
        seq,
        ts: chrono::Utc::now().to_rfc3339(),
        repo: "repo".into(),
        agent_id: "usage-seed".into(),
        kind,
        actor: None,
        on_behalf_of: None,
    };
    let store = manager.store();
    store
        .append(&seeded(
            1,
            prospero_core::EventKind::AgentFinished {
                outcome: "EndOfTurn".into(),
                cost_usd: 0.5,
                turns: 3,
            },
        ))
        .await
        .unwrap();
    store
        .append(&seeded(
            2,
            prospero_core::EventKind::StatusChanged {
                from: prospero_core::AgentStatus::Running,
                to: prospero_core::AgentStatus::Done,
            },
        ))
        .await
        .unwrap();

    // `--json` is the server's own report for the same window. An explicit
    // `--since` pins the start; `until` defaults to "now" on each request, so
    // it is the one field that legitimately differs between the two reads.
    let (ok, out) = run_cli(&base, &["usage", "--since", "2000-01-01", "--json"]);
    assert!(ok, "usage --json failed: {out}");
    let from_cli: serde_json::Value = serde_json::from_str(&out).expect("usage --json is JSON");
    let direct_url = format!("{base}/api/usage?since=2000-01-01T00:00:00%2B00:00");
    let direct: serde_json::Value =
        tokio::task::spawn_blocking(move || ureq::get(&direct_url).call().unwrap().into_json())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(from_cli["since"], direct["since"], "same window start");
    assert_eq!(
        from_cli["groups"], direct["groups"],
        "`usage --json` must be the server's report"
    );
    let repo_group = direct["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["workspace"] == "repo")
        .expect("the seeded finish should appear under `repo`");

    // The table shows the same numbers the report does.
    let (ok, out) = run_cli(&base, &["usage", "--since", "2000-01-01"]);
    assert!(ok, "usage failed: {out}");
    let row = out
        .lines()
        .find(|l| l.starts_with("repo"))
        .unwrap_or_else(|| panic!("usage table should have a repo row:\n{out}"));
    let cost = format!("${:.4}", repo_group["cost_usd"].as_f64().unwrap());
    assert!(row.contains(&cost), "row should show {cost}: {row}");

    let (ok, out) = run_cli(&base, &["usage", "--workspace", "nope"]);
    assert!(ok, "usage --workspace failed: {out}");
    assert!(
        out.contains("no usage recorded for workspace 'nope'"),
        "a filter matching nothing should say so: {out}"
    );

    // A window the CLI cannot turn into a real bound is refused, not sent —
    // the server would compare it as a string and answer for some other window.
    let (ok, out) = run_cli(&base, &["usage", "--since", "yesterday"]);
    assert!(!ok, "a bad --since must fail: {out}");
    assert!(
        out.contains("--since"),
        "the error should name the flag: {out}"
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
    // caliband). Verify the daemon persisted it via /api/workspaces.
    let (ok, out) = run_cli(
        &base,
        &[
            "workspace",
            "config",
            "repo",
            "--provider",
            "openai",
            "--base-url",
            "http://h:9292/v1",
        ],
    );
    assert!(ok, "repo config failed: {out}");
    assert!(
        out.contains("updated provider config"),
        "config output: {out}"
    );

    // `/api/workspaces` serves the poll-refreshed snapshot, so the just-set
    // config becomes visible eventually (typically within a poll cycle), not
    // necessarily on the very next read — the workspace also legitimately goes
    // `unreachable` here because the config change restarted its caliband. Poll
    // for the provider to appear rather than asserting a single immediate read
    // (prospero #85 — the underlying registry clobber is fixed in core; this
    // keeps the e2e robust to the endpoint's eventual-consistency).
    let repos_url = format!("{base}/api/workspaces");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let cfg = loop {
        let url = repos_url.clone();
        let repos: serde_json::Value =
            tokio::task::spawn_blocking(move || ureq::get(&url).call().unwrap().into_json())
                .await
                .unwrap()
                .unwrap();
        let cfg = repos.as_array().unwrap()[0]["config"].clone();
        if cfg["provider"].as_str() == Some("openai") {
            break cfg;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "repo config never became visible via /api/workspaces: {repos}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    assert_eq!(cfg["provider"].as_str(), Some("openai"));
    assert_eq!(cfg["base_url"].as_str(), Some("http://h:9292/v1"));
}

fn run_cli_env(base: &str, env: &[(&str, &str)], args: &[&str]) -> (bool, String) {
    let mut cmd = Command::new(PROSPERO_BIN);
    cmd.env_remove("PROSPERO_TOKEN")
        .env_remove("PROSPERO_TOKEN_FILE");
    for (k, v) in env {
        cmd.env(k, v);
    }
    let output = cmd
        .arg("--addr")
        .arg(base)
        .args(args)
        .output()
        .expect("running prospero binary");
    let mut combined = String::from_utf8_lossy(&output.stdout).to_string();
    combined.push_str(&String::from_utf8_lossy(&output.stderr));
    (output.status.success(), combined)
}

// `token new` is fully offline: it must succeed even when PROSPERO_TOKEN_FILE names
// a path that doesn't exist (e.g. a stale env var), and it must not touch the
// network at all — the base URL below is never actually dialed (#2 review finding,
// round 1: `main` used to resolve --token/--token-file before dispatch, so a bad
// token file broke this offline command too).
#[test]
fn token_new_ignores_an_unreadable_token_file() {
    let (ok, out) = run_cli_env(
        "http://127.0.0.1:1",
        &[("PROSPERO_TOKEN_FILE", "/nonexistent/prospero-token")],
        &["token", "new", "ci", "--scope", "read"],
    );
    assert!(ok, "token new must stay offline: {out}");
    assert!(
        out.contains("token (shown once): "),
        "token line missing: {out}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_authenticates_against_an_auth_enabled_daemon() {
    use prospero_api::auth::{AuthState, SessionKey};
    use prospero_core::Scope;
    use prospero_core::auth::{TokenSet, generate_token, tokens_file_line};

    let repo_dir = tempfile::tempdir().unwrap();
    let runtime_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let repo_root = repo_dir.path().canonicalize().unwrap();
    let env = DiscoveryEnv {
        caliban_daemon_runtime_dir: Some(runtime_dir.path().to_path_buf()),
        xdg_runtime_dir: None,
        tmpdir: None,
    };
    let _fake = FakeCaliband::start_at(&control_socket_path(&repo_root, &env))
        .await
        .unwrap();
    let mut config = FleetConfig::new("e2e-auth", data_dir.path());
    config.discovery_env = env;
    config.ensure = EnsureConfig {
        autostart: false,
        ..EnsureConfig::default()
    };
    config.poll_interval = Duration::from_millis(100);
    let store = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let manager = FleetManager::new(config, store).await.unwrap();
    manager.add_repo("repo", repo_root).await.unwrap();
    tokio::spawn(manager.clone().run());

    let ops = generate_token();
    let tokens = TokenSet::parse(&tokens_file_line("ops", Scope::Operate, &ops)).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let local = LocalFleet::new(manager.clone());
    let app = prospero_api::router_with_auth(
        Arc::new(local.clone()),
        Some(Arc::new(local)),
        manager.store(),
        manager.bus(),
        None,
        AuthState::enabled(tokens, SessionKey::random(), false),
    );
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    wait_for_health(&base).await;

    let (ok, out) = run_cli_env(&base, &[], &["ls"]);
    assert!(!ok, "ls without a token must fail: {out}");
    assert!(out.contains("PROSPERO_TOKEN"), "401 message: {out}");

    let with_token = [("PROSPERO_TOKEN", ops.as_str())];
    let (ok, out) = run_cli_env(&base, &with_token, &["ls"]);
    assert!(ok && out.contains("repo"), "ls with token: {out}");

    let (ok, out) = run_cli_env(&base, &with_token, &["whoami"]);
    assert!(ok && out.contains("ops (operate)"), "whoami: {out}");

    let (ok, out) = run_cli_env(&base, &with_token, &["workspace", "rm", "repo"]);
    assert!(!ok && out.contains("requires scope admin"), "403: {out}");

    let (ok, out) = run_cli_env(&base, &[], &["token", "new", "ci", "--scope", "read"]);
    assert!(ok, "token new: {out}");
    let token = out
        .lines()
        .find_map(|l| l.strip_prefix("token (shown once): "))
        .expect("token line")
        .trim()
        .to_string();
    let line = out
        .lines()
        .find_map(|l| l.strip_prefix("tokens-file line:  "))
        .expect("tokens-file line");
    let parsed = TokenSet::parse(line).unwrap();
    assert_eq!(parsed.authenticate(&token).unwrap().name, "ci");
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
