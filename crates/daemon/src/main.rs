//! `prosperod` — the long-running Prospero control-plane daemon.
//!
//! Wires a [`FleetManager`] to the HTTP/SSE API + dashboard, runs the
//! background poll loop, and serves until interrupted.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use prospero_core::discovery::{DiscoveryEnv, EnsureConfig};
use prospero_core::fleet::{FleetConfig, FleetManager};
use prospero_core::sqlite_store::SqliteStore;

/// Prospero control-plane daemon.
#[derive(Debug, Parser)]
#[command(name = "prosperod", version, about)]
struct Args {
    /// Address to bind the HTTP API + dashboard on.
    #[arg(long, env = "PROSPERO_ADDR", default_value = "127.0.0.1:7878")]
    addr: SocketAddr,

    /// Directory for the registry and event store.
    #[arg(long, env = "PROSPERO_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Host identity reported in fleet snapshots.
    #[arg(long, env = "PROSPERO_HOST", default_value = "local")]
    host: String,

    /// Poll interval in milliseconds.
    #[arg(long, default_value_t = 2000)]
    poll_interval_ms: u64,

    /// Do not auto-start caliband daemons for registered repos.
    #[arg(long)]
    no_autostart: bool,

    /// Path/name of the caliban daemon binary used for autostart.
    #[arg(long, default_value = "caliband")]
    caliband_bin: String,

    /// Default env var applied under every repo's resolved config (repeatable).
    #[arg(long = "default-env", value_parser = parse_key_val)]
    default_env: Vec<(String, String)>,
}

/// Parse a `KEY=VALUE` pair (value may contain further `=`).
fn parse_key_val(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(format!("expected KEY=VALUE, got '{s}'")),
    }
}

/// Default data dir: `$XDG_DATA_HOME/prospero` or `$HOME/.local/share/prospero`.
fn default_data_dir() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_DATA_HOME") {
        PathBuf::from(xdg).join("prospero")
    } else if let Some(home) = std::env::var_os("HOME") {
        PathBuf::from(home).join(".local/share/prospero")
    } else {
        PathBuf::from(".prospero")
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let data_dir = args.data_dir.clone().unwrap_or_else(default_data_dir);
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("creating data dir {}", data_dir.display()))?;

    let mut config = FleetConfig::new(args.host.clone(), data_dir.clone());
    config.poll_interval = Duration::from_millis(args.poll_interval_ms);
    config.discovery_env = DiscoveryEnv::from_process();
    config.ensure = EnsureConfig {
        autostart: !args.no_autostart,
        caliband_bin: args.caliband_bin.clone(),
        ..EnsureConfig::default()
    };
    config.default_env = args.default_env.iter().cloned().collect();

    let store = Arc::new(
        SqliteStore::open(&data_dir)
            .await
            .with_context(|| "opening event store")?,
    );
    let manager = FleetManager::new(config, store).with_context(|| "building fleet manager")?;

    // Background poll loop.
    let poll_handle = tokio::spawn(manager.clone().run());

    let app = prospero_api::router(manager.clone());
    let listener = tokio::net::TcpListener::bind(args.addr)
        .await
        .with_context(|| format!("binding {}", args.addr))?;

    tracing::info!(
        addr = %args.addr,
        data_dir = %data_dir.display(),
        "prosperod listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .with_context(|| "serving HTTP")?;

    // HTTP has drained; now drain the background poll loop and attach tasks so we
    // don't abandon an in-flight poll/append mid-iteration.
    manager.begin_shutdown();
    if let Err(e) = poll_handle.await {
        tracing::warn!(error = %e, "poll loop did not drain cleanly");
    }

    tracing::info!("prosperod shut down");
    Ok(())
}

/// Resolve when the process receives Ctrl-C (and SIGTERM on Unix).
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::parse_key_val;

    #[test]
    fn parses_key_value() {
        assert_eq!(
            parse_key_val("A=b").unwrap(),
            ("A".to_string(), "b".to_string())
        );
        // Values may contain '='.
        assert_eq!(
            parse_key_val("URL=http://h:1?x=1").unwrap(),
            ("URL".to_string(), "http://h:1?x=1".to_string())
        );
        assert!(parse_key_val("noequals").is_err());
        assert!(parse_key_val("=val").is_err()); // empty key rejected
    }
}
