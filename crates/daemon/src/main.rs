//! `prosperod` — the long-running Prospero control-plane daemon.
//!
//! Wires a [`FleetManager`] to the HTTP/SSE API + dashboard, runs the
//! background poll loop, and serves until interrupted.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use prospero_core::bus::{EventBus, InProcessBus};
use prospero_core::config_store::{ConfigStore, SqliteConfigStore};
use prospero_core::discovery::{DiscoveryEnv, EnsureConfig};
use prospero_core::fleet::{FleetConfig, FleetManager};
use prospero_core::fleet_provider::LocalFleet;
use prospero_core::ownership::{Ownership, SelfOwnsAll};
use prospero_core::sqlite_store::SqliteStore;
use prospero_core::store::Store;
use prospero_core::{DistributedBus, LeasedOwnership, PostgresConfigStore, PostgresStore};
use tokio::task::JoinHandle;

/// Fleet control-plane backend, selected by `--fleet-backend`/`PROSPERO_FLEET`.
///
/// `local` (default) is the caliband-over-Unix-sockets `LocalFleet`.
/// `k8s` selects the `K8sFleet` backend (`CalibanTask` CRs + network session
/// plane, ADR 0008), wired into the request path via the `FleetProvider`/
/// `FleetAdmin` seams (#76); it needs a build with `--features k8s`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[value(rename_all = "lower")]
enum FleetBackend {
    Local,
    K8s,
}

/// Prospero control-plane daemon.
#[derive(Debug, Parser)]
#[command(name = "prosperod", version, about)]
struct Args {
    /// Address to bind the HTTP API + dashboard on.
    #[arg(long, env = "PROSPERO_ADDR", default_value = "127.0.0.1:7878")]
    addr: SocketAddr,

    /// Fleet control-plane backend: `local` (caliband over Unix) or `k8s`
    /// (CalibanTask CRs; requires a build with `--features k8s`). See
    /// docs/container.md "Fleet backends".
    #[arg(long, env = "PROSPERO_FLEET", default_value = "local")]
    fleet_backend: FleetBackend,

    /// k8s only: PEM CA bundle trusting caliband's session-plane serving cert.
    /// When set, per-agent dials use TLS; unset ⇒ plaintext (unchanged).
    #[arg(long, env = "PROSPERO_K8S_CALIBAND_CA_FILE")]
    k8s_caliband_ca_file: Option<PathBuf>,

    /// k8s only: file holding the session-plane bearer token (contents trimmed).
    /// When set, per-agent dials present the token; unset ⇒ no token.
    #[arg(long, env = "PROSPERO_K8S_CALIBAND_TOKEN_FILE")]
    k8s_caliband_token_file: Option<PathBuf>,

    /// k8s only: SNI / cert-validation name for the session-plane TLS check.
    #[arg(
        long,
        env = "PROSPERO_K8S_CALIBAND_SERVER_NAME",
        default_value = "caliband"
    )]
    k8s_caliband_server_name: String,

    /// k8s only: explicit kubeconfig file. Unset ⇒ infer (in-cluster, then
    /// ambient kubeconfig).
    #[arg(long, env = "KUBECONFIG")]
    kubeconfig: Option<PathBuf>,

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

    /// Delete events older than this many days on an hourly loop. 0 disables.
    #[arg(long, default_value_t = 0)]
    retention_days: u64,

    /// Postgres connection URL. When set, prosperod runs in CLUSTERED mode
    /// (Postgres store/config + LISTEN/NOTIFY bus + leased ownership); when
    /// unset, it runs STANDALONE (sqlite + in-process bus + self-owns-all).
    #[arg(long, env = "PROSPERO_DATABASE_URL")]
    database_url: Option<String>,

    /// Clustered only: this replica's identity for lease ownership. Defaults to
    /// the HOSTNAME env (the pod name under k8s). MUST be unique per replica.
    #[arg(long, env = "PROSPERO_REPLICA_ID")]
    replica_id: Option<String>,

    /// Clustered only: lease time-to-live in seconds. A stream's owner must
    /// heartbeat within this window or a peer may take the stream over.
    #[arg(long, default_value_t = 30.0)]
    lease_ttl_secs: f64,

    /// Clustered only: how often (ms) to renew held leases. Defaults to a third
    /// of the lease TTL.
    #[arg(long)]
    heartbeat_interval_ms: Option<u64>,
}

/// Parse a `KEY=VALUE` pair (value may contain further `=`).
fn parse_key_val(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(format!("expected KEY=VALUE, got '{s}'")),
    }
}

/// Read a bearer token from a mounted-Secret file, trimming the trailing
/// whitespace/newline that Secret files commonly carry. A missing or
/// unreadable path is fatal — a silently-empty token would defeat auth.
///
/// Not feature-gated (its tests run in every build), but only *called* from the
/// k8s arm — so a bin-only build without `k8s` sees it as dead. Allow that.
/// Refuse to start the k8s backend with a session-plane token but no TLS: the
/// token would be written in the clear on every per-agent dial (see the preamble
/// in `caliband::transport`), defeating its purpose. Fail fast at startup rather
/// than silently transmit it (#107).
///
/// Like [`read_token_file`], only *called* from the k8s arm, so a non-k8s build
/// sees it as dead — but its test runs in every build.
#[cfg_attr(not(feature = "k8s"), allow(dead_code))]
fn require_token_tls(token_present: bool, tls_present: bool) -> anyhow::Result<()> {
    if token_present && !tls_present {
        anyhow::bail!(
            "a session-plane token is configured (--k8s-caliband-token-file / \
             PROSPERO_K8S_CALIBAND_TOKEN_FILE) but TLS is not (--k8s-caliband-ca-file / \
             PROSPERO_K8S_CALIBAND_CA_FILE); the token would be sent in cleartext. \
             Configure the CA file to enable TLS, or unset the token."
        );
    }
    Ok(())
}

#[cfg_attr(not(feature = "k8s"), allow(dead_code))]
fn read_token_file(path: &Path) -> anyhow::Result<String> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading session-plane token file {}", path.display()))?;
    Ok(raw.trim_end().to_string())
}

/// Build client-side session-plane TLS from a CA file, when one is configured.
/// `None` ⇒ TLS stays off (plaintext, unchanged). A good PEM ⇒ `Some(client)`
/// trusting that CA and validating the server presents `server_name`. An
/// unreadable file or unparseable PEM is fatal (fail fast — no silent plaintext
/// fall-back).
#[cfg(feature = "k8s")]
fn load_session_plane_tls(
    ca_file: Option<&Path>,
    server_name: &str,
) -> anyhow::Result<Option<prospero_core::caliband::transport::TlsClient>> {
    let Some(ca_file) = ca_file else {
        return Ok(None);
    };
    let ca_pem = std::fs::read(ca_file)
        .with_context(|| format!("reading session-plane CA file {}", ca_file.display()))?;
    let client = prospero_core::caliband::transport::tls_client_from_pem(&ca_pem, server_name)
        .with_context(|| format!("building session-plane TLS from {}", ca_file.display()))?;
    Ok(Some(client))
}

/// Build a `kube::Client`: from an explicit kubeconfig file when `kubeconfig`
/// is set, else `try_default()` (infers in-cluster then ambient kubeconfig).
#[cfg(feature = "k8s")]
async fn build_kube_client(kubeconfig: Option<&Path>) -> anyhow::Result<kube::Client> {
    match kubeconfig {
        Some(path) => {
            let kc = kube::config::Kubeconfig::read_from(path)
                .with_context(|| format!("reading kubeconfig {}", path.display()))?;
            let cfg = kube::Config::from_custom_kubeconfig(
                kc,
                &kube::config::KubeConfigOptions::default(),
            )
            .await
            .with_context(|| format!("loading kubeconfig {}", path.display()))?;
            kube::Client::try_from(cfg).with_context(|| "building kube client from kubeconfig")
        }
        None => kube::Client::try_default()
            .await
            .with_context(|| "connecting to the Kubernetes API server"),
    }
}

/// This replica's lease identity: the explicit `--replica-id`, else the
/// `HOSTNAME` env (the pod name in k8s), else a local fallback.
fn resolve_replica_id(explicit: Option<&str>) -> String {
    explicit
        .map(str::to_string)
        .or_else(|| std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty()))
        .unwrap_or_else(|| "prosperod-local".to_string())
}

/// Heartbeat period: explicit `--heartbeat-interval-ms`, else a third of the
/// lease TTL (clamped to at least 1s so a tiny TTL can't busy-loop).
fn heartbeat_interval(explicit_ms: Option<u64>, lease_ttl_secs: f64) -> Duration {
    match explicit_ms {
        Some(ms) => Duration::from_millis(ms.max(1)),
        None => {
            let secs = (lease_ttl_secs / 3.0).max(1.0);
            Duration::from_secs_f64(secs)
        }
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

    // Reject an unsupported backend BEFORE any side effects (#121). Phase 1
    // below creates the data dir and opens the sqlite/Postgres store; doing
    // that first for a `--fleet-backend k8s` invocation on a build without the
    // k8s feature would leave a data dir behind and open a store only to bail
    // out at Phase 2. The `K8s` variant exists in every build — only the
    // Phase-2 match arm is feature-gated — so guard it here, cfg'd out (and
    // thus a no-op) when the k8s feature *is* present.
    #[cfg(not(feature = "k8s"))]
    if args.fleet_backend == FleetBackend::K8s {
        anyhow::bail!(
            "PROSPERO_FLEET=k8s requires a prosperod built with the k8s feature \
             (`cargo build -p prospero-daemon --features k8s`)."
        );
    }

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

    // Phase 1 — the shared observability plane (store + bus), composed per
    // storage topology and handed to whichever backend serves. A Postgres URL
    // ⇒ clustered (Postgres store + LISTEN/NOTIFY bus); else standalone (sqlite
    // + in-process bus). This is all the k8s backend needs (#83): it reads
    // history/SSE from this store/bus and never builds a FleetManager.
    let (store, bus): (Arc<dyn Store>, Arc<dyn EventBus>) = if let Some(url) =
        args.database_url.clone()
    {
        let store: Arc<dyn Store> = Arc::new(
            PostgresStore::connect(&url)
                .await
                .with_context(|| "connecting clustered event store")?,
        );
        let bus: Arc<dyn EventBus> = Arc::new(
            DistributedBus::connect(&url, store.clone())
                .await
                .with_context(|| "connecting clustered event bus")?,
        );
        tracing::info!(target: "prosperod", topology = "clustered", "selected clustered topology");
        (store, bus)
    } else {
        let store: Arc<dyn Store> = Arc::new(
            SqliteStore::open(&data_dir)
                .await
                .with_context(|| "opening event store")?,
        );
        let bus: Arc<dyn EventBus> = Arc::new(InProcessBus::new(config.event_buffer));
        tracing::info!(target: "prosperod", topology = "standalone", "selected standalone topology");
        (store, bus)
    };

    // Phase 2 — select the serving backend over the shared store/bus. `local`
    // builds the full FleetManager (registry + ownership + poll loop); `k8s`
    // serves K8sFleet with NO manager, poll loop, config store, ownership, or
    // heartbeat — those are local-only machinery, inert under k8s (#83). The
    // `FleetAdmin` (workspace registry) is likewise `Some` only for local; its
    // routes return 405 under k8s, where workspaces are `CalibanTask`-driven.
    // The match yields the serving fleet/admin plus the background handles this
    // backend owns (poll loop, heartbeat, manager for graceful shutdown) — all
    // `None` under k8s, which starts none of them (#83).
    #[allow(clippy::type_complexity)]
    let (fleet, admin, poll_handle, heartbeat_handle, manager_for_shutdown): (
        Arc<dyn prospero_core::FleetProvider>,
        Option<Arc<dyn prospero_core::FleetAdmin>>,
        Option<JoinHandle<()>>,
        Option<JoinHandle<()>>,
        Option<FleetManager>,
    ) = match args.fleet_backend {
        FleetBackend::Local => {
            // Per-topology registry + ownership seams (clustered adds a lease
            // heartbeat). Both topologies go through `with_seams`, building the
            // same manager `FleetManager::new` would for standalone.
            let (config_store, ownership, heartbeat_handle): (
                Arc<dyn ConfigStore>,
                Arc<dyn Ownership>,
                Option<JoinHandle<()>>,
            ) = if let Some(url) = args.database_url.clone() {
                let replica_id = resolve_replica_id(args.replica_id.as_deref());
                let config_store: Arc<dyn ConfigStore> = Arc::new(
                    PostgresConfigStore::connect(&url)
                        .await
                        .with_context(|| "connecting clustered config store")?,
                );
                let ownership = Arc::new(
                    LeasedOwnership::connect(&url, replica_id.clone(), args.lease_ttl_secs)
                        .await
                        .with_context(|| "connecting clustered ownership")?,
                );

                // Heartbeat: renew this replica's held leases so it keeps its
                // streams. Local-only — k8s builds no ownership (#83).
                let interval = heartbeat_interval(args.heartbeat_interval_ms, args.lease_ttl_secs);
                let hb = ownership.clone();
                let heartbeat_handle = tokio::spawn(async move {
                    let mut tick = tokio::time::interval(interval);
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tick.tick().await;
                        hb.heartbeat().await;
                    }
                });
                tracing::info!(
                    target: "prosperod", %replica_id,
                    lease_ttl_secs = args.lease_ttl_secs,
                    heartbeat_ms = interval.as_millis() as u64,
                    "clustered ownership + heartbeat active"
                );
                (
                    config_store,
                    ownership as Arc<dyn Ownership>,
                    Some(heartbeat_handle),
                )
            } else {
                let config_store: Arc<dyn ConfigStore> = Arc::new(
                    SqliteConfigStore::open(&data_dir)
                        .await
                        .with_context(|| "opening config store")?,
                );
                (
                    config_store,
                    Arc::new(SelfOwnsAll) as Arc<dyn Ownership>,
                    None,
                )
            };

            let manager = FleetManager::with_seams(
                config,
                store.clone(),
                config_store,
                bus.clone(),
                ownership,
            )
            .await
            .with_context(|| "building fleet manager")?;

            let local = LocalFleet::new(manager.clone());
            let poll_handle = tokio::spawn(manager.clone().run());
            tracing::info!(target: "prosperod", backend = "local", "serving via LocalFleet");
            (
                Arc::new(local.clone()) as Arc<dyn prospero_core::FleetProvider>,
                Some(Arc::new(local) as Arc<dyn prospero_core::FleetAdmin>),
                Some(poll_handle),
                heartbeat_handle,
                Some(manager),
            )
        }
        #[cfg(feature = "k8s")]
        FleetBackend::K8s => {
            let client = build_kube_client(args.kubeconfig.as_deref()).await?;
            let ns =
                std::env::var("PROSPERO_K8S_NAMESPACE").unwrap_or_else(|_| "default".to_string());
            let api = prospero_core::KubeTaskApi::new(client, &ns);

            // Session-plane security (ADR 0051): trust caliband's serving cert
            // via the mounted-Secret CA, and present the shared bearer token.
            // Both are Option — unset ⇒ with_network(None, None), i.e. today's
            // plaintext behavior, so existing deployments are unaffected.
            let tls = load_session_plane_tls(
                args.k8s_caliband_ca_file.as_deref(),
                &args.k8s_caliband_server_name,
            )?;
            let token = args
                .k8s_caliband_token_file
                .as_deref()
                .map(read_token_file)
                .transpose()?;

            // Never send the bearer token over plaintext (#107).
            require_token_tls(token.is_some(), tls.is_some())?;

            // No FleetManager under k8s: K8sFleet serves directly over the
            // shared store/bus (#83).
            let k8s = prospero_core::K8sFleet::new(api, bus.clone(), store.clone())
                .with_network(tls.clone(), token.clone());
            tracing::info!(
                target: "prosperod", backend = "k8s", namespace = %ns,
                session_tls = tls.is_some(), session_token = token.is_some(),
                "serving via K8sFleet (no FleetManager)"
            );
            (
                Arc::new(k8s) as Arc<dyn prospero_core::FleetProvider>,
                None,
                None,
                None,
                None,
            )
        }
        #[cfg(not(feature = "k8s"))]
        FleetBackend::K8s => anyhow::bail!(
            "PROSPERO_FLEET=k8s requires a prosperod built with the k8s feature \
             (`cargo build -p prospero-daemon --features k8s`)."
        ),
    };

    // Age-based retention (#4) — both arms, off the shared store, with no
    // dependency on FleetManager (#83).
    if args.retention_days > 0 {
        let s = store.clone();
        let max_age = Duration::from_secs(args.retention_days * 24 * 3600);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(3600));
            loop {
                tick.tick().await;
                match prospero_core::store::prune_store_older_than(s.as_ref(), max_age).await {
                    Ok(n) if n > 0 => {
                        tracing::info!(target: "prosperod", pruned = n, "retention swept old events")
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(target: "prosperod", error = %e, "retention prune failed")
                    }
                }
            }
        });
    }

    let app = prospero_api::router(fleet, admin, store.clone(), bus.clone());
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

    // HTTP has drained; now drain whatever background work this backend started.
    // Only the local arm builds a poll loop / heartbeat — under k8s there's
    // nothing to drain (#83).
    if let Some(manager) = &manager_for_shutdown {
        manager.begin_shutdown();
    }
    if let Some(poll_handle) = poll_handle
        && let Err(e) = poll_handle.await
    {
        tracing::warn!(error = %e, "poll loop did not drain cleanly");
    }
    if let Some(hb) = heartbeat_handle {
        hb.abort();
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
    use super::read_token_file;
    use super::require_token_tls;
    use super::{heartbeat_interval, resolve_replica_id};
    use std::time::Duration;

    #[test]
    fn read_token_file_trims_trailing_newline() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("token");
        std::fs::write(&p, "s3cr3t\n").unwrap();
        assert_eq!(read_token_file(&p).unwrap(), "s3cr3t");
    }

    #[test]
    fn read_token_file_missing_is_err() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_token_file(&dir.path().join("nope")).is_err());
    }

    #[test]
    fn require_token_tls_rejects_token_without_tls() {
        // The one unsafe combination: a token but no TLS → cleartext token.
        assert!(require_token_tls(true, false).is_err());
        // Every other combination is fine (no token, or token protected by TLS,
        // or TLS with no token).
        assert!(require_token_tls(true, true).is_ok());
        assert!(require_token_tls(false, false).is_ok());
        assert!(require_token_tls(false, true).is_ok());
    }

    #[cfg(feature = "k8s")]
    mod k8s_tls {
        use super::super::load_session_plane_tls;

        fn write_ca(dir: &std::path::Path) -> std::path::PathBuf {
            // A self-signed cert doubles as its own CA for trust-store loading.
            let cert = rcgen::generate_simple_self_signed(vec!["caliband".into()]).unwrap();
            let p = dir.join("ca.crt");
            std::fs::write(&p, cert.cert.pem()).unwrap();
            p
        }

        #[test]
        fn none_ca_means_tls_off() {
            assert!(load_session_plane_tls(None, "caliband").unwrap().is_none());
        }

        #[test]
        fn good_ca_builds_a_client() {
            let dir = tempfile::tempdir().unwrap();
            let ca = write_ca(dir.path());
            assert!(
                load_session_plane_tls(Some(&ca), "caliband")
                    .unwrap()
                    .is_some()
            );
        }

        #[test]
        fn unparseable_pem_is_err() {
            let dir = tempfile::tempdir().unwrap();
            let p = dir.path().join("bad.crt");
            std::fs::write(&p, "not a pem").unwrap();
            assert!(load_session_plane_tls(Some(&p), "caliband").is_err());
        }

        #[test]
        fn missing_ca_file_is_err() {
            let dir = tempfile::tempdir().unwrap();
            assert!(load_session_plane_tls(Some(&dir.path().join("nope")), "caliband").is_err());
        }
    }

    #[test]
    fn replica_id_prefers_explicit_then_falls_back() {
        assert_eq!(resolve_replica_id(Some("r7")), "r7");
        // With no explicit id, falls back to HOSTNAME or the local default.
        // (We don't mutate process env here; just assert it returns non-empty.)
        assert!(!resolve_replica_id(None).is_empty());
    }

    #[test]
    fn heartbeat_defaults_to_a_third_of_ttl_and_is_clamped() {
        assert_eq!(
            heartbeat_interval(Some(500), 30.0),
            Duration::from_millis(500)
        );
        assert_eq!(heartbeat_interval(None, 30.0), Duration::from_secs(10));
        // Tiny TTL clamps to >= 1s; explicit 0 clamps to >= 1ms.
        assert_eq!(heartbeat_interval(None, 0.6), Duration::from_secs(1));
        assert_eq!(heartbeat_interval(Some(0), 30.0), Duration::from_millis(1));
    }

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
