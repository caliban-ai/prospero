//! The runtime heart of the control plane.
//!
//! `FleetManager` owns the in-memory [`FleetSnapshot`], polls each managed
//! repo's caliband for live state, attaches per-agent stream sockets while
//! agents are active, normalizes frames into [`FleetEvent`]s, and fans them out
//! over a broadcast bus while also appending them to the durable [`Store`].

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::AsyncBufReadExt;
use tokio::sync::{Mutex as AsyncMutex, RwLock, watch};

use crate::bus::{EventBus, InProcessBus};
use crate::caliband::client::CalibandClient;
use crate::caliband::stream::{NormalizeOptions, Normalized, normalize_frame};
use crate::caliband::wire::{AgentRecord, AttachInbound, SpawnSpec};
use crate::config_store::{ConfigStore, SqliteConfigStore};
use crate::discovery::{DiscoveryEnv, EnsureConfig, ensure_caliband};
use crate::error::{CoreError, Result};
use crate::event::{EventKind, FleetEvent};
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::model::{Agent, AgentStatus, FleetSnapshot, Repo, RepoHealth};
use crate::ownership::{Ownership, SelfOwnsAll};
use crate::registry::Registry;
use crate::store::Store;

/// A Prospero-level request to launch a new agent. Worktree isolation is the
/// default for parallel work on one codebase; opt out with `isolation_worktree:
/// false`.
#[derive(Debug, Clone)]
pub struct SpawnRequest {
    /// Initial prompt / task.
    pub prompt: String,
    /// Optional human-readable label.
    pub label: Option<String>,
    /// Optional model override.
    pub model: Option<String>,
    /// Run in an isolated git worktree. **Defaults to `true`** via
    /// [`SpawnRequest::new`].
    pub isolation_worktree: bool,
    /// Optional tool allowlist.
    pub tool_allowlist: Option<Vec<String>>,
    /// Run in interactive mode (the worker awaits operator input instead of
    /// finishing). Defaults to `false` via [`SpawnRequest::new`].
    pub interactive: bool,
}

impl SpawnRequest {
    /// A spawn request with worktree isolation on by default.
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            label: None,
            model: None,
            isolation_worktree: true,
            tool_allowlist: None,
            interactive: false,
        }
    }

    fn into_spec(self) -> SpawnSpec {
        SpawnSpec {
            label: self.label,
            frontmatter_path: None,
            initial_prompt: self.prompt,
            model: self.model,
            // Filled in `spawn_agent` from the repo's stored provider config —
            // the request itself carries no provider.
            provider: None,
            tool_allowlist: self.tool_allowlist,
            isolation_worktree: self.isolation_worktree,
            inherit_hooks: true,
            interactive: self.interactive,
        }
    }
}

/// Configuration for a [`FleetManager`].
#[derive(Debug, Clone)]
pub struct FleetConfig {
    /// Host identity (single host in the first stab).
    pub host: String,
    /// Directory for the config store and event store (both in `events.db`).
    pub data_dir: PathBuf,
    /// How often the poll loop refreshes each repo.
    pub poll_interval: Duration,
    /// Environment used for caliband socket discovery.
    pub discovery_env: DiscoveryEnv,
    /// Daemon autostart configuration.
    pub ensure: EnsureConfig,
    /// Stream normalization options.
    pub normalize: NormalizeOptions,
    /// Broadcast channel capacity (events buffered for slow subscribers).
    pub event_buffer: usize,
    /// Global default env merged under each repo's resolved overlay.
    pub default_env: std::collections::BTreeMap<String, String>,
    /// Reconnection backoff for a dropped per-agent attach stream.
    pub attach_backoff: AttachBackoff,
}

/// Bounded exponential-backoff policy for reconnecting a dropped attach stream.
///
/// On a premature stream drop (EOF before the agent's terminal `result`, or a
/// read error) the attach task waits `min(max, base * 2^attempt)` — with a
/// per-(agent, attempt) jitter to decorrelate reconnect storms across agents —
/// then reconnects, up to `max_retries` consecutive failures. Making progress
/// (new frames) resets the attempt counter. When the budget is exhausted the
/// task exits and the poll loop remains the long-term re-attach safety net.
#[derive(Debug, Clone, Copy)]
pub struct AttachBackoff {
    /// Delay before the first reconnect.
    pub base: Duration,
    /// Ceiling on any single backoff delay.
    pub max: Duration,
    /// Maximum consecutive reconnect attempts before giving up in-path.
    pub max_retries: u32,
}

impl Default for AttachBackoff {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(200),
            max: Duration::from_secs(10),
            max_retries: 8,
        }
    }
}

impl AttachBackoff {
    /// Jittered delay for a 0-based `attempt`. Exponential on `base`, capped at
    /// `max`, then scaled by a deterministic per-(agent, attempt) factor in
    /// `[0.5, 1.0)` so concurrent attaches don't reconnect in lockstep.
    fn delay_for(&self, agent_id: &str, attempt: u32) -> Duration {
        use std::hash::{Hash, Hasher};
        let exp = self
            .base
            .saturating_mul(2u32.saturating_pow(attempt.min(31)));
        let capped = exp.min(self.max);
        let mut h = std::collections::hash_map::DefaultHasher::new();
        agent_id.hash(&mut h);
        attempt.hash(&mut h);
        let frac = 0.5 + 0.5 * ((h.finish() % 1000) as f64 / 1000.0);
        Duration::from_secs_f64(capped.as_secs_f64() * frac)
    }
}

impl FleetConfig {
    /// A config rooted at `data_dir` with sensible first-stab defaults.
    pub fn new(host: impl Into<String>, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            host: host.into(),
            data_dir: data_dir.into(),
            poll_interval: Duration::from_secs(2),
            discovery_env: DiscoveryEnv::from_process(),
            ensure: EnsureConfig::default(),
            normalize: NormalizeOptions::default(),
            event_buffer: 1024,
            default_env: std::collections::BTreeMap::new(),
            attach_backoff: AttachBackoff::default(),
        }
    }
}

/// Stamps and dispatches events; cheaply cloneable into background tasks.
#[derive(Clone)]
struct Emitter {
    store: Arc<dyn Store>,
    bus: Arc<dyn EventBus>,
    /// Next `seq` per stream key, seeded lazily from the store's high-water.
    seqs: Arc<AsyncMutex<HashMap<String, u64>>>,
    metrics: Arc<Metrics>,
}

impl Emitter {
    async fn next_event(&self, repo: &str, agent_id: &str, kind: EventKind) -> FleetEvent {
        let stream_key = crate::event::stream_key_for(repo, agent_id);
        let seq = self.next_seq(&stream_key).await;
        FleetEvent {
            seq,
            ts: chrono::Utc::now().to_rfc3339(),
            repo: repo.to_string(),
            agent_id: agent_id.to_string(),
            kind,
        }
    }

    /// Allocate the next per-stream `seq`. The durable `high_water` read on a
    /// stream's first event happens WITHOUT holding the `seqs` lock, so the lock
    /// is never held across `.await` (no executor stall / cross-stream serialize).
    async fn next_seq(&self, stream_key: &str) -> u64 {
        // Fast path: counter already seeded for this stream this run.
        {
            let mut seqs = self.seqs.lock().await;
            if let Some(n) = seqs.get(stream_key) {
                let next = n + 1;
                seqs.insert(stream_key.to_string(), next);
                return next;
            }
        }
        // Slow path: first event this run — read durable high-water unlocked.
        let seeded = self.store.high_water(stream_key).await.unwrap_or_else(|e| {
            tracing::warn!(
                target: "prospero_fleet", stream = %stream_key, error = %e,
                "high_water read failed seeding per-stream seq; starting at 0"
            );
            0
        });
        // Invariant: `emit` records the seq in `seqs` (here) BEFORE it calls
        // `store.append`, so a concurrent task that read a stale `high_water`
        // during the append window still sees the up-to-date counter on its
        // re-check below — keeping `seq` unique and monotonic per stream.
        let mut seqs = self.seqs.lock().await;
        // Re-check: a concurrent task may have seeded while we read high_water.
        let next = match seqs.get(stream_key) {
            Some(n) => n + 1,
            None => seeded + 1,
        };
        seqs.insert(stream_key.to_string(), next);
        next
    }

    async fn emit(&self, repo: &str, agent_id: &str, kind: EventKind) {
        let event = self.next_event(repo, agent_id, kind).await;
        let lost_seq = event.seq;
        let append_err = match self.store.append(&event).await {
            Ok(()) => {
                self.metrics.record_append_ok();
                None
            }
            Err(e) => {
                self.metrics.record_append_failure();
                Some(e)
            }
        };
        // Live SSE flows regardless of persistence (ADR-0004 favors a never-down
        // fleet view). Ignore send errors: no subscribers is fine.
        self.bus.publish(event);
        if let Some(e) = append_err {
            tracing::warn!(target: "prospero_fleet", error = %e, "failed to persist event");
            self.emit_persist_gap(repo, agent_id, lost_seq, e).await;
        }
    }

    /// Record a durable-store divergence: the event at `lost_seq` reached the
    /// live bus but not durable history. Emits a [`EventKind::StorePersistFailed`]
    /// marker — persisted best-effort so a history reader sees the gap, and sent
    /// on the bus so live consumers know history is incomplete. The marker keeps
    /// the live and durable views from silently diverging (#25, ADR-0004).
    async fn emit_persist_gap(&self, repo: &str, agent_id: &str, lost_seq: u64, err: CoreError) {
        let marker = self
            .next_event(
                repo,
                agent_id,
                EventKind::StorePersistFailed {
                    lost_seq,
                    detail: err.to_string(),
                },
            )
            .await;
        match self.store.append(&marker).await {
            Ok(()) => self.metrics.record_append_ok(),
            Err(e) => {
                // Hard-down store: the gap is now observable only via logs
                // (documented degradation), but live consumers are still signalled.
                self.metrics.record_append_failure();
                tracing::warn!(target: "prospero_fleet", error = %e, "failed to persist store-gap marker");
            }
        }
        self.bus.publish(marker);
    }
}

struct Inner {
    config: FleetConfig,
    snapshot: RwLock<FleetSnapshot>,
    registry: RwLock<Registry>,
    config_store: Arc<dyn ConfigStore>,
    /// Per-repo control clients, cached after first discovery.
    clients: Mutex<HashMap<String, CalibandClient>>,
    /// Agent ids with a running attach task.
    attached: Mutex<HashSet<String>>,
    emitter: Emitter,
    ownership: Arc<dyn Ownership>,
    /// Broadcast shutdown signal: `true` once a graceful drain has begun. The
    /// poll loop and attach tasks subscribe and stop cooperatively.
    shutdown: watch::Sender<bool>,
}

/// The fleet control plane.
#[derive(Clone)]
pub struct FleetManager {
    inner: Arc<Inner>,
}

impl FleetManager {
    /// Build a manager, loading the persisted registry from a default
    /// [`SqliteConfigStore`] in `config.data_dir` (the same dir as the event
    /// store). For an injected config backend (e.g. Postgres), use
    /// [`Self::with_config_store`].
    pub async fn new(config: FleetConfig, store: Arc<dyn Store>) -> Result<Self> {
        let config_store = Arc::new(SqliteConfigStore::open(&config.data_dir).await?);
        Self::with_config_store(config, store, config_store).await
    }

    /// Build a manager with an explicit [`ConfigStore`] and the standalone
    /// `EventBus`/`Ownership` seams.
    pub async fn with_config_store(
        config: FleetConfig,
        store: Arc<dyn Store>,
        config_store: Arc<dyn ConfigStore>,
    ) -> Result<Self> {
        let bus: Arc<dyn EventBus> = Arc::new(InProcessBus::new(config.event_buffer));
        let ownership: Arc<dyn Ownership> = Arc::new(SelfOwnsAll);
        Self::with_seams(config, store, config_store, bus, ownership).await
    }

    /// Build a manager with every topology seam injected. Standalone passes
    /// `InProcessBus` + `SelfOwnsAll`; clustered (Phase 2d) passes
    /// `DistributedBus` + `LeasedOwnership`.
    pub async fn with_seams(
        config: FleetConfig,
        store: Arc<dyn Store>,
        config_store: Arc<dyn ConfigStore>,
        bus: Arc<dyn EventBus>,
        ownership: Arc<dyn Ownership>,
    ) -> Result<Self> {
        let registry = Registry {
            repos: config_store.list_repos().await?,
        };
        let emitter = Emitter {
            store,
            bus,
            seqs: Arc::new(AsyncMutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::default()),
        };
        let snapshot = FleetSnapshot {
            host: config.host.clone(),
            repos: registry
                .repos
                .iter()
                .map(|r| Repo {
                    name: r.name.clone(),
                    root: r.root.clone(),
                    health: RepoHealth::Healthy,
                    agents: Vec::new(),
                })
                .collect(),
        };
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                snapshot: RwLock::new(snapshot),
                registry: RwLock::new(registry),
                config_store,
                clients: Mutex::new(HashMap::new()),
                attached: Mutex::new(HashSet::new()),
                emitter,
                ownership,
                shutdown: watch::channel(false).0,
            }),
        })
    }

    /// Signal a graceful shutdown: the poll loop finishes its in-flight cycle and
    /// returns, and attach tasks stop reading between frames. Idempotent.
    ///
    /// Uses `send_replace` so the signal sticks even if no task has subscribed
    /// yet (plain `send` is a no-op when there are no receivers).
    pub fn begin_shutdown(&self) {
        self.inner.shutdown.send_replace(true);
    }

    /// Subscribe to one stream's live event tail (see [`crate::EventBus`]).
    /// Watchers of a single agent pass the agent id (its stream key); repo/fleet
    /// watchers pass `repo:<name>` / `fleet`.
    pub fn subscribe(&self, stream_key: &str) -> crate::bus::BusSubscription {
        self.inner.emitter.bus.subscribe(stream_key)
    }

    /// A clone of the current fleet snapshot.
    pub async fn snapshot(&self) -> FleetSnapshot {
        self.inner.snapshot.read().await.clone()
    }

    /// A snapshot of prosperod's operational counters (`active_attaches` is read
    /// live from the running attach set).
    pub fn metrics(&self) -> MetricsSnapshot {
        let active = self.inner.attached.lock().unwrap().len() as u64;
        self.inner.emitter.metrics.snapshot(active)
    }

    /// Aggregate readiness: store-writability (the ready gate) plus a summary of
    /// per-repo health. Used by the `/readyz` endpoint to distinguish liveness
    /// from readiness.
    pub async fn readiness(&self) -> crate::model::Readiness {
        let store_writable = self.inner.emitter.store.writable().await;
        let snap = self.inner.snapshot.read().await;
        let repos_total = snap.repos.len();
        let repos_healthy = snap
            .repos
            .iter()
            .filter(|r| matches!(r.health, RepoHealth::Healthy))
            .count();
        crate::model::Readiness {
            ready: store_writable,
            store_writable,
            repos_total,
            repos_healthy,
            repos_unreachable: repos_total - repos_healthy,
        }
    }

    /// Replay a stream's history from the store, with `seq >= from_seq`. Callers
    /// watching a single agent pass the agent id, which is that agent's stream
    /// key (see [`crate::event::stream_key_for`]); repo/fleet-level history uses
    /// the `repo:<name>` / `fleet` keys.
    pub async fn history(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
        self.inner.emitter.store.replay(stream_key, from_seq).await
    }

    /// Delete persisted events older than `max_age`. Returns the count removed.
    /// Backs the daemon's age-based retention loop (#4).
    pub async fn prune_older_than(&self, max_age: std::time::Duration) -> Result<u64> {
        let max = chrono::Duration::from_std(max_age).unwrap_or_else(|_| chrono::Duration::zero());
        let before = (chrono::Utc::now() - max).to_rfc3339();
        self.inner.emitter.store.prune(&before).await
    }

    /// Register a repo and persist the registry. Triggers an immediate poll.
    pub async fn add_repo(&self, name: impl Into<String>, root: impl Into<PathBuf>) -> Result<()> {
        self.add_repo_with_config(name, root, Default::default())
            .await
    }

    /// Register a repo with an initial provider config.
    pub async fn add_repo_with_config(
        &self,
        name: impl Into<String>,
        root: impl Into<PathBuf>,
        config: crate::registry::RepoProviderConfig,
    ) -> Result<()> {
        let name = name.into();
        let root = root.into();
        // Canonicalize so symlink aliases (e.g. `/tmp` vs `/private/tmp`)
        // collapse to one root — both for the duplicate-alias guard in the
        // registry and so the stored root matches the one caliband hashes for
        // its socket (#45, #47). Best-effort: a not-yet-existing path is kept
        // as-is rather than rejected.
        let root = std::fs::canonicalize(&root).unwrap_or(root);
        let repo = {
            let mut reg = self.inner.registry.write().await;
            reg.add(name.clone(), root.clone())?;
            reg.set_config(&name, config);
            reg.get(&name)
                .cloned()
                .expect("repo just inserted must exist")
        };
        self.inner.config_store.upsert_repo(&repo).await?;
        {
            let mut snap = self.inner.snapshot.write().await;
            if !snap.repos.iter().any(|r| r.name == name) {
                snap.repos.push(Repo {
                    name: name.clone(),
                    root: root.clone(),
                    health: RepoHealth::Healthy,
                    agents: Vec::new(),
                });
            }
        }
        self.poll_repo_once(&name).await;
        Ok(())
    }

    /// The stored provider config for a repo, if registered.
    pub async fn repo_config(&self, repo: &str) -> Option<crate::registry::RepoProviderConfig> {
        self.inner
            .registry
            .read()
            .await
            .get(repo)
            .map(|r| r.config.clone())
    }

    /// Unregister a repo and persist the registry.
    pub async fn remove_repo(&self, name: &str) -> Result<bool> {
        let removed = {
            let mut reg = self.inner.registry.write().await;
            reg.remove(name)
        };
        if removed {
            // Persist the removal first; only prune derived state once durable.
            self.inner.config_store.delete_repo(name).await?;
            self.inner
                .snapshot
                .write()
                .await
                .repos
                .retain(|r| r.name != name);
            self.inner.clients.lock().unwrap().remove(name);
        }
        Ok(removed)
    }

    /// Build the `EnsureConfig` for a repo, resolving its env overlay from the
    /// global default + the repo's stored provider config + prosperod's env.
    pub async fn ensure_config_for(&self, repo: &str) -> Result<EnsureConfig> {
        let cfg = {
            let reg = self.inner.registry.read().await;
            reg.get(repo)
                .map(|r| r.config.clone())
                .ok_or_else(|| CoreError::RepoNotFound(repo.to_string()))?
        };
        let env = crate::provider_env::resolve_env(&self.inner.config.default_env, &cfg, &|k| {
            std::env::var(k).ok()
        });
        let mut ensure = self.inner.config.ensure.clone();
        ensure.env = env;
        Ok(ensure)
    }

    /// Update a repo's provider config in the registry only (no restart).
    pub async fn set_repo_config_registry_only(
        &self,
        repo: &str,
        config: crate::registry::RepoProviderConfig,
    ) -> Result<()> {
        let record = {
            let mut reg = self.inner.registry.write().await;
            if !reg.set_config(repo, config) {
                return Err(CoreError::RepoNotFound(repo.to_string()));
            }
            reg.get(repo)
                .cloned()
                .expect("repo exists after successful set_config")
        };
        self.inner.config_store.upsert_repo(&record).await?;
        Ok(())
    }

    /// Get-or-create the control client for a repo (running discovery once).
    async fn client_for(&self, repo: &str) -> Result<CalibandClient> {
        if let Some(c) = self.inner.clients.lock().unwrap().get(repo).cloned() {
            return Ok(c);
        }
        let root = {
            let reg = self.inner.registry.read().await;
            reg.get(repo)
                .map(|r| r.root.clone())
                .ok_or_else(|| CoreError::RepoNotFound(repo.to_string()))?
        };
        let ensure = self.ensure_config_for(repo).await?;
        let client = ensure_caliband(&root, &self.inner.config.discovery_env, &ensure).await?;
        self.inner
            .clients
            .lock()
            .unwrap()
            .insert(repo.to_string(), client.clone());
        Ok(client)
    }

    /// Validate that `repo`'s selected provider has its required credential
    /// before a spawn is issued, so a misconfigured repo surfaces an actionable
    /// error to the caller rather than spawning a doomed agent. Resolves the env
    /// the same way [`Self::ensure_config_for`] does and checks the result.
    async fn validate_provider_env(&self, repo: &str) -> Result<()> {
        let cfg = {
            let reg = self.inner.registry.read().await;
            reg.get(repo)
                .map(|r| r.config.clone())
                .ok_or_else(|| CoreError::RepoNotFound(repo.to_string()))?
        };
        let env = crate::provider_env::resolve_env(&self.inner.config.default_env, &cfg, &|k| {
            std::env::var(k).ok()
        });
        crate::provider_env::validate_provider_env(&cfg, &env)
            .map_err(CoreError::ProviderMisconfigured)
    }

    /// Launch a new agent under `repo`. Returns the new agent id.
    pub async fn spawn_agent(&self, repo: &str, req: SpawnRequest) -> Result<String> {
        self.validate_provider_env(repo).await?;
        let client = self.client_for(repo).await?;
        let mut spec = req.into_spec();
        // Select the provider via the wire spec (#93): the caliban worker reads
        // `SpawnSpec.provider`, not `CALIBAN_PROVIDER`, so carry the repo's
        // configured provider through. Base URL / API key still flow via the
        // caliband daemon env (see `provider_env::resolve_env`).
        spec.provider = self.repo_config(repo).await.and_then(|c| c.provider);
        let (id, _socket) = client.spawn(spec).await?;
        self.inner
            .emitter
            .emit(repo, &id, EventKind::AgentSpawned)
            .await;
        self.start_attach(repo, &id, client).await;
        Ok(id)
    }

    /// Kill an agent (resolving its repo from the snapshot).
    pub async fn kill_agent(&self, agent_id: &str) -> Result<()> {
        let repo = self.repo_of(agent_id).await?;
        self.client_for(&repo).await?.kill(agent_id).await
    }

    /// Send an inbound control frame to an interactive agent. Rejects if the
    /// agent is unknown (`AgentNotFound`), terminal, or was not spawned
    /// interactive (`InvalidState`).
    ///
    /// The state gate reads the last poll snapshot (up to one poll interval
    /// stale); caliband remains authoritative, so a just-terminated agent may
    /// pass the gate and fail at `attach`/`send_inbound` instead.
    pub async fn send_agent_input(&self, agent_id: &str, input: AttachInbound) -> Result<()> {
        let (repo, interactive, terminal) = {
            let snap = self.inner.snapshot.read().await;
            let (repo, agent) = snap
                .find_agent(agent_id)
                .ok_or_else(|| CoreError::AgentNotFound(agent_id.to_string()))?;
            (
                repo.to_string(),
                agent.interactive,
                agent.status.is_terminal(),
            )
        };
        if terminal {
            return Err(CoreError::InvalidState {
                op: "send_input".into(),
                id: agent_id.to_string(),
                status: "terminal".into(),
            });
        }
        if !interactive {
            return Err(CoreError::InvalidState {
                op: "send_input".into(),
                id: agent_id.to_string(),
                status: "not interactive".into(),
            });
        }
        let client = self.client_for(&repo).await?;
        let socket = client.attach(agent_id).await?;
        CalibandClient::send_inbound(&socket, &input).await
    }

    /// Respawn an agent; returns the new id.
    pub async fn respawn_agent(&self, agent_id: &str) -> Result<String> {
        let repo = self.repo_of(agent_id).await?;
        self.client_for(&repo).await?.respawn(agent_id).await
    }

    /// Remove an agent from caliban's registry.
    pub async fn rm_agent(&self, agent_id: &str, force: bool) -> Result<()> {
        let repo = self.repo_of(agent_id).await?;
        self.client_for(&repo).await?.rm(agent_id, force).await
    }

    async fn repo_of(&self, agent_id: &str) -> Result<String> {
        self.inner
            .snapshot
            .read()
            .await
            .find_agent(agent_id)
            .map(|(repo, _)| repo.to_string())
            .ok_or_else(|| CoreError::AgentNotFound(agent_id.to_string()))
    }

    /// Poll every registered repo once.
    pub async fn poll_all_once(&self) {
        let names: Vec<String> = {
            let reg = self.inner.registry.read().await;
            reg.repos.iter().map(|r| r.name.clone()).collect()
        };
        for name in names {
            self.poll_repo_once(&name).await;
        }
    }

    /// Poll one repo: list agents, reconcile against the snapshot, emit diffs,
    /// and start attach tasks for newly-active agents. Failures degrade the
    /// repo to `Unreachable` rather than propagating.
    pub async fn poll_repo_once(&self, repo: &str) {
        self.inner.emitter.metrics.record_repo_poll();
        let client = match self.client_for(repo).await {
            Ok(c) => c,
            Err(e) => {
                self.mark_unreachable(repo, e.to_string()).await;
                return;
            }
        };
        match client.list().await {
            Ok(records) => self.reconcile(repo, records, client).await,
            Err(e) => {
                // A failed list usually means the socket died; drop the cached
                // client so the next poll re-discovers.
                self.inner.clients.lock().unwrap().remove(repo);
                self.mark_unreachable(repo, e.to_string()).await;
            }
        }
    }

    async fn mark_unreachable(&self, repo: &str, reason: String) {
        let mut snap = self.inner.snapshot.write().await;
        if let Some(r) = snap.repos.iter_mut().find(|r| r.name == repo) {
            let new_health = RepoHealth::Unreachable {
                reason: reason.clone(),
            };
            if r.health != new_health {
                r.health = new_health.clone();
                drop(snap);
                self.inner
                    .emitter
                    .emit(repo, "", EventKind::RepoHealth { state: new_health })
                    .await;
            }
        }
    }

    async fn reconcile(&self, repo: &str, records: Vec<AgentRecord>, client: CalibandClient) {
        // Snapshot prior agent statuses for diffing.
        let prior: HashMap<String, AgentStatus> = {
            let snap = self.inner.snapshot.read().await;
            snap.repos
                .iter()
                .find(|r| r.name == repo)
                .map(|r| r.agents.iter().map(|a| (a.id.clone(), a.status)).collect())
                .unwrap_or_default()
        };

        let mut new_agents = Vec::new();
        let mut to_attach: Vec<String> = Vec::new();
        let attached_now = self.inner.attached.lock().unwrap().clone();

        for rec in &records {
            let agent = Agent {
                id: rec.id.clone(),
                name: rec.name.clone(),
                repo: repo.to_string(),
                status: rec.status,
                started_at: rec.started_at.clone(),
                isolated: rec.spec.isolation_worktree,
                interactive: rec.spec.interactive,
                session_dir: rec.session_dir.clone(),
            };
            match prior.get(&rec.id) {
                // New to the snapshot. Suppress "discovered" for agents we just
                // spawned (already attached + emitted AgentSpawned).
                None if !attached_now.contains(&rec.id) => {
                    self.inner
                        .emitter
                        .emit(repo, &rec.id, EventKind::AgentDiscovered)
                        .await;
                }
                None => {}
                Some(&old) if old != rec.status => {
                    self.inner
                        .emitter
                        .emit(
                            repo,
                            &rec.id,
                            EventKind::StatusChanged {
                                from: old,
                                to: rec.status,
                            },
                        )
                        .await;
                }
                _ => {}
            }
            if rec.status.is_active() && !attached_now.contains(&rec.id) {
                to_attach.push(rec.id.clone());
            }
            new_agents.push(agent);
        }

        // Agents that disappeared from caliban's registry.
        for (old_id, _) in prior.iter() {
            if !records.iter().any(|r| &r.id == old_id) {
                self.inner
                    .emitter
                    .emit(repo, old_id, EventKind::AgentGone)
                    .await;
            }
        }

        {
            let mut snap = self.inner.snapshot.write().await;
            if let Some(r) = snap.repos.iter_mut().find(|r| r.name == repo) {
                let was_unreachable = matches!(r.health, RepoHealth::Unreachable { .. });
                r.health = RepoHealth::Healthy;
                r.agents = new_agents;
                if was_unreachable {
                    drop(snap);
                    self.inner
                        .emitter
                        .emit(
                            repo,
                            "",
                            EventKind::RepoHealth {
                                state: RepoHealth::Healthy,
                            },
                        )
                        .await;
                }
            }
        }

        for id in to_attach {
            self.start_attach(repo, &id, client.clone()).await;
        }
    }

    /// Start a per-agent attach task if one is not already running. The task
    /// reads the agent's stream, normalizes frames into events, and exits when
    /// the stream closes.
    async fn start_attach(&self, repo: &str, agent_id: &str, client: CalibandClient) {
        // Already driving this agent locally? Its attach task holds (and, in
        // clustered mode, heartbeats) the lease — leave it untouched.
        if self.inner.attached.lock().unwrap().contains(agent_id) {
            return;
        }
        // Claim the stream. Standalone always acquires; clustered consults the
        // Postgres lease and returns `None` if another live replica owns it.
        // `try_acquire` is idempotent for a stream THIS process already holds.
        if self.inner.ownership.try_acquire(agent_id).await.is_none() {
            return;
        }
        {
            let mut attached = self.inner.attached.lock().unwrap();
            if !attached.insert(agent_id.to_string()) {
                // Lost a race to another start_attach for the same agent. It now
                // owns the (idempotently-shared) lease and will release it on
                // exit — we must NOT release here or we would orphan its writer.
                return;
            }
        }
        let repo = repo.to_string();
        let agent_id = agent_id.to_string();
        let emitter = self.inner.emitter.clone();
        let normalize = self.inner.config.normalize;
        let backoff = self.inner.config.attach_backoff;
        let mut shutdown = self.inner.shutdown.subscribe();
        let attached = self.inner.clone();

        tokio::spawn(async move {
            let result = attach_loop(
                &client,
                &repo,
                &agent_id,
                &emitter,
                normalize,
                backoff,
                &mut shutdown,
            )
            .await;
            if let Err(e) = result {
                tracing::warn!(
                    target: "prospero_fleet",
                    %repo, %agent_id, error = %e,
                    "attach task ended with error"
                );
            }
            attached.attached.lock().unwrap().remove(&agent_id);
            // Release for prompt failover hand-off (clustered); no-op standalone.
            attached.ownership.release(&agent_id).await;
        });
    }

    /// Names of repos with a cached control client (test/observability helper).
    pub async fn cached_client_names(&self) -> Vec<String> {
        self.inner.clients.lock().unwrap().keys().cloned().collect()
    }

    /// Whether a per-agent attach task is currently registered (test/obs helper).
    pub fn is_attached(&self, agent_id: &str) -> bool {
        self.inner.attached.lock().unwrap().contains(agent_id)
    }

    /// Gracefully shut down a repo's caliband daemon and drop its cached client
    /// so the next access re-runs discovery (respawning with the current env).
    pub async fn restart_caliband(&self, repo: &str) -> Result<()> {
        let client = self.inner.clients.lock().unwrap().get(repo).cloned();
        if let Some(client) = client {
            let res = client.shutdown().await;
            if let Err(e) = res {
                tracing::warn!(target: "prospero_fleet", repo, error = %e,
                    "shutdown request to caliband failed (continuing)");
            }
        }
        self.inner.clients.lock().unwrap().remove(repo);

        let root = {
            let reg = self.inner.registry.read().await;
            reg.get(repo).map(|r| r.root.clone())
        };
        if let Some(root) = root {
            let socket_res =
                crate::discovery::resolve_socket(&root, &self.inner.config.discovery_env);
            if let Ok(socket) = socket_res {
                // Reuse startup_timeout as the upper bound for the daemon to
                // release its socket after Shutdown (a symmetric drain bound).
                let deadline =
                    tokio::time::Instant::now() + self.inner.config.ensure.startup_timeout;
                while tokio::net::UnixStream::connect(&socket).await.is_ok() {
                    if tokio::time::Instant::now() >= deadline {
                        tracing::warn!(target: "prospero_fleet", repo,
                            "old caliband socket still reachable after shutdown; proceeding");
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
        self.poll_repo_once(repo).await;
        Ok(())
    }

    /// Persist a repo's provider config and restart its caliband to apply it.
    pub async fn set_repo_config(
        &self,
        repo: &str,
        config: crate::registry::RepoProviderConfig,
    ) -> Result<()> {
        self.set_repo_config_registry_only(repo, config).await?;
        self.restart_caliband(repo).await
    }

    /// Run the background poll loop until [`Self::begin_shutdown`] is signalled.
    ///
    /// Each iteration runs a *complete* poll cycle (never abandoned mid-append),
    /// then waits the interval. A shutdown signal stops scheduling new polls and
    /// returns after the in-flight cycle finishes — so the daemon can drain
    /// cleanly rather than being killed mid-iteration.
    pub async fn run(self) {
        let interval = self.inner.config.poll_interval;
        let mut shutdown = self.inner.shutdown.subscribe();
        if *shutdown.borrow_and_update() {
            return;
        }
        loop {
            self.poll_all_once().await;
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = shutdown.changed() => break,
            }
        }
        tracing::info!(target: "prospero_fleet", "poll loop drained on shutdown");
    }
}

/// How a single attach connection ended.
enum StreamOutcome {
    /// The agent's terminal `result` frame was seen — the run is done; exit.
    Finished,
    /// EOF arrived before any terminal frame — a premature drop; reconnect.
    Disconnected,
}

/// Attach to an agent's stream and emit its events, **reconnecting with bounded
/// backoff on a premature drop** so transient socket failures don't lose or
/// duplicate events.
///
/// `frames_seen` is a high-water mark over the raw non-empty stream lines: on
/// reconnect caliban replays the stream from the start, so we skip the prefix
/// we already processed and emit only new frames — no duplicates in the live
/// bus or the durable log, and nothing emitted in the gap window is lost (the
/// replay carries it). A clean finish (terminal `result` → `AgentFinished`)
/// exits without retrying; a drop or read error backs off and reconnects until
/// the budget is spent, after which the poll loop remains the re-attach net.
async fn attach_loop(
    client: &CalibandClient,
    repo: &str,
    agent_id: &str,
    emitter: &Emitter,
    normalize: NormalizeOptions,
    backoff: AttachBackoff,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let mut frames_seen: u64 = 0;
    let mut attempt: u32 = 0;
    loop {
        let before = frames_seen;
        let err = match attach_once(
            client,
            repo,
            agent_id,
            emitter,
            normalize,
            &mut frames_seen,
            shutdown,
        )
        .await
        {
            Ok(StreamOutcome::Finished) => return Ok(()),
            Ok(StreamOutcome::Disconnected) => None,
            Err(e) => Some(e),
        };
        // A shutdown was signalled while attached — stop reconnecting and drain.
        if *shutdown.borrow() {
            return Ok(());
        }
        // Progress on this connection resets the backoff window.
        if frames_seen > before {
            attempt = 0;
        }
        if attempt >= backoff.max_retries {
            return match err {
                Some(e) => Err(e),
                None => {
                    tracing::warn!(
                        target: "prospero_fleet", %repo, %agent_id,
                        "attach reconnection budget exhausted; poll loop will re-attach"
                    );
                    Ok(())
                }
            };
        }
        let delay = backoff.delay_for(agent_id, attempt);
        tracing::warn!(
            target: "prospero_fleet", %repo, %agent_id, attempt,
            delay_ms = delay.as_millis() as u64,
            reason = if err.is_some() { "error" } else { "premature-eof" },
            "attach stream dropped; reconnecting after backoff"
        );
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown.changed() => return Ok(()),
        }
        attempt += 1;
    }
}

/// Read one attach connection to its end, emitting only frames past
/// `frames_seen` and advancing it. Returns how the connection ended. A shutdown
/// signal stops reading between frames (after any in-flight emit/append), so no
/// event is left half-persisted.
async fn attach_once(
    client: &CalibandClient,
    repo: &str,
    agent_id: &str,
    emitter: &Emitter,
    normalize: NormalizeOptions,
    frames_seen: &mut u64,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<StreamOutcome> {
    let socket = client.attach(agent_id).await?;
    let mut reader = CalibandClient::open_stream(&socket).await?;
    let mut line = String::new();
    let mut idx: u64 = 0;
    let mut saw_terminal = false;
    loop {
        line.clear();
        let n = tokio::select! {
            r = reader.read_line(&mut line) => r?,
            _ = shutdown.changed() => {
                // Drain: stop reading between frames; the run is being torn down.
                return Ok(StreamOutcome::Finished);
            }
        };
        if n == 0 {
            return Ok(if saw_terminal {
                StreamOutcome::Finished
            } else {
                StreamOutcome::Disconnected
            });
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }
        idx += 1;
        // Skip the prefix already processed before a reconnect (dedup).
        if idx <= *frames_seen {
            continue;
        }
        *frames_seen = idx;
        let frame: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => {
                tracing::warn!(target: "prospero_fleet", %agent_id, "unparseable stream line");
                continue;
            }
        };
        match normalize_frame(&frame, normalize) {
            Normalized::Event(kind) => {
                if matches!(kind, EventKind::AgentFinished { .. }) {
                    saw_terminal = true;
                }
                emitter.emit(repo, agent_id, kind).await;
            }
            Normalized::Dropped => {}
            Normalized::Unknown => {
                emitter.metrics.record_unknown_frame();
                tracing::debug!(target: "prospero_fleet", %agent_id, "unknown caliban frame type");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attach_backoff_is_exponential_capped_and_jittered() {
        let b = AttachBackoff {
            base: Duration::from_millis(100),
            max: Duration::from_millis(800),
            max_retries: 8,
        };
        // Each delay sits in [50%, 100%) of the exponential value, capped at max.
        for (attempt, exp_ms) in [(0u32, 100u64), (1, 200), (2, 400), (3, 800)] {
            let d = b.delay_for("agent-a", attempt).as_millis() as u64;
            assert!(
                d >= exp_ms / 2 && d <= exp_ms,
                "attempt {attempt}: {d}ms outside [{}, {exp_ms}]",
                exp_ms / 2
            );
        }
        // Beyond the cap, delays never exceed `max`.
        let capped = b.delay_for("agent-a", 20).as_millis() as u64;
        assert!((400..=800).contains(&capped), "capped delay {capped}ms");
        // Jitter is deterministic per (agent, attempt) — stable across calls.
        assert_eq!(
            b.delay_for("agent-a", 2).as_millis(),
            b.delay_for("agent-a", 2).as_millis()
        );
    }

    #[tokio::test]
    async fn restart_caliband_shuts_down_and_clears_client() {
        use crate::registry::RepoProviderConfig;
        use crate::testkit::FakeCaliband;

        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
        config.ensure.autostart = false; // no real caliband to spawn in tests
        let root = dir.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        let socket = crate::discovery::resolve_socket(&root, &config.discovery_env).unwrap();

        let fake = FakeCaliband::start_at(&socket).await.unwrap();
        let store = std::sync::Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).await.unwrap();
        mgr.add_repo("p", &root).await.unwrap();

        mgr.poll_repo_once("p").await; // cache a client by talking to the repo

        mgr.set_repo_config("p", RepoProviderConfig::default())
            .await
            .unwrap();

        assert_eq!(fake.shutdowns(), 1, "restart should send one Shutdown");
        assert!(
            mgr.cached_client_names().await.iter().all(|n| n != "p"),
            "cached client for the repo should be cleared after restart"
        );
    }

    #[tokio::test]
    async fn send_agent_input_rejects_terminal_unknown_and_non_interactive() {
        use crate::caliband::wire::AttachInbound;
        use crate::model::AgentStatus;
        use crate::testkit::{FakeCaliband, test_record};

        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
        config.ensure.autostart = false;
        let root = dir.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        let socket = crate::discovery::resolve_socket(&root, &config.discovery_env).unwrap();

        let mut fake = FakeCaliband::start_at(&socket).await.unwrap();
        // Terminal agent (Done), even though interactive → reject as terminal.
        let mut done = test_record("ag-done", dir.path(), AgentStatus::Done, false);
        done.spec.interactive = true;
        fake.add_agent(done, vec![]).await;
        // Idle but NOT interactive → reject.
        let idle = test_record("ag-idle", dir.path(), AgentStatus::Idle, false);
        fake.add_agent(idle, vec![]).await;

        let store = std::sync::Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).await.unwrap();
        mgr.add_repo("repo", &root).await.unwrap();
        mgr.poll_repo_once("repo").await;

        let r1 = mgr
            .send_agent_input("ag-done", AttachInbound::EndInput)
            .await;
        assert!(
            matches!(r1, Err(CoreError::InvalidState { .. })),
            "terminal must reject"
        );
        let r2 = mgr
            .send_agent_input("ag-idle", AttachInbound::EndInput)
            .await;
        assert!(
            matches!(r2, Err(CoreError::InvalidState { .. })),
            "non-interactive must reject"
        );
        let r3 = mgr.send_agent_input("nope", AttachInbound::EndInput).await;
        assert!(
            matches!(r3, Err(CoreError::AgentNotFound(_))),
            "unknown id must 404"
        );
    }

    #[tokio::test]
    async fn ownership_gates_the_attach_path() {
        // A FleetManager built with SelfOwnsAll attaches normally: spawning an
        // agent records it in the attached set (ownership never refuses).
        use crate::testkit::FakeCaliband;

        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
        config.ensure.autostart = false;
        let root = dir.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        let socket = crate::discovery::resolve_socket(&root, &config.discovery_env).unwrap();
        let _fake = FakeCaliband::start_at(&socket).await.unwrap();

        let store = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).await.unwrap();
        mgr.add_repo("p", &root).await.unwrap();

        let id = mgr.spawn_agent("p", SpawnRequest::new("hi")).await.unwrap();
        // is_attached is set synchronously in start_attach before tokio::spawn,
        // so the check needs no delay.
        assert!(mgr.is_attached(&id), "owned agent must be attached");
    }

    #[tokio::test]
    async fn refused_ownership_blocks_the_attach_path() {
        use crate::bus::InProcessBus;
        use crate::ownership::{Lease, Ownership};
        use crate::testkit::FakeCaliband;
        use async_trait::async_trait;

        // Ownership that never grants a lease (simulates a peer-owned stream).
        struct NeverOwns;
        #[async_trait]
        impl Ownership for NeverOwns {
            async fn try_acquire(&self, _: &str) -> Option<Lease> {
                None
            }
            async fn renew(&self, _: &Lease) -> crate::error::Result<()> {
                Ok(())
            }
            async fn release(&self, _: &str) {}
            fn owns(&self, _: &str) -> bool {
                false
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
        config.ensure.autostart = false;
        let root = dir.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        let socket = crate::discovery::resolve_socket(&root, &config.discovery_env).unwrap();
        let _fake = FakeCaliband::start_at(&socket).await.unwrap();

        let store: Arc<dyn Store> = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let config_store: Arc<dyn ConfigStore> = Arc::new(
            crate::config_store::SqliteConfigStore::open(dir.path())
                .await
                .unwrap(),
        );
        let bus: Arc<dyn EventBus> = Arc::new(InProcessBus::new(config.event_buffer));
        let ownership: Arc<dyn Ownership> = Arc::new(NeverOwns);
        let mgr = FleetManager::with_seams(config, store, config_store, bus, ownership)
            .await
            .unwrap();
        mgr.add_repo("p", &root).await.unwrap();

        let id = mgr.spawn_agent("p", SpawnRequest::new("hi")).await.unwrap();
        assert!(
            !mgr.is_attached(&id),
            "peer-owned agent must NOT be attached locally"
        );
    }

    #[tokio::test]
    async fn spawn_passes_repo_provider_into_spawnspec() {
        use crate::registry::RepoProviderConfig;
        use crate::testkit::FakeCaliband;

        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
        config.ensure.autostart = false; // no real caliband to spawn in tests
        let root = dir.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        let socket = crate::discovery::resolve_socket(&root, &config.discovery_env).unwrap();

        let fake = FakeCaliband::start_at(&socket).await.unwrap();
        let store = std::sync::Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).await.unwrap();
        mgr.add_repo("p", &root).await.unwrap();
        mgr.set_repo_config_registry_only(
            "p",
            RepoProviderConfig {
                provider: Some("ollama".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        mgr.spawn_agent("p", SpawnRequest::new("hi")).await.unwrap();

        let specs = fake.received_specs();
        assert_eq!(specs.len(), 1, "exactly one spawn reached caliband");
        assert_eq!(
            specs[0].provider.as_deref(),
            Some("ollama"),
            "the repo's configured provider must be carried in SpawnSpec.provider (#93)"
        );
    }

    #[tokio::test]
    async fn ensure_config_for_merges_default_and_repo_config() {
        use crate::registry::RepoProviderConfig;
        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.default_env.insert("KEEP".into(), "global".into());
        let store = std::sync::Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).await.unwrap();

        mgr.add_repo("p", "/tmp/p").await.ok(); // discovery may fail; the registry write is what matters
        let cfg = RepoProviderConfig {
            provider: Some("ollama".into()),
            base_url: Some("http://h:11434".into()),
            env: [("EXTRA".to_string(), "1".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        mgr.set_repo_config_registry_only("p", cfg).await.unwrap();

        let ec = mgr.ensure_config_for("p").await.unwrap();
        assert_eq!(ec.env.get("KEEP").unwrap(), "global");
        assert_eq!(ec.env.get("CALIBAN_PROVIDER").unwrap(), "ollama");
        assert_eq!(ec.env.get("OLLAMA_BASE_URL").unwrap(), "http://h:11434");
        assert_eq!(ec.env.get("EXTRA").unwrap(), "1");
    }

    /// A `Store` that fails `append` for a configured set of seqs and otherwise
    /// delegates to a real `JsonlStore` — lets a test inject a persist failure
    /// for one event while letting the gap marker through.
    struct FlakyStore {
        inner: crate::store::JsonlStore,
        fail_seqs: std::sync::Mutex<std::collections::HashSet<u64>>,
    }

    impl FlakyStore {
        fn new(inner: crate::store::JsonlStore, fail: impl IntoIterator<Item = u64>) -> Self {
            Self {
                inner,
                fail_seqs: std::sync::Mutex::new(fail.into_iter().collect()),
            }
        }
    }

    #[async_trait::async_trait]
    impl Store for FlakyStore {
        async fn append(&self, event: &FleetEvent) -> Result<()> {
            if self.fail_seqs.lock().unwrap().contains(&event.seq) {
                return Err(CoreError::Store("injected append failure".into()));
            }
            self.inner.append(event).await
        }
        async fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
            self.inner.replay(stream_key, from_seq).await
        }
        async fn high_water(&self, stream_key: &str) -> Result<u64> {
            self.inner.high_water(stream_key).await
        }
        async fn writable(&self) -> bool {
            self.inner.writable().await
        }
        async fn prune(&self, before_ts: &str) -> Result<u64> {
            self.inner.prune(before_ts).await
        }
    }

    fn emitter_with(store: Arc<dyn Store>) -> Emitter {
        Emitter {
            store,
            bus: Arc::new(InProcessBus::new(16)),
            seqs: Arc::new(AsyncMutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::default()),
        }
    }

    fn ev(seq: u64, agent: &str, chunk: &str) -> FleetEvent {
        use crate::event::OutputStream;
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

    #[tokio::test]
    async fn seq_is_monotonic_per_stream_not_global() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let emitter = emitter_with(store);

        // Interleave two agents; each stream numbers from 1 independently.
        emitter.emit("r", "a1", EventKind::AgentSpawned).await;
        emitter.emit("r", "a2", EventKind::AgentSpawned).await;
        emitter.emit("r", "a1", EventKind::AgentGone).await;

        let a1 = emitter
            .store
            .replay(&crate::event::stream_key_for("r", "a1"), 0)
            .await
            .unwrap();
        let a2 = emitter
            .store
            .replay(&crate::event::stream_key_for("r", "a2"), 0)
            .await
            .unwrap();
        assert_eq!(a1.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(a2.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1]);
    }

    #[tokio::test]
    async fn seq_resumes_per_stream_from_high_water() {
        let dir = tempfile::tempdir().unwrap();
        // Pre-seed the store: stream "a1" already reached seq 5.
        {
            let store = crate::store::JsonlStore::open(dir.path()).unwrap();
            store.append(&ev(5, "a1", "old")).await.unwrap();
        }
        let store = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let emitter = emitter_with(store);
        emitter.emit("r", "a1", EventKind::AgentGone).await;

        let a1 = emitter
            .store
            .replay(&crate::event::stream_key_for("r", "a1"), 0)
            .await
            .unwrap();
        // The new event continues from the stored high-water (5 → 6).
        assert_eq!(a1.last().unwrap().seq, 6);
    }

    #[tokio::test]
    async fn append_failure_emits_persist_gap_marker_visible_to_history() {
        use crate::event::OutputStream;

        let dir = tempfile::tempdir().unwrap();
        let inner = crate::store::JsonlStore::open(dir.path()).unwrap();
        let store = Arc::new(FlakyStore::new(inner, [1])); // fail the data event (seq 1)
        let emitter = emitter_with(store.clone());
        use tokio_stream::StreamExt;
        let mut sub = emitter
            .bus
            .subscribe(&crate::event::stream_key_for("repo", "a1"));

        emitter
            .emit(
                "repo",
                "a1",
                EventKind::Output {
                    stream: OutputStream::Stdout,
                    chunk: "lost".into(),
                },
            )
            .await;

        // Live SSE still flows (ADR-0004): the original event reaches the bus...
        let ev = match sub.next().await {
            Some(crate::bus::BusEvent::Event(ev)) => ev,
            other => panic!("expected a live event, got {other:?}"),
        };
        assert_eq!(ev.seq, 1);
        assert!(matches!(ev.kind, EventKind::Output { .. }));
        // ...immediately followed by a durable-gap marker naming the lost seq.
        let marker = match sub.next().await {
            Some(crate::bus::BusEvent::Event(ev)) => ev,
            other => panic!("expected a live event, got {other:?}"),
        };
        assert_eq!(marker.agent_id, "a1");
        assert!(matches!(
            marker.kind,
            EventKind::StorePersistFailed { lost_seq: 1, .. }
        ));

        // The marker is visible to a history reader (persisted), not just logs,
        // and the lost event itself is absent — the gap is real but now labeled.
        let history = store.replay("a1", 0).await.unwrap();
        assert!(
            history
                .iter()
                .any(|e| matches!(e.kind, EventKind::StorePersistFailed { lost_seq: 1, .. })),
            "history reader must see the persist-gap marker"
        );
        assert!(
            !history.iter().any(|e| e.seq == 1),
            "the un-persisted event must not appear in durable history"
        );
    }

    #[tokio::test]
    async fn healthy_append_emits_no_gap_marker() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let emitter = emitter_with(store);
        use tokio_stream::StreamExt;
        let mut sub = emitter
            .bus
            .subscribe(&crate::event::stream_key_for("repo", "a1"));

        emitter.emit("repo", "a1", EventKind::AgentSpawned).await;

        let ev = match sub.next().await {
            Some(crate::bus::BusEvent::Event(ev)) => ev,
            other => panic!("expected a live event, got {other:?}"),
        };
        assert!(matches!(ev.kind, EventKind::AgentSpawned));
        // A healthy append must not emit a gap marker: nothing more arrives.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), sub.next())
                .await
                .is_err(),
            "a healthy append must not emit a gap marker"
        );
    }

    #[tokio::test]
    async fn append_failure_and_success_advance_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let inner = crate::store::JsonlStore::open(dir.path()).unwrap();
        // Fail the data event (seq 1); the gap marker (seq 2) appends fine.
        let store = Arc::new(FlakyStore::new(inner, [1]));
        let emitter = emitter_with(store);

        emitter.emit("repo", "a1", EventKind::AgentSpawned).await;

        let m = emitter.metrics.snapshot(0);
        assert_eq!(m.append_failures, 1, "the failed append must be counted");
        assert_eq!(
            m.events_appended, 1,
            "the successful gap-marker append must be counted"
        );
    }

    #[tokio::test]
    async fn run_drains_and_returns_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.poll_interval = Duration::from_millis(50);
        let store = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).await.unwrap();

        let signaller = mgr.clone();
        let handle = tokio::spawn(mgr.run());
        signaller.begin_shutdown();

        // run() must drain the in-flight poll and return promptly on the signal,
        // rather than looping forever.
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("run() must return after begin_shutdown")
            .expect("run task panicked");
    }

    #[tokio::test]
    async fn prune_older_than_removes_aged_events() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        store
            .append(&FleetEvent {
                seq: 1,
                ts: "2000-01-01T00:00:00+00:00".into(),
                repo: "r".into(),
                agent_id: "a".into(),
                kind: EventKind::AgentSpawned,
            })
            .await
            .unwrap();
        store
            .append(&FleetEvent {
                seq: 2,
                ts: chrono::Utc::now().to_rfc3339(),
                repo: "r".into(),
                agent_id: "a".into(),
                kind: EventKind::AgentGone,
            })
            .await
            .unwrap();

        let config = FleetConfig::new("local", dir.path());
        let mgr = FleetManager::new(config, store).await.unwrap();

        let removed = mgr
            .prune_older_than(std::time::Duration::from_secs(24 * 3600))
            .await
            .unwrap();
        assert_eq!(removed, 1);
        assert_eq!(mgr.history("a", 0).await.unwrap().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_seed_of_same_stream_assigns_unique_seqs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let emitter = emitter_with(store);
        // Two tasks race the slow-path seed for the same new stream "a1".
        let e1 = emitter.clone();
        let e2 = emitter.clone();
        let t1 = tokio::spawn(async move { e1.emit("r", "a1", EventKind::AgentSpawned).await });
        let t2 = tokio::spawn(async move { e2.emit("r", "a1", EventKind::AgentGone).await });
        t1.await.unwrap();
        t2.await.unwrap();
        let a1 = emitter
            .store
            .replay(&crate::event::stream_key_for("r", "a1"), 0)
            .await
            .unwrap();
        let seqs: Vec<u64> = a1.iter().map(|e| e.seq).collect();
        assert_eq!(seqs.len(), 2, "both events persisted");
        assert_ne!(seqs[0], seqs[1], "no duplicate seq within a stream");
    }
}
