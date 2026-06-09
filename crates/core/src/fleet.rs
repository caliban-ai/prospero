//! The runtime heart of the control plane.
//!
//! `FleetManager` owns the in-memory [`FleetSnapshot`], polls each managed
//! repo's caliband for live state, attaches per-agent stream sockets while
//! agents are active, normalizes frames into [`FleetEvent`]s, and fans them out
//! over a broadcast bus while also appending them to the durable [`Store`].

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::AsyncBufReadExt;
use tokio::sync::{RwLock, broadcast};

use crate::caliband::client::CalibandClient;
use crate::caliband::stream::{NormalizeOptions, Normalized, normalize_frame};
use crate::caliband::wire::{AgentRecord, SpawnSpec};
use crate::discovery::{DiscoveryEnv, EnsureConfig, ensure_caliband};
use crate::error::{CoreError, Result};
use crate::event::{EventKind, FleetEvent};
use crate::model::{Agent, AgentStatus, FleetSnapshot, Repo, RepoHealth};
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
        }
    }

    fn into_spec(self) -> SpawnSpec {
        SpawnSpec {
            label: self.label,
            frontmatter_path: None,
            initial_prompt: self.prompt,
            model: self.model,
            tool_allowlist: self.tool_allowlist,
            isolation_worktree: self.isolation_worktree,
            inherit_hooks: true,
        }
    }
}

/// Configuration for a [`FleetManager`].
#[derive(Debug, Clone)]
pub struct FleetConfig {
    /// Host identity (single host in the first stab).
    pub host: String,
    /// Directory for the registry file and event store.
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
        }
    }

    fn registry_path(&self) -> PathBuf {
        self.data_dir.join("registry.json")
    }
}

/// Stamps and dispatches events; cheaply cloneable into background tasks.
#[derive(Clone)]
struct Emitter {
    store: Arc<dyn Store>,
    bus: broadcast::Sender<FleetEvent>,
    seq: Arc<AtomicU64>,
}

impl Emitter {
    fn emit(&self, repo: &str, agent_id: &str, kind: EventKind) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let event = FleetEvent {
            seq,
            ts: chrono::Utc::now().to_rfc3339(),
            repo: repo.to_string(),
            agent_id: agent_id.to_string(),
            kind,
        };
        if let Err(e) = self.store.append(&event) {
            tracing::warn!(target: "prospero_fleet", error = %e, "failed to persist event");
        }
        // Ignore send errors: no subscribers is fine.
        let _ = self.bus.send(event);
    }
}

struct Inner {
    config: FleetConfig,
    snapshot: RwLock<FleetSnapshot>,
    registry: RwLock<Registry>,
    /// Per-repo control clients, cached after first discovery.
    clients: Mutex<HashMap<String, CalibandClient>>,
    /// Agent ids with a running attach task.
    attached: Mutex<HashSet<String>>,
    emitter: Emitter,
}

/// The fleet control plane.
#[derive(Clone)]
pub struct FleetManager {
    inner: Arc<Inner>,
}

impl FleetManager {
    /// Build a manager, loading the persisted registry and seeding the event
    /// sequence from the store's high-water mark.
    pub fn new(config: FleetConfig, store: Arc<dyn Store>) -> Result<Self> {
        let registry = Registry::load(&config.registry_path())?;
        let high_water = store.high_water()?;
        let (bus, _) = broadcast::channel(config.event_buffer);
        let emitter = Emitter {
            store,
            bus,
            seq: Arc::new(AtomicU64::new(high_water)),
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
                clients: Mutex::new(HashMap::new()),
                attached: Mutex::new(HashSet::new()),
                emitter,
            }),
        })
    }

    /// Subscribe to the live event bus.
    pub fn subscribe(&self) -> broadcast::Receiver<FleetEvent> {
        self.inner.emitter.bus.subscribe()
    }

    /// A clone of the current fleet snapshot.
    pub async fn snapshot(&self) -> FleetSnapshot {
        self.inner.snapshot.read().await.clone()
    }

    /// Replay an agent's history from the store, with `seq >= from_seq`.
    pub fn history(&self, agent_id: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
        self.inner.emitter.store.replay(agent_id, from_seq)
    }

    /// Register a repo and persist the registry. Triggers an immediate poll.
    pub async fn add_repo(&self, name: impl Into<String>, root: impl Into<PathBuf>) -> Result<()> {
        let name = name.into();
        let root = root.into();
        {
            let mut reg = self.inner.registry.write().await;
            reg.add(name.clone(), root.clone())?;
            reg.save(&self.inner.config.registry_path())?;
        }
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

    /// Unregister a repo and persist the registry.
    pub async fn remove_repo(&self, name: &str) -> Result<bool> {
        let removed = {
            let mut reg = self.inner.registry.write().await;
            let removed = reg.remove(name);
            if removed {
                reg.save(&self.inner.config.registry_path())?;
            }
            removed
        };
        if removed {
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
        let env = crate::provider_env::resolve_env(
            &self.inner.config.default_env,
            &cfg,
            &|k| std::env::var(k).ok(),
        );
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
        let mut reg = self.inner.registry.write().await;
        if !reg.set_config(repo, config) {
            return Err(CoreError::RepoNotFound(repo.to_string()));
        }
        reg.save(&self.inner.config.registry_path())?;
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

    /// Launch a new agent under `repo`. Returns the new agent id.
    pub async fn spawn_agent(&self, repo: &str, req: SpawnRequest) -> Result<String> {
        let client = self.client_for(repo).await?;
        let (id, _socket) = client.spawn(req.into_spec()).await?;
        self.inner.emitter.emit(repo, &id, EventKind::AgentSpawned);
        self.start_attach(repo, &id, client).await;
        Ok(id)
    }

    /// Kill an agent (resolving its repo from the snapshot).
    pub async fn kill_agent(&self, agent_id: &str) -> Result<()> {
        let repo = self.repo_of(agent_id).await?;
        self.client_for(&repo).await?.kill(agent_id).await
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
                    .emit(repo, "", EventKind::RepoHealth { state: new_health });
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
                session_dir: rec.session_dir.clone(),
            };
            match prior.get(&rec.id) {
                // New to the snapshot. Suppress "discovered" for agents we just
                // spawned (already attached + emitted AgentSpawned).
                None if !attached_now.contains(&rec.id) => {
                    self.inner
                        .emitter
                        .emit(repo, &rec.id, EventKind::AgentDiscovered);
                }
                None => {}
                Some(&old) if old != rec.status => {
                    self.inner.emitter.emit(
                        repo,
                        &rec.id,
                        EventKind::StatusChanged {
                            from: old,
                            to: rec.status,
                        },
                    );
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
                self.inner.emitter.emit(repo, old_id, EventKind::AgentGone);
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
                    self.inner.emitter.emit(
                        repo,
                        "",
                        EventKind::RepoHealth {
                            state: RepoHealth::Healthy,
                        },
                    );
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
        {
            let mut attached = self.inner.attached.lock().unwrap();
            if !attached.insert(agent_id.to_string()) {
                return; // already attached
            }
        }
        let repo = repo.to_string();
        let agent_id = agent_id.to_string();
        let emitter = self.inner.emitter.clone();
        let normalize = self.inner.config.normalize;
        let attached = self.inner.clone();

        tokio::spawn(async move {
            let result = attach_loop(&client, &repo, &agent_id, &emitter, normalize).await;
            if let Err(e) = result {
                tracing::warn!(
                    target: "prospero_fleet",
                    %repo, %agent_id, error = %e,
                    "attach task ended with error"
                );
            }
            attached.attached.lock().unwrap().remove(&agent_id);
        });
    }

    /// Run the background poll loop forever (until the task is dropped).
    pub async fn run(self) {
        let interval = self.inner.config.poll_interval;
        loop {
            self.poll_all_once().await;
            tokio::time::sleep(interval).await;
        }
    }
}

async fn attach_loop(
    client: &CalibandClient,
    repo: &str,
    agent_id: &str,
    emitter: &Emitter,
    normalize: NormalizeOptions,
) -> Result<()> {
    let socket = client.attach(agent_id).await?;
    let mut reader = CalibandClient::open_stream(&socket).await?;
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break; // end of stream
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }
        let frame: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => {
                tracing::warn!(target: "prospero_fleet", %agent_id, "unparseable stream line");
                continue;
            }
        };
        match normalize_frame(&frame, normalize) {
            Normalized::Event(kind) => emitter.emit(repo, agent_id, kind),
            Normalized::Dropped => {}
            Normalized::Unknown => {
                tracing::debug!(target: "prospero_fleet", %agent_id, "unknown caliban frame type");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ensure_config_for_merges_default_and_repo_config() {
        use crate::registry::RepoProviderConfig;
        let dir = tempfile::tempdir().unwrap();
        let mut config = FleetConfig::new("local", dir.path());
        config.default_env.insert("KEEP".into(), "global".into());
        let store = std::sync::Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let mgr = FleetManager::new(config, store).unwrap();

        mgr.add_repo("p", "/tmp/p").await.ok(); // discovery may fail; the registry write is what matters
        let cfg = RepoProviderConfig {
            provider: Some("ollama".into()),
            base_url: Some("http://h:11434".into()),
            env: [("EXTRA".to_string(), "1".to_string())].into_iter().collect(),
            ..Default::default()
        };
        mgr.set_repo_config_registry_only("p", cfg).await.unwrap();

        let ec = mgr.ensure_config_for("p").await.unwrap();
        assert_eq!(ec.env.get("KEEP").unwrap(), "global");
        assert_eq!(ec.env.get("CALIBAN_PROVIDER").unwrap(), "ollama");
        assert_eq!(ec.env.get("OLLAMA_BASE_URL").unwrap(), "http://h:11434");
        assert_eq!(ec.env.get("EXTRA").unwrap(), "1");
    }
}
