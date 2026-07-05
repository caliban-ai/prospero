//! `K8sFleet` — a Kubernetes `FleetProvider` backend that drives a fleet via
//! `CalibanTask` custom resources (ADR 0008 §2).
//!
//! The kube CRUD calls are behind the small [`CalibanTaskApi`] seam so
//! `K8sFleet`'s ensure/stop/restart/watch logic is unit-testable against an
//! in-memory fake with **no real cluster**.
//!
//! `watch_fleet` (Task B3) is a **poll-diff over `CalibanTaskApi::list()`**,
//! not a native `kube::runtime::watcher`: it reuses the same seam B2 already
//! built (so `MemTaskApi`/B5's `FakeK8s` cover it with no apiserver) and
//! mirrors how `FleetManager::watch_changes` synthesizes `LocalFleet`'s
//! `watch_fleet` from its own poll→diff cycle (fleet.rs:799). A native
//! `kube::runtime::watcher` (server-side watch, no polling latency) is a
//! plausible future optimization once this ships — not required for
//! correctness, since Kubernetes' own control loop already tolerates
//! poll-based reconciliation on this timescale.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use sha2::{Digest, Sha256};
use tokio_stream::wrappers::ReceiverStream;

use crate::bus::EventBus;
use crate::caliband::client::CalibandClient;
use crate::caliband::stream::NormalizeOptions;
use crate::caliband::transport::TlsClient;
use crate::caliband::wire::Endpoint;
use crate::error::{CoreError, Result};
use crate::fleet::{AttachBackoff, Emitter, attach_loop};
use crate::fleet_provider::FleetProvider;
use crate::k8s::crd::{CalibanTask, CalibanTaskSpec, Source, TaskSpec as CrdTaskSpec, Workspace};
use crate::model::{Agent, AgentHandle, AgentId, AgentStatus, DrainPolicy, FleetChange, TaskSpec};
use crate::store::Store;

/// Deterministic, DNS-1123-safe name for the `CalibanTask` CR backing `spec`.
///
/// Hashes `(repo, prompt, label)` — the fields that make two specs "the same
/// desired agent" for MVP purposes — so `ensure_agent` is idempotent: calling
/// it twice with an equal spec targets the same CR instead of spawning a
/// duplicate. `worktree`/`model`/`tool_allowlist`/`interactive` don't
/// currently affect the CR's built name; document as a known simplification.
#[must_use]
pub fn task_name(spec: &TaskSpec) -> String {
    let mut hasher = Sha256::new();
    hasher.update(spec.workspace.as_bytes());
    hasher.update([0u8]);
    hasher.update(spec.request.prompt.as_bytes());
    hasher.update([0u8]);
    if let Some(label) = &spec.request.label {
        hasher.update(label.as_bytes());
    }
    let digest = hasher.finalize();
    format!("ct-{}", &hex::encode(digest)[..16])
}

/// Derive a fresh CR name for a restart, salted off the old name plus a
/// monotonic nonce so repeated restarts of the same agent each get a distinct
/// name (names are otherwise spec-deterministic via [`task_name`]).
fn restart_name(old_name: &str, nonce: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(old_name.as_bytes());
    hasher.update([0u8]);
    hasher.update(nonce.to_le_bytes());
    let digest = hasher.finalize();
    format!("ct-{}", &hex::encode(digest)[..16])
}

/// Map a `TaskSpec` onto the `CalibanTask` CR that expresses it.
///
/// MVP simplification (documented per the plan): the workspace gets exactly
/// **one** [`Source`] built from `spec.workspace`, checked out at `main` and
/// mounted at `/work/<repo>`. Multi-source workspaces, non-`main` refs, and
/// isolation-from-request mapping are out of scope until a later task widens
/// `TaskSpec`/`SpawnRequest` to carry that information explicitly.
#[must_use]
pub fn build_calibantask(spec: &TaskSpec, name: &str) -> CalibanTask {
    let source = Source {
        name: spec.workspace.clone(),
        repo: spec.workspace.clone(),
        r#ref: "main".to_string(),
        path: format!("/work/{}", spec.workspace),
    };
    let crd_spec = CalibanTaskSpec {
        workspace: Workspace {
            sources: vec![source],
            services: Vec::new(),
        },
        task: CrdTaskSpec {
            prompt: spec.request.prompt.clone(),
            agent_type: None,
        },
        // No isolation knob on `SpawnRequest`/`TaskSpec` yet beyond the
        // worktree bool, which the operator's `IsolationSpec` doesn't model
        // 1:1 — leave unset for MVP (documented simplification).
        isolation: None,
    };
    CalibanTask::new(name, crd_spec)
}

/// If `task` has reached `status.phase == "Running"` with a resolved
/// `calibandEndpoint`, build the `AgentHandle` callers can attach through.
/// Returns `None` while still provisioning (or if the name is somehow unset).
#[must_use]
pub fn handle_from(task: &CalibanTask, repo: String) -> Option<AgentHandle> {
    let status = task.status.as_ref()?;
    if status.phase != "Running" {
        return None;
    }
    let endpoint_addr = status.caliband_endpoint.as_ref()?;
    let name = task.metadata.name.clone()?;
    Some(AgentHandle {
        id: AgentId::from(name),
        workspace: repo,
        endpoint: Endpoint::Tcp {
            addr: endpoint_addr.clone(),
        },
    })
}

/// Map a `CalibanTask`'s `status.phase` string onto Prospero's `AgentStatus`.
///
/// Mirrors the operator's `Phase` enum (`Pending`/`Provisioning`/`Running`/
/// `Draining`/`Completed`/`Failed` — see `caliban-operator/src/crd.rs`)
/// without depending on the operator crate (ADR 0008 §1): `status.phase` is
/// read as a plain `String` (`CalibanTaskStatus::phase`) precisely so a
/// phase this mirror doesn't know about still deserializes, and this
/// function is where that string gets a defensive fallback instead of the
/// deserializer.
///
/// Mapping choices:
/// - `Pending`/`Provisioning` → `Spawning` (not yet attachable).
/// - `Running` → `Running`.
/// - `Draining` → `Idle`: the task is mid-teardown, not accepting new work
///   but not gone yet either; `Idle` ("no compute pending") reads truer than
///   `Running` for a dashboard, and it isn't `is_terminal()` since the CR is
///   still present.
/// - `Completed` → `Done`, `Failed` → `Failed` (direct terminal mapping).
/// - Anything else (an operator phase this mirror doesn't know about yet, or
///   a blank/unset phase) → `Spawning`, the safe "not yet ready" default,
///   logged as a warning rather than silently reported as `Running`.
#[must_use]
pub fn phase_to_status(phase: &str) -> AgentStatus {
    match phase {
        "Pending" | "Provisioning" => AgentStatus::Spawning,
        "Running" => AgentStatus::Running,
        "Draining" => AgentStatus::Idle,
        "Completed" => AgentStatus::Done,
        "Failed" => AgentStatus::Failed,
        other => {
            tracing::warn!(
                target: "prospero_k8s_fleet", phase = other,
                "unrecognized CalibanTask phase; defaulting to AgentStatus::Spawning"
            );
            AgentStatus::Spawning
        }
    }
}

/// Project a `CalibanTask` onto Prospero's `model::Agent` view.
///
/// k8s-side placeholders (documented, not bugs):
/// - `isolated`/`interactive` are always `false` — the CR's `isolation`/
///   `task` fields don't carry either bit today (same MVP simplification
///   `build_calibantask` already documents for the reverse direction).
/// - `session_dir` is always an empty `PathBuf` — a k8s-backed agent has no
///   prosperod-local session directory; `LocalFleet`'s meaning for that
///   field (a path on the daemon's own disk) doesn't apply here.
/// - `repo` prefers the first workspace source's name, falling back to the
///   task's own name if the workspace is somehow sourceless (shouldn't
///   happen for a `K8sFleet`-applied CR, but `list()` can also observe CRs
///   this process didn't create).
/// - `started_at` comes from `metadata.creationTimestamp` (RFC-3339 via
///   `Display`), or `""` if unset (a CR that hasn't round-tripped through
///   the apiserver yet, e.g. straight out of `MemTaskApi` in tests).
#[must_use]
pub fn agent_from_task(task: &CalibanTask) -> Agent {
    let name = task.metadata.name.clone().unwrap_or_default();
    let repo = task
        .spec
        .workspace
        .sources
        .first()
        .map(|s| s.name.clone())
        .unwrap_or_else(|| name.clone());
    let phase = task
        .status
        .as_ref()
        .map(|s| s.phase.as_str())
        .unwrap_or_default();
    let started_at = task
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_string())
        .unwrap_or_default();
    Agent {
        id: name.clone(),
        name,
        workspace: repo,
        status: phase_to_status(phase),
        started_at,
        isolated: false,
        interactive: false,
        session_dir: std::path::PathBuf::new(),
    }
}

/// The kube I/O seam: CRUD over `CalibanTask` custom resources. Abstracted so
/// `K8sFleet`'s control logic can be exercised against an in-memory fake
/// (`MemTaskApi`, below, and its generalization in Task B5's `FakeK8s`)
/// without a real apiserver.
#[async_trait]
pub trait CalibanTaskApi: Send + Sync {
    /// Server-side-apply `ct` (create-or-update, keyed by its name).
    async fn apply(&self, ct: &CalibanTask) -> Result<()>;
    /// Fetch a `CalibanTask` by name, or `None` if it doesn't exist.
    async fn get(&self, name: &str) -> Result<Option<CalibanTask>>;
    /// Delete a `CalibanTask` by name. Idempotent: deleting an already-gone
    /// name is `Ok(())`, not an error.
    async fn delete(&self, name: &str) -> Result<()>;
    /// List all `CalibanTask`s this API is scoped to (its namespace).
    async fn list(&self) -> Result<Vec<CalibanTask>>;
}

/// Real `CalibanTaskApi` backed by a `kube::Api<CalibanTask>`.
#[cfg(feature = "k8s")]
pub struct KubeTaskApi {
    api: kube::Api<CalibanTask>,
}

#[cfg(feature = "k8s")]
impl KubeTaskApi {
    /// A `CalibanTaskApi` scoped to `namespace` on `client`.
    #[must_use]
    pub fn new(client: kube::Client, namespace: &str) -> Self {
        Self {
            api: kube::Api::namespaced(client, namespace),
        }
    }
}

#[cfg(feature = "k8s")]
fn map_kube_err(op: &str, e: kube::Error) -> CoreError {
    CoreError::Fleet(format!("{op}: {e}"))
}

#[cfg(feature = "k8s")]
#[async_trait]
impl CalibanTaskApi for KubeTaskApi {
    async fn apply(&self, ct: &CalibanTask) -> Result<()> {
        let name = ct
            .metadata
            .name
            .as_deref()
            .ok_or_else(|| CoreError::Fleet("CalibanTask missing metadata.name".to_string()))?;
        let params = kube::api::PatchParams::apply("prospero").force();
        self.api
            .patch(name, &params, &kube::api::Patch::Apply(ct))
            .await
            .map_err(|e| map_kube_err("apply CalibanTask", e))?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Option<CalibanTask>> {
        self.api
            .get_opt(name)
            .await
            .map_err(|e| map_kube_err("get CalibanTask", e))
    }

    async fn delete(&self, name: &str) -> Result<()> {
        match self
            .api
            .delete(name, &kube::api::DeleteParams::default())
            .await
        {
            Ok(_) => Ok(()),
            // Already gone: idempotent delete, not a failure.
            Err(kube::Error::Api(status)) if status.code == 404 => Ok(()),
            Err(e) => Err(map_kube_err("delete CalibanTask", e)),
        }
    }

    async fn list(&self) -> Result<Vec<CalibanTask>> {
        let list = self
            .api
            .list(&kube::api::ListParams::default())
            .await
            .map_err(|e| map_kube_err("list CalibanTask", e))?;
        Ok(list.items)
    }
}

/// How long `ensure_agent` polls for a `CalibanTask` to become `Running`
/// before giving up, and how often it checks in between.
#[derive(Debug, Clone, Copy)]
pub struct PollConfig {
    /// Total time budget before `ensure_agent` gives up.
    pub deadline: Duration,
    /// Interval between polls.
    pub interval: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            deadline: Duration::from_secs(30),
            interval: Duration::from_millis(250),
        }
    }
}

/// A Kubernetes `FleetProvider` backend: drives a fleet by CRUD + watch on
/// `CalibanTask` custom resources, and (Task B4, ADR 0008 §3) bridges each
/// Running agent's live session over the network into the same event
/// bus + `Store` the API's SSE/history reads — so `/stream` works for a
/// k8s-backed agent unchanged.
pub struct K8sFleet<A: CalibanTaskApi> {
    // `Arc` (not a bare `A`) so `watch_fleet` can hand a 'static-owned handle
    // to its background poll-diff task without requiring `A: Clone`.
    api: Arc<A>,
    poll: PollConfig,
    restart_nonce: AtomicU64,
    /// How often `watch_fleet`'s background poll-diff loop calls `list()`.
    /// Production default is ~2s; tests override it much shorter (e.g. 20ms)
    /// via [`Self::with_watch_poll_interval`] so change assertions don't wait
    /// on the production cadence.
    watch_poll_interval: Duration,
    /// Session-plane emitter: shares the exact bus/store `FleetManager`'s own
    /// attach loop feeds (`crate::fleet::Emitter`).
    emitter: Emitter,
    /// TLS trust material for dialing each agent's caliband control endpoint,
    /// operator-injected (env/Secret; see [`Self::with_network`]). `None` in
    /// the fake/test plaintext path.
    tls: Option<TlsClient>,
    /// Bearer token presented after the TLS handshake (ADR 0051). `None` in
    /// the fake/test no-auth path.
    token: Option<String>,
    /// CalibanTask names (== the `AgentId` `ensure_agent` hands back) with an
    /// already-running session-plane attach task, guarding
    /// [`Self::start_agent_stream`] against double-starting one when
    /// `ensure_agent` is called again for an already-`Running` agent
    /// (`ensure_agent`'s CR name is spec-deterministic, so this is a real
    /// case, not just theoretical).
    attached: Arc<Mutex<HashSet<String>>>,
}

/// Default cadence for `watch_fleet`'s poll-diff loop.
const DEFAULT_WATCH_POLL_INTERVAL: Duration = Duration::from_secs(2);

impl<A: CalibanTaskApi> K8sFleet<A> {
    /// A `K8sFleet` with the default (~30s) `ensure_agent` poll budget and
    /// the default (~2s) `watch_fleet` poll cadence, feeding session-plane
    /// events into `bus`/`store` (in-process defaults for the fake/test
    /// wiring; production threads through the daemon's real seams — Task
    /// B6).
    #[must_use]
    pub fn new(api: A, bus: Arc<dyn EventBus>, store: Arc<dyn Store>) -> Self {
        Self::with_poll_config(api, PollConfig::default(), bus, store)
    }

    /// A `K8sFleet` with an explicit `ensure_agent` poll budget — tests use a
    /// short deadline so a never-Running CR fails fast instead of hanging
    /// ~30s.
    #[must_use]
    pub fn with_poll_config(
        api: A,
        poll: PollConfig,
        bus: Arc<dyn EventBus>,
        store: Arc<dyn Store>,
    ) -> Self {
        Self {
            api: Arc::new(api),
            poll,
            restart_nonce: AtomicU64::new(0),
            watch_poll_interval: DEFAULT_WATCH_POLL_INTERVAL,
            emitter: Emitter::new(bus, store),
            tls: None,
            token: None,
            attached: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Override `watch_fleet`'s poll cadence. Tests use a short interval
    /// (e.g. 20ms) so diff assertions don't block on the production default.
    #[must_use]
    pub fn with_watch_poll_interval(mut self, interval: Duration) -> Self {
        self.watch_poll_interval = interval;
        self
    }

    /// Configure the TLS trust root + bearer token [`Self::start_agent_stream`]
    /// uses to dial each agent's caliband control endpoint (ADR 0008 §3).
    /// Defaults to `(None, None)` — plaintext/no-auth, the fake/test path.
    #[must_use]
    pub fn with_network(mut self, tls: Option<TlsClient>, token: Option<String>) -> Self {
        self.tls = tls;
        self.token = token;
        self
    }
}

impl<A: CalibanTaskApi + 'static> K8sFleet<A> {
    /// Dial `endpoint` (the agent's pod-caliband **control** endpoint) over
    /// #75's transport, attach to `agent_id`'s per-agent stream, and feed
    /// normalized frames into the same bus + `Store` `FleetManager`'s own
    /// attach loop feeds (ADR 0008 §3) — reusing `crate::fleet::attach_loop`
    /// verbatim rather than a k8s-local duplicate.
    ///
    /// A no-op (idempotent) if a stream for `agent_id` is already running,
    /// and a logged no-op if `endpoint` isn't `Endpoint::Tcp` (a k8s-backed
    /// agent is always network-attached; a Unix endpoint here would mean a
    /// misconfigured `CalibanTaskApi` implementation).
    ///
    /// ## Agent-id simplification (documented, MVP)
    /// `agent_id` is used both as the bus/store stream key — so it must
    /// match the `AgentId` `ensure_agent` returned, for `/stream` to find
    /// these events — **and** as the id sent in the pod caliband's `Attach`
    /// request (`attach_loop` calls `client.attach(agent_id)` internally,
    /// mirroring the Unix path's single-id design). Those only coincide if
    /// the pod caliband registers its (single) agent under this same name;
    /// plausible in production since the operator, having created the
    /// `CalibanTask`, can tell the pod's caliband to use the CR's name as
    /// the agent id. The plan's alternative — discover the real id via
    /// `client.list()`'s first entry when a direct `attach` 404s — is a
    /// documented follow-up, not implemented here (kept to the narrowest
    /// version that proves network streaming into a `Store`).
    ///
    /// ## Where this is called from (MVP scope)
    /// Wired from [`FleetProvider::ensure_agent`] once a handle resolves.
    /// A production implementation would also (re)attach on a `Running`
    /// transition observed via `watch_fleet` (e.g. after an operator-driven
    /// restart that bypasses `ensure_agent`) — not implemented here.
    pub fn start_agent_stream(&self, repo: &str, agent_id: &str, endpoint: &Endpoint) {
        let addr = match endpoint {
            Endpoint::Tcp { addr } => addr.clone(),
            Endpoint::Unix { .. } => {
                tracing::warn!(
                    target: "prospero_k8s_fleet", %agent_id,
                    "start_agent_stream: k8s agent handle carries a Unix endpoint; \
                     skipping session-plane attach (expected Tcp)"
                );
                return;
            }
        };
        {
            let mut attached = self.attached.lock().unwrap();
            if !attached.insert(agent_id.to_string()) {
                // Already streaming this agent (e.g. a repeat `ensure_agent`
                // for the same spec-deterministic CR name).
                return;
            }
        }

        let client = CalibandClient::connect_tcp(addr, self.tls.clone(), self.token.clone());
        let repo = repo.to_string();
        let agent_id = agent_id.to_string();
        let emitter = self.emitter.clone();
        let attached = Arc::clone(&self.attached);

        tokio::spawn(async move {
            // No graceful-drain wiring at this seam yet (MVP) — an inert
            // shutdown channel whose sender is kept alive in this task's own
            // scope (never signalled) so `attach_loop`'s `shutdown.changed()`
            // branch never fires spuriously from a dropped sender.
            let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
            let result = attach_loop(
                &client,
                &repo,
                &agent_id,
                &emitter,
                NormalizeOptions::default(),
                AttachBackoff::default(),
                &mut shutdown_rx,
            )
            .await;
            if let Err(e) = result {
                tracing::warn!(
                    target: "prospero_k8s_fleet", %repo, %agent_id, error = %e,
                    "k8s session-plane attach task ended with error"
                );
            }
            attached.lock().unwrap().remove(&agent_id);
        });
    }
}

#[async_trait]
impl<A: CalibanTaskApi + 'static> FleetProvider for K8sFleet<A> {
    async fn ensure_agent(&self, spec: TaskSpec) -> Result<AgentHandle> {
        let name = task_name(&spec);
        let repo = spec.workspace.clone();
        let ct = build_calibantask(&spec, &name);
        self.api.apply(&ct).await?;

        let deadline = tokio::time::Instant::now() + self.poll.deadline;
        loop {
            if let Some(task) = self.api.get(&name).await?
                && let Some(handle) = handle_from(&task, repo.clone())
            {
                // Session-plane bridge (Task B4, ADR 0008 §3): as soon as the
                // agent is attachable, start feeding its live output into the
                // same bus/store `/stream` reads. See `start_agent_stream`'s
                // doc comment for the MVP-only-from-`ensure_agent` scope note.
                self.start_agent_stream(&repo, handle.id.as_str(), &handle.endpoint);
                return Ok(handle);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(CoreError::Fleet(format!(
                    "timed out waiting for CalibanTask {name} to become Running"
                )));
            }
            tokio::time::sleep(self.poll.interval).await;
        }
    }

    fn watch_fleet(&self) -> BoxStream<'static, FleetChange> {
        let api = Arc::clone(&self.api);
        let poll_interval = self.watch_poll_interval;
        // Small buffer: one poll cycle rarely produces more than a handful
        // of changes, and a slow subscriber just backpressures the sender
        // (no `Lagged`-style drop path exists at this seam, unlike the bus).
        let (tx, rx) = tokio::sync::mpsc::channel::<FleetChange>(128);

        tokio::spawn(async move {
            // Last-observed (status, repo) per task name. Starts empty, so
            // the very first poll naturally treats every currently-present
            // task as newly `Discovered` — that IS the "seed from the
            // initial listing" behavior the plan asks for, with no special
            // first-iteration branch required.
            let mut known: HashMap<String, (AgentStatus, String)> = HashMap::new();

            loop {
                let tasks = match api.list().await {
                    Ok(tasks) => tasks,
                    Err(e) => {
                        tracing::warn!(
                            target: "prospero_k8s_fleet", error = %e,
                            "watch_fleet: list() failed; retrying next poll"
                        );
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                };

                let mut seen: HashSet<String> = HashSet::with_capacity(tasks.len());
                for task in &tasks {
                    let Some(name) = task.metadata.name.clone() else {
                        // A CR with no name can't be addressed by later
                        // get/delete calls; nothing sane to diff against.
                        continue;
                    };
                    seen.insert(name.clone());

                    let agent = agent_from_task(task);
                    let status = agent.status;
                    let repo = agent.workspace.clone();

                    let change = match known.get(&name) {
                        None => Some(FleetChange::Discovered {
                            id: AgentId::from(name.clone()),
                            workspace: repo.clone(),
                            agent,
                        }),
                        Some((prev_status, _)) if *prev_status != status => {
                            Some(FleetChange::StatusChanged {
                                id: AgentId::from(name.clone()),
                                workspace: repo.clone(),
                                from: *prev_status,
                                to: status,
                            })
                        }
                        Some(_) => None,
                    };
                    known.insert(name, (status, repo));

                    if let Some(change) = change
                        && tx.send(change).await.is_err()
                    {
                        // Receiver dropped: stop polling instead of leaking
                        // this task forever.
                        return;
                    }
                }

                let gone: Vec<(String, String)> = known
                    .iter()
                    .filter(|(name, _)| !seen.contains(*name))
                    .map(|(name, (_, repo))| (name.clone(), repo.clone()))
                    .collect();
                for (name, repo) in gone {
                    known.remove(&name);
                    let change = FleetChange::Gone {
                        id: AgentId::from(name),
                        workspace: repo,
                    };
                    if tx.send(change).await.is_err() {
                        return;
                    }
                }

                tokio::time::sleep(poll_interval).await;
            }
        });

        ReceiverStream::new(rx).boxed()
    }

    async fn stop_agent(&self, id: &AgentId, drain: DrainPolicy) -> Result<()> {
        match drain {
            DrainPolicy::Kill => self.api.delete(id.as_str()).await,
            DrainPolicy::Graceful { timeout_ms } => {
                self.api.delete(id.as_str()).await?;
                let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
                loop {
                    if self.api.get(id.as_str()).await?.is_none() {
                        return Ok(());
                    }
                    if tokio::time::Instant::now() >= deadline {
                        // Best-effort: the CR may still be terminating
                        // (finalizers etc). Not an error.
                        return Ok(());
                    }
                    tokio::time::sleep(
                        Duration::from_millis(50).min(Duration::from_millis(timeout_ms)),
                    )
                    .await;
                }
            }
        }
    }

    async fn restart_agent(&self, id: &AgentId) -> Result<AgentId> {
        let old_name = id.as_str();
        let old = self
            .api
            .get(old_name)
            .await?
            .ok_or_else(|| CoreError::AgentNotFound(old_name.to_string()))?;

        let nonce = self.restart_nonce.fetch_add(1, Ordering::Relaxed);
        let new_name = restart_name(old_name, nonce);
        debug_assert_ne!(new_name, old_name, "restart must produce a fresh CR name");

        let mut fresh = CalibanTask::new(&new_name, old.spec.clone());
        // A brand-new CR starts with no status; the operator populates it.
        fresh.status = None;

        self.api.delete(old_name).await?;
        self.api.apply(&fresh).await?;

        Ok(AgentId::from(new_name))
    }

    async fn remove_agent(&self, id: &AgentId, _force: bool) -> Result<()> {
        // k8s: forgetting an agent is deleting its CalibanTask CR.
        self.api.delete(id.as_str()).await
    }

    async fn snapshot(&self) -> crate::model::FleetSnapshot {
        // List the live CalibanTasks and project each into a prospero `Agent`.
        // k8s has no prospero registry, so all agents group under one synthetic
        // workspace named for the backend (namespace scoping is the api's).
        let tasks = self.api.list().await.unwrap_or_default();
        let agents: Vec<crate::model::Agent> = tasks.iter().map(agent_from_task).collect();
        let ws = crate::model::Workspace {
            name: "k8s".into(),
            root: std::path::PathBuf::new(),
            sources: Vec::new(),
            health: crate::model::WorkspaceHealth::Healthy,
            config: crate::registry::RepoProviderConfig::default(),
            agents,
        };
        crate::model::FleetSnapshot {
            host: "k8s".into(),
            workspaces: vec![ws],
        }
    }

    async fn readiness(&self) -> crate::model::Readiness {
        // Ready iff the store accepts writes AND the kube API is reachable
        // (a `list` doubles as the reachability probe). No per-workspace poll
        // health under k8s — report the single synthetic namespace workspace.
        let store_writable = self.emitter.store().writable().await;
        let api_ok = self.api.list().await.is_ok();
        crate::model::Readiness {
            ready: store_writable && api_ok,
            store_writable,
            repos_total: 1,
            repos_healthy: usize::from(api_ok),
            repos_unreachable: usize::from(!api_ok),
        }
    }

    fn metrics(&self) -> crate::metrics::MetricsSnapshot {
        let active = self.attached.lock().unwrap().len() as u64;
        self.emitter.metrics_snapshot(active)
    }
}

/// An in-memory `CalibanTaskApi` — precursor to Task B5's more general
/// `FakeK8s`. Deliberately minimal: a name-keyed store behind a `Mutex`, no
/// watch support (that's B3/B5's concern).
#[cfg(all(test, feature = "k8s"))]
pub(crate) struct MemTaskApi {
    store: std::sync::Mutex<std::collections::HashMap<String, CalibanTask>>,
}

#[cfg(all(test, feature = "k8s"))]
impl MemTaskApi {
    fn new() -> Self {
        Self {
            store: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Test helper: flip a stored CR's status to `Running` with the given
    /// endpoint, as if the operator had reconciled it. Panics if `name` isn't
    /// present (apply it first).
    fn set_running(&self, name: &str, endpoint: &str) {
        let mut store = self.store.lock().unwrap();
        let task = store
            .get_mut(name)
            .unwrap_or_else(|| panic!("no CalibanTask named {name} to mark Running"));
        task.status = Some(crate::k8s::crd::CalibanTaskStatus {
            phase: "Running".to_string(),
            caliband_endpoint: Some(endpoint.to_string()),
            sandbox_ref: None,
        });
    }
}

#[cfg(all(test, feature = "k8s"))]
#[async_trait]
impl CalibanTaskApi for MemTaskApi {
    async fn apply(&self, ct: &CalibanTask) -> Result<()> {
        let name = ct
            .metadata
            .name
            .clone()
            .ok_or_else(|| CoreError::Fleet("CalibanTask missing metadata.name".to_string()))?;
        self.store.lock().unwrap().insert(name, ct.clone());
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Option<CalibanTask>> {
        Ok(self.store.lock().unwrap().get(name).cloned())
    }

    async fn delete(&self, name: &str) -> Result<()> {
        self.store.lock().unwrap().remove(name);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<CalibanTask>> {
        Ok(self.store.lock().unwrap().values().cloned().collect())
    }
}

#[cfg(all(test, feature = "k8s"))]
mod tests {
    use super::*;
    use crate::fleet::SpawnRequest;

    fn spec(repo: &str, prompt: &str, label: Option<&str>) -> TaskSpec {
        let mut request = SpawnRequest::new(prompt);
        request.label = label.map(str::to_string);
        TaskSpec {
            workspace: repo.to_string(),
            request,
        }
    }

    /// In-process bus + a fresh `JsonlStore` for tests that don't care about
    /// the session-plane bridge's bus/store wiring, just need `K8sFleet`'s
    /// constructor satisfied. The backing tempdir is intentionally leaked
    /// (`mem::forget`) so the store's file outlives the test.
    fn test_seams() -> (Arc<dyn EventBus>, Arc<dyn Store>) {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn Store> = Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        std::mem::forget(dir);
        let bus: Arc<dyn EventBus> = Arc::new(crate::bus::InProcessBus::new(64));
        (bus, store)
    }

    #[test]
    fn task_name_is_deterministic() {
        let a = spec("repo-a", "do the thing", None);
        let b = spec("repo-a", "do the thing", None);
        assert_eq!(task_name(&a), task_name(&b));
    }

    #[test]
    fn task_name_is_dns_safe() {
        let name = task_name(&spec("repo-a", "do the thing", Some("lbl")));
        assert!(name.starts_with("ct-"));
        assert!(
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "name must be DNS-1123-safe, got: {name}"
        );
        assert_eq!(name.len(), "ct-".len() + 16);
    }

    #[test]
    fn task_name_differs_for_different_specs() {
        let base = spec("repo-a", "prompt-1", None);
        let diff_repo = spec("repo-b", "prompt-1", None);
        let diff_prompt = spec("repo-a", "prompt-2", None);
        let diff_label = spec("repo-a", "prompt-1", Some("l1"));

        let names = [
            task_name(&base),
            task_name(&diff_repo),
            task_name(&diff_prompt),
            task_name(&diff_label),
        ];
        for i in 0..names.len() {
            for j in (i + 1)..names.len() {
                assert_ne!(names[i], names[j], "collision between spec {i} and {j}");
            }
        }
    }

    #[test]
    fn build_calibantask_maps_prompt_and_repo() {
        let s = spec("my-repo", "refactor the thing", None);
        let name = task_name(&s);
        let ct = build_calibantask(&s, &name);

        assert_eq!(ct.metadata.name.as_deref(), Some(name.as_str()));
        assert_eq!(ct.spec.task.prompt, "refactor the thing");
        assert_eq!(ct.spec.workspace.sources.len(), 1);
        assert_eq!(ct.spec.workspace.sources[0].name, "my-repo");
        assert_eq!(ct.spec.workspace.sources[0].repo, "my-repo");
        assert_eq!(ct.spec.workspace.sources[0].path, "/work/my-repo");
    }

    #[tokio::test]
    async fn ensure_agent_returns_handle_once_running() {
        let api = MemTaskApi::new();
        let (bus, store) = test_seams();
        let fleet = K8sFleet::with_poll_config(
            api,
            PollConfig {
                deadline: Duration::from_secs(5),
                interval: Duration::from_millis(10),
            },
            bus,
            store,
        );

        let s = spec("repo-a", "task", None);
        let expected_name = task_name(&s);

        // Flip the CR to Running shortly after `ensure_agent` applies it, off
        // the same task so the poll loop actually has to poll more than once.
        let flip = async {
            loop {
                if fleet.api.get(&expected_name).await.unwrap().is_some() {
                    fleet.api.set_running(&expected_name, "10.0.0.5:9443");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };

        let (handle, ()) = tokio::join!(fleet.ensure_agent(s), flip);
        let handle = handle.expect("ensure_agent");

        assert_eq!(handle.id, AgentId::from(expected_name));
        assert_eq!(handle.workspace, "repo-a");
        assert_eq!(
            handle.endpoint,
            Endpoint::Tcp {
                addr: "10.0.0.5:9443".to_string()
            }
        );
    }

    #[tokio::test]
    async fn ensure_agent_times_out_if_never_running() {
        let api = MemTaskApi::new();
        let (bus, store) = test_seams();
        let fleet = K8sFleet::with_poll_config(
            api,
            PollConfig {
                deadline: Duration::from_millis(50),
                interval: Duration::from_millis(10),
            },
            bus,
            store,
        );

        let err = fleet
            .ensure_agent(spec("repo-a", "task", None))
            .await
            .expect_err("must time out when the CR never reaches Running");
        assert!(matches!(err, CoreError::Fleet(_)));
    }

    #[tokio::test]
    async fn stop_agent_kill_deletes_the_cr() {
        let api = MemTaskApi::new();
        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store);

        let s = spec("repo-a", "task", None);
        let name = task_name(&s);
        fleet
            .api
            .apply(&build_calibantask(&s, &name))
            .await
            .unwrap();
        assert!(fleet.api.get(&name).await.unwrap().is_some());

        fleet
            .stop_agent(&AgentId::from(name.clone()), DrainPolicy::Kill)
            .await
            .expect("stop_agent");

        assert!(fleet.api.get(&name).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn restart_agent_yields_a_new_id_and_a_fresh_cr() {
        let api = MemTaskApi::new();
        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store);

        let s = spec("repo-a", "task", None);
        let old_name = task_name(&s);
        fleet
            .api
            .apply(&build_calibantask(&s, &old_name))
            .await
            .unwrap();
        fleet.api.set_running(&old_name, "10.0.0.5:9443");

        let new_id = fleet
            .restart_agent(&AgentId::from(old_name.clone()))
            .await
            .expect("restart_agent");

        assert_ne!(new_id.as_str(), old_name);
        assert!(fleet.api.get(&old_name).await.unwrap().is_none());
        let fresh = fleet
            .api
            .get(new_id.as_str())
            .await
            .unwrap()
            .expect("fresh CR exists");
        // Spec carries over from the old CR...
        assert_eq!(fresh.spec.task.prompt, "task");
        // ...but status is reset, since a brand-new CR hasn't been
        // reconciled yet.
        assert!(fresh.status.is_none());
    }

    #[test]
    fn phase_to_status_maps_known_operator_phases() {
        let cases = [
            ("Pending", AgentStatus::Spawning),
            ("Provisioning", AgentStatus::Spawning),
            ("Running", AgentStatus::Running),
            ("Draining", AgentStatus::Idle),
            ("Completed", AgentStatus::Done),
            ("Failed", AgentStatus::Failed),
        ];
        for (phase, expected) in cases {
            assert_eq!(
                phase_to_status(phase),
                expected,
                "phase {phase} should map to {expected:?}"
            );
        }
    }

    #[test]
    fn phase_to_status_defaults_unknown_phases_to_spawning() {
        assert_eq!(phase_to_status("SomeFuturePhase"), AgentStatus::Spawning);
        assert_eq!(phase_to_status(""), AgentStatus::Spawning);
    }

    #[test]
    fn agent_from_task_projects_cr_fields_onto_agent() {
        let s = spec("repo-a", "do the thing", None);
        let name = task_name(&s);
        let mut ct = build_calibantask(&s, &name);
        ct.status = Some(crate::k8s::crd::CalibanTaskStatus {
            phase: "Running".to_string(),
            caliband_endpoint: Some("10.0.0.5:9443".to_string()),
            sandbox_ref: None,
        });

        let agent = agent_from_task(&ct);

        assert_eq!(agent.id, name);
        assert_eq!(agent.name, name);
        assert_eq!(agent.workspace, "repo-a");
        assert_eq!(agent.status, AgentStatus::Running);
        assert!(!agent.isolated);
        assert!(!agent.interactive);
        assert_eq!(agent.session_dir, std::path::PathBuf::new());
    }

    #[test]
    fn agent_from_task_defaults_status_when_no_status_yet() {
        let s = spec("repo-a", "do the thing", None);
        let name = task_name(&s);
        let ct = build_calibantask(&s, &name); // status: None (fresh apply)

        let agent = agent_from_task(&ct);
        assert_eq!(agent.status, AgentStatus::Spawning);
        assert_eq!(agent.started_at, "");
    }

    /// `watch_fleet`'s first poll must seed `Discovered` for every task
    /// already present — a subscriber that starts after `ensure_agent`
    /// still learns about the existing agent instead of only seeing it on
    /// its *next* status transition.
    #[tokio::test]
    async fn watch_fleet_seeds_discovered_from_initial_listing() {
        let api = MemTaskApi::new();
        let s = spec("repo-a", "task", None);
        let name = task_name(&s);
        api.apply(&build_calibantask(&s, &name)).await.unwrap();
        api.set_running(&name, "10.0.0.5:9443");

        let (bus, store) = test_seams();
        let fleet =
            K8sFleet::new(api, bus, store).with_watch_poll_interval(Duration::from_millis(20));
        let mut changes = fleet.watch_fleet();

        let change = tokio::time::timeout(Duration::from_secs(1), changes.next())
            .await
            .expect("timed out waiting for the initial Discovered")
            .expect("watch_fleet stream ended unexpectedly");

        match change {
            FleetChange::Discovered {
                id,
                workspace: repo,
                agent,
            } => {
                assert_eq!(id, AgentId::from(name.clone()));
                assert_eq!(repo, "repo-a");
                assert_eq!(agent.status, AgentStatus::Running);
            }
            other => panic!("expected Discovered, got {other:?}"),
        }
    }

    /// A task that appears *after* `watch_fleet` is already subscribed must
    /// still surface as `Discovered`, and a subsequent phase flip on that
    /// same task must surface as `StatusChanged` (not a second `Discovered`).
    #[tokio::test]
    async fn watch_fleet_reports_live_discovered_then_status_changed() {
        let api = MemTaskApi::new();
        let (bus, store) = test_seams();
        let fleet =
            K8sFleet::new(api, bus, store).with_watch_poll_interval(Duration::from_millis(20));
        let mut changes = fleet.watch_fleet();

        let s = spec("repo-a", "task", None);
        let name = task_name(&s);
        fleet
            .api
            .apply(&build_calibantask(&s, &name))
            .await
            .unwrap();

        let discovered = tokio::time::timeout(Duration::from_secs(1), changes.next())
            .await
            .expect("timed out waiting for the live Discovered")
            .expect("watch_fleet stream ended unexpectedly");
        match discovered {
            FleetChange::Discovered {
                id,
                workspace: repo,
                agent,
            } => {
                assert_eq!(id, AgentId::from(name.clone()));
                assert_eq!(repo, "repo-a");
                // No status yet (fresh apply, no phase) -> Spawning.
                assert_eq!(agent.status, AgentStatus::Spawning);
            }
            other => panic!("expected Discovered, got {other:?}"),
        }

        fleet.api.set_running(&name, "10.0.0.5:9443");

        let status_changed = tokio::time::timeout(Duration::from_secs(1), changes.next())
            .await
            .expect("timed out waiting for StatusChanged")
            .expect("watch_fleet stream ended unexpectedly");
        match status_changed {
            FleetChange::StatusChanged {
                id,
                workspace: repo,
                from,
                to,
                ..
            } => {
                assert_eq!(id, AgentId::from(name));
                assert_eq!(repo, "repo-a");
                assert_eq!(from, AgentStatus::Spawning);
                assert_eq!(to, AgentStatus::Running);
            }
            other => panic!("expected StatusChanged, got {other:?}"),
        }
    }

    /// Deleting a `CalibanTask` that `watch_fleet` had already seen must
    /// surface as `Gone`, carrying the same repo the earlier `Discovered`
    /// reported.
    #[tokio::test]
    async fn watch_fleet_reports_gone_after_delete() {
        let api = MemTaskApi::new();
        let s = spec("repo-a", "task", None);
        let name = task_name(&s);
        api.apply(&build_calibantask(&s, &name)).await.unwrap();

        let (bus, store) = test_seams();
        let fleet =
            K8sFleet::new(api, bus, store).with_watch_poll_interval(Duration::from_millis(20));
        let mut changes = fleet.watch_fleet();

        let first = tokio::time::timeout(Duration::from_secs(1), changes.next())
            .await
            .expect("timed out waiting for the initial Discovered")
            .expect("watch_fleet stream ended unexpectedly");
        assert!(
            matches!(first, FleetChange::Discovered { .. }),
            "expected the seed Discovered first, got {first:?}"
        );

        fleet.api.delete(&name).await.unwrap();

        let gone = tokio::time::timeout(Duration::from_secs(1), changes.next())
            .await
            .expect("timed out waiting for Gone")
            .expect("watch_fleet stream ended unexpectedly");
        match gone {
            FleetChange::Gone {
                id,
                workspace: repo,
            } => {
                assert_eq!(id, AgentId::from(name));
                assert_eq!(repo, "repo-a");
            }
            other => panic!("expected Gone, got {other:?}"),
        }
    }

    // ---- #76: extended FleetProvider methods ----

    #[tokio::test]
    async fn k8s_snapshot_lists_calibantasks_as_agents() {
        let api = MemTaskApi::new();
        api.apply(&build_calibantask(&spec("repo-a", "p", None), "a1"))
            .await
            .unwrap();
        api.apply(&build_calibantask(&spec("repo-a", "p", None), "a2"))
            .await
            .unwrap();
        api.set_running("a2", "10.0.0.9:9443");
        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store);

        let snap = fleet.snapshot().await;
        let agents: Vec<_> = snap.workspaces.iter().flat_map(|w| &w.agents).collect();
        assert_eq!(agents.len(), 2, "both CRs projected as agents");
        assert_eq!(snap.workspaces[0].name, "k8s");
    }

    #[tokio::test]
    async fn k8s_remove_agent_deletes_the_cr() {
        let api = MemTaskApi::new();
        api.apply(&build_calibantask(&spec("repo-a", "p", None), "a1"))
            .await
            .unwrap();
        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store);

        fleet
            .remove_agent(&AgentId::from("a1"), true)
            .await
            .unwrap();
        assert!(
            fleet
                .snapshot()
                .await
                .workspaces
                .iter()
                .all(|w| w.agents.is_empty()),
            "the CR is gone after remove_agent"
        );
    }

    #[tokio::test]
    async fn k8s_readiness_true_when_api_and_store_healthy() {
        let api = MemTaskApi::new();
        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store);
        let r = fleet.readiness().await;
        assert!(r.store_writable);
        assert!(r.ready);
        assert_eq!(r.repos_healthy, 1);
    }
}
