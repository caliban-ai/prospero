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
use futures::stream::BoxStream;
use sha2::{Digest, Sha256};

use crate::bus::EventBus;
use crate::caliband::client::CalibandClient;
use crate::caliband::stream::NormalizeOptions;
use crate::caliband::transport::TlsClient;
use crate::caliband::wire::Endpoint;
use crate::error::{CoreError, Result};
use crate::fleet::{AttachBackoff, Emitter, attach_loop};
use crate::fleet_provider::FleetProvider;
use crate::k8s::crd::{CalibanTask, CalibanTaskSpec, TaskSpec as CrdTaskSpec, WorkspaceRef};
use crate::k8s::workspace_api::WorkspaceApi;
use crate::model::{Agent, AgentHandle, AgentId, AgentStatus, DrainPolicy, FleetChange, TaskSpec};
use crate::ownership::{Ownership, SelfOwnsAll};
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

/// Map a `TaskSpec` onto the `CalibanTask` CR that expresses it.
///
/// Post-#11 shape: the task carries a `workspaceRef` naming the `Workspace` CR
/// it runs against (the operator resolves + pins its sources/provider at
/// admission), not an inline source list. The referenced workspace name is
/// `spec.workspace` — today's implicit per-repo identity, preserved until the
/// Phase D launch modal supplies an explicit workspace/provider.
///
/// The task binds the workspace's provider named by `spec.request.provider_ref`
/// (or the operator's `defaultProvider` when unset) and carries the per-run tool
/// allow-list override. Per-run `isolation` and a per-run model stay unset:
/// isolation defaults live on the `Workspace`, and a model override is expressed
/// by provider selection (the frozen CRD has no per-task model field).
#[must_use]
pub fn build_calibantask(spec: &TaskSpec, name: &str) -> CalibanTask {
    let crd_spec = CalibanTaskSpec {
        workspace_ref: WorkspaceRef {
            name: spec.workspace.clone(),
        },
        provider_ref: spec.request.provider_ref.clone(),
        task: CrdTaskSpec {
            prompt: spec.request.prompt.clone(),
            agent_type: None,
        },
        isolation: None,
        tools: spec.request.tool_allowlist.clone(),
    };
    CalibanTask::new(name, crd_spec)
}

/// Validate the operator-provided `calibandEndpoint` before it's handed to the
/// dialer as a raw `host:port` (#127). Rejects the obviously-malformed cases —
/// empty, a scheme-qualified URL (`tcp://…`, `https://…`), or embedded
/// whitespace — so a misconfigured CR fails fast with a clear message instead
/// of deferring to a late, generic connect error deep in the dial path. This is
/// a cheap sanity gate, not a full authority parse: `caliband_endpoint` is a
/// bare `host:port` by contract, and anything shaped unlike one is a config bug.
fn validate_caliband_endpoint(addr: &str) -> Result<()> {
    if addr.trim().is_empty() {
        return Err(CoreError::Fleet(
            "CalibanTask status.calibandEndpoint is empty".to_string(),
        ));
    }
    if addr.contains("://") {
        return Err(CoreError::Fleet(format!(
            "CalibanTask status.calibandEndpoint must be a bare host:port, not a URL: {addr:?}"
        )));
    }
    if addr.chars().any(char::is_whitespace) {
        return Err(CoreError::Fleet(format!(
            "CalibanTask status.calibandEndpoint contains whitespace: {addr:?}"
        )));
    }
    Ok(())
}

/// If `task` has reached `status.phase == "Running"` with a resolved,
/// well-formed `calibandEndpoint`, build the `AgentHandle` callers can attach
/// through. `Ok(None)` while still provisioning (or if the name is somehow
/// unset); `Err(..)` if the endpoint is present but malformed (#127) — a clear
/// config error beats a late generic dial failure.
pub fn handle_from(task: &CalibanTask, repo: String) -> Result<Option<AgentHandle>> {
    let Some(status) = task.status.as_ref() else {
        return Ok(None);
    };
    if status.phase != "Running" {
        return Ok(None);
    }
    let Some(endpoint_addr) = status.caliband_endpoint.as_ref() else {
        return Ok(None);
    };
    let Some(name) = task.metadata.name.clone() else {
        return Ok(None);
    };
    validate_caliband_endpoint(endpoint_addr)?;
    let ep = Endpoint::Tcp {
        addr: endpoint_addr.clone(),
    };
    Ok(Some(AgentHandle {
        id: AgentId::from(name),
        workspace: repo,
        endpoint: Some(ep),
    }))
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
/// - A blank/unset phase (`""`) → `Spawning`: a CR the operator hasn't
///   reconciled yet genuinely hasn't started, so "not yet ready" is correct.
/// - Any *other* unrecognized, non-empty phase → `Failed`, a terminal state,
///   logged as a warning. Mapping a named-but-unknown phase to the
///   non-terminal `Spawning` was actively harmful: a phase we can't interpret
///   (including a *finished* one this mirror is too old to name, e.g.
///   "Succeeded"/"TimedOut") would look like it's still starting forever,
///   wedging every `is_terminal`/`is_active` caller. A terminal fallback fails
///   safe — a new *terminal* phase is far likelier than a new *provisioning*
///   one, so "unknown ⇒ done/failed" beats "unknown ⇒ eternally spawning".
#[must_use]
pub fn phase_to_status(phase: &str) -> AgentStatus {
    match phase {
        // Not-yet-reconciled CR (no status written): still legitimately coming up.
        "" | "Pending" | "Provisioning" => AgentStatus::Spawning,
        "Running" => AgentStatus::Running,
        "Draining" => AgentStatus::Idle,
        "Completed" => AgentStatus::Done,
        "Failed" => AgentStatus::Failed,
        other => {
            tracing::warn!(
                target: "prospero_k8s_fleet", phase = other,
                "unrecognized CalibanTask phase; defaulting to terminal AgentStatus::Failed"
            );
            AgentStatus::Failed
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
/// - `workspace` is the `spec.workspaceRef.name` the task references — i.e. the
///   `Workspace` object the agent belongs to. This is what groups agents under
///   their workspace on the read side (`GET /api/workspaces`); the workspace's
///   *sources* live inside `status.resolvedWorkspace` and are a different axis.
/// - `started_at` comes from `metadata.creationTimestamp` (RFC-3339 via
///   `Display`), or `""` if unset (a CR that hasn't round-tripped through
///   the apiserver yet, e.g. straight out of `MemTaskApi` in tests).
#[must_use]
pub fn agent_from_task(task: &CalibanTask) -> Agent {
    let name = task.metadata.name.clone().unwrap_or_default();
    let repo = task.spec.workspace_ref.name.clone();
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

/// Project a registered `Workspace` CR plus the agents that reference it into
/// the fleet's `model::Workspace` view (#149/#151).
///
/// `health` is reported `Healthy` here: under k8s the *reconciliation* status
/// (`status.phase`/`message`, surfaced separately via `GET /api/workspaces`) is
/// the real signal, not the poll-based control-socket health this field models
/// for the local backend — so the read-side handler reports `Healthy` for k8s
/// workspaces too, and this mirrors it. `root`/`config` don't apply to a k8s
/// workspace (its config lives in the CR spec, read via the registry), so they
/// stay empty/default.
fn workspace_view(ws: &crate::k8s::crd::Workspace, agents: Vec<Agent>) -> crate::model::Workspace {
    let name = ws.metadata.name.clone().unwrap_or_default();
    let sources = ws
        .spec
        .sources
        .iter()
        .map(|s| crate::Source {
            name: s.name.clone(),
            path: std::path::PathBuf::from(&s.path),
        })
        .collect();
    crate::model::Workspace {
        name,
        root: std::path::PathBuf::new(),
        sources,
        health: crate::model::WorkspaceHealth::Healthy,
        config: crate::registry::RepoProviderConfig::default(),
        agents,
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
    /// Same namespace + resource as `api`, but deserialized generically as
    /// `DynamicObject` so `list` never fails on a single schema-skewed CR
    /// (#148). Per-item typed decoding happens in `parse_calibantask_list`.
    dyn_api: kube::Api<kube::api::DynamicObject>,
}

#[cfg(feature = "k8s")]
impl KubeTaskApi {
    /// A `CalibanTaskApi` scoped to `namespace` on `client`.
    #[must_use]
    pub fn new(client: kube::Client, namespace: &str) -> Self {
        // Erase `CalibanTask`'s type into an `ApiResource` so we can also talk
        // to the same CRD endpoint generically (for lenient listing, #148).
        let ar = kube::api::ApiResource::erase::<CalibanTask>(&());
        Self {
            api: kube::Api::namespaced(client.clone(), namespace),
            dyn_api: kube::Api::namespaced_with(client, namespace, &ar),
        }
    }
}

#[cfg(feature = "k8s")]
pub(crate) fn map_kube_err(op: &str, e: kube::Error) -> CoreError {
    CoreError::Fleet(format!("{op}: {e}"))
}

/// Decode a raw list of `CalibanTask` objects **leniently**: each item is
/// deserialized independently, and any one that fails strict decoding is
/// **skipped with a warning** instead of failing the whole list (#148).
///
/// A single incompatible CR — e.g. a stale `CalibanTask` predating a now-required
/// field like `workspaceRef` (caliban-operator #11) — must not poison the fleet
/// snapshot. `KubeTaskApi::list` therefore lists generically (`DynamicObject`,
/// which never fails on schema skew) and hands the items here, so one bad CR
/// no longer wedges the watch loop → keeps the fleet populated and `/readyz`
/// out of a permanent 503 (the pod stays Ready).
fn parse_calibantask_list(items: Vec<serde_json::Value>) -> Vec<CalibanTask> {
    items
        .into_iter()
        .filter_map(|item| {
            let name = item
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<unnamed>")
                .to_string();
            match serde_json::from_value::<CalibanTask>(item) {
                Ok(task) => Some(task),
                Err(e) => {
                    tracing::warn!(
                        target: "prospero_k8s_fleet", cr = %name, error = %e,
                        "list CalibanTask: skipping a CR that failed to deserialize \
                         (schema skew?); it will not appear in the fleet, but the \
                         rest of the list is unaffected"
                    );
                    None
                }
            }
        })
        .collect()
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
        // List generically so one malformed CR can't fail the whole poll
        // (#148): `DynamicObject` deserializes any object shape, then each item
        // is decoded into `CalibanTask` independently — the bad ones are
        // skipped + logged, not fatal. See `parse_calibantask_list`.
        let list = self
            .dyn_api
            .list(&kube::api::ListParams::default())
            .await
            .map_err(|e| map_kube_err("list CalibanTask", e))?;
        let items = list
            .items
            .into_iter()
            .map(|obj| {
                serde_json::to_value(obj).map_err(|e| {
                    CoreError::Fleet(format!("list CalibanTask: re-serialize item: {e}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(parse_calibantask_list(items))
    }
}

/// How long [`K8sFleet::restart_agent`] polls for the old `CalibanTask` to
/// finish deleting before re-applying the same name, and how often it checks
/// in between. `FakeK8s` deletes synchronously, so this never matters in
/// tests; real kube deletion with finalizers needs the wait.
const RESTART_DELETE_DEADLINE: Duration = Duration::from_secs(30);
const RESTART_DELETE_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// A Kubernetes `FleetProvider` backend: drives a fleet by CRUD + watch on
/// `CalibanTask` custom resources, and (Task B4, ADR 0008 §3) bridges each
/// Running agent's live session over the network into the same event
/// bus + `Store` the API's SSE/history reads — so `/stream` works for a
/// k8s-backed agent unchanged.
pub struct K8sFleet<A: CalibanTaskApi> {
    // `Arc` (not a bare `A`) so `watch_fleet` can hand a 'static-owned handle
    // to its background poll-diff task without requiring `A: Clone`.
    api: Arc<A>,
    /// How often `watch_fleet`'s background poll-diff loop calls `list()`.
    /// Production default is ~2s; tests override it much shorter (e.g. 20ms)
    /// via [`Self::with_watch_poll_interval`] so change assertions don't wait
    /// on the production cadence.
    watch_poll_interval: Duration,
    /// The session-plane: emitter + dial materials + the live per-agent attach
    /// tasks + the ownership lease that gates who attaches. Cloned into the
    /// shared poll loop so the lease owner can attach observed-`Running` agents
    /// (#113), and shared with the `FleetProvider` methods so stop/remove/
    /// restart can stop a stale attach task (#112). All its mutable state is
    /// behind `Arc`, so a clone shares one attach set / lease mirror.
    session: SessionPlane,
    /// Canonical last-observed agents, maintained by the shared poll loop and
    /// read by `watch_fleet` to seed new subscribers consistently. (#77 M2)
    known: KnownAgents,
    /// Broadcast of fleet changes from the single shared poll-diff loop. Every
    /// `watch_fleet` subscriber seeds from `known` then tails this, so all
    /// subscribers share one `list()` cadence and see `Gone` exactly once.
    /// (#77 M2)
    changes: tokio::sync::broadcast::Sender<FleetChange>,
    /// The shared poll-diff loop's task, aborted on drop so a dropped fleet
    /// (e.g. between tests) doesn't leak a forever-polling task.
    poll_task: tokio::task::JoinHandle<()>,
    /// The k8s Workspace registry (#142), wired via [`Self::with_workspaces`].
    /// When present, [`FleetProvider::snapshot`] surfaces the registered
    /// `Workspace` CRs as the fleet's workspaces (#149/#151) instead of a single
    /// synthetic 'k8s' entry. `None` for local/test wiring, which keeps the
    /// synthetic fallback. Read-only in `snapshot`, so it never touches the poll
    /// loop.
    workspaces: Option<Arc<dyn WorkspaceApi>>,
}

impl<A: CalibanTaskApi> Drop for K8sFleet<A> {
    fn drop(&mut self) {
        self.poll_task.abort();
    }
}

/// Control handle for one agent's live session-plane attach task, so
/// stop/remove/restart can stop it promptly instead of leaving it dialing a
/// dead endpoint (#112). Kept in [`SessionPlane::attached`] keyed by agent id.
struct AttachTask {
    /// Cooperative shutdown: send `true` and [`attach_loop`] stops reconnecting
    /// and drains between frames. Sufficient once the stream is established.
    shutdown: tokio::sync::watch::Sender<bool>,
    /// Hard-stop even while the task is blocked *dialing* a dead endpoint —
    /// cooperative shutdown can't interrupt an in-flight connect, which is
    /// exactly the ~30s-of-reconnect-budget hang #112 is about. `None` only for
    /// the vanishing window between reserving the slot and the spawn returning.
    abort: Option<tokio::task::AbortHandle>,
    /// Distinguishes successive attach tasks for the same agent id so a stale
    /// task's exit-cleanup can't evict a *newer* task's entry (restart
    /// re-attach): the exiting task removes its entry only if the generation
    /// still matches.
    generation: u64,
}

/// The k8s session plane: the shared bus/store [`Emitter`], the dial materials
/// for each agent's caliband control endpoint, the live per-agent attach tasks,
/// and the [`Ownership`] lease that elects the single replica that attaches a
/// given agent (#108). Cloned into the poll loop and shared with the
/// `FleetProvider` methods; all mutable state is `Arc`, so every clone shares
/// one attach set and one lease mirror.
#[derive(Clone)]
struct SessionPlane {
    /// Session-plane emitter: shares the exact bus/store `FleetManager`'s own
    /// attach loop feeds (`crate::fleet::Emitter`).
    emitter: Emitter,
    /// TLS trust material for dialing each agent's caliband control endpoint,
    /// operator-injected (env/Secret; see [`K8sFleet::with_network`]). `None`
    /// in the fake/test plaintext path.
    tls: Option<TlsClient>,
    /// Bearer token presented after the TLS handshake (ADR 0051). `None` in
    /// the fake/test no-auth path.
    token: Option<String>,
    /// Agent ids (== the `AgentId` `ensure_agent` hands back) with a live
    /// session-plane attach task. Guards [`Self::attach`] against
    /// double-starting one, and gives stop/remove/restart a handle to stop it
    /// (#112).
    attached: Arc<Mutex<HashMap<String, AttachTask>>>,
    /// Single-writer election for the session plane (#108): only the replica
    /// that holds a given agent's per-agent lease attaches it, so 2+ replicas
    /// don't both stream the same agent (duplicate SSE + racing seq). Defaults
    /// to [`SelfOwnsAll`], so standalone/local behavior is unchanged.
    ownership: Arc<dyn Ownership>,
    /// Monotonic source for [`AttachTask::generation`].
    generation: Arc<AtomicU64>,
}

impl SessionPlane {
    /// Dial `endpoint` (the agent's pod-caliband **control** endpoint) and feed
    /// its normalized frames into the shared bus/store — but only if THIS
    /// replica wins the agent's ownership lease (#108). Idempotent: a no-op if a
    /// stream for `agent_id` is already running here, a logged no-op for a
    /// non-`Tcp` endpoint, and a silent no-op when a peer owns the lease.
    async fn attach(&self, repo: &str, agent_id: &str, endpoint: &Endpoint) {
        let addr = match endpoint {
            Endpoint::Tcp { addr } => addr.clone(),
            Endpoint::Unix { .. } => {
                tracing::warn!(
                    target: "prospero_k8s_fleet", %agent_id,
                    "attach: k8s agent handle carries a Unix endpoint; \
                     skipping session-plane attach (expected Tcp)"
                );
                return;
            }
        };

        // Fast dedup before the (possibly remote) lease call: already streaming
        // this agent here? Its task holds — and heartbeats — the lease; leave it.
        if self
            .attached
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(agent_id)
        {
            return;
        }

        // Ownership gate (#108): claim the per-agent stream. `SelfOwnsAll`
        // always acquires (standalone unchanged); `LeasedOwnership` returns
        // `None` if another live replica owns it, so exactly one replica
        // attaches. Idempotent for a stream this replica already holds.
        if self.ownership.try_acquire(agent_id).await.is_none() {
            return;
        }

        let generation = self.generation.fetch_add(1, Ordering::Relaxed);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        // Reserve the slot under the lock (dedup against a concurrent attach for
        // the same id — e.g. `ensure_agent` racing the poll loop). If someone
        // already reserved it, that task owns the (idempotently-shared) lease and
        // releases it on exit; we must NOT release here or we'd orphan its
        // writer (mirrors `FleetManager::start_attach`).
        {
            let mut attached = self.attached.lock().unwrap_or_else(|e| e.into_inner());
            if attached.contains_key(agent_id) {
                return;
            }
            attached.insert(
                agent_id.to_string(),
                AttachTask {
                    shutdown: shutdown_tx,
                    abort: None,
                    generation,
                },
            );
        }

        let client = CalibandClient::connect_tcp(addr, self.tls.clone(), self.token.clone());
        let repo = repo.to_string();
        // Owned copy for the task; the `&str` param stays valid for the
        // abort-handle registration below.
        let task_agent_id = agent_id.to_string();
        let emitter = self.emitter.clone();
        let attached = Arc::clone(&self.attached);
        let ownership = Arc::clone(&self.ownership);

        let handle = tokio::spawn(async move {
            let result = attach_loop(
                &client,
                &repo,
                &task_agent_id,
                &emitter,
                NormalizeOptions::default(),
                AttachBackoff::default(),
                &mut shutdown_rx,
            )
            .await;
            if let Err(e) = result {
                tracing::warn!(
                    target: "prospero_k8s_fleet", %repo, agent_id = %task_agent_id, error = %e,
                    "k8s session-plane attach task ended with error"
                );
            }
            // Self-cleanup, but only if we're still the current task for this id:
            // a restart may have replaced us with a newer generation, whose entry
            // we must not evict. (#112)
            {
                let mut attached = attached.lock().unwrap_or_else(|e| e.into_inner());
                if attached.get(&task_agent_id).map(|t| t.generation) == Some(generation) {
                    attached.remove(&task_agent_id);
                }
            }
            // Release for prompt failover hand-off (clustered); no-op standalone.
            ownership.release(&task_agent_id).await;
        });

        // Record the abort handle so stop/remove/restart can hard-stop a task
        // wedged mid-dial. Guard on generation: `stop` may have already removed
        // (and replaced or cleared) our reservation while we were spawning.
        {
            let mut attached = self.attached.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(task) = attached.get_mut(agent_id)
                && task.generation == generation
            {
                task.abort = Some(handle.abort_handle());
            }
        }
    }

    /// Stop the live attach task for `agent_id` (if any) and release its lease.
    /// Prompt: cooperative shutdown drains an established stream, and the abort
    /// handle kills one wedged mid-dial so it doesn't burn the reconnect budget
    /// against a dead endpoint (#112). Clears the `attached` bookkeeping so a
    /// later re-attach (#113 observe-`Running`) isn't suppressed.
    async fn stop(&self, agent_id: &str) {
        let task = self
            .attached
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(agent_id);
        if let Some(task) = task {
            let _ = task.shutdown.send(true);
            if let Some(abort) = task.abort {
                abort.abort();
            }
        }
        // Release the per-agent lease so a peer (or a future re-attach here) can
        // claim it promptly rather than waiting out the TTL. Idempotent /
        // no-op under `SelfOwnsAll`.
        self.ownership.release(agent_id).await;
    }

    /// Live attach count — the `metrics()` active gauge.
    fn active_count(&self) -> u64 {
        self.attached
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len() as u64
    }
}

/// Default cadence for `watch_fleet`'s poll-diff loop.
const DEFAULT_WATCH_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// The canonical last-observed agent per CR name, shared between the poll loop
/// (which maintains it) and `watch_fleet` (which seeds new subscribers from it).
/// Sharing this — rather than each seeding via its own `list()` — keeps a
/// subscriber's seed and the loop's diff stream consistent, so an agent the
/// loop never observed can't be seed-`Discovered` without a matching `Gone`.
type KnownAgents = Arc<Mutex<HashMap<String, Agent>>>;

/// Spawn the single shared poll-diff loop: `list()` on `interval`, diff against
/// the shared `known` state, update it, broadcast each `FleetChange` once, and
/// (#113) attach every observed-`Running` agent this replica is elected to own.
/// Runs for the fleet's lifetime (aborted by `K8sFleet::drop`). (#77 M2)
fn spawn_watch_loop<A: CalibanTaskApi + 'static>(
    api: Arc<A>,
    known: KnownAgents,
    tx: tokio::sync::broadcast::Sender<FleetChange>,
    interval: Duration,
    session: SessionPlane,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let tasks = match api.list().await {
                Ok(tasks) => tasks,
                Err(e) => {
                    tracing::warn!(
                        target: "prospero_k8s_fleet", error = %e,
                        "watch loop: list() failed; retrying next poll"
                    );
                    tokio::time::sleep(interval).await;
                    continue;
                }
            };
            // #113: any agent observed `Running` — including operator/peer-created
            // ones this replica never spawned — must get attached so its `/stream`
            // isn't permanently empty. Collect the attachable handles here (under
            // the diff lock, `handle_from` is pure), then attach after releasing
            // the lock. `session.attach` itself is gated by the #108 ownership
            // lease, so exactly one replica actually attaches each.
            let mut to_attach: Vec<(String, String, Endpoint)> = Vec::new();
            // Diff under the lock so a concurrent `watch_fleet` seed sees a
            // consistent snapshot, then broadcast after releasing it.
            let mut changes: Vec<FleetChange> = Vec::new();
            {
                // Poison-tolerant: recover the guard if a prior holder panicked,
                // so one panic can't wedge every later poll/watch/metrics call
                // (#126). The map is a plain last-observed cache, safe to reuse.
                let mut known = known.lock().unwrap_or_else(|e| e.into_inner());
                let mut seen: HashSet<String> = HashSet::with_capacity(tasks.len());
                for task in &tasks {
                    let Some(name) = task.metadata.name.clone() else {
                        continue;
                    };
                    seen.insert(name.clone());
                    let agent = agent_from_task(task);
                    // #113: queue an attach for a Running, attachable agent. A
                    // malformed endpoint on a Running CR is logged and skipped
                    // (same defensive posture as `handle_from`'s callers), not
                    // fatal to the poll loop.
                    match handle_from(task, agent.workspace.clone()) {
                        Ok(Some(handle)) => {
                            if let Some(endpoint) = handle.endpoint {
                                to_attach.push((
                                    agent.workspace.clone(),
                                    handle.id.as_str().to_string(),
                                    endpoint,
                                ));
                            }
                        }
                        Ok(None) => {}
                        Err(e) => tracing::warn!(
                            target: "prospero_k8s_fleet", agent = %name, error = %e,
                            "watch loop: Running CR has a malformed endpoint; skipping attach"
                        ),
                    }
                    match known.get(&name) {
                        None => changes.push(FleetChange::Discovered {
                            id: AgentId::from(name.clone()),
                            workspace: agent.workspace.clone(),
                            agent: agent.clone(),
                        }),
                        Some(prev) if prev.status != agent.status => {
                            changes.push(FleetChange::StatusChanged {
                                id: AgentId::from(name.clone()),
                                workspace: agent.workspace.clone(),
                                from: prev.status,
                                to: agent.status,
                            })
                        }
                        Some(_) => {}
                    }
                    known.insert(name, agent);
                }
                let gone: Vec<String> = known
                    .keys()
                    .filter(|name| !seen.contains(*name))
                    .cloned()
                    .collect();
                for name in gone {
                    let agent = known.remove(&name).expect("present");
                    changes.push(FleetChange::Gone {
                        id: AgentId::from(name),
                        workspace: agent.workspace,
                    });
                }
            }
            // A send error just means no live subscribers right now; `known`
            // stays canonical for later subscribers.
            for change in changes {
                let _ = tx.send(change);
            }
            // #113: attach observed-Running agents (lock released above). Each
            // call is deduped + ownership-gated inside `attach`, so a foreign or
            // already-attached agent is cheap (a no-op after a lease miss / a
            // map hit) and only the elected replica actually streams.
            for (repo, id, endpoint) in to_attach {
                session.attach(&repo, &id, &endpoint).await;
            }
            tokio::time::sleep(interval).await;
        }
    })
}

impl<A: CalibanTaskApi + 'static> K8sFleet<A> {
    /// A `K8sFleet` with the default (~2s) `watch_fleet` poll cadence,
    /// feeding session-plane events into `bus`/`store` (in-process defaults
    /// for the fake/test wiring; production threads through the daemon's
    /// real seams — Task B6).
    #[must_use]
    pub fn new(api: A, bus: Arc<dyn EventBus>, store: Arc<dyn Store>) -> Self {
        let api = Arc::new(api);
        let known: KnownAgents = Arc::new(Mutex::new(HashMap::new()));
        let (changes, _) = tokio::sync::broadcast::channel::<FleetChange>(256);
        let session = SessionPlane {
            emitter: Emitter::new(bus, store),
            tls: None,
            token: None,
            attached: Arc::new(Mutex::new(HashMap::new())),
            // Standalone default (#108): every stream is owned unconditionally,
            // so the lease is a no-op and single-replica/local behavior is
            // identical to before. The daemon injects `LeasedOwnership` when
            // clustered via [`Self::with_ownership`].
            ownership: Arc::new(SelfOwnsAll),
            generation: Arc::new(AtomicU64::new(0)),
        };
        let poll_task = spawn_watch_loop(
            Arc::clone(&api),
            Arc::clone(&known),
            changes.clone(),
            DEFAULT_WATCH_POLL_INTERVAL,
            session.clone(),
        );
        Self {
            api,
            watch_poll_interval: DEFAULT_WATCH_POLL_INTERVAL,
            session,
            known,
            changes,
            poll_task,
            workspaces: None,
        }
    }

    /// Restart the shared poll loop so it captures the current session-plane
    /// config (`tls`/`token`/`ownership`) and cadence. The builders below set
    /// that config *after* construction, so each re-spawns the loop; since all
    /// the loop's mutable state (`known`, `attached`, the broadcast channel) is
    /// shared behind `Arc`, restarting it is seamless. Startup-only, so the
    /// extra spawn is negligible.
    fn respawn_watch_loop(&mut self) {
        self.poll_task.abort();
        self.poll_task = spawn_watch_loop(
            Arc::clone(&self.api),
            Arc::clone(&self.known),
            self.changes.clone(),
            self.watch_poll_interval,
            self.session.clone(),
        );
    }

    /// Override `watch_fleet`'s poll cadence. Tests use a short interval
    /// (e.g. 20ms) so diff assertions don't block on the production default.
    /// Restarts the shared poll loop at the new cadence.
    #[must_use]
    pub fn with_watch_poll_interval(mut self, interval: Duration) -> Self {
        self.watch_poll_interval = interval;
        self.respawn_watch_loop();
        self
    }

    /// Configure the TLS trust root + bearer token [`Self::start_agent_stream`]
    /// uses to dial each agent's caliband control endpoint (ADR 0008 §3).
    /// Defaults to `(None, None)` — plaintext/no-auth, the fake/test path.
    #[must_use]
    pub fn with_network(mut self, tls: Option<TlsClient>, token: Option<String>) -> Self {
        self.session.tls = tls;
        self.session.token = token;
        self.respawn_watch_loop();
        self
    }

    /// Inject the single-writer election for the session plane (#108). The
    /// daemon passes a `LeasedOwnership` when clustered (Postgres present) so
    /// exactly one replica attaches — and thus emits events for — a given agent;
    /// standalone keeps the default [`SelfOwnsAll`], so behavior is unchanged.
    #[must_use]
    pub fn with_ownership(mut self, ownership: Arc<dyn Ownership>) -> Self {
        self.session.ownership = ownership;
        self.respawn_watch_loop();
        self
    }

    /// Wire the k8s Workspace registry (#142) so [`FleetProvider::snapshot`]
    /// surfaces the registered `Workspace` CRs as the fleet's workspaces
    /// (#149/#151) instead of a single synthetic 'k8s' entry. Unset (local/test)
    /// keeps the synthetic fallback. `snapshot` reads the registry directly, so
    /// this — unlike the session-plane builders — doesn't respawn the poll loop.
    #[must_use]
    pub fn with_workspaces(mut self, workspaces: Arc<dyn WorkspaceApi>) -> Self {
        self.workspaces = Some(workspaces);
        self
    }
}

impl<A: CalibanTaskApi + 'static> K8sFleet<A> {
    /// #130: refine each projected agent's `interactive` + `status` from its pod
    /// caliband's control `List`, in place. The CR only carries the coarse
    /// reconciled phase; the pod caliband carries the interactive flag and the
    /// fine-grained `Idle` ("awaiting input") state the dashboard reply box needs.
    ///
    /// Only `Running`, attachable CRs are consulted (via [`handle_from`], which
    /// yields `Some` exactly for those). One `List` is issued per distinct pod
    /// endpoint — a pod caliband may host more than one agent — and each returned
    /// record overlays the agent sharing its id (the pod registers its agent under
    /// the CR name; see [`Self::start_agent_stream`]'s agent-id note). A pod that
    /// fails to answer is logged and skipped: that agent keeps its CR-phase
    /// status rather than dropping out or failing the whole snapshot (#148).
    async fn overlay_pod_status(&self, tasks: &[CalibanTask], agents: &mut [Agent]) {
        // Distinct Running pod endpoints to query (dedup: one List per pod).
        let mut endpoints: Vec<String> = Vec::new();
        for task in tasks {
            let repo = task.spec.workspace_ref.name.clone();
            if let Ok(Some(handle)) = handle_from(task, repo)
                && let Some(Endpoint::Tcp { addr }) = &handle.endpoint
                && !endpoints.contains(addr)
            {
                endpoints.push(addr.clone());
            }
        }
        if endpoints.is_empty() {
            return;
        }

        // id -> live record, merged across every reachable pod.
        let mut records: HashMap<String, crate::caliband::wire::AgentRecord> = HashMap::new();
        for addr in endpoints {
            let client = CalibandClient::connect_tcp(
                addr.clone(),
                self.session.tls.clone(),
                self.session.token.clone(),
            );
            match client.list().await {
                Ok(recs) => {
                    for rec in recs {
                        records.insert(rec.id.clone(), rec);
                    }
                }
                Err(e) => tracing::debug!(
                    target: "prospero_k8s_fleet", endpoint = %addr, error = %e,
                    "snapshot overlay: pod caliband List failed; keeping CR-phase status"
                ),
            }
        }

        for agent in agents.iter_mut() {
            if let Some(rec) = records.get(&agent.id) {
                agent.interactive = rec.spec.interactive;
                agent.status = rec.status;
            }
        }
    }

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
    /// ## Where this is called from
    /// Wired from [`FleetProvider::ensure_agent`] once a handle resolves, and —
    /// as of #113 — from the shared poll loop for any agent observed `Running`
    /// (including operator/peer-created ones this replica never spawned, and a
    /// restarted CR once it comes back up). The #108 ownership lease inside
    /// [`SessionPlane::attach`] ensures exactly one replica attaches.
    pub async fn start_agent_stream(&self, repo: &str, agent_id: &str, endpoint: &Endpoint) {
        self.session.attach(repo, agent_id, endpoint).await;
    }

    /// Stop the live session-plane attach for `agent_id`, if any (#112). Public
    /// for symmetry with [`Self::start_agent_stream`]; the `FleetProvider`
    /// stop/remove/restart paths call it so a torn-down agent's task doesn't
    /// keep dialing a dead endpoint (and over-report in `metrics()`).
    pub async fn stop_agent_stream(&self, agent_id: &str) {
        self.session.stop(agent_id).await;
    }
}

#[async_trait]
impl<A: CalibanTaskApi + 'static> FleetProvider for K8sFleet<A> {
    async fn ensure_agent(&self, spec: TaskSpec) -> Result<AgentHandle> {
        let name = task_name(&spec);
        let repo = spec.workspace.clone();
        let ct = build_calibantask(&spec, &name);
        // `apply` runs the operator's admission webhook synchronously, so an
        // invalid workspaceRef / empty providers still fails fast here (4xx).
        // We deliberately do NOT wait for status.phase == "Running": the shared
        // watch loop (spawn_watch_loop) surfaces the agent on the dashboard and
        // #113-attaches its session when the pod becomes Running. Blocking here
        // would couple the HTTP response to full reconcile latency (Bug 1).
        self.api.apply(&ct).await?;
        Ok(AgentHandle {
            id: AgentId::from(name),
            workspace: repo,
            endpoint: None,
        })
    }

    fn watch_fleet(&self) -> BoxStream<'static, FleetChange> {
        // Subscribe BEFORE reading `known` so no diff is missed in the gap, then
        // seed from the shared canonical `known` (Discovered per present agent)
        // and tail the broadcast — deduping the seed/tail overlap by id. Seeding
        // from the *same* state the loop maintains (not an independent `list()`)
        // guarantees an agent the loop never observed can't be seed-Discovered
        // without a matching Gone. One `list()` cadence for all subscribers;
        // `Gone` delivered exactly once per live subscriber. (#77 M2)
        let mut rx = self.changes.subscribe();
        let known = Arc::clone(&self.known);

        let stream = async_stream::stream! {
            let seed: Vec<(String, Agent)> = {
                // Poison-tolerant (#126): recover rather than panic the stream.
                let known = known.lock().unwrap_or_else(|e| e.into_inner());
                known.iter().map(|(n, a)| (n.clone(), a.clone())).collect()
            };
            let mut seen: HashSet<String> = HashSet::with_capacity(seed.len());
            for (name, agent) in seed {
                let workspace = agent.workspace.clone();
                seen.insert(name.clone());
                yield FleetChange::Discovered { id: AgentId::from(name), workspace, agent };
            }

            loop {
                match rx.recv().await {
                    Ok(change) => {
                        // Drop a Discovered already emitted by the seed (the
                        // seed and the broadcast overlap by up to one cycle).
                        if let FleetChange::Discovered { ref id, .. } = change
                            && seen.remove(id.as_str())
                        {
                            continue;
                        }
                        yield change;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Slow subscriber fell behind: re-seed from the shared
                        // `known` rather than silently dropping changes.
                        let reseed: Vec<(String, Agent)> = {
                            // Poison-tolerant (#126).
                            let known = known.lock().unwrap_or_else(|e| e.into_inner());
                            known.iter().map(|(n, a)| (n.clone(), a.clone())).collect()
                        };
                        seen.clear();
                        for (name, agent) in reseed {
                            let workspace = agent.workspace.clone();
                            seen.insert(name.clone());
                            yield FleetChange::Discovered { id: AgentId::from(name), workspace, agent };
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        };
        Box::pin(stream)
    }

    async fn stop_agent(&self, id: &AgentId, drain: DrainPolicy) -> Result<()> {
        // Stop the live session-plane attach first so it stops dialing the
        // about-to-be-deleted endpoint and `metrics()` stops counting it (#112).
        self.stop_agent_stream(id.as_str()).await;
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

        // A CR's name is a pure function of spec (`task_name`, set at
        // `ensure_agent`), so a restart re-applies the SAME name rather than a
        // salted one — keeping identity idempotent so a future declarative
        // `ensure_agent(spec)` reconcile targets the one CR instead of applying
        // a duplicate. (#77 M1)
        // Stop the stale attach task and clear its `attached` bookkeeping (#112):
        // the old CR is going away, and clearing the entry is what lets the
        // #113 observe-`Running` re-attach fire for the fresh CR instead of
        // being suppressed as "already attached".
        self.stop_agent_stream(old_name).await;
        self.api.delete(old_name).await?;
        // Wait for the old CR to actually disappear before re-applying the same
        // name, so we never race a not-yet-finalized delete. `FakeK8s` deletes
        // synchronously; real kube deletion with finalizers needs this poll.
        let deadline = tokio::time::Instant::now() + RESTART_DELETE_DEADLINE;
        while self.api.get(old_name).await?.is_some() {
            if tokio::time::Instant::now() >= deadline {
                return Err(CoreError::Fleet(format!(
                    "restart: CalibanTask {old_name} did not delete within the budget"
                )));
            }
            tokio::time::sleep(RESTART_DELETE_POLL_INTERVAL).await;
        }

        let mut fresh = CalibanTask::new(old_name, old.spec.clone());
        // A brand-new CR starts with no status; the operator populates it.
        fresh.status = None;
        self.api.apply(&fresh).await?;

        // Re-establishment of the session-plane stream (#112) happens through the
        // #113 observe-`Running` re-attach in the shared poll loop: the fresh CR
        // has no endpoint yet (status reset to `None`), so there is nothing to
        // attach *now* — the operator must first reconcile it back to `Running`.
        // Because we cleared the stale `attached` entry above, the poll loop's
        // ownership-gated `attach` fires as soon as the new endpoint appears,
        // rather than blocking this call for up to the poll deadline waiting on
        // the operator. (If you'd rather `restart_agent` re-attach directly, a
        // bounded wait-for-`Running`-then-`start_agent_stream` — mirroring
        // `ensure_agent`'s tail — would be the change.)
        Ok(AgentId::from(old_name))
    }

    async fn remove_agent(&self, id: &AgentId, _force: bool) -> Result<()> {
        // Stop the live attach so it doesn't keep dialing the removed endpoint
        // (and stops counting in `metrics()`) (#112).
        self.stop_agent_stream(id.as_str()).await;
        // k8s: forgetting an agent is deleting its CalibanTask CR.
        self.api.delete(id.as_str()).await
    }

    async fn snapshot(&self) -> crate::model::FleetSnapshot {
        // List the live CalibanTasks and project each into a prospero `Agent`.
        // Each agent already carries its workspace (`agent.workspace` ==
        // `spec.workspaceRef.name`), so grouping below is purely a regroup.
        let tasks = self.api.list().await.unwrap_or_default();
        let mut agents: Vec<crate::model::Agent> = tasks.iter().map(agent_from_task).collect();

        // #130: overlay live per-agent `interactive` + awaiting-input status from
        // each pod caliband's control `List`. The CR carries only the coarse
        // reconciled phase (`Pending`/`Running`/`Draining`) — never the
        // interactive flag and never "awaiting input" (`Idle`), so from the CR
        // alone the dashboard reply box (`interactive && idle`) can never appear.
        // The pod caliband knows both; prospero already dials it for attach. The
        // CR still supplies membership/lifecycle above; this only refines the
        // per-turn detail of the Running agents (ADR 0004's hybrid-observability
        // split applied to k8s).
        self.overlay_pod_status(&tasks, &mut agents).await;

        // When the Workspace registry is wired (k8s config plane, #142), the
        // fleet's workspaces ARE the registered `Workspace` CRs (#149/#151):
        // surface each, grouping agents under the workspace they reference. This
        // replaces the old single synthetic 'k8s' workspace, which collided with
        // the registry ('workspace not registered: k8s', #149) and hid
        // registered workspaces from `/api/fleet` (#151). It also keeps
        // `/api/fleet` and `/api/workspaces` agreeing on the workspace set —
        // both now derive from the same registry list.
        if let Some(ws_api) = &self.workspaces
            && let Ok(registered) = ws_api.list().await
        {
            let mut by_workspace: HashMap<String, Vec<crate::model::Agent>> = HashMap::new();
            for agent in agents {
                by_workspace
                    .entry(agent.workspace.clone())
                    .or_default()
                    .push(agent);
            }
            let workspaces = registered
                .iter()
                .map(|ws| {
                    let name = ws.metadata.name.clone().unwrap_or_default();
                    let agents = by_workspace.remove(&name).unwrap_or_default();
                    workspace_view(ws, agents)
                })
                .collect();
            return crate::model::FleetSnapshot {
                host: "k8s".into(),
                workspaces,
            };
        }

        // Fallback (no registry wired — local/test paths, or a registry that
        // failed to list): one synthetic workspace named for the backend holds
        // every agent, as before.
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
        let store_writable = self.session.emitter.store().writable().await;
        let api_ok = self.api.list().await.is_ok();
        crate::model::Readiness {
            ready: store_writable && api_ok,
            store_writable,
            workspaces_total: 1,
            workspaces_healthy: usize::from(api_ok),
            workspaces_unreachable: usize::from(!api_ok),
        }
    }

    fn metrics(&self) -> crate::metrics::MetricsSnapshot {
        // Poison-tolerant (#126): a metrics scrape must never panic.
        let active = self.session.active_count();
        self.session.emitter.metrics_snapshot(active)
    }

    async fn send_input(
        &self,
        id: &AgentId,
        input: crate::caliband::wire::AttachInbound,
    ) -> Result<()> {
        // Resolve the agent's networked caliband endpoint from its CR status,
        // then deliver the frame over the same TCP+TLS+token session plane
        // `start_agent_stream` uses (ADR 0008 §3).
        let task = self
            .api
            .get(id.as_str())
            .await?
            .ok_or_else(|| CoreError::AgentNotFound(id.as_str().to_string()))?;
        let handle = handle_from(&task, "k8s".into())?.ok_or_else(|| CoreError::InvalidState {
            op: "send_input".to_string(),
            id: id.as_str().to_string(),
            status: "not attachable".to_string(),
        })?;
        let Some(Endpoint::Tcp { addr }) = &handle.endpoint else {
            return Err(CoreError::Fleet(
                "k8s agent endpoint is not Tcp".to_string(),
            ));
        };
        let client = CalibandClient::connect_tcp(
            addr.clone(),
            self.session.tls.clone(),
            self.session.token.clone(),
        );
        // Resolve the agent's **per-agent** endpoint via an `Attach` control
        // round-trip before delivering — `send_inbound` writes to the per-agent
        // inbox endpoint `attach` returns, NOT the pod's control endpoint. This
        // mirrors `LocalFleet::send_agent_input` and the streaming path
        // (`attach_once`), both of which `attach` first (#130).
        let per_agent = client.attach(id.as_str()).await?;
        client.send_inbound(&per_agent, &input).await
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
            resolved_workspace: None,
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
    use crate::testkit::FakeCaliband;
    use futures::StreamExt as _;

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
    fn parse_calibantask_list_skips_malformed_and_keeps_valid() {
        // #148: one well-formed CR and one that predates the now-required
        // `workspaceRef` field (the exact prod failure — a leftover CalibanTask
        // created before caliban-operator #11 made `workspaceRef` required).
        // The stale CR must be skipped, not fatal to the whole list, so the
        // fleet snapshot (and thus /readyz) survives a single bad CR.
        let good = serde_json::json!({
            "apiVersion": "caliban.caliban-ai.dev/v1alpha1",
            "kind": "CalibanTask",
            "metadata": { "name": "good-task", "namespace": "caliban" },
            "spec": {
                "workspaceRef": { "name": "team-a-ws" },
                "task": { "prompt": "do the thing" }
            }
        });
        let stale = serde_json::json!({
            "apiVersion": "caliban.caliban-ai.dev/v1alpha1",
            "kind": "CalibanTask",
            "metadata": { "name": "stale-task", "namespace": "caliban" },
            // No `workspaceRef`: a CR from before the field became required.
            "spec": { "task": { "prompt": "legacy" } }
        });

        // Order-independent: the bad CR sitting first must not drop the good one.
        let tasks = parse_calibantask_list(vec![stale, good]);

        assert_eq!(
            tasks.len(),
            1,
            "the malformed CR must be skipped, not fatal"
        );
        assert_eq!(tasks[0].metadata.name.as_deref(), Some("good-task"));
        assert_eq!(tasks[0].spec.workspace_ref.name, "team-a-ws");
    }

    #[test]
    fn build_calibantask_maps_prompt_and_repo() {
        let s = spec("my-repo", "refactor the thing", None);
        let name = task_name(&s);
        let ct = build_calibantask(&s, &name);

        assert_eq!(ct.metadata.name.as_deref(), Some(name.as_str()));
        assert_eq!(ct.spec.task.prompt, "refactor the thing");
        // Post-#11: the task references its workspace by name (the operator
        // resolves + pins sources at admission); no inline source list here.
        assert_eq!(ct.spec.workspace_ref.name, "my-repo");
        assert!(ct.spec.provider_ref.is_none());
    }

    #[test]
    fn build_calibantask_emits_provider_ref_and_tools() {
        let mut s = spec("my-ws", "do it", None);
        s.request.provider_ref = Some("workers".to_string());
        s.request.tool_allowlist = Some(vec!["Read".to_string(), "Edit".to_string()]);
        let ct = build_calibantask(&s, "ct-x");
        assert_eq!(ct.spec.workspace_ref.name, "my-ws");
        assert_eq!(ct.spec.provider_ref.as_deref(), Some("workers"));
        assert_eq!(
            ct.spec.tools.as_deref(),
            Some(["Read".to_string(), "Edit".to_string()].as_slice())
        );
    }

    #[tokio::test]
    async fn ensure_agent_returns_immediately_without_waiting_for_running() {
        let api = MemTaskApi::new();
        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store);

        let s = spec("repo-a", "task", None);
        let expected_name = task_name(&s);

        // The CR is never flipped to Running. ensure_agent must still return.
        let handle = fleet.ensure_agent(s).await.expect("ensure_agent");

        assert_eq!(handle.id, AgentId::from(expected_name.clone()));
        assert_eq!(handle.workspace, "repo-a");
        // No endpoint yet — the pod isn't Running.
        assert_eq!(handle.endpoint, None);
        // The CR was applied (admission happened synchronously).
        assert!(fleet.api.get(&expected_name).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn ensure_agent_does_not_block_on_a_never_running_cr() {
        let api = MemTaskApi::new();
        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store);

        // A generous ceiling: the old code blocked ~30s here. The new code
        // returns in well under a second regardless of CR phase.
        let out = tokio::time::timeout(
            Duration::from_secs(2),
            fleet.ensure_agent(spec("repo-a", "task", None)),
        )
        .await;
        assert!(out.is_ok(), "ensure_agent must not block on Running");
        out.unwrap().expect("ensure_agent");
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
    async fn restart_agent_reapplies_the_same_name_with_reset_status() {
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

        // #77 M1: restart keeps the spec-deterministic name (stable identity).
        assert_eq!(new_id.as_str(), old_name);
        let fresh = fleet
            .api
            .get(new_id.as_str())
            .await
            .unwrap()
            .expect("fresh CR exists at the same name");
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
    fn phase_to_status_maps_unknown_named_phases_to_terminal_not_spawning() {
        // #114: a named-but-unrecognized phase must NOT be reported as the
        // non-terminal `Spawning` (which would make a finished agent look like
        // it's still starting forever). It maps to a terminal state instead.
        for phase in ["Succeeded", "TimedOut", "SomethingNew"] {
            let status = phase_to_status(phase);
            assert_ne!(
                status,
                AgentStatus::Spawning,
                "unknown phase {phase} must not map to the non-terminal Spawning"
            );
            assert!(
                status.is_terminal(),
                "unknown phase {phase} should map to a terminal status, got {status:?}"
            );
            assert_eq!(status, AgentStatus::Failed);
        }
        // A blank/unset phase is the one exception: a not-yet-reconciled CR is
        // genuinely still coming up, so it stays `Spawning`.
        assert_eq!(phase_to_status(""), AgentStatus::Spawning);
    }

    /// #127: a Running CR whose `calibandEndpoint` is malformed (empty, a
    /// scheme-qualified URL, or whitespace-laden) must surface a clear error
    /// from `handle_from`, not a well-formed handle that fails later at dial.
    #[test]
    fn handle_from_rejects_malformed_endpoint() {
        let s = spec("repo-a", "task", None);
        let name = task_name(&s);

        let with_endpoint = |endpoint: &str| {
            let mut ct = build_calibantask(&s, &name);
            ct.status = Some(crate::k8s::crd::CalibanTaskStatus {
                phase: "Running".to_string(),
                caliband_endpoint: Some(endpoint.to_string()),
                sandbox_ref: None,
                resolved_workspace: None,
            });
            ct
        };

        for bad in [
            "",
            "   ",
            "tcp://10.0.0.5:9443",
            "https://host:9443",
            "10.0.0.5 9443",
        ] {
            let err = handle_from(&with_endpoint(bad), "repo-a".into())
                .expect_err(&format!("endpoint {bad:?} should be rejected"));
            assert!(
                matches!(err, CoreError::Fleet(_)),
                "got {err:?} for {bad:?}"
            );
        }

        // A well-formed bare host:port still yields a handle.
        let handle = handle_from(&with_endpoint("10.0.0.5:9443"), "repo-a".into())
            .expect("valid endpoint is Ok")
            .expect("valid endpoint yields a handle");
        assert_eq!(
            handle.endpoint,
            Some(Endpoint::Tcp {
                addr: "10.0.0.5:9443".to_string()
            })
        );

        // Still provisioning (no status) is Ok(None), not an error.
        assert!(
            handle_from(&build_calibantask(&s, &name), "repo-a".into())
                .expect("no status is Ok")
                .is_none()
        );
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
            resolved_workspace: None,
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

    /// Build a `Workspace` CR for the registry fake.
    #[cfg(feature = "k8s")]
    fn workspace_cr(name: &str) -> crate::k8s::crd::Workspace {
        use crate::k8s::crd::{Provider, Source as CrdSource, Workspace, WorkspaceSpec};
        Workspace::new(
            name,
            WorkspaceSpec {
                display_name: format!("{name} display"),
                sources: vec![CrdSource {
                    name: "caliban".into(),
                    repo: "git@example:caliban".into(),
                    r#ref: "main".into(),
                    path: "/work/caliban".into(),
                }],
                providers: vec![Provider {
                    name: "ollama".into(),
                    kind: "ollama".into(),
                    base_url: None,
                    model: None,
                    credentials_ref: None,
                }],
                default_provider: None,
                env: Vec::new(),
                isolation: None,
            },
        )
    }

    #[tokio::test]
    async fn k8s_snapshot_surfaces_registered_workspaces_not_synthetic() {
        // #149/#151: with a Workspace registry wired, the fleet snapshot must
        // surface the registered `Workspace` CRs (agents grouped by the
        // workspace they reference) — NOT a single synthetic 'k8s' workspace,
        // which collides with the registry ('workspace not registered: k8s')
        // and hides registered workspaces from `/api/fleet`.
        use crate::k8s::fake::FakeWorkspaceApi;

        let api = MemTaskApi::new();
        // Two tasks referencing the registered workspace "team-ws".
        api.apply(&build_calibantask(&spec("team-ws", "p", None), "a1"))
            .await
            .unwrap();
        api.apply(&build_calibantask(&spec("team-ws", "p", Some("l")), "a2"))
            .await
            .unwrap();

        let ws_api = Arc::new(FakeWorkspaceApi::new());
        ws_api.apply(&workspace_cr("team-ws")).await.unwrap();

        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store).with_workspaces(ws_api);

        let snap = fleet.snapshot().await;
        let names: Vec<&str> = snap.workspaces.iter().map(|w| w.name.as_str()).collect();
        assert_eq!(
            names,
            ["team-ws"],
            "surfaces the registered workspace, not synthetic 'k8s'"
        );
        assert!(
            !names.contains(&"k8s"),
            "no synthetic 'k8s' phantom when the registry is wired"
        );
        // Its agents are grouped under it, and its sources come from the CR.
        assert_eq!(snap.workspaces[0].agents.len(), 2, "both agents grouped");
        assert_eq!(snap.workspaces[0].sources.len(), 1);
        assert_eq!(snap.workspaces[0].sources[0].name, "caliban");
    }

    #[tokio::test]
    async fn k8s_snapshot_empty_registry_has_no_synthetic_workspace() {
        // #149: a fresh deploy with zero Workspace CRs must NOT surface a
        // synthetic 'k8s' workspace (which then errors 'workspace not
        // registered: k8s'); it shows an empty workspace set until one is
        // registered.
        use crate::k8s::fake::FakeWorkspaceApi;

        let api = MemTaskApi::new();
        let ws_api = Arc::new(FakeWorkspaceApi::new());
        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store).with_workspaces(ws_api);

        let snap = fleet.snapshot().await;
        assert!(
            snap.workspaces.is_empty(),
            "no phantom 'k8s' workspace when the registry is empty"
        );
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

    /// An `Ownership` that never wins a lease, so the background session-plane
    /// attach loop (`session.attach`, gated by `try_acquire`) never dials an
    /// agent. `send_input` is deliberately NOT ownership-gated, so this isolates
    /// its own `attach` round-trip from the poll loop's.
    #[cfg(feature = "k8s")]
    struct OwnsNothing;
    #[cfg(feature = "k8s")]
    #[async_trait]
    impl crate::ownership::Ownership for OwnsNothing {
        async fn try_acquire(&self, _key: &str) -> Option<crate::ownership::Lease> {
            None
        }
        async fn renew(&self, _lease: &crate::ownership::Lease) -> Result<()> {
            Ok(())
        }
        async fn release(&self, _key: &str) {}
        fn owns(&self, _key: &str) -> bool {
            false
        }
    }

    /// Find a single projected agent by id in a snapshot.
    #[cfg(feature = "k8s")]
    fn find_agent<'a>(
        snap: &'a crate::model::FleetSnapshot,
        id: &str,
    ) -> Option<&'a crate::model::Agent> {
        snap.workspaces
            .iter()
            .flat_map(|w| &w.agents)
            .find(|a| a.id == id)
    }

    /// #130: a `Running` k8s agent that the pod caliband reports as `Idle` +
    /// `interactive` must surface those from the pod's control `List` — not the
    /// coarse CR phase — so the dashboard reply box (`interactive && idle`) can
    /// appear. The CR still supplies membership; the pod supplies live detail.
    #[tokio::test]
    async fn k8s_snapshot_overlays_interactive_idle_from_pod_caliband() {
        let token = "overlay-test-token";
        let (mut fake, tls) = FakeCaliband::start_tcp_tls(token)
            .await
            .expect("start fake caliband over tcp+tls");

        // The pod caliband's `List` reports this agent as awaiting input.
        let id = "ct-interactive";
        fake.add_agent_tcp(id, Vec::new()).await;
        fake.set_status(id, AgentStatus::Idle);
        fake.set_interactive(id, true);

        // The CR: a Running task whose calibandEndpoint is the fake's control
        // addr. From the CR alone, `agent_from_task` yields Running + !interactive.
        let api = MemTaskApi::new();
        api.apply(&build_calibantask(&spec("repo-a", "p", None), id))
            .await
            .unwrap();
        api.set_running(id, &tls.addr);

        let (bus, store) = test_seams();
        let client_tls =
            crate::caliband::transport::tls_client_from_pem(&tls.ca_pem, "localhost").unwrap();
        let fleet = K8sFleet::new(api, bus, store)
            .with_network(Some(client_tls), Some(token.to_string()))
            .with_ownership(Arc::new(OwnsNothing));

        let snap = fleet.snapshot().await;
        let agent = find_agent(&snap, id).expect("agent projected");
        assert!(
            agent.interactive,
            "interactive must be overlaid from the pod caliband List, got {agent:?}"
        );
        assert_eq!(
            agent.status,
            AgentStatus::Idle,
            "status must be overlaid from the pod caliband List (awaiting input), got {agent:?}"
        );
    }

    /// #130: if the pod caliband is unreachable, the overlay must degrade —
    /// keep the agent with its CR-phase status, never drop it or fail the whole
    /// snapshot (mirrors the resilient-list posture, #148).
    #[tokio::test]
    async fn k8s_snapshot_overlay_degrades_when_pod_unreachable() {
        let id = "ct-unreachable";
        let api = MemTaskApi::new();
        api.apply(&build_calibantask(&spec("repo-a", "p", None), id))
            .await
            .unwrap();
        // Nothing is listening here; the overlay's List dial must fail fast.
        api.set_running(id, "127.0.0.1:1");

        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store).with_ownership(Arc::new(OwnsNothing));

        let snap = fleet.snapshot().await;
        let agent = find_agent(&snap, id).expect("agent still present despite unreachable pod");
        assert_eq!(
            agent.status,
            AgentStatus::Running,
            "falls back to the CR-phase status when the pod List fails"
        );
        assert!(
            !agent.interactive,
            "no interactive overlay when the pod List fails"
        );
    }

    /// #130: `send_input` must resolve the agent's per-agent endpoint via an
    /// `attach` control round-trip (as `LocalFleet` and the streaming path do)
    /// before delivering the inbound frame — not fire it at the pod's control
    /// endpoint. Proven by the fake recording the `Attach` for this id. With
    /// `OwnsNothing`, the background poll loop never attaches, so the only
    /// `Attach` the fake sees is `send_input`'s own.
    #[tokio::test]
    async fn k8s_send_input_resolves_per_agent_endpoint_via_attach() {
        let token = "send-input-test-token";
        let (mut fake, tls) = FakeCaliband::start_tcp_tls(token)
            .await
            .expect("start fake caliband over tcp+tls");

        let id = "ct-reply";
        fake.add_agent_tcp(id, Vec::new()).await;

        let api = MemTaskApi::new();
        api.apply(&build_calibantask(&spec("repo-a", "p", None), id))
            .await
            .unwrap();
        api.set_running(id, &tls.addr);

        let (bus, store) = test_seams();
        let client_tls =
            crate::caliband::transport::tls_client_from_pem(&tls.ca_pem, "localhost").unwrap();
        let fleet = K8sFleet::new(api, bus, store)
            .with_network(Some(client_tls), Some(token.to_string()))
            .with_ownership(Arc::new(OwnsNothing));

        fleet
            .send_input(
                &AgentId::from(id),
                crate::caliband::wire::AttachInbound::UserMessage {
                    text: "resume please".into(),
                },
            )
            .await
            .expect("send_input succeeds");

        assert!(
            fake.received_attach_ids().iter().any(|a| a == id),
            "send_input must issue an Attach round-trip to resolve the per-agent \
             endpoint; got attach ids {:?}",
            fake.received_attach_ids()
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
        assert_eq!(r.workspaces_healthy, 1);
    }

    // ---- #77 M2: single shared poll loop ----

    #[tokio::test]
    async fn watch_fleet_shared_loop_seeds_and_gones_once_per_subscriber() {
        use std::time::Duration;

        let api = MemTaskApi::new();
        api.apply(&build_calibantask(&spec("repo-a", "p", None), "a1"))
            .await
            .unwrap();
        let (bus, store) = test_seams();
        let fleet =
            K8sFleet::new(api, bus, store).with_watch_poll_interval(Duration::from_millis(20));

        // Two independent subscribers both seed the present agent as Discovered.
        let mut w1 = fleet.watch_fleet();
        let mut w2 = fleet.watch_fleet();
        for w in [&mut w1, &mut w2] {
            let ev = tokio::time::timeout(Duration::from_secs(1), w.next())
                .await
                .expect("seed Discovered timed out")
                .expect("stream ended");
            assert!(
                matches!(ev, FleetChange::Discovered { ref id, .. } if id.as_str() == "a1"),
                "each subscriber seeds the present agent: {ev:?}"
            );
        }

        // Delete → the shared loop broadcasts Gone once; every live subscriber
        // receives exactly one Gone for a1.
        fleet.api.delete("a1").await.unwrap();
        for w in [&mut w1, &mut w2] {
            let ev = tokio::time::timeout(Duration::from_secs(1), w.next())
                .await
                .expect("Gone timed out")
                .expect("stream ended");
            assert!(
                matches!(ev, FleetChange::Gone { ref id, .. } if id.as_str() == "a1"),
                "each live subscriber sees Gone once: {ev:?}"
            );
        }
    }

    // ---- #108 / #112 / #113: ownership-gated session plane ----

    /// A fake [`Ownership`] with a shared backing map (key → owner replica id):
    /// first replica to claim a key wins, a peer's `try_acquire` returns `None`,
    /// and `release` frees it — enough to prove the #108 attach gate elects one
    /// replica without a Postgres `LeasedOwnership`.
    struct FakeOwnership {
        replica: String,
        shared: Arc<Mutex<HashMap<String, String>>>,
    }

    #[async_trait]
    impl Ownership for FakeOwnership {
        async fn try_acquire(&self, stream_key: &str) -> Option<crate::ownership::Lease> {
            let mut m = self.shared.lock().unwrap();
            match m.get(stream_key) {
                Some(owner) if owner != &self.replica => None,
                _ => {
                    m.insert(stream_key.to_string(), self.replica.clone());
                    Some(crate::ownership::Lease {
                        stream_key: stream_key.to_string(),
                        epoch: 1,
                    })
                }
            }
        }
        async fn renew(&self, _lease: &crate::ownership::Lease) -> Result<()> {
            Ok(())
        }
        async fn release(&self, stream_key: &str) {
            let mut m = self.shared.lock().unwrap();
            if m.get(stream_key) == Some(&self.replica) {
                m.remove(stream_key);
            }
        }
        fn owns(&self, stream_key: &str) -> bool {
            self.shared.lock().unwrap().get(stream_key) == Some(&self.replica)
        }
    }

    fn session_with_ownership(ownership: Arc<dyn Ownership>) -> SessionPlane {
        let (bus, store) = test_seams();
        SessionPlane {
            emitter: Emitter::new(bus, store),
            tls: None,
            token: None,
            attached: Arc::new(Mutex::new(HashMap::new())),
            ownership,
            generation: Arc::new(AtomicU64::new(0)),
        }
    }

    /// #108 CORE: two replicas sharing one lease backing both try to attach the
    /// same agent; exactly one wins (the lease owner), so only that replica
    /// streams — no duplicate SSE / racing seq. The reservation entry is
    /// inserted synchronously before the (doomed) dial spawns, so the counts are
    /// deterministic right after `attach`.
    #[tokio::test]
    async fn attach_is_gated_by_ownership_so_only_one_replica_streams() {
        let shared = Arc::new(Mutex::new(HashMap::new()));
        let a = session_with_ownership(Arc::new(FakeOwnership {
            replica: "a".into(),
            shared: shared.clone(),
        }));
        let b = session_with_ownership(Arc::new(FakeOwnership {
            replica: "b".into(),
            shared: shared.clone(),
        }));
        let ep = Endpoint::Tcp {
            addr: "127.0.0.1:1".into(),
        };

        a.attach("repo-a", "agent-1", &ep).await;
        b.attach("repo-a", "agent-1", &ep).await;

        assert_eq!(
            a.active_count() + b.active_count(),
            1,
            "exactly one replica (the lease owner) attaches the agent"
        );
    }

    /// #108: `SelfOwnsAll` (the standalone default) always acquires, so the
    /// attach happens exactly as before — non-clustered behavior is unchanged.
    #[tokio::test]
    async fn self_owns_all_attaches_unconditionally() {
        let plane = session_with_ownership(Arc::new(SelfOwnsAll));
        let ep = Endpoint::Tcp {
            addr: "127.0.0.1:1".into(),
        };
        plane.attach("repo-a", "agent-1", &ep).await;
        assert_eq!(plane.active_count(), 1);
        // Idempotent: a second attach for the same id is a no-op, not a dupe.
        plane.attach("repo-a", "agent-1", &ep).await;
        assert_eq!(plane.active_count(), 1);
    }

    /// #112: `stop` promptly tears the attach task down and clears bookkeeping,
    /// so `metrics()` stops over-reporting and a re-attach isn't suppressed.
    #[tokio::test]
    async fn stop_agent_stops_the_attach_task() {
        // A real lease-tracking ownership (not `SelfOwnsAll`, whose `owns()` is
        // always true) so the release is observable.
        let plane = session_with_ownership(Arc::new(FakeOwnership {
            replica: "a".into(),
            shared: Arc::new(Mutex::new(HashMap::new())),
        }));
        let ep = Endpoint::Tcp {
            addr: "127.0.0.1:1".into(),
        };
        plane.attach("repo-a", "agent-1", &ep).await;
        assert_eq!(plane.active_count(), 1);

        plane.stop("agent-1").await;
        assert_eq!(plane.active_count(), 0, "stop clears the attach entry");
        assert!(
            !plane.ownership.owns("agent-1"),
            "stop releases the per-agent lease for prompt failover"
        );

        // After a stop the id can be re-attached (bookkeeping was cleared).
        plane.attach("repo-a", "agent-1", &ep).await;
        assert_eq!(
            plane.active_count(),
            1,
            "re-attach after stop is not a no-op"
        );
    }

    /// #112: `remove_agent` stops the live attach (not just deletes the CR), so
    /// the task stops dialing the dead endpoint and the active gauge drops.
    #[tokio::test]
    async fn remove_agent_stops_the_attach() {
        let api = MemTaskApi::new();
        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store);
        let ep = Endpoint::Tcp {
            addr: "127.0.0.1:1".into(),
        };
        fleet.start_agent_stream("repo-a", "agent-x", &ep).await;
        assert_eq!(fleet.session.active_count(), 1);

        fleet
            .remove_agent(&AgentId::from("agent-x"), true)
            .await
            .unwrap();
        assert_eq!(
            fleet.session.active_count(),
            0,
            "remove_agent stops the session-plane attach (#112)"
        );
    }

    /// #113: an agent this replica never spawned (operator/peer-created), once
    /// observed `Running` by the shared poll loop, gets attached by the lease
    /// owner — so its `/stream` isn't permanently empty. Uses `SelfOwnsAll` (the
    /// owner) + a short watch cadence and waits for the attach to land.
    #[tokio::test]
    async fn poll_loop_attaches_observed_running_agent_not_spawned_here() {
        let api = MemTaskApi::new();
        // Operator-created: applied + marked Running WITHOUT ensure_agent.
        api.apply(&build_calibantask(
            &spec("repo-a", "p", None),
            "operator-agent",
        ))
        .await
        .unwrap();
        api.set_running("operator-agent", "127.0.0.1:1");

        let (bus, store) = test_seams();
        let fleet =
            K8sFleet::new(api, bus, store).with_watch_poll_interval(Duration::from_millis(20));

        // The poll loop should observe it Running and attach it.
        let attached = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if fleet.session.active_count() == 1 {
                    break true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or(false);
        assert!(
            attached,
            "the lease owner attaches an observed-Running agent it never spawned (#113)"
        );
    }

    /// #108 + #113 compose: with a peer already holding the agent's lease, this
    /// replica's poll loop observes the same Running agent but does NOT attach —
    /// only the lease owner streams it.
    #[tokio::test]
    async fn poll_loop_does_not_attach_when_a_peer_owns_the_lease() {
        let shared = Arc::new(Mutex::new(HashMap::new()));
        // A peer already owns the agent's per-agent lease.
        shared
            .lock()
            .unwrap()
            .insert("operator-agent".to_string(), "peer".to_string());

        let api = MemTaskApi::new();
        api.apply(&build_calibantask(
            &spec("repo-a", "p", None),
            "operator-agent",
        ))
        .await
        .unwrap();
        api.set_running("operator-agent", "127.0.0.1:1");

        let (bus, store) = test_seams();
        let ours: Arc<dyn Ownership> = Arc::new(FakeOwnership {
            replica: "ours".into(),
            shared: shared.clone(),
        });
        let fleet = K8sFleet::new(api, bus, store)
            .with_ownership(ours)
            .with_watch_poll_interval(Duration::from_millis(20));

        // Give the poll loop several cycles; it must never attach a peer-owned agent.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            fleet.session.active_count(),
            0,
            "a peer owns the lease, so this replica must not attach (#108)"
        );
    }
}
