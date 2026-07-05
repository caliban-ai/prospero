//! `K8sFleet` — a Kubernetes `FleetProvider` backend that drives a fleet via
//! `CalibanTask` custom resources (ADR 0008 §2).
//!
//! The kube CRUD calls are behind the small [`CalibanTaskApi`] seam so
//! `K8sFleet`'s ensure/stop/restart logic is unit-testable against an
//! in-memory fake with **no real cluster**. `watch_fleet` is Task B3's job;
//! this module leaves it as an empty stream (see the `FleetProvider` impl
//! below) so the crate still compiles and existing conformance-style callers
//! don't panic against a `todo!()`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use sha2::{Digest, Sha256};

use crate::caliband::wire::Endpoint;
use crate::error::{CoreError, Result};
use crate::fleet_provider::FleetProvider;
use crate::k8s::crd::{CalibanTask, CalibanTaskSpec, Source, TaskSpec as CrdTaskSpec, Workspace};
use crate::model::{AgentHandle, AgentId, DrainPolicy, FleetChange, TaskSpec};

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
    hasher.update(spec.repo.as_bytes());
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
/// **one** [`Source`] built from `spec.repo`, checked out at `main` and
/// mounted at `/work/<repo>`. Multi-source workspaces, non-`main` refs, and
/// isolation-from-request mapping are out of scope until a later task widens
/// `TaskSpec`/`SpawnRequest` to carry that information explicitly.
#[must_use]
pub fn build_calibantask(spec: &TaskSpec, name: &str) -> CalibanTask {
    let source = Source {
        name: spec.repo.clone(),
        repo: spec.repo.clone(),
        r#ref: "main".to_string(),
        path: format!("/work/{}", spec.repo),
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
        repo,
        endpoint: Endpoint::Tcp {
            addr: endpoint_addr.clone(),
        },
    })
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

/// A Kubernetes `FleetProvider` backend: drives a fleet by CRUD + (eventually,
/// Task B3) watch on `CalibanTask` custom resources.
pub struct K8sFleet<A: CalibanTaskApi> {
    api: A,
    poll: PollConfig,
    restart_nonce: AtomicU64,
}

impl<A: CalibanTaskApi> K8sFleet<A> {
    /// A `K8sFleet` with the default (~30s) `ensure_agent` poll budget.
    #[must_use]
    pub fn new(api: A) -> Self {
        Self::with_poll_config(api, PollConfig::default())
    }

    /// A `K8sFleet` with an explicit poll budget — tests use a short deadline
    /// so a never-Running CR fails fast instead of hanging ~30s.
    #[must_use]
    pub fn with_poll_config(api: A, poll: PollConfig) -> Self {
        Self {
            api,
            poll,
            restart_nonce: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl<A: CalibanTaskApi + 'static> FleetProvider for K8sFleet<A> {
    async fn ensure_agent(&self, spec: TaskSpec) -> Result<AgentHandle> {
        let name = task_name(&spec);
        let repo = spec.repo.clone();
        let ct = build_calibantask(&spec, &name);
        self.api.apply(&ct).await?;

        let deadline = tokio::time::Instant::now() + self.poll.deadline;
        loop {
            if let Some(task) = self.api.get(&name).await?
                && let Some(handle) = handle_from(&task, repo.clone())
            {
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
        // TODO(B3): drive a `kube::runtime::watcher` over `CalibanTask` and
        // translate Applied/Deleted + phase transitions into `FleetChange`,
        // seeded by an initial `list`. Left as an empty stream for now so the
        // crate compiles and callers that merely hold a `BoxStream` (rather
        // than block on an item from it) don't panic against a `todo!()`.
        stream::empty().boxed()
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
            repo: repo.to_string(),
            request,
        }
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
        let fleet = K8sFleet::with_poll_config(
            api,
            PollConfig {
                deadline: Duration::from_secs(5),
                interval: Duration::from_millis(10),
            },
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
        assert_eq!(handle.repo, "repo-a");
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
        let fleet = K8sFleet::with_poll_config(
            api,
            PollConfig {
                deadline: Duration::from_millis(50),
                interval: Duration::from_millis(10),
            },
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
        let fleet = K8sFleet::new(api);

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
        let fleet = K8sFleet::new(api);

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
}
