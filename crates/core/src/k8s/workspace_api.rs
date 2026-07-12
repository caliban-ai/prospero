//! The kube I/O seam for `Workspace` custom resources: CRUD that the k8s
//! config plane (`FleetAdmin` under k8s) drives, abstracted so its control logic
//! can be exercised against an in-memory fake (`super::fake::FakeWorkspaceApi`)
//! with no real apiserver — the same pattern as [`super::fleet::CalibanTaskApi`]
//! / `KubeTaskApi` (ADR 0007 / ADR 0008 §4).
//!
//! Prospero is a pure *editor* of the operator-owned `Workspace` CR (ADR 0008
//! §1): it writes the spec (`apply`) and reads config + reconciliation status
//! (`get`/`list`); the operator owns reconciliation and is the sole reader of
//! provider credential Secrets.

use async_trait::async_trait;

use crate::error::Result;
use crate::k8s::crd::Workspace;

/// CRUD over `Workspace` custom resources, scoped to prospero's configured
/// namespace. `delete` returns whether an object existed (so the API's
/// `remove_workspace` can report `Result<bool>` without a prior `get`).
#[async_trait]
pub trait WorkspaceApi: Send + Sync {
    /// Server-side-apply `ws` (create-or-update, keyed by its name).
    async fn apply(&self, ws: &Workspace) -> Result<()>;
    /// Fetch a `Workspace` by name, or `None` if it doesn't exist.
    async fn get(&self, name: &str) -> Result<Option<Workspace>>;
    /// List all `Workspace`s in the api's namespace (config + status).
    async fn list(&self) -> Result<Vec<Workspace>>;
    /// Delete a `Workspace` by name; `Ok(true)` if one existed, `Ok(false)` if
    /// it was already gone (idempotent).
    async fn delete(&self, name: &str) -> Result<bool>;
}

/// Real `WorkspaceApi` backed by a `kube::Api<Workspace>`.
#[cfg(feature = "k8s")]
pub struct KubeWorkspaceApi {
    api: kube::Api<Workspace>,
}

#[cfg(feature = "k8s")]
impl KubeWorkspaceApi {
    /// A `WorkspaceApi` scoped to `namespace` on `client`.
    #[must_use]
    pub fn new(client: kube::Client, namespace: &str) -> Self {
        Self {
            api: kube::Api::namespaced(client, namespace),
        }
    }
}

#[cfg(feature = "k8s")]
#[async_trait]
impl WorkspaceApi for KubeWorkspaceApi {
    async fn apply(&self, ws: &Workspace) -> Result<()> {
        use crate::k8s::fleet::map_kube_err;
        let name = ws.metadata.name.as_deref().ok_or_else(|| {
            crate::error::CoreError::Fleet("Workspace missing metadata.name".to_string())
        })?;
        let params = kube::api::PatchParams::apply("prospero").force();
        self.api
            .patch(name, &params, &kube::api::Patch::Apply(ws))
            .await
            .map_err(|e| map_kube_err("apply Workspace", e))?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Option<Workspace>> {
        use crate::k8s::fleet::map_kube_err;
        self.api
            .get_opt(name)
            .await
            .map_err(|e| map_kube_err("get Workspace", e))
    }

    async fn list(&self) -> Result<Vec<Workspace>> {
        use crate::k8s::fleet::map_kube_err;
        let list = self
            .api
            .list(&kube::api::ListParams::default())
            .await
            .map_err(|e| map_kube_err("list Workspace", e))?;
        Ok(list.items)
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        use crate::k8s::fleet::map_kube_err;
        match self
            .api
            .delete(name, &kube::api::DeleteParams::default())
            .await
        {
            Ok(_) => Ok(true),
            // Already gone: idempotent delete reports "did not exist".
            Err(kube::Error::Api(status)) if status.code == 404 => Ok(false),
            Err(e) => Err(map_kube_err("delete Workspace", e)),
        }
    }
}
