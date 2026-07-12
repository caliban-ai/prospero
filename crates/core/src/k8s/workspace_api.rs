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

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::fleet_provider::FleetAdmin;
use crate::k8s::crd::{
    CredentialsRef, EnvEntry, IsolationSpec, Provider, Source, Workspace, WorkspaceSpec,
};
use crate::registry::WorkspaceConfig;

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

/// Build a `Workspace` custom resource from the backend-neutral
/// [`WorkspaceConfig`] (snake_case) — mapping the rich fields onto the CR's
/// camelCase spec. `env` comes from the flattened `RepoProviderConfig.env`
/// (the workspace's non-secret environment). Credentials are carried through as
/// `credentialsRef` (Secret *by name*); prospero never reads the Secret.
#[must_use]
pub fn workspace_from_config(name: &str, config: &WorkspaceConfig) -> Workspace {
    let sources = config
        .sources
        .iter()
        .map(|s| Source {
            name: s.name.clone(),
            repo: s.repo.clone(),
            r#ref: s.r#ref.clone().unwrap_or_else(|| "main".to_string()),
            path: s.path.clone(),
        })
        .collect();
    let providers = config
        .providers
        .iter()
        .map(|p| Provider {
            name: p.name.clone(),
            kind: p.kind.clone(),
            base_url: p.base_url.clone(),
            model: p.model.clone(),
            credentials_ref: p.credentials_ref.as_ref().map(|c| CredentialsRef {
                secret_name: c.secret_name.clone(),
                key: c.key.clone(),
            }),
        })
        .collect();
    let env = config
        .local
        .env
        .iter()
        .map(|(name, value)| EnvEntry {
            name: name.clone(),
            value: value.clone(),
        })
        .collect();
    let isolation = config.isolation.as_ref().map(|i| IsolationSpec {
        runtime_class: i.runtime_class.clone(),
        worktrees: i.worktrees.clone(),
    });
    Workspace::new(
        name,
        WorkspaceSpec {
            display_name: config
                .display_name
                .clone()
                .unwrap_or_else(|| name.to_string()),
            sources,
            providers,
            default_provider: config.default_provider.clone(),
            env,
            isolation,
        },
    )
}

/// The k8s realization of [`FleetAdmin`]: a pure editor of operator-owned
/// `Workspace` CRs over a [`WorkspaceApi`]. Wiring this as the daemon's `admin`
/// seam removes the `405 Method Not Allowed` k8s workspaces hit today (#142).
/// Config writes are async — the operator reconciles the CR toward Ready/Failed.
pub struct K8sWorkspaceAdmin<W: WorkspaceApi> {
    api: Arc<W>,
}

impl<W: WorkspaceApi> K8sWorkspaceAdmin<W> {
    /// A `FleetAdmin` over `api` (scoped to prospero's namespace).
    #[must_use]
    pub fn new(api: Arc<W>) -> Self {
        Self { api }
    }
}

#[async_trait]
impl<W: WorkspaceApi> FleetAdmin for K8sWorkspaceAdmin<W> {
    async fn add_workspace(
        &self,
        name: String,
        _root: std::path::PathBuf,
        config: WorkspaceConfig,
    ) -> Result<()> {
        // k8s ignores `root` (a LocalFleet checkout path); sources come from
        // `config`. Server-side-apply is create-or-update.
        self.api.apply(&workspace_from_config(&name, &config)).await
    }

    async fn remove_workspace(&self, name: &str) -> Result<bool> {
        self.api.delete(name).await
    }

    async fn set_workspace_config(&self, name: &str, config: WorkspaceConfig) -> Result<()> {
        // Patch the CR's spec; the operator re-reconciles. A spec apply doesn't
        // touch the status subresource, so reconciliation state is preserved.
        self.api.apply(&workspace_from_config(name, &config)).await
    }

    async fn list_workspaces(&self) -> Result<Vec<crate::registry::WorkspaceInfo>> {
        Ok(self.api.list().await?.iter().map(workspace_info).collect())
    }

    fn workspace_ops_are_async(&self) -> bool {
        // Applying a Workspace CR only enqueues the operator's reconcile; the
        // config isn't live until it reaches `Ready`. The API answers `202`.
        true
    }
}

/// Project a `Workspace` CR onto the read-side [`WorkspaceInfo`]: config plus
/// reconciliation status, with credentials reduced to a `has_credentials` flag
/// (the Secret reference itself is never surfaced on the read side).
#[must_use]
fn workspace_info(ws: &Workspace) -> crate::registry::WorkspaceInfo {
    use crate::registry::{ProviderInfo, WorkspaceSourceSpec, WorkspaceStatusInfo};
    crate::registry::WorkspaceInfo {
        name: ws.metadata.name.clone().unwrap_or_default(),
        display_name: (!ws.spec.display_name.is_empty()).then(|| ws.spec.display_name.clone()),
        sources: ws
            .spec
            .sources
            .iter()
            .map(|s| WorkspaceSourceSpec {
                name: s.name.clone(),
                repo: s.repo.clone(),
                r#ref: Some(s.r#ref.clone()),
                path: s.path.clone(),
            })
            .collect(),
        providers: ws
            .spec
            .providers
            .iter()
            .map(|p| ProviderInfo {
                name: p.name.clone(),
                kind: p.kind.clone(),
                model: p.model.clone(),
                has_credentials: p.credentials_ref.is_some(),
            })
            .collect(),
        default_provider: ws.spec.default_provider.clone(),
        status: ws.status.as_ref().map(|s| WorkspaceStatusInfo {
            phase: s.phase.clone(),
            message: s.message.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::fake::FakeWorkspaceApi;
    use crate::registry::{CredentialsRef as CfgCredentialsRef, ProviderSpec, WorkspaceSourceSpec};

    fn rich_config() -> WorkspaceConfig {
        let mut config = WorkspaceConfig {
            display_name: Some("Team A".to_string()),
            sources: vec![WorkspaceSourceSpec {
                name: "caliban".to_string(),
                repo: "git@example:caliban".to_string(),
                r#ref: None,
                path: "/work/caliban".to_string(),
            }],
            providers: vec![
                ProviderSpec {
                    name: "planner".to_string(),
                    kind: "anthropic".to_string(),
                    base_url: None,
                    model: Some("claude-opus-4-8".to_string()),
                    credentials_ref: Some(CfgCredentialsRef {
                        secret_name: "anthropic-key".to_string(),
                        key: "api-key".to_string(),
                    }),
                },
                ProviderSpec {
                    name: "workers".to_string(),
                    kind: "ollama".to_string(),
                    base_url: Some("http://192.168.1.240:11434".to_string()),
                    model: Some("qwen2.5-coder".to_string()),
                    credentials_ref: None,
                },
            ],
            default_provider: Some("planner".to_string()),
            isolation: None,
            local: Default::default(),
        };
        config
            .local
            .env
            .insert("LOG".to_string(), "debug".to_string());
        config
    }

    #[test]
    fn workspace_from_config_maps_rich_fields_to_cr() {
        let ws = workspace_from_config("team-a-ws", &rich_config());
        assert_eq!(ws.metadata.name.as_deref(), Some("team-a-ws"));
        assert_eq!(ws.spec.display_name, "Team A");
        assert_eq!(ws.spec.sources[0].r#ref, "main"); // defaulted
        assert_eq!(ws.spec.providers.len(), 2);
        let cred = ws.spec.providers[0].credentials_ref.as_ref().unwrap();
        assert_eq!(cred.secret_name, "anthropic-key");
        assert!(ws.spec.providers[1].credentials_ref.is_none());
        assert_eq!(ws.spec.default_provider.as_deref(), Some("planner"));
        assert_eq!(ws.spec.env[0].name, "LOG");

        // Serialized CR uses camelCase (the operator's wire contract).
        let json = serde_json::to_value(&ws.spec).unwrap();
        assert!(json["displayName"].is_string());
        assert!(json["providers"][0]["credentialsRef"]["secretName"].is_string());
    }

    #[tokio::test]
    async fn admin_add_list_config_remove_round_trip() {
        let api = Arc::new(FakeWorkspaceApi::new());
        let admin = K8sWorkspaceAdmin::new(Arc::clone(&api));

        admin
            .add_workspace("team-a-ws".to_string(), "/ignored".into(), rich_config())
            .await
            .unwrap();

        // Persisted, with providers, surfaced via list (config plane, no 405).
        let listed = api.list().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].spec.providers.len(), 2);

        // Operator reconciles → status surfaces on the next list.
        api.set_status("team-a-ws", "Ready", None);
        assert_eq!(
            api.list().await.unwrap()[0].status.as_ref().unwrap().phase,
            "Ready"
        );

        // set_workspace_config patches spec without clobbering status.
        let mut updated = rich_config();
        updated.default_provider = Some("workers".to_string());
        admin
            .set_workspace_config("team-a-ws", updated)
            .await
            .unwrap();
        let after = api.get("team-a-ws").await.unwrap().unwrap();
        assert_eq!(after.spec.default_provider.as_deref(), Some("workers"));
        assert_eq!(after.status.as_ref().unwrap().phase, "Ready");

        // remove reports prior existence; a second remove reports absence.
        assert!(admin.remove_workspace("team-a-ws").await.unwrap());
        assert!(!admin.remove_workspace("team-a-ws").await.unwrap());
    }

    #[tokio::test]
    async fn list_workspaces_projects_config_and_status_for_the_read_side() {
        let api = Arc::new(FakeWorkspaceApi::new());
        let admin = K8sWorkspaceAdmin::new(Arc::clone(&api));
        admin
            .add_workspace("team-a-ws".to_string(), "/ignored".into(), rich_config())
            .await
            .unwrap();
        api.set_status(
            "team-a-ws",
            "Failed",
            Some("secret 'anthropic-key' not found"),
        );

        let infos = admin.list_workspaces().await.unwrap();
        assert_eq!(infos.len(), 1);
        let wi = &infos[0];
        assert_eq!(wi.name, "team-a-ws");
        assert_eq!(wi.display_name.as_deref(), Some("Team A"));
        assert_eq!(wi.default_provider.as_deref(), Some("planner"));
        assert_eq!(wi.providers.len(), 2);
        // Credentials reduced to a bool; the Secret ref is not surfaced.
        assert!(wi.providers[0].has_credentials);
        assert!(!wi.providers[1].has_credentials);
        assert_eq!(wi.providers[1].kind, "ollama");
        // Reconciliation status flows through for the dashboard pill/tooltip.
        let status = wi.status.as_ref().unwrap();
        assert_eq!(status.phase, "Failed");
        assert_eq!(
            status.message.as_deref(),
            Some("secret 'anthropic-key' not found")
        );
    }
}
