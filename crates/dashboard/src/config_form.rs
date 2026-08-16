//! Workspace configuration form state and validation.
//!
//! Two shapes, chosen at runtime by `capabilities.async_workspace_ops`: the
//! local backend's single-provider/env form, and the k8s `Workspace`-CR editor
//! with named providers and Secret references.
//!
//! All of it is plain data with **no Dioxus**, so the fiddly parts — which
//! blank fields get omitted, what the CRD's `minItems` demands, how a default
//! provider survives a rename — are unit-tested on the host target rather than
//! discovered in a browser.

use std::collections::BTreeMap;

use prospero_types::{
    CredentialsRef, ProviderInfo, ProviderSpec, RepoProviderConfig, WorkspaceConfig,
    WorkspaceSourceSpec,
};

/// Provider kinds offered in the pickers. Mirrors v1's list.
pub const PROVIDER_KINDS: [&str; 6] = [
    "ollama",
    "anthropic",
    "openai",
    "google",
    "bedrock",
    "vertex",
];

/// Trim, and treat blank as absent.
fn some_trimmed(s: &str) -> Option<String> {
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

/// Collect `(key, value)` rows into a map, dropping rows with a blank key.
///
/// A blank *value* is kept: `FOO=` is a meaningful override.
fn env_map(rows: &[(String, String)]) -> BTreeMap<String, String> {
    rows.iter()
        .filter_map(|(k, v)| {
            let k = k.trim();
            (!k.is_empty()).then(|| (k.to_string(), v.trim().to_string()))
        })
        .collect()
}

// --- Local ------------------------------------------------------------------

/// The local backend's single-provider form.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalForm {
    /// Provider identifier (`ollama`, `anthropic`, …). Blank ⇒ backend default.
    pub provider: String,
    /// Provider base URL.
    pub base_url: String,
    /// **Name** of an env var in prosperod's environment holding the API key.
    /// Never the secret itself.
    pub api_key_from_env: String,
    /// Raw env overrides.
    pub env: Vec<(String, String)>,
}

impl LocalForm {
    /// Prefill from a workspace's persisted config.
    pub fn from_config(cfg: &RepoProviderConfig) -> Self {
        Self {
            provider: cfg.provider.clone().unwrap_or_default(),
            base_url: cfg.base_url.clone().unwrap_or_default(),
            api_key_from_env: cfg.api_key_from_env.clone().unwrap_or_default(),
            env: cfg
                .env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }

    /// Build the request payload. Blank fields are omitted rather than sent as
    /// empty strings, which the backend would treat as "set to empty".
    pub fn to_config(&self) -> WorkspaceConfig {
        WorkspaceConfig {
            local: RepoProviderConfig {
                provider: some_trimmed(&self.provider),
                base_url: some_trimmed(&self.base_url),
                api_key_from_env: some_trimmed(&self.api_key_from_env),
                env: env_map(&self.env),
            },
            ..Default::default()
        }
    }

    /// Reject what the backend would reject, before a round-trip.
    pub fn validate(&self) -> Result<(), String> {
        // #120: an api_key_from_env on a keyless provider is silently ignored
        // server-side, which looks like the credential was accepted. Say so.
        if !self.api_key_from_env.trim().is_empty() && self.provider.trim() == "ollama" {
            return Err(
                "ollama takes no API key — clear the env-var field, or pick another provider."
                    .into(),
            );
        }
        Ok(())
    }
}

// --- k8s --------------------------------------------------------------------

/// One source checkout row in the k8s editor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceRow {
    /// Source identifier.
    pub name: String,
    /// Git remote to clone.
    pub repo: String,
    /// Git ref; blank ⇒ the operator's default (`main`).
    pub r#ref: String,
    /// Absolute mount path in the pod.
    pub path: String,
}

impl SourceRow {
    /// A row the operator has not started filling in.
    fn is_blank(&self) -> bool {
        self.name.trim().is_empty() && self.repo.trim().is_empty() && self.path.trim().is_empty()
    }
}

/// One named-provider row in the k8s editor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderRow {
    /// Provider name, unique within the workspace.
    pub name: String,
    /// Provider kind.
    pub kind: String,
    /// Override base URL.
    pub base_url: String,
    /// Default model.
    pub model: String,
    /// Name of an existing Secret holding the API key.
    pub secret_name: String,
    /// Key within that Secret.
    pub secret_key: String,
}

/// The k8s `Workspace`-CR editor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct K8sForm {
    /// Human-friendly dashboard label.
    pub display_name: String,
    /// Source checkouts (CRD requires at least one).
    pub sources: Vec<SourceRow>,
    /// Named providers (CRD requires at least one).
    pub providers: Vec<ProviderRow>,
    /// Which provider agents bind when they request none.
    pub default_provider: Option<String>,
    /// Raw env overrides.
    pub env: Vec<(String, String)>,
}

impl K8sForm {
    /// A blank form, seeded with one empty row of each kind so the operator has
    /// somewhere to type.
    pub fn blank() -> Self {
        Self {
            sources: vec![SourceRow::default()],
            providers: vec![ProviderRow {
                kind: PROVIDER_KINDS[0].to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Prefill from what `GET /api/workspaces` returned.
    ///
    /// **Secret references are deliberately not readable** — the API never
    /// returns them — so `secret_name`/`secret_key` start blank on an edit even
    /// for a provider that has credentials. [`credentials_warning`] exists to
    /// say so, because silently saving would strip the credential.
    pub fn from_summary(
        display_name: Option<&str>,
        sources: &[WorkspaceSourceSpec],
        providers: &[ProviderInfo],
        default_provider: Option<&str>,
    ) -> Self {
        let sources: Vec<SourceRow> = sources
            .iter()
            .map(|s| SourceRow {
                name: s.name.clone(),
                repo: s.repo.clone(),
                r#ref: s.r#ref.clone().unwrap_or_default(),
                path: s.path.clone(),
            })
            .collect();
        let providers: Vec<ProviderRow> = providers
            .iter()
            .map(|p| ProviderRow {
                name: p.name.clone(),
                kind: p.kind.clone(),
                // Read back rather than blanked: an empty box here would save
                // as "no base URL" and silently unpick a self-hosted provider
                // (#188). Secret refs below stay blank — those really are
                // unreadable, and blanking them is the documented behaviour.
                base_url: p.base_url.clone().unwrap_or_default(),
                model: p.model.clone().unwrap_or_default(),
                secret_name: String::new(),
                secret_key: String::new(),
            })
            .collect();
        Self {
            display_name: display_name.unwrap_or_default().to_string(),
            sources: if sources.is_empty() {
                vec![SourceRow::default()]
            } else {
                sources
            },
            providers: if providers.is_empty() {
                vec![ProviderRow {
                    kind: PROVIDER_KINDS[0].to_string(),
                    ..Default::default()
                }]
            } else {
                providers
            },
            default_provider: default_provider.map(str::to_string),
            env: Vec::new(),
        }
    }

    /// Whether any provider on the original workspace held credentials that the
    /// operator has not re-entered — saving now would make it keyless.
    ///
    /// The API never returns Secret references, so this cannot be inferred from
    /// the form alone; the caller passes what the read side reported.
    pub fn credentials_warning(&self, had_credentials: &[&str]) -> Option<String> {
        let at_risk: Vec<&str> = self
            .providers
            .iter()
            .filter(|p| {
                had_credentials.contains(&p.name.as_str()) && p.secret_name.trim().is_empty()
            })
            .map(|p| p.name.as_str())
            .collect();
        if at_risk.is_empty() {
            return None;
        }
        Some(format!(
            "Credentials are never read back. Re-enter the Secret for {} or saving will \
             leave {} keyless.",
            at_risk.join(", "),
            if at_risk.len() == 1 { "it" } else { "them" }
        ))
    }

    /// Build the request payload, dropping rows the operator never filled in.
    pub fn to_config(&self) -> WorkspaceConfig {
        let sources = self
            .sources
            .iter()
            .filter(|s| !s.is_blank())
            .map(|s| WorkspaceSourceSpec {
                name: s.name.trim().to_string(),
                repo: s.repo.trim().to_string(),
                r#ref: some_trimmed(&s.r#ref),
                path: s.path.trim().to_string(),
            })
            .collect();

        let providers: Vec<ProviderSpec> = self
            .providers
            .iter()
            .filter(|p| !p.name.trim().is_empty())
            .map(|p| ProviderSpec {
                name: p.name.trim().to_string(),
                kind: p.kind.trim().to_string(),
                base_url: some_trimmed(&p.base_url),
                model: some_trimmed(&p.model),
                // Only send a reference when a Secret name was actually given;
                // a name without a key is incomplete and would be rejected.
                credentials_ref: some_trimmed(&p.secret_name).map(|secret_name| CredentialsRef {
                    secret_name,
                    key: p.secret_key.trim().to_string(),
                }),
            })
            .collect();

        // Only keep a default that still names a provider on the form — a
        // renamed or deleted provider must not leave a dangling default the
        // operator would then have to debug from a reconcile failure.
        let default_provider = self
            .default_provider
            .as_deref()
            .and_then(some_trimmed)
            .filter(|d| providers.iter().any(|p| &p.name == d));

        WorkspaceConfig {
            display_name: some_trimmed(&self.display_name),
            sources,
            providers,
            default_provider,
            isolation: None,
            local: RepoProviderConfig {
                env: env_map(&self.env),
                ..Default::default()
            },
        }
    }

    /// Reject what the CRD would reject.
    ///
    /// The `Workspace` CRD sets `minItems: 1` on both `sources` and
    /// `providers`, and requires every source to carry a name, repo, and path.
    /// Submitting less produced an opaque 422 (#150), so catch it inline where
    /// the operator can see which field is missing.
    pub fn validate(&self) -> Result<(), String> {
        let cfg = self.to_config();

        if cfg.sources.is_empty() {
            return Err("Add at least one source — a name, git remote, and mount path.".into());
        }
        for s in &cfg.sources {
            if s.name.is_empty() || s.repo.is_empty() || s.path.is_empty() {
                return Err(format!(
                    "Source \"{}\" needs a name, a git remote, and a mount path.",
                    if s.name.is_empty() {
                        "(unnamed)"
                    } else {
                        &s.name
                    }
                ));
            }
            if !s.path.starts_with('/') {
                return Err(format!(
                    "Source \"{}\" needs an absolute mount path (starting with /).",
                    s.name
                ));
            }
        }

        if cfg.providers.is_empty() {
            return Err("Add at least one provider — a name and a kind.".into());
        }
        for p in &cfg.providers {
            if p.kind.is_empty() {
                return Err(format!("Provider \"{}\" needs a kind.", p.name));
            }
            // A Secret name without a key addresses nothing.
            if let Some(c) = &p.credentials_ref
                && c.key.is_empty()
            {
                return Err(format!(
                    "Provider \"{}\" names Secret \"{}\" but no key within it.",
                    p.name, c.secret_name
                ));
            }
        }

        // Duplicate names would silently collide inside the CR.
        let mut seen = Vec::new();
        for p in &cfg.providers {
            if seen.contains(&p.name) {
                return Err(format!("Two providers are both named \"{}\".", p.name));
            }
            seen.push(p.name.clone());
        }
        let mut seen = Vec::new();
        for s in &cfg.sources {
            if seen.contains(&s.name) {
                return Err(format!("Two sources are both named \"{}\".", s.name));
            }
            seen.push(s.name.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filled_source() -> SourceRow {
        SourceRow {
            name: "caliban".into(),
            repo: "git@github.com:caliban-ai/caliban.git".into(),
            r#ref: String::new(),
            path: "/work/caliban".into(),
        }
    }

    fn filled_provider() -> ProviderRow {
        ProviderRow {
            name: "planner".into(),
            kind: "anthropic".into(),
            base_url: String::new(),
            model: "claude-opus-5".into(),
            secret_name: "llm-keys".into(),
            secret_key: "anthropic".into(),
        }
    }

    fn valid_form() -> K8sForm {
        K8sForm {
            display_name: "Team A".into(),
            sources: vec![filled_source()],
            providers: vec![filled_provider()],
            default_provider: Some("planner".into()),
            env: vec![],
        }
    }

    // --- local ---

    #[test]
    fn local_blank_fields_are_omitted_not_sent_empty() {
        let cfg = LocalForm::default().to_config();
        assert_eq!(cfg.local.provider, None);
        assert_eq!(cfg.local.base_url, None);
        assert_eq!(cfg.local.api_key_from_env, None);
        assert!(cfg.local.env.is_empty());
        // Serialising must not emit the keys at all.
        let json = serde_json::to_string(&cfg).unwrap();
        assert_eq!(
            json, "{}",
            "blank local form should serialise empty: {json}"
        );
    }

    #[test]
    fn local_trims_and_round_trips_through_config() {
        let form = LocalForm {
            provider: "  anthropic ".into(),
            base_url: " https://api.example ".into(),
            api_key_from_env: " ANTHROPIC_API_KEY ".into(),
            env: vec![("  FOO ".into(), " bar ".into())],
        };
        let cfg = form.to_config();
        assert_eq!(cfg.local.provider.as_deref(), Some("anthropic"));
        assert_eq!(cfg.local.base_url.as_deref(), Some("https://api.example"));
        assert_eq!(
            cfg.local.api_key_from_env.as_deref(),
            Some("ANTHROPIC_API_KEY")
        );
        assert_eq!(cfg.local.env.get("FOO").map(String::as_str), Some("bar"));

        // And it survives a trip back into the form.
        let back = LocalForm::from_config(&cfg.local);
        assert_eq!(back.provider, "anthropic");
        assert_eq!(back.env, vec![("FOO".to_string(), "bar".to_string())]);
    }

    #[test]
    fn local_env_keeps_a_blank_value_but_drops_a_blank_key() {
        let form = LocalForm {
            env: vec![
                ("SET_EMPTY".into(), String::new()),
                (String::new(), "orphan".into()),
                ("   ".into(), "whitespace-key".into()),
            ],
            ..Default::default()
        };
        let env = form.to_config().local.env;
        assert_eq!(env.get("SET_EMPTY").map(String::as_str), Some(""));
        assert_eq!(env.len(), 1, "blank keys must be dropped: {env:?}");
    }

    #[test]
    fn local_rejects_an_api_key_on_a_keyless_provider() {
        // #120: the server accepts then ignores this, which reads as success.
        let form = LocalForm {
            provider: "ollama".into(),
            api_key_from_env: "OLLAMA_KEY".into(),
            ..Default::default()
        };
        assert!(form.validate().is_err());

        let ok = LocalForm {
            provider: "anthropic".into(),
            api_key_from_env: "ANTHROPIC_API_KEY".into(),
            ..Default::default()
        };
        assert!(ok.validate().is_ok());
    }

    // --- k8s validation (#150) ---

    #[test]
    fn k8s_a_valid_form_passes() {
        assert_eq!(valid_form().validate(), Ok(()));
    }

    /// A self-hosted provider's base URL must survive create *and* the edit
    /// round-trip. Without it the worker falls back to the in-pod default and
    /// every agent dies at preflight with `ProviderError` (#188).
    #[test]
    fn k8s_provider_base_url_round_trips() {
        let mut form = valid_form();
        form.providers[0].base_url = " http://192.168.1.240:11434 ".into();

        let cfg = form.to_config();
        assert_eq!(
            cfg.providers[0].base_url.as_deref(),
            Some("http://192.168.1.240:11434"),
            "base URL must reach the CR, trimmed"
        );

        // Reopening the editor must show it again. Projecting it away here is
        // what made a routine model edit wipe the base URL.
        let providers: Vec<ProviderInfo> = cfg
            .providers
            .iter()
            .map(|p| ProviderInfo {
                name: p.name.clone(),
                kind: p.kind.clone(),
                base_url: p.base_url.clone(),
                model: p.model.clone(),
                has_credentials: p.credentials_ref.is_some(),
            })
            .collect();
        let reopened = K8sForm::from_summary(
            cfg.display_name.as_deref(),
            &cfg.sources,
            &providers,
            cfg.default_provider.as_deref(),
        );
        assert_eq!(
            reopened.providers[0].base_url, "http://192.168.1.240:11434",
            "the editor must show the saved base URL, not a blank box"
        );
        assert_eq!(
            reopened.to_config().providers[0].base_url.as_deref(),
            Some("http://192.168.1.240:11434"),
            "and saving again must not drop it"
        );
    }

    #[test]
    fn k8s_requires_at_least_one_source_and_provider() {
        // The CRD's minItems:1 — submitting less produced an opaque 422.
        let no_sources = K8sForm {
            sources: vec![SourceRow::default()],
            ..valid_form()
        };
        assert!(no_sources.validate().is_err());

        let no_providers = K8sForm {
            providers: vec![],
            ..valid_form()
        };
        assert!(no_providers.validate().is_err());

        // A pristine blank form has placeholder rows but no real content.
        assert!(K8sForm::blank().validate().is_err());
    }

    #[test]
    fn k8s_a_partly_filled_source_is_rejected_by_name() {
        let form = K8sForm {
            sources: vec![SourceRow {
                name: "caliban".into(),
                repo: String::new(),
                r#ref: String::new(),
                path: "/work/caliban".into(),
            }],
            ..valid_form()
        };
        let err = form.validate().unwrap_err();
        assert!(
            err.contains("caliban"),
            "error should name the source: {err}"
        );
    }

    #[test]
    fn k8s_source_path_must_be_absolute() {
        let form = K8sForm {
            sources: vec![SourceRow {
                path: "work/caliban".into(),
                ..filled_source()
            }],
            ..valid_form()
        };
        assert!(form.validate().unwrap_err().contains("absolute"));
    }

    #[test]
    fn k8s_a_secret_without_a_key_is_rejected() {
        let form = K8sForm {
            providers: vec![ProviderRow {
                secret_name: "llm-keys".into(),
                secret_key: String::new(),
                ..filled_provider()
            }],
            ..valid_form()
        };
        let err = form.validate().unwrap_err();
        assert!(err.contains("llm-keys"), "should name the Secret: {err}");
    }

    #[test]
    fn k8s_duplicate_names_are_rejected() {
        let dup_providers = K8sForm {
            providers: vec![filled_provider(), filled_provider()],
            ..valid_form()
        };
        assert!(dup_providers.validate().unwrap_err().contains("planner"));

        let dup_sources = K8sForm {
            sources: vec![filled_source(), filled_source()],
            ..valid_form()
        };
        assert!(dup_sources.validate().unwrap_err().contains("caliban"));
    }

    // --- k8s payload ---

    #[test]
    fn k8s_blank_rows_are_dropped_from_the_payload() {
        let form = K8sForm {
            sources: vec![filled_source(), SourceRow::default()],
            providers: vec![filled_provider(), ProviderRow::default()],
            ..valid_form()
        };
        let cfg = form.to_config();
        assert_eq!(cfg.sources.len(), 1);
        assert_eq!(cfg.providers.len(), 1);
    }

    #[test]
    fn k8s_omits_a_blank_ref_so_the_operator_default_applies() {
        let cfg = valid_form().to_config();
        assert_eq!(cfg.sources[0].r#ref, None);

        let pinned = K8sForm {
            sources: vec![SourceRow {
                r#ref: "release-1.2".into(),
                ..filled_source()
            }],
            ..valid_form()
        };
        assert_eq!(
            pinned.to_config().sources[0].r#ref.as_deref(),
            Some("release-1.2")
        );
    }

    #[test]
    fn k8s_credentials_ref_is_only_sent_when_a_secret_is_named() {
        let with = valid_form().to_config();
        let c = with.providers[0].credentials_ref.as_ref().unwrap();
        assert_eq!(c.secret_name, "llm-keys");
        assert_eq!(c.key, "anthropic");

        let keyless = K8sForm {
            providers: vec![ProviderRow {
                secret_name: String::new(),
                secret_key: String::new(),
                ..filled_provider()
            }],
            ..valid_form()
        };
        assert_eq!(keyless.to_config().providers[0].credentials_ref, None);
    }

    /// A default pointing at a provider that was renamed or deleted would
    /// reconcile-fail with a message far from the form that caused it.
    #[test]
    fn k8s_a_dangling_default_provider_is_dropped() {
        let renamed = K8sForm {
            default_provider: Some("planner".into()),
            providers: vec![ProviderRow {
                name: "planner-v2".into(),
                ..filled_provider()
            }],
            ..valid_form()
        };
        assert_eq!(renamed.to_config().default_provider, None);

        // The happy path still keeps it.
        assert_eq!(
            valid_form().to_config().default_provider.as_deref(),
            Some("planner")
        );
    }

    // --- k8s prefill + the credentials trap ---

    #[test]
    fn k8s_prefill_leaves_secret_fields_blank_because_they_are_never_returned() {
        let providers = vec![ProviderInfo {
            name: "planner".into(),
            kind: "anthropic".into(),
            base_url: None,
            model: Some("claude-opus-5".into()),
            has_credentials: true,
        }];
        let form = K8sForm::from_summary(Some("Team A"), &[], &providers, Some("planner"));
        assert_eq!(form.providers[0].name, "planner");
        assert_eq!(form.providers[0].model, "claude-opus-5");
        assert!(
            form.providers[0].secret_name.is_empty(),
            "the API never returns Secret references"
        );
    }

    /// Saving an edited workspace without re-entering a Secret silently strips
    /// the credential. The operator has to be told before they click save.
    #[test]
    fn k8s_warns_when_saving_would_strip_a_credential() {
        let form = K8sForm {
            providers: vec![ProviderRow {
                secret_name: String::new(),
                secret_key: String::new(),
                ..filled_provider()
            }],
            ..valid_form()
        };
        let warning = form.credentials_warning(&["planner"]).unwrap();
        assert!(warning.contains("planner"), "must name it: {warning}");

        // Re-entered ⇒ no warning.
        assert_eq!(valid_form().credentials_warning(&["planner"]), None);
        // Never had one ⇒ no warning.
        assert_eq!(form.credentials_warning(&[]), None);
    }

    #[test]
    fn k8s_prefill_seeds_an_empty_row_so_there_is_somewhere_to_type() {
        let form = K8sForm::from_summary(None, &[], &[], None);
        assert_eq!(form.sources.len(), 1);
        assert_eq!(form.providers.len(), 1);
        assert!(form.providers[0].kind == PROVIDER_KINDS[0]);
    }
}
