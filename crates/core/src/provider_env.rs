//! Pure resolution of a repo's caliband environment overlay.
//!
//! Combines the prosperod-level default env, a repo's curated provider fields,
//! and its raw env map into one overlay applied to the caliband process.

use std::collections::BTreeMap;

use crate::registry::RepoProviderConfig;

/// `(base_url_var, api_key_var)` for a provider, or `(None, None)` for
/// provider-only backends (bedrock/vertex use ambient cloud credentials).
fn provider_vars(provider: &str) -> (Option<&'static str>, Option<&'static str>) {
    match provider {
        "ollama" => (Some("OLLAMA_BASE_URL"), None),
        "anthropic" => (Some("ANTHROPIC_BASE_URL"), Some("ANTHROPIC_API_KEY")),
        "openai" => (Some("OPENAI_BASE_URL"), Some("OPENAI_API_KEY")),
        "google" => (Some("GEMINI_BASE_URL"), Some("GEMINI_API_KEY")),
        _ => (None, None), // bedrock, vertex, unknown
    }
}

/// Resolve the environment overlay for a repo's caliband daemon.
///
/// Layered lowest → highest: `default_env` (global) → curated provider fields →
/// `cfg.env` (raw). `process_env` looks up prosperod's own environment for
/// `api_key_from_env` references.
pub fn resolve_env(
    default_env: &BTreeMap<String, String>,
    cfg: &RepoProviderConfig,
    process_env: &dyn Fn(&str) -> Option<String>,
) -> BTreeMap<String, String> {
    let mut out = default_env.clone();

    if let Some(provider) = &cfg.provider {
        out.insert("CALIBAN_PROVIDER".to_string(), provider.clone());
        let (base_var, key_var) = provider_vars(provider);
        if let Some(base_url) = &cfg.base_url {
            match base_var {
                Some(var) => {
                    out.insert(var.to_string(), base_url.clone());
                }
                None => tracing::warn!(
                    target: "prospero_provider_env",
                    provider, "base_url set but provider has no base-URL env var; ignored"
                ),
            }
        }
        if let Some(name) = &cfg.api_key_from_env {
            match key_var {
                Some(var) => match process_env(name) {
                    Some(value) => {
                        out.insert(var.to_string(), value);
                    }
                    None => tracing::warn!(
                        target: "prospero_provider_env",
                        env_var = %name,
                        "api_key_from_env references an unset variable; skipped"
                    ),
                },
                None => tracing::warn!(
                    target: "prospero_provider_env",
                    provider, "api_key_from_env set but provider has no api-key env var; ignored"
                ),
            }
        }
    }

    for (k, v) in &cfg.env {
        out.insert(k.clone(), v.clone());
    }
    out
}

/// Check that a *resolved* env satisfies the selected provider's API-key
/// requirement, returning an actionable message when it does not.
///
/// This complements [`resolve_env`], which only `warn!`s on a dangling
/// `api_key_from_env` reference and then proceeds. Validating the resolved map
/// (rather than the raw config) means a key supplied through any layer —
/// curated `api_key_from_env`, raw `cfg.env`, or the global `default_env` —
/// counts, so there are no false positives. Providers without a key var
/// (ollama, bedrock, vertex) and an unset provider always pass.
pub fn validate_provider_env(
    cfg: &RepoProviderConfig,
    resolved: &BTreeMap<String, String>,
) -> std::result::Result<(), String> {
    let Some(provider) = &cfg.provider else {
        return Ok(());
    };
    let (_base_var, key_var) = provider_vars(provider);
    let Some(key_var) = key_var else {
        return Ok(());
    };
    if resolved.get(key_var).is_some_and(|v| !v.is_empty()) {
        return Ok(());
    }
    Err(match &cfg.api_key_from_env {
        Some(name) => format!(
            "provider '{provider}' requires {key_var}, but api_key_from_env \
             references '{name}', which is unset in prosperod's environment"
        ),
        None => format!(
            "provider '{provider}' requires {key_var}, but no value is configured \
             (set api_key_from_env to a variable in prosperod's environment, or \
             supply {key_var} in the repo env)"
        ),
    })
}

/// Statically validate a repo's provider config for internal coherence,
/// independent of the runtime environment — so settings that would be silently
/// dropped at spawn/poll time are rejected at config-set time with an
/// actionable message instead.
///
/// Today this catches an `api_key_from_env` on a provider that has no api-key
/// env var (ollama, bedrock, vertex, unknown): [`resolve_env`] only `warn!`s
/// `"api_key_from_env set but provider has no api-key env var; ignored"` and
/// proceeds, so the setting looks accepted but never takes effect. Surfacing it
/// here lets the config-set path return a `400` (#120).
///
/// An unset provider always passes (nothing to validate).
pub fn validate_provider_config(cfg: &RepoProviderConfig) -> std::result::Result<(), String> {
    let Some(provider) = &cfg.provider else {
        return Ok(());
    };
    let (_base_var, key_var) = provider_vars(provider);
    if cfg.api_key_from_env.is_some() && key_var.is_none() {
        return Err(format!(
            "provider '{provider}' has no api-key env var, so api_key_from_env \
             would be ignored; remove api_key_from_env for this provider (it is \
             only meaningful for anthropic/openai/google)"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RepoProviderConfig {
        RepoProviderConfig::default()
    }
    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn provider_and_base_url_map_to_env_vars() {
        let mut c = cfg();
        c.provider = Some("ollama".into());
        c.base_url = Some("http://h:11434".into());
        let out = resolve_env(&BTreeMap::new(), &c, &no_env);
        assert_eq!(out.get("CALIBAN_PROVIDER").unwrap(), "ollama");
        assert_eq!(out.get("OLLAMA_BASE_URL").unwrap(), "http://h:11434");
    }

    #[test]
    fn api_key_from_env_is_resolved_from_process_env() {
        let mut c = cfg();
        c.provider = Some("anthropic".into());
        c.api_key_from_env = Some("MY_KEY".into());
        let proc = |k: &str| (k == "MY_KEY").then(|| "secret-value".to_string());
        let out = resolve_env(&BTreeMap::new(), &c, &proc);
        assert_eq!(out.get("ANTHROPIC_API_KEY").unwrap(), "secret-value");
    }

    #[test]
    fn dangling_api_key_reference_is_skipped() {
        let mut c = cfg();
        c.provider = Some("anthropic".into());
        c.api_key_from_env = Some("UNSET_VAR".into());
        let out = resolve_env(&BTreeMap::new(), &c, &no_env);
        assert!(!out.contains_key("ANTHROPIC_API_KEY"));
    }

    #[test]
    fn precedence_is_global_then_curated_then_raw() {
        let mut default_env = BTreeMap::new();
        default_env.insert("CALIBAN_PROVIDER".into(), "openai".into());
        default_env.insert("KEEP".into(), "from-global".into());
        let mut c = cfg();
        c.provider = Some("ollama".into());
        c.env.insert("CALIBAN_PROVIDER".into(), "raw-wins".into());
        let out = resolve_env(&default_env, &c, &no_env);
        assert_eq!(out.get("CALIBAN_PROVIDER").unwrap(), "raw-wins");
        assert_eq!(out.get("KEEP").unwrap(), "from-global");
    }

    #[test]
    fn provider_only_backend_ignores_base_url() {
        let mut c = cfg();
        c.provider = Some("bedrock".into());
        c.base_url = Some("http://ignored".into());
        let out = resolve_env(&BTreeMap::new(), &c, &no_env);
        assert_eq!(out.get("CALIBAN_PROVIDER").unwrap(), "bedrock");
        assert!(out.keys().all(|k| k == "CALIBAN_PROVIDER"));
    }

    #[test]
    fn empty_config_passes_default_env_through() {
        let mut default_env = BTreeMap::new();
        default_env.insert("FOO".into(), "bar".into());
        let out = resolve_env(&default_env, &cfg(), &no_env);
        assert_eq!(out.get("FOO").unwrap(), "bar");
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn validate_rejects_unset_api_key() {
        let mut c = cfg();
        c.provider = Some("anthropic".into());
        let resolved = resolve_env(&BTreeMap::new(), &c, &no_env);
        let err = validate_provider_env(&c, &resolved).unwrap_err();
        assert!(err.contains("anthropic"), "message names provider: {err}");
        assert!(
            err.contains("ANTHROPIC_API_KEY"),
            "message names key var: {err}"
        );
    }

    #[test]
    fn validate_rejects_dangling_api_key_reference() {
        let mut c = cfg();
        c.provider = Some("openai".into());
        c.api_key_from_env = Some("UNSET_VAR".into());
        let resolved = resolve_env(&BTreeMap::new(), &c, &no_env);
        let err = validate_provider_env(&c, &resolved).unwrap_err();
        assert!(
            err.contains("UNSET_VAR"),
            "message names the dangling reference: {err}"
        );
        assert!(
            err.contains("OPENAI_API_KEY"),
            "message names key var: {err}"
        );
    }

    #[test]
    fn validate_accepts_key_resolved_from_env() {
        let mut c = cfg();
        c.provider = Some("anthropic".into());
        c.api_key_from_env = Some("MY_KEY".into());
        let proc = |k: &str| (k == "MY_KEY").then(|| "secret".to_string());
        let resolved = resolve_env(&BTreeMap::new(), &c, &proc);
        assert!(validate_provider_env(&c, &resolved).is_ok());
    }

    #[test]
    fn validate_accepts_key_supplied_via_default_env() {
        let mut default_env = BTreeMap::new();
        default_env.insert("ANTHROPIC_API_KEY".into(), "from-global".into());
        let mut c = cfg();
        c.provider = Some("anthropic".into());
        let resolved = resolve_env(&default_env, &c, &no_env);
        assert!(validate_provider_env(&c, &resolved).is_ok());
    }

    #[test]
    fn validate_treats_empty_key_value_as_unset() {
        let mut default_env = BTreeMap::new();
        default_env.insert("ANTHROPIC_API_KEY".into(), String::new());
        let mut c = cfg();
        c.provider = Some("anthropic".into());
        let resolved = resolve_env(&default_env, &c, &no_env);
        assert!(validate_provider_env(&c, &resolved).is_err());
    }

    #[test]
    fn validate_accepts_keyless_provider() {
        let mut c = cfg();
        c.provider = Some("ollama".into());
        let resolved = resolve_env(&BTreeMap::new(), &c, &no_env);
        assert!(validate_provider_env(&c, &resolved).is_ok());
    }

    #[test]
    fn validate_accepts_provider_only_backend() {
        let mut c = cfg();
        c.provider = Some("bedrock".into());
        let resolved = resolve_env(&BTreeMap::new(), &c, &no_env);
        assert!(validate_provider_env(&c, &resolved).is_ok());
    }

    #[test]
    fn validate_accepts_no_provider() {
        let resolved = resolve_env(&BTreeMap::new(), &cfg(), &no_env);
        assert!(validate_provider_env(&cfg(), &resolved).is_ok());
    }

    #[test]
    fn validate_config_rejects_api_key_on_keyless_provider() {
        let mut c = cfg();
        c.provider = Some("ollama".into());
        c.api_key_from_env = Some("SOME_VAR".into());
        let err = validate_provider_config(&c).unwrap_err();
        assert!(err.contains("ollama"), "message names provider: {err}");
        assert!(
            err.contains("api_key_from_env"),
            "message names the offending field: {err}"
        );
    }

    #[test]
    fn validate_config_rejects_api_key_on_provider_only_backend() {
        let mut c = cfg();
        c.provider = Some("bedrock".into());
        c.api_key_from_env = Some("SOME_VAR".into());
        assert!(validate_provider_config(&c).is_err());
    }

    #[test]
    fn validate_config_accepts_api_key_on_keyed_provider() {
        let mut c = cfg();
        c.provider = Some("anthropic".into());
        c.api_key_from_env = Some("ANTHROPIC_KEY_VAR".into());
        assert!(validate_provider_config(&c).is_ok());
    }

    #[test]
    fn validate_config_accepts_keyless_provider_without_api_key() {
        let mut c = cfg();
        c.provider = Some("ollama".into());
        assert!(validate_provider_config(&c).is_ok());
    }

    #[test]
    fn validate_config_accepts_no_provider() {
        assert!(validate_provider_config(&cfg()).is_ok());
    }
}
