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
}
