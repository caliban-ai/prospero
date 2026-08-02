//! Request/response payloads for the HTTP API.
//!
//! The shared contract types now live in `prospero-types` (#172) so the WASM
//! dashboard can use the exact same definitions — see that crate's `api` module
//! for why. They are re-exported here from their original paths, so every import
//! site and all serde output are unchanged.
//!
//! What stays in this crate is what does *not* belong in a neutral DTO crate:
//! the axum query extractor, and the mapping from a wire body onto
//! `prospero-core`'s domain types. ADR 0006 makes `api → core` one-directional,
//! so that mapping is the adapter's job — pushing it into `core` (or into
//! `prospero-types`) would drag a transport concept downward.

use prospero_core::fleet::SpawnRequest;
use serde::Deserialize;

pub use prospero_types::{
    AddWorkspaceBody, AgentInputBody, Capabilities, RespawnedResponse, SetConfigBody, SpawnBody,
    SpawnedResponse, WorkspaceSummary,
};

/// Query params for `GET /api/agents/{id}/events` and `/stream`.
///
/// Stays here: this is an axum extractor for a URL query string, not part of the
/// client/server body contract.
#[derive(Debug, Deserialize)]
pub struct FromSeq {
    /// Return events with `seq >= from` (default 0).
    #[serde(default)]
    pub from: u64,
}

/// Map a spawn request body onto the core [`SpawnRequest`].
///
/// A free function rather than a method because [`SpawnBody`] is defined in
/// `prospero-types` and an inherent impl can only be written by the defining
/// crate. Keeping the conversion here — rather than adding a `From` impl in
/// `prospero-core` — preserves ADR 0006's one-way `api → core` dependency.
pub fn spawn_request(body: SpawnBody) -> SpawnRequest {
    let isolation_worktree = body.isolation_worktree();
    SpawnRequest {
        prompt: body.prompt,
        label: body.label,
        model: body.model,
        isolation_worktree,
        tool_allowlist: body.tool_allowlist,
        interactive: body.interactive,
        frontmatter_path: body.frontmatter_path.map(std::path::PathBuf::from),
        provider_ref: body.provider_ref,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_body_interactive_round_trips_and_defaults_false() {
        let with: SpawnBody = serde_json::from_str(r#"{"prompt":"p","interactive":true}"#).unwrap();
        assert!(spawn_request(with).interactive);
        let without: SpawnBody = serde_json::from_str(r#"{"prompt":"p"}"#).unwrap();
        assert!(!spawn_request(without).interactive);
    }

    #[test]
    fn spawn_body_carries_frontmatter_path() {
        let with: SpawnBody =
            serde_json::from_str(r#"{"prompt":"p","frontmatter_path":"/tpl.md"}"#).unwrap();
        assert_eq!(
            spawn_request(with).frontmatter_path,
            Some(std::path::PathBuf::from("/tpl.md"))
        );
        let without: SpawnBody = serde_json::from_str(r#"{"prompt":"p"}"#).unwrap();
        assert_eq!(spawn_request(without).frontmatter_path, None);
    }

    #[test]
    fn spawn_defaults_to_worktree_and_only_shared_opts_out() {
        let default: SpawnBody = serde_json::from_str(r#"{"prompt":"p"}"#).unwrap();
        assert!(spawn_request(default).isolation_worktree);
        let shared: SpawnBody =
            serde_json::from_str(r#"{"prompt":"p","isolation":"shared"}"#).unwrap();
        assert!(!spawn_request(shared).isolation_worktree);
    }

    #[test]
    fn workspace_summary_exposes_sources() {
        let s = WorkspaceSummary {
            name: "ws".into(),
            root: "/ws".into(),
            sources: vec![prospero_core::Source {
                name: "a".into(),
                path: "/ws/a".into(),
            }],
            health: prospero_core::WorkspaceHealth::Healthy,
            agent_count: 0,
            config: prospero_core::registry::RepoProviderConfig::default(),
            display_name: None,
            providers: Vec::new(),
            default_provider: None,
            status: None,
        };
        let j = serde_json::to_value(&s).unwrap();
        assert_eq!(j["sources"][0]["name"], "a");
    }
}
