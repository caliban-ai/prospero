//! Shared, wasm-compatible serde DTOs for the prospero API surface.
//!
//! Depends only on `serde`/`serde_json`, so both the native server
//! (`prospero-core`/`prospero-api`) and the WASM dashboard (prospero #97) can
//! share these exact types — no client/server drift. Behavior-bearing types
//! (the fleet manager, stores, k8s backend, …) stay in `prospero-core`, which
//! re-exports each type here from its original path for source compatibility.

mod event;
mod model;

pub use event::{EventKind, FleetEvent, OutputStream, stream_key_for};
pub use model::{
    Agent, AgentId, AgentStatus, CredentialsRef, FleetSnapshot, IsolationConfig, ProviderInfo,
    ProviderSpec, Readiness, RepoProviderConfig, Source, Workspace, WorkspaceConfig,
    WorkspaceHealth, WorkspaceInfo, WorkspaceSourceSpec, WorkspaceStatusInfo,
};
