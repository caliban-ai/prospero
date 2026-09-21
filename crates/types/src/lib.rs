//! Shared, wasm-compatible serde DTOs for the prospero API surface.
//!
//! Depends only on `serde`/`serde_json`, so both the native server
//! (`prospero-core`/`prospero-api`) and the WASM dashboard (prospero #97) can
//! share these exact types — no client/server drift. Behavior-bearing types
//! (the fleet manager, stores, k8s backend, …) stay in `prospero-core`, which
//! re-exports each type here from its original path for source compatibility.

mod api;
mod auth;
mod automation;
mod event;
mod model;

pub use api::{
    AddWorkspaceBody, AgentInputBody, Capabilities, GapSignal, MAX_USAGE_WINDOW_DAYS,
    OutcomeCounts, RespawnedResponse, SetConfigBody, SpawnBody, SpawnedResponse, UsageBucket,
    UsageGroup, UsageReport, WorkspaceSummary,
};
pub use auth::{Scope, SessionInfo, SignInBody};
pub use automation::{
    Automation, AutomationRun, CreateAutomationBody, CreatedAutomationResponse, FiredResponse,
    RunSource, SetEnabledBody, SpawnTemplate, Trigger,
};
pub use event::{EventKind, FleetEvent, OutputStream, stream_key_for};
pub use model::{
    Agent, AgentId, AgentStatus, CredentialsRef, FleetSnapshot, IsolationConfig, PermissionPosture,
    ProviderInfo, ProviderSpec, Readiness, RepoProviderConfig, Source, Workspace, WorkspaceConfig,
    WorkspaceHealth, WorkspaceInfo, WorkspaceSourceSpec, WorkspaceStatusInfo,
};
