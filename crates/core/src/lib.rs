//! Core orchestration logic and shared types for Prospero.
//!
//! This crate is the shared library behind the `prospero` CLI, the
//! `prosperod` daemon, and the web/API surface. It models a fleet of Caliban
//! agents across repos and speaks caliban's NDJSON IPC protocol through a thin,
//! self-contained client — the wire format is the only coupling to caliban.

pub mod caliband;
pub mod discovery;
pub mod error;
pub mod event;
pub mod fleet;
pub mod model;
pub mod registry;
pub mod store;

#[cfg(any(test, feature = "testkit"))]
pub mod testkit;

pub use error::{CoreError, Result};
pub use event::{EventKind, FleetEvent, OutputStream};
pub use fleet::{FleetConfig, FleetManager, SpawnRequest};
pub use model::{Agent, AgentStatus, FleetSnapshot, Repo, RepoHealth};
pub use registry::{RegisteredRepo, Registry};
pub use store::{JsonlStore, Store};

/// Crate version, sourced from `Cargo.toml`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
