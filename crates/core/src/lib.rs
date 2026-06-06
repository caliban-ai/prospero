//! Core orchestration logic and shared types for Prospero.
//!
//! This crate is the shared library behind the `prospero` CLI, the
//! `prosperod` daemon, and the web/API surface. It models a fleet of Caliban
//! agents across repos and speaks caliban's NDJSON IPC protocol through a thin,
//! self-contained client — the wire format is the only coupling to caliban.

pub mod caliband;
pub mod error;
pub mod event;
pub mod model;

pub use error::{CoreError, Result};
pub use event::{EventKind, FleetEvent, OutputStream};
pub use model::{Agent, AgentStatus, FleetSnapshot, Repo, RepoHealth};

/// Crate version, sourced from `Cargo.toml`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
