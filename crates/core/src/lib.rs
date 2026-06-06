//! Core orchestration logic and shared types for Prospero.
//!
//! This crate is the shared library behind the `prospero` CLI, the
//! `prosperod` daemon, and the web/API surface.

/// Crate version, sourced from `Cargo.toml`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
