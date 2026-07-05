//! Core orchestration logic and shared types for Prospero.
//!
//! This crate is the shared library behind the `prospero` CLI, the
//! `prosperod` daemon, and the web/API surface. It models a fleet of Caliban
//! agents across repos and speaks caliban's NDJSON IPC protocol through a thin,
//! self-contained client — the wire format is the only coupling to caliban.

pub mod bus;
pub mod caliband;
pub mod config_store;
pub mod discovery;
pub mod distributed_bus;
pub mod error;
pub mod event;
pub mod fleet;
pub mod fleet_provider;
#[cfg(feature = "k8s")]
pub mod k8s;
pub mod leased_ownership;
pub mod metrics;
pub mod model;
pub mod ownership;
mod pg;
pub mod postgres_config_store;
pub mod postgres_store;
pub mod provider_env;
pub mod registry;
pub mod sqlite_store;
pub mod store;

#[cfg(any(test, feature = "testkit"))]
pub mod testkit;

pub use bus::{BusEvent, BusSubscription, EventBus, InProcessBus};
pub use caliband::wire::AttachInbound;
pub use config_store::{ConfigStore, SqliteConfigStore};
pub use distributed_bus::DistributedBus;
pub use error::{CoreError, Result};
pub use event::{EventKind, FleetEvent, OutputStream};
pub use fleet::{FleetConfig, FleetManager, SpawnRequest};
pub use fleet_provider::{FleetProvider, LocalFleet};
#[cfg(feature = "k8s")]
pub use k8s::fleet::{CalibanTaskApi, K8sFleet, KubeTaskApi};
pub use leased_ownership::LeasedOwnership;
pub use metrics::{Metrics, MetricsSnapshot};
pub use model::{Agent, AgentStatus, FleetSnapshot, Readiness, Repo, RepoHealth};
pub use ownership::{Lease, Ownership, SelfOwnsAll};
pub use postgres_config_store::PostgresConfigStore;
pub use postgres_store::PostgresStore;
pub use registry::{RegisteredRepo, Registry, RepoProviderConfig};
pub use sqlite_store::SqliteStore;
pub use store::{JsonlStore, Store};

/// Crate version, sourced from `Cargo.toml`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
