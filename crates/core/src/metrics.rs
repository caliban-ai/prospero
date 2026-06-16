//! Process-lifetime operational counters for prosperod.
//!
//! ADR-0004 specifies a failed `Store.append` is "logged **and metered**", and
//! `caliband::stream` documents an unknown frame as something the caller should
//! "log **+ count**". These counters meter both, plus a little surrounding
//! signal (events appended, repos polled, active attaches), so operators aren't
//! blind to persistence loss, protocol drift, or attach load. They are exposed
//! over the API via [`MetricsSnapshot`].

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

/// Monotonic operational counters, incremented from the hot paths. Cheap,
/// lock-free, and shared (behind an `Arc`) across the manager and its tasks.
#[derive(Debug, Default)]
pub struct Metrics {
    events_appended: AtomicU64,
    append_failures: AtomicU64,
    unknown_frames: AtomicU64,
    repos_polled: AtomicU64,
}

impl Metrics {
    /// A `Store.append` succeeded.
    pub(crate) fn record_append_ok(&self) {
        self.events_appended.fetch_add(1, Ordering::Relaxed);
    }

    /// A `Store.append` failed (durability loss; see ADR-0004).
    pub(crate) fn record_append_failure(&self) {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// An unrecognized caliban stream frame was seen (protocol drift).
    pub(crate) fn record_unknown_frame(&self) {
        self.unknown_frames.fetch_add(1, Ordering::Relaxed);
    }

    /// One repo poll cycle ran.
    pub(crate) fn record_repo_poll(&self) {
        self.repos_polled.fetch_add(1, Ordering::Relaxed);
    }

    /// Read the counters into a serializable snapshot. `active_attaches` is a
    /// gauge supplied by the caller (the current attach-task count).
    pub fn snapshot(&self, active_attaches: u64) -> MetricsSnapshot {
        MetricsSnapshot {
            events_appended: self.events_appended.load(Ordering::Relaxed),
            append_failures: self.append_failures.load(Ordering::Relaxed),
            unknown_frames: self.unknown_frames.load(Ordering::Relaxed),
            repos_polled: self.repos_polled.load(Ordering::Relaxed),
            active_attaches,
        }
    }
}

/// A point-in-time snapshot of prosperod's operational counters, returned by
/// `GET /api/metrics`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetricsSnapshot {
    /// Events successfully appended to the durable store.
    pub events_appended: u64,
    /// `Store.append` failures (durability loss).
    pub append_failures: u64,
    /// Unrecognized caliban stream frames (protocol drift).
    pub unknown_frames: u64,
    /// Repo poll cycles run.
    pub repos_polled: u64,
    /// Attach tasks currently running (a gauge, not a counter).
    pub active_attaches: u64,
}
