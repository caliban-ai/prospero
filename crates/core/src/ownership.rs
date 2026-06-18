//! Which process is the single writer for a given stream.
//!
//! Standalone uses [`SelfOwnsAll`]: one process owns every stream, so the lease
//! is a no-op. The clustered `LeasedOwnership` (a Postgres lease row + reaper)
//! drops in behind the same trait in a later phase — see the topology design
//! spec §3.3. The `epoch` on [`Lease`] exists now so control-fencing can be
//! added later without a wire change.

use crate::error::Result;

/// A claim on a stream's single-writer role. `epoch` is a monotonic fencing
/// token (always 0 under [`SelfOwnsAll`]).
#[derive(Debug, Clone)]
pub struct Lease {
    /// The owned stream key.
    pub stream_key: String,
    /// Monotonic fencing epoch for the claim.
    pub epoch: u64,
}

/// Single-writer ownership of streams.
pub trait Ownership: Send + Sync {
    /// Claim `stream_key` if it is free (or already held by this process).
    /// Returns the lease, or `None` if another writer owns it.
    fn try_acquire(&self, stream_key: &str) -> Option<Lease>;

    /// Extend a held lease. Errors if the lease was lost (stolen/expired).
    fn renew(&self, lease: &Lease) -> Result<()>;

    /// Release a held stream so a peer may claim it.
    fn release(&self, stream_key: &str);

    /// Whether this process currently owns `stream_key`.
    fn owns(&self, stream_key: &str) -> bool;
}

/// Standalone ownership: this process owns every stream unconditionally.
pub struct SelfOwnsAll;

impl Ownership for SelfOwnsAll {
    fn try_acquire(&self, stream_key: &str) -> Option<Lease> {
        Some(Lease {
            stream_key: stream_key.to_string(),
            epoch: 0,
        })
    }
    fn renew(&self, _lease: &Lease) -> Result<()> {
        Ok(())
    }
    fn release(&self, _stream_key: &str) {}
    fn owns(&self, _stream_key: &str) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_owns_all_always_acquires_and_owns() {
        let o = SelfOwnsAll;
        let lease = o.try_acquire("a1").expect("standalone always acquires");
        assert_eq!(lease.stream_key, "a1");
        assert_eq!(lease.epoch, 0);
        assert!(o.owns("a1"));
        assert!(o.owns("anything-else"));
        o.renew(&lease).unwrap();
        o.release("a1"); // no-op, must not panic
    }
}
