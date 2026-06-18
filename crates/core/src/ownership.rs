//! Which process is the single writer for a given stream.
//!
//! Standalone uses [`SelfOwnsAll`]: one process owns every stream, so the lease
//! is a no-op. The clustered `LeasedOwnership` (a Postgres lease row + reaper)
//! drops in behind the same trait in a later phase — see the topology design
//! spec §3.3. The `epoch` on [`Lease`] exists now so control-fencing can be
//! added later without a wire change.

use async_trait::async_trait;

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
#[async_trait]
pub trait Ownership: Send + Sync {
    /// Claim `stream_key` if it is free, expired, or already held by THIS
    /// process (idempotent — re-acquiring your own live lease returns it and
    /// does not change its epoch). Returns `None` if another live replica owns
    /// it.
    async fn try_acquire(&self, stream_key: &str) -> Option<Lease>;

    /// Extend a held lease. `Err` if the lease was lost (stolen/expired) — which
    /// is how a replica learns it is no longer the owner.
    async fn renew(&self, lease: &Lease) -> Result<()>;

    /// Release a held stream so a peer may claim it immediately (graceful
    /// hand-off), rather than waiting for TTL expiry.
    async fn release(&self, stream_key: &str);

    /// Whether this process currently owns `stream_key`. Cheap/in-memory: it is
    /// consulted on the poll loop's hot path.
    fn owns(&self, stream_key: &str) -> bool;
}

/// Standalone ownership: this process owns every stream unconditionally.
pub struct SelfOwnsAll;

#[async_trait]
impl Ownership for SelfOwnsAll {
    async fn try_acquire(&self, stream_key: &str) -> Option<Lease> {
        Some(Lease {
            stream_key: stream_key.to_string(),
            epoch: 0,
        })
    }
    async fn renew(&self, _lease: &Lease) -> Result<()> {
        Ok(())
    }
    async fn release(&self, _stream_key: &str) {}
    fn owns(&self, _stream_key: &str) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn self_owns_all_always_acquires_and_owns() {
        let o = SelfOwnsAll;
        let lease = o
            .try_acquire("a1")
            .await
            .expect("standalone always acquires");
        assert_eq!(lease.stream_key, "a1");
        assert_eq!(lease.epoch, 0);
        assert!(o.owns("a1"));
        assert!(o.owns("anything-else"));
        o.renew(&lease).await.unwrap();
        o.release("a1").await; // no-op, must not panic
    }
}
