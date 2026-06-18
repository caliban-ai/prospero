//! Clustered single-writer ownership via a Postgres lease row per stream.
//!
//! One row per active stream — `(stream_key, owner_replica_id, epoch,
//! expires_at)`. `try_acquire` is `INSERT … ON CONFLICT … WHERE expired-or-ours`
//! (so it both claims a free stream and reaps an expired one — the shared poll
//! loop is the reaper/takeover, spec §3.3); the owner extends `expires_at` via
//! `heartbeat`/`renew`; `renew` failing is how a replica learns its lease was
//! stolen. Expiry uses the Postgres clock (`now()` + `make_interval`) so it is
//! immune to per-replica clock skew. `owns` answers from an in-memory mirror of
//! held leases (cheap, for the poll hot path); the `UNIQUE(stream_key, seq)`
//! store constraint plus the `epoch` fencing token are the two-writer backstops.

use std::collections::HashMap;
use std::sync::Mutex;

use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::Result;
use crate::error::CoreError;
use crate::ownership::{Lease, Ownership};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS leases (\
    stream_key TEXT PRIMARY KEY,\
    owner_replica_id TEXT NOT NULL,\
    epoch BIGINT NOT NULL,\
    expires_at TIMESTAMPTZ NOT NULL)";

/// Clustered `Ownership`: a Postgres lease per stream (spec §3.3).
pub struct LeasedOwnership {
    pool: PgPool,
    replica_id: String,
    /// Lease TTL in seconds (DB-clock relative). The daemon must call
    /// [`LeasedOwnership::heartbeat`] well within this window.
    ttl_secs: f64,
    /// In-memory mirror of leases this replica believes it holds: key → epoch.
    held: Mutex<HashMap<String, u64>>,
}

impl LeasedOwnership {
    /// Connect, ensure the lease table exists, and identify this replica.
    /// `ttl_secs` is the lease lifetime; call [`Self::heartbeat`] well within it.
    pub async fn connect(url: &str, replica_id: String, ttl_secs: f64) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .connect(url)
            .await
            .map_err(|e| CoreError::Store(format!("connecting to postgres: {e}")))?;
        // `CREATE TABLE IF NOT EXISTS` is not atomic at the system-catalog level:
        // when several replicas boot concurrently against a fresh DB they can race
        // creating the table. The loser surfaces the race as one of a few codes —
        // a duplicate-key error on a `pg_*` catalog index (e.g.
        // `pg_type_typname_nsp_index`, SQLSTATE 23505 unique_violation),
        // duplicate_table (42P07), or duplicate_object/type for the table's
        // implicit row-type (42710, "type \"leases\" already exists"). All three
        // mean the table now exists, which is all we wanted, so treat them as
        // success and fail only on a genuine error.
        if let Err(e) = sqlx::query(SCHEMA).execute(&pool).await {
            let benign = e
                .as_database_error()
                .and_then(|db| db.code())
                .map(|code| matches!(code.as_ref(), "23505" | "42P07" | "42710"))
                .unwrap_or(false);
            if !benign {
                return Err(CoreError::Store(format!("creating leases table: {e}")));
            }
        }
        Ok(Self {
            pool,
            replica_id,
            ttl_secs,
            held: Mutex::new(HashMap::new()),
        })
    }

    /// Build on an existing pool (shared-pool clustered wiring, Phase 2d).
    pub fn new(pool: PgPool, replica_id: String, ttl_secs: f64) -> Self {
        Self {
            pool,
            replica_id,
            ttl_secs,
            held: Mutex::new(HashMap::new()),
        }
    }

    /// Renew every lease this replica holds; drop any it has lost. The daemon's
    /// reconciliation tick calls this (Phase 2d).
    pub async fn heartbeat(&self) {
        let leases: Vec<Lease> = {
            let held = self.held.lock().unwrap();
            held.iter()
                .map(|(k, e)| Lease {
                    stream_key: k.clone(),
                    epoch: *e,
                })
                .collect()
        };
        for lease in leases {
            if self.renew(&lease).await.is_err() {
                self.held.lock().unwrap().remove(&lease.stream_key);
                tracing::warn!(
                    target: "prospero_ownership",
                    stream = %lease.stream_key, "lease lost; dropping ownership"
                );
            }
        }
    }

    #[cfg(any(test, feature = "testkit"))]
    pub async fn reset_for_tests(&self) -> Result<()> {
        sqlx::query("TRUNCATE leases")
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("truncating leases: {e}")))?;
        self.held.lock().unwrap().clear();
        Ok(())
    }

    /// Test helper: unconditionally take a stream for this replica (used to
    /// simulate a takeover by a peer without waiting out a TTL).
    #[cfg(any(test, feature = "testkit"))]
    pub async fn force_steal(&self, stream_key: &str) {
        let row = sqlx::query(
            "INSERT INTO leases (stream_key, owner_replica_id, epoch, expires_at) \
             VALUES ($1, $2, 1, now() + make_interval(secs => $3)) \
             ON CONFLICT (stream_key) DO UPDATE \
                SET owner_replica_id = excluded.owner_replica_id, \
                    epoch = leases.epoch + 1, \
                    expires_at = excluded.expires_at \
             RETURNING epoch",
        )
        .bind(stream_key)
        .bind(&self.replica_id)
        .bind(self.ttl_secs)
        .fetch_one(&self.pool)
        .await
        .expect("force_steal");
        let epoch: i64 = row.get("epoch");
        self.held
            .lock()
            .unwrap()
            .insert(stream_key.to_string(), epoch as u64);
    }
}

#[async_trait::async_trait]
impl Ownership for LeasedOwnership {
    async fn try_acquire(&self, stream_key: &str) -> Option<Lease> {
        // Claim a free key, reap an expired one, or idempotently re-confirm our
        // own. Epoch is kept when re-acquiring ours, bumped when stealing an
        // expired lease (fences the dead owner). The WHERE makes the UPDATE — and
        // thus the RETURNING row — vanish when another live replica owns it.
        let row = sqlx::query(
            "INSERT INTO leases (stream_key, owner_replica_id, epoch, expires_at) \
             VALUES ($1, $2, 1, now() + make_interval(secs => $3)) \
             ON CONFLICT (stream_key) DO UPDATE \
                SET owner_replica_id = excluded.owner_replica_id, \
                    epoch = CASE WHEN leases.owner_replica_id = excluded.owner_replica_id \
                                 THEN leases.epoch ELSE leases.epoch + 1 END, \
                    expires_at = excluded.expires_at \
             WHERE leases.expires_at < now() \
                OR leases.owner_replica_id = excluded.owner_replica_id \
             RETURNING epoch",
        )
        .bind(stream_key)
        .bind(&self.replica_id)
        .bind(self.ttl_secs)
        .fetch_optional(&self.pool)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(target: "prospero_ownership", stream = %stream_key, error = %e, "try_acquire failed");
            None
        })?;
        let epoch: i64 = row.get("epoch");
        let epoch = epoch as u64;
        self.held
            .lock()
            .unwrap()
            .insert(stream_key.to_string(), epoch);
        Some(Lease {
            stream_key: stream_key.to_string(),
            epoch,
        })
    }

    async fn renew(&self, lease: &Lease) -> Result<()> {
        // Extend only while we still hold it at the SAME epoch — a steal advances
        // owner/epoch, so 0 rows affected means we lost the lease.
        let res = sqlx::query(
            "UPDATE leases SET expires_at = now() + make_interval(secs => $1) \
             WHERE stream_key = $2 AND owner_replica_id = $3 AND epoch = $4",
        )
        .bind(self.ttl_secs)
        .bind(&lease.stream_key)
        .bind(&self.replica_id)
        .bind(lease.epoch as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("renewing lease: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(CoreError::Store(format!(
                "lease for {} lost (stolen or expired)",
                lease.stream_key
            )));
        }
        Ok(())
    }

    async fn release(&self, stream_key: &str) {
        self.held.lock().unwrap().remove(stream_key);
        if let Err(e) =
            sqlx::query("DELETE FROM leases WHERE stream_key = $1 AND owner_replica_id = $2")
                .bind(stream_key)
                .bind(&self.replica_id)
                .execute(&self.pool)
                .await
        {
            tracing::warn!(target: "prospero_ownership", stream = %stream_key, error = %e, "releasing lease failed");
        }
    }

    fn owns(&self, stream_key: &str) -> bool {
        self.held.lock().unwrap().contains_key(stream_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique(tag: &str) -> String {
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{tag}-{nanos}-{}", N.fetch_add(1, Ordering::Relaxed))
    }

    async fn owner(url: &str, ttl_secs: f64) -> LeasedOwnership {
        LeasedOwnership::connect(url, unique("replica"), ttl_secs)
            .await
            .unwrap()
    }

    macro_rules! db_url {
        ($name:literal) => {
            match std::env::var("DATABASE_URL") {
                Ok(u) => u,
                Err(_) => {
                    eprintln!(concat!("SKIP ", $name, ": DATABASE_URL unset"));
                    return;
                }
            }
        };
    }

    #[tokio::test]
    async fn acquires_a_free_stream_and_owns_it() {
        let url = db_url!("acquires_a_free_stream_and_owns_it");
        let o = owner(&url, 30.0).await;
        let key = unique("s");
        let lease = o.try_acquire(&key).await.expect("free stream acquires");
        assert_eq!(lease.stream_key, key);
        assert!(lease.epoch >= 1);
        assert!(o.owns(&key));
    }

    #[tokio::test]
    async fn reacquiring_own_live_lease_is_idempotent_and_keeps_epoch() {
        let url = db_url!("reacquiring_own_live_lease_is_idempotent_and_keeps_epoch");
        let o = owner(&url, 30.0).await;
        let key = unique("s");
        let first = o.try_acquire(&key).await.unwrap();
        let again = o.try_acquire(&key).await.expect("own lease re-acquires");
        assert_eq!(
            again.epoch, first.epoch,
            "re-acquiring your own lease must not bump epoch"
        );
    }

    #[tokio::test]
    async fn a_live_lease_blocks_another_replica() {
        let url = db_url!("a_live_lease_blocks_another_replica");
        let key = unique("s");
        let a = owner(&url, 30.0).await;
        let b = owner(&url, 30.0).await;
        assert!(a.try_acquire(&key).await.is_some());
        assert!(
            b.try_acquire(&key).await.is_none(),
            "peer must not steal a live lease"
        );
        assert!(!b.owns(&key));
    }

    #[tokio::test]
    async fn an_expired_lease_is_stolen_with_a_bumped_epoch_and_renew_then_fails() {
        let url = db_url!("an_expired_lease_is_stolen_with_a_bumped_epoch_and_renew_then_fails");
        let key = unique("s");
        let a = owner(&url, 1.0).await; // 1s TTL
        let b = owner(&url, 30.0).await;
        let a_lease = a.try_acquire(&key).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(1200)).await; // let it expire

        let b_lease = b.try_acquire(&key).await.expect("expired lease is stolen");
        assert!(
            b_lease.epoch > a_lease.epoch,
            "takeover must bump the fencing epoch"
        );
        // The dethroned owner learns it lost the lease on its next renew.
        assert!(
            a.renew(&a_lease).await.is_err(),
            "stale owner's renew must fail"
        );
        assert!(b.owns(&key));
    }

    #[tokio::test]
    async fn renew_extends_a_held_lease() {
        let url = db_url!("renew_extends_a_held_lease");
        let o = owner(&url, 2.0).await;
        let key = unique("s");
        let lease = o.try_acquire(&key).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        o.renew(&lease).await.expect("owner renews its live lease");
        // After renew the lease is good for another full TTL, so a peer can't steal.
        let peer = owner(&url, 30.0).await;
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        assert!(
            peer.try_acquire(&key).await.is_none(),
            "renewed lease stays held"
        );
    }

    #[tokio::test]
    async fn release_frees_the_stream_for_a_peer() {
        let url = db_url!("release_frees_the_stream_for_a_peer");
        let key = unique("s");
        let a = owner(&url, 30.0).await;
        let b = owner(&url, 30.0).await;
        a.try_acquire(&key).await.unwrap();
        a.release(&key).await;
        assert!(!a.owns(&key));
        assert!(
            b.try_acquire(&key).await.is_some(),
            "released stream is claimable"
        );
    }

    #[tokio::test]
    async fn heartbeat_renews_all_held_and_drops_lost_leases() {
        let url = db_url!("heartbeat_renews_all_held_and_drops_lost_leases");
        let kept = unique("s");
        let lost = unique("s");
        let a = owner(&url, 2.0).await;
        let thief = owner(&url, 30.0).await;
        a.try_acquire(&kept).await.unwrap();
        a.try_acquire(&lost).await.unwrap();
        // A peer steals `lost` out from under `a` (forced, simulating a takeover
        // after a missed heartbeat). Use the raw DELETE+claim via try_acquire on
        // an expired lease is slow, so steal directly:
        thief.force_steal(&lost).await;

        a.heartbeat().await;
        assert!(a.owns(&kept), "still-held lease survives heartbeat");
        assert!(!a.owns(&lost), "heartbeat drops a lease lost to a peer");
    }
}
