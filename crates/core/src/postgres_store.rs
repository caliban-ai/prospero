//! sqlx-backed Postgres [`Store`] — the clustered-tier event backend.
//!
//! Mirrors [`crate::sqlite_store::SqliteStore`] with Postgres dialect (`$N`
//! placeholders, `BIGSERIAL`/`BIGINT`). Runs the same `testkit` conformance
//! batteries, gated on `DATABASE_URL` (skipped when unset). See spec §3/§4.

use async_trait::async_trait;
use sqlx::Row;
use sqlx::postgres::PgPool;

use crate::error::{CoreError, Result};
use crate::event::FleetEvent;
use crate::store::{Store, map_append_error};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS events (\
    global_ordinal BIGSERIAL PRIMARY KEY,\
    stream_key TEXT NOT NULL,\
    seq        BIGINT NOT NULL,\
    ts         TEXT NOT NULL,\
    repo       TEXT NOT NULL,\
    agent_id   TEXT NOT NULL,\
    kind       TEXT NOT NULL,\
    UNIQUE(stream_key, seq)\
)";

/// sqlx/Postgres-backed durable event store (clustered tier).
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Connect to Postgres at `url` and ensure the schema exists.
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = crate::pg::connect(url).await?;
        crate::pg::ensure_schema(&pool, SCHEMA, "events table").await?;
        Ok(Self { pool })
    }

    /// Truncate all events. Test-only (resets between conformance batteries).
    #[cfg(any(test, feature = "testkit"))]
    pub async fn reset_for_tests(&self) -> Result<()> {
        sqlx::query("TRUNCATE events")
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("reset: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl Store for PostgresStore {
    async fn append(&self, event: &FleetEvent) -> Result<()> {
        let kind = serde_json::to_string(&event.kind)?;
        sqlx::query(
            "INSERT INTO events (stream_key, seq, ts, repo, agent_id, kind) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(event.stream_key())
        .bind(event.seq as i64)
        .bind(&event.ts)
        .bind(&event.repo)
        .bind(&event.agent_id)
        .bind(kind)
        .execute(&self.pool)
        .await
        .map_err(map_append_error)?;
        Ok(())
    }

    async fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
        let rows = sqlx::query(
            "SELECT seq, ts, repo, agent_id, kind FROM events \
             WHERE stream_key = $1 AND seq >= $2 ORDER BY seq",
        )
        .bind(stream_key)
        .bind(from_seq as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("replay: {e}")))?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let decode = |e: sqlx::Error| CoreError::Store(format!("replay decode: {e}"));
            let seq: i64 = row.try_get("seq").map_err(decode)?;
            let ts: String = row.try_get("ts").map_err(decode)?;
            let repo: String = row.try_get("repo").map_err(decode)?;
            let agent_id: String = row.try_get("agent_id").map_err(decode)?;
            let kind_json: String = row.try_get("kind").map_err(decode)?;
            events.push(FleetEvent {
                seq: seq as u64,
                ts,
                repo,
                agent_id,
                kind: serde_json::from_str(&kind_json)?,
            });
        }
        Ok(events)
    }

    async fn high_water(&self, stream_key: &str) -> Result<u64> {
        let row =
            sqlx::query("SELECT COALESCE(MAX(seq), 0) AS hw FROM events WHERE stream_key = $1")
                .bind(stream_key)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| CoreError::Store(format!("high_water: {e}")))?;
        let hw: i64 = row
            .try_get("hw")
            .map_err(|e| CoreError::Store(format!("high_water decode: {e}")))?;
        Ok(hw as u64)
    }

    async fn writable(&self) -> bool {
        // Non-destructive write probe: insert a sentinel row inside a
        // transaction we always roll back. Exercises the same write path as
        // `append` (detecting a read-only / full store) without persisting
        // anything and without DDL. `seq = -1` cannot collide with a real
        // event (seq is u64) and the rollback ensures it never lands.
        let Ok(mut tx) = self.pool.begin().await else {
            return false;
        };
        let ok = sqlx::query(
            "INSERT INTO events (stream_key, seq, ts, repo, agent_id, kind) \
             VALUES ('__writable_probe__', -1, '', '', '', 'null')",
        )
        .execute(&mut *tx)
        .await
        .is_ok();
        let _ = tx.rollback().await;
        ok
    }

    async fn prune(&self, before_ts: &str) -> Result<u64> {
        let res = sqlx::query("DELETE FROM events WHERE ts < $1")
            .bind(before_ts)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("prune: {e}")))?;
        Ok(res.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn connect_or_skip() -> Option<PostgresStore> {
        let url = std::env::var("DATABASE_URL").ok()?;
        let store = PostgresStore::connect(&url).await.unwrap();
        store.reset_for_tests().await.unwrap();
        Some(store)
    }

    #[tokio::test]
    async fn postgres_store_satisfies_conformance() {
        let Some(store) = connect_or_skip().await else {
            eprintln!("SKIP postgres_store_satisfies_conformance: DATABASE_URL unset");
            return;
        };
        crate::testkit::store_conformance(&store).await;
        store.reset_for_tests().await.unwrap();
        crate::testkit::store_prune_conformance(&store).await;
    }
}
