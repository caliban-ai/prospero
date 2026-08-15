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

    /// The sqlite query's Postgres twin: same grouping, same filter, same shape.
    /// `kind` is a TEXT column (not `jsonb`), so each read casts before
    /// extracting. The explicit `::bigint` casts on the counts are load-bearing —
    /// Postgres widens `SUM` over an integer to `numeric`, which sqlx will not
    /// decode as `i64`.
    async fn usage(&self, since: &str, until: &str) -> Result<Vec<crate::store::UsageRow>> {
        let rows = sqlx::query(
            "SELECT repo AS workspace, substr(ts, 1, 10) AS day, \
                COALESCE(SUM(CASE WHEN kind::jsonb->>'kind' = 'agent_finished' \
                    THEN (kind::jsonb->>'cost_usd')::double precision END), 0.0)::double precision \
                    AS cost_usd, \
                COALESCE(SUM(CASE WHEN kind::jsonb->>'kind' = 'agent_finished' \
                    THEN (kind::jsonb->>'turns')::bigint END), 0)::bigint AS turns, \
                COALESCE(SUM(CASE WHEN kind::jsonb->>'to' = 'done' THEN 1 END), 0)::bigint AS done, \
                COALESCE(SUM(CASE WHEN kind::jsonb->>'to' = 'failed' THEN 1 END), 0)::bigint \
                    AS failed, \
                COALESCE(SUM(CASE WHEN kind::jsonb->>'to' = 'killed' THEN 1 END), 0)::bigint \
                    AS killed, \
                COALESCE(SUM(CASE WHEN kind::jsonb->>'to' = 'crashed' THEN 1 END), 0)::bigint \
                    AS crashed \
             FROM events \
             WHERE ts >= $1 AND ts < $2 AND ( \
                kind::jsonb->>'kind' = 'agent_finished' OR ( \
                    kind::jsonb->>'kind' = 'status_changed' \
                    AND kind::jsonb->>'to' IN ('done', 'failed', 'killed', 'crashed'))) \
             GROUP BY repo, substr(ts, 1, 10) \
             ORDER BY repo, day",
        )
        .bind(since)
        .bind(until)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("usage: {e}")))?;

        let decode = |e: sqlx::Error| CoreError::Store(format!("usage decode: {e}"));
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(crate::store::UsageRow {
                workspace: row.try_get("workspace").map_err(decode)?,
                day: row.try_get("day").map_err(decode)?,
                cost_usd: row.try_get::<f64, _>("cost_usd").map_err(decode)?,
                turns: row.try_get::<i64, _>("turns").map_err(decode)? as u64,
                done: row.try_get::<i64, _>("done").map_err(decode)? as u64,
                failed: row.try_get::<i64, _>("failed").map_err(decode)? as u64,
                killed: row.try_get::<i64, _>("killed").map_err(decode)? as u64,
                crashed: row.try_get::<i64, _>("crashed").map_err(decode)? as u64,
            });
        }
        Ok(out)
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
        store.reset_for_tests().await.unwrap();
        crate::testkit::store_usage_conformance(&store).await;
    }
}
