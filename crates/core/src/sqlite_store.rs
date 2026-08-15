//! sqlx-backed sqlite [`Store`] — the default `prosperod` backend.
//!
//! One `events` table; `global_ordinal` (the rowid) records durable insertion
//! order for future fleet-wide queries, while consumers see the per-stream
//! `seq`. The SQL uses sqlx's runtime query API (no compile-time `DATABASE_URL`)
//! so the same statements port to Postgres in Phase 2.

use std::path::Path;

use async_trait::async_trait;
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

use crate::error::{CoreError, Result};
use crate::event::FleetEvent;
use crate::store::{Store, map_append_error};

/// One-table schema. `global_ordinal` (rowid) is durable insertion order;
/// `UNIQUE(stream_key, seq)` is the per-stream monotonicity backstop and also
/// the index `replay` scans.
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS events (\
    global_ordinal INTEGER PRIMARY KEY AUTOINCREMENT,\
    stream_key TEXT NOT NULL,\
    seq        INTEGER NOT NULL,\
    ts         TEXT NOT NULL,\
    repo       TEXT NOT NULL,\
    agent_id   TEXT NOT NULL,\
    kind       TEXT NOT NULL,\
    UNIQUE(stream_key, seq)\
)";

/// sqlx/sqlite-backed durable event store.
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    /// Open (creating it + parent dirs if missing) the store at `dir/events.db`.
    pub async fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("events.db");
        let opts = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            // WAL lets readers run during a write; concurrent *writers* still
            // serialize, so wait for the write lock instead of erroring with
            // SQLITE_BUSY immediately.
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .connect_with(opts)
            .await
            .map_err(|e| CoreError::Store(format!("opening sqlite store: {e}")))?;
        sqlx::query(SCHEMA)
            .execute(&pool)
            .await
            .map_err(|e| CoreError::Store(format!("initializing sqlite schema: {e}")))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl Store for SqliteStore {
    async fn append(&self, event: &FleetEvent) -> Result<()> {
        let kind = serde_json::to_string(&event.kind)?;
        sqlx::query(
            "INSERT INTO events (stream_key, seq, ts, repo, agent_id, kind) \
             VALUES (?, ?, ?, ?, ?, ?)",
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
             WHERE stream_key = ? AND seq >= ? ORDER BY seq",
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
            sqlx::query("SELECT COALESCE(MAX(seq), 0) AS hw FROM events WHERE stream_key = ?")
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
        let res = sqlx::query("DELETE FROM events WHERE ts < ?")
            .bind(before_ts)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("prune: {e}")))?;
        Ok(res.rows_affected())
    }

    /// Aggregated in SQL (JSON1 `json_extract` over the `kind` column) so a long
    /// log is grouped by the database rather than replayed into memory. The
    /// `WHERE` clause discards every other event kind before grouping, so
    /// workspaces that only produced output never open a row.
    async fn usage(&self, since: &str, until: &str) -> Result<Vec<crate::store::UsageRow>> {
        let rows = sqlx::query(
            "SELECT repo AS workspace, substr(ts, 1, 10) AS day, \
                COALESCE(SUM(CASE WHEN json_extract(kind, '$.kind') = 'agent_finished' \
                    THEN json_extract(kind, '$.cost_usd') END), 0.0) AS cost_usd, \
                COALESCE(SUM(CASE WHEN json_extract(kind, '$.kind') = 'agent_finished' \
                    THEN json_extract(kind, '$.turns') END), 0) AS turns, \
                COALESCE(SUM(CASE WHEN json_extract(kind, '$.to') = 'done' THEN 1 END), 0) AS done, \
                COALESCE(SUM(CASE WHEN json_extract(kind, '$.to') = 'failed' THEN 1 END), 0) AS failed, \
                COALESCE(SUM(CASE WHEN json_extract(kind, '$.to') = 'killed' THEN 1 END), 0) AS killed, \
                COALESCE(SUM(CASE WHEN json_extract(kind, '$.to') = 'crashed' THEN 1 END), 0) AS crashed \
             FROM events \
             WHERE ts >= ? AND ts < ? AND ( \
                json_extract(kind, '$.kind') = 'agent_finished' OR ( \
                    json_extract(kind, '$.kind') = 'status_changed' \
                    AND json_extract(kind, '$.to') IN ('done', 'failed', 'killed', 'crashed'))) \
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

    fn ev(seq: u64, agent: &str) -> crate::event::FleetEvent {
        crate::event::FleetEvent {
            seq,
            ts: "t".into(),
            repo: "r".into(),
            agent_id: agent.into(),
            kind: crate::event::EventKind::AgentSpawned,
        }
    }

    #[tokio::test]
    async fn sqlite_store_satisfies_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path()).await.unwrap();
        crate::testkit::store_conformance(&store).await;
    }

    #[tokio::test]
    async fn reopen_resumes_per_stream_high_water() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = SqliteStore::open(dir.path()).await.unwrap();
            s.append(&ev(5, "a")).await.unwrap();
        }
        let s = SqliteStore::open(dir.path()).await.unwrap();
        assert_eq!(s.high_water("a").await.unwrap(), 5);
    }

    #[tokio::test]
    async fn duplicate_stream_seq_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let s = SqliteStore::open(dir.path()).await.unwrap();
        s.append(&ev(1, "a")).await.unwrap();
        assert!(s.append(&ev(1, "a")).await.is_err());
    }

    #[tokio::test]
    async fn sqlite_store_prunes_by_age() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path()).await.unwrap();
        crate::testkit::store_prune_conformance(&store).await;
    }

    #[tokio::test]
    async fn sqlite_store_aggregates_usage() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path()).await.unwrap();
        crate::testkit::store_usage_conformance(&store).await;
    }
}
