//! Mutable config records (the managed-repo registry) on the shared DB.
//!
//! Distinct from [`crate::store::Store`] because the access pattern is key-value
//! upsert/read, not append/replay. Standalone uses [`SqliteConfigStore`] in the
//! same `events.db`; a Postgres-backed impl drops in behind the trait in the
//! clustered tier (Phase 2). See the topology design spec §3.4.

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

use crate::automation::{AutomationRun, RUN_HISTORY_LIMIT, RunSource, StoredAutomation};
use crate::error::{CoreError, Result};
use crate::registry::RegisteredWorkspace;

/// Durable, mutable store for the managed-repo registry.
#[async_trait]
pub trait ConfigStore: Send + Sync {
    /// All registered repos, ordered by name.
    async fn list_repos(&self) -> Result<Vec<RegisteredWorkspace>>;
    /// Insert or update a repo (keyed by `name`).
    async fn upsert_repo(&self, repo: &RegisteredWorkspace) -> Result<()>;
    /// Remove a repo by name. Returns whether a row was deleted.
    async fn delete_repo(&self, name: &str) -> Result<bool>;

    /// All automations, ordered by id (#220).
    async fn list_automations(&self) -> Result<Vec<StoredAutomation>>;

    /// Insert or update an automation (keyed by `id`).
    ///
    /// Does **not** write `last_fired_at`: that column is owned by
    /// [`Self::claim_automation_fire`], and letting an ordinary edit move it
    /// would hand an operator a way to replay or skip a tick.
    async fn upsert_automation(&self, automation: &StoredAutomation) -> Result<()>;

    /// Remove an automation and its run history. Returns whether it existed.
    async fn delete_automation(&self, id: &str) -> Result<bool>;

    /// Claim `fire_at` for `id`, returning whether **this** caller won it.
    ///
    /// The exactly-once primitive behind scheduled spawns: a conditional
    /// `UPDATE … WHERE last_fired_at < fire_at` that the database serializes,
    /// so of N replicas noticing the same due tick, exactly one is told to act.
    ///
    /// This is the guarantee, not the lifecycle lease. A lease makes two
    /// replicas *unlikely* to act on one tick; it cannot rule it out, because
    /// ownership can change hands between the moment a replica checks and the
    /// moment it spawns. The compare-and-set closes that window, and keeps
    /// holding when a lease is handed over mid-tick.
    async fn claim_automation_fire(&self, id: &str, fire_at: &str) -> Result<bool>;

    /// Append a run to an automation's history, trimming to
    /// [`RUN_HISTORY_LIMIT`].
    async fn record_run(&self, run: &AutomationRun) -> Result<()>;

    /// An automation's runs, newest first, at most `limit`.
    async fn list_runs(&self, automation_id: &str, limit: usize) -> Result<Vec<AutomationRun>>;
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS repos (\
    name   TEXT PRIMARY KEY,\
    root   TEXT NOT NULL,\
    config TEXT NOT NULL\
)";

/// `last_fired_at` is a column rather than a field inside `spec` so the claim
/// statement can compare it in SQL — a JSON round trip could not be made
/// atomic across replicas. `spec` holds the rest of the [`Automation`].
const AUTOMATIONS_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS automations (\
    id             TEXT PRIMARY KEY,\
    spec           TEXT NOT NULL,\
    webhook_secret TEXT,\
    last_fired_at  TEXT\
)";

const RUNS_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS automation_runs (\
    automation_id TEXT NOT NULL,\
    fired_at      TEXT NOT NULL,\
    source        TEXT NOT NULL,\
    agent_id      TEXT,\
    error         TEXT\
)";

const RUNS_INDEX: &str = "CREATE INDEX IF NOT EXISTS automation_runs_by_time ON automation_runs (automation_id, fired_at DESC)";

/// Serialize an automation's `spec` column.
///
/// `last_fired_at` is cleared first: the column of the same name is the single
/// source of truth (only the claim statement may move it), and a second copy
/// inside the JSON would be a stale value waiting to be read by mistake.
pub(crate) fn encode_spec(automation: &StoredAutomation) -> Result<String> {
    let mut spec = automation.automation.clone();
    spec.last_fired_at = None;
    Ok(serde_json::to_string(&spec)?)
}

/// Render a [`RunSource`] for its column via serde, so the stored spelling and
/// the wire spelling cannot drift apart.
pub(crate) fn encode_source(source: RunSource) -> Result<String> {
    serde_json::to_value(source)?
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| CoreError::Store("run source did not serialize as a string".into()))
}

/// Rebuild a [`StoredAutomation`] from a row of `spec`, `webhook_secret` and
/// `last_fired_at`. Shared by both backends — one decoder, one behaviour.
pub(crate) fn decode_automation<R>(row: &R) -> Result<StoredAutomation>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> &'a str: sqlx::ColumnIndex<R>,
{
    let decode = |e: sqlx::Error| CoreError::Store(format!("automation decode: {e}"));
    let spec: String = row.try_get("spec").map_err(decode)?;
    let webhook_secret: Option<String> = row.try_get("webhook_secret").map_err(decode)?;
    let last_fired_at: Option<String> = row.try_get("last_fired_at").map_err(decode)?;
    let mut automation: crate::automation::Automation = serde_json::from_str(&spec)?;
    automation.last_fired_at = last_fired_at;
    Ok(StoredAutomation {
        automation,
        webhook_secret,
    })
}

/// Rebuild an [`AutomationRun`] from its row.
pub(crate) fn decode_run<R>(row: &R) -> Result<AutomationRun>
where
    R: Row,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> &'a str: sqlx::ColumnIndex<R>,
{
    let decode = |e: sqlx::Error| CoreError::Store(format!("run decode: {e}"));
    let source: String = row.try_get("source").map_err(decode)?;
    Ok(AutomationRun {
        automation_id: row.try_get("automation_id").map_err(decode)?,
        fired_at: row.try_get("fired_at").map_err(decode)?,
        source: serde_json::from_value(serde_json::Value::String(source))?,
        agent_id: row.try_get("agent_id").map_err(decode)?,
        error: row.try_get("error").map_err(decode)?,
    })
}

/// sqlx/sqlite-backed config store — shares `events.db` with the event store.
pub struct SqliteConfigStore {
    pool: SqlitePool,
}

impl SqliteConfigStore {
    /// Open (creating it + parent dirs if missing) the config store in
    /// `dir/events.db` (the same file the event store uses).
    pub async fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("events.db");
        let opts = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .connect_with(opts)
            .await
            .map_err(|e| CoreError::Store(format!("opening config store: {e}")))?;
        for ddl in [SCHEMA, AUTOMATIONS_SCHEMA, RUNS_SCHEMA, RUNS_INDEX] {
            sqlx::query(ddl)
                .execute(&pool)
                .await
                .map_err(|e| CoreError::Store(format!("initializing config schema: {e}")))?;
        }
        Ok(Self { pool })
    }
}

#[async_trait]
impl ConfigStore for SqliteConfigStore {
    async fn list_repos(&self) -> Result<Vec<RegisteredWorkspace>> {
        let rows = sqlx::query("SELECT name, root, config FROM repos ORDER BY name")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("list_repos: {e}")))?;
        let mut repos = Vec::with_capacity(rows.len());
        for row in rows {
            let decode = |e: sqlx::Error| CoreError::Store(format!("list_repos decode: {e}"));
            let name: String = row.try_get("name").map_err(decode)?;
            let root: String = row.try_get("root").map_err(decode)?;
            let config_json: String = row.try_get("config").map_err(decode)?;
            repos.push(RegisteredWorkspace {
                name,
                root: root.into(),
                config: serde_json::from_str(&config_json)?,
            });
        }
        Ok(repos)
    }

    async fn upsert_repo(&self, repo: &RegisteredWorkspace) -> Result<()> {
        let config = serde_json::to_string(&repo.config)?;
        // Surface a non-UTF8 root explicitly rather than silently lossy-mangling it.
        let root = repo
            .root
            .to_str()
            .ok_or_else(|| CoreError::Store(format!("non-UTF8 repo root path: {:?}", repo.root)))?;
        sqlx::query(
            "INSERT INTO repos (name, root, config) VALUES (?, ?, ?) \
             ON CONFLICT(name) DO UPDATE SET root = excluded.root, config = excluded.config",
        )
        .bind(&repo.name)
        .bind(root)
        .bind(config)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("upsert_repo: {e}")))?;
        Ok(())
    }

    async fn delete_repo(&self, name: &str) -> Result<bool> {
        let res = sqlx::query("DELETE FROM repos WHERE name = ?")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("delete_repo: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    async fn list_automations(&self) -> Result<Vec<StoredAutomation>> {
        let rows =
            sqlx::query("SELECT spec, webhook_secret, last_fired_at FROM automations ORDER BY id")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| CoreError::Store(format!("list_automations: {e}")))?;
        rows.iter().map(decode_automation).collect()
    }

    async fn upsert_automation(&self, automation: &StoredAutomation) -> Result<()> {
        let spec = encode_spec(automation)?;
        sqlx::query(
            "INSERT INTO automations (id, spec, webhook_secret, last_fired_at) \
             VALUES (?, ?, ?, NULL) \
             ON CONFLICT(id) DO UPDATE SET spec = excluded.spec, \
             webhook_secret = excluded.webhook_secret",
        )
        .bind(&automation.automation.id)
        .bind(spec)
        .bind(&automation.webhook_secret)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("upsert_automation: {e}")))?;
        Ok(())
    }

    async fn delete_automation(&self, id: &str) -> Result<bool> {
        sqlx::query("DELETE FROM automation_runs WHERE automation_id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("delete_automation runs: {e}")))?;
        let res = sqlx::query("DELETE FROM automations WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("delete_automation: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    async fn claim_automation_fire(&self, id: &str, fire_at: &str) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE automations SET last_fired_at = ? \
             WHERE id = ? AND (last_fired_at IS NULL OR last_fired_at < ?)",
        )
        .bind(fire_at)
        .bind(id)
        .bind(fire_at)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("claim_automation_fire: {e}")))?;
        Ok(res.rows_affected() == 1)
    }

    async fn record_run(&self, run: &AutomationRun) -> Result<()> {
        sqlx::query(
            "INSERT INTO automation_runs (automation_id, fired_at, source, agent_id, error) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&run.automation_id)
        .bind(&run.fired_at)
        .bind(encode_source(run.source)?)
        .bind(&run.agent_id)
        .bind(&run.error)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("record_run: {e}")))?;

        // Trim in the same call that appends, so history stays bounded even if
        // nothing ever reads it.
        sqlx::query(
            "DELETE FROM automation_runs WHERE automation_id = ? AND rowid NOT IN \
             (SELECT rowid FROM automation_runs WHERE automation_id = ? \
              ORDER BY fired_at DESC LIMIT ?)",
        )
        .bind(&run.automation_id)
        .bind(&run.automation_id)
        .bind(RUN_HISTORY_LIMIT as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("trim runs: {e}")))?;
        Ok(())
    }

    async fn list_runs(&self, automation_id: &str, limit: usize) -> Result<Vec<AutomationRun>> {
        let rows = sqlx::query(
            "SELECT automation_id, fired_at, source, agent_id, error FROM automation_runs \
             WHERE automation_id = ? ORDER BY fired_at DESC LIMIT ?",
        )
        .bind(automation_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("list_runs: {e}")))?;
        rows.iter().map(decode_run).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_config_store_satisfies_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteConfigStore::open(dir.path()).await.unwrap();
        crate::testkit::config_store_conformance(&store).await;
    }

    #[tokio::test]
    async fn sqlite_config_store_satisfies_automation_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteConfigStore::open(dir.path()).await.unwrap();
        crate::testkit::automation_store_conformance(&store).await;
    }

    #[tokio::test]
    async fn sqlite_config_store_claims_a_tick_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteConfigStore::open(dir.path()).await.unwrap();
        crate::testkit::automation_claim_conformance(&store).await;
    }
}
