//! sqlx-backed Postgres [`ConfigStore`] — the clustered-tier config backend.
//!
//! Mirrors [`crate::config_store::SqliteConfigStore`] with Postgres dialect.
//! Runs the same conformance battery, gated on `DATABASE_URL`.

use async_trait::async_trait;
use sqlx::Row;
use sqlx::postgres::PgPool;

use crate::automation::{AutomationRun, RUN_HISTORY_LIMIT, StoredAutomation};
use crate::config_store::ConfigStore;
use crate::error::{CoreError, Result};
use crate::registry::RegisteredWorkspace;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS repos (\
    name   TEXT PRIMARY KEY,\
    root   TEXT NOT NULL,\
    config TEXT NOT NULL\
)";

/// Mirrors the sqlite schema; see `config_store::AUTOMATIONS_SCHEMA` for why
/// `last_fired_at` is a column rather than a field inside `spec`.
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

/// sqlx/Postgres-backed config store (clustered tier).
pub struct PostgresConfigStore {
    pool: PgPool,
}

impl PostgresConfigStore {
    /// Connect to Postgres at `url` and ensure the schema exists.
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = crate::pg::connect(url).await?;
        crate::pg::ensure_schema(&pool, SCHEMA, "repos table").await?;
        crate::pg::ensure_schema(&pool, AUTOMATIONS_SCHEMA, "automations table").await?;
        crate::pg::ensure_schema(&pool, RUNS_SCHEMA, "automation_runs table").await?;
        crate::pg::ensure_schema(&pool, RUNS_INDEX, "automation_runs index").await?;
        Ok(Self { pool })
    }

    /// Truncate all repos. Test-only.
    #[cfg(any(test, feature = "testkit"))]
    pub async fn reset_for_tests(&self) -> Result<()> {
        sqlx::query("TRUNCATE repos, automations, automation_runs")
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("reset: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl ConfigStore for PostgresConfigStore {
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
        let root = repo
            .root
            .to_str()
            .ok_or_else(|| CoreError::Store(format!("non-UTF8 repo root path: {:?}", repo.root)))?;
        sqlx::query(
            "INSERT INTO repos (name, root, config) VALUES ($1, $2, $3) \
             ON CONFLICT (name) DO UPDATE SET root = excluded.root, config = excluded.config",
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
        let res = sqlx::query("DELETE FROM repos WHERE name = $1")
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
        rows.iter()
            .map(crate::config_store::decode_automation)
            .collect()
    }

    async fn upsert_automation(&self, automation: &StoredAutomation) -> Result<()> {
        let spec = crate::config_store::encode_spec(automation)?;
        sqlx::query(
            "INSERT INTO automations (id, spec, webhook_secret, last_fired_at) \
             VALUES ($1, $2, $3, NULL) \
             ON CONFLICT (id) DO UPDATE SET spec = excluded.spec, \
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
        sqlx::query("DELETE FROM automation_runs WHERE automation_id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("delete_automation runs: {e}")))?;
        let res = sqlx::query("DELETE FROM automations WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("delete_automation: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    async fn claim_automation_fire(&self, id: &str, fire_at: &str) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE automations SET last_fired_at = $1 \
             WHERE id = $2 AND (last_fired_at IS NULL OR last_fired_at < $1)",
        )
        .bind(fire_at)
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("claim_automation_fire: {e}")))?;
        Ok(res.rows_affected() == 1)
    }

    async fn record_run(&self, run: &AutomationRun) -> Result<()> {
        sqlx::query(
            "INSERT INTO automation_runs (automation_id, fired_at, source, agent_id, error) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&run.automation_id)
        .bind(&run.fired_at)
        .bind(crate::config_store::encode_source(run.source)?)
        .bind(&run.agent_id)
        .bind(&run.error)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("record_run: {e}")))?;

        // `ctid` is Postgres' physical row identifier — the equivalent of
        // sqlite's `rowid` here, and the reason the trim can address individual
        // rows even when two runs share a `fired_at`.
        sqlx::query(
            "DELETE FROM automation_runs WHERE automation_id = $1 AND ctid NOT IN \
             (SELECT ctid FROM automation_runs WHERE automation_id = $1 \
              ORDER BY fired_at DESC LIMIT $2)",
        )
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
             WHERE automation_id = $1 ORDER BY fired_at DESC LIMIT $2",
        )
        .bind(automation_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::Store(format!("list_runs: {e}")))?;
        rows.iter().map(crate::config_store::decode_run).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn postgres_config_store_satisfies_conformance() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP postgres_config_store_satisfies_conformance: DATABASE_URL unset");
            return;
        };
        let store = PostgresConfigStore::connect(&url).await.unwrap();
        store.reset_for_tests().await.unwrap();
        crate::testkit::config_store_conformance(&store).await;
    }

    #[tokio::test]
    async fn postgres_config_store_satisfies_automation_conformance() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!(
                "SKIP postgres_config_store_satisfies_automation_conformance: DATABASE_URL unset"
            );
            return;
        };
        let store = PostgresConfigStore::connect(&url).await.unwrap();
        store.reset_for_tests().await.unwrap();
        crate::testkit::automation_store_conformance(&store).await;
    }

    #[tokio::test]
    async fn postgres_config_store_claims_a_tick_once() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP postgres_config_store_claims_a_tick_once: DATABASE_URL unset");
            return;
        };
        let store = PostgresConfigStore::connect(&url).await.unwrap();
        store.reset_for_tests().await.unwrap();
        crate::testkit::automation_claim_conformance(&store).await;
    }
}
