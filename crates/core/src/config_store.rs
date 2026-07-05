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
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS repos (\
    name   TEXT PRIMARY KEY,\
    root   TEXT NOT NULL,\
    config TEXT NOT NULL\
)";

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
        sqlx::query(SCHEMA)
            .execute(&pool)
            .await
            .map_err(|e| CoreError::Store(format!("initializing config schema: {e}")))?;
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
}
