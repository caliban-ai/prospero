//! sqlx-backed Postgres [`ConfigStore`] — the clustered-tier config backend.
//!
//! Mirrors [`crate::config_store::SqliteConfigStore`] with Postgres dialect.
//! Runs the same conformance battery, gated on `DATABASE_URL`.

use async_trait::async_trait;
use sqlx::Row;
use sqlx::postgres::PgPool;

use crate::config_store::ConfigStore;
use crate::error::{CoreError, Result};
use crate::registry::RegisteredRepo;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS repos (\
    name   TEXT PRIMARY KEY,\
    root   TEXT NOT NULL,\
    config TEXT NOT NULL\
)";

/// sqlx/Postgres-backed config store (clustered tier).
pub struct PostgresConfigStore {
    pool: PgPool,
}

impl PostgresConfigStore {
    /// Connect to Postgres at `url` and ensure the schema exists.
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = crate::pg::connect(url).await?;
        crate::pg::ensure_schema(&pool, SCHEMA, "repos table").await?;
        Ok(Self { pool })
    }

    /// Truncate all repos. Test-only.
    #[cfg(any(test, feature = "testkit"))]
    pub async fn reset_for_tests(&self) -> Result<()> {
        sqlx::query("TRUNCATE repos")
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::Store(format!("reset: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl ConfigStore for PostgresConfigStore {
    async fn list_repos(&self) -> Result<Vec<RegisteredRepo>> {
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
            repos.push(RegisteredRepo {
                name,
                root: root.into(),
                config: serde_json::from_str(&config_json)?,
            });
        }
        Ok(repos)
    }

    async fn upsert_repo(&self, repo: &RegisteredRepo) -> Result<()> {
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
}
