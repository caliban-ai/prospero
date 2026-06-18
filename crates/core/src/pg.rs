//! Shared Postgres plumbing for the clustered-tier backends ([`crate::PostgresStore`],
//! [`crate::PostgresConfigStore`], [`crate::LeasedOwnership`]). Keeps connection
//! setup and idempotent schema creation in one place so the three backends stay
//! consistent (notably the concurrent-startup DDL-race tolerance below).

use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::error::{CoreError, Result};

/// Open a connection pool to `url`.
pub(crate) async fn connect(url: &str) -> Result<PgPool> {
    PgPoolOptions::new()
        .connect(url)
        .await
        .map_err(|e| CoreError::Store(format!("connecting to postgres: {e}")))
}

/// Run an idempotent `CREATE TABLE IF NOT EXISTS` (`ddl`), tolerating the
/// catalog-level race that concurrent first-boot replicas hit. `what` names the
/// object for error context (e.g. `"events table"`).
///
/// `CREATE TABLE IF NOT EXISTS` is **not** atomic at the system-catalog level:
/// when several replicas boot concurrently against a fresh shared DB they can
/// race creating the same table. The loser surfaces the race as one of a few
/// codes — a duplicate-key error on a `pg_*` catalog index (e.g.
/// `pg_type_typname_nsp_index`, SQLSTATE `23505` unique_violation), `42P07`
/// duplicate_table, or `42710` duplicate_object for the table's implicit
/// row-type. All three mean the object now exists, which is exactly what we
/// wanted, so treat them as success and fail only on a genuine error.
pub(crate) async fn ensure_schema(pool: &PgPool, ddl: &str, what: &str) -> Result<()> {
    if let Err(e) = sqlx::query(ddl).execute(pool).await {
        let benign = e
            .as_database_error()
            .and_then(|db| db.code())
            .map(|code| matches!(code.as_ref(), "23505" | "42P07" | "42710"))
            .unwrap_or(false);
        if !benign {
            return Err(CoreError::Store(format!("creating {what}: {e}")));
        }
    }
    Ok(())
}
