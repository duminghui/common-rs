use std::time::{Duration, Instant};

use sqlx::mysql::MySqlArguments;
use sqlx::{Executor, MySqlPool};

use crate::human::HumanCountFixPad;

#[derive(thiserror::Error, Debug)]
pub enum ExecError {
    #[error("Sql: [\n{0}\n]\nerr: {1}")]
    Sqlx(String, sqlx::Error),
}

impl From<ExecError> for String {
    fn from(value: ExecError) -> Self {
        value.to_string()
    }
}

#[derive(Debug, Default)]
pub struct ExecInfo {
    pub rows_affected: u64,
    pub elapsed:       Duration,
}

impl std::fmt::Display for ExecInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(Rows affected:{:>9}[{:>9.3?}])",
            HumanCountFixPad(self.rows_affected),
            self.elapsed,
        )
    }
}

pub async fn exec_sql<'a>(pool: &MySqlPool, sql: &str) -> Result<ExecInfo, ExecError> {
    let start = Instant::now();
    let r = pool
        .execute(sql)
        .await
        .map_err(|e| ExecError::Sqlx(sql.to_string(), e))?;

    Ok(ExecInfo {
        rows_affected: r.rows_affected(),
        elapsed:       start.elapsed(),
    })
}

pub async fn exec_sql_args(
    pool: &MySqlPool,
    sql: &str,
    args: MySqlArguments,
) -> Result<ExecInfo, ExecError> {
    let start = Instant::now();
    let r = sqlx::query_with(sql, args)
        .execute(pool)
        .await
        .map_err(|e| ExecError::Sqlx(sql.to_string(), e))?;
    Ok(ExecInfo {
        rows_affected: r.rows_affected(),
        elapsed:       start.elapsed(),
    })
}

/// charset: utf8mb4
/// collation: utf8mb4_general_ci
pub async fn create_db(
    pool: &MySqlPool,
    db_name: &str,
    charset: &str,
    collation: &str,
) -> Result<ExecInfo, ExecError> {
    let sql = format!("CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET {charset} DEFAULT COLLATE {collation}");
    exec_sql(pool, &sql).await
}
