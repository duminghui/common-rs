use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use lazy_static::lazy_static;
use serde::Deserialize;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode};
use sqlx::{ConnectOptions, Executor, MySqlPool};

use crate::yaml;
use crate::yaml::YamlParseError;

#[cfg(feature = "mysqlx_batch")]
pub mod batch_exec;

pub mod exec;
pub mod table;

#[derive(Debug, Deserialize)]
struct PoolConfig {
    #[serde(rename = "default")]
    default:              Option<bool>,
    #[serde(rename = "host")]
    host:                 String,
    #[serde(rename = "port")]
    port:                 u16,
    #[serde(rename = "user")]
    username:             String,
    #[serde(rename = "passwd")]
    password:             String,
    #[serde(rename = "database")]
    database:             Option<String>,
    #[serde(rename = "charset")]
    // utf8
    charset: String,
    #[serde(rename = "collation")]
    // utf8_general_ci
    collation: String,
    #[serde(rename = "minConns")]
    min_conns:            u32,
    #[serde(rename = "maxConns")]
    max_conns:            u32,
    #[serde(rename = "idleTimeoutSecs")]
    idle_timeout_secs:    u64,
    #[serde(rename = "acquireTimeoutSecs")]
    acquire_timeout_secs: u64,
    #[serde(rename = "logSql")]
    log_sql:              bool,
}

fn conn_config_from_file(
    filepath: impl AsRef<Path> + std::fmt::Debug,
) -> Result<HashMap<String, PoolConfig>, YamlParseError> {
    yaml::parse_from_file(filepath)
}

#[derive(Debug, thiserror::Error)]
pub enum PoolConnError {
    #[error("{0}")]
    YamlParseError(#[from] YamlParseError),

    #[error(r#"db connect "{0}" not exists!"#)]
    KeyNotExist(String),

    #[error("{0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("init err when read: {0}")]
    InitLoclRead(#[from] PoisonError<RwLockReadGuard<'static, MySqlPools>>),

    #[error("init err when write: {0}")]
    InitLockWrite(#[from] PoisonError<RwLockWriteGuard<'static, MySqlPools>>),
}

fn connect_pool(config: PoolConfig) -> Result<MySqlPool, PoolConnError> {
    let mut connect_opts = MySqlConnectOptions::new()
        .host(&config.host)
        .port(config.port)
        .username(&config.username)
        .password(&config.password)
        .charset(&config.charset)
        .collation(&config.collation)
        .ssl_mode(MySqlSslMode::Disabled);

    if let Some(database) = &config.database {
        connect_opts = connect_opts.database(database);
    }

    if !config.log_sql {
        connect_opts.log_statements(log::LevelFilter::Off);
    }

    let pool_mysql = MySqlPoolOptions::new()
        .min_connections(config.min_conns)
        .max_connections(config.max_conns)
        .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
        .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
        .after_connect(|conn, _meta| {
            // fix: time_zone = '+00:00'
            Box::pin(async move {
                // let mut options = String::new();
                // options.push_str(r#"SET sql_mode=(SELECT CONCAT(@@sql_mode, ',PIPES_AS_CONCAT,NO_ENGINE_SUBSTITUTION')),"#);
                // options.push_str(r#"time_zone='+08:00',"#);
                // options.push_str(&format!(
                //     r#"NAMES {} COLLATE {};"#,
                //     "utf8",
                //     "utf8_general_ci",
                // ));
                // let b = options.as_str();

                // conn.execute(b).await?;

                conn.execute("SET time_zone = '+08:00';").await?;

                Ok(())
            })
        })
        .connect_lazy_with(connect_opts);
    // .connect_with(connect_opts).await?;
    Ok(pool_mysql)
}

lazy_static! {
    static ref MYSQL_POOLS: RwLock<MySqlPools> = RwLock::new(Default::default());
    // static ref MYSQL_POOLS: Arc<RwLock<MySqlPools>> = Arc::new(Default::default());
}

/// mysql数据连接池的管理
#[derive(Default)]
pub struct MySqlPools {
    default:   Option<Arc<MySqlPool>>,
    pool_hmap: HashMap<String, Arc<MySqlPool>>,
}

impl MySqlPools {
    pub fn init_pools(
        config_file: impl AsRef<Path> + std::fmt::Debug,
    ) -> Result<(), PoolConnError> {
        let config_hmap = conn_config_from_file(config_file)?;
        let mut pools = MYSQL_POOLS.write()?;
        for (key, config) in config_hmap {
            let default = config.default;
            let pool_mysql = Arc::new(connect_pool(config)?);
            pools.pool_hmap.insert(key.to_owned(), pool_mysql.clone());
            if let Some(default) = default {
                if default {
                    pools.default = Some(pool_mysql);
                }
            }
        }

        Ok(())
    }

    pub fn pool() -> Arc<MySqlPool> {
        MYSQL_POOLS
            .read()
            .unwrap()
            .default
            .as_ref()
            .unwrap()
            .clone()
    }

    pub fn by_key(key: &str) -> Arc<MySqlPool> {
        let pools = MYSQL_POOLS.read().unwrap();
        pools.pool_hmap.get(key).unwrap().clone()
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use super::conn_config_from_file;
    use crate::mysqlx::{MySqlPools, MYSQL_POOLS};

    #[test]
    fn test_read_conn_config() {
        let config_hm = conn_config_from_file("./_cfg/c-db-rs.yaml");
        println!("{:#?}", config_hm);
    }

    #[tokio::test]
    async fn test_init() {
        MySqlPools::init_pools("./_cfg/c-db-rs.yaml").unwrap();
        let arc_count = Arc::strong_count(MYSQL_POOLS.read().unwrap().default.as_ref().unwrap());
        println!("count: {} count==2: {}", arc_count, arc_count == 2);
        let pool = MySqlPools::pool();
        let arc_count = Arc::strong_count(&pool);
        println!("count: {} count==3: {}", arc_count, arc_count == 3);
    }
}
