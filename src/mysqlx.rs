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

// pub type DateTime = chrono::DateTime<chrono::Utc>;

pub struct PoolConfig {
    database: Option<String>,
    min_conns: u32,
    max_conns: u32,
    idle_timeout: u64,
    acquire_timeout: u64,
}

impl PoolConfig {
    pub fn new(
        database: Option<String>,
        min_conns: u32,
        max_conns: u32,
        idle_timeout: u64,
        acquire_timeout: u64,
    ) -> PoolConfig {
        PoolConfig {
            database,
            min_conns,
            max_conns,
            idle_timeout,
            acquire_timeout,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ConnectConfig {
    #[serde(rename = "host")]
    host: String,
    #[serde(rename = "port")]
    port: u16,
    #[serde(rename = "user")]
    username: String,
    #[serde(rename = "passwd")]
    password: String,
}

pub fn conn_config_from_file(
    filepath: impl AsRef<Path> + std::fmt::Debug,
) -> Result<HashMap<String, ConnectConfig>, YamlParseError> {
    // let config_hmap = yaml::parse_from_file::<_, HashMap<String, ConnectConfig>>(filepath)?;
    // Ok(config_hmap)
    yaml::parse_from_file::<_, HashMap<String, ConnectConfig>>(filepath)
}

#[derive(Debug, thiserror::Error)]
pub enum PoolConnError {
    #[error(r#"db connect info "{0}" not exists!"#)]
    ConfigKeyNotExist(String),

    #[error("{0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("init err when read: {0}")]
    InitLoclRead(#[from] PoisonError<RwLockReadGuard<'static, MySqlPools>>),

    #[error("init err when write: {0}")]
    InitLockWrite(#[from] PoisonError<RwLockWriteGuard<'static, MySqlPools>>),
}

pub fn connect_pool(
    config_hmap: &HashMap<String, ConnectConfig>,
    key: &str,
    pool_config: &PoolConfig,
    log_sql: bool,
) -> Result<MySqlPool, PoolConnError> {
    let config = config_hmap
        .get(key)
        .ok_or_else(|| PoolConnError::ConfigKeyNotExist(key.to_owned()))?;

    let mut connect_opts = MySqlConnectOptions::new()
        .host(&config.host)
        .port(config.port)
        .username(&config.username)
        .password(&config.password)
        // .database("hqdb")
        .charset("utf8")
        .collation("utf8_general_ci")
        .ssl_mode(MySqlSslMode::Disabled);

    if let Some(database) = &pool_config.database {
        connect_opts = connect_opts.database(database);
    }

    if !log_sql {
        connect_opts.log_statements(log::LevelFilter::Off);
    }

    let pool_mysql = MySqlPoolOptions::new()
        .min_connections(pool_config.min_conns)
        .max_connections(pool_config.max_conns)
        .idle_timeout(Duration::from_secs(pool_config.idle_timeout))
        .acquire_timeout(Duration::from_millis(pool_config.acquire_timeout))
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
    default: Option<Arc<MySqlPool>>,
    pool_hmap: HashMap<String, Arc<MySqlPool>>,
}

impl MySqlPools {
    pub fn init_one_pool(
        config_hmap: &HashMap<String, ConnectConfig>,
        key: &str,
        pool_config: &PoolConfig,
        log_sql: bool,
        default: bool,
    ) -> Result<(), PoolConnError> {
        if MYSQL_POOLS.read()?.pool_hmap.contains_key(key) {
            return Ok(());
        }
        let mut pools = MYSQL_POOLS.write()?;
        let pool_mysql = Arc::new(connect_pool(config_hmap, key, pool_config, log_sql)?);
        pools.pool_hmap.insert(key.to_owned(), pool_mysql.clone());
        if default {
            pools.default = Some(pool_mysql);
        }
        Ok(())
    }

    pub fn default() -> Arc<MySqlPool> {
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::{conn_config_from_file, ConnectConfig, MySqlPools, PoolConfig, MYSQL_POOLS};

    #[test]
    fn test_read_conn_config() {
        let config_hm = conn_config_from_file("/opt/kds/work/configs/db-rs.yaml");
        println!("{:#?}", config_hm);
    }

    #[tokio::test]
    async fn test_init() {
        let conf_hmap: HashMap<String, ConnectConfig> =
            conn_config_from_file("/opt/kds/work/configs/db-rs.yaml").unwrap();
        MySqlPools::init_one_pool(
            &conf_hmap,
            "s133",
            &PoolConfig::new(None, 1, 1, 3000, 3000),
            true,
            true,
        )
        .unwrap();
        let arc_count = Arc::strong_count(MYSQL_POOLS.read().unwrap().default.as_ref().unwrap());
        println!("count: {} count==2: {}", arc_count, arc_count == 2);
        let pool = MySqlPools::default();
        let arc_count = Arc::strong_count(&pool);
        println!("count: {} count==3: {}", arc_count, arc_count == 3);
    }

    #[cfg(feature = "qh")]
    #[tokio::test]
    async fn test_thread() {
        use crate::qh::klineitem::KLineItemUtil;
        let conf_hmap: HashMap<String, ConnectConfig> =
            conn_config_from_file("/opt/kds/work/configs/db-rs.yaml").unwrap();
        MySqlPools::init_one_pool(
            &conf_hmap,
            "s133",
            &PoolConfig::new(None, 1, 3, 3000, 3000),
            true,
            true,
        )
        .unwrap();

        println!("3: {}", Arc::strong_count(&MySqlPools::default()));

        let mut handles = Vec::with_capacity(10);
        for i in 0..10 {
            let pool = MySqlPools::default();
            handles.push(tokio::spawn(async move {
                let klit = KLineItemUtil::new("hqdb");
                let item_vec = klit.item_vec_oldest(&pool, "ag", 5, 10).await.unwrap();
                for item in item_vec.iter() {
                    println!("{} {}", i, item);
                }
            }))
        }
        for handle in handles {
            handle.await.unwrap();
        }
        println!("5: {}", Arc::strong_count(&MySqlPools::default()));
    }
}
