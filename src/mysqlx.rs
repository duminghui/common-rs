use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use eyre::eyre;
use log::{debug, error};
use serde::Deserialize;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode};
use sqlx::{ConnectOptions, Executor, MySqlPool};
use tokio::sync::Mutex;

use crate::ssh::connect::Ssh;
use crate::ssh::tunnel::{ForwarderMessage, SshTunnel};
use crate::toml::{self, TomlParseError};
use crate::yaml::{self, YamlError};

#[cfg(feature = "mysqlx-batch")]
pub mod batch_exec;
#[cfg(feature = "mysqlx-batch")]
pub mod batch_exec_merger;

pub mod exec;
pub mod sql_builder;
pub mod table;
pub mod types;
pub mod variables;

#[derive(Debug, Deserialize)]
struct PoolConfig {
    #[serde(rename = "default", default)]
    default:              bool,
    #[serde(rename = "ssh-tunnel")]
    ssh:                  Option<Ssh>,
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
    #[serde(rename = "min-conns")]
    min_conns:            u32,
    #[serde(rename = "max-conns")]
    max_conns:            u32,
    #[serde(rename = "acquire-timeout-secs")]
    acquire_timeout_secs: u64,
    #[serde(rename = "idle-timeout-secs")]
    idle_timeout_secs:    u64,
    #[serde(rename = "log-sql")]
    log_sql:              bool,
}

fn conn_config_from_file(
    filepath: impl AsRef<Path> + std::fmt::Debug,
) -> Result<HashMap<String, PoolConfig>, PoolConnError> {
    let file_extension = filepath.as_ref().extension().unwrap_or_default();
    if file_extension != "yaml" && file_extension != "toml" {
        return Err(PoolConnError::Error(eyre!(
            "mysql conn 错误的配置文件: {:?}",
            filepath
        )));
    }
    let config = if file_extension == "yaml" {
        yaml::parse_from_file::<_, HashMap<String, PoolConfig>>(filepath)?
    } else {
        toml::parse_from_file::<_, HashMap<String, PoolConfig>>(filepath)?
    };
    Ok(config)
}

#[derive(Debug, thiserror::Error)]
pub enum PoolConnError {
    #[error("{0}")]
    Error(#[from] eyre::Error),

    #[error("{0}")]
    YamlParseError(#[from] YamlError),

    #[error("{0}")]
    TomlParseError(#[from] TomlParseError),

    #[error(r#"db connect "{0}" not exists!"#)]
    KeyNotExist(String),
    // #[error("{0}")]
    // Sqlx(#[from] sqlx::Error),
    // #[error("init err when read: {0}")]
    // InitLoclRead(#[from] PoisonError<RwLockReadGuard<'static, MySqlPools>>),

    // #[error("init err when write: {0}")]
    // InitLockWrite(#[from] PoisonError<RwLockWriteGuard<'static, MySqlPools>>),
}

async fn connect_pool(config: &PoolConfig) -> Result<MySqlPool, PoolConnError> {
    let (host, port) = if let Some(ssh) = &config.ssh {
        let target_addr = format!("{}:{}", config.host, config.port);
        let ssh_tunnel = SshTunnel::new_by_ssh(ssh.clone(), target_addr)?;
        let (port, mut receiver) = ssh_tunnel.open_tunnel().await?;
        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                match msg {
                    ForwarderMessage::LocalAcceptError(e) => {
                        error!("[ssh-tunnel] local accept error: {:?}", e)
                    },
                    ForwarderMessage::LocalAcceptSuccess(s) => {
                        debug!("[ssh-tunnel] local accept success: {}", s)
                    },
                    ForwarderMessage::LocalReadEof(addr) => {
                        debug!("[ssh-tunnel] local read eof: {}", addr);
                    },
                    ForwarderMessage::TunnelChannelReadEof(addr) => {
                        debug!("[ssh-tunnel] tunnel channel read eof: {}", addr);
                    },
                    ForwarderMessage::Error((addr, e)) => {
                        error!("[ssh-tunnel] tunnel err: {} {:?}", addr, e)
                    },
                }
            }
        });
        ("127.0.0.1", port)
    } else {
        (config.host.as_str(), config.port)
    };
    let mut connect_opts = MySqlConnectOptions::new()
        .host(host)
        .port(port)
        .username(&config.username)
        .password(&config.password)
        .charset(&config.charset)
        .collation(&config.collation)
        .ssl_mode(MySqlSslMode::Disabled);

    if let Some(database) = &config.database {
        connect_opts = connect_opts.database(database);
    }

    if !config.log_sql {
        connect_opts = connect_opts.log_statements(log::LevelFilter::Off);
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

static POOL_CONFIGS: OnceLock<Configs> = OnceLock::new();
static POOLS: OnceLock<Mutex<HashMap<String, Arc<MySqlPool>>>> = OnceLock::new();

#[derive(Debug)]
struct Configs {
    default:     String,
    config_hmap: HashMap<String, PoolConfig>,
    ssh_hmap:    HashMap<String, Arc<Ssh>>,
}

/// mysql数据连接池的管理
#[derive(Debug, Default)]
pub struct MySqlPools {}

impl MySqlPools {
    pub fn init_pools(
        config_file: impl AsRef<Path> + std::fmt::Debug,
    ) -> Result<(), PoolConnError> {
        if POOLS.get().is_some() {
            return Ok(());
        }
        let config_hmap = conn_config_from_file(config_file)?;
        let mut default = String::new();
        let mut ssh_hmap = HashMap::new();
        for (key, config) in config_hmap.iter() {
            if config.default {
                default = key.clone();
            }
            if let Some(ssh) = &config.ssh {
                ssh_hmap.insert(key.clone(), Arc::new(ssh.clone()));
            }
        }
        let configs = Configs {
            default,
            config_hmap,
            ssh_hmap,
        };

        POOL_CONFIGS.set(configs).unwrap();
        POOLS.set(Default::default()).unwrap();

        Ok(())
    }

    pub async fn pool(key: &str) -> Result<Arc<MySqlPool>, PoolConnError> {
        let pool_configs = POOL_CONFIGS.get().unwrap();
        if let Some(config) = pool_configs.config_hmap.get(key) {
            let pools = POOLS.get().unwrap();
            let mut pools = pools.lock().await;
            let pool = if let Some(pool) = pools.get(key) {
                pool.clone()
            } else {
                let pool = connect_pool(config).await?;
                let pool = Arc::new(pool);
                pools.insert(key.to_owned(), pool.clone());
                pool
            };
            drop(pools);
            Ok(pool)
        } else {
            Err(PoolConnError::KeyNotExist(key.to_string()))
        }
    }

    pub async fn pool_default() -> Result<Arc<MySqlPool>, PoolConnError> {
        let pool_configs = POOL_CONFIGS.get().unwrap();
        Self::pool(&pool_configs.default).await
    }

    pub fn pool_ssh(key: &str) -> Arc<Ssh> {
        POOL_CONFIGS
            .get()
            .unwrap()
            .ssh_hmap
            .get(key)
            .unwrap()
            .clone()
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use sqlx::MySqlPool;

    use crate::mysqlx::{conn_config_from_file, MySqlPools};
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    #[test]
    fn test_read_conn_config() {
        let config_hm = conn_config_from_file("./_data/db-conn.yaml");
        println!("{:#?}", config_hm);
    }

    #[tokio::test]
    async fn test_init() {
        MySqlPools::init_pools("./_data/db-conn.yaml").unwrap();
        let pool = MySqlPools::pool_default().await.unwrap();
        let arc_count = Arc::strong_count(&pool);
        println!("count: {} count==2: {}", arc_count, arc_count == 2);
        let pool = MySqlPools::pool_default().await.unwrap();
        let arc_count = Arc::strong_count(&pool);
        println!("count: {} count==3: {}", arc_count, arc_count == 3);
    }

    async fn query_test(pool: &MySqlPool) {
        let sql = "SHOW VARIABLES LIKE 'secure_file_priv';";
        let r = sqlx::query_as::<_, (String, String)>(sql)
            .fetch_one(pool)
            .await;
        println!("{:?}", r)
    }

    #[tokio::test]
    async fn test_ssh_yaml() {
        init_test_mysql_pools();
        // let pool = MySqlPools::by_key("ssh-db-password");
        // query_test(pool.as_ref()).await;

        let pool = MySqlPools::pool("ssh-db-key-pair").await.unwrap();
        query_test(pool.as_ref()).await;
    }

    #[tokio::test]
    async fn test_ssh_toml() {
        MySqlPools::init_pools("./_data/db-conn.toml").unwrap();
        // let pool = MySqlPools::by_key("ssh-db-password");
        // query_test(pool.as_ref()).await;
        //
        let pool = MySqlPools::pool("local-db").await.unwrap();
        query_test(pool.as_ref()).await;

        let pool = MySqlPools::pool("ssh-db-key-pair").await.unwrap();
        query_test(pool.as_ref()).await;
    }
}
