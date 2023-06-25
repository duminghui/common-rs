use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, OnceLock, PoisonError, RwLockReadGuard, RwLockWriteGuard};

use redis::{
    Client, ConnectionAddr, ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo, RedisError,
    RedisResult,
};
use serde::Deserialize;

use crate::yaml::{parse_from_file, YamlParseError};

#[derive(Debug, Deserialize, Clone)]
struct RedisConnInfo {
    #[serde(rename = "default")]
    default:  Option<bool>,
    #[serde(rename = "host")]
    host:     String,
    #[serde(rename = "port")]
    port:     u16,
    #[serde(rename = "db")]
    db:       i64,
    #[serde(rename = "username")]
    username: Option<String>,
    #[serde(rename = "password")]
    password: Option<String>,
}

impl IntoConnectionInfo for RedisConnInfo {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        Ok(ConnectionInfo {
            addr:  ConnectionAddr::Tcp(self.host, self.port),
            redis: RedisConnectionInfo {
                db:       self.db,
                username: self.username,
                password: self.password,
            },
        })
    }
}

fn conn_config_from_file(
    filepath: impl AsRef<Path> + std::fmt::Debug,
) -> Result<HashMap<String, RedisConnInfo>, YamlParseError> {
    // let config_hmap = yaml::parse_from_file::<_, HashMap<String, ConnectConfig>>(filepath)?;
    // Ok(config_hmap)
    parse_from_file(filepath)
}

#[derive(Debug, thiserror::Error)]
pub enum RedisConnError {
    #[error("{0}")]
    YamlParseError(#[from] YamlParseError),

    // #[error(r#"redis key "{0}" not exists!"#)]
    // KeyNotExist(String),
    #[error("{0}")]
    RedisError(#[from] RedisError),

    #[error("init err when read: {0}")]
    InitLoclRead(#[from] PoisonError<RwLockReadGuard<'static, RedisClients>>),

    #[error("init err when write: {0}")]
    InitLockWrite(#[from] PoisonError<RwLockWriteGuard<'static, RedisClients>>),
}

static CLIENTS: OnceLock<RedisClients> = OnceLock::new();

#[derive(Debug, Default)]
pub struct RedisClients {
    default:     Option<Arc<Client>>,
    client_hmap: HashMap<String, Arc<Client>>,
}

impl RedisClients {
    pub fn init_clients(
        config_file: impl AsRef<Path> + std::fmt::Debug,
    ) -> Result<(), RedisConnError> {
        let config_hmap = conn_config_from_file(config_file)?;

        let mut clients = RedisClients::default();

        for (key, conn_info) in config_hmap {
            let default = conn_info.default;
            let client = Client::open(conn_info)?;
            let client = Arc::new(client);
            clients.client_hmap.insert(key, client.clone());
            if let Some(default) = default {
                if default {
                    clients.default = Some(client);
                }
            }
        }
        CLIENTS.set(clients).unwrap();
        Ok(())
    }

    pub fn client() -> Arc<Client> {
        CLIENTS.get().unwrap().default.as_ref().unwrap().clone()
    }

    pub fn by_key(key: &str) -> Arc<Client> {
        let clients = CLIENTS.get().unwrap();
        clients.client_hmap.get(key).unwrap().clone()
    }
}

#[cfg(test)]
mod tests {

    use redis::Commands;

    use super::conn_config_from_file;
    use crate::redis::RedisClients;

    #[test]
    fn test_read_conn_config() {
        let config_hm = conn_config_from_file("./_cfg/c-redis-rs.yaml");
        println!("{:#?}", config_hm);
    }

    #[test]
    fn test_init() {
        let r = RedisClients::init_clients("./_cfg/c-redis-rs.yaml");
        println!("{:?}", r);
    }

    #[test]
    fn test_conn() {
        RedisClients::init_clients("./_cfg/c-redis-rs.yaml").unwrap();
        let client = RedisClients::client();
        let mut con = client.get_connection().unwrap();
        let tmp: Option<String> = con.get("Tmp").unwrap();
        println!("#: Tmp: {:?}", tmp);
    }
}
