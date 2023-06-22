// use std::collections::HashMap;

// use lazy_static::lazy_static;
// use sqlx::MySqlPool;

// use crate::mysqlx::{conn_config_from_file, connect_pool, ConnectConfig, PoolConfig};

// lazy_static! {
//     static ref CONNECT_CONFIG_HMAP: HashMap<String, ConnectConfig> =
//         conn_config_from_file("/opt/kds/work/configs/db-rs.yaml").unwrap();
//     pub(crate) static ref TEST_MYSQL_POOL: MySqlPool = connect_pool(
//         &CONNECT_CONFIG_HMAP,
//         "s133",
//         &PoolConfig::new(None, 1, 1, 3000, 3000),
//         true
//     )
//     .unwrap();
// }

#[cfg(test)]
pub(crate) fn init_test_mysql_pools() {
    use crate::mysqlx::MySqlPools;

    MySqlPools::init_pools("./_cfg/c-db-rs.yaml").unwrap();
}
