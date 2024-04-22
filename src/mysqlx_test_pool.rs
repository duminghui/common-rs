#[cfg(test)]
pub(crate) fn init_test_mysql_pools() {
    use crate::mysqlx::MySqlPools;

    if let Err(e) = MySqlPools::init_pools("./_data/db-conn.yaml") {
        println!("conn err: {}", e)
    }
}
