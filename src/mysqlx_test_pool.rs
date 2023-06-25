#[cfg(test)]
pub(crate) fn init_test_mysql_pools() {
    use crate::mysqlx::MySqlPools;

    MySqlPools::init_pools("./_cfg/c-db-rs.yaml").unwrap();
}
