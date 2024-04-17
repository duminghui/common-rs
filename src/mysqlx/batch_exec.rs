use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::mysql::MySqlArguments;
use sqlx::MySqlPool;
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

pub trait SqlEntityReplace: Send {
    fn sql_entity_replace(&self, key: &str, db: &str, tbl_name: &str) -> SqlEntity;
}

#[derive(Debug, Clone)]
pub struct SqlEntity {
    key:  String,
    idx:  u16,
    sql:  String,
    args: MySqlArguments,
}

impl std::fmt::Display for SqlEntity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "key:{}, idx:{}, sql:{}", self.key, self.idx, self.sql)
    }
}

impl SqlEntity {
    pub fn new(key: &str, sql: &str, args: MySqlArguments) -> SqlEntity {
        let key = if key.is_empty() {
            Uuid::now_v7().to_string()
        } else {
            key.to_owned()
        };
        SqlEntity {
            key,
            idx: 0,
            sql: sql.to_owned(),
            args,
        }
    }

    // pub fn add_arg<T>(&mut self, value: T)
    // where
    //     T: Send + for<'a> Encode<'a, MySql> + Type<MySql>,
    // {
    //     if let Some(args) = self.args.as_mut() {
    //         args.add(value);
    //     }
    // }
}

type Result = std::result::Result<BatchExecInfo, BatchExecError>;

/// RA: rows affected
/// C: count
/// T: threshold
#[derive(Debug, Default)]
pub struct BatchExecInfo {
    is_exec:          bool,
    exec_threshold:   u16,
    pub entity_count: u16,
    rows_affected:    u64,
    elapsed:          Duration,
}

impl std::fmt::Display for BatchExecInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_exec {
            write!(
                f,
                "[{:>9.3?}] Rows affected:{:>4}/{:>4} (T:{:>4})",
                self.elapsed, self.rows_affected, self.entity_count, self.exec_threshold
            )
        } else {
            write!(
                f,
                "*Not Exec* C:{:>4}/T:{:>4}",
                self.entity_count, self.exec_threshold,
            )
        }
    }
}

impl BatchExecInfo {
    pub fn is_exec(&self) -> bool {
        self.is_exec
    }
}

#[derive(Error, Debug)]
pub enum BatchExecError {
    #[error("{sql}, {err}")]
    Query { sql: String, err: sqlx::Error },
    #[error("{0}")]
    Sqlx(#[from] sqlx::Error),
}

/// 只支持单线程
pub struct BatchExec {
    pool:           Arc<MySqlPool>,
    exec_threshold: u16,
    entity_idx:     u16,
    entity_map:     HashMap<String, SqlEntity>,
    lock:           Arc<Mutex<()>>,
}

impl BatchExec {
    pub fn new(pool: Arc<MySqlPool>, exec_threshold: u16) -> BatchExec {
        BatchExec {
            pool,
            exec_threshold,
            entity_idx: 0,
            entity_map: Default::default(),
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn add(&mut self, mut entity: SqlEntity) {
        self.entity_idx += 1;

        entity.idx = self.entity_idx;

        self.entity_map.insert(entity.key.clone(), entity);
    }

    async fn sorted_entity_vec(&mut self) -> Vec<SqlEntity> {
        let mut entity_vec = self
            .entity_map
            .values()
            .cloned()
            .collect::<Vec<SqlEntity>>();
        entity_vec.sort_by(|a, b| a.idx.cmp(&b.idx));

        self.entity_idx = 0;
        self.entity_map.clear();

        entity_vec
    }

    async fn execute(&mut self, exec_threshold: u16) -> Result {
        let lock = self.lock.clone();
        let lock = lock.lock().await;

        let start = Instant::now();
        let mut exec_info = BatchExecInfo::default();

        let entity_len = self.entity_map.len() as u16;

        exec_info.exec_threshold = exec_threshold;
        exec_info.entity_count = entity_len;

        if entity_len == 0 || entity_len < exec_threshold {
            drop(lock);
            return Ok(exec_info);
        }

        let pool = &*self.pool.clone();

        let sql_entity_vec = self.sorted_entity_vec().await;

        let mut transaction = pool.begin().await?;

        let mut rows_affected = 0;
        for SqlEntity { sql, args, .. } in sql_entity_vec {
            let result = sqlx::query_with(&sql, args)
                .execute(&mut *transaction)
                .await;
            match result {
                Ok(result) => {
                    rows_affected += result.rows_affected();
                },
                Err(err) => {
                    return Err(BatchExecError::Query { sql, err });
                },
            }
        }
        transaction.commit().await?;

        drop(lock);

        exec_info.is_exec = true;
        exec_info.entity_count = entity_len;
        exec_info.rows_affected = rows_affected;
        exec_info.elapsed = start.elapsed();

        Ok(exec_info)
    }

    pub async fn execute_threshold(&mut self) -> Result {
        self.execute(self.exec_threshold).await
    }

    pub async fn execute_all(&mut self) -> Result {
        self.execute(0).await
    }

    pub async fn execute_single(
        pool: &MySqlPool,
        sql_entity: SqlEntity,
    ) -> std::result::Result<(), sqlx::Error> {
        sqlx::query_with(&sql_entity.sql, sql_entity.args)
            .execute(pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod botch_exec_tests {
    use sqlx::Arguments;

    use super::*;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    #[test]
    fn test_batch_exec_info() {
        println!("{}", BatchExecInfo::default());
    }

    #[test]
    fn test_sql_entity_new() {
        let mut args = MySqlArguments::default();
        args.add(100i32);
        args.add("aaaa");
        SqlEntity::new("", "", args);
    }

    fn batch_exec() -> BatchExec {
        let pool = MySqlPools::pool();
        let mut be = BatchExec::new(pool, 10);
        // let sql = "UPDATE tmp.tbl_tmp SET v_v=? WHERE id=?";
        let sql = "REPLACE INTO tmp.tbl_tmp(v_v,id) VALUES(?,?)";
        let mut args = MySqlArguments::default();
        args.add("v-v-1");
        args.add(0i32);

        let entity = SqlEntity::new("2", sql, args);
        be.add(entity);

        let mut args = MySqlArguments::default();
        args.add("v-v-2");
        args.add(1i32);
        let entity = SqlEntity::new("2", sql, args);
        be.add(entity);

        let mut args = MySqlArguments::default();
        args.add("v-v-3-2");
        args.add(2i32);
        let entity = SqlEntity::new("1", sql, args);
        be.add(entity);

        let mut args = MySqlArguments::default();
        args.add("v-v-4-2");
        args.add(3i32);
        let entity = SqlEntity::new("2", sql, args);
        be.add(entity);

        let mut args = MySqlArguments::default();
        args.add("v-v-5-2");
        args.add(4i32);
        let entity = SqlEntity::new("3", sql, args);
        be.add(entity);

        let mut args = MySqlArguments::default();
        args.add("v-v-5-3");
        args.add(5i32);
        let entity = SqlEntity::new("", sql, args);
        be.add(entity);

        // let entity = SqlEntity::new("4", "UPDATE tmp.tbl_tmp SET v_v='ccc'", Default::default());
        // be.add(entity);

        be
    }

    #[test]
    fn test_batch_exec_new() {
        let be = batch_exec();
        for (k, v) in &be.entity_map {
            println!("## {:?}, {}", k, v);
        }
        let mut a = Some(1);
        a.take();
    }

    #[tokio::test]
    async fn test_sorted_entity_vec() {
        init_test_mysql_pools();
        let mut be = batch_exec();
        let entity_vec = be.sorted_entity_vec().await;
        println!("# len {:?}", entity_vec.len());
        for e in entity_vec {
            println!("## {:?}, {}", e.key, e);
        }
    }

    #[tokio::test]
    async fn test_batch_exec_execute() {
        init_test_mysql_pools();
        let mut be = batch_exec();
        let result = be.execute_all().await;
        match result {
            Ok(info) => {
                println!("Exec info: {}", info);
            },
            Err(err) => {
                eprintln!("Exec err: {}", err);
            },
        }
    }
}
