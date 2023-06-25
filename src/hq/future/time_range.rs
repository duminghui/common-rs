use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{NaiveDate, NaiveTime};
use itertools::Itertools;
use lazy_static::lazy_static;
use sqlx::MySqlPool;

use crate::mysqlx::types::VecType;

// use crate::mysqlx::types::{VecNaiveTime, VecString};

#[derive(Debug, sqlx::FromRow)]
pub struct TimeRange {
    #[sqlx(rename = "Breed")]
    pub breed:       String,
    #[sqlx(rename = "TDDay")]
    pub td_day:      NaiveDate,
    #[sqlx(rename = "closestart")]
    pub close_start: VecType<NaiveTime>,
    #[sqlx(rename = "closetimes")]
    pub close_times: VecType<NaiveTime>,
    #[sqlx(rename = "opentimes")]
    pub open_times:  VecType<NaiveTime>,
    #[sqlx(rename = "openstart")]
    pub open_start:  VecType<NaiveTime>,
    #[sqlx(rename = "closeend")]
    pub close_end:   VecType<NaiveTime>,
    #[sqlx(rename = "ks1day")]
    pub ks1_day:     i32,
    #[sqlx(rename = "ks1span")]
    pub ks1_span:    VecType<String>,
    #[sqlx(rename = "ks1WD")]
    pub ks1_wd:      i32,
    #[sqlx(rename = "ks1MD")]
    pub ks1_md:      i32,
}

impl TimeRange {
    pub fn times_vec_unique(&self) -> (Vec<&NaiveTime>, Vec<&NaiveTime>) {
        let open_times = self.open_times.iter().unique().collect::<Vec<_>>();
        let close_times = self.close_times.iter().unique().collect::<Vec<_>>();
        (open_times, close_times)
    }
}

lazy_static! {
    static ref TX_TIME_RANGE_DATA: RwLock<Arc<HashMap<String, Arc<TimeRange>>>> =
        RwLock::new(Default::default());
}

async fn time_range_list_from_db(pool: &MySqlPool) -> Result<Vec<TimeRange>, sqlx::Error> {
    let sql = "SELECT Breed,TDDay,closestart,closetimes,opentimes,openstart,closeend,ks1day,ks1span,ks1WD,ks1MD FROM basedata.tbl_time_range";
    let items = sqlx::query_as::<_, TimeRange>(sql).fetch_all(pool).await?;
    Ok(items)
}

#[derive(Debug, thiserror::Error)]
pub enum TimeRangeError {
    #[error("{0}")]
    SqxlError(#[from] sqlx::Error),

    #[error("breed: {0}, open_times close_times not same")]
    OpenCloseTimeCountError(String),
}

pub async fn init_from_db(pool: &MySqlPool) -> Result<(), TimeRangeError> {
    if !TX_TIME_RANGE_DATA.read().unwrap().is_empty() {
        return Ok(());
    }
    let items = time_range_list_from_db(pool).await?;
    let mut hmap = HashMap::new();
    for item in items {
        if item.open_times.len() != item.close_times.len() {
            Err(TimeRangeError::OpenCloseTimeCountError(item.breed.clone()))?;
        }
        hmap.insert(item.breed.clone(), Arc::new(item));
    }
    *TX_TIME_RANGE_DATA.write().unwrap() = Arc::new(hmap);
    Ok(())
}

pub(crate) fn hash_map() -> Arc<HashMap<String, Arc<TimeRange>>> {
    TX_TIME_RANGE_DATA.read().unwrap().clone()
}

pub fn time_range_by_breed(breed: &str) -> Result<Arc<TimeRange>, String> {
    let hmap = TX_TIME_RANGE_DATA.read().unwrap();
    let time_range = hmap
        .get(breed)
        .ok_or_else(|| format!("breed not exist: {}", breed))?;
    Ok(time_range.clone())
}

#[cfg(test)]
mod tests {
    use super::{init_from_db, time_range_list_from_db};
    use crate::hq::future::time_range::time_range_by_breed;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    #[tokio::test]
    async fn test_time_range_list_from_db() {
        init_test_mysql_pools();
        let r = time_range_list_from_db(&MySqlPools::pool()).await;
        println!("{:?}", r)
    }

    #[tokio::test]
    async fn test_init_from_db_and_get() {
        init_test_mysql_pools();
        init_from_db(&MySqlPools::pool()).await.unwrap();
        let time_range = time_range_by_breed("a").unwrap();
        println!("{:?}", time_range)
    }
}
