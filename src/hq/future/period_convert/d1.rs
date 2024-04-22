use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::MySqlPool;

use super::PeriodConvertError;
use crate::hq::future::time_range;

static BREED_CONVERTER1D_MAP: OnceLock<HashMap<String, Arc<Converter1d>>> = OnceLock::new();

pub async fn init_from_time_range(pool: Arc<MySqlPool>) -> Result<(), PeriodConvertError> {
    if BREED_CONVERTER1D_MAP.get().is_some() {
        return Ok(());
    }
    time_range::init_from_db(pool).await?;

    let mut breed_converter1d_map = HashMap::new();
    let time_range_hmap = time_range::hash_map();
    for (breed, time_range) in time_range_hmap {
        let (_, close_time) = time_range.times_vec().last().unwrap();
        breed_converter1d_map.insert(
            breed.to_string(),
            Arc::new(Converter1d {
                close_time: *close_time,
            }),
        );
    }
    BREED_CONVERTER1D_MAP.set(breed_converter1d_map).unwrap();
    Ok(())
}

#[derive(Debug)]
pub struct Converter1d {
    close_time: NaiveTime,
}

impl Converter1d {
    pub fn convert(&self, trade_date: &NaiveDate) -> NaiveDateTime {
        trade_date.and_time(self.close_time)
    }
}

pub(crate) fn by_breed(breed: &str) -> Result<Arc<Converter1d>, PeriodConvertError> {
    let converter1m = BREED_CONVERTER1D_MAP
        .get()
        .unwrap()
        .get(breed)
        .ok_or(PeriodConvertError::BreedError(breed.to_string()))?
        .clone();
    Ok(converter1m)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::by_breed;
    use crate::hq::future::period_convert::d1::init_from_time_range;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    #[tokio::test]
    async fn test_ag() {
        init_test_mysql_pools();
        init_from_time_range(MySqlPools::pool_default().await.unwrap())
            .await
            .unwrap();
        let trade_date = NaiveDate::from_ymd_opt(2023, 6, 25).unwrap();
        let converter1d = by_breed("ag").unwrap();
        let period_dt = converter1d.convert(&trade_date);
        println!("{}", period_dt);
    }
}
