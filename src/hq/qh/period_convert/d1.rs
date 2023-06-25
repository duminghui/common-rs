use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use lazy_static::lazy_static;
use sqlx::MySqlPool;

use super::PeriodConvertError;
use crate::hq::qh::time_range;

lazy_static! {
    // 一些通用规则之外的时间点
    static ref BREED_CLOSE_TIME_MAP: RwLock<Arc<HashMap<String, NaiveTime>>> = RwLock::new(Default::default());
}

pub async fn init_from_time_range(pool: &MySqlPool) -> Result<(), PeriodConvertError> {
    if !BREED_CLOSE_TIME_MAP.read().unwrap().is_empty() {
        return Ok(());
    }
    time_range::init_from_db(pool).await?;

    let mut breed_close_time = HashMap::new();
    let time_range_hmap = time_range::hash_map();
    for (breed, time_range) in &*time_range_hmap {
        let (_, close_times) = time_range.times_vec_unique();
        let close_time = close_times.last().unwrap();
        breed_close_time.insert(breed.to_string(), *close_time.clone());
    }
    Ok(())
}

pub struct Converter1d;

impl Converter1d {
    pub fn convert(
        breed: &str,
        trade_date: &NaiveDate,
    ) -> Result<NaiveDateTime, PeriodConvertError> {
        let close_time_map = BREED_CLOSE_TIME_MAP.read().unwrap();
        let close_time = close_time_map
            .get(breed)
            .ok_or(PeriodConvertError::BreedError(breed.to_string()))?;
        Ok(trade_date.and_time(close_time.to_owned()))
    }
}
