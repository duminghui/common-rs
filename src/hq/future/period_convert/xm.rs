use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use lazy_static::lazy_static;
use sqlx::MySqlPool;

use super::PeriodConvertError;
use crate::hq::future::time_range;
use crate::hq::period::PeriodValue;

#[derive(Debug, Clone)]
pub struct PeriodTimeInfo {
    // 周期开始时间
    pub s_time:         NaiveTime,
    // 周期结束时间
    pub e_time:         NaiveTime,
    // 是否使用trade_date作为日期
    pub use_trade_date: bool,
}

// 5m,15m,30m,60m,120m
// HashMap<time,(start_time,end_time)>
type TypeMapTimeTime = HashMap<NaiveTime, PeriodTimeInfo>;
// HashMap<Period,_>
type TypeMapPeriodTime = HashMap<String, TypeMapTimeTime>;
// HashMap<Breed,_>
type TypeMapBreedPeriodTime = HashMap<String, TypeMapPeriodTime>;

lazy_static! {
    static ref PERIOD_TIEM_DATA: RwLock<Arc<TypeMapBreedPeriodTime>> =
        RwLock::new(Default::default());
}

pub async fn init_from_time_range(pool: &MySqlPool) -> Result<(), PeriodConvertError> {
    if !PERIOD_TIEM_DATA.read().unwrap().is_empty() {
        return Ok(());
    }
    time_range::init_from_db(pool).await?;

    let mut breed_period_time = HashMap::new();
    let periods = &["5m", "15m", "30m", "60m", "120m"];
    let date = NaiveDate::default();
    let time_range_hmap = time_range::hash_map();
    for (breed, time_range) in &*time_range_hmap {
        let (open_times, close_times) = time_range.times_vec_unique();
        let open_times_len = open_times.len();

        let mut period_time_map = HashMap::new();

        for period in periods {
            let pv = PeriodValue::pv(period).unwrap();
            let mut idx = 0;
            let mut period_s_time = None;
            let mut time_vec = Vec::new();
            let mut time_ptime_map = HashMap::new();
            for i in 0..open_times_len {
                let open_time = date.and_time(unsafe { **open_times.get_unchecked(i) });
                let mut close_time = date.and_time(unsafe { **close_times.get_unchecked(i) });
                if open_time > close_time {
                    close_time += Duration::days(1);
                }
                let mut time = open_time + Duration::minutes(1);
                while time <= close_time {
                    if period_s_time.is_none() {
                        period_s_time = Some(time);
                    }
                    idx += 1;
                    let start_time = period_s_time.unwrap();
                    time_vec.push((start_time, time));
                    if idx % pv == 0 {
                        let start_time = period_s_time.take().unwrap();
                        let end_time = time;
                        let period_time_info = PeriodTimeInfo {
                            s_time:         start_time.time(),
                            e_time:         end_time.time(),
                            use_trade_date: start_time.date() != end_time.date(),
                        };
                        // println!("{:?} {:?}", period_time_info, time_vec);
                        for (_, time) in time_vec.iter() {
                            time_ptime_map.insert(time.time(), period_time_info.clone());
                        }
                        time_vec.clear();
                    }
                    time += Duration::minutes(1);
                }
            }
            if !time_vec.is_empty() {
                let (start_time, _) = time_vec.first().unwrap();
                let (_, end_time) = time_vec.last().unwrap();
                let period_time_info = PeriodTimeInfo {
                    s_time:         start_time.time(),
                    e_time:         end_time.time(),
                    use_trade_date: start_time.date() != end_time.date(),
                };
                for (_, time) in time_vec {
                    time_ptime_map.insert(time.time(), period_time_info.clone());
                }
            }
            period_time_map.insert(period.to_string(), time_ptime_map);
        }
        breed_period_time.insert(breed.to_string(), period_time_map);
    }
    *PERIOD_TIEM_DATA.write().unwrap() = Arc::new(breed_period_time);
    Ok(())
}

pub struct ConverterXm;

impl ConverterXm {
    pub fn convert(
        breed: &str,
        period: &str,
        dt: &NaiveDateTime,
        trade_date: &NaiveDate,
    ) -> Result<NaiveDateTime, PeriodConvertError> {
        let period_time_map = PERIOD_TIEM_DATA.read().unwrap();

        let period_time_map = period_time_map
            .get(breed)
            .ok_or(PeriodConvertError::BreedError(breed.to_string()))?;

        let time_period_info_map = period_time_map
            .get(period)
            .ok_or(PeriodConvertError::PeriodError(period.to_string()))?;

        let time_key = dt.time();
        let period_time = time_period_info_map
            .get(&time_key)
            .ok_or(PeriodConvertError::TimeError(dt.clone()))?;
        if period_time.use_trade_date {
            Ok(trade_date.and_time(period_time.e_time))
        } else {
            Ok(dt.date().and_time(period_time.e_time))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{Duration, NaiveDate, NaiveDateTime};

    use super::init_from_time_range;
    use crate::hq::future::period_convert::xm::ConverterXm;
    use crate::hq::future::time_range;
    use crate::hq::period::PeriodValue;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    #[tokio::test]
    async fn test_init_from_time_range() {
        init_test_mysql_pools();
        let r = init_from_time_range(&MySqlPools::pool()).await;
        println!("r: {:?}", r);
    }

    async fn test_breed_period(breed: &str, period: &str) {
        println!("==== {} {} ======", breed, period);
        init_test_mysql_pools();
        init_from_time_range(&MySqlPools::pool()).await.unwrap();
        let date = NaiveDate::default() + Duration::days(1);
        let trade_date = date;
        let time_range = time_range::time_range_by_breed(breed).unwrap();
        let (open_times, close_times) = time_range.times_vec_unique();

        // 周期时间和对应的时间vec;
        let mut ptime_time_map = HashMap::<NaiveDateTime, Vec<NaiveDateTime>>::new();
        let mut ptime_vec = Vec::<NaiveDateTime>::new();
        for i in 0..open_times.len() {
            let open_time = unsafe { **open_times.get_unchecked(i) };
            let close_time = unsafe { **close_times.get_unchecked(i) };
            println!("{} ~ {}", open_time, close_time);
        }
        println!("");

        for i in 0..open_times.len() {
            let mut open_time = date.and_time(unsafe { **open_times.get_unchecked(i) });
            let close_time = date.and_time(unsafe { **close_times.get_unchecked(i) });
            if open_time > close_time {
                open_time -= Duration::days(1);
            }
            let mut time = open_time + Duration::minutes(1);

            while time <= close_time {
                let period_time = ConverterXm::convert(breed, period, &time, &trade_date).unwrap();
                let entity = ptime_time_map.entry(period_time).or_insert_with_key(|k| {
                    ptime_vec.push(k.clone());
                    Vec::new()
                });
                entity.push(time);
                time += Duration::minutes(1);
            }
        }
        let pv = PeriodValue::pv(period).unwrap();
        for period_time in ptime_vec {
            let time_vec = ptime_time_map.get(&period_time).unwrap();
            let time_vec_len = time_vec.len();
            let start_time = time_vec.first().unwrap();
            let end_time = time_vec.last().unwrap();
            println!(
                "{} {}[{}] [{} .. {}]",
                period_time,
                time_vec_len,
                time_vec_len == *pv as usize,
                start_time,
                end_time
            )
        }
        println!("");
    }

    #[tokio::test]
    async fn test_period_lr() {
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "LR";
        test_breed_period(breed, "5m").await;
        test_breed_period(breed, "15m").await;
        test_breed_period(breed, "30m").await;
        test_breed_period(breed, "60m").await;
        test_breed_period(breed, "120m").await;
    }

    #[tokio::test]
    async fn test_period_ic() {
        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:00:00
        let breed = "IC";
        test_breed_period(breed, "5m").await;
        test_breed_period(breed, "15m").await;
        test_breed_period(breed, "30m").await;
        test_breed_period(breed, "60m").await;
        test_breed_period(breed, "120m").await;
    }

    #[tokio::test]
    async fn test_period_tf() {
        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:15:00
        let breed = "TF";
        test_breed_period(breed, "5m").await;
        test_breed_period(breed, "15m").await;
        test_breed_period(breed, "30m").await;
        test_breed_period(breed, "60m").await;
        test_breed_period(breed, "120m").await;
    }

    #[tokio::test]
    async fn test_period_sa() {
        // 21:00:00 ~ 23:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "SA";
        test_breed_period(breed, "5m").await;
        test_breed_period(breed, "15m").await;
        test_breed_period(breed, "30m").await;
        test_breed_period(breed, "60m").await;
        test_breed_period(breed, "120m").await;
    }

    #[tokio::test]
    async fn test_period_zn() {
        // 21:00:00 ~ 01:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "zn";
        test_breed_period(breed, "5m").await;
        test_breed_period(breed, "15m").await;
        test_breed_period(breed, "30m").await;
        test_breed_period(breed, "60m").await;
        test_breed_period(breed, "120m").await;
    }

    #[tokio::test]
    async fn test_period_ag() {
        // 21:00:00 ~ 02:30:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "ag";
        test_breed_period(breed, "5m").await;
        test_breed_period(breed, "15m").await;
        test_breed_period(breed, "30m").await;
        test_breed_period(breed, "60m").await;
        test_breed_period(breed, "120m").await;
    }
}
