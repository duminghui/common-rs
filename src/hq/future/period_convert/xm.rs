use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::MySqlPool;

use super::PeriodConvertError;
use crate::hq::future::time_range;
use crate::hq::period::PeriodValue;

#[allow(unused)]
#[derive(Debug, Clone)]
struct PeriodTimeInfo {
    // 周期开始时间
    pub s_time: NaiveTime,
    // 周期结束时间
    e_time:     NaiveTime,

    // 交易日是否添加一天
    day_add_1: bool,

    // 是否使用trade_date作为日期
    use_trade_date: bool,
}

static BREED_CONVERTERXM_HMAP: OnceLock<HashMap<String, Arc<ConverterXm>>> = OnceLock::new();

pub async fn init_from_time_range(pool: Arc<MySqlPool>) -> Result<(), PeriodConvertError> {
    if BREED_CONVERTERXM_HMAP.get().is_some() {
        return Ok(());
    }
    time_range::init_from_db(pool).await?;

    let mut breed_period_time = HashMap::new();
    let periods = &["5m", "15m", "30m", "60m", "120m"];

    let date = NaiveDate::default();
    let time_range_hmap = time_range::hash_map();

    let time_2059 = NaiveTime::from_hms_opt(20, 59, 0).unwrap();
    let time_235959 = NaiveTime::from_hms_opt(23, 59, 59).unwrap();
    let time_0300 = NaiveTime::from_hms_opt(3, 0, 0).unwrap();
    let time_0859 = NaiveTime::from_hms_opt(8, 59, 0).unwrap();

    let mut period_time_info_map = HashMap::new();

    for (breed, time_range) in time_range_hmap {
        let times_vec = time_range.times_vec();

        let mut period_time_map = HashMap::new();

        for period in periods {
            let pv = PeriodValue::pv(period).unwrap();
            let mut idx = 0;
            let mut period_s_dt = None;
            let mut time_vec = Vec::new();
            let mut time_ptime_map = HashMap::new();
            for (open_time, close_time) in times_vec.iter() {
                let open_dt = date.and_time(*open_time);
                let close_dt = if open_time > close_time {
                    date.succ_opt().unwrap().and_time(*close_time)
                } else {
                    date.and_time(*close_time)
                };
                let mut time = open_dt + Duration::try_minutes(1).unwrap();
                while time <= close_dt {
                    if period_s_dt.is_none() {
                        period_s_dt = Some(time);
                    }
                    idx += 1;
                    let start_time = period_s_dt.unwrap();
                    time_vec.push((start_time, time));
                    if idx % pv == 0 {
                        let start_dt = period_s_dt.take().unwrap();
                        let end_dt = time;
                        let mut night_diff_day = false;
                        let mut use_trade_date = false;
                        let s_time = start_dt.time();
                        let e_time = end_dt.time();
                        if s_time > time_2059 && e_time < time_0300 {
                            night_diff_day = true;
                        } else if s_time < time_0300 && e_time > time_0859 {
                            use_trade_date = true
                        }
                        for (_, dt) in time_vec.iter() {
                            let time = dt.time();
                            let day_add_1 =
                                night_diff_day && time >= time_2059 && time <= time_235959;

                            let key =
                                format!("{}-{}-{}-{}", s_time, e_time, day_add_1, use_trade_date);
                            let period_time_info = period_time_info_map
                                .entry(key)
                                .or_insert_with(|| {
                                    Arc::new(PeriodTimeInfo {
                                        s_time,
                                        e_time,
                                        day_add_1,
                                        use_trade_date,
                                    })
                                })
                                .clone();

                            time_ptime_map.insert(time, period_time_info.clone());
                        }
                        time_vec.clear();
                    }
                    time += Duration::try_minutes(1).unwrap();
                }
            }

            if !time_vec.is_empty() {
                let (start_dt, _) = time_vec.first().unwrap();
                let (_, end_dt) = time_vec.last().unwrap();
                let mut night_diff_day = false;
                let mut use_trade_date = false;
                let s_time = start_dt.time();
                let e_time = end_dt.time();
                if s_time > time_2059 && e_time < time_0300 {
                    night_diff_day = true;
                } else if s_time < time_0300 && e_time > time_0859 {
                    use_trade_date = true
                }
                for (_, dt) in time_vec {
                    let time = dt.time();
                    let day_add_1 = night_diff_day && time >= time_2059 && time <= time_235959;

                    let key = format!("{}-{}-{}-{}", s_time, e_time, day_add_1, use_trade_date);
                    let period_time_info = period_time_info_map
                        .entry(key)
                        .or_insert_with(|| {
                            Arc::new(PeriodTimeInfo {
                                s_time,
                                e_time,
                                day_add_1,
                                use_trade_date,
                            })
                        })
                        .clone();
                    time_ptime_map.insert(time, period_time_info.clone());
                }
            }
            period_time_map.insert(period.to_string(), time_ptime_map);
        }
        breed_period_time.insert(breed.to_string(), Arc::new(ConverterXm { period_time_map }));
    }
    BREED_CONVERTERXM_HMAP.set(breed_period_time).unwrap();
    Ok(())
}

#[derive(Debug)]
pub struct ConverterXm {
    period_time_map: HashMap<String, HashMap<NaiveTime, Arc<PeriodTimeInfo>>>,
}

impl ConverterXm {
    ///
    /// trade_date
    pub fn convert(
        &self,
        period: &str,
        dt: &NaiveDateTime,
        trade_date: &NaiveDate,
    ) -> Result<NaiveDateTime, PeriodConvertError> {
        let time_period_info_map = self
            .period_time_map
            .get(period)
            .ok_or(PeriodConvertError::PeriodError(period.to_string()))?;

        let time_key = dt.time();
        let period_time_info = time_period_info_map
            .get(&time_key)
            .ok_or(PeriodConvertError::TimeError(*dt))?;

        let e_time = period_time_info.e_time;

        let datetime = if period_time_info.day_add_1 {
            dt.date().succ_opt().unwrap().and_time(e_time)
        } else if period_time_info.use_trade_date {
            trade_date.and_time(e_time)
        } else {
            dt.date().and_time(e_time)
        };
        Ok(datetime)
    }
}

pub(crate) fn by_breed(breed: &str) -> Result<Arc<ConverterXm>, PeriodConvertError> {
    let converter1m = BREED_CONVERTERXM_HMAP
        .get()
        .unwrap()
        .get(breed)
        .ok_or(PeriodConvertError::BreedError(breed.to_string()))?
        .clone();
    Ok(converter1m)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{NaiveDate, NaiveDateTime};

    use super::init_from_time_range;
    use crate::hq::future::period_convert::xm::by_breed;
    use crate::hq::future::time_range;
    use crate::hq::period::PeriodValue;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    #[tokio::test]
    async fn test_init_from_time_range() {
        init_test_mysql_pools();
        let r = init_from_time_range(MySqlPools::pool()).await;
        println!("r: {:?}", r);
    }

    async fn print_period_time_range(breed: &str) {
        println!("==== {} ======", breed);
        init_test_mysql_pools();
        init_from_time_range(MySqlPools::pool()).await.unwrap();

        let time_range = time_range::time_range_by_breed(breed).unwrap();
        for (open_time, close_time) in time_range.times_vec().iter() {
            println!("{} ~ {}", open_time, close_time);
        }
        println!();
    }

    async fn print_breed_period_info(breed: &str, period: &str, day: &NaiveDate) {
        println!("==== {} {} ======", breed, period);
        init_test_mysql_pools();
        init_from_time_range(MySqlPools::pool()).await.unwrap();
        let time_range = time_range::time_range_by_breed(breed).unwrap();

        // 周期时间和对应的时间vec;
        let mut ptime_time_map = HashMap::<NaiveDateTime, Vec<NaiveDateTime>>::new();
        let mut ptime_vec = Vec::<NaiveDateTime>::new();

        let converterxm = by_breed(breed).unwrap();

        let (minutes, trade_date) = time_range.day_minutes(day);

        let mut pre_period_time = None;

        let mut idx = 1;

        for minute in minutes {
            let period_time = converterxm.convert(period, &minute, &trade_date).unwrap();
            if let Some(pre_period_time) = pre_period_time {
                if pre_period_time != period_time {
                    println!("------------------");
                    idx = 1;
                }
            }
            println!("{:3} {} {}", idx, minute, period_time);
            let entity = ptime_time_map.entry(period_time).or_insert_with_key(|k| {
                ptime_vec.push(*k);
                Vec::new()
            });
            entity.push(minute);
            pre_period_time = Some(period_time);
            idx += 1;
        }
        println!();
        let pv = PeriodValue::pv(period).unwrap();
        for period_time in ptime_vec {
            let time_vec = ptime_time_map.get(&period_time).unwrap();
            let time_vec_len = time_vec.len();
            let start_time = time_vec.first().unwrap();
            let end_time = time_vec.last().unwrap();
            println!(
                "{} {:3}[{:5}] [{} .. {}]",
                period_time,
                time_vec_len,
                time_vec_len == *pv as usize,
                start_time,
                end_time
            )
        }
        println!();
    }

    #[tokio::test]
    async fn test_print_period_info_lr() {
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "LR";
        print_period_time_range(breed).await;
        //节假日
        // let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap();
        //平常
        // let day = NaiveDate::from_ymd_opt(2023, 6, 26).unwrap();
        //跨周
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();
        print_breed_period_info(breed, "5m", &day).await;
        // print_breed_period_info(breed, "15m", &day).await;
        // print_breed_period_info(breed, "30m", &day).await;
        // print_breed_period_info(breed, "60m", &day).await;
        // print_breed_period_info(breed, "120m", &day).await;
    }

    #[tokio::test]
    async fn test_print_period_info_ic() {
        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:00:00
        let breed = "IC";
        print_period_time_range(breed).await;
        //节假日
        // let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap();
        //平常
        // let day = NaiveDate::from_ymd_opt(2023, 6, 26).unwrap();
        //跨周
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();
        print_breed_period_info(breed, "5m", &day).await;
        // print_breed_period_info(breed, "15m", &day).await;
        // print_breed_period_info(breed, "30m", &day).await;
        // print_breed_period_info(breed, "60m", &day).await;
        // print_breed_period_info(breed, "120m", &day).await;
    }

    #[tokio::test]
    async fn test_print_period_info_tf() {
        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:15:00
        let breed = "TF";
        print_period_time_range(breed).await;
        //节假日
        // let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap();
        //平常
        // let day = NaiveDate::from_ymd_opt(2023, 6, 26).unwrap();
        //跨周
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();
        // print_breed_period_info(breed, "5m", &day).await;
        // print_breed_period_info(breed, "15m", &day).await;
        // print_breed_period_info(breed, "30m", &day).await;
        // print_breed_period_info(breed, "60m", &day).await;
        print_breed_period_info(breed, "120m", &day).await;
    }

    #[tokio::test]
    async fn test_print_period_info_sa() {
        // 21:00:00 ~ 23:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "SA";
        print_period_time_range(breed).await;
        //节假日
        // let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap();
        //平常
        // let day = NaiveDate::from_ymd_opt(2023, 6, 26).unwrap();
        //跨周
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();
        // print_breed_period_info(breed, "5m", &day).await;
        // print_breed_period_info(breed, "15m", &day).await;
        // print_breed_period_info(breed, "30m", &day).await;
        // print_breed_period_info(breed, "60m", &day).await;
        print_breed_period_info(breed, "120m", &day).await;
    }

    #[tokio::test]
    async fn test_print_period_info_zn() {
        // 21:00:00 ~ 01:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "zn";
        print_period_time_range(breed).await;
        //节假日
        // let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap();
        //平常
        // let day = NaiveDate::from_ymd_opt(2023, 6, 26).unwrap();
        //跨周
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();
        // print_breed_period_info(breed, "5m", &day).await;
        // print_breed_period_info(breed, "15m", &day).await;
        // print_breed_period_info(breed, "30m", &day).await;
        // print_breed_period_info(breed, "60m", &day).await;
        print_breed_period_info(breed, "120m", &day).await;
    }

    #[tokio::test]
    async fn test_print_period_info_ag() {
        // 21:00:00 ~ 02:30:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed: &str = "ag";
        print_period_time_range(breed).await;
        //节假日
        // let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap();
        //平常
        // let day = NaiveDate::from_ymd_opt(2023, 6, 26).unwrap();
        //跨周
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();

        // print_breed_period_info(breed, "5m", &day).await;
        // print_breed_period_info(breed, "15m", &day).await;
        // print_breed_period_info(breed, "30m", &day).await;
        print_breed_period_info(breed, "60m", &day).await;
        // print_breed_period_info(breed, "120m", &day).await;
    }
}
