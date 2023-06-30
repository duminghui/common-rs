use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use chrono::{Duration, NaiveDateTime, NaiveTime, Timelike};
use sqlx::MySqlPool;

use super::PeriodConvertError;
use crate::hq::future::time_range;
use crate::ymdhms::Hms;

static BREED_CONVERTER1M_HAMP: OnceLock<HashMap<String, Arc<Converter1m>>> = OnceLock::new();

pub async fn init_from_time_range(pool: Arc<MySqlPool>) -> Result<(), PeriodConvertError> {
    if BREED_CONVERTER1M_HAMP.get().is_some() {
        return Ok(());
    }
    time_range::init_from_db(pool).await?;

    let mut breed_converter1m_hmap = HashMap::new();
    let time_range_hmap = time_range::hash_map();
    for (breed, time_range) in time_range_hmap {
        let (open_times, close_times) = time_range.times_vec();
        let mut hhmm_time_map = HashMap::new();
        for idx in 0..open_times.len() {
            if idx == 0 {
                let open_time = unsafe { open_times.get_unchecked(idx) };
                let hhmmss: Hms = open_time.into();
                match hhmmss.hhmmss {
                    9_00_00 => {
                        hhmm_time_map.insert(859, NaiveTime::from_hms_opt(9, 1, 0).unwrap());
                    },
                    9_30_00 => {
                        hhmm_time_map.insert(929, NaiveTime::from_hms_opt(9, 31, 0).unwrap());
                    },
                    21_00_00 => {
                        hhmm_time_map.insert(2059, NaiveTime::from_hms_opt(21, 1, 0).unwrap());
                    },
                    start => panic!("error start: {}", start),
                }
            }
            let close_time = unsafe { close_times.get_unchecked(idx) };
            let hhmmss: Hms = close_time.into();
            hhmm_time_map.insert(hhmmss.hhmm, *close_time);
        }
        if unsafe { *close_times.get_unchecked(0) } < NaiveTime::from_hms_opt(3, 0, 0).unwrap() {
            hhmm_time_map.insert(0u16, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        }
        breed_converter1m_hmap.insert(breed.to_string(), Arc::new(Converter1m { hhmm_time_map }));
    }
    BREED_CONVERTER1M_HAMP.set(breed_converter1m_hmap).unwrap();

    Ok(())
}

#[derive(Debug)]
pub struct Converter1m {
    // 一些通用规则之外的时间点
    hhmm_time_map: HashMap<u16, NaiveTime>,
}

impl Converter1m {
    /// Tick时间转成1m时间
    /// 特殊时间点
    /// 1. 开盘的前一分钟及第一分钟是属于开盘的时间, 如20:59:xx~21:00:59的K线时间为 21:01:00
    /// 2. 每个交易段的最后时间是属于该段结束时间,  如11:30:00K线时间为11:30:00
    /// 3. 00:00:00时间是属于00:00:00, 而不是 00:01:00
    /// 其他时间
    /// hh:mm:00~xx:mm:59的数据属于hh:(mm+1):00的K线数据
    /// time 为自然时间
    pub fn convert(&self, dt: &NaiveDateTime) -> NaiveDateTime {
        let date = dt.date();
        let hms = Hms::from(dt);

        self.hhmm_time_map.get(&hms.hhmm).map_or_else(
            || {
                let time = dt.time();
                let hour = time.hour();
                let min = time.minute();

                date.and_time(NaiveTime::from_hms_opt(hour, min, 0).unwrap()) + Duration::minutes(1)
            },
            |v| {
                if hms.hhmm == 0 {
                    if hms.second == 0 {
                        date.and_hms_opt(0, 0, 0).unwrap()
                    } else {
                        date.and_hms_opt(0, 1, 0).unwrap()
                    }
                } else {
                    date.and_time(*v)
                }
            },
        )
    }
}

pub(crate) fn by_breed(breed: &str) -> Result<Arc<Converter1m>, PeriodConvertError> {
    let converter1m = BREED_CONVERTER1M_HAMP
        .get()
        .unwrap()
        .get(breed)
        .ok_or(PeriodConvertError::BreedError(breed.to_string()))?
        .clone();
    Ok(converter1m)
}

#[cfg(test)]
mod tests {

    use chrono::NaiveDateTime;

    use super::{by_breed, init_from_time_range};
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    // #[tokio::test]
    // async fn test_init() {
    //     init_test_mysql_pools();
    //     let pool = MySqlPools::pool();
    //     init_from_time_range(&pool).await.unwrap();
    // }

    async fn test_1m(breed: &str, results: &[(&str, &str)]) {
        println!("==== {} ======", breed);
        init_test_mysql_pools();
        init_from_time_range(MySqlPools::pool()).await.unwrap();

        for (source, target) in results {
            let dt = NaiveDateTime::parse_from_str(source, "%Y-%m-%d %H:%M:%S").unwrap();

            let converter1m = by_breed(breed).unwrap();

            let time_1m = converter1m.convert(&dt);
            let time_t = NaiveDateTime::parse_from_str(target, "%Y-%m-%d %H:%M:%S").unwrap();
            println!("{}: {} {} {}", source, time_1m, target, time_1m == time_t)
        }
    }

    // #[tokio::test]
    // async fn test_lr() {
    //     // 09:00:00 ~ 10:15:00
    //     // 10:30:00 ~ 11:30:00
    //     // 13:30:00 ~ 15:00:00
    //     let results = vec![
    //         ("2022-06-13 08:59:00", "2022-06-13 09:01:00"),
    //         ("2022-06-13 09:00:00", "2022-06-13 09:01:00"),
    //         ("2022-06-13 09:01:00", "2022-06-13 09:02:00"),
    //         ("2022-06-13 10:14:59", "2022-06-13 10:15:00"),
    //         ("2022-06-13 10:15:00", "2022-06-13 10:15:00"),
    //         ("2022-06-13 10:30:00", "2022-06-13 10:31:00"),
    //         ("2022-06-13 10:30:59", "2022-06-13 10:31:00"),
    //         ("2022-06-13 11:29:59", "2022-06-13 11:30:00"),
    //         ("2022-06-13 11:30:00", "2022-06-13 11:30:00"),
    //         ("2022-06-13 13:30:00", "2022-06-13 13:31:00"),
    //         ("2022-06-13 14:59:00", "2022-06-13 15:00:00"),
    //         ("2022-06-13 14:59:59", "2022-06-13 15:00:00"),
    //         ("2022-06-13 15:00:00", "2022-06-13 15:00:00"),
    //     ];
    //     test_1m("LR", &results).await;
    // }

    // #[tokio::test]
    // async fn test_ic() {
    //     // 09:30:00 ~ 11:30:00
    //     // 13:00:00 ~ 15:00:00
    //     let results = vec![
    //         ("2022-06-10 09:29:00", "2022-06-10 09:31:00"),
    //         ("2022-06-10 10:15:00", "2022-06-10 10:16:00"),
    //         ("2022-06-10 13:00:00", "2022-06-10 13:01:00"),
    //         ("2022-06-10 13:00:59", "2022-06-10 13:01:00"),
    //         ("2022-06-13 14:59:00", "2022-06-13 15:00:00"),
    //         ("2022-06-13 14:59:59", "2022-06-13 15:00:00"),
    //         ("2022-06-13 15:00:00", "2022-06-13 15:00:00"),
    //     ];
    //     test_1m("IC", &results).await;
    // }

    // #[tokio::test]
    // async fn test_tf() {
    //     // 09:30:00 ~ 11:30:00
    //     // 13:00:00 ~ 15:15:00
    //     let results = vec![
    //         ("2022-06-10 09:29:00", "2022-06-10 09:31:00"),
    //         ("2022-06-10 10:15:00", "2022-06-10 10:16:00"),
    //         ("2022-06-10 13:00:00", "2022-06-10 13:01:00"),
    //         ("2022-06-10 13:00:59", "2022-06-10 13:01:00"),
    //         ("2022-06-13 14:59:00", "2022-06-13 15:00:00"),
    //         ("2022-06-13 14:59:59", "2022-06-13 15:00:00"),
    //         ("2022-06-13 15:00:00", "2022-06-13 15:01:00"),
    //         ("2022-06-13 15:14:59", "2022-06-13 15:15:00"),
    //         ("2022-06-13 15:15:00", "2022-06-13 15:15:00"),
    //     ];
    //     test_1m("TF", &results).await;
    // }

    // #[tokio::test]
    // async fn test_sa() {
    //     // 21:00:00 ~ 23:00:00
    //     // 09:00:00 ~ 10:15:00
    //     // 10:30:00 ~ 11:30:00
    //     // 13:30:00 ~ 15:00:00
    //     let results = vec![
    //         // 夜盘 start
    //         ("2022-06-10 20:59:59", "2022-06-10 21:01:00"),
    //         ("2022-06-10 21:00:00", "2022-06-10 21:01:00"),
    //         ("2022-06-10 22:00:00", "2022-06-10 22:01:00"),
    //         ("2022-06-10 22:59:00", "2022-06-10 23:00:00"),
    //         ("2022-06-10 23:00:00", "2022-06-10 23:00:00"),
    //         // 夜盘 end
    //         // 白盘 start
    //         ("2022-06-13 09:00:00", "2022-06-13 09:01:00"),
    //         ("2022-06-13 10:14:59", "2022-06-13 10:15:00"),
    //         ("2022-06-13 10:15:00", "2022-06-13 10:15:00"),
    //         ("2022-06-13 10:30:00", "2022-06-13 10:31:00"),
    //         ("2022-06-13 10:30:59", "2022-06-13 10:31:00"),
    //         ("2022-06-13 11:29:59", "2022-06-13 11:30:00"),
    //         ("2022-06-13 11:30:00", "2022-06-13 11:30:00"),
    //         ("2022-06-13 13:30:00", "2022-06-13 13:31:00"),
    //         ("2022-06-13 14:59:00", "2022-06-13 15:00:00"),
    //         ("2022-06-13 14:59:59", "2022-06-13 15:00:00"),
    //         ("2022-06-13 15:00:00", "2022-06-13 15:00:00"),
    //         // 白盘 end
    //     ];
    //     test_1m("SA", &results).await;
    // }

    // #[tokio::test]
    // async fn test_zn() {
    //     // 21:00:00 ~ 01:00:00
    //     // 09:00:00 ~ 10:15:00
    //     // 10:30:00 ~ 11:30:00
    //     // 13:30:00 ~ 15:00:00
    //     let results = vec![
    //         // 跨周的时间
    //         // 夜盘 start
    //         ("2022-06-10 20:59:59", "2022-06-10 21:01:00"),
    //         ("2022-06-10 21:00:00", "2022-06-10 21:01:00"),
    //         ("2022-06-10 23:58:33", "2022-06-10 23:59:00"),
    //         ("2022-06-10 23:59:33", "2022-06-11 00:00:00"),
    //         ("2022-06-11 00:00:00", "2022-06-11 00:00:00"),
    //         ("2022-06-11 00:00:33", "2022-06-11 00:01:00"),
    //         ("2022-06-11 00:01:00", "2022-06-11 00:02:00"),
    //         ("2022-06-11 00:59:00", "2022-06-11 01:00:00"),
    //         ("2022-06-11 00:59:59", "2022-06-11 01:00:00"),
    //         ("2022-06-11 01:00:00", "2022-06-11 01:00:00"),
    //         // 夜盘 end
    //         // 白盘 start
    //         ("2022-06-13 09:00:00", "2022-06-13 09:01:00"),
    //         ("2022-06-13 10:14:59", "2022-06-13 10:15:00"),
    //         ("2022-06-13 10:15:00", "2022-06-13 10:15:00"),
    //         ("2022-06-13 10:30:00", "2022-06-13 10:31:00"),
    //         ("2022-06-13 10:30:59", "2022-06-13 10:31:00"),
    //         ("2022-06-13 11:29:59", "2022-06-13 11:30:00"),
    //         ("2022-06-13 11:30:00", "2022-06-13 11:30:00"),
    //         ("2022-06-13 13:30:00", "2022-06-13 13:31:00"),
    //         ("2022-06-13 14:59:00", "2022-06-13 15:00:00"),
    //         ("2022-06-13 14:59:59", "2022-06-13 15:00:00"),
    //         ("2022-06-13 15:00:00", "2022-06-13 15:00:00"),
    //         // 白盘 end
    //     ];
    //     test_1m("zn", &results).await;
    // }

    #[tokio::test]
    async fn test_ag() {
        // 21:00:00 ~ 02:30:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let results = vec![
            // 跨周的时间处理
            // 夜盘 start
            ("2022-06-10 20:59:59", "2022-06-10 21:01:00"),
            ("2022-06-10 21:00:00", "2022-06-10 21:01:00"),
            ("2022-06-10 23:58:33", "2022-06-10 23:59:00"),
            ("2022-06-10 23:59:33", "2022-06-11 00:00:00"),
            ("2022-06-11 00:00:00", "2022-06-11 00:00:00"),
            ("2022-06-11 00:00:33", "2022-06-11 00:01:00"),
            ("2022-06-11 00:01:00", "2022-06-11 00:02:00"),
            ("2022-06-11 00:59:00", "2022-06-11 01:00:00"),
            ("2022-06-11 00:59:59", "2022-06-11 01:00:00"),
            ("2022-06-11 01:30:59", "2022-06-11 01:31:00"),
            ("2022-06-11 01:59:59", "2022-06-11 02:00:00"),
            ("2022-06-11 02:00:33", "2022-06-11 02:01:00"),
            ("2022-06-11 02:29:33", "2022-06-11 02:30:00"),
            ("2022-06-11 02:30:00", "2022-06-11 02:30:00"),
            // 夜盘 end
            // 白盘 start
            ("2022-06-13 09:00:00", "2022-06-13 09:01:00"),
            ("2022-06-13 10:14:59", "2022-06-13 10:15:00"),
            ("2022-06-13 10:15:00", "2022-06-13 10:15:00"),
            ("2022-06-13 10:30:00", "2022-06-13 10:31:00"),
            ("2022-06-13 10:30:59", "2022-06-13 10:31:00"),
            ("2022-06-13 11:29:59", "2022-06-13 11:30:00"),
            ("2022-06-13 11:30:00", "2022-06-13 11:30:00"),
            ("2022-06-13 13:30:00", "2022-06-13 13:31:00"),
            ("2022-06-13 14:59:00", "2022-06-13 15:00:00"),
            ("2022-06-13 14:59:59", "2022-06-13 15:00:00"),
            ("2022-06-13 15:00:00", "2022-06-13 15:00:00"),
            // 白盘 end
        ];
        test_1m("ag", &results).await;
    }
}
