use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use itertools::Itertools;
use sqlx::MySqlPool;

use super::trade_day;
use crate::mysqlx::types::VecType;

#[allow(unused)]
#[derive(Debug, sqlx::FromRow)]
struct TimeRangeDbItem {
    #[sqlx(rename = "Breed")]
    breed:       String,
    #[sqlx(rename = "TDDay")]
    td_day:      NaiveDate,
    #[sqlx(rename = "closestart")]
    close_start: VecType<NaiveTime>,
    #[sqlx(rename = "closetimes")]
    close_times: VecType<NaiveTime>,
    #[sqlx(rename = "opentimes")]
    open_times:  VecType<NaiveTime>,
    #[sqlx(rename = "openstart")]
    open_start:  VecType<NaiveTime>,
    #[sqlx(rename = "closeend")]
    close_end:   VecType<NaiveTime>,
    #[sqlx(rename = "ks1day")]
    ks1_day:     i32,
    #[sqlx(rename = "ks1span")]
    ks1_span:    VecType<String>,
    #[sqlx(rename = "ks1WD")]
    ks1_wd:      i32,
    #[sqlx(rename = "ks1MD")]
    ks1_md:      i32,
}

impl TimeRangeDbItem {
    pub fn times_vec_unique(&self) -> (Vec<NaiveTime>, Vec<NaiveTime>) {
        let open_times = self.open_times.iter().unique().copied().collect::<Vec<_>>();
        let close_times = self
            .close_times
            .iter()
            .unique()
            .copied()
            .collect::<Vec<_>>();
        (open_times, close_times)
    }
}

async fn time_range_list_from_db(
    pool: Arc<MySqlPool>,
) -> Result<Vec<TimeRangeDbItem>, sqlx::Error> {
    let sql = "SELECT Breed,TDDay,closestart,closetimes,opentimes,openstart,closeend,ks1day,ks1span,ks1WD,ks1MD FROM basedata.tbl_time_range";
    let items = sqlx::query_as::<_, TimeRangeDbItem>(sql)
        .fetch_all(&*pool)
        .await?;
    Ok(items)
}

static TX_TIME_RANGE_DATA: OnceLock<HashMap<String, Arc<TimeRange>>> = OnceLock::new();

// 夜盘结束点,收盘点的特殊时间
#[derive(Debug)]
pub(crate) struct CloseTimeInfo {
    next:                 NaiveTime, // 15:00:00|15:15:00: 下一分钟在夜盘, 其他:正常加一分钟
    non_night_next:       NaiveTime, // 15:00:00|15:15:00: 下一分钟不在夜盘
    is_night_close_2300:  bool,      // 是否夜盘结束点: 23:00
    is_night_close_other: bool,      // 是否夜盘结束点: 1:00 2:30
    is_day_close:         bool,      // 是否收市时间点
}

#[derive(Debug)]
pub struct TimeRange {
    open_times:          Vec<NaiveTime>,
    close_times:         Vec<NaiveTime>,
    has_night:           bool,
    night_open_time:     NaiveTime,
    non_night_open_time: NaiveTime,
    close_time_info_map: HashMap<NaiveTime, CloseTimeInfo>,
}

impl TimeRange {
    pub fn times_vec(&self) -> (&Vec<NaiveTime>, &Vec<NaiveTime>) {
        (&self.open_times, &self.close_times)
    }

    /// dt为自然时间
    pub fn is_first_minute(&self, dt: &NaiveDateTime) -> bool {
        if self.has_night {
            if trade_day::has_night(&dt.date()) {
                dt.time() == self.night_open_time
            } else {
                dt.time() == self.non_night_open_time
            }
        } else {
            dt.time() == self.non_night_open_time
        }
    }

    /// 夜盘结束时间点:
    ///     23:00:00: 取下一交易日
    ///     1:00:00|2:30:00: 当天为交易日, 取当天日期, 当天非交易日(一般是周六的情况), 用传进来的交易日
    /// 15:00:00|15:15:00:
    ///     当天有夜盘: 取当天日期
    ///     当天无夜盘, 当前品种无夜盘: 取下一交易日
    /// 其他结束点: 当天日期
    /// 其他:
    ///     直接加1分钟
    /// 关于返回的NaiveDate:
    ///     如果是收盘时间点: 返回下一交易日
    ///     其他, 返回None
    pub fn next_minute(
        &self,
        dt: &NaiveDateTime,
        trade_day: &NaiveDate,
    ) -> (NaiveDateTime, Option<NaiveDate>) {
        let date = dt.date();
        let td_info = trade_day::trade_day(&date);
        self.close_time_info_map.get(&dt.time()).map_or_else(
            || (*dt + Duration::minutes(1), None),
            |v| {
                let date = if v.is_night_close_2300 {
                    td_info.unwrap().td_next
                } else if v.is_night_close_other {
                    if td_info.is_some() {
                        date
                    } else {
                        *trade_day
                    }
                } else if v.is_day_close {
                    let td_info = td_info.unwrap();

                    if self.has_night && td_info.has_night {
                        date
                    } else {
                        td_info.td_next
                    }
                } else {
                    date
                };
                if v.is_day_close {
                    let td_info = td_info.unwrap();
                    if td_info.has_night {
                        (date.and_time(v.next), Some(td_info.td_next))
                    } else {
                        (date.and_time(v.non_night_next), Some(td_info.td_next))
                    }
                } else {
                    (date.and_time(v.next), None)
                }
            },
        )
    }

    /// 是否一个交易区域的收市时间
    pub fn is_close_time(&self, time: &NaiveTime) -> bool {
        self.close_time_info_map.contains_key(time)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TimeRangeError {
    #[error("{0}")]
    SqxlError(#[from] sqlx::Error),

    #[error("breed: {0}, open_times close_times not same")]
    OpenCloseTimeCountError(String),

    #[error("breed err: {0}")]
    BreedError(String),
}

pub async fn init_from_db(pool: Arc<MySqlPool>) -> Result<(), TimeRangeError> {
    if TX_TIME_RANGE_DATA.get().is_some() {
        return Ok(());
    }
    trade_day::init_from_db(pool.clone()).await?;
    let items = time_range_list_from_db(pool).await?;
    let mut tr_hmap = HashMap::new();
    let mut hmap = HashMap::new();
    let time_2300 = NaiveTime::from_hms_opt(23, 0, 0).unwrap();
    for item in items {
        if item.open_times.len() != item.close_times.len() {
            Err(TimeRangeError::OpenCloseTimeCountError(item.breed.clone()))?;
        }
        let open_times_str = item.open_times.iter().join(",");
        let close_times_str = item.close_times.iter().join(",");
        let key = format!("{}-{}", open_times_str, close_times_str);
        let has_night = unsafe { item.open_times.get_unchecked(0) }
            != unsafe { item.open_times.get_unchecked(1) };

        let time_range = tr_hmap.entry(key).or_insert_with(|| {
            let (open_times, close_times) = item.times_vec_unique();
            let (night_open_time, non_night_open_time) = if has_night {
                unsafe { (open_times.get_unchecked(0), open_times.get_unchecked(1)) }
            } else {
                let open_time = unsafe { open_times.get_unchecked(0) };
                (open_time, open_time)
            };

            let night_open_time = *night_open_time + Duration::minutes(1);
            let non_night_open_time = *non_night_open_time + Duration::minutes(1);

            let mut close_time_info_map = HashMap::new();

            let time_len = open_times.len();

            for i in 0..time_len {
                let close_time = unsafe { *close_times.get_unchecked(i) };
                let next_idx = (i + 1) % time_len;
                let time_next =
                    unsafe { *open_times.get_unchecked(next_idx) + Duration::minutes(1) };
                let mut non_night_next = time_next;
                let mut is_night_close_2300 = false;
                let mut is_night_close_other = false;
                let mut is_day_close = false;
                if has_night {
                    if i == 0 {
                        if close_time == time_2300 {
                            is_night_close_2300 = true;
                        } else {
                            is_night_close_other = true;
                        }
                    }
                    if i == time_len - 1 {
                        non_night_next =
                            unsafe { *open_times.get_unchecked(1) + Duration::minutes(1) };
                    }
                }

                if i == time_len - 1 {
                    is_day_close = true;
                }

                close_time_info_map.insert(
                    close_time,
                    CloseTimeInfo {
                        next: time_next,
                        non_night_next,
                        is_night_close_2300,
                        is_night_close_other,
                        is_day_close,
                    },
                );
            }

            Arc::new(TimeRange {
                open_times: open_times.clone(),
                close_times,
                has_night,
                night_open_time,
                non_night_open_time,
                close_time_info_map,
            })
        });

        hmap.insert(item.breed.clone(), time_range.clone());
    }
    TX_TIME_RANGE_DATA.set(hmap).unwrap();
    Ok(())
}

pub(crate) fn hash_map<'a>() -> &'a HashMap<String, Arc<TimeRange>> {
    TX_TIME_RANGE_DATA.get().unwrap()
}

pub fn time_range_by_breed(breed: &str) -> Result<Arc<TimeRange>, TimeRangeError> {
    let hmap = TX_TIME_RANGE_DATA.get().unwrap();
    let time_range = hmap
        .get(breed)
        .ok_or(TimeRangeError::BreedError(breed.to_string()))?;
    Ok(time_range.clone())
}

// pub fn is_first_minute(breed: &str, dt: &NaiveDateTime) -> Result<bool, TimeRangeError> {
//     TX_TIME_RANGE_DATA
//         .get()
//         .unwrap()
//         .get(breed)
//         .ok_or(TimeRangeError::BreedError(breed.to_string()))
//         .map(|v| v.is_first_minute(dt))
// }

// pub fn next_minute(
//     breed: &str,
//     dt: &NaiveDateTime,
//     trade_day: &NaiveDate,
// ) -> Result<(NaiveDateTime, Option<NaiveDate>), TimeRangeError> {
//     let datetime = TX_TIME_RANGE_DATA
//         .get()
//         .unwrap()
//         .get(breed)
//         .ok_or(TimeRangeError::BreedError(breed.to_string()))?
//         .next_minute(dt, trade_day);
//     Ok(datetime)
// }

// pub fn breed_exist(breed: &str) -> bool {
//     TX_TIME_RANGE_DATA
//         .get()
//         .unwrap()
//         .get(breed)
//         .map_or(false, |_| true)
// }

#[allow(unused)]
#[cfg(test)]
mod tests {

    use chrono::{NaiveDate, NaiveDateTime};

    use super::{init_from_db, time_range_list_from_db};
    use crate::hq::future::time_range::time_range_by_breed;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    // #[tokio::test]
    // async fn test_time_range_list_from_db() {
    //     init_test_mysql_pools();
    //     let r = time_range_list_from_db(&MySqlPools::pool()).await;
    //     println!("{:?}", r)
    // }

    async fn print_time_range(breed: &str) {
        println!("============ {} ===============", breed);
        init_test_mysql_pools();
        init_from_db(MySqlPools::pool()).await.unwrap();
        let time_range = time_range_by_breed(breed).unwrap();
        let open_times = &time_range.open_times;
        let close_times = &time_range.close_times;
        println!("open_times: {:?}", open_times);
        println!("close_times: {:?}", close_times);
        println!("has_night: {}", time_range.has_night);
        println!("night_open_time: {}", time_range.night_open_time);
        println!("non_night_open_time: {}", time_range.non_night_open_time);
        let time_len = open_times.len();

        let minute_info_map = &time_range.close_time_info_map;
        for i in 0..time_len {
            let close_time = unsafe { close_times.get_unchecked(i) };
            let minute_info = minute_info_map.get(close_time).unwrap();
            println!("{}: {:?}", close_time, minute_info);
        }
        println!();
    }

    // #[tokio::test]
    async fn test_init_from_db_and_get() {
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        print_time_range("LR").await;

        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:00:00
        print_time_range("IC").await;

        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:15:00
        print_time_range("TF").await;

        // 21:00:00 ~ 23:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        print_time_range("SA").await;

        // 21:00:00 ~ 01:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        print_time_range("zn").await;

        // 21:00:00 ~ 02:30:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00

        print_time_range("ag").await;
    }

    // #[tokio::test]
    // async fn test_init_from_db_and_get_ag() {
    // }
    //

    async fn test_next_minute(breed: &str, results: &[(&str, &str, &str)]) {
        init_test_mysql_pools();
        init_from_db(MySqlPools::pool()).await.unwrap();
        for (source, target, trade_day) in results {
            let source = NaiveDateTime::parse_from_str(source, "%Y-%m-%d %H:%M:%S").unwrap();
            let target = NaiveDateTime::parse_from_str(target, "%Y-%m-%d %H:%M:%S").unwrap();
            let trade_day = NaiveDate::parse_from_str(trade_day, "%Y-%m-%d").unwrap();
            let time_range = time_range_by_breed(breed).unwrap();
            let (next, next_td) = time_range.next_minute(&source, &trade_day);
            println!(
                "{}, next: {}, {}, {} {:?}",
                source,
                next,
                target,
                target == next,
                next_td
            )
        }
    }

    // #[tokio::test]
    async fn test_next_minute_lr() {
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "LR";
        let results = vec![
            ("2023-06-21 15:00:00", "2023-06-26 09:01:00", "2023-06-26"),
            ("2023-06-27 09:01:00", "2023-06-27 09:02:00", "2023-06-27"),
            ("2023-06-27 10:15:00", "2023-06-27 10:31:00", "2023-06-27"),
            ("2023-06-27 11:30:00", "2023-06-27 13:31:00", "2023-06-27"),
            ("2023-06-27 13:31:00", "2023-06-27 13:32:00", "2023-06-27"),
            ("2023-06-27 14:31:00", "2023-06-27 14:32:00", "2023-06-27"),
            ("2023-06-27 15:00:00", "2023-06-28 09:01:00", "2023-06-28"),
            ("2023-06-30 15:00:00", "2023-07-03 09:01:00", "2023-07-03"),
        ];

        test_next_minute(breed, &results).await;
    }

    // #[tokio::test]
    async fn test_next_minute_ic() {
        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:00:00
        let breed = "IC";
        let results = vec![
            ("2023-06-21 15:00:00", "2023-06-26 09:31:00", "2023-06-26"),
            ("2023-06-27 09:31:00", "2023-06-27 09:32:00", "2023-06-27"),
            ("2023-06-27 10:15:00", "2023-06-27 10:16:00", "2023-06-27"),
            ("2023-06-27 11:30:00", "2023-06-27 13:01:00", "2023-06-27"),
            ("2023-06-27 13:31:00", "2023-06-27 13:32:00", "2023-06-27"),
            ("2023-06-27 14:31:00", "2023-06-27 14:32:00", "2023-06-27"),
            ("2023-06-27 15:00:00", "2023-06-28 09:31:00", "2023-06-28"),
            ("2023-06-30 15:00:00", "2023-07-03 09:31:00", "2023-07-03"),
        ];

        test_next_minute(breed, &results).await;
    }

    // #[tokio::test]
    async fn test_next_minute_tf() {
        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:15:00
        let breed = "TF";
        let results = vec![
            ("2023-06-21 15:15:00", "2023-06-26 09:31:00", "2023-06-26"),
            ("2023-06-27 09:31:00", "2023-06-27 09:32:00", "2023-06-27"),
            ("2023-06-27 10:15:00", "2023-06-27 10:16:00", "2023-06-27"),
            ("2023-06-27 11:30:00", "2023-06-27 13:01:00", "2023-06-27"),
            ("2023-06-27 13:31:00", "2023-06-27 13:32:00", "2023-06-27"),
            ("2023-06-27 14:31:00", "2023-06-27 14:32:00", "2023-06-27"),
            ("2023-06-27 15:15:00", "2023-06-28 09:31:00", "2023-06-28"),
            ("2023-06-30 15:15:00", "2023-07-03 09:31:00", "2023-07-03"),
        ];

        test_next_minute(breed, &results).await;
    }

    // #[tokio::test]
    async fn test_next_minute_sa() {
        // 21:00:00 ~ 23:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "SA";
        let results = vec![
            ("2023-06-21 15:00:00", "2023-06-26 09:01:00", "2023-06-26"), // 节假日
            ("2023-06-27 09:31:00", "2023-06-27 09:32:00", "2023-06-27"),
            ("2023-06-27 10:15:00", "2023-06-27 10:31:00", "2023-06-27"),
            ("2023-06-27 11:30:00", "2023-06-27 13:31:00", "2023-06-27"),
            ("2023-06-27 13:31:00", "2023-06-27 13:32:00", "2023-06-27"),
            ("2023-06-27 14:31:00", "2023-06-27 14:32:00", "2023-06-27"),
            ("2023-06-27 15:00:00", "2023-06-27 21:01:00", "2023-06-28"),
            ("2023-06-27 21:01:00", "2023-06-27 21:02:00", "2023-06-28"),
            ("2023-06-27 23:00:00", "2023-06-28 09:01:00", "2023-06-28"),
            ("2023-06-30 15:00:00", "2023-06-30 21:01:00", "2023-06-30"),
            ("2023-06-30 21:01:00", "2023-06-30 21:02:00", "2023-06-30"),
            ("2023-06-30 22:01:00", "2023-06-30 22:02:00", "2023-06-30"),
            ("2023-06-30 23:00:00", "2023-07-03 09:01:00", "2023-07-03"),
        ];

        test_next_minute(breed, &results).await;
    }

    async fn test_next_minute_zn() {
        // 21:00:00 ~ 01:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "zn";
        let results = vec![
            ("2023-06-21 15:00:00", "2023-06-26 09:01:00", "2023-06-26"), // 节假日
            ("2023-06-27 09:31:00", "2023-06-27 09:32:00", "2023-06-27"),
            ("2023-06-27 10:15:00", "2023-06-27 10:31:00", "2023-06-27"),
            ("2023-06-27 11:30:00", "2023-06-27 13:31:00", "2023-06-27"),
            ("2023-06-27 13:31:00", "2023-06-27 13:32:00", "2023-06-27"),
            ("2023-06-27 14:31:00", "2023-06-27 14:32:00", "2023-06-27"),
            ("2023-06-27 15:00:00", "2023-06-27 21:01:00", "2023-06-28"),
            ("2023-06-27 21:01:00", "2023-06-27 21:02:00", "2023-06-28"),
            ("2023-06-27 23:00:00", "2023-06-27 23:01:00", "2023-06-28"),
            ("2023-06-27 23:59:00", "2023-06-28 00:00:00", "2023-06-28"),
            ("2023-06-28 00:00:00", "2023-06-28 00:01:00", "2023-06-28"),
            ("2023-06-28 00:58:00", "2023-06-28 00:59:00", "2023-06-28"),
            ("2023-06-28 00:59:00", "2023-06-28 01:00:00", "2023-06-28"),
            ("2023-06-28 01:00:00", "2023-06-28 09:01:00", "2023-06-28"),
            ("2023-06-30 15:00:00", "2023-06-30 21:01:00", "2023-06-30"),
            ("2023-06-30 21:01:00", "2023-06-30 21:02:00", "2023-06-30"),
            ("2023-06-30 22:01:00", "2023-06-30 22:02:00", "2023-06-30"),
            ("2023-07-01 01:00:00", "2023-07-03 09:01:00", "2023-07-03"), // 周六
        ];

        test_next_minute(breed, &results).await;
    }

    #[tokio::test]
    async fn test_next_minute_ag() {
        // 21:00:00 ~ 02:30:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "ag";
        let results: Vec<(&str, &str, &str)> = vec![
            ("2023-06-21 15:00:00", "2023-06-26 09:01:00", "2023-06-26"), // 节假日
            ("2023-06-27 09:31:00", "2023-06-27 09:32:00", "2023-06-27"),
            ("2023-06-27 10:15:00", "2023-06-27 10:31:00", "2023-06-27"),
            ("2023-06-27 11:30:00", "2023-06-27 13:31:00", "2023-06-27"),
            ("2023-06-27 13:31:00", "2023-06-27 13:32:00", "2023-06-27"),
            ("2023-06-27 14:31:00", "2023-06-27 14:32:00", "2023-06-27"),
            ("2023-06-27 15:00:00", "2023-06-27 21:01:00", "2023-06-28"),
            ("2023-06-27 21:01:00", "2023-06-27 21:02:00", "2023-06-28"),
            ("2023-06-27 23:00:00", "2023-06-27 23:01:00", "2023-06-28"),
            ("2023-06-27 23:59:00", "2023-06-28 00:00:00", "2023-06-28"),
            ("2023-06-28 00:00:00", "2023-06-28 00:01:00", "2023-06-28"),
            ("2023-06-28 00:58:00", "2023-06-28 00:59:00", "2023-06-28"),
            ("2023-06-28 00:59:00", "2023-06-28 01:00:00", "2023-06-28"),
            ("2023-06-28 01:00:00", "2023-06-28 01:01:00", "2023-06-28"),
            ("2023-06-28 02:30:00", "2023-06-28 09:01:00", "2023-06-28"),
            ("2023-06-30 15:00:00", "2023-06-30 21:01:00", "2023-06-30"),
            ("2023-06-30 21:01:00", "2023-06-30 21:02:00", "2023-06-30"),
            ("2023-06-30 22:01:00", "2023-06-30 22:02:00", "2023-06-30"),
            ("2023-07-01 02:30:00", "2023-07-03 09:01:00", "2023-07-03"), // 周六
        ];

        test_next_minute(breed, &results).await;
    }
}
