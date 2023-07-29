use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use itertools::Itertools;
use sqlx::MySqlPool;

use self::minutes::Minutes;
use super::trade_day;
use crate::mysqlx::types::VecType;

pub mod minutes;

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

// 夜盘结束点,收盘点的特殊时间
#[derive(Debug)]
pub(crate) struct CloseTimeInfo {
    next:                 NaiveTime, // 有夜盘情况下的开盘时间
    non_night_next:       NaiveTime, // 无夜盘情况下的开盘时间
    is_night_close_2300:  bool,      // 是否夜盘结束点: 23:00
    is_night_close_other: bool,      // 是否夜盘结束点: 1:00 2:30
    is_day_close:         bool,      // 是否收市时间点
}

#[derive(Debug)]
pub struct TimeRange {
    times_vec:                  Vec<(NaiveTime, NaiveTime)>, // Vec<(open_time,close_time)>
    has_night:                  bool,
    night_open_time:            NaiveTime,
    non_night_open_time:        NaiveTime,
    close_time_info_map:        HashMap<NaiveTime, CloseTimeInfo>,
    non_night_first_close_time: NaiveTime,
    minutes:                    Minutes,
}

impl TimeRange {
    pub fn has_night(&self) -> bool {
        self.has_night
    }

    pub fn times_vec(&self) -> &Vec<(NaiveTime, NaiveTime)> {
        &self.times_vec
    }

    /// day为开始的自然日
    /// 无夜盘的品种, day为交易日返回day的分钟集, day为非交易日返回下一交易日的分钟集
    /// 有夜盘的品种, day为非交易日返回下一交易日白盘的分钟集, day为交易日时, 返回夜盘分钟集(有夜盘)加白盘分钟集
    pub fn day_minutes(&self, day: &NaiveDate) -> (Vec<NaiveDateTime>, NaiveDate) {
        let trade_day = trade_day::trade_day(day);
        let night_day;
        let daytime;

        if !self.has_night {
            night_day = None;

            if trade_day.is_trade_day {
                daytime = trade_day.day;
            } else {
                daytime = trade_day.td_next
            }
        } else if trade_day.is_trade_day {
            if trade_day.has_night {
                night_day = Some(trade_day.day);
            } else {
                night_day = None;
            }
            daytime = trade_day.td_next;
        } else {
            night_day = None;
            daytime = trade_day.td_next;
        }

        let mut minutes = Vec::new();

        for (i, (open_time, close_time)) in self.times_vec.iter().enumerate() {
            let open_time = *open_time;
            let close_time = *close_time;
            if i == 0 && self.has_night && night_day.is_none() {
                continue;
            }
            let day = if self.has_night && i == 0 {
                night_day.unwrap()
            } else {
                daytime
            };
            let mut time = day.and_time(open_time) + Duration::minutes(1);
            let close_dt = if open_time > close_time {
                day.succ_opt().unwrap().and_time(close_time)
            } else {
                day.and_time(close_time)
            };

            while time <= close_dt {
                minutes.push(time);
                time += Duration::minutes(1);
            }
        }

        (minutes, daytime)
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
    pub fn next_minute(&self, dt: &NaiveDateTime) -> (NaiveDateTime, Option<NaiveDate>) {
        let date = dt.date();
        let td_info = trade_day::trade_day(&date);
        self.close_time_info_map.get(&dt.time()).map_or_else(
            || (*dt + Duration::minutes(1), None),
            |v| {
                let date = if v.is_night_close_2300 {
                    td_info.td_next
                } else if v.is_night_close_other {
                    if td_info.is_trade_day {
                        date
                    } else {
                        td_info.td_next
                    }
                } else if v.is_day_close {
                    if self.has_night && td_info.has_night {
                        date
                    } else {
                        td_info.td_next
                    }
                } else {
                    date
                };
                if v.is_day_close {
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

    pub fn next_close_time(&self, dt: &NaiveDateTime) -> Result<NaiveDateTime, String> {
        let next_close_time = self
            .minutes
            .next_close_time(dt, &self.non_night_first_close_time);
        let dt_default = NaiveDateTime::default();
        if next_close_time == dt_default {
            Err(format!("get a default time:{} ", dt_default))
        } else {
            Ok(next_close_time)
        }
    }

    // 当前时间所在的交易时间段的收盘时间
    // pub fn next_close_time(&self, dt: &NaiveDateTime) -> Result<NaiveDateTime, String> {
    //     let day = dt.date();
    //     let trade_day = trade_day::trade_day(&day);

    //     let time = dt.time();

    //     let time_235959 = NaiveTime::from_hms_milli_opt(23, 59, 59, 1999).unwrap();
    //     let time_000000 = NaiveTime::from_hms_opt(0, 0, 0).unwrap();

    //     let len = self.times_vec.len();

    //     let mut result_dt = NaiveDateTime::default();

    //     for (idx, (_, close_time)) in self.times_vec.iter().enumerate() {
    //         let idx = (idx + 1) % len;
    //         let (_, next_close_time) = unsafe { self.times_vec.get_unchecked(idx) };

    //         // let close_time_info = self.close_time_info_map.get(close_time).unwrap();
    //         let next_close_time_info = self.close_time_info_map.get(next_close_time).unwrap();

    //         let close_time = *close_time;
    //         let next_close_time = *next_close_time;

    //         if time > close_time && time <= next_close_time {
    //             // 10:15~11:30 11:30~15:00 11:30~15:15 15:00~23:00 01:00~10:15 02:30~10:15
    //             if !trade_day.is_trade_day {
    //                 // 非交易日: 下一交易日白盘的第一个结束时间
    //                 let idx = if self.has_night { 1 } else { 0 };
    //                 let (_, close_time) = unsafe { self.times_vec.get_unchecked(idx) };
    //                 result_dt = trade_day.td_next.and_time(*close_time);
    //                 break;
    //             } else if next_close_time_info.is_night_close_2300 {
    //                 if trade_day.has_night {
    //                     result_dt = day.and_time(next_close_time);
    //                     break;
    //                 } else {
    //                     let (_, close_time) = unsafe { self.times_vec.get_unchecked(1) };
    //                     result_dt = trade_day.td_next.and_time(*close_time);
    //                     break;
    //                 }
    //             } else {
    //                 result_dt = day.and_time(next_close_time);
    //                 break;
    //             }
    //         } else if close_time > next_close_time {
    //             // 15:00~10:15, 15:00~11:30, 15:15~11:30, 23:00~10:15, 15:00~01:00, 15:00~02:30
    //             // 按跨天处理
    //             if (time > close_time && time < time_235959)
    //                 || (time >= time_000000 && time <= next_close_time)
    //             {
    //                 if time > close_time && time < time_235959 {
    //                     if next_close_time_info.is_night_close_other {
    //                         if trade_day.has_night {
    //                             result_dt = day.succ_opt().unwrap().and_time(next_close_time);
    //                             break;
    //                         } else {
    //                             let idx = if self.has_night { 1 } else { 0 };
    //                             let (_, close_time) = unsafe { self.times_vec.get_unchecked(idx) };
    //                             result_dt = trade_day.td_next.and_time(*close_time);
    //                             break;
    //                         }
    //                     } else {
    //                         result_dt = trade_day.td_next.and_time(next_close_time);
    //                         break;
    //                     }
    //                 } else if self.has_night {
    //                     // 判断前一天是否有夜盘
    //                     let prev_day = day.pred_opt().unwrap();
    //                     let prev_day_info = trade_day::trade_day(&prev_day);

    //                     if prev_day_info.has_night {
    //                         result_dt = day.and_time(next_close_time);
    //                         break;
    //                     } else {
    //                         let idx = if self.has_night { 1 } else { 0 };
    //                         let (_, close_time) = unsafe { self.times_vec.get_unchecked(idx) };
    //                         result_dt = prev_day_info.td_next.and_time(*close_time);
    //                         break;
    //                     }
    //                 } else if trade_day.is_trade_day {
    //                     result_dt = day.and_time(next_close_time);
    //                     break;
    //                 } else {
    //                     let idx = if self.has_night { 1 } else { 0 };
    //                     let (_, close_time) = unsafe { self.times_vec.get_unchecked(idx) };
    //                     result_dt = trade_day.td_next.and_time(*close_time);
    //                     break;
    //                 }
    //             }
    //         }
    //     }

    //     if result_dt == Default::default() {
    //         Err("default time".to_string())
    //     } else {
    //         Ok(result_dt)
    //     }
    // }

    pub fn minute_idx(&self, time: &NaiveTime, day_has_night: bool) -> Result<i16, String> {
        self.minutes.minute_idx(time, day_has_night)
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

static TX_TIME_RANGE_DATA: OnceLock<HashMap<String, Arc<TimeRange>>> = OnceLock::new();

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
            let mut times_vec = Vec::new();

            for i in 0..time_len {
                let open_time = unsafe { *open_times.get_unchecked(i) };
                let close_time = unsafe { *close_times.get_unchecked(i) };
                times_vec.push((open_time, close_time));

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

            let non_night_first_close_time_idx = if has_night { 1 } else { 0 };

            let non_night_first_close_time =
                *unsafe { close_times.get_unchecked(non_night_first_close_time_idx) };

            let minutes = Minutes::new_from_times_vec(&times_vec);

            Arc::new(TimeRange {
                times_vec,
                has_night,
                night_open_time,
                non_night_open_time,
                close_time_info_map,
                non_night_first_close_time,
                minutes,
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

pub fn time_range_qh_base() -> Arc<TimeRange> {
    time_range_by_breed("QHbase").unwrap()
}

pub fn day_all_minutes(day: &NaiveDate) -> Vec<NaiveDateTime> {
    let mut minutes = Vec::new();

    let mut minute = day.and_hms_opt(15, 1, 0).unwrap();
    let end_minute = day.succ_opt().unwrap().and_hms_opt(15, 1, 0).unwrap();
    while minute < end_minute {
        minutes.push(minute);
        minute += Duration::minutes(1)
    }

    minutes
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};

    use super::{init_from_db, time_range_list_from_db};
    use crate::hq::future::time_range::{day_all_minutes, time_range_by_breed};
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    #[test]
    fn test_chrono() {
        let time = NaiveTime::from_hms_milli_opt(23, 59, 59, 999).unwrap();
        println!("{:?}", time);
        let time1 = NaiveTime::from_hms_opt(23, 59, 59).unwrap();
        println!("{:?} {}", time1, time1 < time);
        let time1 = NaiveTime::from_hms_milli_opt(23, 59, 59, 1000).unwrap();
        println!("{:?} {}", time1, time1 < time);
        let time1 = NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap();
        println!("{:?} {}", time1, time1 < time);
    }

    #[test]
    fn test_chrono_2() {
        let mut time = NaiveTime::from_hms_opt(23, 58, 0).unwrap();
        time += Duration::minutes(1);
        println!("{time}");
        time += Duration::minutes(1);
        println!("{time}");
        time += Duration::minutes(1);
        println!("{time}");
        time += Duration::minutes(1);
        println!("{time}");
        time += Duration::minutes(1);
        println!("{time}");
        time += Duration::minutes(1);
        println!("{time}");
    }

    #[tokio::test]
    async fn test_time_range_list_from_db() {
        init_test_mysql_pools();
        let r = time_range_list_from_db(MySqlPools::pool()).await;
        println!("{:?}", r)
    }

    async fn print_time_range(breed: &str) {
        println!("============ {} ===============", breed);
        init_test_mysql_pools();
        init_from_db(MySqlPools::pool()).await.unwrap();
        let time_range = time_range_by_breed(breed).unwrap();
        println!("times_vec: {:?}", time_range.times_vec);
        println!("has_night: {}", time_range.has_night);
        println!("night_open_time: {}", time_range.night_open_time);
        println!("non_night_open_time: {}", time_range.non_night_open_time);

        let minute_info_map = &time_range.close_time_info_map;
        for (_, close_time) in time_range.times_vec.iter() {
            let minute_info = minute_info_map.get(close_time).unwrap();
            println!("{}: {:?}", close_time, minute_info);
        }
        println!();
    }

    #[tokio::test]
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

    async fn test_next_minute(breed: &str, results: &[(&str, &str)]) {
        init_test_mysql_pools();
        init_from_db(MySqlPools::pool()).await.unwrap();
        let time_range = time_range_by_breed(breed).unwrap();
        for (source, target) in results {
            let source = NaiveDateTime::parse_from_str(source, "%Y-%m-%d %H:%M:%S").unwrap();
            let target = NaiveDateTime::parse_from_str(target, "%Y-%m-%d %H:%M:%S").unwrap();
            let (next, next_td) = time_range.next_minute(&source);
            println!(
                "{}, next: {}, t:{}, {} {:?}",
                source,
                next,
                target,
                target == next,
                next_td
            )
        }
    }

    #[tokio::test]
    async fn test_next_minute_lr() {
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "LR";
        let results = vec![
            ("2023-06-21 15:00:00", "2023-06-26 09:01:00"),
            ("2023-06-27 09:01:00", "2023-06-27 09:02:00"),
            ("2023-06-27 10:15:00", "2023-06-27 10:31:00"),
            ("2023-06-27 11:30:00", "2023-06-27 13:31:00"),
            ("2023-06-27 13:31:00", "2023-06-27 13:32:00"),
            ("2023-06-27 14:31:00", "2023-06-27 14:32:00"),
            ("2023-06-27 15:00:00", "2023-06-28 09:01:00"),
            ("2023-06-30 15:00:00", "2023-07-03 09:01:00"),
        ];

        test_next_minute(breed, &results).await;
    }

    #[tokio::test]
    async fn test_next_minute_ic() {
        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:00:00
        let breed = "IC";
        let results = vec![
            ("2023-06-21 15:00:00", "2023-06-26 09:31:00"),
            ("2023-06-27 09:31:00", "2023-06-27 09:32:00"),
            ("2023-06-27 10:15:00", "2023-06-27 10:16:00"),
            ("2023-06-27 11:30:00", "2023-06-27 13:01:00"),
            ("2023-06-27 13:31:00", "2023-06-27 13:32:00"),
            ("2023-06-27 14:31:00", "2023-06-27 14:32:00"),
            ("2023-06-27 15:00:00", "2023-06-28 09:31:00"),
            ("2023-06-30 15:00:00", "2023-07-03 09:31:00"),
        ];

        test_next_minute(breed, &results).await;
    }

    #[tokio::test]
    async fn test_next_minute_tf() {
        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:15:00
        let breed = "TF";
        let results = vec![
            ("2023-06-21 15:15:00", "2023-06-26 09:31:00"),
            ("2023-06-27 09:31:00", "2023-06-27 09:32:00"),
            ("2023-06-27 10:15:00", "2023-06-27 10:16:00"),
            ("2023-06-27 11:30:00", "2023-06-27 13:01:00"),
            ("2023-06-27 13:31:00", "2023-06-27 13:32:00"),
            ("2023-06-27 14:31:00", "2023-06-27 14:32:00"),
            ("2023-06-27 15:15:00", "2023-06-28 09:31:00"),
            ("2023-06-30 15:15:00", "2023-07-03 09:31:00"),
        ];

        test_next_minute(breed, &results).await;
    }

    #[tokio::test]
    async fn test_next_minute_sa() {
        // 21:00:00 ~ 23:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "SA";
        let results = vec![
            ("2023-06-21 15:00:00", "2023-06-26 09:01:00"), // 节假日
            ("2023-06-27 09:31:00", "2023-06-27 09:32:00"),
            ("2023-06-27 10:15:00", "2023-06-27 10:31:00"),
            ("2023-06-27 11:30:00", "2023-06-27 13:31:00"),
            ("2023-06-27 13:31:00", "2023-06-27 13:32:00"),
            ("2023-06-27 14:31:00", "2023-06-27 14:32:00"),
            ("2023-06-27 15:00:00", "2023-06-27 21:01:00"),
            ("2023-06-27 21:01:00", "2023-06-27 21:02:00"),
            ("2023-06-27 23:00:00", "2023-06-28 09:01:00"),
            ("2023-06-30 15:00:00", "2023-06-30 21:01:00"),
            ("2023-06-30 21:01:00", "2023-06-30 21:02:00"),
            ("2023-06-30 22:01:00", "2023-06-30 22:02:00"),
            ("2023-06-30 23:00:00", "2023-07-03 09:01:00"),
        ];

        test_next_minute(breed, &results).await;
    }

    #[tokio::test]
    async fn test_next_minute_zn() {
        // 21:00:00 ~ 01:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let breed = "zn";
        let results = vec![
            ("2023-06-21 15:00:00", "2023-06-26 09:01:00"), // 节假日
            ("2023-06-27 09:31:00", "2023-06-27 09:32:00"),
            ("2023-06-27 10:15:00", "2023-06-27 10:31:00"),
            ("2023-06-27 11:30:00", "2023-06-27 13:31:00"),
            ("2023-06-27 13:31:00", "2023-06-27 13:32:00"),
            ("2023-06-27 14:31:00", "2023-06-27 14:32:00"),
            ("2023-06-27 15:00:00", "2023-06-27 21:01:00"),
            ("2023-06-27 21:01:00", "2023-06-27 21:02:00"),
            ("2023-06-27 23:00:00", "2023-06-27 23:01:00"),
            ("2023-06-27 23:59:00", "2023-06-28 00:00:00"),
            ("2023-06-28 00:00:00", "2023-06-28 00:01:00"),
            ("2023-06-28 00:58:00", "2023-06-28 00:59:00"),
            ("2023-06-28 00:59:00", "2023-06-28 01:00:00"),
            ("2023-06-28 01:00:00", "2023-06-28 09:01:00"),
            ("2023-06-30 15:00:00", "2023-06-30 21:01:00"),
            ("2023-06-30 21:01:00", "2023-06-30 21:02:00"),
            ("2023-06-30 22:01:00", "2023-06-30 22:02:00"),
            ("2023-07-01 01:00:00", "2023-07-03 09:01:00"), // 周六
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
        let results = vec![
            ("2023-06-21 15:00:00", "2023-06-26 09:01:00"), // 节假日
            ("2023-06-27 09:31:00", "2023-06-27 09:32:00"),
            ("2023-06-27 10:15:00", "2023-06-27 10:31:00"),
            ("2023-06-27 11:30:00", "2023-06-27 13:31:00"),
            ("2023-06-27 13:31:00", "2023-06-27 13:32:00"),
            ("2023-06-27 14:31:00", "2023-06-27 14:32:00"),
            ("2023-06-27 15:00:00", "2023-06-27 21:01:00"),
            ("2023-06-27 21:01:00", "2023-06-27 21:02:00"),
            ("2023-06-27 23:00:00", "2023-06-27 23:01:00"),
            ("2023-06-27 23:59:00", "2023-06-28 00:00:00"),
            ("2023-06-28 00:00:00", "2023-06-28 00:01:00"),
            ("2023-06-28 00:58:00", "2023-06-28 00:59:00"),
            ("2023-06-28 00:59:00", "2023-06-28 01:00:00"),
            ("2023-06-28 01:00:00", "2023-06-28 01:01:00"),
            ("2023-06-28 02:30:00", "2023-06-28 09:01:00"),
            ("2023-06-30 15:00:00", "2023-06-30 21:01:00"),
            ("2023-06-30 21:01:00", "2023-06-30 21:02:00"),
            ("2023-06-30 22:01:00", "2023-06-30 22:02:00"),
            ("2023-07-01 02:30:00", "2023-07-03 09:01:00"), // 周六
        ];

        test_next_minute(breed, &results).await;
    }

    async fn print_day_minutes(breed: &str, day: &NaiveDate) {
        init_test_mysql_pools();
        init_from_db(MySqlPools::pool()).await.unwrap();
        let time_range = time_range_by_breed(breed).unwrap();
        let (minutes, trade_date) = time_range.day_minutes(day);
        for (idx, minute) in minutes.iter().enumerate() {
            println!("{:3} {} {}", idx + 1, minute, trade_date);
        }
    }

    #[tokio::test]
    async fn test_day_minutes_lr() {
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();
        print_day_minutes("LR", &day).await;
    }

    #[tokio::test]
    async fn test_day_minutes_ic() {
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();
        print_day_minutes("IC", &day).await;
    }

    #[tokio::test]
    async fn test_day_minutes_tf() {
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();
        print_day_minutes("TF", &day).await;
    }

    #[tokio::test]
    async fn test_day_minutes_sa() {
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();
        print_day_minutes("SA", &day).await;
    }

    #[tokio::test]
    async fn test_day_minutes_zn() {
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();
        print_day_minutes("zn", &day).await;
    }

    #[tokio::test]
    async fn test_day_minutes_ag() {
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();
        print_day_minutes("ag", &day).await;
    }

    async fn print_next_close_time_range(breeds: &[&str]) {
        init_test_mysql_pools();
        init_from_db(MySqlPools::pool()).await.unwrap();
        let mut map1 = HashMap::new();
        let mut map2 = HashMap::new();

        for breed in breeds {
            println!("========= {} ============", breed);
            let time_range = time_range_by_breed(breed).unwrap();
            let len = time_range.times_vec.len();
            for (idx, (_, close_time)) in time_range.times_vec.iter().enumerate() {
                let idx = (idx + 1) % len;
                let (_, next_close_time) = unsafe { time_range.times_vec.get_unchecked(idx) };

                let close_time = *close_time;
                let next_close_time = *next_close_time;

                let key = format!(
                    "{}~{}",
                    close_time.format("%H:%M"),
                    next_close_time.format("%H:%M")
                );

                if close_time < next_close_time {
                    map1.insert(key, 1);
                } else {
                    map2.insert(key, 1);
                }
                println!(
                    "{} {} {}",
                    close_time,
                    next_close_time,
                    close_time > next_close_time
                );
            }
        }
        println!("{:?}", map1.keys());
        println!("{:?}", map2.keys());
    }

    #[tokio::test]
    async fn next_close_time_range() {
        print_next_close_time_range(&["LR", "IC", "TF", "SA", "zn", "ag"]).await;
    }

    async fn test_next_close_time(breed: &str, results: &[(&str, &str)]) {
        println!("========= {} ============", breed);
        init_test_mysql_pools();
        init_from_db(MySqlPools::pool()).await.unwrap();
        let time_range = time_range_by_breed(breed).unwrap();
        for (src, target) in results {
            let src_time = NaiveDateTime::parse_from_str(src, "%Y-%m-%d %H:%M:%S").unwrap();
            let target_time = time_range.next_close_time(&src_time);
            if let Err(err) = target_time {
                println!("{} {}", src_time, err);
                break;
            }
            let target_time = target_time.unwrap();
            let check_time = NaiveDateTime::parse_from_str(target, "%Y-%m-%d %H:%M:%S").unwrap();
            println!(
                "{} t:{} c:{} {}",
                src_time,
                target_time,
                check_time,
                target_time == check_time
            );
        }
    }

    async fn test_next_close_time_all(breed: &str, day: &NaiveDate) {
        println!("========= {} ============", breed);
        init_test_mysql_pools();
        init_from_db(MySqlPools::pool()).await.unwrap();
        let time_range = time_range_by_breed(breed).unwrap();
        let minutes = day_all_minutes(day);
        for (idx, minute) in minutes.iter().enumerate() {
            let close_time = time_range.next_close_time(minute);
            if let Ok(close_time) = close_time {
                println!("{:4} {}, {}", idx + 1, minute, close_time);
            } else {
                println!("{} error", minute);
            }
        }
    }

    #[tokio::test]
    async fn test_next_close_time_lr() {
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let results = &[
            ("2023-06-20 08:59:00", "2023-06-20 10:15:00"),
            ("2023-06-20 09:00:00", "2023-06-20 10:15:00"),
            ("2023-06-20 10:16:00", "2023-06-20 11:30:00"),
            ("2023-06-20 10:30:00", "2023-06-20 11:30:00"),
            ("2023-06-20 11:30:00", "2023-06-20 11:30:00"),
            ("2023-06-20 11:31:00", "2023-06-20 15:00:00"),
            ("2023-06-20 13:30:00", "2023-06-20 15:00:00"),
            ("2023-06-20 15:01:00", "2023-06-21 10:15:00"),
            ("2023-06-20 23:01:00", "2023-06-21 10:15:00"),
            ("2023-06-21 00:00:00", "2023-06-21 10:15:00"),
            ("2023-06-21 01:00:00", "2023-06-21 10:15:00"),
            ("2023-06-21 02:31:00", "2023-06-21 10:15:00"),
            ("2023-06-21 08:59:00", "2023-06-21 10:15:00"),
            ("2023-06-21 09:00:00", "2023-06-21 10:15:00"),
            ("2023-06-21 10:16:00", "2023-06-21 11:30:00"),
            ("2023-06-21 10:30:00", "2023-06-21 11:30:00"),
            ("2023-06-21 11:30:00", "2023-06-21 11:30:00"),
            ("2023-06-21 11:31:00", "2023-06-21 15:00:00"),
            ("2023-06-21 13:30:00", "2023-06-21 15:00:00"),
            ("2023-06-21 15:01:00", "2023-06-26 10:15:00"),
            ("2023-06-21 23:01:00", "2023-06-26 10:15:00"),
            ("2023-06-22 00:00:00", "2023-06-26 10:15:00"),
            ("2023-06-22 01:00:00", "2023-06-26 10:15:00"),
            ("2023-06-22 02:31:00", "2023-06-26 10:15:00"),
            ("2023-06-30 08:59:00", "2023-06-30 10:15:00"),
            ("2023-06-30 09:00:00", "2023-06-30 10:15:00"),
            ("2023-06-30 10:16:00", "2023-06-30 11:30:00"),
            ("2023-06-30 10:30:00", "2023-06-30 11:30:00"),
            ("2023-06-30 11:30:00", "2023-06-30 11:30:00"),
            ("2023-06-30 11:31:00", "2023-06-30 15:00:00"),
            ("2023-06-30 13:30:00", "2023-06-30 15:00:00"),
            ("2023-06-30 15:01:00", "2023-07-03 10:15:00"),
            ("2023-06-30 23:01:00", "2023-07-03 10:15:00"),
            ("2023-07-01 00:00:00", "2023-07-03 10:15:00"),
            ("2023-07-01 01:00:00", "2023-07-03 10:15:00"),
            ("2023-07-01 02:31:00", "2023-07-03 10:15:00"),
            ("2023-07-02 08:59:00", "2023-07-03 10:15:00"),
            ("2023-07-02 09:00:00", "2023-07-03 10:15:00"),
            ("2023-07-02 10:16:00", "2023-07-03 10:15:00"),
            ("2023-07-02 10:30:00", "2023-07-03 10:15:00"),
            ("2023-07-02 11:30:00", "2023-07-03 10:15:00"),
            ("2023-07-02 11:31:00", "2023-07-03 10:15:00"),
            ("2023-07-02 13:30:00", "2023-07-03 10:15:00"),
            ("2023-07-02 15:01:00", "2023-07-03 10:15:00"),
            ("2023-07-03 23:01:00", "2023-07-04 10:15:00"),
            ("2023-07-04 00:00:00", "2023-07-04 10:15:00"),
            ("2023-07-04 01:00:00", "2023-07-04 10:15:00"),
            ("2023-07-04 02:31:00", "2023-07-04 10:15:00"),
        ];
        test_next_close_time("LR", results).await;
    }

    #[tokio::test]
    async fn test_next_close_time_all_lr() {
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let day = NaiveDate::from_ymd_opt(2023, 6, 20).unwrap(); // 正常
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap(); // 节假日
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 6, 22).unwrap(); // 节假日
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap(); // 跨周
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 7, 2).unwrap(); // 正常
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 7, 3).unwrap(); // 正常
        test_next_close_time_all("LR", &day).await;
    }

    #[tokio::test]
    async fn test_next_close_time_ic() {
        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:00:00
        let results = &[
            ("2023-06-20 08:59:00", "2023-06-20 11:30:00"),
            ("2023-06-20 09:00:00", "2023-06-20 11:30:00"),
            ("2023-06-20 10:16:00", "2023-06-20 11:30:00"),
            ("2023-06-20 10:30:00", "2023-06-20 11:30:00"),
            ("2023-06-20 11:30:00", "2023-06-20 11:30:00"),
            ("2023-06-20 11:31:00", "2023-06-20 15:00:00"),
            ("2023-06-20 13:30:00", "2023-06-20 15:00:00"),
            ("2023-06-20 15:01:00", "2023-06-21 11:30:00"),
            ("2023-06-20 23:01:00", "2023-06-21 11:30:00"),
            ("2023-06-21 00:00:00", "2023-06-21 11:30:00"),
            ("2023-06-21 01:00:00", "2023-06-21 11:30:00"),
            ("2023-06-21 02:31:00", "2023-06-21 11:30:00"),
            ("2023-06-21 08:59:00", "2023-06-21 11:30:00"),
            ("2023-06-21 09:00:00", "2023-06-21 11:30:00"),
            ("2023-06-21 10:16:00", "2023-06-21 11:30:00"),
            ("2023-06-21 10:30:00", "2023-06-21 11:30:00"),
            ("2023-06-21 11:30:00", "2023-06-21 11:30:00"),
            ("2023-06-21 11:31:00", "2023-06-21 15:00:00"),
            ("2023-06-21 13:30:00", "2023-06-21 15:00:00"),
            ("2023-06-21 15:01:00", "2023-06-26 11:30:00"),
            ("2023-06-21 23:01:00", "2023-06-26 11:30:00"),
            ("2023-06-22 00:00:00", "2023-06-26 11:30:00"),
            ("2023-06-22 01:00:00", "2023-06-26 11:30:00"),
            ("2023-06-22 02:31:00", "2023-06-26 11:30:00"),
            ("2023-06-30 08:59:00", "2023-06-30 11:30:00"),
            ("2023-06-30 09:00:00", "2023-06-30 11:30:00"),
            ("2023-06-30 10:16:00", "2023-06-30 11:30:00"),
            ("2023-06-30 10:30:00", "2023-06-30 11:30:00"),
            ("2023-06-30 11:30:00", "2023-06-30 11:30:00"),
            ("2023-06-30 11:31:00", "2023-06-30 15:00:00"),
            ("2023-06-30 13:30:00", "2023-06-30 15:00:00"),
            ("2023-06-30 15:01:00", "2023-07-03 11:30:00"),
            ("2023-06-30 23:01:00", "2023-07-03 11:30:00"),
            ("2023-07-01 00:00:00", "2023-07-03 11:30:00"),
            ("2023-07-01 01:00:00", "2023-07-03 11:30:00"),
            ("2023-07-01 02:31:00", "2023-07-03 11:30:00"),
            ("2023-07-02 08:59:00", "2023-07-03 11:30:00"),
            ("2023-07-02 09:00:00", "2023-07-03 11:30:00"),
            ("2023-07-02 10:16:00", "2023-07-03 11:30:00"),
            ("2023-07-02 10:30:00", "2023-07-03 11:30:00"),
            ("2023-07-02 11:30:00", "2023-07-03 11:30:00"),
            ("2023-07-02 11:31:00", "2023-07-03 11:30:00"),
            ("2023-07-02 13:30:00", "2023-07-03 11:30:00"),
            ("2023-07-02 15:01:00", "2023-07-03 11:30:00"),
            ("2023-07-03 23:01:00", "2023-07-04 11:30:00"),
            ("2023-07-04 00:00:00", "2023-07-04 11:30:00"),
            ("2023-07-04 01:00:00", "2023-07-04 11:30:00"),
            ("2023-07-04 02:31:00", "2023-07-04 11:30:00"),
        ];
        test_next_close_time("IC", results).await;
    }

    #[tokio::test]
    async fn test_next_close_time_all_ic() {
        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:00:00
        // let day = NaiveDate::from_ymd_opt(2023, 6, 20).unwrap(); // 正常
        // let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap(); // 节假日
        // let day = NaiveDate::from_ymd_opt(2023, 6, 22).unwrap(); // 节假日
        // let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap(); // 跨周
        let day = NaiveDate::from_ymd_opt(2023, 7, 2).unwrap(); // 正常
        test_next_close_time_all("IC", &day).await;
    }

    #[tokio::test]
    async fn test_next_close_time_tf() {
        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:15:00
        let results = &[
            ("2023-06-20 08:59:00", "2023-06-20 11:30:00"),
            ("2023-06-20 09:00:00", "2023-06-20 11:30:00"),
            ("2023-06-20 10:16:00", "2023-06-20 11:30:00"),
            ("2023-06-20 10:30:00", "2023-06-20 11:30:00"),
            ("2023-06-20 11:30:00", "2023-06-20 11:30:00"),
            ("2023-06-20 11:31:00", "2023-06-20 15:15:00"),
            ("2023-06-20 13:30:00", "2023-06-20 15:15:00"),
            ("2023-06-20 15:01:00", "2023-06-20 15:15:00"),
            ("2023-06-20 23:01:00", "2023-06-21 11:30:00"),
            ("2023-06-21 00:00:00", "2023-06-21 11:30:00"),
            ("2023-06-21 01:00:00", "2023-06-21 11:30:00"),
            ("2023-06-21 02:31:00", "2023-06-21 11:30:00"),
            ("2023-06-21 08:59:00", "2023-06-21 11:30:00"),
            ("2023-06-21 09:00:00", "2023-06-21 11:30:00"),
            ("2023-06-21 10:16:00", "2023-06-21 11:30:00"),
            ("2023-06-21 10:30:00", "2023-06-21 11:30:00"),
            ("2023-06-21 11:30:00", "2023-06-21 11:30:00"),
            ("2023-06-21 11:31:00", "2023-06-21 15:15:00"),
            ("2023-06-21 13:30:00", "2023-06-21 15:15:00"),
            ("2023-06-21 15:01:00", "2023-06-21 15:15:00"),
            ("2023-06-21 23:01:00", "2023-06-26 11:30:00"),
            ("2023-06-22 00:00:00", "2023-06-26 11:30:00"),
            ("2023-06-22 01:00:00", "2023-06-26 11:30:00"),
            ("2023-06-22 02:31:00", "2023-06-26 11:30:00"),
            ("2023-06-30 08:59:00", "2023-06-30 11:30:00"),
            ("2023-06-30 09:00:00", "2023-06-30 11:30:00"),
            ("2023-06-30 10:16:00", "2023-06-30 11:30:00"),
            ("2023-06-30 10:30:00", "2023-06-30 11:30:00"),
            ("2023-06-30 11:30:00", "2023-06-30 11:30:00"),
            ("2023-06-30 11:31:00", "2023-06-30 15:15:00"),
            ("2023-06-30 13:30:00", "2023-06-30 15:15:00"),
            ("2023-06-30 15:01:00", "2023-06-30 15:15:00"),
            ("2023-06-30 23:01:00", "2023-07-03 11:30:00"),
            ("2023-07-01 00:00:00", "2023-07-03 11:30:00"),
            ("2023-07-01 01:00:00", "2023-07-03 11:30:00"),
            ("2023-07-01 02:31:00", "2023-07-03 11:30:00"),
            ("2023-07-02 08:59:00", "2023-07-03 11:30:00"),
            ("2023-07-02 09:00:00", "2023-07-03 11:30:00"),
            ("2023-07-02 10:16:00", "2023-07-03 11:30:00"),
            ("2023-07-02 10:30:00", "2023-07-03 11:30:00"),
            ("2023-07-02 11:30:00", "2023-07-03 11:30:00"),
            ("2023-07-02 11:31:00", "2023-07-03 11:30:00"),
            ("2023-07-02 13:30:00", "2023-07-03 11:30:00"),
            ("2023-07-02 15:01:00", "2023-07-03 11:30:00"),
            ("2023-07-03 23:01:00", "2023-07-04 11:30:00"),
            ("2023-07-04 00:00:00", "2023-07-04 11:30:00"),
            ("2023-07-04 01:00:00", "2023-07-04 11:30:00"),
            ("2023-07-04 02:31:00", "2023-07-04 11:30:00"),
        ];
        test_next_close_time("TF", results).await;
    }

    #[tokio::test]
    async fn test_next_close_time_all_tf() {
        // 09:30:00 ~ 11:30:00
        // 13:00:00 ~ 15:15:00
        // let day = NaiveDate::from_ymd_opt(2023, 6, 20).unwrap(); // 正常
        let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap(); // 节假日
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 6, 22).unwrap(); // 节假日
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap(); // 跨周
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 7, 2).unwrap(); // 正常
        test_next_close_time_all("TF", &day).await;
    }

    #[tokio::test]
    async fn test_next_close_time_sa() {
        // 21:00:00 ~ 23:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let results = &[
            ("2023-06-20 20:59:00", "2023-06-20 23:00:00"),
            ("2023-06-20 21:01:00", "2023-06-20 23:00:00"),
            ("2023-06-20 23:01:00", "2023-06-21 10:15:00"),
            ("2023-06-21 00:00:00", "2023-06-21 10:15:00"),
            ("2023-06-21 01:01:00", "2023-06-21 10:15:00"),
            ("2023-06-21 02:31:00", "2023-06-21 10:15:00"),
            ("2023-06-21 08:59:00", "2023-06-21 10:15:00"),
            ("2023-06-21 09:01:00", "2023-06-21 10:15:00"),
            ("2023-06-21 10:16:00", "2023-06-21 11:30:00"),
            ("2023-06-21 13:31:00", "2023-06-21 15:00:00"),
            ("2023-06-21 20:59:00", "2023-06-26 10:15:00"),
            ("2023-06-21 23:59:00", "2023-06-26 10:15:00"),
            ("2023-06-22 00:00:00", "2023-06-26 10:15:00"),
            ("2023-06-22 01:00:00", "2023-06-26 10:15:00"),
            ("2023-06-22 02:00:00", "2023-06-26 10:15:00"),
            ("2023-06-22 03:00:00", "2023-06-26 10:15:00"),
            ("2023-06-25 03:00:00", "2023-06-26 10:15:00"),
            ("2023-06-30 20:59:00", "2023-06-30 23:00:00"),
            ("2023-06-30 21:01:00", "2023-06-30 23:00:00"),
            ("2023-06-30 23:01:00", "2023-07-03 10:15:00"),
            ("2023-07-01 00:00:00", "2023-07-03 10:15:00"),
            ("2023-07-01 01:01:00", "2023-07-03 10:15:00"),
            ("2023-07-01 02:31:00", "2023-07-03 10:15:00"),
            ("2023-07-01 08:59:00", "2023-07-03 10:15:00"),
            ("2023-07-01 09:01:00", "2023-07-03 10:15:00"),
            ("2023-07-01 10:16:00", "2023-07-03 10:15:00"),
            ("2023-07-01 13:31:00", "2023-07-03 10:15:00"),
            ("2023-07-01 20:59:00", "2023-07-03 10:15:00"),
            ("2023-07-01 23:59:00", "2023-07-03 10:15:00"),
            ("2023-07-02 00:00:00", "2023-07-03 10:15:00"),
            ("2023-07-02 01:00:00", "2023-07-03 10:15:00"),
            ("2023-07-02 02:00:00", "2023-07-03 10:15:00"),
            ("2023-07-02 03:00:00", "2023-07-03 10:15:00"),
            ("2023-07-03 03:00:00", "2023-07-03 10:15:00"),
            ("2023-07-03 08:59:00", "2023-07-03 10:15:00"),
            ("2023-07-03 10:16:00", "2023-07-03 11:30:00"),
            ("2023-07-03 11:31:00", "2023-07-03 15:00:00"),
            ("2023-07-03 12:31:00", "2023-07-03 15:00:00"),
            ("2023-07-03 15:31:00", "2023-07-03 23:00:00"),
        ];
        test_next_close_time("SA", results).await;
    }

    #[tokio::test]
    async fn test_next_close_time_all_sa() {
        // 21:00:00 ~ 23:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let day = NaiveDate::from_ymd_opt(2023, 6, 20).unwrap(); // 正常
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap(); // 节假日
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 6, 22).unwrap(); // 节假日
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap(); // 跨周
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 7, 2).unwrap(); // 跨周
                                                                 // let day = NaiveDate::from_ymd_opt(2023, 7, 3).unwrap(); // 正常
        test_next_close_time_all("SA", &day).await;
    }

    #[tokio::test]
    async fn test_next_close_time_zn() {
        // 21:00:00 ~ 01:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let results = &[
            ("2023-06-20 20:59:00", "2023-06-21 01:00:00"),
            ("2023-06-20 21:01:00", "2023-06-21 01:00:00"),
            ("2023-06-20 23:01:00", "2023-06-21 01:00:00"),
            ("2023-06-21 00:00:00", "2023-06-21 01:00:00"),
            ("2023-06-21 01:01:00", "2023-06-21 10:15:00"),
            ("2023-06-21 02:31:00", "2023-06-21 10:15:00"),
            ("2023-06-21 08:59:00", "2023-06-21 10:15:00"),
            ("2023-06-21 09:01:00", "2023-06-21 10:15:00"),
            ("2023-06-21 10:16:00", "2023-06-21 11:30:00"),
            ("2023-06-21 13:31:00", "2023-06-21 15:00:00"),
            ("2023-06-21 20:59:00", "2023-06-26 10:15:00"),
            ("2023-06-21 23:59:00", "2023-06-26 10:15:00"),
            ("2023-06-22 00:00:00", "2023-06-26 10:15:00"),
            ("2023-06-22 01:00:00", "2023-06-26 10:15:00"),
            ("2023-06-22 02:00:00", "2023-06-26 10:15:00"),
            ("2023-06-22 03:00:00", "2023-06-26 10:15:00"),
            ("2023-06-25 03:00:00", "2023-06-26 10:15:00"),
            ("2023-06-30 20:59:00", "2023-07-01 01:00:00"),
            ("2023-06-30 21:01:00", "2023-07-01 01:00:00"),
            ("2023-06-30 23:01:00", "2023-07-01 01:00:00"),
            ("2023-07-01 00:00:00", "2023-07-01 01:00:00"),
            ("2023-07-01 01:01:00", "2023-07-03 10:15:00"),
            ("2023-07-01 02:31:00", "2023-07-03 10:15:00"),
            ("2023-07-01 08:59:00", "2023-07-03 10:15:00"),
            ("2023-07-01 09:01:00", "2023-07-03 10:15:00"),
            ("2023-07-01 10:16:00", "2023-07-03 10:15:00"),
            ("2023-07-01 13:31:00", "2023-07-03 10:15:00"),
            ("2023-07-01 20:59:00", "2023-07-03 10:15:00"),
            ("2023-07-01 23:59:00", "2023-07-03 10:15:00"),
            ("2023-07-02 00:00:00", "2023-07-03 10:15:00"),
            ("2023-07-02 01:00:00", "2023-07-03 10:15:00"),
            ("2023-07-02 02:00:00", "2023-07-03 10:15:00"),
            ("2023-07-02 03:00:00", "2023-07-03 10:15:00"),
            ("2023-07-03 03:00:00", "2023-07-03 10:15:00"),
            ("2023-07-03 08:59:00", "2023-07-03 10:15:00"),
            ("2023-07-03 10:16:00", "2023-07-03 11:30:00"),
            ("2023-07-03 11:31:00", "2023-07-03 15:00:00"),
            ("2023-07-03 12:31:00", "2023-07-03 15:00:00"),
            ("2023-07-03 15:31:00", "2023-07-04 01:00:00"),
        ];
        test_next_close_time("zn", results).await;
        // println!("");
        // test_next_close_time("ag", results).await;
    }

    #[tokio::test]
    async fn test_next_close_time_all_zn() {
        // 21:00:00 ~ 01:00:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        // let day = NaiveDate::from_ymd_opt(2023, 6, 20).unwrap(); // 正常
        // let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap(); // 节假日
        // let day = NaiveDate::from_ymd_opt(2023, 6, 22).unwrap(); // 节假日
        // let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap(); // 跨周
        // let day = NaiveDate::from_ymd_opt(2023, 7, 2).unwrap(); // 跨周
        let day = NaiveDate::from_ymd_opt(2023, 7, 3).unwrap(); // 正常
        test_next_close_time_all("zn", &day).await;
    }

    #[tokio::test]
    async fn test_next_close_time_ag() {
        // 21:00:00 ~ 02:30:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        let results = &[
            ("2023-06-20 20:59:00", "2023-06-21 02:30:00"),
            ("2023-06-20 21:01:00", "2023-06-21 02:30:00"),
            ("2023-06-20 23:01:00", "2023-06-21 02:30:00"),
            ("2023-06-21 00:00:00", "2023-06-21 02:30:00"),
            ("2023-06-21 01:01:00", "2023-06-21 02:30:00"),
            ("2023-06-21 02:31:00", "2023-06-21 10:15:00"),
            ("2023-06-21 08:59:00", "2023-06-21 10:15:00"),
            ("2023-06-21 09:01:00", "2023-06-21 10:15:00"),
            ("2023-06-21 10:16:00", "2023-06-21 11:30:00"),
            ("2023-06-21 13:31:00", "2023-06-21 15:00:00"),
            ("2023-06-21 20:59:00", "2023-06-26 10:15:00"),
            ("2023-06-21 23:59:00", "2023-06-26 10:15:00"),
            ("2023-06-22 00:00:00", "2023-06-26 10:15:00"),
            ("2023-06-22 01:00:00", "2023-06-26 10:15:00"),
            ("2023-06-22 02:00:00", "2023-06-26 10:15:00"),
            ("2023-06-22 03:00:00", "2023-06-26 10:15:00"),
            ("2023-06-25 03:00:00", "2023-06-26 10:15:00"),
            ("2023-06-30 20:59:00", "2023-07-01 02:30:00"),
            ("2023-06-30 21:01:00", "2023-07-01 02:30:00"),
            ("2023-06-30 23:01:00", "2023-07-01 02:30:00"),
            ("2023-07-01 00:00:00", "2023-07-01 02:30:00"),
            ("2023-07-01 01:01:00", "2023-07-01 02:30:00"),
            ("2023-07-01 02:31:00", "2023-07-03 10:15:00"),
            ("2023-07-01 08:59:00", "2023-07-03 10:15:00"),
            ("2023-07-01 09:01:00", "2023-07-03 10:15:00"),
            ("2023-07-01 10:16:00", "2023-07-03 10:15:00"),
            ("2023-07-01 13:31:00", "2023-07-03 10:15:00"),
            ("2023-07-01 20:59:00", "2023-07-03 10:15:00"),
            ("2023-07-01 23:59:00", "2023-07-03 10:15:00"),
            ("2023-07-02 00:00:00", "2023-07-03 10:15:00"),
            ("2023-07-02 01:00:00", "2023-07-03 10:15:00"),
            ("2023-07-02 02:00:00", "2023-07-03 10:15:00"),
            ("2023-07-02 03:00:00", "2023-07-03 10:15:00"),
            ("2023-07-03 03:00:00", "2023-07-03 10:15:00"),
            ("2023-07-03 08:59:00", "2023-07-03 10:15:00"),
            ("2023-07-03 10:16:00", "2023-07-03 11:30:00"),
            ("2023-07-03 11:31:00", "2023-07-03 15:00:00"),
            ("2023-07-03 12:31:00", "2023-07-03 15:00:00"),
            ("2023-07-03 15:00:00", "2023-07-04 02:30:00"),
            ("2023-07-03 15:00:01", "2023-07-04 02:30:00"),
            ("2023-07-03 15:31:00", "2023-07-04 02:30:00"),
        ];
        test_next_close_time("ag", results).await;
        // println!("");
        // test_next_close_time("ag", results).await;
    }

    #[tokio::test]
    async fn test_next_close_time_all_ag() {
        // 21:00:00 ~ 02:30:00
        // 09:00:00 ~ 10:15:00
        // 10:30:00 ~ 11:30:00
        // 13:30:00 ~ 15:00:00
        // let day = NaiveDate::from_ymd_opt(2023, 6, 20).unwrap(); // 正常
        // let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap(); // 节假日
        // let day = NaiveDate::from_ymd_opt(2023, 6, 22).unwrap(); // 节假日
        // let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap(); // 跨周
        // let day = NaiveDate::from_ymd_opt(2023, 7, 2).unwrap(); // 跨周
        let day = NaiveDate::from_ymd_opt(2023, 7, 3).unwrap(); // 正常
        test_next_close_time_all("ag", &day).await;
    }
}
