use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use crate::hq::future::trade_day;

#[derive(Debug)]
pub struct MinuteStrategyInfo {
    close_time:                        NaiveTime, // 所属的收盘点
    is_use_next_td_first_close:        bool,      // 下一交易日的白盘的第一个收盘点
    is_check_day:                      bool, /* 判断当天是不是交易日, 是: day+close_time, 否: 下一交易日的白盘的第一个收盘点 */
    is_check_night_2300:               bool, /* 判断当天是否有夜盘, 有: day+23:00, 否: 下一交易日的白盘的第一个收盘点 */
    is_check_night_next_day_0100_0230: bool, /* 判断当天是否有夜盘, 有: (day+1)+(1:00|2:30),  否: 下一交易日的白盘的第一个收盘点 */
    is_check_prev_night_0100_0230:     bool, /* 判断前一天是否有夜盘, 有:day+(1:00|2:30), 否下一交易日的白盘的第一个收盘点 */
}

#[derive(Debug, Default)]
pub struct Minutes {
    times_vec:            Vec<(NaiveTime, NaiveTime)>,
    minute_strategy_hmap: HashMap<NaiveTime, Arc<MinuteStrategyInfo>>,
    minute_idx_hmap:      HashMap<NaiveTime, (i16, i16)>,
}

impl Minutes {
    // 无夜盘的品种
    //    白盘收盘后到23:59:59时间点, 下一交易日的白盘的第一个收盘点
    //    00:00~白盘收盘时间点: 当天不是交易日: 下一交易日的白盘的第一个收盘点, 当天是交易日: 当天其所属的收盘点
    // 有夜盘的品种
    //    夜盘时间结束时间: (非交易日都无夜盘, 交易日会有无夜盘的情况, 所以对于夜盘时间点只判断有无夜盘就可以)
    //       23:00收盘: 白盘收盘后到23:00的时间点: 当天是否有夜盘, 有: 当天时间加23:00, 无: 下一交易日的第一个收盘点
    //                 23:00~23:59:59时间点: 下一交易日第一个收盘点.
    //                 00:00~白盘收盘时间点: 当天不是交易日: 下一交易日的白盘的第个收盘点, 当天是交易日: 当天其所属的收盘点
    //       1:00|2:30收盘: 白盘收盘后到23:59:59的时间点: 当天有否有夜盘: 有: 第二天1:00|2:30, 无: 下一交易日的第一个收盘点
    //                      00:00~1:00|2:30: 前一天是否有夜盘: 有: 当天1:00|2:30, 无: 下一交易日的第一个收盘点
    //                      1:00|2:30~白盘收盘点: 当天不是交易日: 下一交易日的白盘的第一个收盘点, 当天是交易日: 当天其所属的收盘点
    pub(super) fn new_from_times_vec(times_vec: &[(NaiveTime, NaiveTime)]) -> Minutes {
        // 10:15~11:30 11:30~15:00 11:30~15:15 15:00~23:00 01:00~10:15 02:30~10:15
        // 15:00~10:15, 15:00~11:30, 15:15~11:30, 23:00~10:15, 15:00~01:00, 15:00~02:30

        let (_, night_close_time) = unsafe { times_vec.get_unchecked(0) };
        let night_close_time = *night_close_time;

        let (_, day_close_time) = times_vec.last().unwrap();
        let day_close_time = *day_close_time;

        let time_2300 = NaiveTime::from_hms_opt(23, 0, 0).unwrap();
        let time_235959 = NaiveTime::from_hms_milli_opt(23, 59, 59, 1999).unwrap();
        let time_0100 = NaiveTime::from_hms_opt(1, 0, 0).unwrap();
        let time_0230 = NaiveTime::from_hms_opt(2, 30, 0).unwrap();

        let is_night_close_2300 = night_close_time == time_2300;
        let is_ngiht_close_0100_0230 =
            night_close_time == time_0100 || night_close_time == time_0230;

        let day = NaiveDate::default();

        let len = times_vec.len();

        let mut strategy_hmap = HashMap::new();
        let mut minute_strategy_hmap = HashMap::new();

        for (idx, (_, close_time)) in times_vec.iter().enumerate() {
            let idx = (idx + 1) % len;
            let (_, next_close_time) = unsafe { times_vec.get_unchecked(idx) };
            let close_time = *close_time;
            let next_close_time = *next_close_time;
            let start_time = day.and_time(close_time);
            let end_time = if close_time > next_close_time {
                day.succ_opt().unwrap()
            } else {
                day
            }
            .and_time(next_close_time);

            let mut dt_time = start_time + Duration::minutes(1);
            while dt_time <= end_time {
                let minute = dt_time.time();
                // 下一交易日的白盘的第一个收盘点
                let mut is_use_next_td_first_close = false;
                // 判断当天是不是交易日, 是: day+close_time, 否: 下一交易日的白盘的第一个收盘点
                let mut is_check_day = false;
                // 判断当天是否有夜盘, 有: day+23:00, 否: 下一交易日的白盘的第一个收盘点
                let mut is_check_night_2300 = false;
                // 判断当天是否有夜盘, 有: (day+1)+(1:00|2:30),  否: 下一交易日的白盘的第一个收盘点
                let mut is_check_night_next_day_0100_0230 = false;
                // 判断前一天是否有夜盘, 有:day+(1:00|2:30), 否下一交易日的白盘的第一个收盘点
                let mut is_check_prev_night_0100_0230 = false;

                if !is_night_close_2300 && !is_ngiht_close_0100_0230 {
                    // 无夜盘品种
                    // 白盘收盘后到23:59:59时间点, 下一交易日的白盘的第一个收盘点
                    // 当天不是交易日: 下一交易日的白盘的第一个收盘点, 当天是交易日: 当天其所属的收盘点
                    if minute > day_close_time && minute < time_235959 {
                        is_use_next_td_first_close = true
                    } else {
                        is_check_day = true
                    }
                } else if is_night_close_2300 {
                    //       23:00收盘: 白盘收盘后到23:00的时间点: 当天是否有夜盘, 有: 当天时间加23:00, 无: 下一交易日的第一个收盘点
                    //                 23:00~23:59:59时间点: 下一交易日第一个收盘点.
                    //                 00:00~白盘收盘时间点: 当天不是交易日: 下一交易日的白盘的第个收盘点, 当天是交易日: 当天其所属的收盘点
                    if minute > day_close_time && minute <= time_2300 {
                        is_check_night_2300 = true;
                    } else if minute > time_2300 && minute < time_235959 {
                        is_use_next_td_first_close = true;
                    } else {
                        is_check_day = true;
                    }
                } else {
                    //       1:00|2:30收盘: 白盘收盘后到23:59:59的时间点: 当天有否有夜盘: 有: 第二天1:00|2:30, 无: 下一交易日的第一个收盘点
                    //                      00:00~1:00|2:30: 前一天是否有夜盘: 有: 当天1:00|2:30, 无: 下一交易日的第一个收盘点
                    //                      1:00|2:30~白盘收盘点: 当天不是交易日: 下一交易日的白盘的第一个收盘点, 当天是交易日: 当天其所属的收盘点
                    if minute > day_close_time && minute < time_235959 {
                        is_check_night_next_day_0100_0230 = true;
                    } else if minute <= night_close_time {
                        is_check_prev_night_0100_0230 = true;
                    } else {
                        is_check_day = true;
                    }
                }

                let key = format!(
                    "{}-{}-{}-{}-{}-{}",
                    next_close_time,
                    is_use_next_td_first_close,
                    is_check_day,
                    is_check_night_2300,
                    is_check_night_next_day_0100_0230,
                    is_check_prev_night_0100_0230
                );

                let minute_strategy = strategy_hmap.entry(key).or_insert_with(|| {
                    Arc::new(MinuteStrategyInfo {
                        close_time: next_close_time,
                        is_use_next_td_first_close,
                        is_check_day,
                        is_check_night_2300,
                        is_check_night_next_day_0100_0230,
                        is_check_prev_night_0100_0230,
                    })
                });

                minute_strategy_hmap.insert(minute, minute_strategy.clone());

                dt_time += Duration::minutes(1);
            }
        }
        let minute_idx_hmap = Minutes::minute_idx_hmap(times_vec);
        Minutes {
            times_vec: times_vec.to_vec(),
            minute_strategy_hmap,
            minute_idx_hmap,
        }
    }

    fn minute_idx_hmap(times_vec: &[(NaiveTime, NaiveTime)]) -> HashMap<NaiveTime, (i16, i16)> {
        let (_, close_time) = unsafe { times_vec.get_unchecked(0) };
        let time_2300 = NaiveTime::from_hms_opt(23, 0, 0).unwrap();
        let time_0100 = NaiveTime::from_hms_opt(1, 0, 0).unwrap();
        let time_0230 = NaiveTime::from_hms_opt(2, 30, 0).unwrap();
        let has_night = vec![time_2300, time_0100, time_0230].contains(close_time);

        let day = NaiveDate::default();

        let mut minute_idx_map = HashMap::new();

        let mut night_idx_offset = 0;

        let mut minute_idx = 0i16;

        for (idx, (open_time, close_time)) in times_vec.iter().enumerate() {
            let open_time = *open_time;
            let close_time = *close_time;

            let mut time = day.and_time(open_time);
            let close_dt = if open_time > close_time {
                day.succ_opt().unwrap().and_time(close_time)
            } else {
                day.and_time(close_time)
            };

            if has_night && idx == 0 {
                night_idx_offset = (close_dt - time).num_minutes() as i16;
            }

            time += Duration::minutes(1);
            while time <= close_dt {
                minute_idx += 1;

                let minute_idx_non_night = if has_night {
                    if idx != 0 {
                        minute_idx - night_idx_offset
                    } else {
                        0
                    }
                } else {
                    minute_idx
                };

                minute_idx_map.insert(time.time(), (minute_idx, minute_idx_non_night));

                time += Duration::minutes(1);
            }
        }

        minute_idx_map
    }

    // time必须为转换后的1m时间
    pub fn minute_idx(&self, time: &NaiveTime, day_has_night: bool) -> i16 {
        let (idx_full, idx_non_night) = self
            .minute_idx_hmap
            .get(time)
            .ok_or_else(|| {
                let times_vec_str = self
                    .times_vec
                    .iter()
                    .map(|v| format!("({},{})", v.0.format("%H:%M:%S"), v.1.format("%H:%M:%S")))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("错误的time:{} [{}]", time, times_vec_str)
            })
            .unwrap();
        if day_has_night {
            *idx_full
        } else {
            *idx_non_night
        }
    }

    pub fn next_close_time(
        &self,
        dt: &NaiveDateTime,
        non_night_first_close: &NaiveTime,
    ) -> NaiveDateTime {
        let time = dt.time();
        let time = NaiveTime::from_hms_opt(time.hour(), time.minute(), 0).unwrap();
        let stragegy = self.minute_strategy_hmap.get(&time).unwrap();
        let day = dt.date();
        let trade_day = trade_day::trade_day(&day);
        if stragegy.is_use_next_td_first_close {
            trade_day.td_next.and_time(*non_night_first_close)
        } else if stragegy.is_check_day {
            if trade_day.is_trade_day {
                day.and_time(stragegy.close_time)
            } else {
                trade_day.td_next.and_time(*non_night_first_close)
            }
        } else if stragegy.is_check_night_2300 {
            if trade_day.has_night {
                day.and_time(stragegy.close_time)
            } else {
                trade_day.td_next.and_time(*non_night_first_close)
            }
        } else if stragegy.is_check_night_next_day_0100_0230 {
            if trade_day.has_night {
                day.succ_opt().unwrap().and_time(stragegy.close_time)
            } else {
                trade_day.td_next.and_time(*non_night_first_close)
            }
        } else if stragegy.is_check_prev_night_0100_0230 {
            let prev_day = day.pred_opt().unwrap();
            let prev_trade_day = trade_day::trade_day(&prev_day);
            if prev_trade_day.has_night {
                day.and_time(stragegy.close_time)
            } else {
                trade_day.td_next.and_time(*non_night_first_close)
            }
        } else {
            NaiveDateTime::default()
        }
    }
}

#[cfg(test)]
mod test {
    use chrono::NaiveDate;

    use super::Minutes;
    use crate::hq::future::time_range::{init_from_db, time_range_by_breed};
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    async fn print_new_from_time_range(breed: &str) {
        init_test_mysql_pools();
        init_from_db(MySqlPools::pool()).await.unwrap();
        let time_range = time_range_by_breed(breed).unwrap();
        Minutes::new_from_times_vec(&time_range.times_vec);
    }

    #[tokio::test]
    async fn test_new_from_time_range_lr() {
        print_new_from_time_range("ag").await;
    }

    async fn print_minute_idx_map(breed: &str, day: &NaiveDate) {
        init_test_mysql_pools();
        init_from_db(MySqlPools::pool()).await.unwrap();
        let time_range = time_range_by_breed(breed).unwrap();
        let minute_idx_map = Minutes::minute_idx_hmap(&time_range.times_vec);

        let (minutes, _) = time_range.day_minutes(day);
        for minute in minutes {
            let (idx, idx2) = minute_idx_map.get(&minute.time()).unwrap();
            println!("{} {} {}", minute, idx, idx2);
        }
    }

    #[tokio::test]
    async fn test_print_minute_idx_lr() {
        let day = NaiveDate::from_ymd_opt(2023, 7, 6).unwrap();
        print_minute_idx_map("LR", &day).await;
    }

    #[tokio::test]
    async fn test_print_minute_idx_ic() {
        let day = NaiveDate::from_ymd_opt(2023, 7, 6).unwrap();
        print_minute_idx_map("IC", &day).await;
    }

    #[tokio::test]
    async fn test_print_minute_idx_tf() {
        let day = NaiveDate::from_ymd_opt(2023, 7, 6).unwrap();
        print_minute_idx_map("TF", &day).await;
    }

    #[tokio::test]
    async fn test_print_minute_idx_sa() {
        let day = NaiveDate::from_ymd_opt(2023, 7, 6).unwrap();
        print_minute_idx_map("SA", &day).await;
    }

    #[tokio::test]
    async fn test_print_minute_idx_zn() {
        let day = NaiveDate::from_ymd_opt(2023, 7, 6).unwrap();
        print_minute_idx_map("zn", &day).await;
    }

    #[tokio::test]
    async fn test_print_minute_idx_ag() {
        let day = NaiveDate::from_ymd_opt(2023, 7, 6).unwrap();
        print_minute_idx_map("ag", &day).await;
    }
}
