use chrono::{Duration, NaiveDateTime, Timelike};

use super::TimeRangeDateTime;
use crate::qh::period::PeriodUtil;

pub(crate) struct ConvertTo3m5m15m;

impl ConvertTo3m5m15m {
    /// time: 必须是经过日夜盘时间修正后的时间.
    pub(crate) fn time_range(period: &str, time: &NaiveDateTime) -> TimeRangeDateTime {
        let pv = PeriodUtil::pv(period)
            .unwrap_or_else(|| panic!("Convert3m5m15m period err: {}", period));
        let time_offset = time.minute() as u16 % pv;
        let stime_offset;
        let etime_offset;

        if time_offset == 0 {
            stime_offset = pv - 1;
            etime_offset = time_offset;
        } else {
            stime_offset = time_offset - 1;
            etime_offset = pv - time_offset;
        }
        TimeRangeDateTime::new(
            *time - Duration::minutes(stime_offset as i64),
            *time + Duration::minutes(etime_offset as i64),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{Duration, NaiveDate, NaiveTime, Timelike};

    use super::ConvertTo3m5m15m;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;
    use crate::qh::klinetime::tx_time_range::TxTimeRangeData;
    use crate::qh::period::PeriodUtil;
    use crate::qh::trading_day::TradingDayUtil;

    fn test_to_xm_sub(breed: &str, tx_ranges: &str, period: &str) {
        println!("=== {} {} {} ===", breed, period, tx_ranges);
        let trd = TxTimeRangeData::current();
        let tx_range_fix_vec = trd.time_range_fix_vec(breed).unwrap();
        let date = NaiveDate::from_ymd_opt(2022, 6, 17).unwrap();
        let next_date = date + Duration::days(1);
        let next_td = NaiveDate::from(TradingDayUtil::current().next(&20220617).unwrap());

        let mut key_vec = vec![];
        let mut xm_vec_map = HashMap::<String, Vec<_>>::new();

        for st_hms in tx_range_fix_vec {
            let mut sdatetime = date.and_time(NaiveTime::from(&st_hms.start));
            let edatetime = date.and_time(NaiveTime::from(&st_hms.end));
            while sdatetime <= edatetime {
                let time = sdatetime.time();
                let datetime = if (0..=3).contains(&time.hour()) {
                    next_date.and_time(sdatetime.time())
                } else if time.hour() < 21 {
                    next_td.and_time(sdatetime.time())
                } else {
                    sdatetime
                };
                let tr_dt = ConvertTo3m5m15m::time_range(period, &datetime);
                let key = tr_dt.to_string();
                if !xm_vec_map.contains_key(&key) {
                    key_vec.push(key.clone());
                }
                let xm_vec = xm_vec_map.entry(key).or_default();
                xm_vec.push(datetime);
                sdatetime += Duration::minutes(1);
            }
        }

        let pv = PeriodUtil::pv(period).unwrap();
        for key in key_vec.iter() {
            let datetime_vec = xm_vec_map.get(key).unwrap();
            println!(
                "# {}: {:?}",
                key,
                datetime_vec
                    .iter()
                    .map(|v| { v.format("%Y-%m-%d %H:%M:%S").to_string() })
                    .collect::<Vec<String>>()
            );
            assert_eq!(datetime_vec.len(), *pv as usize);
        }
        println!();
    }

    #[tokio::test]
    async fn test_to_xm_1() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let breed = "IC";
        let tx_ranges = "[(931,1130),(1301,1500)]";
        test_to_xm_sub(breed, tx_ranges, "3m");
        test_to_xm_sub(breed, tx_ranges, "5m");
        test_to_xm_sub(breed, tx_ranges, "15m");
    }

    #[tokio::test]
    async fn test_to_xm_2() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let breed = "TF";
        let tx_ranges = "[(931,1130),(1301,1515)]";
        test_to_xm_sub(breed, tx_ranges, "3m");
        test_to_xm_sub(breed, tx_ranges, "5m");
        test_to_xm_sub(breed, tx_ranges, "15m");
    }

    #[tokio::test]
    async fn test_to_xm_3() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let breed = "AP";
        let tx_ranges = "[(901,1015),(1031,1130),(1331,1500)]";
        test_to_xm_sub(breed, tx_ranges, "3m");
        test_to_xm_sub(breed, tx_ranges, "5m");
        test_to_xm_sub(breed, tx_ranges, "15m");
    }

    #[tokio::test]
    async fn test_to_xm_4() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let breed = "a";
        let tx_ranges = "[(2101,2300),(901,1015),(1031,1130),(1331,1500)]";
        test_to_xm_sub(breed, tx_ranges, "3m");
        test_to_xm_sub(breed, tx_ranges, "5m");
        test_to_xm_sub(breed, tx_ranges, "15m");
    }

    #[tokio::test]
    async fn test_to_xm_5() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let breed = "al";
        let tx_ranges = "[(2101,100),(901,1015),(1031,1130),(1331,1500)]";
        test_to_xm_sub(breed, tx_ranges, "3m");
        test_to_xm_sub(breed, tx_ranges, "5m");
        test_to_xm_sub(breed, tx_ranges, "15m");
    }

    #[tokio::test]
    async fn test_to_xm_6() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let breed = "ag";
        let tx_ranges = "[(2101,230),(901,1015),(1031,1130),(1331,1500)]";
        test_to_xm_sub(breed, tx_ranges, "3m");
        test_to_xm_sub(breed, tx_ranges, "5m");
        test_to_xm_sub(breed, tx_ranges, "15m");
    }
}
