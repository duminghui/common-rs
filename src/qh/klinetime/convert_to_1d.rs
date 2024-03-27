use std::sync::{Arc, OnceLock};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

use super::tx_time_range::TxTimeRangeData;
use super::{KLineTimeError, TimeRangeDateTime};
use crate::qh::trading_day::TradingDayUtil;
use crate::ymdhms::{Hms, Ymd};

// TODO: NOT INIT
static CONVERT_1D: OnceLock<Arc<ConvertTo1d>> = OnceLock::new();

pub(crate) struct ConvertTo1d {
    trd: Arc<TxTimeRangeData>,
}

impl Default for ConvertTo1d {
    fn default() -> Self {
        Self {
            trd: TxTimeRangeData::current(),
        }
    }
}

// TxTimeRangeData::init
// TradingDayUtil::init
impl ConvertTo1d {
    pub(crate) fn current() -> Arc<Self> {
        CONVERT_1D.get().unwrap().clone()
    }

    pub(crate) fn time_range(
        &self,
        breed: &str,
        datetime: &NaiveDateTime,
    ) -> Result<TimeRangeDateTime, KLineTimeError> {
        let tx_time_range_vec = self.trd.time_range_vec(breed)?;

        let first_time_range_hms = tx_time_range_vec.first().unwrap();
        let stime = NaiveTime::from(&first_time_range_hms.start);
        let shhmmss = first_time_range_hms.start.hhmmss;

        let last_time_range_hms = tx_time_range_vec.last().unwrap();
        let etime = NaiveTime::from(&last_time_range_hms.end);

        let yyyymmdd = Ymd::from(&datetime.date()).yyyymmdd;
        let hhmmss = Hms::from(&datetime.time()).hhmmss;

        let tdu = TradingDayUtil::current();

        // 默认的时间为无夜盘的算法, K线的日期加收盘时间,
        let mut sdatetime = datetime.date().and_time(stime);
        let mut edatetime = datetime.date().and_time(etime);

        if shhmmss == 210100 {
            // 有夜盘的情况 先不合并处理了, 方便以后添加新情况时进行处理.
            if (210100..=235959).contains(&hhmmss) {
                // 夜盘 0点之前 取下一交易日
                let next_td = tdu.next(&yyyymmdd)?;
                edatetime = NaiveDate::from(next_td).and_time(etime);
            } else if hhmmss <= 23000 {
                // 夜盘 0点之后
                let prev_td = tdu.prev(&yyyymmdd)?;
                sdatetime = NaiveDate::from(prev_td).and_time(stime);
                if !tdu.is_td(&yyyymmdd) {
                    let next_td = tdu.next(&yyyymmdd)?;
                    edatetime = NaiveDate::from(next_td).and_time(etime);
                }
            } else if (90100..=last_time_range_hms.end.hhmmss).contains(&hhmmss) {
                let prev_td = tdu.prev(&yyyymmdd)?;
                sdatetime = NaiveDate::from(prev_td).and_time(stime);
            }
        }

        Ok(TimeRangeDateTime::new(sdatetime, edatetime))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, NaiveDate, NaiveTime, Timelike};

    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;
    use crate::qh::klinetime::convert_to_1d::ConvertTo1d;
    use crate::qh::klinetime::tx_time_range::TxTimeRangeData;
    use crate::qh::trading_day::TradingDayUtil;
    use crate::ymdhms::Ymd;

    fn test_to_1d_sub(breed: &str, tx_ranges: &str, yyyymmdd: u32, rdatetime_str: &str) {
        println!("=== {} {}===", breed, tx_ranges);

        let trd = TxTimeRangeData::current();
        let tx_range_fix_vec = trd.time_range_fix_vec(breed).unwrap();
        let date = NaiveDate::from(&Ymd::from_yyyymmdd(yyyymmdd));
        let next_date = date.succ_opt().unwrap();
        let next_td = NaiveDate::from(TradingDayUtil::current().next(&yyyymmdd).unwrap());

        for st_hms in tx_range_fix_vec {
            let mut sdatetime = date.and_time(NaiveTime::from(&st_hms.start));
            let edatetime = date.and_time(NaiveTime::from(&st_hms.end));
            while sdatetime <= edatetime {
                let time = sdatetime.time();
                let datetime = if (0..=3).contains(&time.hour()) {
                    next_date.and_time(time)
                } else if time.hour() < 21 {
                    next_td.and_time(time)
                } else {
                    sdatetime
                };
                let d1_datetime = ConvertTo1d::current().time_range(breed, &datetime).unwrap();
                println!("{}: {}", datetime, d1_datetime);
                assert_eq!(d1_datetime.to_string(), rdatetime_str);
                sdatetime += Duration::try_minutes(1).unwrap();
            }
        }

        println!();
    }

    #[tokio::test]
    async fn test_to_1d() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();

        let yyyymmdd = 20220617;

        let breed = "IC";
        let tx_ranges = "[(931,1130),(1301,1500)]";
        test_to_1d_sub(
            breed,
            tx_ranges,
            yyyymmdd,
            "(2022-06-20 09:31:00~2022-06-20 15:00:00)",
        );

        let breed = "TF";
        let tx_ranges = "[(931,1130),(1301,1515)]";
        test_to_1d_sub(
            breed,
            tx_ranges,
            yyyymmdd,
            "(2022-06-20 09:31:00~2022-06-20 15:15:00)",
        );

        let breed = "AP";
        let tx_ranges = "[(901,1015),(1031,1130),(1331,1500)]";
        test_to_1d_sub(
            breed,
            tx_ranges,
            yyyymmdd,
            "(2022-06-20 09:01:00~2022-06-20 15:00:00)",
        );

        let breed = "a";
        let tx_ranges = "[(2101,2300),(901,1015),(1031,1130),(1331,1500)]";
        test_to_1d_sub(
            breed,
            tx_ranges,
            yyyymmdd,
            "(2022-06-17 21:01:00~2022-06-20 15:00:00)",
        );

        let breed = "ag";
        let tx_ranges = "[(2101,230),(901,1015),(1031,1130),(1331,1500)]";
        test_to_1d_sub(
            breed,
            tx_ranges,
            yyyymmdd,
            "(2022-06-17 21:01:00~2022-06-20 15:00:00)",
        );

        let breed = "al";
        let tx_ranges = "[(2101,100),(901,1015),(1031,1130),(1331,1500)]";
        test_to_1d_sub(
            breed,
            tx_ranges,
            yyyymmdd,
            "(2022-06-17 21:01:00~2022-06-20 15:00:00)",
        );
    }
}
