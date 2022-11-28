use std::ops::Sub;
use std::sync::{Arc, RwLock};

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime};
use lazy_static::lazy_static;

use super::tx_time_range::TxTimeRangeData;
use super::{KLineTimeError, TimeRangeDateTime};
use crate::qh::trading_day::TradingDayUtil;
use crate::ymdhms::Ymd;

lazy_static! {
    static ref CONVERT_1MTH: RwLock<Arc<ConvertTo1Month>> = RwLock::new(Default::default());
}

pub(crate) struct ConvertTo1Month {
    trd: Arc<TxTimeRangeData>,
    tdu: Arc<TradingDayUtil>,
}

impl Default for ConvertTo1Month {
    fn default() -> Self {
        Self {
            trd: TxTimeRangeData::current(),
            tdu: TradingDayUtil::current(),
        }
    }
}

// TxTimeRangeData::init
// TradingDayUtil::init
impl ConvertTo1Month {
    pub(crate) fn current() -> Arc<Self> {
        CONVERT_1MTH.read().unwrap().clone()
    }

    pub fn time_range(
        &self,
        breed: &str,
        datetime: &NaiveDateTime,
    ) -> Result<TimeRangeDateTime, KLineTimeError> {
        let date = datetime.date();
        let year = date.year();
        let month = date.month();
        let days = days_in_month(year, month);

        let trd = &self.trd;
        let trh_vec = trd.time_range_vec(breed)?;
        let start_time = NaiveTime::from(&trh_vec.first().unwrap().start);
        let end_time = NaiveTime::from(&trh_vec.last().unwrap().end);

        let mut edate = NaiveDate::from_ymd(year, month, days);
        let tdu = &self.tdu;
        let eyyyymmdd = Ymd::from(&edate).yyyymmdd;
        if !tdu.is_td(&eyyyymmdd) {
            edate = NaiveDate::from(tdu.prev(&eyyyymmdd)?);
        }

        let mut sdate = NaiveDate::from_ymd(year, month, 1);

        let edatetime = edate.and_time(end_time);

        if !trd.is_had_night(breed) {
            // 无夜盘的品种所属的交易时间的范围
            Ok(TimeRangeDateTime::new(
                sdate.and_time(start_time),
                edatetime,
            ))
        } else {
            if datetime > &edatetime {
                // 超过本月的交易范围属于下一个月的.
                let (year, month) = if month == 12 {
                    (year + 1, 1)
                } else {
                    (year, month + 1)
                };
                let days = days_in_month(year, month);
                sdate = NaiveDate::from_ymd(year, month, 1);

                edate = NaiveDate::from_ymd(year, month, days);

                let eyyyymmdd = Ymd::from(&edate).yyyymmdd;
                if !tdu.is_td(&eyyyymmdd) {
                    edate = NaiveDate::from(tdu.prev(&eyyyymmdd)?);
                }
            }
            // 有夜盘的开始时间取上一交易日
            sdate = NaiveDate::from(tdu.prev(&Ymd::from(&sdate).yyyymmdd)?);

            Ok(TimeRangeDateTime::new(
                sdate.and_time(start_time),
                edate.and_time(end_time),
            ))
        }
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    if month == 12 {
        NaiveDate::from_ymd(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd(year, month + 1, 1)
    }
    .sub(NaiveDate::from_ymd(year, month, 1))
    .num_days() as u32
}

#[cfg(test)]
mod tests {
    use std::ops::Sub;

    use chrono::{Duration, NaiveDate, NaiveTime};

    use super::ConvertTo1Month;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;
    use crate::qh::klinetime::tx_time_range::TxTimeRangeData;
    use crate::qh::trading_day::TradingDayUtil;

    fn test_time_range_sub(breed: &str, tx_ranges: &str) {
        println!("=== {} {}===", breed, tx_ranges);
        let mut sdate = NaiveDate::from_ymd(2019, 12, 31);
        let edate = NaiveDate::from_ymd(2020, 12, 30);
        let trd = TxTimeRangeData::current();
        let trh_vec = trd.time_range_vec(breed).unwrap();
        let start_time = NaiveTime::from(&trh_vec.first().unwrap().start);
        let end_time = NaiveTime::from(&trh_vec.last().unwrap().end);
        let is_had_night = trd.is_had_night(breed);
        while sdate < edate {
            let datetime1 = if is_had_night {
                sdate.and_time(start_time) - Duration::days(1)
            } else {
                sdate.and_time(start_time)
            };
            let kltr1 = ConvertTo1Month::current().time_range(breed, &datetime1);
            if let Ok(kltr1) = kltr1 {
                if kltr1.start == datetime1 {
                    println!("---------- s");
                }
                println!("{} {}", datetime1, kltr1);
            } else {
                println!("{} None", datetime1);
            }
            let datetime2 = sdate.and_time(end_time);
            let kltr2 = ConvertTo1Month::current().time_range(breed, &datetime2);
            if let Ok(kltr2) = kltr2 {
                println!("{} {}", datetime2, kltr2);
                if kltr2.end == datetime2 {
                    println!("---------- e");
                }
            } else {
                println!("{} None", datetime2);
            }

            sdate += Duration::days(1);
        }
    }

    #[tokio::test]
    async fn test_time_range_1() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let breed = "IC";
        let tx_ranges = "[(931,1130),(1301,1500)]";
        test_time_range_sub(breed, tx_ranges);
    }

    #[tokio::test]
    async fn test_time_range_2() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let breed = "TF";
        let tx_ranges = "[(931,1130),(1301,1515)]";
        test_time_range_sub(breed, tx_ranges);
    }

    #[tokio::test]
    async fn test_time_range_3() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let breed = "AP";
        let tx_ranges = "[(901,1015),(1031,1130),(1331,1500)]";
        test_time_range_sub(breed, tx_ranges);
    }

    #[tokio::test]
    async fn test_time_range_4() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let breed = "a";
        let tx_ranges = "[(2101,2300),(901,1015),(1031,1130),(1331,1500)]";
        test_time_range_sub(breed, tx_ranges);
    }

    #[tokio::test]
    async fn test_time_range_5() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let breed = "ag";
        let tx_ranges = "[(2101,230),(901,1015),(1031,1130),(1331,1500)]";
        test_time_range_sub(breed, tx_ranges);
    }

    #[tokio::test]
    async fn test_time_range_6() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let breed = "al";
        let tx_ranges = "[(2101,100),(901,1015),(1031,1130),(1331,1500)]";
        test_time_range_sub(breed, tx_ranges);
    }

    #[test]
    fn test_chrono_1() {
        let year = 2020;
        for (m, d) in (1..=12).map(|m| {
            (
                m,
                if m == 12 {
                    NaiveDate::from_ymd(year + 1, 1, 1)
                } else {
                    NaiveDate::from_ymd(year, m + 1, 1)
                }
                .signed_duration_since(NaiveDate::from_ymd(year, m, 1))
                .num_days(),
            )
        }) {
            println!("days {} in month {}", d, m);
        }
    }

    #[test]
    fn test_chrono_2() {
        let year = 2020;
        let stime = NaiveDate::from_ymd(year, 2, 1);
        let etime = NaiveDate::from_ymd(year, 3, 1);
        let diff = etime.sub(stime);
        println!("{} {}", diff, diff.num_days());
    }
}
