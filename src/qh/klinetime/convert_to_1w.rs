use std::sync::{Arc, OnceLock};

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Weekday};

use super::tx_time_range::TxTimeRangeData;
use super::{KLineTimeError, TimeRangeDateTime};
use crate::qh::trading_day::TradingDayUtil;
use crate::ymdhms::{Hms, Ymd};

// TODO: NOT INIT
static CONVERT_1W: OnceLock<Arc<ConvertTo1W>> = OnceLock::new();

// 后面是否需要重构成将所有的交易日存到内存中, 以加快计算速度?
pub(crate) struct ConvertTo1W {
    trd: Arc<TxTimeRangeData>,
    tdu: Arc<TradingDayUtil>,
}

impl Default for ConvertTo1W {
    fn default() -> Self {
        Self {
            trd: TxTimeRangeData::current(),
            tdu: TradingDayUtil::current(),
        }
    }
}

// TxTimeRangeData::init
// TradingDayUtil::init
impl ConvertTo1W {
    pub(crate) fn current() -> Arc<Self> {
        CONVERT_1W.get().unwrap().clone()
    }

    /// 先计算一周的结束日期为本周五, 再计算出开始日期: 如果有夜盘, 则为上周五, 如果无夜盘, 则为周一.
    /// 如果结束日是非交易日, 取上一次交易日, 如果交易日不在本周范围内, 返回错误.
    /// 如果开始日是非交易日, 则取下一次交易日, 如果交易日不在本周范围内, 返回错误.
    /// 没有做假期前的夜盘时间的判断, 只要不传入该类的时间就不会影响数据
    pub(crate) fn time_range(
        &self,
        breed: &str,
        datetime: &NaiveDateTime,
    ) -> Result<TimeRangeDateTime, KLineTimeError> {
        let hhmmss = Hms::from(&datetime.time()).hhmmss;
        let date = datetime.date();
        let weekday = date.weekday();
        let number_from_monday = weekday.number_from_monday();
        let mut end_date = match weekday {
            Weekday::Fri if hhmmss > 210000 => date + Duration::try_days(7).unwrap(),
            Weekday::Sat | Weekday::Sun => {
                date + Duration::try_days(12 - number_from_monday as i64).unwrap()
            },
            _ => date + Duration::try_days(5 - number_from_monday as i64).unwrap(),
        };
        let trd = &self.trd;
        let start_date = if trd.is_had_night(breed) {
            end_date - Duration::try_days(7).unwrap()
        } else {
            end_date - Duration::try_days(4).unwrap()
        };
        let tdu = &self.tdu;
        // 暂时不做开始时间的判断, 在时间显示上只是显示结束的日期, 如果判断开始日期, 开始时期也会变更, 没必要.
        // 另外, 交易日数据库的开始日期也是固定的, 也会引起错误.
        // let syyyymmdd = Ymd::from(start_date).yyyymmdd;
        // if !tdu.is_td(&syyyymmdd) {
        //     start_date = NaiveDate::from(*tdu.next(&syyyymmdd)?);
        //     if start_date > end_date {
        //         return Err(KLineTimeError::WeekNotHadTxDay(*datetime));
        //     }
        // }
        let eyyyymmdd = Ymd::from(&end_date).yyyymmdd;
        if !tdu.is_td(&eyyyymmdd) {
            end_date = NaiveDate::from(tdu.prev(&eyyyymmdd)?);
            if end_date < start_date {
                return Err(KLineTimeError::WeekNotHadTxDay(*datetime));
            }
        }
        let trh_vec = trd.time_range_vec(breed)?;
        let start_time = NaiveTime::from(&trh_vec.first().unwrap().start);
        let end_time = NaiveTime::from(&trh_vec.last().unwrap().end);
        let sdatetime = start_date.and_time(start_time);
        let edatetime = end_date.and_time(end_time);
        if sdatetime > edatetime {
            return Err(KLineTimeError::WeekNotHadTxDay(*datetime));
        }

        Ok(TimeRangeDateTime::new(sdatetime, edatetime))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, Duration, NaiveDate, NaiveTime, Weekday};

    use super::ConvertTo1W;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;
    use crate::qh::klinetime::tx_time_range::TxTimeRangeData;
    use crate::qh::trading_day::TradingDayUtil;
    use crate::ymdhms::Hms;

    #[test]
    fn test_chrono() {
        let date = NaiveDate::from_ymd_opt(2022, 6, 13).unwrap();
        let time = NaiveTime::from_hms_opt(1, 1, 0).unwrap();
        let datetime = date.and_time(time);
        let hhmmss = Hms::from(&time).hhmmss;
        for i in 0..9 {
            let datetime = datetime + Duration::try_days(i).unwrap();
            let weekday = datetime.weekday();
            let number_from_monday = weekday.number_from_monday();
            let end_date = match weekday {
                Weekday::Fri if hhmmss > 210000 => datetime + Duration::try_days(7).unwrap(),
                Weekday::Sat | Weekday::Sun => {
                    datetime + Duration::try_days(12 - number_from_monday as i64).unwrap()
                },
                _ => datetime + Duration::try_days(5 - number_from_monday as i64).unwrap(),
            };
            let start_date1 = end_date - Duration::try_days(7).unwrap();
            let start_date2 = end_date - Duration::try_days(4).unwrap();
            println!(
                "{}({}) {}({}) {}({}) {}({})",
                datetime,
                datetime.weekday(),
                end_date,
                end_date.weekday(),
                start_date1,
                start_date1.weekday(),
                start_date2,
                start_date2.weekday(),
            );
        }
    }

    fn test_time_range_sub(breed: &str, tx_ranges: &str) {
        println!("=== {} {}===", breed, tx_ranges);
        let mut sdate = NaiveDate::from_ymd_opt(2021, 12, 31).unwrap();
        let edate = NaiveDate::from_ymd_opt(2022, 12, 30).unwrap();
        let trd = TxTimeRangeData::current();
        let trh_vec = trd.time_range_vec(breed).unwrap();
        let start_time = NaiveTime::from(&trh_vec.first().unwrap().start);
        let end_time = NaiveTime::from(&trh_vec.last().unwrap().end);
        let is_had_night = trd.is_had_night(breed);
        while sdate < edate {
            let datetime1 = if is_had_night {
                sdate.pred_opt().unwrap().and_time(start_time)
            } else {
                sdate.and_time(start_time)
            };
            let kldt1 = ConvertTo1W::current().time_range(breed, &datetime1);
            if let Ok(kldt1) = kldt1 {
                if kldt1.start == datetime1 {
                    println!("----------");
                }
                println!("1: {}({}) {}", datetime1, datetime1.weekday(), kldt1);
            } else {
                println!("1: {}({}) Out Tx Range", datetime1, datetime1.weekday());
            }

            let datetime2 = sdate.and_time(end_time);
            let kldt2 = ConvertTo1W::current().time_range(breed, &datetime2);
            if let Ok(kldt2) = kldt2 {
                println!("2: {}({}) {}", datetime2, datetime2.weekday(), kldt2);
                if kldt2.end == datetime2 {
                    println!("----------");
                }
            } else {
                println!("2: {}({}) Out Tx Range", datetime2, datetime2.weekday());
            }

            sdate = sdate.succ_opt().unwrap()
        }
    }

    #[tokio::test]
    async fn test_time_range_1() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();

        let breed = "IC";
        let tx_ranges = "[(931,1130),(1301,1500)]";
        test_time_range_sub(breed, tx_ranges);
    }

    #[tokio::test]
    async fn test_time_range_2() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();

        let breed = "TF";
        let tx_ranges = "[(931,1130),(1301,1515)]";
        test_time_range_sub(breed, tx_ranges);
    }

    #[tokio::test]
    async fn test_time_range_3() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();

        let breed = "AP";
        let tx_ranges = "[(901,1015),(1031,1130),(1331,1500)]";
        test_time_range_sub(breed, tx_ranges);
    }

    #[tokio::test]
    async fn test_time_range_4() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();

        let breed = "a";
        let tx_ranges = "[(2101,2300),(901,1015),(1031,1130),(1331,1500)]";
        test_time_range_sub(breed, tx_ranges);
    }

    #[tokio::test]
    async fn test_time_range_5() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();

        let breed = "ag";
        let tx_ranges = "[(2101,230),(901,1015),(1031,1130),(1331,1500)]";
        test_time_range_sub(breed, tx_ranges);
    }

    #[tokio::test]
    async fn test_time_range_6() {
        init_test_mysql_pools();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();

        let breed = "al";
        let tx_ranges = "[(2101,100),(901,1015),(1031,1130),(1331,1500)]";
        test_time_range_sub(breed, tx_ranges);
    }
}
