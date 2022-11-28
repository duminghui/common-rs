//! 从Tick拿到的时间生成1m时间.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use lazy_static::lazy_static;
use tracing::error;

use super::tx_time_range::TxTimeRangeData;
use super::KLineTimeError;
use crate::qh::breed::{BreedInfo, BreedInfoVec};
use crate::qh::trading_day::TradingDayUtil;
use crate::ymdhms::{Hms, Ymd};

lazy_static! {
    static ref CONVERT_1M: RwLock<Arc<ConvertTo1m>> = RwLock::new(Default::default());
}

/// Tick时间转成1m时间
pub(crate) struct ConvertTo1m {
    trd:               Arc<TxTimeRangeData>,
    tdu:               Arc<TradingDayUtil>,
    /// breed 几个特殊时间点对应的hhmmss
    breed_1mtime_hmap: HashMap<String, HashMap<u16, Hms>>,
}

impl Default for ConvertTo1m {
    fn default() -> Self {
        Self {
            trd:               TxTimeRangeData::current(),
            tdu:               TradingDayUtil::current(),
            breed_1mtime_hmap: Default::default(),
        }
    }
}

pub type KLineDateTime = NaiveDateTime;
pub type TickDateTime = NaiveDateTime;

impl ConvertTo1m {
    pub fn current() -> Arc<ConvertTo1m> {
        CONVERT_1M.read().unwrap().clone()
    }

    // BreedVec::init
    // TxTimeRangeData::init
    pub fn init() -> Result<(), KLineTimeError> {
        if !Self::current().is_empty() {
            return Ok(());
        }
        let mut tc = ConvertTo1m::default();
        tc.init_for_breed_vec()?;
        *CONVERT_1M.write().unwrap() = Arc::new(tc);
        Ok(())
    }

    fn init_for_breed_vec(&mut self) -> Result<(), KLineTimeError> {
        let breed_vec = BreedInfoVec::current();
        if breed_vec.is_empty() {
            return Err(KLineTimeError::BreedVecEmpty);
        }
        let trd = &self.trd;
        if trd.is_empty() {
            return Err(KLineTimeError::TxTimeRangeDataEmpty);
        }

        for BreedInfo { breed, .. } in breed_vec.vec() {
            let mut time_hmap = HashMap::new();
            let tx_time_range_vec = trd.time_range_vec(breed);
            if let Err(err) = tx_time_range_vec {
                error!("{} Convert1m init err: {}", breed, err);
                continue;
            }
            let tx_time_range_vec = tx_time_range_vec.unwrap();
            for (idx, tr) in tx_time_range_vec.iter().enumerate() {
                if idx == 0 {
                    match tr.start.hhmmss {
                        90100 => {
                            time_hmap.insert(859u16, Hms::from_hhmmss(90100));
                        },
                        93100 => {
                            time_hmap.insert(929u16, Hms::from_hhmmss(93100));
                        },
                        210100 => {
                            time_hmap.insert(2059u16, Hms::from_hhmmss(210100));
                        },
                        start => panic!("error start hhmmss: {:?}", start),
                    }
                }
                time_hmap.insert(tr.end.hhmm, tr.end);
            }
            // println!("{}: {:?}", breed, time_hmap);
            self.breed_1mtime_hmap.insert(breed.to_owned(), time_hmap);
        }

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.breed_1mtime_hmap.is_empty()
    }

    pub fn to_1m_with_min_dg_day(
        &self,
        breed: &str,
        min_dg_day: u32,
        time: &impl Timelike,
    ) -> Result<(KLineDateTime, TickDateTime), KLineTimeError> {
        let mut date = NaiveDate::from(&Ymd::from_yyyymmdd(min_dg_day));
        let hour = time.hour();
        let min = time.minute();
        let sec = time.second();

        if hour < 3 {
            date += Duration::days(1);
        }
        let kl_datetime = self.to_1m(breed, &date, hour as u8, min as u8, sec as u8);
        kl_datetime.and_then(|v| Ok((v, date.and_hms_nano(hour, min, sec, time.nanosecond()))))
    }

    pub fn to_1m_with_trading_day(
        &self,
        breed: &str,
        trading_day: u32,
        time: &impl Timelike,
    ) -> Result<(KLineDateTime, TickDateTime), KLineTimeError> {
        let tdu = &self.tdu;
        let hour = time.hour();
        let min = time.minute();
        let sec = time.second();

        let hhmm = hour as u16 * 100 + min as u16;
        let date = match hhmm {
            hhmm if hhmm >= 2058 => NaiveDate::from(tdu.prev(&trading_day)?),
            hhmm if hhmm < 300 => NaiveDate::from(tdu.prev(&trading_day)?) + Duration::days(1),
            _ => NaiveDate::from(&Ymd::from_yyyymmdd(trading_day)),
        };

        let kl_datetime = self.to_1m(breed, &date, hour as u8, min as u8, sec as u8);
        kl_datetime.and_then(|v| Ok((v, date.and_hms_nano(hour, min, sec, time.nanosecond()))))
    }

    /// Tick时间转成1m时间
    /// 特殊时间点
    /// 1. 开盘的前一分钟及第一分钟是属于开盘的时间, 如20:59:xx~21:00:59的K线时间为 21:01:00
    /// 2. 每个交易段的最后时间是属于该段结束时间,  如11:30:00K线时间为11:30:00
    /// 3. 00:00:00时间是属于00:00:00, 而不是 00:01:00
    /// #. 第1,2点已经缓存到HashMap中
    /// 其他时间
    /// hh:mm:00~xx:mm:59的数据属于hh:(mm+1):00的K线数据
    /// min_dg_day: 如果有夜盘,开始时为前一交易日,白盘的时候变为当天的交易日, 如果无夜盘则为当天的交易日
    /// 暂时没有判断min_dg_day是否为交易日.
    /// date为转换后的自然日
    fn to_1m(
        &self,
        breed: &str,
        date: &NaiveDate,
        hour: u8,
        min: u8,
        sec: u8,
    ) -> Result<NaiveDateTime, KLineTimeError> {
        let hms = Hms::from_hms(hour, min, sec);
        if hms.hhmmss == 0 {
            return Ok(date.and_hms(0, 0, 0));
        }
        let datetime = self
            .breed_1mtime_hmap
            .get(breed)
            .ok_or(KLineTimeError::BreedNotExist {
                breed: breed.to_owned(),
                scope: "Convert1m".to_owned(),
            })?
            .get(&hms.hhmm)
            .map_or_else(
                || {
                    date.and_time(NaiveTime::from_hms(hour as u32, min as u32, 0))
                        + Duration::minutes(1)
                },
                |v| date.and_time(NaiveTime::from(v)),
            );
        if !self.trd.is_trading_time(breed, &datetime) {
            return Err(KLineTimeError::DatetimeNotInRange {
                breed: breed.to_owned(),
                datetime,
            });
        }
        Ok(datetime)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};

    use super::ConvertTo1m;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;
    use crate::qh::breed::{BreedInfo, BreedInfoVec};
    use crate::qh::klinetime::tx_time_range::TxTimeRangeData;
    use crate::qh::trading_day::TradingDayUtil;
    use crate::ymdhms::Ymd;

    #[derive(Debug, PartialEq)]
    enum DayType {
        MinDgDay,
        TradingDay,
    }

    fn test_to_time_1m_sub(
        day_type: DayType,
        breed: &str,
        tx_ranges: &str,
        results: &[(&str, &str)],
    ) {
        println!("=== {} {} {:?} ===", breed, tx_ranges, day_type);
        let format = "%Y-%m-%d %H:%M:%S";

        let t1mcvt = ConvertTo1m::current();

        for (source, target) in results.iter() {
            let datetime = NaiveDateTime::from_str(source).unwrap();
            let ymd = Ymd::from(&datetime);
            let (time1m, _) = if day_type == DayType::MinDgDay {
                t1mcvt
                    .to_1m_with_min_dg_day(breed, ymd.yyyymmdd, &datetime)
                    .unwrap()
            } else {
                t1mcvt
                    .to_1m_with_trading_day(breed, ymd.yyyymmdd, &datetime)
                    .unwrap()
            };
            let time1m_str = time1m.format(format).to_string();
            println!(
                "{}: '{}' '{}' {}",
                source,
                time1m_str,
                target,
                time1m_str == *target
            );
            assert_eq!(time1m_str, *target);
        }
        println!();
    }

    async fn init() {
        init_test_mysql_pools();
        let pool = MySqlPools::default();
        BreedInfoVec::init(&pool).await.unwrap();
        TxTimeRangeData::init(&pool).await.unwrap();
        TradingDayUtil::init(&pool).await.unwrap();
        ConvertTo1m::init().unwrap();
    }

    #[tokio::test]
    async fn test_to_time_1m_1() {
        init().await;
        let breed = "IC";
        let tx_ranges = "[(931,1130),(1301,1500)]";
        let results = vec![
            ("2022-06-10T09:29:00", "2022-06-10 09:31:00"),
            ("2022-06-10T10:15:00", "2022-06-10 10:16:00"),
            ("2022-06-10T13:00:00", "2022-06-10 13:01:00"),
            ("2022-06-10T13:00:59", "2022-06-10 13:01:00"),
            ("2022-06-13T14:59:00", "2022-06-13 15:00:00"),
            ("2022-06-13T14:59:59", "2022-06-13 15:00:00"),
            ("2022-06-13T15:00:00", "2022-06-13 15:00:00"),
        ];
        test_to_time_1m_sub(DayType::MinDgDay, breed, tx_ranges, &results);

        test_to_time_1m_sub(DayType::TradingDay, breed, tx_ranges, &results);
    }

    #[tokio::test]
    async fn test_to_time_1m_2() {
        init().await;
        let breed = "TF";
        let tx_ranges = "[(931,1130),(1301,1515)]";
        let results = vec![
            ("2022-06-10T09:29:00", "2022-06-10 09:31:00"),
            ("2022-06-10T10:15:00", "2022-06-10 10:16:00"),
            ("2022-06-10T13:00:00", "2022-06-10 13:01:00"),
            ("2022-06-10T13:00:59", "2022-06-10 13:01:00"),
            ("2022-06-13T14:59:00", "2022-06-13 15:00:00"),
            ("2022-06-13T14:59:59", "2022-06-13 15:00:00"),
            ("2022-06-13T15:00:00", "2022-06-13 15:01:00"),
            ("2022-06-13T15:14:59", "2022-06-13 15:15:00"),
            ("2022-06-13T15:15:00", "2022-06-13 15:15:00"),
        ];
        test_to_time_1m_sub(DayType::MinDgDay, breed, tx_ranges, &results);

        test_to_time_1m_sub(DayType::TradingDay, breed, tx_ranges, &results);
    }

    #[tokio::test]
    async fn test_to_time_1m_3() {
        init().await;
        let breed = "AP";
        let tx_ranges = "[(901,1015),(1031,1130),(1331,1500)]";
        let results = vec![
            ("2022-06-13T08:59:00", "2022-06-13 09:01:00"),
            ("2022-06-13T09:00:00", "2022-06-13 09:01:00"),
            ("2022-06-13T09:01:00", "2022-06-13 09:02:00"),
            ("2022-06-13T10:14:59", "2022-06-13 10:15:00"),
            ("2022-06-13T10:15:00", "2022-06-13 10:15:00"),
            ("2022-06-13T10:30:00", "2022-06-13 10:31:00"),
            ("2022-06-13T10:30:59", "2022-06-13 10:31:00"),
            ("2022-06-13T11:29:59", "2022-06-13 11:30:00"),
            ("2022-06-13T11:30:00", "2022-06-13 11:30:00"),
            ("2022-06-13T13:30:00", "2022-06-13 13:31:00"),
            ("2022-06-13T14:59:00", "2022-06-13 15:00:00"),
            ("2022-06-13T14:59:59", "2022-06-13 15:00:00"),
            ("2022-06-13T15:00:00", "2022-06-13 15:00:00"),
        ];
        test_to_time_1m_sub(DayType::MinDgDay, breed, tx_ranges, &results);

        test_to_time_1m_sub(DayType::TradingDay, breed, tx_ranges, &results);
    }

    #[tokio::test]
    async fn test_to_time_1m_4() {
        init().await;
        let breed = "a";
        let tx_ranges = "[(2101,2300),(901,1015),(1031,1130),(1331,1500)]";
        let results = vec![
            // 夜盘 start
            ("2022-06-10T20:59:59", "2022-06-10 21:01:00"),
            ("2022-06-10T21:00:00", "2022-06-10 21:01:00"),
            ("2022-06-10T22:00:00", "2022-06-10 22:01:00"),
            ("2022-06-10T22:59:00", "2022-06-10 23:00:00"),
            ("2022-06-10T23:00:00", "2022-06-10 23:00:00"),
            // 夜盘 end
            // 白盘 start
            ("2022-06-13T09:00:00", "2022-06-13 09:01:00"),
            ("2022-06-13T10:14:59", "2022-06-13 10:15:00"),
            ("2022-06-13T10:15:00", "2022-06-13 10:15:00"),
            ("2022-06-13T10:30:00", "2022-06-13 10:31:00"),
            ("2022-06-13T10:30:59", "2022-06-13 10:31:00"),
            ("2022-06-13T11:29:59", "2022-06-13 11:30:00"),
            ("2022-06-13T11:30:00", "2022-06-13 11:30:00"),
            ("2022-06-13T13:30:00", "2022-06-13 13:31:00"),
            ("2022-06-13T14:59:00", "2022-06-13 15:00:00"),
            ("2022-06-13T14:59:59", "2022-06-13 15:00:00"),
            ("2022-06-13T15:00:00", "2022-06-13 15:00:00"),
            // 白盘 end
        ];

        test_to_time_1m_sub(DayType::MinDgDay, breed, tx_ranges, &results);

        let results = vec![
            // 夜盘 start
            ("2022-06-13T20:59:59", "2022-06-10 21:01:00"),
            ("2022-06-13T21:00:00", "2022-06-10 21:01:00"),
            ("2022-06-13T22:00:00", "2022-06-10 22:01:00"),
            ("2022-06-13T22:59:00", "2022-06-10 23:00:00"),
            ("2022-06-13T23:00:00", "2022-06-10 23:00:00"),
            // 夜盘 end
            // 白盘 start
            ("2022-06-13T09:00:00", "2022-06-13 09:01:00"),
            ("2022-06-13T10:14:59", "2022-06-13 10:15:00"),
            ("2022-06-13T10:15:00", "2022-06-13 10:15:00"),
            ("2022-06-13T10:30:00", "2022-06-13 10:31:00"),
            ("2022-06-13T10:30:59", "2022-06-13 10:31:00"),
            ("2022-06-13T11:29:59", "2022-06-13 11:30:00"),
            ("2022-06-13T11:30:00", "2022-06-13 11:30:00"),
            ("2022-06-13T13:30:00", "2022-06-13 13:31:00"),
            ("2022-06-13T14:59:00", "2022-06-13 15:00:00"),
            ("2022-06-13T14:59:59", "2022-06-13 15:00:00"),
            ("2022-06-13T15:00:00", "2022-06-13 15:00:00"),
            // 白盘 end
        ];

        test_to_time_1m_sub(DayType::TradingDay, breed, tx_ranges, &results);
    }

    #[tokio::test]
    async fn test_to_time_1m_5() {
        init().await;
        let breed = "ag";
        let tx_ranges = "[(2101,230),(901,1015),(1031,1130),(1331,1500)]";
        let results = vec![
            // 跨周的时间处理
            // 夜盘 start
            ("2022-06-10T20:59:59", "2022-06-10 21:01:00"),
            ("2022-06-10T21:00:00", "2022-06-10 21:01:00"),
            ("2022-06-10T23:58:33", "2022-06-10 23:59:00"),
            ("2022-06-10T23:59:33", "2022-06-11 00:00:00"),
            ("2022-06-10T00:00:00", "2022-06-11 00:00:00"),
            ("2022-06-10T00:00:33", "2022-06-11 00:01:00"),
            ("2022-06-10T00:01:00", "2022-06-11 00:02:00"),
            ("2022-06-10T00:59:00", "2022-06-11 01:00:00"),
            ("2022-06-10T00:59:59", "2022-06-11 01:00:00"),
            ("2022-06-10T01:30:59", "2022-06-11 01:31:00"),
            ("2022-06-10T01:59:59", "2022-06-11 02:00:00"),
            ("2022-06-10T02:00:33", "2022-06-11 02:01:00"),
            ("2022-06-10T02:29:33", "2022-06-11 02:30:00"),
            ("2022-06-10T02:30:00", "2022-06-11 02:30:00"),
            // 夜盘 end
            // 白盘 start
            ("2022-06-13T09:00:00", "2022-06-13 09:01:00"),
            ("2022-06-13T10:14:59", "2022-06-13 10:15:00"),
            ("2022-06-13T10:15:00", "2022-06-13 10:15:00"),
            ("2022-06-13T10:30:00", "2022-06-13 10:31:00"),
            ("2022-06-13T10:30:59", "2022-06-13 10:31:00"),
            ("2022-06-13T11:29:59", "2022-06-13 11:30:00"),
            ("2022-06-13T11:30:00", "2022-06-13 11:30:00"),
            ("2022-06-13T13:30:00", "2022-06-13 13:31:00"),
            ("2022-06-13T14:59:00", "2022-06-13 15:00:00"),
            ("2022-06-13T14:59:59", "2022-06-13 15:00:00"),
            ("2022-06-13T15:00:00", "2022-06-13 15:00:00"),
            // 白盘 end
        ];
        test_to_time_1m_sub(DayType::MinDgDay, breed, tx_ranges, &results);

        let results = vec![
            // 跨周的时间处理
            // 夜盘 start
            ("2022-06-13T20:59:59", "2022-06-10 21:01:00"),
            ("2022-06-13T21:00:00", "2022-06-10 21:01:00"),
            ("2022-06-13T23:58:33", "2022-06-10 23:59:00"),
            ("2022-06-13T23:59:33", "2022-06-11 00:00:00"),
            ("2022-06-13T00:00:00", "2022-06-11 00:00:00"),
            ("2022-06-13T00:00:33", "2022-06-11 00:01:00"),
            ("2022-06-13T00:01:00", "2022-06-11 00:02:00"),
            ("2022-06-13T00:59:00", "2022-06-11 01:00:00"),
            ("2022-06-13T00:59:59", "2022-06-11 01:00:00"),
            ("2022-06-13T01:30:59", "2022-06-11 01:31:00"),
            ("2022-06-13T01:59:59", "2022-06-11 02:00:00"),
            ("2022-06-13T02:00:33", "2022-06-11 02:01:00"),
            ("2022-06-13T02:29:33", "2022-06-11 02:30:00"),
            ("2022-06-13T02:30:00", "2022-06-11 02:30:00"),
            // 夜盘 end
            // 白盘 start
            ("2022-06-13T09:00:00", "2022-06-13 09:01:00"),
            ("2022-06-13T10:14:59", "2022-06-13 10:15:00"),
            ("2022-06-13T10:15:00", "2022-06-13 10:15:00"),
            ("2022-06-13T10:30:00", "2022-06-13 10:31:00"),
            ("2022-06-13T10:30:59", "2022-06-13 10:31:00"),
            ("2022-06-13T11:29:59", "2022-06-13 11:30:00"),
            ("2022-06-13T11:30:00", "2022-06-13 11:30:00"),
            ("2022-06-13T13:30:00", "2022-06-13 13:31:00"),
            ("2022-06-13T14:59:00", "2022-06-13 15:00:00"),
            ("2022-06-13T14:59:59", "2022-06-13 15:00:00"),
            ("2022-06-13T15:00:00", "2022-06-13 15:00:00"),
            // 白盘 end
        ];
        test_to_time_1m_sub(DayType::TradingDay, breed, tx_ranges, &results);
    }

    #[tokio::test]
    async fn test_to_time_1m_6() {
        init().await;
        let breed = "al";
        let tx_ranges = "[(2101,100),(901,1015),(1031,1130),(1331,1500)]";
        let results = vec![
            // 跨周的时间处理
            // 夜盘 start
            ("2022-06-10T20:59:59", "2022-06-10 21:01:00"),
            ("2022-06-10T21:00:00", "2022-06-10 21:01:00"),
            ("2022-06-10T23:58:33", "2022-06-10 23:59:00"),
            ("2022-06-10T23:59:33", "2022-06-11 00:00:00"),
            ("2022-06-10T00:00:00", "2022-06-11 00:00:00"),
            ("2022-06-10T00:00:33", "2022-06-11 00:01:00"),
            ("2022-06-10T00:01:00", "2022-06-11 00:02:00"),
            ("2022-06-10T00:59:00", "2022-06-11 01:00:00"),
            ("2022-06-10T00:59:59", "2022-06-11 01:00:00"),
            ("2022-06-10T01:00:00", "2022-06-11 01:00:00"),
            // 夜盘 end
            // 白盘 start
            ("2022-06-13T09:00:00", "2022-06-13 09:01:00"),
            ("2022-06-13T10:14:59", "2022-06-13 10:15:00"),
            ("2022-06-13T10:15:00", "2022-06-13 10:15:00"),
            ("2022-06-13T10:30:00", "2022-06-13 10:31:00"),
            ("2022-06-13T10:30:59", "2022-06-13 10:31:00"),
            ("2022-06-13T11:29:59", "2022-06-13 11:30:00"),
            ("2022-06-13T11:30:00", "2022-06-13 11:30:00"),
            ("2022-06-13T13:30:00", "2022-06-13 13:31:00"),
            ("2022-06-13T14:59:00", "2022-06-13 15:00:00"),
            ("2022-06-13T14:59:59", "2022-06-13 15:00:00"),
            ("2022-06-13T15:00:00", "2022-06-13 15:00:00"),
            // 白盘 end
        ];
        test_to_time_1m_sub(DayType::MinDgDay, breed, tx_ranges, &results);

        let results = vec![
            // 跨周的时间处理
            // 夜盘 start
            ("2022-06-13T20:59:59", "2022-06-10 21:01:00"),
            ("2022-06-13T21:00:00", "2022-06-10 21:01:00"),
            ("2022-06-13T23:58:33", "2022-06-10 23:59:00"),
            ("2022-06-13T23:59:33", "2022-06-11 00:00:00"),
            ("2022-06-13T00:00:00", "2022-06-11 00:00:00"),
            ("2022-06-13T00:00:33", "2022-06-11 00:01:00"),
            ("2022-06-13T00:01:00", "2022-06-11 00:02:00"),
            ("2022-06-13T00:59:00", "2022-06-11 01:00:00"),
            ("2022-06-13T00:59:59", "2022-06-11 01:00:00"),
            ("2022-06-13T01:00:00", "2022-06-11 01:00:00"),
            // 夜盘 end
            // 白盘 start
            ("2022-06-13T09:00:00", "2022-06-13 09:01:00"),
            ("2022-06-13T10:14:59", "2022-06-13 10:15:00"),
            ("2022-06-13T10:15:00", "2022-06-13 10:15:00"),
            ("2022-06-13T10:30:00", "2022-06-13 10:31:00"),
            ("2022-06-13T10:30:59", "2022-06-13 10:31:00"),
            ("2022-06-13T11:29:59", "2022-06-13 11:30:00"),
            ("2022-06-13T11:30:00", "2022-06-13 11:30:00"),
            ("2022-06-13T13:30:00", "2022-06-13 13:31:00"),
            ("2022-06-13T14:59:00", "2022-06-13 15:00:00"),
            ("2022-06-13T14:59:59", "2022-06-13 15:00:00"),
            ("2022-06-13T15:00:00", "2022-06-13 15:00:00"),
            // 白盘 end
        ];
        test_to_time_1m_sub(DayType::TradingDay, breed, tx_ranges, &results);
    }

    #[tokio::test]
    async fn test_to_1m_error() {
        // "2022-06-27 15:40:38 +0800"
        init_test_mysql_pools();
        BreedInfoVec::init(&MySqlPools::default()).await.unwrap();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        ConvertTo1m::init().unwrap();
        let time = NaiveTime::from_hms(15, 40, 38);
        let time1m = ConvertTo1m::current().to_1m_with_min_dg_day("IC", 20220627, &time);
        println!("{:?}", time1m);
    }

    #[tokio::test]
    async fn test_init() {
        init_test_mysql_pools();
        BreedInfoVec::init(&MySqlPools::default()).await.unwrap();
        TxTimeRangeData::init(&MySqlPools::default()).await.unwrap();
        ConvertTo1m::init().unwrap();
        let t1mcvt = ConvertTo1m::current();
        for BreedInfo { breed, .. } in BreedInfoVec::current().vec() {
            println!(
                "{}: {:?}",
                breed,
                t1mcvt.breed_1mtime_hmap.get(breed).unwrap()
            );
        }
    }

    #[test]
    fn test_chrono() {
        let time = NaiveTime::from_hms(0, 0, 0);
        let date = NaiveDate::from_ymd(2022, 10, 31);
        let datetime = date.and_time(time);
        println!("time: {}", time);
        println!("date: {}", date);
        println!("datetime: {}", datetime);
        let mut datetime = NaiveDate::from_ymd(2022, 10, 31).and_hms(23, 59, 59);
        datetime += Duration::seconds(1);
        println!("add: {}", datetime);
    }
}
