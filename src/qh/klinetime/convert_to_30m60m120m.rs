use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use futures::TryStreamExt;
use lazy_static::lazy_static;
use sqlx::{FromRow, MySqlPool};

use super::{KLineTimeError, TimeRangeDateTime};
use crate::qh::trading_day::TradingDayUtil;
use crate::ymdhms::{Hms, TimeRangeHms, Ymd};

#[derive(FromRow)]
struct DbItem {
    breed:     String,
    period:    String,
    rangelist: String,
}

// breed,period,vec<TimeRangeHms>
type StoreData = HashMap<String, HashMap<String, Vec<TimeRangeHms>>>;

impl Extend<DbItem> for StoreData {
    fn extend<T: IntoIterator<Item = DbItem>>(&mut self, iter: T) {
        // 临时共用存储数据的HashMap
        let mut tr_key_vec_tr_hmap = HashMap::new();
        for row in iter {
            let vec_time_range_hms =
                tr_key_vec_tr_hmap
                    .entry(row.rangelist)
                    .or_insert_with_key(|key| {
                        let value_vec = key
                            .replace([' ', '[', ']', '(', ')'], "")
                            .split(',')
                            .map(|v| v.parse::<u16>().unwrap())
                            .collect::<Vec<_>>();
                        let range_len = value_vec.len() / 2;
                        let mut range_vec = Vec::new();
                        for i in 0..range_len {
                            let shhmmss = *value_vec.get(i * 2).unwrap() as u32 * 100;
                            let ehhmmss = *value_vec.get(i * 2 + 1).unwrap() as u32 * 100;
                            range_vec.push(TimeRangeHms::new(shhmmss, ehhmmss));
                        }
                        range_vec
                    });
            let period_vec_hmap = self.entry(row.breed).or_insert_with(Default::default);
            period_vec_hmap
                .entry(row.period)
                .or_insert_with(|| vec_time_range_hms.to_vec());
        }
    }
}

lazy_static! {
    static ref CONVERT_30M60M120M: RwLock<Arc<ConvertTo30m60m120m>> =
        RwLock::new(Default::default());
}

pub(crate) struct ConvertTo30m60m120m {
    tdu:        Arc<TradingDayUtil>,
    store_data: StoreData,
}

impl Default for ConvertTo30m60m120m {
    fn default() -> Self {
        Self {
            tdu:        TradingDayUtil::current(),
            store_data: Default::default(),
        }
    }
}

impl ConvertTo30m60m120m {
    pub fn current() -> Arc<ConvertTo30m60m120m> {
        CONVERT_30M60M120M.read().unwrap().clone()
    }

    // TradingDayUtil::init
    pub(crate) async fn init(pool: &MySqlPool) -> Result<(), sqlx::Error> {
        if !Self::current().store_data.is_empty() {
            return Ok(());
        }
        let mut ct = ConvertTo30m60m120m::default();
        ct.init_from_db(pool).await?;
        *CONVERT_30M60M120M.write().unwrap() = Arc::new(ct);
        Ok(())
    }

    async fn init_from_db(&mut self, pool: &MySqlPool) -> Result<(), sqlx::Error> {
        let sql = "SELECT breed,period,rangelist FROM `hqdb`.`tbl_future_period_time_range`";
        let store_data = sqlx::query_as::<_, DbItem>(sql)
            .fetch(pool)
            .try_collect::<StoreData>()
            .await?;
        self.store_data = store_data;
        Ok(())
    }

    /// 转换成对应周期的时间
    pub(crate) fn time_range(
        &self,
        breed: &str,
        period: &str,
        datetime: &NaiveDateTime,
    ) -> Result<TimeRangeDateTime, KLineTimeError> {
        let time_range_hms = self
            .store_data
            .get(&breed.to_uppercase())
            .ok_or(KLineTimeError::BreedNotExist {
                breed: breed.to_owned(),
                scope: "Convert30m60m120m".to_owned(),
            })?
            .get(period)
            .ok_or(KLineTimeError::PeriodNotExist {
                period: period.to_owned(),
                scope:  "Convert30m60m120m".to_owned(),
            })?
            .iter()
            .find(|v| v.in_range_time(&datetime.time()))
            .ok_or(KLineTimeError::DatetimeNotInRange {
                breed:    breed.to_owned(),
                datetime: *datetime,
            })?;

        let hms = Hms::from(&datetime.time());
        let s = time_range_hms.start;
        let e = time_range_hms.end;
        let shhmmss = s.hhmmss;
        let ehhmmss = e.hhmmss;

        let hhmmss = hms.hhmmss;

        let mut sdate = datetime.date();
        let stime = NaiveTime::from(&s);

        let mut edate = datetime.date();
        let etime = NaiveTime::from(&e);

        if shhmmss > ehhmmss {
            // 跨天了
            if (shhmmss..23_59_59).contains(&hhmmss) {
                // 结束时间: 0点之前的时间加一天, 0点之后的时间不做处理
                edate += Duration::days(1);
            } else if hhmmss <= ehhmmss {
                // 开始时间: 0点之后的时间减一天
                sdate -= Duration::days(1);
            }
        } else if e.hour - s.hour >= 7 {
            // 当时间跨段从夜盘到白盘
            if hhmmss < 3_00_00 {
                // 夜盘时间
                // 结束时间: 如果是非交易日, 取下一交易日.
                // 处理周六天或节假日的情况.
                let ymd = Ymd::from(&datetime.date());
                let tdu = &self.tdu;
                if !tdu.is_td(&ymd.yyyymmdd) {
                    let next_td = tdu.next(&ymd.yyyymmdd)?;
                    edate = NaiveDate::from(next_td)
                }
            } else if hms.hour > 8 {
                // 白盘时间
                // 取上一交易日 + 1天
                let ymd = Ymd::from(&datetime.date());
                let tdu = &self.tdu;
                let prev_td = tdu.prev(&ymd.yyyymmdd)?;
                sdate = NaiveDate::from(prev_td) + Duration::days(1);
            }
        }
        Ok(TimeRangeDateTime::new(
            sdate.and_time(stime),
            edate.and_time(etime),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{Duration, NaiveDate, NaiveTime, Timelike};
    use tokio::runtime::Runtime;

    use super::ConvertTo30m60m120m;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;
    use crate::qh::klinetime::tx_time_range::TxTimeRangeData;
    use crate::qh::period::PeriodUtil;
    use crate::qh::trading_day::TradingDayUtil;

    #[test]
    fn test_init() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            init_test_mysql_pools();
            ConvertTo30m60m120m::init(&MySqlPools::pool())
                .await
                .unwrap();
            let show_breeds = vec!["IC", "TF", "AP", "a", "ag", "al"];
            let store_data = &ConvertTo30m60m120m::current().store_data;
            for breed in show_breeds {
                let breed_period_rt_vec = store_data.get(&breed.to_uppercase()).unwrap();
                for (period, vec_trh) in breed_period_rt_vec {
                    println!(
                        "{} {} {:?}",
                        breed,
                        period,
                        vec_trh
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<String>>()
                    );
                }
            }

            // for (breed, v1) in store_data {
            //     if !show_breeds.contains(&&**breed) {
            //         continue;
            //     }
            //     for (period, vec_trh) in v1 {
            //         println!(
            //             "{} {} {:?}",
            //             breed,
            //             period,
            //             vec_trh
            //                 .iter()
            //                 .map(|v| v.to_string())
            //                 .collect::<Vec<String>>()
            //         );
            //     }
            //     println!();
            // }
        })
    }

    fn test_to_xm_sub(breed: &str, tx_ranges: &str, period: &str, last_vec_len: usize) {
        println!("=== {} {} {} ===", breed, period, tx_ranges);
        let trd = TxTimeRangeData::current();
        let cvt = ConvertTo30m60m120m::current();
        let tx_range_fix_vec = trd.time_range_fix_vec(breed).unwrap();
        let date = NaiveDate::from_ymd_opt(2022, 6, 17).unwrap();
        let next_date = date + Duration::days(1);
        let next_td = NaiveDate::from(TradingDayUtil::current().next(&20220617).unwrap());

        let mut key_vec = vec![];
        let mut xm_vec_map: HashMap<String, Vec<_>> = HashMap::new();

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
                let tr_dt = cvt.time_range(breed, period, &datetime).unwrap();
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
        let key_max_idx = key_vec.len() - 1;
        for (idx, key) in key_vec.iter().enumerate() {
            let datetime_vec = xm_vec_map.get(key).unwrap();
            let len = datetime_vec.len();
            println!(
                "# {}: {:?} *{}*",
                key,
                datetime_vec
                    .iter()
                    .map(|v| { v.format("%Y-%m-%d %H:%M:%S").to_string() })
                    .collect::<Vec<String>>(),
                len
            );
            let right = if key_max_idx > idx {
                len == *pv as usize
            } else {
                len == last_vec_len
            };
            assert!(right, "{}, {}, len:{}, pv:{}", breed, period, len, pv);
        }
        println!();
    }

    #[tokio::test]
    async fn test_to_xm_1() {
        init_test_mysql_pools();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        ConvertTo30m60m120m::init(&MySqlPools::pool())
            .await
            .unwrap();

        let breed = "IC";
        let tx_ranges = "[(931,1130),(1301,1500)]";
        test_to_xm_sub(breed, tx_ranges, "30m", 30);
        test_to_xm_sub(breed, tx_ranges, "60m", 60);
        test_to_xm_sub(breed, tx_ranges, "120m", 120);
    }

    #[tokio::test]
    async fn test_to_xm_2() {
        init_test_mysql_pools();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        ConvertTo30m60m120m::init(&MySqlPools::pool())
            .await
            .unwrap();
        let breed = "TF";
        let tx_ranges = "[(931,1130),(1301,1515)]";
        test_to_xm_sub(breed, tx_ranges, "30m", 15);
        test_to_xm_sub(breed, tx_ranges, "60m", 15);
        test_to_xm_sub(breed, tx_ranges, "120m", 15);
    }

    #[tokio::test]
    async fn test_to_xm_3() {
        init_test_mysql_pools();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        ConvertTo30m60m120m::init(&MySqlPools::pool())
            .await
            .unwrap();
        let breed = "AP";
        let tx_ranges = "[(901,1015),(1031,1130),(1331,1500)]";
        test_to_xm_sub(breed, tx_ranges, "30m", 15);
        test_to_xm_sub(breed, tx_ranges, "60m", 45);
        test_to_xm_sub(breed, tx_ranges, "120m", 105);
    }

    #[tokio::test]
    async fn test_to_xm_4() {
        init_test_mysql_pools();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        ConvertTo30m60m120m::init(&MySqlPools::pool())
            .await
            .unwrap();
        let breed = "a";
        let tx_ranges = "[(2101,2300),(901,1015),(1031,1130),(1331,1500)]";
        test_to_xm_sub(breed, tx_ranges, "30m", 15);
        test_to_xm_sub(breed, tx_ranges, "60m", 45);
        test_to_xm_sub(breed, tx_ranges, "120m", 105);
    }

    #[tokio::test]
    async fn test_to_xm_5() {
        init_test_mysql_pools();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        ConvertTo30m60m120m::init(&MySqlPools::pool())
            .await
            .unwrap();
        let breed = "ag";
        let tx_ranges = "[(2101,230),(901,1015),(1031,1130),(1331,1500)]";
        test_to_xm_sub(breed, tx_ranges, "30m", 15);
        test_to_xm_sub(breed, tx_ranges, "60m", 15);
        test_to_xm_sub(breed, tx_ranges, "120m", 75);
    }

    #[tokio::test]
    async fn test_to_xm_6() {
        init_test_mysql_pools();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        ConvertTo30m60m120m::init(&MySqlPools::pool())
            .await
            .unwrap();
        let breed = "al";
        let tx_ranges = "[(2101,100),(901,1015),(1031,1130),(1331,1500)]";
        test_to_xm_sub(breed, tx_ranges, "30m", 15);
        test_to_xm_sub(breed, tx_ranges, "60m", 45);
        test_to_xm_sub(breed, tx_ranges, "120m", 105);
    }
}
