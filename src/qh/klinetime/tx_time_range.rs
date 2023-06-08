//! 交易时间段相关的数据与操作.
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use futures::TryStreamExt;
use lazy_static::lazy_static;
use sqlx::{FromRow, MySqlPool};

use super::KLineTimeError;
use crate::qh::trading_day::TradingDayUtil;
use crate::ymdhms::{Hms, TimeRangeHms, Ymd};

lazy_static! {
    static ref TX_TIME_RANGE_DATA: RwLock<Arc<TxTimeRangeData>> = RwLock::new(Default::default());
}

#[derive(FromRow)]
struct TxTimeRangeDbItem {
    breed:     String,
    rangelist: String,
}

struct BreedTxTimeRange {
    // 大写
    breed:      String,
    has_night:  bool,
    // 对应的时间范围集合. 一定不要重新排序, 如果合约有夜盘就是夜盘开始的时间.
    tr_vec:     Vec<TimeRangeHms>,
    // 对应修正了开始时间的时间范围集合.
    tr_vec_fix: Vec<TimeRangeHms>,

    range_end_hmap: HashMap<u32, ()>,
}

impl BreedTxTimeRange {
    // [(931,1130),(1301,150)]
    // [(931,1130),(1301,1515)]
    // [(901,1015),(1031,1130),(1331,1500)]
    // [(2101,2300),(901,1015),(1031,1130),(1331,1500)]
    // [(2101,100),(901,1015),(1031,1130),(1331,1500)]
    // [(2101,230),(901,1015),(1031,1130),(1331,1500)]
    fn next_minute(&self, datetime: &NaiveDateTime) -> Result<NaiveDateTime, KLineTimeError> {
        let mut close_idx = None;
        let hhmm = Hms::from(datetime).hhmm;
        for (idx, hms) in self.tr_vec.iter().enumerate() {
            let TimeRangeHms { start, end } = hms;
            if (start > end
                && ((start.hhmm..=2359).contains(&hhmm) || (0..=end.hhmm).contains(&hhmm)))
                || (start.hhmm..=end.hhmm).contains(&hhmm)
            {
                if hhmm == end.hhmm {
                    close_idx = Some(idx);
                    break;
                } else {
                    return Ok(*datetime + Duration::minutes(1));
                }
            }
        }

        let mut next_tr = close_idx
            .map(|v| {
                let len = self.tr_vec.len();
                let next_idx = (v + 1) % len;
                // self.tr_vec[next_idx].clone()
                self.tr_vec.get(next_idx).unwrap()
            })
            .ok_or_else(|| KLineTimeError::DatetimeNotInRange {
                breed:    self.breed.clone(),
                datetime: *datetime,
            })?;

        let end_hhmm = self.tr_vec.last().unwrap().end.hhmm;

        let tdu = TradingDayUtil::current();

        let ymd = &Ymd::from(datetime);

        let yyyymmdd = ymd.yyyymmdd;

        let ymd = match hhmm {
            2300 => {
                // 直接取下一交易日
                tdu.next(&yyyymmdd)?
            },
            100 | 230 => {
                if tdu.is_td(&yyyymmdd) {
                    ymd
                } else {
                    tdu.next(&yyyymmdd)?
                }
            },
            hhmm if hhmm == end_hhmm => {
                let next_td = tdu.next(&yyyymmdd)?;

                if self.has_night {
                    if tdu.has_night(&next_td.yyyymmdd) {
                        ymd
                    } else {
                        next_tr = self.tr_vec.get(1).unwrap();
                        next_td
                    }
                } else {
                    next_td
                }
            },
            _ => ymd,
        };
        Ok(NaiveDate::from(ymd).and_time(
            NaiveTime::from_hms_opt(next_tr.start.hour as u32, next_tr.start.minute as u32, 0)
                .unwrap(),
        ))
    }

    fn is_trading_time(&self, time: &impl Timelike) -> bool {
        let hhmmss = Hms::from(time).hhmmss;
        for tr in self.tr_vec_fix.iter() {
            if (tr.start.hhmmss..=tr.end.hhmmss).contains(&hhmmss) {
                return true;
            }
        }
        false
    }

    fn is_first_minute(&self, trading_day: &u32, time: &impl Timelike) -> bool {
        let hms = Hms::from(time);
        if self.has_night {
            if TradingDayUtil::current().has_night(trading_day) {
                hms == self.tr_vec[0].start
            } else {
                hms == self.tr_vec[1].start
            }
        } else {
            hms == self.tr_vec[0].start
        }
    }

    fn is_range_end(&self, time: &impl Timelike) -> bool {
        let hhmmss = Hms::from(time).hhmmss;
        self.range_end_hmap.contains_key(&hhmmss)
    }
}

impl From<TxTimeRangeDbItem> for BreedTxTimeRange {
    fn from(item: TxTimeRangeDbItem) -> Self {
        // [(2101,230),(901,1015),(1031,1130),(1331,1500)]
        let value_vec = item
            .rangelist
            .replace([' ', '[', ']', '(', ')'], "")
            .split(',')
            .map(|v| v.parse::<u16>().unwrap())
            .collect::<Vec<_>>();
        let first_value = value_vec.first().unwrap();
        let second_value = value_vec.get(1).unwrap();
        let need_fix = first_value > second_value;
        let range_len = value_vec.len() / 2;

        let mut range_vec = Vec::new();
        let mut range_vec_fix = Vec::new();

        let has_night = first_value == &2101;

        let mut range_end_hmap = HashMap::new();
        for i in 0..range_len {
            let shhmmss = *value_vec.get(i * 2).unwrap() as u32 * 100;
            let ehhmmss = *value_vec.get(i * 2 + 1).unwrap() as u32 * 100;

            range_vec.push(TimeRangeHms::new(shhmmss, ehhmmss));
            if need_fix && i == 0 {
                range_vec_fix.push(TimeRangeHms::new(shhmmss, 235959));
                range_vec_fix.push(TimeRangeHms::new(0, ehhmmss));
            } else {
                range_vec_fix.push(TimeRangeHms::new(shhmmss, ehhmmss));
            }
            range_end_hmap.insert(ehhmmss, ());
        }
        BreedTxTimeRange {
            breed: item.breed,
            has_night,
            tr_vec: range_vec,
            tr_vec_fix: range_vec_fix,
            range_end_hmap,
        }
    }
}

/// 每个品种的交易时间段数据.
#[derive(Default)]
pub struct TxTimeRangeData {
    breed_ttr_hmap: HashMap<String, BreedTxTimeRange>,
}

impl TxTimeRangeData {
    pub fn current() -> Arc<TxTimeRangeData> {
        TX_TIME_RANGE_DATA.read().unwrap().clone()
    }

    pub async fn init(pool: &MySqlPool) -> Result<(), sqlx::Error> {
        if !Self::current().is_empty() {
            return Ok(());
        }
        let mut tru = TxTimeRangeData::default();
        tru.init_from_db(pool).await?;
        *TX_TIME_RANGE_DATA.write().unwrap() = Arc::new(tru);
        Ok(())
    }

    async fn init_from_db(&mut self, pool: &MySqlPool) -> Result<(), sqlx::Error> {
        let sql =
            "SELECT breed,rangelist FROM `hqdb`.`tbl_future_tx_time_range` ORDER BY rangelist";
        let hmap = sqlx::query_as::<_, TxTimeRangeDbItem>(sql)
            .fetch(pool)
            .map_ok(|v| (v.breed.clone(), BreedTxTimeRange::from(v)))
            .try_collect::<HashMap<String, BreedTxTimeRange>>()
            .await?;
        self.breed_ttr_hmap = hmap;
        Ok(())
    }

    pub(crate) fn time_range_vec(&self, breed: &str) -> Result<&Vec<TimeRangeHms>, KLineTimeError> {
        self.breed_ttr_hmap
            .get(&breed.to_uppercase())
            .ok_or(KLineTimeError::BreedNotExist {
                breed: breed.to_owned(),
                scope: "TxTimeRangeDate".to_owned(),
            })
            .map(|v| &v.tr_vec)
    }

    #[allow(unused)]
    pub(crate) fn time_range_fix_vec(
        &self,
        breed: &str,
    ) -> Result<&Vec<TimeRangeHms>, KLineTimeError> {
        self.breed_ttr_hmap
            .get(&breed.to_uppercase())
            .ok_or(KLineTimeError::BreedNotExist {
                breed: breed.to_owned(),
                scope: "TxTimeRangeDate".to_owned(),
            })
            .map(|v| &v.tr_vec_fix)
    }

    /// 是否交易时间
    /// datetime为经过处理后的时间, 不包括从tick直接拿到的时间
    pub fn is_trading_time(&self, breed: &str, time: &impl Timelike) -> bool {
        self.breed_ttr_hmap
            .get(&breed.to_uppercase())
            .map_or(false, |v| v.is_trading_time(time))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.breed_ttr_hmap.is_empty()
    }

    pub(crate) fn is_had_night(&self, breed: &str) -> bool {
        self.breed_ttr_hmap
            .get(&breed.to_uppercase())
            .map_or(false, |v| v.has_night)
    }

    pub fn next_minute(
        &self,
        breed: &str,
        datetime: &NaiveDateTime,
    ) -> Result<NaiveDateTime, KLineTimeError> {
        self.breed_ttr_hmap
            .get(&breed.to_uppercase())
            .ok_or(KLineTimeError::BreedNotExist {
                breed: breed.to_owned(),
                scope: "TxTimeRangeDate".to_owned(),
            })
            .map(|v| v.next_minute(datetime))?
    }

    pub fn is_first_minute(&self, breed: &str, trading_day: &u32, time: &impl Timelike) -> bool {
        self.breed_ttr_hmap
            .get(&breed.to_uppercase())
            .map_or(false, |v| v.is_first_minute(trading_day, time))
    }

    pub fn is_range_end(&self, breed: &str, time: &impl Timelike) -> bool {
        self.breed_ttr_hmap
            .get(&breed.to_uppercase())
            .map_or(false, |v| v.is_range_end(time))
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};

    use super::TxTimeRangeData;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;
    use crate::qh::breed::{BreedInfo, BreedInfoVec};
    use crate::qh::trading_day::TradingDayUtil;

    #[tokio::test]
    async fn test_time_range_util_init() {
        init_test_mysql_pools();
        BreedInfoVec::init(&MySqlPools::pool()).await.unwrap();
        let mut trd = TxTimeRangeData::default();
        trd.init_from_db(&MySqlPools::pool()).await.unwrap();
        for BreedInfo { breed, .. } in BreedInfoVec::current().vec() {
            println!(
                "{}: {:?}",
                breed,
                trd.time_range_vec(breed)
                    .unwrap()
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<String>>()
            );
            println!(
                "{}: {:?}",
                breed,
                trd.time_range_fix_vec(breed)
                    .unwrap()
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<String>>()
            );
        }
    }

    async fn test_next_minute_sub(breed: &str, time: &NaiveDateTime) {
        init_test_mysql_pools();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        println!("############## start: {}", breed);
        let ttrd = TxTimeRangeData::current();
        let mut key_hmap = HashMap::new();
        let mut c_minute = *time;
        let dt_fmt = "%Y-%m-%d %H:%M:%S";
        loop {
            let n_minute = ttrd.next_minute(breed, &c_minute).unwrap();

            println!("{} -> {}", c_minute.format(dt_fmt), n_minute.format(dt_fmt));
            let key = c_minute.format("%H:%M:%S").to_string();
            if key_hmap.contains_key(&key) {
                break;
            }
            c_minute = n_minute;
            key_hmap.insert(key, 1);
        }
    }

    // IC
    // [(931,1130),(1301,1500)]
    #[tokio::test]
    async fn test_next_minute_ic() {
        let time = NaiveDate::from_ymd_opt(2022, 7, 22)
            .unwrap()
            .and_hms_opt(9, 31, 0)
            .unwrap();
        test_next_minute_sub("IC", &time).await;
    }

    // TF
    // [(931,1130),(1301,1515)]
    #[tokio::test]
    async fn test_next_minute_tf() {
        let time = NaiveDate::from_ymd_opt(2022, 7, 25)
            .unwrap()
            .and_hms_opt(9, 31, 0)
            .unwrap();
        test_next_minute_sub("TF", &time).await;
    }

    // AP
    // [(901,1015),(1031,1130),(1331,1500)]
    #[tokio::test]
    async fn test_next_minute_ap() {
        let time = NaiveDate::from_ymd_opt(2022, 7, 25)
            .unwrap()
            .and_hms_opt(9, 1, 0)
            .unwrap();
        test_next_minute_sub("AP", &time).await;
    }

    // A 第二天
    // [(2101,2300),(901,1015),(1031,1130),(1331,1500)]
    #[tokio::test]
    async fn test_next_minute_a_1() {
        let time = NaiveDate::from_ymd_opt(2022, 7, 25)
            .unwrap()
            .and_hms_opt(21, 1, 0)
            .unwrap();
        test_next_minute_sub("A", &time).await;
    }

    // A 周六天
    // [(2101,2300),(901,1015),(1031,1130),(1331,1500)]
    #[tokio::test]
    async fn test_next_minute_a_2() {
        let time = NaiveDate::from_ymd_opt(2022, 7, 22)
            .unwrap()
            .and_hms_opt(21, 1, 0)
            .unwrap();
        test_next_minute_sub("A", &time).await;
    }

    // A 节假日
    // [(2101,2300),(901,1015),(1031,1130),(1331,1500)]
    #[tokio::test]
    async fn test_next_minute_a_3() {
        let time = NaiveDate::from_ymd_opt(2022, 6, 1)
            .unwrap()
            .and_hms_opt(21, 1, 0)
            .unwrap();
        test_next_minute_sub("A", &time).await;
    }

    // AL 第二天
    // [(2101,100),(901,1015),(1031,1130),(1331,1500)]
    #[tokio::test]
    async fn test_next_minute_al_1() {
        let time = NaiveDate::from_ymd_opt(2022, 7, 25)
            .unwrap()
            .and_hms_opt(21, 1, 0)
            .unwrap();
        test_next_minute_sub("AL", &time).await;
    }

    // AL 周六天
    // [(2101,100),(901,1015),(1031,1130),(1331,1500)]
    #[tokio::test]
    async fn test_next_minute_al_2() {
        let time = NaiveDate::from_ymd_opt(2022, 7, 22)
            .unwrap()
            .and_hms_opt(21, 1, 0)
            .unwrap();
        test_next_minute_sub("AL", &time).await;
    }

    // AL 节假日
    // [(2101,100),(901,1015),(1031,1130),(1331,1500)]
    #[tokio::test]
    async fn test_next_minute_al_3() {
        let time = NaiveDate::from_ymd_opt(2022, 6, 1)
            .unwrap()
            .and_hms_opt(21, 1, 0)
            .unwrap();
        test_next_minute_sub("AL", &time).await;
    }

    // AG 第二天
    // [(2101,230),(901,1015),(1031,1130),(1331,1500)]
    #[tokio::test]
    async fn test_next_minute_ag_1() {
        let time = NaiveDate::from_ymd_opt(2022, 7, 25)
            .unwrap()
            .and_hms_opt(21, 1, 0)
            .unwrap();
        test_next_minute_sub("AG", &time).await;
    }

    // AG 周六天
    // [(2101,230),(901,1015),(1031,1130),(1331,1500)]
    #[tokio::test]
    async fn test_next_minute_ag_2() {
        let time = NaiveDate::from_ymd_opt(2022, 7, 22)
            .unwrap()
            .and_hms_opt(21, 1, 0)
            .unwrap();
        test_next_minute_sub("AG", &time).await;
    }

    // AG 节假日
    // [(2101,230),(901,1015),(1031,1130),(1331,1500)]
    #[tokio::test]
    async fn test_next_minute_ag_3() {
        let time = NaiveDate::from_ymd_opt(2022, 6, 1)
            .unwrap()
            .and_hms_opt(21, 1, 0)
            .unwrap();
        test_next_minute_sub("AG", &time).await;
    }

    async fn test_is_first_minute_sub(
        breed: &str,
        trading_day: &u32,
        time: &NaiveTime,
        result: bool,
    ) {
        let is_first_minute = TxTimeRangeData::current().is_first_minute(breed, trading_day, time);
        println!("# {} {} {} {}", breed, trading_day, time, is_first_minute);
        assert_eq!(is_first_minute, result);
    }

    #[tokio::test]
    async fn test_is_first_minute() {
        init_test_mysql_pools();
        TradingDayUtil::init(&MySqlPools::pool()).await.unwrap();
        TxTimeRangeData::init(&MySqlPools::pool()).await.unwrap();
        let time = NaiveTime::from_hms_opt(9, 31, 0).unwrap();
        test_is_first_minute_sub("IC", &20220805, &time, true).await;
        let time = NaiveTime::from_hms_opt(9, 32, 0).unwrap();
        test_is_first_minute_sub("IC", &20220805, &time, false).await;

        let time = NaiveTime::from_hms_opt(9, 31, 0).unwrap();
        test_is_first_minute_sub("TF", &20220805, &time, true).await;
        let time = NaiveTime::from_hms_opt(9, 32, 0).unwrap();
        test_is_first_minute_sub("TF", &20220805, &time, false).await;

        let time = NaiveTime::from_hms_opt(9, 1, 0).unwrap();
        test_is_first_minute_sub("AP", &20220805, &time, true).await;
        let time = NaiveTime::from_hms_opt(9, 2, 0).unwrap();
        test_is_first_minute_sub("AP", &20220805, &time, false).await;

        let time = NaiveTime::from_hms_opt(9, 1, 0).unwrap();
        test_is_first_minute_sub("A", &20220805, &time, false).await;
        let time = NaiveTime::from_hms_opt(9, 2, 0).unwrap();
        test_is_first_minute_sub("A", &20220805, &time, false).await;
        let time = NaiveTime::from_hms_opt(21, 1, 0).unwrap();
        test_is_first_minute_sub("A", &20220805, &time, true).await;
        let time = NaiveTime::from_hms_opt(21, 1, 0).unwrap();
        test_is_first_minute_sub("A", &20220606, &time, false).await;
        let time = NaiveTime::from_hms_opt(9, 1, 0).unwrap();
        test_is_first_minute_sub("A", &20220606, &time, true).await;

        let time = NaiveTime::from_hms_opt(9, 1, 0).unwrap();
        test_is_first_minute_sub("AL", &20220805, &time, false).await;
        let time = NaiveTime::from_hms_opt(9, 2, 0).unwrap();
        test_is_first_minute_sub("AL", &20220805, &time, false).await;
        let time = NaiveTime::from_hms_opt(21, 1, 0).unwrap();
        test_is_first_minute_sub("AL", &20220805, &time, true).await;
        let time = NaiveTime::from_hms_opt(21, 1, 0).unwrap();
        test_is_first_minute_sub("AL", &20220606, &time, false).await;
        let time = NaiveTime::from_hms_opt(9, 1, 0).unwrap();
        test_is_first_minute_sub("AL", &20220606, &time, true).await;

        let time = NaiveTime::from_hms_opt(9, 1, 0).unwrap();
        test_is_first_minute_sub("ag", &20220805, &time, false).await;
        let time = NaiveTime::from_hms_opt(9, 2, 0).unwrap();
        test_is_first_minute_sub("ag", &20220805, &time, false).await;
        let time = NaiveTime::from_hms_opt(21, 1, 0).unwrap();
        test_is_first_minute_sub("ag", &20220805, &time, true).await;
        let time = NaiveTime::from_hms_opt(21, 1, 0).unwrap();
        test_is_first_minute_sub("ag", &20220606, &time, false).await;
        let time = NaiveTime::from_hms_opt(9, 1, 0).unwrap();
        test_is_first_minute_sub("ag", &20220606, &time, true).await;
    }

    #[test]
    fn test() {
        // 2022-08-05 02:46:01
        let datetime = NaiveDate::from_ymd_opt(2022, 8, 5)
            .unwrap()
            .and_hms_opt(2, 46, 1)
            .unwrap();
        let datetime = datetime - Duration::hours(6) - Duration::seconds(57);
        println!("{}", datetime);
    }
}
