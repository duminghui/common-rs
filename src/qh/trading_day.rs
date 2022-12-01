use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{Duration, NaiveDate, NaiveDateTime, Timelike};
use futures::TryStreamExt;
use lazy_static::lazy_static;
use sqlx::{FromRow, MySqlPool};

use super::klinetime::KLineTimeError;
use crate::ymdhms::Ymd;

lazy_static! {
    static ref TRADING_DAY_UTIL: RwLock<Arc<TradingDayUtil>> = RwLock::new(Default::default());
    // static ref TRADING_DAY_UTIL: RwLock<TradingDayUtil> = RwLock::new(Default::default());
    // static ref TRADING_DAY_UTIL2: &'static mut TradingDayUtil =
    //     TradingDayUtil::new_ref_static_mut();
}

// cannot call non-const fn <Arc<TradingDayUtilInner> as Default>::default in statics calls in statics are limited to constant functions
// static TRADING_DAY_UTIL: RwLock<Arc<TradingDayUtilInner>> = RwLock::new(Default::default());

#[derive(FromRow)]
struct TradingDayDbItem {
    trading_day: i32, // 数据库解析要求i32
}

impl From<TradingDayDbItem> for Ymd {
    fn from(item: TradingDayDbItem) -> Self {
        let TradingDayDbItem { trading_day } = item;
        Ymd::from_yyyymmdd(trading_day as u32)
    }
}

// impl Extend<TradingDayDbItem> for Vec<TradingDay> {
//     fn extend<T: IntoIterator<Item = TradingDayDbItem>>(&mut self, iter: T) {
//         for t in iter {
//             self.push(t.into());
//         }
//     }
// }
//

#[derive(Debug)]
struct DayInfo {
    is_td: bool,     // 是否交易日
    prev_idx: usize, // 前一交易日index
    idx: usize,      /* 非交易日:所属交易日的index,和next_idx相同. 交易日:在列表中的index */
    next_idx: usize, // 下一交易日index
    has_night: bool, // 是否有夜盘
}

#[derive(thiserror::Error, Debug)]
pub enum TradingDayUtilInitError {
    #[error("{0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("database record is empty")]
    Empty,
}

#[derive(Debug, Default)]
pub struct TradingDayUtil {
    td_vec: Vec<Ymd>,                    // 交易日列表
    day_info_map: HashMap<u32, DayInfo>, // day, idx
}

impl TradingDayUtil {
    pub fn current() -> Arc<TradingDayUtil> {
        TRADING_DAY_UTIL.read().unwrap().clone()
    }

    // pub fn current() -> RwLockReadGuard<'static, TradingDayUtil> {
    //     TRADING_DAY_UTIL.read().unwrap()
    // }

    // 不能用, 不知道在使用lazy_static的情况下怎么调用&mut self的方法
    // fn new_ref_static_mut() -> &'static mut TradingDayUtil {
    //     Box::leak(Box::new(TradingDayUtil::default()))
    // }

    pub async fn init(pool: &MySqlPool) -> Result<(), TradingDayUtilInitError> {
        if !Self::current().td_vec.is_empty() {
            return Ok(());
        }
        let mut new_inner = TradingDayUtil::default();
        new_inner.init_from_db(pool).await?;
        *TRADING_DAY_UTIL.write().unwrap() = Arc::new(new_inner);
        Ok(())
    }

    // pub async fn init(pool: &MySqlPool) -> Result<(), TradingDayUtilInitError> {
    //     TRADING_DAY_UTIL.write().unwrap().init_from_db(pool).await
    // }

    async fn init_from_db(&mut self, pool: &MySqlPool) -> Result<(), TradingDayUtilInitError> {
        let sql = "SELECT trading_day FROM `hqdb`.`tbl_ths_trading_day` ORDER BY trading_day";
        let mut db_rows = sqlx::query_as::<_, TradingDayDbItem>(sql).fetch(pool);
        let mut td_vec: Vec<Ymd> = Vec::new();

        let mut day_idx_map: HashMap<u32, DayInfo> = HashMap::new();
        let mut prev_idx = 0;
        let mut idx = 0;
        let mut prev_date = None;
        while let Some(db_item) = db_rows.try_next().await? {
            let td = Ymd::from(db_item);
            td_vec.push(td);

            let date = NaiveDate::from(&td);

            let has_night = if let Some(prev_date) = prev_date {
                // 有夜盘的情况
                // 相差一天, 两个交易日是紧挨着的
                // 相差三天, 两个交易日隔了二天, 中间两天可能是周六天, 也可能是节假日, 目前的条件没办法判断具体的情况, 先按周六天的情况来处理
                //
                // 无夜盘的情况
                // 相差两天, 两个交易日隔了一天, 中间一天是节假日
                // 相差大于三天, 中间是节假日
                let diff = date - prev_date;
                diff == Duration::days(1) || diff == Duration::days(3)
            } else {
                // 如果没有前一个交易日的数据, 则默认为有夜盘
                true
            };

            let day_info = DayInfo {
                is_td: true,
                prev_idx,
                idx,
                next_idx: idx + 1,
                has_night,
            };
            day_idx_map.insert(td.yyyymmdd, day_info);
            prev_idx = idx;
            prev_date = Some(date);
            idx += 1;
        }
        if td_vec.is_empty() {
            return Err(TradingDayUtilInitError::Empty);
        }

        let mut date = NaiveDate::from(td_vec.first().unwrap());

        let end_date = NaiveDate::from(td_vec.last().unwrap());

        let mut idx = 0;
        // 补充非交易日的数据
        while date < end_date {
            let yyyymmdd = Ymd::from(&date).yyyymmdd;
            day_idx_map.entry(yyyymmdd).or_insert_with(|| {
                idx -= 1;
                let n_idx = idx + 1;
                DayInfo {
                    is_td: false,
                    prev_idx: idx,
                    idx: n_idx,
                    next_idx: n_idx,
                    has_night: false,
                }
            });
            idx += 1;
            date += Duration::days(1);
        }
        self.td_vec = td_vec;
        self.day_info_map = day_idx_map;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.td_vec.is_empty()
    }

    pub fn is_td(&self, day: &u32) -> bool {
        self.day_info_map.get(day).map_or(false, |v| v.is_td)
    }

    // pub fn is_td_by_date(&self, date: &impl Datelike) -> bool {
    //     self.is_td(&Ymd::from(date).yyyymmdd)
    // }

    pub fn prev(&self, day: &u32) -> Result<&Ymd, KLineTimeError> {
        self.day_info_map
            .get(day)
            .and_then(|v| {
                if v.idx == 0 {
                    None
                } else {
                    self.td_vec.get(v.prev_idx)
                    // let v = self.td_vec.get(v.prev_idx);
                    // println!("# prev  : &:{:p}, unwrap:{:p}", &v, v.unwrap());
                    // v
                }
            })
            .ok_or(KLineTimeError::PrevTradingDay(*day))
    }

    // 仅用于测试
    #[allow(unused)]
    fn prev_slow(&self, day: &u32) -> Result<&Ymd, KLineTimeError> {
        let mut p_td = Err(KLineTimeError::PrevTradingDay(*day));
        for td in self.td_vec.iter() {
            if td.yyyymmdd >= *day {
                return p_td;
            }
            p_td = Ok(td)
        }
        Err(KLineTimeError::PrevTradingDay(*day))
    }

    // pub fn next_by_date(&self, date: &impl Datelike) -> Result<&Ymd, KLineTimeError> {
    //     self.next(&Ymd::from(date).yyyymmdd)
    // }

    /// day: 自然时间, 包括非交易日 格式:20220607,
    pub fn next(&self, day: &u32) -> Result<&Ymd, KLineTimeError> {
        self.day_info_map
            .get(day)
            .and_then(|v| self.td_vec.get(v.next_idx))
            .ok_or(KLineTimeError::NextTradingDay(*day))
    }

    // 仅用于测试
    #[allow(unused)]
    fn next_slow(&self, day: &u32) -> Result<&Ymd, KLineTimeError> {
        for td in &self.td_vec {
            if td.yyyymmdd > *day {
                return Ok(td);
            }
        }
        Err(KLineTimeError::NextTradingDay(*day))
    }

    // 获取自然时间所属交易日, 白盘直接返回yyyymmdd, 夜盘:21点后返回下一交易日, 3点前返回前一交易日的下一交易日
    pub fn trading_day_from_datetime(
        &self,
        datetime: &NaiveDateTime,
    ) -> Result<Ymd, KLineTimeError> {
        let ymd = Ymd::from(datetime);
        let hh = datetime.hour();
        if (9..=15).contains(&hh) {
            Ok(ymd)
        } else if hh >= 21 {
            Ok(*self.next(&ymd.yyyymmdd)?)
        } else if hh <= 2 {
            let prev_td = self.prev(&ymd.yyyymmdd)?;
            Ok(*self.next(&prev_td.yyyymmdd)?)
        } else {
            Err(KLineTimeError::DatetimeNotSupport(*datetime))
        }
    }

    /// 一个自然日对应的夜盘开始交易日及收盘交易日
    pub fn start_end_day(&self, day: &u32) -> Option<(&Ymd, &Ymd)> {
        self.day_info_map
            .get(day)
            .and_then(|v| self.td_vec.get(v.prev_idx).zip(self.td_vec.get(v.idx)))
    }

    pub fn has_night(&self, trading_day: &u32) -> bool {
        self.day_info_map
            .get(trading_day)
            .map_or(false, |v| v.has_night)
    }
}

// pub struct TradingDayUtilOut;

// impl TradingDayUtilOut {
//     pub async fn init(pool: &MySqlPool) -> Result<(), TradingDayUtilInitError> {
//         let mut new_inner = TradingDayUtil::default();
//         new_inner.init_from_db(pool).await?;
//         *TRADING_DAY_UTIL.write().unwrap() = Arc::new(new_inner);
//         Ok(())
//     }

//     pub fn is_td(day: &u32) -> bool {
//         TRADING_DAY_UTIL.read().unwrap().clone().is_td(day)
//     }

//     pub fn prev(day: &u32) -> Option<&'_ TradingDay> {
//         TRADING_DAY_UTIL
//             .read()
//             .unwrap()
//             .clone()
//             .prev(day)
//             .and_then(|v| {
//                 let ptr = v as *const TradingDay;
//                 // println!("#2: {:p}", v);
//                 // println!("#3: {:p}", ptr);
//                 unsafe { ptr.as_ref() }
//             })
//     }

//     pub fn next(day: &u32) -> Option<&'_ TradingDay> {
//         TRADING_DAY_UTIL
//             .read()
//             .unwrap()
//             .clone()
//             .next(day)
//             .and_then(|v| {
//                 let ptr = v as *const TradingDay;
//                 unsafe { ptr.as_ref() }
//             })
//     }

//     pub fn start_end_day(day: &u32) -> Option<(&TradingDay, &TradingDay)> {
//         TRADING_DAY_UTIL
//             .read()
//             .unwrap()
//             .clone()
//             .start_end_day(day)
//             .and_then(|v| {
//                 let (v1, v2) = v;
//                 let ptr1 = v1 as *const TradingDay;
//                 let ptr2 = v2 as *const TradingDay;
//                 unsafe { ptr1.as_ref().zip(ptr2.as_ref()) }
//             })
//     }
// }

// use std::sync::RwLock as StdRwLock;

// #[allow(unused)]
// #[derive(Default)]
// struct Config {
//     pub debug_mode: bool,
// }

// impl Config {
//     #[allow(unused)]
//     fn current() -> Arc<Config> {
//         CURRENT_CONFIG.read().unwrap().clone()
//     }
//     #[allow(unused)]
//     fn make_current(self) {
//         *CURRENT_CONFIG.write().unwrap() = Arc::new(self);
//     }
// }

// lazy_static! {
//     static ref CURRENT_CONFIG: StdRwLock<Arc<Config>> = StdRwLock::new(Default::default());
// }

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::{Duration, NaiveDate};

    use super::TradingDayUtil;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;
    use crate::ymdhms::Ymd;

    #[tokio::test]
    async fn test_start_end_day() {
        init_test_mysql_pools();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();
        let tdu = TradingDayUtil::current();
        let r = tdu.start_end_day(&20220611);
        println!("#1: {:?}", r);
        assert_eq!(r.unwrap().0.yyyymmdd, 20220610);
        assert_eq!(r.unwrap().1.yyyymmdd, 20220613);
        let r = tdu.start_end_day(&0);
        println!("#2: {:?}", r); // None
    }

    #[tokio::test]
    async fn test_trading_day_next() {
        let mut result_hmap = HashMap::new();

        result_hmap.insert(20220607, 20220608);
        result_hmap.insert(20220608, 20220609);
        result_hmap.insert(20220609, 20220610);
        result_hmap.insert(20220610, 20220613);
        result_hmap.insert(20220611, 20220613);
        result_hmap.insert(20220612, 20220613);
        result_hmap.insert(20220613, 20220614);
        result_hmap.insert(20220614, 20220615);

        let arc_hmap = Arc::new(result_hmap);

        init_test_mysql_pools();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let mut handles = Vec::with_capacity(10);

        for i in 20220607..=20220614 {
            let arc_hamp = arc_hmap.clone();
            handles.push(tokio::spawn(async move {
                let tdu = TradingDayUtil::current();
                let result = arc_hamp.get(&i).unwrap();
                let n_td = tdu.next(&i);
                let n_td = n_td.unwrap();
                println!("day:{}, {:?}, {}", i, n_td, &n_td.yyyymmdd == result);
                assert_eq!(&n_td.yyyymmdd, result);
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_trading_day_prev_2() {
        init_test_mysql_pools();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();
        let tdu = TradingDayUtil::current();
        let a = tdu.prev(&20220607);
        // 这块打印出来的unwrap地址要和prev方法中打印出来的地址一样.
        println!("# test 1: a: &:{:p}", &a);
        let ymd = a.unwrap();
        println!("# test 2: unwrap:{:p}", ymd);
        assert_eq!(ymd.yyyymmdd, 20220606);
        let a = tdu.prev(&19700101);
        println!("# test 2: &:{:p} {:?}", &a, a);
    }

    #[tokio::test]
    async fn test_trading_day_prev() {
        let mut result_hmap = HashMap::new();

        result_hmap.insert(20220607, 20220606);
        result_hmap.insert(20220608, 20220607);
        result_hmap.insert(20220609, 20220608);
        result_hmap.insert(20220610, 20220609);
        result_hmap.insert(20220611, 20220610);
        result_hmap.insert(20220612, 20220610);
        result_hmap.insert(20220613, 20220610);
        result_hmap.insert(20220614, 20220613);

        let arc_hmap = Arc::new(result_hmap);

        init_test_mysql_pools();
        TradingDayUtil::init(&MySqlPools::default()).await.unwrap();

        let mut handles = Vec::with_capacity(10);

        for i in 20220607..=20220614 {
            let arc_hamp = arc_hmap.clone();
            handles.push(tokio::spawn(async move {
                let tdu = TradingDayUtil::current();
                let result = arc_hamp.get(&i).unwrap();
                let n_td = tdu.prev(&i);
                let n_td = n_td.unwrap();
                println!("day:{}, {:?}, {}", i, n_td, &n_td.yyyymmdd == result);
                assert_eq!(&n_td.yyyymmdd, result);
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_trading_day_util_init() {
        let mut handles = Vec::with_capacity(10);

        init_test_mysql_pools();
        TradingDayUtil::init(&*MySqlPools::default()).await.unwrap();
        let tdu = TradingDayUtil::current();
        let len = tdu.td_vec.len();
        println!("td size: {}", len);

        for _ in 0..10 {
            handles.push(tokio::spawn(async move {
                let tdu = TradingDayUtil::current();
                let mut date = NaiveDate::from_ymd_opt(2017, 1, 3).unwrap();
                let e_date = NaiveDate::from_ymd_opt(2022, 12, 30).unwrap();
                while date <= e_date {
                    let yyyymmdd = Ymd::from(&date).yyyymmdd;

                    let a_p_td = tdu.prev(&yyyymmdd);
                    let b_p_td = tdu.prev_slow(&yyyymmdd);
                    if a_p_td.is_ok() && b_p_td.is_ok() {
                        assert_eq!(a_p_td.unwrap().yyyymmdd, b_p_td.unwrap().yyyymmdd)
                    } else if a_p_td.is_err() && b_p_td.is_ok() {
                        println!("Prev Error 1: {}", yyyymmdd);
                    } else if a_p_td.is_ok() && b_p_td.is_err() {
                        println!("Prev Error 2: {}", yyyymmdd);
                    }

                    let a_n_td = tdu.next(&yyyymmdd);
                    let b_n_td = tdu.next_slow(&yyyymmdd);
                    if a_n_td.is_ok() && b_n_td.is_ok() {
                        assert_eq!(a_n_td.unwrap().yyyymmdd, b_n_td.unwrap().yyyymmdd)
                    } else if a_n_td.is_err() && b_n_td.is_ok() {
                        println!("Next Error 1: {}", yyyymmdd);
                    } else if a_n_td.is_ok() && b_n_td.is_err() {
                        println!("Next Error 2: {}", yyyymmdd);
                    }

                    date = date + Duration::days(1);
                }
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        println!("# End");
    }

    #[tokio::test]
    async fn test_has_night() {
        init_test_mysql_pools();
        TradingDayUtil::init(&*MySqlPools::default()).await.unwrap();
        let tdu = TradingDayUtil::current();
        let date = 20220602;
        println!("{} {}", date, tdu.has_night(&date));
        assert_eq!(true, tdu.has_night(&date));
        let date = 20220606;
        println!("{} {}", date, tdu.has_night(&date));
        assert_eq!(false, tdu.has_night(&date));
    }

    #[tokio::test]
    async fn test_trading_day_from_datetime() {
        init_test_mysql_pools();
        TradingDayUtil::init(&*MySqlPools::default()).await.unwrap();
        let tdu = TradingDayUtil::current();
        for day in 6..=9 {
            let datetime = NaiveDate::from_ymd_opt(2022, 8, day)
                .unwrap()
                .and_hms_opt(2, 0, 0)
                .unwrap();
            let td = tdu.trading_day_from_datetime(&datetime).unwrap();
            println!("{} {:?}", datetime, td);
        }
    }

    // #[test]
    // fn test_thread_local() {
    //     Config { debug_mode: true }.make_current();
    //     println!("##: {}", Config::current().debug_mode);
    //     let rt = Runtime::new().unwrap();
    //     rt.block_on(async {
    //         let mut handles = Vec::with_capacity(10);
    //         for i in 0..10 {
    //             handles.push(tokio::spawn(async move {
    //                 // TRADING_DAY_UTIL
    //                 //     .write()
    //                 //     .unwrap()
    //                 //     .init(&MySqlPools::current())
    //                 //     .await
    //                 //     .unwrap();

    //                 let mode = Config::current().debug_mode;
    //                 println!("this is number: {}, {}", i, mode)
    //             }))
    //         }
    //         for handle in handles {
    //             handle.await.unwrap();
    //         }
    //     });
    // }

    #[tokio::test]
    async fn test_tokio() {
        let mut handles = Vec::with_capacity(10);
        for i in 0..10 {
            handles.push(tokio::spawn(async move {
                println!("this is number: {}", i);
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        // 下面的可以不用
        // std::thread::sleep(std::time::Duration::from_millis(1000));
    }

    #[test]
    fn test_vec() {
        let vec = vec![1, 2, 3];
        // println!("{:?}", vec.get_unchecked(-1));
        let r = unsafe { vec.get_unchecked(2) };
        println!("{:?}", r);
        let r = vec.get(4);
        println!("{:?}", r);
    }

    #[test]
    fn test_naive_date() {
        let mut date = NaiveDate::from_ymd_opt(2022, 6, 8).unwrap();
        for _ in 0..=30 {
            date = date + Duration::days(1);
            println!("# {:?}", date.format("%Y-%m-%d").to_string());
        }
        let date = NaiveDate::from_ymd_opt(2022, 6, 8).unwrap();
        let date_str = date.format("%Y-%m-%d").to_string();
        let date2 = NaiveDate::from_ymd_opt(2022, 6, 8).unwrap();
        let date2_str = date2.format("%Y-%m-%d").to_string();
        let date3 = NaiveDate::from_ymd_opt(2022, 6, 9).unwrap();
        let date3_str = date3.format("%Y-%m-%d").to_string();

        println!("{}=={} {}", date_str, date2_str, date == date2);
        println!("{}> {} {}", date_str, date2_str, date > date2);
        println!("{}>={} {}", date_str, date2_str, date >= date2);
        println!("{}> {} {}", date_str, date3_str, date > date3);
        println!("{}>={} {}", date_str, date3_str, date >= date3);
        println!("{}< {} {}", date_str, date3_str, date < date3);
        println!("{}<={} {}", date_str, date3_str, date <= date3);
    }
}
