use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use chrono::NaiveDate;
use sqlx::MySqlPool;

#[derive(Debug, Clone, sqlx::FromRow)]
struct TradeDayDbItem {
    #[sqlx(rename = "TDday")]
    td_day:  NaiveDate,
    #[sqlx(rename = "TDNext")]
    td_next: NaiveDate,
    #[sqlx(rename = "TDREF")]
    td_prev: NaiveDate,
    #[sqlx(rename = "Night")]
    night:   i8,
}

async fn trade_days_from_db(pool: Arc<MySqlPool>) -> Result<Vec<TradeDayDbItem>, sqlx::Error> {
    let sql = "SELECT TDday,TDNext,TDREF,Night FROM basedata.tbl_calendar_data";
    let items = sqlx::query_as::<_, TradeDayDbItem>(sql)
        .fetch_all(&*pool)
        .await?;
    Ok(items)
}

#[allow(unused)]
#[derive(Debug)]
pub struct TradeDay {
    pub is_trade_day: bool,
    pub day:          NaiveDate,
    pub td_next:      NaiveDate,
    pub td_prev:      NaiveDate,
    pub has_night:    bool,
}

impl From<TradeDayDbItem> for TradeDay {
    fn from(value: TradeDayDbItem) -> Self {
        TradeDay {
            is_trade_day: true,
            day:          value.td_day,
            td_next:      value.td_next,
            td_prev:      value.td_prev,
            has_night:    value.night == 1,
        }
    }
}

static TRADE_DAY_HMAP: OnceLock<HashMap<NaiveDate, Arc<TradeDay>>> = OnceLock::new();

pub async fn init_from_db(pool: Arc<MySqlPool>) -> Result<(), sqlx::Error> {
    if TRADE_DAY_HMAP.get().is_some() {
        return Ok(());
    }
    let mut hmap = HashMap::new();
    let trade_day_vec = trade_days_from_db(pool).await?;

    let mut prev_day_info: Option<Arc<TradeDay>> = None;

    for item in trade_day_vec {
        if let Some(prev_day_info) = prev_day_info {
            for day in prev_day_info.day.succ_opt().unwrap().iter_days() {
                if day == item.td_day {
                    break;
                }
                let day_info = Arc::new(TradeDay {
                    is_trade_day: false,
                    day,
                    td_next: prev_day_info.td_next,
                    td_prev: prev_day_info.day,
                    has_night: false,
                });
                hmap.insert(day_info.day, day_info);
            }
        }

        let day_info = Arc::new(TradeDay::from(item));
        hmap.insert(day_info.day, day_info.clone());
        prev_day_info = Some(day_info)
    }

    TRADE_DAY_HMAP.set(hmap).unwrap();
    Ok(())
}

pub fn has_night(day: &NaiveDate) -> bool {
    TRADE_DAY_HMAP
        .get()
        .unwrap()
        .get(day)
        .map_or(false, |v| v.has_night)
}

/// 返回下一交易日, day是自然时间
pub fn next_trade_day(day: &NaiveDate) -> &Arc<TradeDay> {
    let trade_day_map = TRADE_DAY_HMAP.get().unwrap();
    trade_day_map
        .get(day)
        .map(|v| trade_day_map.get(&v.td_next).unwrap())
        .unwrap()
}

/// 返回时间所处的交易日
/// 非交易日, 取下一个交易日
/// 交易日, 15:15:00 之前当前交易日, 之后: 下一交易日
// pub fn trade_day_by_time(dt: &NaiveDateTime) -> &Arc<TradeDay> {

// }

/// 返回trade_day, 以目前的情况不会出现None
pub fn trade_day(day: &NaiveDate) -> &Arc<TradeDay> {
    TRADE_DAY_HMAP.get().unwrap().get(day).unwrap()
}

#[cfg(test)]
mod tests {

    use chrono::NaiveDate;

    use super::init_from_db;
    use crate::hq::future::trade_day::next_trade_day;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    #[tokio::test]
    async fn test_init_from_db() {
        init_test_mysql_pools();
        init_from_db(MySqlPools::pool()).await.unwrap();
    }

    #[tokio::test]
    async fn test_next_trade_day() {
        init_test_mysql_pools();
        init_from_db(MySqlPools::pool()).await.unwrap();
        let day = NaiveDate::from_ymd_opt(2023, 6, 21).unwrap();
        let trade_day = next_trade_day(&day);
        println!("{} {:?}", day, trade_day);
        let day = NaiveDate::from_ymd_opt(2023, 6, 29).unwrap();
        let trade_day = next_trade_day(&day);
        println!("{} {:?}", day, trade_day);
        let day = NaiveDate::from_ymd_opt(2023, 6, 30).unwrap();
        let trade_day = next_trade_day(&day);
        println!("{} {:?}", day, trade_day);
        let day = NaiveDate::from_ymd_opt(2023, 7, 1).unwrap();
        let trade_day = next_trade_day(&day);
        println!("{} {:?}", day, trade_day);
    }

    #[test]
    pub fn test_chrono() {
        let day = NaiveDate::from_ymd_opt(2023, 12, 30).unwrap();
        for (idx, day) in day.iter_days().enumerate() {
            println!("{} {}", idx, day);
            if idx > 10 {
                break;
            }
        }
    }
}
