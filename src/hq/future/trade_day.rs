use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use chrono::{Duration, NaiveDate};
use sqlx::MySqlPool;

#[derive(Debug, sqlx::FromRow)]
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
    pub td_day:    NaiveDate,
    pub td_next:   NaiveDate,
    pub td_prev:   NaiveDate,
    pub has_night: bool,
}

impl From<TradeDayDbItem> for TradeDay {
    fn from(value: TradeDayDbItem) -> Self {
        TradeDay {
            td_day:    value.td_day,
            td_next:   value.td_next,
            td_prev:   value.td_prev,
            has_night: value.night == 1,
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
    for item in trade_day_vec {
        hmap.insert(item.td_day, Arc::new(TradeDay::from(item)));
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
    trade_day_map.get(day).map_or_else(
        || {
            let mut day = *day;
            loop {
                day += Duration::days(1);
                let trade_day = trade_day_map.get(&day);
                if let Some(trade_day) = trade_day {
                    break trade_day;
                }
            }
        },
        |v| trade_day_map.get(&v.td_next).unwrap(),
    )
}

pub fn trade_day(day: &NaiveDate) -> Option<&Arc<TradeDay>> {
    TRADE_DAY_HMAP.get().unwrap().get(day)
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
}
