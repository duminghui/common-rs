use std::collections::HashMap;
use std::sync::OnceLock;

use chrono::NaiveDate;
use sqlx::MySqlPool;

#[derive(Debug, sqlx::FromRow)]
struct TradeDayDbItem {
    #[sqlx(rename = "TDday")]
    td_day:  NaiveDate,
    #[sqlx(rename = "TDNext")]
    td_next: NaiveDate,
    #[sqlx(rename = "Night")]
    night:   i8,
}

async fn trade_days_from_db(pool: &MySqlPool) -> Result<Vec<TradeDayDbItem>, sqlx::Error> {
    let sql = "SELECT TDday,TDNext, Night FROM basedata.tbl_calendar_data";
    let items = sqlx::query_as::<_, TradeDayDbItem>(sql)
        .fetch_all(pool)
        .await?;
    Ok(items)
}

#[allow(unused)]
#[derive(Debug)]
pub struct TradeDay {
    td_day:        NaiveDate,
    pub td_next:   NaiveDate,
    pub has_night: bool,
}

impl From<TradeDayDbItem> for TradeDay {
    fn from(value: TradeDayDbItem) -> Self {
        TradeDay {
            td_day:    value.td_day,
            td_next:   value.td_next,
            has_night: value.night == 1,
        }
    }
}

static TRADE_DAY_HMAP: OnceLock<HashMap<NaiveDate, TradeDay>> = OnceLock::new();

pub async fn init_from_db(pool: &MySqlPool) -> Result<(), sqlx::Error> {
    if TRADE_DAY_HMAP.get().is_some() {
        return Ok(());
    }
    let mut hmap = HashMap::new();
    let trade_day_vec = trade_days_from_db(pool).await?;
    for item in trade_day_vec {
        hmap.insert(item.td_day, TradeDay::from(item));
    }
    TRADE_DAY_HMAP.set(hmap).unwrap();
    Ok(())
}

pub fn has_night(td: &NaiveDate) -> bool {
    TRADE_DAY_HMAP
        .get()
        .unwrap()
        .get(td)
        .map_or(false, |v| v.has_night)
}

pub fn next_td(td: &NaiveDate) -> NaiveDate {
    TRADE_DAY_HMAP
        .get()
        .unwrap()
        .get(td)
        .map(|v| v.td_next)
        .unwrap()
}

pub fn trade_day(td: &NaiveDate) -> Option<&TradeDay> {
    TRADE_DAY_HMAP.get().unwrap().get(td)
}

#[cfg(test)]
mod tests {
    use super::init_from_db;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    #[tokio::test]
    async fn test_init_from_db() {
        init_test_mysql_pools();
        init_from_db(&MySqlPools::pool()).await.unwrap();
    }
}
