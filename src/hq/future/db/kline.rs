use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use futures::TryStreamExt;
use rust_decimal::Decimal;
use sqlx::mysql::MySqlArguments;
use sqlx::{Arguments, MySqlPool};

use crate::mysqlx::batch_exec::SqlEntity;
use crate::mysqlx::exec::ExecError;
use crate::mysqlx::table::{table_name, TableCreator, TableExecInfo};

pub struct KLineTable;

/// 创建数据库表
impl KLineTable {
    pub async fn create_table<'a>(
        pool: Arc<MySqlPool>,
        db_name: &str,
        tbl_name: &str,
    ) -> Result<TableExecInfo, ExecError> {
        let tc = TableCreator::new(db_name, tbl_name)
            .add_field("trade_date", "date", false, "", "交易日期")
            .add_field("trade_time", "datetime", false, "", "K线时间")
            .add_field("code", "char(8)", false, "", "期货代码")
            .add_field("period", "int(8)", false, "0", "周期")
            .add_field("open", "decimal(18,4)", true, "0", "开盘价")
            .add_field("high", "decimal(18,4)", true, "0", "最高价")
            .add_field("low", "decimal(18,4)", true, "0", "最低价")
            .add_field("close", "decimal(18,4)", true, "0", "收盘价")
            .add_field("volume", "int(11)", true, "0", "成交量(手)")
            .add_field("TotalVolume", "int(11)", true, "0", "总成交量(手)")
            .add_field("amount", "decimal(30,4)", true, "0", "成交额")
            .add_field("TotalAmount", "decimal(30,4)", true, "0", "总成交额")
            .add_field("NumT", "int(5)", true, "0", "Tick数量")
            .add_field("NumK", "int(5)", true, "0", "第几根K线")
            .add_field("io", "int(11)", true, "0", "持仓量")
            .add_field("REFio", "int(11)", true, "0", "昨日持仓量")
            .add_field("REFclose", "decimal(18,4)", true, "0", "昨收")
            .add_field("OpenPrice", "decimal(18,4)", true, "0", "今开")
            .add_field("HighPrice", "decimal(18,4)", true, "0", "今日最高价")
            .add_field("LowPrice", "decimal(18,4)", true, "0", "今日最低价")
            .add_field("REFSetPrice", "decimal(18,4)", true, "0", "昨结价")
            .add_field("uplimitprice", "decimal(18,4)", true, "0", "涨停价")
            .add_field("dwlimitprice", "decimal(18,4)", true, "0", "跌停价")
            .add_field("time", "decimal(14,4)", true, "0", "trade_time的时间戳")
            .add_field(
                "update_time",
                "datetime",
                true,
                "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
                "更新时间",
            )
            .primary_keys(&["code", "trade_time", "period"]);

        tc.create(pool).await
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct KLineItem {
    #[sqlx(rename = "trade_date")]
    pub trade_date:    NaiveDate,
    #[sqlx(rename = "trade_time")]
    pub trade_time:    NaiveDateTime,
    #[sqlx(rename = "code")]
    pub code:          String,
    #[sqlx(rename = "period")]
    pub period:        i16,
    #[sqlx(rename = "open")]
    pub open:          Decimal,
    #[sqlx(rename = "high")]
    pub high:          Decimal,
    #[sqlx(rename = "low")]
    pub low:           Decimal,
    #[sqlx(rename = "close")]
    pub close:         Decimal,
    #[sqlx(rename = "volume")]
    pub volume:        i64,
    #[sqlx(rename = "TotalVolume")]
    pub total_volume:  i64,
    #[sqlx(rename = "amount")]
    pub amount:        Decimal,
    #[sqlx(rename = "TotalAmount")]
    pub total_amount:  Decimal,
    #[sqlx(rename = "io")]
    pub io:            i32,
    #[sqlx(rename = "REFio")]
    pub ref_io:        i32,
    #[sqlx(rename = "REFclose")]
    pub ref_close:     Decimal,
    #[sqlx(rename = "OpenPrice")]
    pub open_price:    Decimal,
    #[sqlx(rename = "HighPrice")]
    pub high_price:    Decimal,
    #[sqlx(rename = "LowPrice")]
    pub low_price:     Decimal,
    #[sqlx(rename = "REFSetPrice")]
    pub ref_set_price: Decimal,
    #[sqlx(rename = "uplimitprice")]
    pub uplimit_price: Decimal,
    #[sqlx(rename = "dwlimitprice")]
    pub dwlimit_price: Decimal,
    #[sqlx(rename = "NumT")]
    pub num_t:         i16,
    #[sqlx(rename = "NumK")]
    pub num_k:         i16,
    #[sqlx(rename = "time")]
    pub time:          Decimal,
}

impl KLineItem {
    const REPLACE_INTO_SQL_TEMPLATE: &str = "REPLACE INTO {{table_name}}(trade_date,trade_time,code,period,open,high,low,close,volume,TotalVolume,amount,TotalAmount,io,REFio,REFclose,OpenPrice,HighPrice,LowPrice,REFSetPrice,uplimitprice,dwlimitprice,NumT,NumK,time) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    pub fn sql_entity_replace(&self, key: &str, db: &str, tbl_name: &str) -> SqlEntity {
        let table_name = &table_name(db, tbl_name);
        let sql = Self::REPLACE_INTO_SQL_TEMPLATE.replace("{{table_name}}", table_name);
        let mut args = MySqlArguments::default();
        args.add(self.trade_date);
        args.add(self.trade_time);
        args.add(&self.code);
        args.add(self.period);
        args.add(self.open);
        args.add(self.high);
        args.add(self.low);
        args.add(self.close);
        args.add(self.volume);
        args.add(self.total_volume);
        args.add(self.amount);
        args.add(self.total_amount);
        args.add(self.io);
        args.add(self.ref_io);
        args.add(self.ref_close);
        args.add(self.open_price);
        args.add(self.high_price);
        args.add(self.low_price);
        args.add(self.ref_set_price);
        args.add(self.uplimit_price);
        args.add(self.dwlimit_price);
        args.add(self.num_t);
        args.add(self.num_k);
        args.add(self.time);
        SqlEntity::new(key, &sql, args)
    }
}

const KLINE_ITEM_VEC_LATEST_BY_SYMBOL_SQL_TEMPLATE: &str =
"SELECT * FROM (SELECT trade_date,trade_time,code,period,open,high,low,close,volume,TotalVolume,amount,TotalAmount,io,REFio,REFclose,OpenPrice,HighPrice,LowPrice,REFSetPrice,uplimitprice,dwlimitprice,NumT,NumK,time FROM {{table_name}} WHERE code=? AND period=? ORDER BY trade_time DESC LIMIT ?) AS T ORDER BY trade_time";

/// 获取某一合约的最新的数据列表, 时间正序.
pub async fn item_vec_latest_by_symbol(
    pool: &MySqlPool,
    db: &str,
    tbl_name: &str,
    contract: &str,
    period: u16,
    limit: u16,
) -> Result<Vec<KLineItem>, sqlx::Error> {
    let table_name = table_name(db, tbl_name);
    let sql = KLINE_ITEM_VEC_LATEST_BY_SYMBOL_SQL_TEMPLATE.replace("{{table_name}}", &table_name);

    let mut args = MySqlArguments::default();
    args.add(contract);
    args.add(period);
    args.add(limit);

    sqlx::query_as_with::<_, KLineItem, _>(&sql, args)
        .fetch(pool)
        // .map(Self::item_breed_from_symbol)
        .try_collect()
        .await
}
