use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use chrono::NaiveDateTime;
use futures_util::{StreamExt, TryStreamExt};
use rust_decimal::Decimal;
use sqlx::mysql::MySqlArguments;
use sqlx::{Arguments, MySqlPool};

use super::breed;
use crate::mysqlx::batch_exec::SqlEntity;

#[derive(Debug, sqlx::FromRow, Clone)]
pub struct KLineItem {
    // #[sqlx(default)]
    // pub breed:          String,
    #[sqlx(rename = "code")]
    pub code:           String,
    pub datetime:       NaiveDateTime,
    pub period:         i32,
    pub open:           Decimal,
    pub high:           Decimal,
    pub low:            Decimal,
    pub close:          Decimal,
    pub volume:         i64,
    pub total_volume:   i64,
    pub open_oi:        i64,
    pub close_oi:       i64,
    pub last_item_time: NaiveDateTime,
}

impl std::fmt::Display for KLineItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{},|{}|,{:>3},{},{},{},{},v:{},tv:{},{},{},|{}|",
            self.code,
            self.datetime.format("%Y-%m-%d %H:%M:%S"),
            self.period,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.total_volume,
            self.open_oi,
            self.close_oi,
            self.last_item_time.format("%F %T%.3f"),
        ))
    }
}

impl KLineItem {
    const KLINE_ITEM_REPLACE_INTO_SQL_TEMPLATE: &'static str = "REPLACE INTO {{table_name}}(code,datetime,period,open,high,low,close,volume,total_volume,open_oi,close_oi,last_item_time) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";

    pub fn new(code: &str, datetime: &NaiveDateTime, period: i32) -> KLineItem {
        KLineItem {
            code: code.to_owned(),
            datetime: datetime.to_owned(),
            period,
            open: Default::default(),
            high: Default::default(),
            low: Default::default(),
            close: Default::default(),
            volume: 0,
            total_volume: 0,
            open_oi: 0,
            close_oi: 0,
            last_item_time: datetime.to_owned(),
        }
    }

    pub fn breed(&self) -> String {
        breed::breed_from_symbol(&self.code)
    }

    pub fn sql_entity_replace(&self, key: &str, table_name: &str) -> SqlEntity {
        let sql = Self::KLINE_ITEM_REPLACE_INTO_SQL_TEMPLATE.replace("{{table_name}}", table_name);
        let mut args = MySqlArguments::default();
        args.add(&self.code);
        args.add(self.datetime);
        args.add(self.period);
        args.add(self.open);
        args.add(self.high);
        args.add(self.low);
        args.add(self.close);
        args.add(self.volume);
        args.add(self.total_volume);
        args.add(self.open_oi);
        args.add(self.close_oi);
        args.add(self.last_item_time);
        SqlEntity::new(key, &sql, args)
    }
}

static KLINE_ITEM_UTILS: OnceLock<KLineItemUtils> = OnceLock::new();

#[derive(Debug, Default)]
pub struct KLineItemUtils {
    default:   Option<Arc<KLineItemUtil>>,
    util_hmap: HashMap<String, Arc<KLineItemUtil>>,
}

impl KLineItemUtils {
    pub fn init_one_util(db: &str, default: bool) {
        let mut klius = KLineItemUtils::default();
        let util = Arc::new(KLineItemUtil::new(db));
        if default {
            klius.default = Some(util.clone());
        }
        klius.util_hmap.insert(db.to_owned(), util);
        KLINE_ITEM_UTILS.set(klius).unwrap();
    }

    pub fn util() -> Arc<KLineItemUtil> {
        KLINE_ITEM_UTILS
            .get()
            .unwrap()
            .default
            .as_ref()
            .unwrap()
            .clone()
    }

    // 通过key获取util, key=db-suffix
    pub fn by_key(key: &str) -> Arc<KLineItemUtil> {
        let utils = KLINE_ITEM_UTILS.get().unwrap();
        utils.util_hmap.get(key).unwrap().clone()
    }
}

#[derive(Debug)]
pub struct KLineItemUtil {
    tbl_tmpl: String,
}

impl KLineItemUtil {
    pub fn new(db: &str) -> KLineItemUtil {
        let tbl_tmpl = if db.is_empty() {
            "`tbl_code_{{tbl_suffix}}`".to_owned()
        } else {
            format!("`{}`.`tbl_code_{{{{tbl_suffix}}}}`", db)
        };
        KLineItemUtil { tbl_tmpl }
    }

    fn table_name(&self, tbl_suffix: &str) -> String {
        self.tbl_tmpl.replace("{{tbl_suffix}}", tbl_suffix)
    }

    // 这块的代码用不到了.
    // const RENAME_SQL_TEMPLATE: &'static str =
    //     r#"RENAME TABLE {{from_table_name}} TO {{to_table_name}}"#;

    // pub fn table_rename(&self, breed: &str, dest_db: &str, new_suffix: &str) {
    //     let from_table_name = self.table_name(breed);
    //     let to_table_name = Self::table_name_with_suffix(dest_db, breed, new_suffix);
    //     let sql = Self::RENAME_SQL_TEMPLATE.replace("{{from_table_name}}", &from_table_name);
    //     let sql = sql.replace("{{to_table_name}}", &to_table_name);
    //     println!("{}", sql);
    // }

    // 没必要在读取后就计算breed, 在用的时候再计算.
    // fn item_breed_from_symbol(
    //     mut v: Result<KLineItem, sqlx::Error>,
    // ) -> Result<KLineItem, sqlx::Error> {
    //     if let Ok(mut item) = v.as_mut() {
    //         item.breed = breed::breed_from_symbol(&item.code);
    //     }
    //     v
    // }
}

/// 数据添加相关
impl KLineItemUtil {
    pub fn sql_entity_replace(&self, tbl_suffix: &str, key: &str, item: &KLineItem) -> SqlEntity {
        item.sql_entity_replace(key, &self.table_name(tbl_suffix))
    }
}

/// 创建数据库表
impl KLineItemUtil {
    const KLINE_TABLE_CREATE_SQL_TEMPLAGE: &'static str = r#"
    CREATE TABLE IF NOT EXISTS {{table_name}} (
        `code` varchar(12) DEFAULT '' COMMENT '主力合约',
        `datetime` datetime NOT NULL COMMENT '时间戳，精确到秒',
        `period` int(11) NOT NULL COMMENT '分钟周期.1表示1分钟,5表示5分钟,30表示30分钟',
        `open` decimal(18,3) DEFAULT '0.000' COMMENT '开盘价',
        `high` decimal(18,3) DEFAULT '0.000' COMMENT '最高',
        `low` decimal(18,3) DEFAULT '0.000' COMMENT '最低',
        `close` decimal(18,3) DEFAULT '0.000' COMMENT '收盘价',
        `volume` int(11) DEFAULT '0' COMMENT '成交量',
        `total_volume` int(11) DEFAULT '0' COMMENT '总成交量',
        `open_oi` int(11) DEFAULT '0' COMMENT 'K线起始时的持仓量',
        `close_oi` int(11) DEFAULT '0' COMMENT 'K线结束时的持仓量',
        `last_item_time` datetime(6) COMMENT '计算K线数据的最后一条数据的时间, 1分钟数据是最后一条Tick的时间, 来源于文件的数据用当前时间',
        `update_time` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新时间',
        PRIMARY KEY (`code`, `datetime`, `period`),
        INDEX(`period`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    "#;

    pub async fn create_table(
        &self,
        pool: &MySqlPool,
        tbl_suffix: &str,
    ) -> Result<String, sqlx::Error> {
        let table_name = self.table_name(tbl_suffix);
        let sql = Self::KLINE_TABLE_CREATE_SQL_TEMPLAGE.replace("{{table_name}}", &table_name);
        sqlx::query(&sql).execute::<_>(pool).await?;
        Ok(table_name)
    }
}

/// 列表相关的操作
impl KLineItemUtil {
    const KLINE_ITEM_VEC_LATEST_BY_SYMBOL_SQL_TEMPLATE: &'static str =
        "SELECT * FROM (SELECT code,datetime,period,open,high,low,close,volume,total_volume,open_oi,close_oi,last_item_time FROM {{table_name}} WHERE code=? AND period=? ORDER BY datetime DESC LIMIT ?) AS T ORDER BY datetime";
    const KLINE_ITEM_VEC_LATEST_SQL_TEMPLATE: &'static str =
        "SELECT * FROM (SELECT code,datetime,period,open,high,low,close,volume,total_volume,open_oi,close_oi,last_item_time FROM {{table_name}} WHERE period=? ORDER BY datetime DESC LIMIT ?) AS T ORDER BY datetime";
    const KLINE_ITEM_VEC_OLDEST_SQL_TEMPLATE: &'static str =
        "SELECT code,datetime,period,open,high,low,close,volume,total_volume,open_oi,close_oi,last_item_time FROM {{table_name}} WHERE period=? ORDER BY datetime LIMIT ?";
    const KLINE_ITEM_VEC_RANGE_SQL_TEMPLATE: &'static str =
        "SELECT code,datetime,period,open,high,low,close,volume,total_volume,open_oi,close_oi,last_item_time FROM {{table_name}} WHERE datetime>=? AND datetime <=? AND period=? ORDER BY datetime LIMIT ?";
    const KLINE_ITEM_VEC_SQL_TEMPLATE: &'static str =
        "SELECT code,datetime,period,open,high,low,close,volume,total_volume,open_oi,close_oi,last_item_time FROM {{table_name}} WHERE datetime>=? AND period=? ORDER BY datetime LIMIT ?";

    /// 大于等于某一时间点的数据列表, 结果按时间正序排序
    pub async fn item_vec_egt_dt(
        &self,
        pool: &MySqlPool,
        tbl_suffix: &str,
        period: u16,
        datetime: &str,
        limit: u16,
    ) -> Result<Vec<KLineItem>, sqlx::Error> {
        let table_name = self.table_name(tbl_suffix);
        let sql = Self::KLINE_ITEM_VEC_SQL_TEMPLATE.replace("{{table_name}}", &table_name);

        let mut args = MySqlArguments::default();
        args.add(datetime);
        args.add(period);
        args.add(limit);

        sqlx::query_as_with::<_, KLineItem, _>(&sql, args)
            .fetch(pool)
            // .map(|mut v| {
            //     if let Ok(mut item) = v.as_mut() {
            //         item.breed = breed::breed_from_symbol(&item.code)
            //     }
            //     v
            // })
            // .map(Self::item_breed_from_symbol)
            .try_collect()
            .await

        // let stream = sqlx::query_as_with::<_, KLineItem, _>(&sql, args)
        //     .fetch_all(pool)
        //     .await;

        // stream
    }

    pub async fn item_vec_egt_dt_by_datetime(
        &self,
        pool: &MySqlPool,
        tbl_suffix: &str,
        period: u16,
        datetime: &NaiveDateTime,
        limit: u16,
    ) -> Result<Vec<KLineItem>, sqlx::Error> {
        self.item_vec_egt_dt(
            pool,
            tbl_suffix,
            period,
            &datetime.format("%Y-%m-%d %H:%M:%S").to_string(),
            limit,
        )
        .await
    }

    /// 时间范围内的数据列表, 时间正序
    pub async fn item_vec_range(
        &self,
        pool: &MySqlPool,
        tbl_suffix: &str,
        period: u16,
        sdatetime: &str,
        edatetime: &str,
        limit: u16,
    ) -> Result<Vec<KLineItem>, sqlx::Error> {
        let table_name = self.table_name(tbl_suffix);
        let sql = Self::KLINE_ITEM_VEC_RANGE_SQL_TEMPLATE.replace("{{table_name}}", &table_name);
        let mut args = MySqlArguments::default();
        args.add(sdatetime);
        args.add(edatetime);
        args.add(period);
        args.add(limit);

        sqlx::query_as_with::<_, KLineItem, _>(&sql, args)
            .fetch(pool)
            .try_collect()
            .await
    }

    /// 时间范围内的数据列表, 时间正序
    pub async fn item_vec_range_by_datetime(
        &self,
        pool: &MySqlPool,
        tbl_suffix: &str,
        period: u16,
        sdatetime: &NaiveDateTime,
        edatetime: &NaiveDateTime,
        limit: u16,
    ) -> Result<Vec<KLineItem>, sqlx::Error> {
        let sdatetime = &sdatetime.format("%Y-%m-%d %H:%M:%S").to_string();
        let edatetime = &edatetime.format("%Y-%m-%d %H:%M:%S").to_string();
        self.item_vec_range(pool, tbl_suffix, period, sdatetime, edatetime, limit)
            .await
    }

    /// 最老的数据, 时间正序
    pub async fn item_vec_oldest(
        &self,
        pool: &MySqlPool,
        tbl_suffix: &str,
        period: u16,
        limit: u16,
    ) -> Result<Vec<KLineItem>, sqlx::Error> {
        let table_name = self.table_name(tbl_suffix);
        let sql = Self::KLINE_ITEM_VEC_OLDEST_SQL_TEMPLATE.replace("{{table_name}}", &table_name);
        let mut args = MySqlArguments::default();
        args.add(period);
        args.add(limit);

        sqlx::query_as_with::<_, KLineItem, _>(&sql, args)
            .fetch(pool)
            // .map(Self::item_breed_from_symbol)
            .try_collect()
            .await
    }

    /// 最新的数据, 时间正序.
    pub async fn item_vec_latest(
        &self,
        pool: &MySqlPool,
        tbl_suffix: &str,
        period: u16,
        limit: u16,
    ) -> Result<Vec<KLineItem>, sqlx::Error> {
        let table_name = self.table_name(tbl_suffix);
        let sql = Self::KLINE_ITEM_VEC_LATEST_SQL_TEMPLATE.replace("{{table_name}}", &table_name);
        let mut args = MySqlArguments::default();
        args.add(period);
        args.add(limit);

        sqlx::query_as_with::<_, KLineItem, _>(&sql, args)
            .fetch(pool)
            // .map(Self::item_breed_from_symbol)
            .try_collect()
            .await
    }

    /// 获取某一合约的最新的数据列表, 时间正序.
    pub async fn item_vec_latest_by_symbol(
        &self,
        pool: &MySqlPool,
        tbl_suffix: &str,
        period: u16,
        symbol: &str,
        limit: u16,
    ) -> Result<Vec<KLineItem>, sqlx::Error> {
        let table_name = self.table_name(tbl_suffix);
        let sql = Self::KLINE_ITEM_VEC_LATEST_BY_SYMBOL_SQL_TEMPLATE
            .replace("{{table_name}}", &table_name);

        let mut args = MySqlArguments::default();
        args.add(symbol);
        args.add(period);
        args.add(limit);

        sqlx::query_as_with::<_, KLineItem, _>(&sql, args)
            .fetch(pool)
            // .map(Self::item_breed_from_symbol)
            .try_collect()
            .await
    }
}

impl KLineItemUtil {
    const SYMBOL_VEC_SQL_TEMPLATE: &'static str = "SELECT DISTINCT code FROM {{table_name}}";

    /// 表中存在的合约列表
    pub async fn symbol_vec(
        &self,
        pool: &MySqlPool,
        tbl_suffix: &str,
    ) -> Result<Vec<String>, sqlx::Error> {
        let table_name = self.table_name(tbl_suffix);
        let sql = Self::SYMBOL_VEC_SQL_TEMPLATE.replace("{{table_name}}", &table_name);

        sqlx::query_as::<_, (String,)>(&sql)
            .fetch(pool)
            .map(|item| item.map(|code| code.0))
            .try_collect()
            .await
    }
}

#[cfg(test)]
mod tests {

    use chrono::NaiveDate;

    use super::KLineItemUtil;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    #[tokio::test]
    async fn test_kline_item_vec() {
        init_test_mysql_pools();
        let kline_db_util = KLineItemUtil::new("hqdb");
        let kline_item_stream = kline_db_util
            .item_vec_egt_dt(
                &MySqlPools::pool_default().await.unwrap(),
                "agL9",
                1,
                "2022-05-13",
                10,
            )
            .await
            .unwrap();
        for kline_item in kline_item_stream.iter() {
            println!("{}", kline_item)
        }
    }

    #[tokio::test]
    async fn test_kline_item_vec_range() {
        init_test_mysql_pools();
        let kiu = KLineItemUtil::new("hqdb");
        let kline_item_vec_range = kiu
            .item_vec_range(
                &MySqlPools::pool_default().await.unwrap(),
                "agL9",
                1,
                "2022-06-20 09:01:00",
                "2022-06-20 15:00:00",
                500,
            )
            .await
            .unwrap();
        for item in kline_item_vec_range.iter() {
            println!("{}", item);
        }
        println!("{}", kline_item_vec_range.len());
    }

    #[tokio::test]
    async fn test_kline_item_vec_range_by_time() {
        init_test_mysql_pools();
        let kiu = KLineItemUtil::new("hqdb");
        let sdatetime = NaiveDate::from_ymd_opt(2022, 6, 20)
            .unwrap()
            .and_hms_opt(9, 1, 0)
            .unwrap();
        let edatetime = NaiveDate::from_ymd_opt(2022, 6, 20)
            .unwrap()
            .and_hms_opt(15, 1, 0)
            .unwrap();
        let kline_item_vec_range = kiu
            .item_vec_range_by_datetime(
                &MySqlPools::pool_default().await.unwrap(),
                "agL9",
                1,
                &sdatetime,
                &edatetime,
                500,
            )
            .await
            .unwrap();
        for item in kline_item_vec_range.iter() {
            println!("{}", item);
        }
        println!("{}", kline_item_vec_range.len());
    }

    #[tokio::test]
    async fn test_item_vec_oldest() {
        init_test_mysql_pools();
        let kiu = KLineItemUtil::new("hqdb");
        let kline_item_vec = kiu
            .item_vec_oldest(&MySqlPools::pool_default().await.unwrap(), "agL9", 5, 100)
            .await
            .unwrap();
        for item in kline_item_vec.iter() {
            println!("{}", item);
        }
        println!("{}", kline_item_vec.len());
    }

    #[tokio::test]
    async fn test_item_vec_latest() {
        init_test_mysql_pools();
        let kiu = KLineItemUtil::new("hqdb");
        let kline_item_vec = kiu
            .item_vec_latest(&MySqlPools::pool_default().await.unwrap(), "agL9", 1, 10)
            .await
            .unwrap();
        for item in kline_item_vec.iter() {
            println!("{}", item);
        }
        println!("{}", kline_item_vec.len());
    }

    #[tokio::test]
    async fn test_kline_item_vec_range_by_time_zero() {
        init_test_mysql_pools();

        let kiu = KLineItemUtil::new("hqdb");
        let sdatetime = NaiveDate::from_ymd_opt(2022, 6, 20)
            .unwrap()
            .and_hms_opt(9, 1, 0)
            .unwrap();
        let edatetime = NaiveDate::from_ymd_opt(2022, 6, 20)
            .unwrap()
            .and_hms_opt(8, 1, 0)
            .unwrap();
        let kline_item_vec_range = kiu
            .item_vec_range_by_datetime(
                &MySqlPools::pool_default().await.unwrap(),
                "agL9",
                1,
                &sdatetime,
                &edatetime,
                500,
            )
            .await
            .unwrap();
        for item in kline_item_vec_range.iter() {
            println!("{}", item);
        }
        println!("{}", kline_item_vec_range.len());
    }

    #[tokio::test]
    async fn test_kline_item_vec_latest_by_symbol() {
        init_test_mysql_pools();

        let kline_db_util = KLineItemUtil::new("hqdb");
        let kline_item_stream = kline_db_util
            .item_vec_latest_by_symbol(
                &MySqlPools::pool_default().await.unwrap(),
                "agL9",
                5,
                "agL9",
                5,
            )
            .await
            .unwrap();
        let kline_item_stream = &kline_item_stream;
        println!("record count: {}", kline_item_stream.len());
        for kline_item in kline_item_stream {
            println!("{}", kline_item)
        }
    }

    #[tokio::test]
    async fn test_symbol_vec() {
        init_test_mysql_pools();

        let kline_db_util = KLineItemUtil::new("hqdb");
        let symbol_vec = kline_db_util
            .symbol_vec(&MySqlPools::pool_default().await.unwrap(), "agL9")
            .await
            .unwrap();
        let symbol_vec = &symbol_vec;
        println!("record count: {}", symbol_vec.len());
        for symbol in symbol_vec {
            println!("{}", symbol)
        }
    }

    //  这个一定不要启用
    // #[test]
    // fn test_table_rename() {
    //     tokio::runtime::Builder::new_current_thread()
    //         .enable_all()
    //         .build()
    //         .unwrap()
    //         .block_on(async {
    //             let kline_db_util = KLineDbUtil::new("hqdb", "L9.tmp");
    //             kline_db_util.table_rename("ag", "hqdb", "L9");
    //             Ok::<(), sqlx::Error>(())
    //         })
    //         .unwrap();
    // }
}
