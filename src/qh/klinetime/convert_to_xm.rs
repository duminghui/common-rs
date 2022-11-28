use std::sync::{Arc, RwLock};

use chrono::{NaiveDateTime, Timelike};
use lazy_static::lazy_static;
use sqlx::MySqlPool;

use super::convert_to_1d::ConvertTo1d;
use super::convert_to_1m::{ConvertTo1m, KLineDateTime, TickDateTime};
use super::convert_to_1month::ConvertTo1Month;
use super::convert_to_1w::ConvertTo1W;
use super::convert_to_30m60m120m::ConvertTo30m60m120m;
use super::convert_to_3m5m15m::ConvertTo3m5m15m;
use super::tx_time_range::TxTimeRangeData;
use super::{KLineTimeError, TimeRangeDateTime};
use crate::qh::breed::BreedInfoVec;
use crate::qh::trading_day::TradingDayUtil;

pub async fn init(pool: &MySqlPool) -> Result<(), KLineTimeError> {
    BreedInfoVec::init(pool).await?;
    TradingDayUtil::init(pool).await?;
    TxTimeRangeData::init(pool).await?;

    ConvertTo1m::init()?;
    ConvertTo30m60m120m::init(pool).await?;

    Ok(())
}

lazy_static! {
    static ref CONVERT_XM: RwLock<Arc<ConvertToXm>> = RwLock::new(Default::default());
}

pub struct ConvertToXm {
    c1m:         Arc<ConvertTo1m>,
    c30_60_120m: Arc<ConvertTo30m60m120m>,
    c1d:         Arc<ConvertTo1d>,
    c1w:         Arc<ConvertTo1W>,
    c1mth:       Arc<ConvertTo1Month>,
}

impl Default for ConvertToXm {
    fn default() -> Self {
        Self {
            c1m:         ConvertTo1m::current(),
            c30_60_120m: ConvertTo30m60m120m::current(),
            c1d:         ConvertTo1d::current(),
            c1w:         ConvertTo1W::current(),
            c1mth:       ConvertTo1Month::current(),
        }
    }
}

impl ConvertToXm {
    pub fn current() -> Arc<ConvertToXm> {
        CONVERT_XM.read().unwrap().clone()
    }

    /// time 必须是tick time经过处理后的1m, 否则不准确
    pub fn time_range_xm(
        &self,
        breed: &str,
        period: &str,
        datetime: &NaiveDateTime,
    ) -> Result<TimeRangeDateTime, KLineTimeError> {
        match period {
            "3m" | "5m" | "15m" => Ok(ConvertTo3m5m15m::time_range(period, datetime)),
            "30m" | "60m" | "120m" => self.c30_60_120m.time_range(breed, period, datetime),
            "1d" => self.c1d.time_range(breed, datetime),
            "1w" => self.c1w.time_range(breed, datetime),
            "1mth" | "1month" => self.c1mth.time_range(breed, datetime),
            _ => {
                Err(KLineTimeError::PeriodNotSupport {
                    period: period.to_owned(),
                    scope:  "convert_xm::time_range_xm".to_owned(),
                })
            },
        }
    }

    pub fn to_1m_with_min_dg_day(
        &self,
        breed: &str,
        min_dg_day: u32,
        time: &impl Timelike,
    ) -> Result<(KLineDateTime, TickDateTime), KLineTimeError> {
        self.c1m.to_1m_with_min_dg_day(breed, min_dg_day, time)
    }

    pub fn to_1m_with_trading_day(
        &self,
        breed: &str,
        trading_day: u32,
        time: &impl Timelike,
    ) -> Result<(KLineDateTime, TickDateTime), KLineTimeError> {
        self.c1m.to_1m_with_trading_day(breed, trading_day, time)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDateTime, NaiveTime};

    use super::init;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;
    use crate::qh::klinetime::convert_to_xm::ConvertToXm;

    #[tokio::test]
    async fn test_to_xm() {
        init_test_mysql_pools();

        init(&MySqlPools::default()).await.unwrap();

        let cxm = ConvertToXm::current();
        let breed = "ag";
        println!("=== {} ===", breed);
        let time = NaiveTime::from_hms(11, 25, 25);
        let (time_1m, _) = cxm.to_1m_with_min_dg_day(breed, 20220616, &time).unwrap();
        println!("{:>6}: {}", "1m", time_1m);
        let time_1m_2 = "2022-06-16T11:26:00".parse::<NaiveDateTime>().unwrap();
        assert_eq!(time_1m, time_1m_2);
        for period in vec!["3m", "5m", "15m", "30m", "60m", "120m", "1w", "1month"] {
            let time = cxm.time_range_xm(breed, period, &time_1m).unwrap();
            println!("{:>6}: {}", period, time);
        }
    }
}
