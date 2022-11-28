//! K线时间处理, 从Tick获取的时间生成组成1m时间, 从1m或其他周期的时间生成大于该周期的时间, 一般是从1m来生成.

use std::fmt;

use chrono::NaiveDateTime;

use super::trading_day::TradingDayUtilInitError;

mod convert_to_1d;
mod convert_to_1m;
mod convert_to_1month;
mod convert_to_1w;
mod convert_to_30m60m120m;
mod convert_to_3m5m15m;
pub mod convert_to_xm;
pub mod tx_time_range;

#[derive(Debug, thiserror::Error)]
pub enum KLineTimeError {
    #[error("Get next trading day for {0} is none")]
    NextTradingDay(u32),

    #[error("Get prev trading day for {0} is none")]
    PrevTradingDay(u32),

    #[error("{0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("breed vec is empty, must init")]
    BreedVecEmpty,

    #[error("TxTimeRangeData is empty, must init")]
    TxTimeRangeDataEmpty,

    #[error("Breed #{breed}# not exist in {scope}")]
    BreedNotExist { breed: String, scope: String },

    #[error("Period #{period}# not exist in {scope}")]
    PeriodNotExist { period: String, scope: String },

    #[error("Period #{period}# not support in {scope}")]
    PeriodNotSupport { period: String, scope: String },

    #[error("#{breed}# datetime #{datetime}# not in tx range")]
    DatetimeNotInRange {
        breed:    String,
        datetime: NaiveDateTime,
    },

    #[error("datetime #{0}# not support")]
    DatetimeNotSupport(NaiveDateTime),

    #[error("{0}")]
    TradingDayUtilInit(#[from] TradingDayUtilInitError),

    #[error("{0}'s week not had tx day")]
    WeekNotHadTxDay(NaiveDateTime),
}

#[derive(Debug)]
pub struct TimeRangeDateTime {
    pub start: NaiveDateTime,
    pub end:   NaiveDateTime,
}

impl TimeRangeDateTime {
    pub(crate) fn new(start: NaiveDateTime, end: NaiveDateTime) -> TimeRangeDateTime {
        TimeRangeDateTime { start, end }
    }
}

impl fmt::Display for TimeRangeDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "({}~{})",
            self.start.format("%Y-%m-%d %H:%M:%S"),
            self.end.format("%Y-%m-%d %H:%M:%S")
        ))
    }
}
