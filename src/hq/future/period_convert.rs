use chrono::{NaiveDate, NaiveDateTime};
use sqlx::MySqlPool;

use self::d1::Converter1d;
use self::m1::Converter1m;
use self::xm::ConverterXm;
use super::time_range::TimeRangeError;

pub(crate) mod d1;
pub(crate) mod m1;
pub(crate) mod xm;

#[derive(Debug, thiserror::Error)]
pub enum PeriodConvertError {
    #[error("{0}")]
    SqxlError(#[from] sqlx::Error),

    #[error("{0}")]
    TimeRangeError(#[from] TimeRangeError),

    #[error("breed err: {0}")]
    BreedError(String),

    #[error("period err: {0}")]
    PeriodError(String),

    #[error("time err: {0}")]
    TimeError(NaiveDateTime),
}

pub async fn init(pool: &MySqlPool) -> Result<(), PeriodConvertError> {
    m1::init_from_time_range(pool).await?;
    xm::init_from_time_range(pool).await?;
    d1::init_from_time_range(pool).await?;
    Ok(())
}

pub struct Converter;

impl Converter {
    pub fn convert_to_1m(
        breed: &str,
        dt: &NaiveDateTime,
    ) -> Result<NaiveDateTime, PeriodConvertError> {
        Converter1m::convert(breed, dt)
    }

    pub fn convert_to_xm(
        breed: &str,
        period: &str,
        dt: &NaiveDateTime,
        trade_date: &NaiveDate,
    ) -> Result<NaiveDateTime, PeriodConvertError> {
        if period == "1d" {
            Converter1d::convert(breed, trade_date)
        } else {
            ConverterXm::convert(breed, period, dt, trade_date)
        }
    }
}
