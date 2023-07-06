use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use chrono::{NaiveDate, NaiveDateTime};
use sqlx::MySqlPool;

use self::d1::Converter1d;
use self::m1::Converter1m;
use self::xm::ConverterXm;
use super::time_range::{self, TimeRangeError};
use super::trade_day;

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

static BREED_CONVERTER_MAP: OnceLock<HashMap<String, Arc<Converter>>> = OnceLock::new();

pub async fn init(pool: Arc<MySqlPool>) -> Result<(), PeriodConvertError> {
    trade_day::init_from_db(pool.clone()).await?;
    time_range::init_from_db(pool.clone()).await?;
    m1::init_from_time_range(pool.clone()).await?;
    xm::init_from_time_range(pool.clone()).await?;
    d1::init_from_time_range(pool).await?;

    if BREED_CONVERTER_MAP.get().is_some() {
        return Ok(());
    }
    let mut breed_converter_map = HashMap::new();
    let time_range_hmap = time_range::hash_map();
    for breed in time_range_hmap.keys() {
        let converter1m = m1::by_breed(breed).unwrap();
        let converterxm = xm::by_breed(breed).unwrap();
        let converter1d = d1::by_breed(breed).unwrap();
        breed_converter_map.insert(
            breed.to_string(),
            Arc::new(Converter {
                converter1m,
                converterxm,
                converter1d,
            }),
        );
    }
    BREED_CONVERTER_MAP.set(breed_converter_map).unwrap();

    Ok(())
}

#[derive(Debug)]
pub struct Converter {
    converter1m: Arc<Converter1m>,
    converterxm: Arc<ConverterXm>,
    converter1d: Arc<Converter1d>,
}

impl Converter {
    pub fn to_1m(&self, dt: &NaiveDateTime) -> NaiveDateTime {
        self.converter1m.convert(dt)
    }

    pub fn to_xm(
        &self,
        period: &str,
        dt: &NaiveDateTime,
        trade_date: &NaiveDate,
    ) -> Result<NaiveDateTime, PeriodConvertError> {
        self.converterxm.convert(period, dt, trade_date)
    }

    pub fn to_1d(&self, trade_date: &NaiveDate) -> NaiveDateTime {
        self.converter1d.convert(trade_date)
    }
}

pub fn converter_by_breed(breed: &str) -> Result<Arc<Converter>, PeriodConvertError> {
    let converter = BREED_CONVERTER_MAP
        .get()
        .unwrap()
        .get(breed)
        .ok_or(PeriodConvertError::BreedError(breed.to_string()))?
        .clone();
    Ok(converter)
}

pub fn converter_qh_base() -> Arc<Converter> {
    converter_by_breed("QHbase").unwrap()
}

// impl Converter {
//     pub fn convert_to_1m(
//         breed: &str,
//         dt: &NaiveDateTime,
//     ) -> Result<NaiveDateTime, PeriodConvertError> {
//         Converter1m::convert_tmp(breed, dt)
//     }

//     pub fn convert_to_xm(
//         breed: &str,
//         period: &str,
//         dt: &NaiveDateTime,
//         trade_date: &NaiveDate,
//     ) -> Result<NaiveDateTime, PeriodConvertError> {
//         if period == "1d" {
//             Converter1d::convert_tmp(breed, trade_date)
//         } else {
//             ConverterXm::convert_tmp(breed, period, dt, trade_date)
//         }
//     }
// }
