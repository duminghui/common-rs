use std::ops::Deref;

use chrono::NaiveTime;
use sqlx::error::BoxDynError;
use sqlx::mysql::{MySqlTypeInfo, MySqlValueRef};
use sqlx::{Decode, MySql, Type};

// String -> Vec<T>
#[derive(Debug, Clone)]
pub struct VecType<T>(Vec<T>);

impl<T> Deref for VecType<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> Type<MySql> for VecType<T> {
    fn type_info() -> MySqlTypeInfo {
        <&str as Type<MySql>>::type_info()
    }

    fn compatible(ty: &MySqlTypeInfo) -> bool {
        <&str as Type<MySql>>::compatible(ty)
    }
}

impl Decode<'_, MySql> for VecType<String> {
    fn decode(value: MySqlValueRef<'_>) -> Result<Self, BoxDynError> {
        let value = <&str as Decode<MySql>>::decode(value)?;
        let vec = value.split(',').map(|v| v.to_owned()).collect::<Vec<_>>();
        Ok(VecType(vec))
    }
}

impl Decode<'_, MySql> for VecType<NaiveTime> {
    fn decode(value: MySqlValueRef<'_>) -> Result<Self, BoxDynError> {
        let value = <&str as Decode<MySql>>::decode(value)?;
        let vec = value
            .split(',')
            .map(|v| NaiveTime::parse_from_str(v, "%H:%M:%S"))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(VecType(vec))
    }
}
