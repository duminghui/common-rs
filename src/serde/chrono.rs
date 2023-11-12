const TIME_FORMAT: &str = "%H:%M";
pub mod naive_time_hhmm {
    use chrono::NaiveTime;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::TIME_FORMAT;

    pub fn serialize<S>(time: &NaiveTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", time.format(TIME_FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveTime::parse_from_str(&s, TIME_FORMAT)
            .map_err(|e| serde::de::Error::custom(format!("{}:{}", e, s)))
    }
}

pub mod opt_naive_time_hhmm {
    use chrono::NaiveTime;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::TIME_FORMAT;

    pub fn serialize<S>(time: &Option<NaiveTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = time.map_or(String::new(), |v| format!("{}", v.format(TIME_FORMAT)));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            return Ok(None);
        }
        NaiveTime::parse_from_str(&s, TIME_FORMAT)
            .map(Some)
            .map_err(|e| serde::de::Error::custom(format!("{}:{}", e, s)))
    }
}

const DATE_FORMAT: &str = "%Y-%m-%d";

pub mod naive_date {
    use chrono::NaiveDate;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::DATE_FORMAT;

    pub fn serialize<S>(date: &NaiveDate, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = date.format(DATE_FORMAT).to_string();
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDate::parse_from_str(&s, DATE_FORMAT)
            .map_err(|e| serde::de::Error::custom(format!("{}:{}", e, s)))
    }
}

pub mod opt_naive_date {
    use chrono::NaiveDate;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::DATE_FORMAT;

    pub fn serialize<S>(date: &Option<NaiveDate>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = date.map_or(String::new(), |v| v.format(DATE_FORMAT).to_string());
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveDate>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            return Ok(None);
        }
        NaiveDate::parse_from_str(&s, DATE_FORMAT)
            .map(Some)
            .map_err(|e| serde::de::Error::custom(format!("{}:{}", e, s)))
    }
}

const DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

pub mod naive_datetime {
    use chrono::NaiveDateTime;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::DATETIME_FORMAT;

    pub fn serialize<S>(datetime: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = datetime.format(DATETIME_FORMAT).to_string();
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&s, DATETIME_FORMAT)
            .map_err(|e| serde::de::Error::custom(format!("{}:{}", e, s)))
    }
}

pub mod opt_naive_datetime {
    use chrono::NaiveDateTime;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::DATETIME_FORMAT;

    pub fn serialize<S>(datetime: &Option<NaiveDateTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = datetime.map_or(String::new(), |v| format!("{}", v.format(DATETIME_FORMAT)));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            return Ok(None);
        }
        NaiveDateTime::parse_from_str(&s, DATETIME_FORMAT)
            .map(Some)
            .map_err(|e| serde::de::Error::custom(format!("{}:{}", e, s)))
    }
}
