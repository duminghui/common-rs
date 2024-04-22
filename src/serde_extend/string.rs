pub mod str_to_vec {
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            return Ok(Vec::new());
        }

        Ok(s.split(',').map(|v| v.trim().into()).collect())
    }
}

pub mod str_lowercase {
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_lowercase();
        Ok(s)
    }
}

pub mod opt_str {
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            Ok(None)
        } else {
            Ok(Some(s))
        }
    }
}

pub mod vec_vec_str {
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Vec<String>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = Vec::<String>::deserialize(deserializer)?;
        if s.is_empty() {
            return Ok(Vec::new());
        }
        Ok(s.iter()
            .map(|v| v.split(',').map(|v| v.to_string()).collect())
            .collect())
    }
}

pub mod string_or_struct {
    use std::marker::PhantomData;
    use std::str::FromStr;

    use serde::de::{self, MapAccess, Visitor};
    use serde::{Deserialize, Deserializer};

    #[derive(Debug)]
    pub enum Void {}

    struct StringOrStruct<T>(PhantomData<fn() -> T>);

    impl<'de, T> Visitor<'de> for StringOrStruct<T>
    where
        T: Deserialize<'de> + FromStr<Err = Void>,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("string or map")
        }

        fn visit_str<E>(self, value: &str) -> Result<T, E>
        where
            E: de::Error,
        {
            Ok(FromStr::from_str(value).unwrap())
        }

        fn visit_map<M>(self, map: M) -> Result<T, M::Error>
        where
            M: MapAccess<'de>,
        {
            // 根据返回的类型确定调用哪个实现了Deserialize的Struct
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: Deserialize<'de> + FromStr<Err = Void>,
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StringOrStruct(PhantomData))
    }
}
