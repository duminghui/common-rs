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
