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
