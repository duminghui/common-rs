pub mod opt_u8_zero {
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = u8::deserialize(deserializer)?;
        if v == 0 {
            Ok(None)
        } else {
            Ok(Some(v))
        }
    }
}
