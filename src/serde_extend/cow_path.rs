use std::borrow::Cow;
use std::path::Path;

use serde::{Deserialize, Deserializer};

use crate::path_plain::PathPlainExt;

pub fn deserialize<'de, 'a, D>(deserializer: D) -> Result<Cow<'a, Path>, D::Error>
where
    D: Deserializer<'de>,
{
    let path = Cow::<Path>::deserialize(deserializer)?;
    path.plain()
        .map(|v| Cow::Owned(v.into_owned()))
        .map_err(serde::de::Error::custom)
}
