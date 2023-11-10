use std::path::Path;
use std::{fs, io};

use log::debug;
use serde::de::DeserializeOwned;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TomlParseError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    SerdeToml(#[from] toml::de::Error),
}

pub fn parse_from_file<P, R>(path: P) -> Result<R, TomlParseError>
where
    P: AsRef<Path>,
    P: std::fmt::Debug,
    R: DeserializeOwned,
{
    let file_content = fs::read_to_string(&path);
    if let Err(err) = file_content {
        let err_msg = format!("Read File Err: {:?}, {:?}", path, err);
        println!("{}", err_msg);
        debug!("{}", err_msg);
        return Err(err.into());
    }
    let file_content = file_content.unwrap();
    let content_msg = format!(
        "# File Content Yaml: {:?}:\n-------content start-------\n{}\n-------content end-------",
        path, file_content
    );
    // println!("{}", content_msg);
    debug!("{}", content_msg);
    let config_hmap = toml::from_str::<R>(&file_content)?;
    Ok(config_hmap)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use crate::utoml::parse_from_file;

    #[derive(Deserialize, Debug)]
    pub struct Test {
        pub f1: String,
        pub f2: i32,
        pub f3: bool,
    }

    #[test]
    fn test_read() {
        let tmp = parse_from_file::<_, Test>("_test.toml");
        println!("{:?}", tmp)
    }
}
