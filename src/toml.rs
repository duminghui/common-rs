use std::path::Path;
use std::{fs, io};

use log::debug;
use serde::Deserialize;
use thiserror::Error;
use toml::Deserializer;

use crate::path_plain::{HomeDirNotFound, PathPlainExt};

#[derive(Debug, Error)]
pub enum TomlParseError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    SerdeToml(#[from] toml::de::Error),
    #[error("{0}")]
    PathPlain(#[from] HomeDirNotFound),
}

fn from_str<'de, T>(s: &str) -> Result<T, toml::de::Error>
where
    T: Deserialize<'de>,
{
    T::deserialize(Deserializer::new(s))
}

pub fn parse_from_file<'de, P, R>(path: P) -> Result<R, TomlParseError>
where
    P: AsRef<Path>,
    P: std::fmt::Debug,
    // R: DeserializeOwned,
    R: Deserialize<'de>,
{
    let path = path.plain()?;
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
    // let r = toml::from_str::<R>(&file_content)?;
    let r = from_str::<R>(&file_content)?;
    Ok(r)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::path::Path;

    use serde::Deserialize;

    use crate::toml::parse_from_file;

    #[test]
    fn test_read() {
        #[derive(Deserialize, Debug)]
        pub struct Test {
            pub f1: String,
            pub f2: i32,
            pub f3: bool,
        }
        let tmp = parse_from_file::<_, Test>("_test.toml");
        println!("{:?}", tmp)
    }

    #[test]
    fn test_cow() {
        #[derive(Debug, Deserialize)]
        pub struct AppConfig<'a> {
            #[serde(rename = "log-root-dir", borrow)]
            pub log_root_dir: Cow<'a, Path>,
            #[serde(rename = "log-file")]
            pub log_file:     String,
        }
        let tmp = parse_from_file::<_, AppConfig>("");
        println!("{:?}", tmp);
    }
}
