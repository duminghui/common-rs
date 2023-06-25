use std::path::Path;
use std::{fs, io};

use serde::de::DeserializeOwned;
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub enum YamlParseError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    SerdeYaml(#[from] ::serde_yaml::Error),
}

pub fn parse_from_file<P, R>(path: P) -> Result<R, YamlParseError>
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
    let config_hmap = serde_yaml::from_str::<R>(&file_content)?;
    Ok(config_hmap)
}
