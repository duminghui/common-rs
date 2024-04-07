use std::path::Path;
use std::{fs, io};

use log::debug;
use serde::Deserialize;
use thiserror::Error;

use crate::path_plain::{HomeDirNotFound, PathPlainExt};

#[derive(Debug, Error)]
pub enum YamlParseError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    SerdeYaml(#[from] ::serde_yaml::Error),
    #[error("{0}")]
    PathPlain(#[from] HomeDirNotFound),
}

pub fn parse_from_file<'de, P, R>(path: P) -> Result<R, YamlParseError>
where
    P: AsRef<Path>,
    // R: DeserializeOwned,
    R: Deserialize<'de>,
{
    let path = path.as_ref();
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
    let file_content = Box::leak(Box::new(file_content));
    // println!("{}", content_msg);
    debug!("{}", content_msg);
    // let config_hmap = serde_yaml::from_str::<R>(&file_content)?;
    let r = serde_yaml::from_str::<R>(file_content)?;
    // let r = from_str::<R>(&file_content)?;
    Ok(r)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::path::Path;

    use serde::Deserialize;

    use crate::yaml::parse_from_file;

    #[test]
    fn test_cow() {
        #[derive(Debug, Deserialize)]
        pub struct AppConfig<'a> {
            #[serde(rename = "log-root-dir")]
            pub log_root_dir: Cow<'a, Path>,
            #[serde(rename = "log-file")]
            pub log_file:     String,
        }
        let tmp = serde_yaml::from_str::<AppConfig>("");
        println!("{:?}", tmp);
        // let file = std::fs::File::open("").unwrap();
        // let tmp = serde_yaml::from_reader::<_, AppConfig>(&file);
        // println!("{:?}", tmp);
        let tmp = parse_from_file::<_, AppConfig>("");
        println!("{:?}", tmp)
    }
}
