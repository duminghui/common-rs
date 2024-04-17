use std::io::BufWriter;
use std::path::Path;
use std::{fs, io};

use log::debug;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::path_plain::{HomeDirNotFound, PathPlainExt};

#[derive(Debug, Error)]
pub enum YamlError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    SerdeYaml(#[from] ::serde_yaml::Error),
    #[error("{0}")]
    PathPlain(#[from] HomeDirNotFound),
}

pub fn parse_from_file<'de, P, R>(path: P) -> Result<R, YamlError>
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

pub fn parse_from_file_simple<'de, P, R>(path: P) -> Result<R, YamlError>
where
    P: AsRef<Path>,
    // R: DeserializeOwned,
    R: Deserialize<'de>,
{
    let path = path.as_ref();
    let path = path.plain()?;
    let file_content = fs::read_to_string(path)?;
    let file_content = Box::leak(Box::new(file_content));
    let r = serde_yaml::from_str::<R>(file_content)?;
    Ok(r)
}

pub fn write_to_file<P, T>(path: P, value: T) -> Result<(), YamlError>
where
    P: AsRef<Path>,
    T: Serialize,
{
    let outfile = fs::File::create(path.as_ref())?;
    let outfile = BufWriter::new(outfile);
    serde_yaml::to_writer(outfile, &value)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::path::Path;

    use indexmap::{indexmap, IndexMap};
    use serde::{Deserialize, Serialize};

    use crate::yaml::{parse_from_file, write_to_file};

    #[derive(Debug, Deserialize, Serialize)]
    struct IndexMapTmp {
        #[serde(flatten)]
        data: IndexMap<String, i64>,
    }

    #[test]
    fn test_indexmap() {
        let data = indexmap! {
            "xxx1".to_string()=>100,
            "xxx2".to_string()=>100,
            "xxx3".to_string()=>100,
        };

        let tmp = IndexMapTmp { data };
        let file = "./_data/yaml-indexmap.yaml";
        write_to_file(file, &tmp).unwrap();

        let tmp = parse_from_file::<_, IndexMapTmp>(file).unwrap();
        println!("{:?}", tmp);
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct Tmp {
        #[serde(rename = "field-1")]
        field1: String,
        field2: bool,
        field3: i64,
    }

    #[test]
    fn test_write() {
        let tmp = Tmp {
            field1: "xxxx".to_string(),
            field2: true,
            field3: 100000000,
        };
        let str = serde_yaml::to_string(&tmp).unwrap();
        println!("{}", str);
        write_to_file("./_data/yaml-write.yaml", &tmp).unwrap();
    }

    #[allow(unused)]
    #[derive(Debug, Deserialize)]
    struct AppConfig<'a> {
        #[serde(rename = "log-root-dir")]
        pub log_root_dir: Cow<'a, Path>,
        #[serde(rename = "log-file")]
        pub log_file:     String,
    }

    #[test]
    fn test_cow() {
        let tmp = serde_yaml::from_str::<AppConfig>("");
        println!("{:?}", tmp);
        // let file = std::fs::File::open("").unwrap();
        // let tmp = serde_yaml::from_reader::<_, AppConfig>(&file);
        // println!("{:?}", tmp);
        let tmp = parse_from_file::<_, AppConfig>("");
        println!("{:?}", tmp)
    }

    #[derive(Debug, Deserialize, Serialize)]
    enum EnumTmp {
        #[serde(rename = "enum1")]
        Enum1,
        #[serde(rename = "enum2")]
        Enum2,
        #[serde(rename = "enum3")]
        Enum3,
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct Tmp2 {
        pub a:    String,
        pub b:    String,
        enum_tmp: EnumTmp,
    }

    #[test]
    fn test_enum() {
        let tmp = Tmp2 {
            a:        "aa".into(),
            b:        "bb".into(),
            enum_tmp: EnumTmp::Enum1,
        };

        let yaml_str = serde_yaml::to_string(&tmp).unwrap();
        println!("{}", yaml_str);

        let yaml_str = r#"
        a: aa
        b: bb
        enum_tmp: enum4
        "#;

        let tmp = serde_yaml::from_str::<Tmp2>(yaml_str).unwrap();
        println!("{:#?}", tmp);
    }
}
