use std::collections::HashSet;
use std::fmt::{self, Write};
use std::path::Path;

use eyre::eyre;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::Deserialize;

use crate::serde_extend::string::{opt_str, vec_vec_str};
use crate::{toml, AResult};

#[derive(Debug, Clone, Default, Deserialize)]
pub struct DDL {
    #[serde(rename = "database", default)]
    database: Vec<String>,
    #[serde(rename = "table")]
    table:    Vec<Table>,
}

#[derive(Debug, Clone, Deserialize)]
struct Table {
    #[serde(rename = "tbl-database")]
    database:    String,
    #[serde(rename = "tbl-name")]
    name:        String,
    #[serde(rename = "tbl-private-key")]
    private_key: Vec<String>,
    #[serde(rename = "tbl-index", with = "vec_vec_str")]
    index:       Vec<Vec<String>>,
    #[serde(flatten)]
    field:       IndexMap<String, Field>,
}

impl Table {
    fn vaildate(&self) -> AResult<()> {
        if self.database.is_empty() {
            Err(eyre!("database is empty"))?;
        }
        let field_name_set = self
            .field
            .keys()
            .map(|v| v.replace('-', "_"))
            .collect::<HashSet<_>>();
        for p_key in self.private_key.iter() {
            let p_key = p_key.replace('-', "_");
            if !field_name_set.contains(&p_key) {
                Err(eyre!("error private key: {}", p_key))?;
            }
        }
        for index_vec in self.index.iter() {
            for index in index_vec {
                let index = index.replace('-', "_");
                if !field_name_set.contains(&index) {
                    Err(eyre!("error index: {}", index))?;
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = self.name.replace('-', "_");
        let database = self.database.replace('-', "_");
        writeln!(f, "CREATE TABLE IF NOT EXISTS `{}`.`{}` (", database, name)?;
        let is_exist_p_key = !self.private_key.is_empty();
        let is_exist_index = !self.index.is_empty();
        for (idx, (name, field)) in self.field.iter().enumerate() {
            let field = field.with_name(name).unwrap();
            let suffix = if idx != self.field.len() - 1 || is_exist_p_key || is_exist_index {
                ","
            } else {
                ""
            };
            writeln!(f, "{}{}", field, suffix)?;
        }
        if is_exist_p_key {
            let p_key = self
                .private_key
                .iter()
                .map(|v| format!("`{}`", v.replace('-', "_")))
                .join(",");
            let suffix = if is_exist_index { "," } else { "" };
            writeln!(f, "  PRIMARY KEY({}){}", p_key, suffix)?;
        }
        if is_exist_index {
            for (idx, index) in self.index.iter().enumerate() {
                let index = index
                    .iter()
                    .map(|v| format!("`{}`", v.replace('-', "_")))
                    .join(",");
                let suffix = if idx == self.index.len() - 1 { "" } else { "," };
                writeln!(f, "  INDEX({}){}", index, suffix)?;
            }
        }
        write!(f, ") ENGINE=INNODB DEFAULT CHARSET=utf8;")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
struct Field {
    #[serde(rename = "type")]
    field_type: String,
    #[serde(rename = "not-null", default)]
    not_null:   bool,
    #[serde(rename = "default", default)]
    default:    Option<String>,
    #[serde(rename = "on-update", default, with = "opt_str")]
    on_update:  Option<String>,
    #[serde(rename = "comment", default, with = "opt_str")]
    comment:    Option<String>,
}

impl Field {
    fn with_name(&self, name: &str) -> AResult<String> {
        let name = name.replace('-', "_");
        let mut result = String::new();
        write!(&mut result, "  `{}` {}", name, self)?;
        Ok(result)
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field_type = self.field_type.to_uppercase();
        write!(f, "{}", field_type)?;
        if self.not_null {
            write!(f, " NOT NULL")?;
        }
        if let Some(default) = &self.default {
            if field_type.contains("CHAR") || field_type.contains("VARCHAR") {
                write!(f, " DEFAULT '{}'", default)?;
            } else {
                write!(f, " DEFAULT {}", default)?;
            }
        }
        if let Some(on_update) = &self.on_update {
            write!(f, " ON UPDATE {}", on_update)?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT '{}'", comment)?;
        }
        Ok(())
    }
}

impl DDL {
    pub fn load<P: AsRef<Path>>(path: P) -> AResult<DDL> {
        let path = path.as_ref();
        let ddl_info = toml::parse_from_file::<_, DDL>(&path)?;
        for db in ddl_info.database.iter() {
            if db.is_empty() {
                Err(eyre!("database emtpy: {}", path.display()))?;
            }
        }
        let tbl_duplicate = ddl_info
            .table
            .iter()
            .duplicates_by(|v| &v.name)
            .map(|v| v.name.clone())
            .collect::<HashSet<_>>();
        if !tbl_duplicate.is_empty() {
            let tbl_names = tbl_duplicate.iter().join(",");
            Err(eyre!("duplication table:{}", tbl_names))?;
        }
        for tbl in ddl_info.table.iter() {
            tbl.vaildate()
                .map_err(|e| eyre!("table {} err: {}, {}", tbl.name, e, path.display()))?;
        }

        Ok(ddl_info)
    }

    pub fn load_more<P: AsRef<Path>>(paths: &[P]) -> AResult<DDL> {
        let mut ddl = DDL::default();
        for path in paths {
            let ddl_append = Self::load(path)?;
            for db in ddl_append.database {
                if !ddl.database.contains(&db) {
                    ddl.database.push(db);
                }
            }
            let tbl_name_set = ddl
                .table
                .iter()
                .map(|v| v.name.clone())
                .collect::<HashSet<String>>();
            for tbl in ddl_append.table {
                if tbl_name_set.contains(&tbl.name) {
                    Err(eyre!("duplication table:{}", tbl.name))?;
                }
                ddl.table.push(tbl);
            }
        }
        Ok(ddl)
    }

    pub fn sql_vec(&self) -> AResult<Vec<String>> {
        let mut sql_vec = vec![];
        for db in self.database.iter() {
            sql_vec.push(format!("CREATE DATABASE IF NOT EXISTS `{}`;", db));
        }
        for tbl in self.table.iter() {
            sql_vec.push(tbl.to_string());
        }
        Ok(sql_vec)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use indexmap::{indexmap, IndexMap};

    use super::{Field, Table, DDL};

    #[test]
    fn test_table() {
        let field_info = Field {
            field_type: "VARCHAR(60)".into(),
            not_null:   true,
            default:    Some("".into()),
            on_update:  None,
            comment:    Some("这是一个测试".into()),
        };
        let field_info2 = Field {
            field_type: "int(10)".into(),
            not_null:   true,
            default:    Some("0".into()),
            on_update:  None,
            comment:    Some("这是一个测试".into()),
        };
        let field = indexmap! {
            "bbb-bbb".to_string()=>field_info,
            "ccc-aaa-ddd".into()=>field_info2
        };
        let table = Table {
            database:    "gp-swindex".into(),
            name:        "tbl-tmp-1".into(),
            field:       field.clone(),
            private_key: Vec::new(),
            index:       Vec::new(),
        };
        let tbl_str = table.to_string();
        println!("{}", tbl_str);
        let table = Table {
            private_key: vec!["bbb-bbb".into()],
            ..table
        };
        let tbl_str = table.to_string();
        println!("{}", tbl_str);
        let table = Table {
            index: vec![vec!["bbb-bbb".into(), "ddd".into()]],
            ..table
        };
        let tbl_str = table.to_string();
        println!("{}", tbl_str);
        let table = Table {
            private_key: vec![],
            ..table
        };
        let tbl_str = table.to_string();
        println!("{}", tbl_str);
    }

    #[test]
    fn test_field() {
        let field_info = Field {
            field_type: "VARCHAR(60)".into(),
            not_null:   true,
            default:    Some("".into()),
            on_update:  None,
            comment:    Some("这是一个测试".into()),
        };
        println!("{:?}", field_info.with_name("bbb-bbb"))
    }

    #[test]
    fn test3() {
        let a = String::new();
        let a = Cow::Borrowed(&a);
        let b = a.clone();
        match b {
            Cow::Borrowed(_) => println!("Borrowed"),
            Cow::Owned(_) => println!("Owned"),
        }
    }

    #[test]
    fn test2() {
        let ddl_info = DDL::load("./_data/db-ddl.toml");
        println!("{:?}", ddl_info);
        let ddl_info = ddl_info.unwrap();
        let sql_vec = ddl_info.sql_vec();
        println!("{:?}", sql_vec);
        let sql_vec = sql_vec.unwrap();
        for sql in sql_vec {
            println!("{}", sql)
        }
    }

    #[test]
    fn test_load_more() {
        let ddl_info = DDL::load_more(&["./_data/db-ddl.toml", "./_data/db-ddl-2.toml"]);
        let ddl_info = ddl_info.unwrap();
        let sql_vec = ddl_info.sql_vec().unwrap();
        for sql in sql_vec {
            println!("{}", sql)
        }
    }

    #[test]
    fn test1() {
        let solar_distance = BTreeMap::from([
            ("Mercury", 0.4),
            ("Venus", 0.7),
            ("Earth", 1.0),
            ("Mars", 1.5),
        ]);
        for (k, v) in solar_distance.iter() {
            println!("{} {}", k, v)
        }
        let mut index_map = IndexMap::new();
        index_map.insert("xx", 100);
        index_map.insert("xx2", 100);
        index_map.insert("xx", 200);
        index_map.insert("xx3", 100);
        index_map.insert("xx2", 100);
        for (k, v) in index_map.iter() {
            println!("{} {}", k, v)
        }
    }
}
