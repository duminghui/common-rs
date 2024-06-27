use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::path::Path;
use std::sync::OnceLock;

use eyre::{eyre, OptionExt};
use indexmap::IndexMap;
use itertools::Itertools;
use serde::Deserialize;

use crate::serde_extend::string::opt_str;
use crate::{toml, AResult};

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SqlLoader {
    #[serde(rename = "database", default)]
    database:         Vec<Database>,
    #[serde(rename = "table", default)]
    table:            Vec<Table>,
    #[serde(skip)]
    tbl_hmap:         HashMap<String, Table>,
    #[serde(rename = "load-data-infile", default)]
    load_data_infile: Vec<LoadDataInfile>,
    #[serde(skip)]
    ldi_hamp:         HashMap<String, LoadDataInfile>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct LoadDataInfile {
    #[serde(rename = "ldi-name")]
    name:               String,
    #[serde(rename = "ldi-local", default)]
    is_local:           bool,
    #[serde(rename = "ldi-columns-terminated", default)]
    columns_terminated: Option<String>,
    #[serde(rename = "ldi-ignore-rows", default)]
    ignore_rows:        Option<usize>,
    #[serde(rename = "ldi-file-column-count", default)]
    file_column_count:  Option<usize>,
    #[serde(flatten)]
    col_set_map:        IndexMap<String, String>,
}

impl LoadDataInfile {
    fn field_map(v: &str) -> Cow<'_, str> {
        if v.starts_with('@') {
            Cow::Borrowed(v)
        } else {
            Cow::Owned(format!("`{}`", v.replace('-', "_")))
        }
    }

    fn sql(&self, ldi_file: &str, database: &str, tbl_name: &str) -> AResult<String> {
        let database = database.replace('-', "_");
        let tbl_name = tbl_name.replace('-', "_");
        let mut s = String::new();
        writeln!(s, "LOAD DATA")?;
        if self.is_local {
            writeln!(s, "  LOCAL")?;
        }
        writeln!(s, "  INFILE '{}'", ldi_file)?;
        writeln!(s, "  REPLACE")?;
        writeln!(s, "  INTO TABLE `{}`.`{}`", database, tbl_name)?;
        writeln!(s, "  COLUMNS")?;
        let fields_terminated = if let Some(fields_terminated) = self.columns_terminated.as_ref() {
            fields_terminated.as_str()
        } else {
            ","
        };
        writeln!(s, "    TERMINATED BY '{}'", fields_terminated)?;

        let ignore_rows = self.ignore_rows.unwrap_or_default();

        writeln!(s, "  IGNORE {} ROWS", ignore_rows)?;

        let col_map = self
            .col_set_map
            .iter()
            .filter(|(v, _)| v.starts_with("col-"))
            .collect::<IndexMap<_, _>>();
        let fields_str = if let Some(column_count) = self.file_column_count {
            let dummy = String::from("@dummy");
            let mut fields = vec![];
            for idx in 0..column_count {
                let field = col_map
                    .get(&format!("col-{}", idx))
                    .map(|&v| Self::field_map(v))
                    .unwrap_or(Cow::Borrowed(&dummy));
                fields.push(field)
            }
            fields.iter().map(|v| v.as_ref()).join(",")
        } else {
            col_map.values().map(|&v| Self::field_map(v)).join(",")
        };
        write!(s, "  ({})", fields_str)?;

        let set_map_str = self
            .col_set_map
            .iter()
            .filter(|(v, _)| v.starts_with("set-"))
            .map(|(k, v)| {
                let field = &k[4..].replace('-', "_");
                format!("`{}` = {}", field, v)
            })
            .join(",\n    ");
        if !set_map_str.is_empty() {
            writeln!(s)?;
            writeln!(s, "  SET")?;
            write!(s, "    {}", set_map_str)?;
        }
        write!(s, ";")?;

        Ok(s)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct Database {
    #[serde(rename = "name")]
    name:      String,
    #[serde(rename = "charset", default)]
    charset:   Option<String>,
    #[serde(rename = "collation", default)]
    collation: Option<String>,
}

impl Database {
    fn sql(&self) -> AResult<String> {
        let mut content = String::new();
        // CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET {charset} DEFAULT COLLATE {collation};
        let db_name = self.name.replace('-', "_");
        write!(content, "CREATE DATABASE IF NOT EXISTS `{}`", db_name)?;
        if let Some(charset) = &self.charset {
            write!(content, " DEFAULT CHARACTER SET {}", charset)?;
        }
        if let Some(collation) = &self.collation {
            write!(content, " DEFAULT COLLATE {}", collation)?;
        }
        write!(content, ";")?;
        Ok(content)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct Table {
    #[serde(rename = "tbl-is-template", default)]
    is_template: bool,
    #[serde(rename = "tbl-database")]
    database:    Option<String>,
    #[serde(rename = "tbl-name")]
    name:        String,
    #[serde(rename = "tbl-private-key")]
    private_key: Vec<String>,
    // #[serde(rename = "tbl-index", default, with = "vec_vec_str")]
    #[serde(rename = "tbl-index", default)]
    index:       Vec<Vec<String>>,
    #[serde(flatten)]
    field:       IndexMap<String, Field>,
}

impl Table {
    fn vaildate(&self) -> AResult<()> {
        if self.database.is_some() && self.database.as_ref().unwrap().is_empty() {
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

    fn sql(&self, db_name: Option<&str>, tbl_name: Option<&str>) -> AResult<String> {
        let db_name = if let Some(db_name) = db_name {
            db_name.replace('-', "_")
        } else {
            self.database
                .as_ref()
                .unwrap_or(&String::new())
                .replace('-', "_")
        };
        if db_name.is_empty() {
            Err(eyre!("database is empty"))?;
        }

        let tbl_name = if let Some(tbl_name) = tbl_name {
            tbl_name.replace('-', "_")
        } else {
            self.name.replace('-', "_")
        };

        if tbl_name.is_empty() {
            Err(eyre!("table name is empty"))?;
        }

        let mut content = String::new();
        writeln!(
            content,
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` (",
            db_name, tbl_name
        )?;
        let is_exist_p_key = !self.private_key.is_empty();
        let is_exist_index = !self.index.is_empty();
        for (idx, (name, field)) in self.field.iter().enumerate() {
            let field = field.with_name(name).unwrap();
            let suffix = if idx != self.field.len() - 1 || is_exist_p_key || is_exist_index {
                ","
            } else {
                ""
            };
            writeln!(content, "  {}{}", field, suffix)?;
        }
        if is_exist_p_key {
            let p_key = self
                .private_key
                .iter()
                .map(|v| format!("`{}`", v.replace('-', "_")))
                .join(",");
            let suffix = if is_exist_index { "," } else { "" };
            writeln!(content, "  PRIMARY KEY({}){}", p_key, suffix)?;
        }
        if is_exist_index {
            for (idx, index) in self.index.iter().enumerate() {
                let index = index
                    .iter()
                    .map(|v| format!("`{}`", v.replace('-', "_")))
                    .join(",");
                let suffix = if idx == self.index.len() - 1 { "" } else { "," };
                writeln!(content, "  INDEX({}){}", index, suffix)?;
            }
        }
        write!(content, ") ENGINE=INNODB DEFAULT CHARSET=utf8;")?;

        Ok(content)
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
        let mut content = String::new();
        let name = name.replace('-', "_");
        let field_type = self.field_type.to_uppercase();
        write!(content, "`{}` {}", name, field_type)?;
        if self.not_null {
            write!(content, " NOT NULL")?;
        }
        if let Some(default) = &self.default {
            if field_type.contains("CHAR") || field_type.contains("VARCHAR") {
                write!(content, " DEFAULT '{}'", default)?;
            } else {
                write!(content, " DEFAULT {}", default)?;
            }
        }
        if let Some(on_update) = &self.on_update {
            write!(content, " ON UPDATE {}", on_update)?;
        }
        if let Some(comment) = &self.comment {
            write!(content, " COMMENT '{}'", comment)?;
        }

        Ok(content)
    }
}

static SQL_LOADER: OnceLock<SqlLoader> = OnceLock::new();

impl SqlLoader {
    fn load<P: AsRef<Path>>(path: P) -> AResult<SqlLoader> {
        let path = path.as_ref();
        let mut sql = toml::parse_from_file::<_, SqlLoader>(&path)?;
        let db_duplicate = sql
            .database
            .iter()
            .duplicates_by(|v| &v.name)
            .map(|v| &v.name)
            .collect::<HashSet<_>>();

        if !db_duplicate.is_empty() {
            let db_names = db_duplicate.iter().join(",");
            Err(eyre!("duplication db:{}", db_names))?;
        }
        for tbl in sql.table.iter() {
            tbl.vaildate()
                .map_err(|e| eyre!("table {} err: {}, {}", tbl.name, e, path.display()))?;
            sql.tbl_hmap.insert(tbl.name.clone(), tbl.clone());
        }
        let tbl_duplicate = sql
            .table
            .iter()
            .duplicates_by(|v| &v.name)
            .map(|v| &v.name)
            .collect::<HashSet<_>>();
        if !tbl_duplicate.is_empty() {
            let tbl_names = tbl_duplicate.iter().join(",");
            Err(eyre!("duplication table:{}", tbl_names))?;
        }

        for ldi in sql.load_data_infile.iter() {
            sql.ldi_hamp.insert(ldi.name.clone(), ldi.clone());
        }
        let ldi_duplicate = sql
            .load_data_infile
            .iter()
            .duplicates_by(|v| &v.name)
            .map(|v| &v.name)
            .collect::<HashSet<_>>();
        if !ldi_duplicate.is_empty() {
            let ldi_names = ldi_duplicate.iter().join(",");
            Err(eyre!("duplication load data infile:{}", ldi_names))?;
        }

        Ok(sql)
    }

    pub fn init_from<P: AsRef<Path>>(paths: &[P]) -> AResult<()> {
        let mut sql = SqlLoader::default();
        for path in paths {
            let ddl_append = Self::load(path)?;
            for db in ddl_append.database {
                if sql.database.iter().any(|v| v.name == db.name) {
                    Err(eyre!("duplication db:{}", db.name))?;
                }
                sql.database.push(db);
            }
            for tbl in ddl_append.table {
                let tbl_name = &tbl.name;
                if sql.tbl_hmap.contains_key(tbl_name) {
                    Err(eyre!("duplication table:{}", tbl_name))?;
                }
                sql.table.push(tbl.clone());
                sql.tbl_hmap.insert(tbl_name.clone(), tbl);
            }
            for ldi in ddl_append.load_data_infile {
                let ldi_name = &ldi.name;
                if sql.ldi_hamp.contains_key(ldi_name) {
                    Err(eyre!("duplication load data infile:{}", ldi_name))?;
                }
                sql.load_data_infile.push(ldi.clone());
                sql.ldi_hamp.insert(ldi_name.clone(), ldi);
            }
        }
        SQL_LOADER.set(sql).unwrap();
        Ok(())
    }

    pub fn get<'a>() -> &'a SqlLoader {
        SQL_LOADER.get().unwrap()
    }

    pub fn database_create_sql_vec(&self) -> Vec<String> {
        let mut sql_vec = vec![];
        for db in self.database.iter() {
            sql_vec.push(db.sql().unwrap());
        }
        sql_vec
    }

    pub fn table_create_sql_vec(&self) -> Vec<String> {
        let mut sql_vec = vec![];
        for tbl in self.table.iter() {
            if !tbl.is_template {
                sql_vec.push(tbl.sql(None, None).unwrap());
            }
        }
        sql_vec
    }

    pub fn table_create_sql(&self, database: &str, tbl_name: &str) -> AResult<String> {
        let database = if database.is_empty() {
            None
        } else {
            Some(database)
        };
        let tbl = self
            .tbl_hmap
            .get(tbl_name)
            .ok_or_eyre(format!("err table name: {}", tbl_name))?;
        let sql = tbl.sql(database, Some(tbl_name))?;
        Ok(sql)
    }

    pub fn table_create_sql_from_template(
        &self,
        tmpl_name: &str,
        database: &str,
        tbl_name: &str,
    ) -> AResult<String> {
        let tbl = self
            .tbl_hmap
            .get(tmpl_name)
            .ok_or_eyre(format!("error template name: {}", tmpl_name))?;
        let sql = tbl.sql(Some(database), Some(tbl_name))?;
        Ok(sql)
    }

    pub fn load_data_infile(
        &self,
        ldi_name: &str,
        ldi_file: &str,
        database: &str,
        tbl_name: &str,
    ) -> AResult<String> {
        let ldi = self
            .ldi_hamp
            .get(ldi_name)
            .ok_or_eyre(format!("error load data infile name: {}", ldi_name))?;

        let sql = ldi.sql(ldi_file, database, tbl_name)?;
        Ok(sql)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use indexmap::IndexMap;

    use super::{Field, SqlLoader};

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
    fn test2() {
        let ddl_info = SqlLoader::load("./_data/db-sql.toml");
        println!("{:?}", ddl_info);
        let ddl_info = ddl_info.unwrap();
        let sql_vec = ddl_info.table_create_sql_vec();
        println!("{:?}", sql_vec);
        for sql in sql_vec {
            println!("{}", sql)
        }
    }

    #[test]
    fn test_load_more() {
        SqlLoader::init_from(&["./_data/db-sql.toml", "./_data/db-sql-2.toml"]).unwrap();
        let sql_loader = SqlLoader::get();
        let db_sql_vec = sql_loader.database_create_sql_vec();
        for sql in db_sql_vec {
            println!("{}", sql)
        }
        let sql_vec = sql_loader.table_create_sql_vec();
        for sql in sql_vec {
            println!("{}", sql)
        }
    }

    #[test]
    fn test_sql_from_template() {
        SqlLoader::init_from(&["./_data/db-sql.toml", "./_data/db-sql-2.toml"]).unwrap();
        let sql_loader = SqlLoader::get();
        let sql = sql_loader
            .table_create_sql_from_template("tbl-tmp-tmpl", "tmp", "bbbb-bbbb")
            .unwrap();
        println!("sql:{}", sql);

        // tbl-tmp-tmpl
    }

    #[test]
    fn test_sql_from_table() {
        SqlLoader::init_from(&["./_data/db-sql.toml", "./_data/db-sql-2.toml"]).unwrap();
        let sql_loader = SqlLoader::get();
        let sql = sql_loader.table_create_sql("xxx", "tbl-tmp-3").unwrap();
        println!("sql:{}", sql);
    }

    #[test]
    fn test_sql_ldi() {
        SqlLoader::init_from(&["./_data/db-sql.toml", "./_data/db-sql-2.toml"]).unwrap();
        let loader = SqlLoader::get();
        let sql = loader
            .load_data_infile("ldi-1", "xxx/xxx/xxx", "tmpssss", "xxxx")
            .unwrap();
        println!("{}", sql);
        let sql = loader
            .load_data_infile("ldi-2", "xxxx", "xxx", "xxx")
            .unwrap();
        println!("{}", sql);
        let sql = loader
            .load_data_infile(
                "ldi-3",
                "~/Downloads/BTCUSDT-1s-2023-12-29.csv",
                "gp_swindex",
                "tbl_tmp",
            )
            .unwrap();
        println!("{}", sql)
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

    #[test]
    fn test_str_sub() {
        let tmp = "set-10000";
        let tmp = &tmp[4..];
        println!("{}", tmp);
    }
}
