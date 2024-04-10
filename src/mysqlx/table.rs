// pub struct Field {
//     name:     String,
//     r#type:   String,
//     not_null: bool,
//     default:  String,
//     comment:  String,
// }

// impl Field {
//     fn new(name: &str, r#type: &str, not_null: bool, default: &str, comment: &str) -> Self {
//         Self {
//             name: name.to_string(),
//             r#type: r#type.to_string(),
//             not_null,
//             default: default.to_string(),
//             comment: comment.to_string(),
//         }
//     }
// }

use std::cmp::max;
use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt};
use itertools::Itertools;
use sqlx::mysql::MySqlArguments;
use sqlx::{Arguments, MySqlPool};

use super::exec::{exec_sql, ExecError, ExecInfo};

pub fn table_name(db_name: &str, tbl_name: &str) -> String {
    if db_name.is_empty() {
        format!("`{}`", tbl_name)
    } else {
        format!("`{}`.`{}`", db_name, tbl_name)
    }
}

pub async fn show_tables(pool: &MySqlPool, db_name: &str) -> Result<Vec<String>, sqlx::Error> {
    let sql = format!("SHOW TABLES FROM {}", db_name);

    let tables = sqlx::query_as::<_, (String,)>(&sql)
        .fetch(pool)
        .map(|v| v.map(|v| v.0))
        .try_collect::<Vec<String>>()
        .await?;

    Ok(tables)
}

// TODO 待优化
pub async fn table_index_columns(
    pool: &MySqlPool,
    db_name: &str,
    tbl_name: &str,
) -> Result<Vec<String>, ExecError> {
    let sql = "SELECT column_name FROM information_schema.statistics WHERE table_schema=? AND table_name=?" ;
    let mut args = MySqlArguments::default();
    args.add(db_name);
    args.add(tbl_name);

    let column_vec = sqlx::query_as_with::<_, (String,), _>(sql, args)
        .fetch(pool)
        .map_ok(|v| v.0)
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| ExecError::Sqlx(sql.to_string(), e))?;
    Ok(column_vec)
}

pub async fn column_idx_add(
    pool: &MySqlPool,
    db_name: &str,
    tbl_name: &str,
    indexs: &[(String, String)],
) -> Result<ExecInfo, ExecError> {
    if indexs.is_empty() {
        return Ok(ExecInfo::default());
    }

    let indexs_str = indexs
        .iter()
        .map(|(index_name, column_name)| format!("ADD INDEX `{}`(`{}`)", index_name, column_name))
        .join(",");

    let tbl_name = table_name(db_name, tbl_name);
    let sql = format!("ALTER TABLE {} {}", tbl_name, indexs_str);
    exec_sql(pool, &sql).await
}

pub async fn column_indexs_not_exist_add(
    pool: &MySqlPool,
    db_name: &str,
    tbl_name: &str,
    indexs: &[(String, String)],
) -> Result<ExecInfo, ExecError> {
    let tbl_index_columns = table_index_columns(pool, db_name, tbl_name).await?;

    let new_indexs = indexs
        .iter()
        .filter(|(_, column)| !tbl_index_columns.contains(column))
        .cloned()
        .collect::<Vec<_>>();

    column_idx_add(pool, db_name, tbl_name, &new_indexs).await
}

struct TableField {
    name:    String,
    r#type:  String,
    null:    bool,
    default: String,
    comment: String,
}

impl std::fmt::Display for TableField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let null_str = if self.null { "" } else { " NOT NULL" };
        let default_str = if self.default.is_empty() {
            "".into()
        } else {
            format! {" DEFAULT {}",self.default}
        };
        write!(
            f,
            "{} {}{}{} COMMENT '{}',",
            self.name, self.r#type, null_str, default_str, self.comment
        )
    }
}

pub struct TableCreator {
    table_name:   String,
    field_vec:    Vec<TableField>,
    indexs:       Vec<String>,
    primary_keys: String,
}

impl std::fmt::Display for TableCreator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CREATE TABLE IF NOT EXISTS {} (", self.table_name)?;
        for field in self.field_vec.iter() {
            writeln!(f, "  {}", field)?;
        }
        for index in self.indexs.iter() {
            writeln!(f, "  {}", index)?;
        }
        writeln!(f, "  {}", self.primary_keys)?;
        writeln!(f, ") ENGINE=InnoDB")
    }
}

impl std::fmt::Debug for TableCreator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CREATE TABLE IF NOT EXISTS {} (", self.table_name)?;
        let (name_padding, type_padding, null_padding, default_padding) =
            self.field_vec.iter().fold(
                (0usize, 0usize, 0usize, 0usize),
                |(nl, tl, nll, dl), field| {
                    let nl = max(nl, field.name.len());
                    let tl = max(tl, field.r#type.len());
                    let f_nll_len = if field.null { 0 } else { 10 };
                    let nll = max(nll, f_nll_len as usize);
                    let dl = max(dl, if field.default.is_empty() { 0 } else { 14 });
                    (nl, tl, nll, dl)
                },
            );

        for field in self.field_vec.iter() {
            let null_str = if field.null { "" } else { "  NOT NULL" };
            let default_str = if field.default.is_empty() {
                "".into()
            } else {
                format!("  DEFAULT {}", field.default)
            };

            writeln!(
                f,
                "  {:name_padding$}  {:type_padding$}{:null_padding$}{:default_padding$}  COMMENT '{}',",
                field.name, field.r#type, null_str,default_str, field.comment
            )?;
        }
        for index in self.indexs.iter() {
            writeln!(f, "  {}", index)?;
        }
        writeln!(f, "  {}", self.primary_keys)?;
        writeln!(f, ") ENGINE=InnoDB")
    }
}

impl TableCreator {
    pub fn new(db_name: &str, tbl_name: &str) -> TableCreator {
        let table_name = table_name(db_name, tbl_name);
        TableCreator {
            table_name,
            field_vec: Vec::new(),
            indexs: Vec::new(),
            primary_keys: String::new(),
        }
    }

    pub fn add_field(
        mut self,
        name: &str,
        r#type: &str,
        null: bool,
        default: &str,
        comment: &str,
    ) -> Self {
        self.field_vec.push(TableField {
            name: format!("`{}`", name),
            r#type: r#type.to_string(),
            null,
            default: default.to_string(),
            comment: comment.to_string(),
        });
        self
    }

    pub fn add_index(mut self, index_name: &str, fields: &[&str]) -> Self {
        let fields_str = fields.iter().map(|v| format!("`{}`", v)).join(",");
        self.indexs
            .push(format!("INDEX {} ({}),", index_name, fields_str));
        self
    }

    pub fn primary_keys(mut self, fields: &[&str]) -> Self {
        let fields_str = fields.iter().map(|v| format!("`{}`", v)).join(",");
        self.primary_keys = format!("PRIMARY KEY ({})", fields_str);
        self
    }

    pub async fn create(&self, pool: &MySqlPool) -> Result<TableExecInfo, ExecError> {
        let sql = self.to_string();
        let exec_info = exec_sql(pool, &sql).await?;
        Ok(TableExecInfo {
            table_name: self.table_name.to_string(),
            elapsed:    exec_info.elapsed,
        })
    }
}

#[derive(Debug)]
pub struct TableExecInfo {
    pub table_name: String,
    elapsed:        Duration,
}

impl std::fmt::Display for TableExecInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{:>9.3?}] {}", self.elapsed, self.table_name)
    }
}

// CREATE TABLE IF NOT EXISTS {{table_name}} (
//     `file`  type           NOT NULL      COMMENT 'file1',
//     `file2` type           NOT NULL      COMMENT 'file2',
//     PRIMARY KEY (`file`, `file2`)
//   ) ENGINE=InnoDB
pub async fn create_table(
    pool: &MySqlPool,
    sql_template: &str,
    db_name: &str,
    tbl_name: &str,
) -> Result<TableExecInfo, ExecError> {
    let table_name = table_name(db_name, tbl_name);
    let sql = sql_template.replace("{{table_name}}", &table_name);

    let exec_info = exec_sql(pool, &sql).await?;

    Ok(TableExecInfo {
        table_name,
        elapsed: exec_info.elapsed,
    })
}

#[cfg(test)]
mod tests {
    use super::TableCreator;
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    fn table_creator() -> TableCreator {
        TableCreator::new("basedata", "tmp")
            .add_field("f22222", "int(11)", true, "0.0", "字段2")
            .add_field("f3", "char(8)", false, "", "字段3")
            .add_field("f4", "char(8)", true, "", "字段4")
            .add_field("f5", "char(8)", true, "", "字段5")
            .add_field("f1", "datetime", true, "", "更新时间")
            .add_index("index_f3", &["f5"])
            .primary_keys(&["f22222", "f3"])
    }

    #[test]
    fn test_table_creator_debug() {
        let tb = table_creator();
        println!("{:?}", tb);
    }

    #[test]
    fn test_table_creator_display() {
        let tb = table_creator();

        println!("{}", tb);
    }

    #[tokio::test]
    async fn test_create_table() {
        init_test_mysql_pools();
        let tb = table_creator();
        let r = tb.create(MySqlPools::pool().as_ref()).await;
        if let Err(err) = r {
            println!("{}", err);
            return;
        }
        println!("{}", r.unwrap());
    }
}
