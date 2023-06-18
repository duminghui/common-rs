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

use std::sync::Arc;

use sqlx::MySqlPool;

use super::exec::{exec_sql, ExecError, ExecInfo};

pub fn table_name(db_name: &str, tbl_name: &str) -> String {
    if db_name.is_empty() {
        tbl_name.to_string()
    } else {
        format!("`{}`.`{}`", db_name, tbl_name)
    }
}

pub struct CreateTableExecInfo {
    pub table_name: String,
    pub exec_info:  ExecInfo,
}

impl std::fmt::Display for CreateTableExecInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}, {}", self.table_name, self.exec_info))
    }
}

// CREATE TABLE IF NOT EXISTS {{table_name}} (
//     `file`  type           NOT NULL      COMMENT 'file1',
//     `file2` type           NOT NULL      COMMENT 'file2',
//     PRIMARY KEY (`file`, `file2`)
//   ) ENGINE=InnoDB
pub async fn create_table(
    pool: Arc<MySqlPool>,
    sql_template: &str,
    db_name: &str,
    tbl_name: &str,
) -> Result<CreateTableExecInfo, ExecError> {
    let table_name = table_name(db_name, tbl_name);
    let sql = sql_template.replace("{{table_name}}", &table_name);

    let r = exec_sql(pool, &sql).await?;

    Ok(CreateTableExecInfo {
        table_name,
        exec_info: r,
    })
}
