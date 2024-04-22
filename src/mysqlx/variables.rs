use std::path::PathBuf;

use sqlx::MySqlPool;

use crate::AResult;

pub async fn secure_file_priv(pool: &MySqlPool) -> AResult<Option<PathBuf>> {
    let sql = "SHOW VARIABLES LIKE 'secure_file_priv';";
    let r = sqlx::query_as::<_, (String, String)>(sql)
        .fetch_one(pool)
        .await?;
    if r.1 == "NULL" {
        Ok(None)
    } else {
        Ok(Some(r.1.into()))
    }
}
