use std::ops::RangeInclusive;
use std::sync::OnceLock;

use futures_util::TryStreamExt;
use sqlx::MySqlPool;

const A_Z_LOWER_RANGE: RangeInclusive<char> = 'a'..='z';
const A_Z_UPPER_RANGE: RangeInclusive<char> = 'A'..='Z';

pub fn breed_from_symbol(symbol: &str) -> String {
    if symbol.ends_with("L9") {
        return symbol.replace("L9", "");
    } else if symbol.ends_with("L8") {
        return symbol.replace("L8", "");
    }
    symbol
        .chars()
        .take_while(|c| A_Z_LOWER_RANGE.contains(c) || A_Z_UPPER_RANGE.contains(c))
        .collect::<String>()
}

static BREED_INFO_VEC: OnceLock<Vec<BreedInfo>> = OnceLock::new();

#[derive(Debug)]
pub struct BreedInfo {
    // 品种代码
    pub breed:  String,
    // 主力合约
    pub symbol: String,
}

impl BreedInfo {
    fn new_from_symbol(symbol: &str) -> BreedInfo {
        let breed = breed_from_symbol(symbol);
        BreedInfo {
            breed,
            symbol: symbol.to_owned(),
        }
    }
}

#[derive(Debug, Default)]
pub struct BreedInfoVec {
    vec: Vec<BreedInfo>,
}

impl BreedInfoVec {
    pub fn current<'a>() -> &'a Vec<BreedInfo> {
        BREED_INFO_VEC.get().unwrap()
    }

    pub async fn init(pool: &MySqlPool) -> Result<(), sqlx::Error> {
        if !Self::current().is_empty() {
            return Ok(());
        }
        let breed_info_vec = Self::init_from_db(pool).await?;
        BREED_INFO_VEC.set(breed_info_vec).unwrap();
        Ok(())
    }

    async fn init_from_db(pool: &MySqlPool) -> Result<Vec<BreedInfo>, sqlx::Error> {
        let sql = "SELECT instrument_id FROM hqdb.tbl_future_main_contract";
        let breed_info_vec = sqlx::query_as::<_, (String,)>(sql)
            .fetch(pool)
            .map_ok(|item| BreedInfo::new_from_symbol(&item.0))
            // .map(|item| item.map(|id| BreedInfo::new_from_symbol(&id.0)))
            .try_collect::<Vec<BreedInfo>>()
            .await?;
        Ok(breed_info_vec)
    }

    pub fn is_empty(&self) -> bool {
        self.vec.is_empty()
    }

    pub fn vec(&self) -> &Vec<BreedInfo> {
        &self.vec
    }
}

#[cfg(test)]
mod tests {
    use super::{breed_from_symbol, BreedInfoVec};
    use crate::mysqlx::MySqlPools;
    use crate::mysqlx_test_pool::init_test_mysql_pools;

    #[test]
    fn test_breed_from_symbol() {
        let breed = breed_from_symbol("agL9");
        println!("1: {}", breed);
        let breed = breed_from_symbol("ag2009");
        println!("2: {}", breed);
        let breed = breed_from_symbol(&String::from("APL9"));
        println!("3: {}", breed);
    }

    #[tokio::test]
    async fn test_breed_list_from_db() {
        init_test_mysql_pools();
        BreedInfoVec::init(&MySqlPools::pool()).await.unwrap();
        let breed_vec = BreedInfoVec::current();
        println!("{:?}", breed_vec);
    }
}
