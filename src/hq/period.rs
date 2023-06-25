use std::collections::HashMap;
use std::sync::OnceLock;

static PERIOD_MAP: OnceLock<HashMap<String, i32>> = OnceLock::new();

pub struct PeriodValue;

impl PeriodValue {
    pub fn pv(period: &str) -> Option<&i32> {
        PERIOD_MAP
            .get_or_init(|| {
                let mut hmap = HashMap::new();
                hmap.insert("1m".to_owned(), 1);
                hmap.insert("3m".to_owned(), 3);
                hmap.insert("5m".to_owned(), 5);
                hmap.insert("15m".to_owned(), 15);
                hmap.insert("30m".to_owned(), 30);
                hmap.insert("60m".to_owned(), 60);
                hmap.insert("120m".to_owned(), 120);
                hmap.insert("1d".to_owned(), 1440);
                hmap.insert("1w".to_owned(), 10080); // 60*24*7
                hmap.insert("1mth".to_owned(), 43200); // 60*24*30
                hmap.insert("1month".to_owned(), 43200); // 60*24*30
                hmap
            })
            .get(period)
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::Runtime;

    use super::{PeriodValue, PERIOD_MAP};

    #[test]
    fn test_get_pv() {
        let mut v = PeriodValue::pv("1m");
        println!("{:?}", v);
        let v = v.take().unwrap().to_owned();
        println!("{}", v);
        println!("{:?}", PERIOD_MAP.get());
    }

    #[test]
    fn test_get_pv_2() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut handles = Vec::with_capacity(10);

            for _ in 0..10 {
                handles.push(tokio::spawn(async move {
                    let mut v = PeriodValue::pv("1m");
                    println!("{:?}", v);
                    let v = v.take().unwrap().to_owned();
                    println!("{}", v);
                    println!("{:?}", PERIOD_MAP.get());
                }))
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
    }
}
