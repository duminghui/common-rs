use std::collections::HashMap;
use std::sync::OnceLock;

use chrono::{Duration, NaiveDateTime, NaiveTime};

static TIME_PERIOD_MAP: OnceLock<HashMap<String, HashMap<NaiveTime, NaiveTime>>> = OnceLock::new();

pub fn init() {
    let mut map = HashMap::<String, HashMap<NaiveTime, NaiveTime>>::new();
    map.insert("5m".to_string(), gen_time_map(5));
    map.insert("15m".to_string(), gen_time_map(15));
    map.insert("30m".to_string(), gen_time_map(30));
    map.insert("60m".to_string(), gen_time_map(60));
    map.insert("120m".to_string(), gen_time_map(120));
    TIME_PERIOD_MAP.set(map).unwrap();
}

fn gen_time_map(period_value: u32) -> HashMap<NaiveTime, NaiveTime> {
    let time_range_vec = vec![
        (
            NaiveTime::from_hms_opt(9, 31, 0).unwrap(),
            NaiveTime::from_hms_opt(11, 30, 0).unwrap(),
        ),
        (
            NaiveTime::from_hms_opt(13, 1, 0).unwrap(),
            NaiveTime::from_hms_opt(15, 0, 0).unwrap(),
        ),
    ];

    let mut time_map = HashMap::new();
    let mut idx = 0;
    let mut time_vec = vec![];
    for (start, end) in time_range_vec {
        let mut time = start;
        while time <= end {
            idx += 1;
            time_vec.push(time);

            if idx % period_value == 0 {
                let period_time = time;
                for time in time_vec.iter() {
                    time_map.insert(*time, period_time);
                }
                time_vec.clear();
            }

            time += Duration::minutes(1);
        }
    }
    if !time_vec.is_empty() {
        let period_time = time_vec.last().unwrap();
        for time in time_vec.iter() {
            time_map.insert(*time, *period_time);
        }
    }
    time_map
}

pub struct Converter;

impl Converter {
    fn convert_1d(dt: &NaiveDateTime) -> NaiveDateTime {
        dt.date().and_hms_opt(15, 0, 0).unwrap()
    }

    pub fn convert(period: &str, dt: &NaiveDateTime) -> Result<NaiveDateTime, String> {
        if period == "1d" {
            return Ok(Self::convert_1d(dt));
        }
        let time_period_map = TIME_PERIOD_MAP
            .get()
            .unwrap()
            .get(period)
            .ok_or(format!("时间周期 错误的周期: {}", period))?;
        let time_key = dt.time();
        let period_time = time_period_map
            .get(&time_key)
            .ok_or(format!("时间周期 错误的时间 {}", dt))?;

        Ok(dt.date().and_time(*period_time))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, NaiveTime};

    use super::{init, TIME_PERIOD_MAP};

    #[test]
    fn test_gen_time_map() {
        init();
        let time_range_vec = vec![
            (
                NaiveTime::from_hms_opt(9, 31, 0).unwrap(),
                NaiveTime::from_hms_opt(11, 30, 0).unwrap(),
            ),
            (
                NaiveTime::from_hms_opt(13, 1, 0).unwrap(),
                NaiveTime::from_hms_opt(15, 0, 0).unwrap(),
            ),
        ];
        let time_map = TIME_PERIOD_MAP.get().unwrap().get("120m").unwrap();
        for (start, end) in time_range_vec {
            let mut time = start;
            while time <= end {
                println!("{}  {:?}", time, time_map.get(&time));
                time += Duration::minutes(1);
            }
        }
    }
}
