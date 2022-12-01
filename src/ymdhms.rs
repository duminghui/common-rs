use std::fmt;

use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};

// pub trait DateConvert: Datelike {
//     fn to_yyyymmdd(&self) -> u32 {
//         (self.year() * 10000) as u32 + (self.month() * 100) as u32 + self.day() as u32
//     }
// }

// impl<T: Datelike> DateConvert for T {
// }
/// 开始时间, 结束时间的整数数据
#[derive(Debug, Clone)]
pub(crate) struct TimeRangeHms {
    pub start: Hms,
    pub end: Hms,
}

impl TimeRangeHms {
    pub fn new(shhmmss: u32, ehhmmss: u32) -> TimeRangeHms {
        TimeRangeHms {
            start: Hms::from_hhmmss(shhmmss),
            end: Hms::from_hhmmss(ehhmmss),
        }
    }

    /// 是否在区间范围内
    pub fn in_range(&self, hhmmss: &u32) -> bool {
        // let hhmmss = *hhmmss;
        let s = self.start.hhmmss;
        let e = self.end.hhmmss;
        if s <= e {
            (s..=e).contains(hhmmss)
        } else {
            (s..=235959).contains(hhmmss) || hhmmss <= &e
        }
    }

    pub fn in_range_hms(&self, hms: &Hms) -> bool {
        self.in_range(&hms.hhmmss)
    }

    pub fn in_range_time(&self, time: &NaiveTime) -> bool {
        let hms = Hms::from(time);
        self.in_range_hms(&hms)
    }
}

impl fmt::Display for TimeRangeHms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("({},{})", self.start, self.end))
    }
}

#[derive(Copy, Clone, Eq)]
pub struct Hms {
    pub hhmmss: u32,
    pub hhmm: u16,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
}

impl Hms {
    pub(crate) fn from_hhmmss(hhmmss: u32) -> Hms {
        let hhmm = (hhmmss / 100) as u16;
        let hour = (hhmm / 100) as u8;
        let minute = (hhmm % 100) as u8;
        let second = (hhmmss % 100) as u8;
        Hms {
            hhmmss,
            hhmm,
            hour,
            minute,
            second,
        }
    }

    pub(crate) fn from_hms(hour: u8, min: u8, sec: u8) -> Hms {
        let hhmm = hour as u16 * 100 + min as u16;
        let hhmmss = hhmm as u32 * 100 + sec as u32;
        Hms {
            hhmmss,
            hhmm,
            hour,
            minute: min,
            second: sec,
        }
    }
}

impl fmt::Debug for Hms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Hms {{{}}}", self.hhmmss))
    }
}

impl fmt::Display for Hms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.hhmmss))
    }
}

impl PartialEq for Hms {
    fn eq(&self, other: &Self) -> bool {
        self.hhmmss == other.hhmmss
    }
}

impl PartialOrd for Hms {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.hhmmss.partial_cmp(&other.hhmmss)
    }
}

impl From<&Hms> for NaiveTime {
    fn from(hms: &Hms) -> NaiveTime {
        NaiveTime::from_hms_opt(hms.hour as u32, hms.minute as u32, hms.second as u32).unwrap()
    }
}

impl<T: Timelike> From<&T> for Hms {
    fn from(time: &T) -> Self {
        Hms::from_hms(time.hour() as u8, time.minute() as u8, time.second() as u8)
    }
}

#[derive(Copy, Clone)]
pub struct Ymd {
    pub yyyymmdd: u32,
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

impl Ymd {
    pub(crate) fn from_yyyymmdd(yyyymmdd: u32) -> Ymd {
        let year = (yyyymmdd / 10000) as u16;
        let month = (yyyymmdd / 100 % 100) as u8;
        let day = (yyyymmdd % 100) as u8;
        Ymd {
            yyyymmdd,
            year,
            month,
            day,
        }
    }

    pub(crate) fn from_ymd(year: u16, month: u8, day: u8) -> Ymd {
        let yyyymmdd = year as u32 * 10000 + month as u32 * 100 + day as u32;
        Ymd {
            yyyymmdd,
            year,
            month,
            day,
        }
    }
}

impl fmt::Debug for Ymd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Ymd {{{}}}", self.yyyymmdd))
    }
}

impl fmt::Display for Ymd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.yyyymmdd))
    }
}

impl From<&Ymd> for NaiveDate {
    fn from(ymd: &Ymd) -> NaiveDate {
        NaiveDate::from_ymd_opt(ymd.year as i32, ymd.month as u32, ymd.day as u32).unwrap()
    }
}

impl<T: Datelike> From<&T> for Ymd {
    fn from(time: &T) -> Ymd {
        Ymd::from_ymd(time.year() as u16, time.month() as u8, time.day() as u8)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, NaiveDate, NaiveTime};

    use super::{Hms, Ymd};

    #[test]
    fn test_ymd_to_naive_date_success() {
        let ymd = Ymd::from_ymd(2022, 6, 12);
        let date = NaiveDate::from(&ymd);
        let r_date = NaiveDate::from_ymd_opt(2022, 6, 12).unwrap();
        println!("{:?}, {:?}, {:?}, {}", ymd, date, r_date, date == r_date);
        assert_eq!(date, r_date);
    }

    #[test]
    #[should_panic]
    fn test_ymd_to_naive_date_failed() {
        let ymd = Ymd::from_ymd(2022, 13, 12);
        let date = NaiveDate::from(&ymd);
        println!("{:?}", date);
    }

    #[test]
    fn test_hms_to_naive_time_success() {
        let hms = Hms::from_hms(23, 59, 59);
        let time = NaiveTime::from(&hms);
        let r_time = NaiveTime::from_hms_opt(23, 59, 59).unwrap();
        println!("{:?}, {:?}, {:?}, {}", hms, time, r_time, time == r_time);
        assert_eq!(time, r_time)
    }

    #[test]
    #[should_panic]
    fn test_hms_to_naive_time_failed() {
        let hms = Hms::from_hms(23, 60, 59);
        let time = NaiveTime::from(&hms);
        println!("{:?}", time);
    }

    #[test]
    fn test_naive_time_add() {
        let mut time = NaiveTime::from_hms_opt(23, 59, 59).unwrap();
        let r_time = NaiveTime::from_hms_opt(0, 0, 59).unwrap();
        time += Duration::minutes(1);
        println!("{:?}, {:?}, {}", time, r_time, time == r_time);
        assert_eq!(time, r_time)
    }

    #[test]
    fn test_hms_cmp() {
        let hms1 = Hms::from_hms(21, 21, 21);
        let hms2 = Hms::from_hms(21, 21, 22);
        println!("{}", hms1 > hms2);
        println!("{}", hms1 >= hms2);
        println!("{}", hms1 == hms2);
        println!("{}", hms1 <= hms2);
        println!("{}", hms1 < hms2);
    }
}
