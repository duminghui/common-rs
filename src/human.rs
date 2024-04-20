use std::fmt::{self, Write};

use rust_decimal::Decimal;

#[derive(Debug)]
pub struct HumanDecimal(pub Decimal);

impl fmt::Display for HumanDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prec = f.precision().unwrap_or(2);

        // 不会四舍五入
        // let num = format!("{:.prec$}", self.0);

        let mut v = self.0;
        v.rescale(prec as u32);
        let num = v.to_string();

        let (int_part, frac_part) = match num.split_once('.') {
            Some((int_str, fract_str)) => (int_str, fract_str),
            None => (num.as_str(), ""),
        };

        let len = int_part.len();

        let mut buf = String::new();
        for (idx, c) in int_part.chars().enumerate() {
            let pos = len - idx - 1;
            buf.write_char(c)?;
            if pos > 0 && pos % 3 == 0 {
                buf.write_char(',')?;
            }
        }
        if !frac_part.is_empty() {
            buf.write_char('.')?;
            buf.write_str(frac_part)?;
        }
        f.pad_integral(true, "", &buf)
    }
}

#[derive(Debug)]
pub struct HumanCountFixPad(pub u64);

impl fmt::Display for HumanCountFixPad {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf = String::new();
        let num = self.0.to_string();
        let len = num.len();
        for (idx, c) in num.chars().enumerate() {
            let pos = len - idx - 1;
            buf.write_char(c)?;
            if pos > 0 && pos % 3 == 0 {
                buf.write_char(',')?;
            }
        }
        // 默认右对齐
        f.pad_integral(true, "", &buf)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use super::{HumanCountFixPad, HumanDecimal};

    #[test]
    fn test_human_count() {
        let count = HumanCountFixPad(10000);
        println!("1: {}", count);
        let s = "String".to_string();
        println!("2: '{:9}'", s);
        println!("2: '{:9}'", "100");
        println!("3: '{:9}'", "str");
        println!("4: '{:9}'", 100);
        println!("5: '{:9}'", count);
        println!("6: '{:<9}'", count);
        println!("7: '{:>9}'", count);
    }

    #[test]
    fn test_decimal() {
        let v1 = Decimal::from_str("3.0001").unwrap();
        println!("{}", v1);
        let v1 = Decimal::from_str("3.0000").unwrap();
        println!("{}", v1);
        let v1 = Decimal::from_str("3").unwrap();
        println!("{}", v1);
    }

    #[test]
    fn test_human_decimal_1() {
        let v1 = Decimal::from_str("100003.0001").unwrap();
        let v1 = HumanDecimal(v1);
        println!("{:.5}", v1);
    }

    #[test]
    fn test_human_decimal() {
        let v1 = Decimal::from_str("3.0001").unwrap();
        let v1 = HumanDecimal(v1);
        assert_eq!("3.00", format!("{}", v1));
        println!("{:.5}", v1);
        assert_eq!("3.00010", format!("{:.5}", v1));

        let v1 = Decimal::from_str("10003.0001").unwrap();
        let v1 = HumanDecimal(v1);
        assert_eq!("10,003.00", format!("{:.2}", v1));

        let v1 = Decimal::from_str("123456.1234567890").unwrap();
        let v1 = HumanDecimal(v1);
        for i in 0..11 {
            println!("{:<20.i$}", v1);
        }
    }

    #[test]
    fn test_1() {
        // 不会四舍五入
        let v1 = Decimal::from_str("3.0071").unwrap();
        let prec = 2;
        let num = format!("{:.prec$}", v1);
        println!("{}", num);

        let mut v = v1;
        v.rescale(prec as u32);
        let num = v.to_string();
        println!("{}", num)
    }

    #[test]
    fn test_2() {
        let yes = "y̆es";
        for (i, char) in yes.chars().enumerate() {
            println!("{}: {}", i, char);
        }
        for (i, char) in yes.char_indices() {
            println!("{}: {}", i, char);
        }
    }
}
