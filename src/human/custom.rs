use std::fmt::{self, Write};

use rust_decimal::Decimal;

#[derive(Debug)]
pub struct HumanDecimal(pub Decimal);

impl fmt::Display for HumanDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prec = f.precision().unwrap_or(2);

        // 这个不会四舍五入
        // let num = format!("{:.prec$}", self.0);

        let mut v = self.0;
        v.rescale(prec as u32);
        let num = v.to_string();

        let (int_part, frac_part) = match num.split_once('.') {
            Some((int_str, fract_str)) => (int_str.to_string(), fract_str),
            None => (self.0.trunc().to_string(), ""),
        };
        let len = int_part.len();
        for (idx, c) in int_part.chars().enumerate() {
            let pos = len - idx - 1;
            f.write_char(c)?;
            if pos > 0 && pos % 3 == 0 {
                f.write_char(',')?;
            }
        }
        if !frac_part.is_empty() {
            f.write_char('.')?;
            f.write_str(frac_part)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use super::HumanDecimal;

    #[test]
    fn test_human_dcimal() {
        let v1 = Decimal::from_str("3.0001").unwrap();
        assert_eq!("3.00", format!("{}", HumanDecimal(v1)));
        assert_eq!("3.00010", format!("{:.5}", HumanDecimal(v1)));
        let v1 = Decimal::from_str("10003.0001").unwrap();
        assert_eq!("10,003.00", format!("{:.2}", HumanDecimal(v1)));
        // let v1 = Decimal::from_str("123456.1234567890").unwrap();
        // for i in 0..11 {
        //     println!("{:.i$}", HumanDecimal(v1));
        // }
    }
}
