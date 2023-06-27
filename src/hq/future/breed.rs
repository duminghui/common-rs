use std::ops::RangeInclusive;

const A_Z_LOWER_RANGE: RangeInclusive<char> = 'a'..='z';
const A_Z_UPPER_RANGE: RangeInclusive<char> = 'A'..='Z';

pub fn breed_from_contract(contract: &str) -> String {
    // if symbol.ends_with("L9") {
    //     return symbol.replace("L9", "");
    // } else if symbol.ends_with("L8") {
    //     return symbol.replace("L8", "");
    // }
    contract
        .chars()
        .take_while(|c| A_Z_LOWER_RANGE.contains(c) || A_Z_UPPER_RANGE.contains(c))
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use crate::hq::future::breed::breed_from_contract;

    #[test]
    fn test_breed_from_symbol() {
        let breed = breed_from_contract("agL9");
        println!("1: {}", breed);
        let breed = breed_from_contract("ag2009");
        println!("2: {}", breed);
        let breed = breed_from_contract(&String::from("APL9"));
        println!("3: {}", breed);
    }
}
