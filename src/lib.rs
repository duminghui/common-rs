#[cfg(feature = "cell")]
pub mod cell;

#[cfg(feature = "mysqlx")]
pub mod mysqlx;
#[cfg(feature = "mysqlx")]
mod mysqlx_test_pool;

#[cfg(feature = "qh")]
pub mod qh;

#[cfg(feature = "sizehmap")]
pub mod sizehmap;

#[cfg(feature = "timer")]
pub mod timer;

#[cfg(feature = "ulog")]
pub mod ulog;

#[cfg(feature = "yaml")]
pub mod yaml;

#[cfg(feature = "ymdhms")]
pub mod ymdhms;

#[cfg(feature = "hq")]
pub mod hq;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
