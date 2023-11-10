#[cfg(feature = "cell")]
pub mod cell;
#[cfg(feature = "hq")]
pub mod hq;
#[cfg(feature = "mysqlx")]
pub mod mysqlx;
#[cfg(feature = "mysqlx")]
mod mysqlx_test_pool;
#[cfg(feature = "qh")]
pub mod qh;
#[cfg(feature = "redis")]
pub mod redis;
pub mod running;
#[cfg(feature = "serde_extend")]
pub mod serde;
#[cfg(feature = "sizehmap")]
pub mod sizehmap;
#[cfg(feature = "timer")]
pub mod timer;
#[cfg(feature = "ulog")]
pub mod ulog;
#[cfg(feature = "utoml")]
pub mod utoml;
#[cfg(feature = "yaml")]
pub mod yaml;
#[cfg(feature = "ymdhms")]
pub mod ymdhms;

pub type AResult<T> = eyre::Result<T>;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
