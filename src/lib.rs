#[cfg(feature = "cell")]
pub mod cell;
pub mod eyre_ext;
#[cfg(feature = "file")]
pub mod file;
#[cfg(feature = "hq")]
pub mod hq;
#[cfg(feature = "human")]
pub mod human;
#[cfg(feature = "mysqlx")]
pub mod mysqlx;
#[cfg(feature = "mysqlx")]
mod mysqlx_test_pool;
#[cfg(feature = "path-plain")]
pub mod path_plain;
#[cfg(feature = "progress-bar")]
pub mod progress_bar;
#[cfg(feature = "qh")]
pub mod qh;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "running")]
pub mod running;
#[cfg(feature = "serde-extend")]
pub mod serde_extend;
#[cfg(feature = "sizehmap")]
pub mod sizehmap;
#[cfg(feature = "sql-loader")]
pub mod sql_loader;
#[cfg(feature = "timer")]
pub mod timer;
#[cfg(feature = "toml")]
pub mod toml;
#[cfg(feature = "tracing-init")]
pub mod tracing_init;
#[cfg(feature = "yaml")]
pub mod yaml;
#[cfg(feature = "ymdhms")]
pub mod ymdhms;

pub use eyre;
pub type AResult<T> = eyre::Result<T>;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
