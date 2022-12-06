use std::fs;
use std::path::Path;

use rolling_file::{BasicRollingFileAppender, RollingConditionBasic};
use time::macros::format_description;
use time::UtcOffset;
use tracing::metadata::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_error::ErrorLayer;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, Registry};

pub struct LogConfig {
    max_files: usize,
    level_filter: LevelFilter,
    console_line_info: bool,
    console_target: bool,
    file_line_info: bool,
    file_target: bool,
    target_filters: Vec<(String, LevelFilter)>,
}

impl LogConfig {
    pub fn new(
        max_files: usize,
        level_filter: LevelFilter,
        console_line_info: bool,
        console_target: bool,
        file_line_info: bool,
        file_target: bool,
    ) -> LogConfig {
        LogConfig {
            max_files,
            level_filter,
            console_line_info,
            console_target,
            file_line_info,
            file_target,
            target_filters: Vec::new(),
        }
    }

    pub fn add_target(&mut self, target: &str) {
        self.target_filters
            .push((target.into(), self.level_filter.clone()));
    }
}

// linux多线程的环境下, 获取UtcOffset会出错
pub fn init_tracing(
    directory: impl AsRef<Path>,
    file_name: impl AsRef<Path>,
    config: &LogConfig,
) -> WorkerGuard {
    let time_format = format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
    // 这个在linux下时间部分会变成<unknown time>
    // let timer = LocalTime::new(time_format);
    // let utc_offset = UtcOffset::current_local_offset().expect("should get local offset!");
    // 需要手工设置
    let utc_offset = UtcOffset::from_hms(8, 0, 0).unwrap();
    let timer = OffsetTime::new(utc_offset, time_format);

    // // 控制台
    // let console_targets = Targets::new()
    //     .with_target("sqlx::query", LevelFilter::OFF)
    //     .with_target("mio::poll", LevelFilter::TRACE)
    //     .not();

    let console_layer = fmt::layer()
        // .pretty()
        .with_ansi(true)
        .with_file(config.console_line_info)
        .with_line_number(config.console_line_info)
        .with_target(config.console_target)
        .with_timer(timer.clone());
    // .with_filter(console_targets);

    // 文件
    // let timer = LocalTime::new(time_format);
    // 不用trace自带的文件生成
    // let file_appender = rolling::daily(directory, file_name);

    let directory = directory.as_ref();

    let _ = fs::create_dir_all(directory);

    let file_appender = BasicRollingFileAppender::new(
        directory.join(file_name),
        RollingConditionBasic::new().daily(),
        config.max_files,
    )
    .unwrap();

    let (non_blocking_appender, file_worker_guard) = tracing_appender::non_blocking(file_appender);

    // let file_appender_targets = Targets::new()
    //     .with_target("sqlx::query", LevelFilter::OFF)
    //     .with_target("mio::poll", LevelFilter::TRACE)
    //     .not();

    let file_appender_layer = fmt::layer()
        .with_ansi(false)
        .with_file(config.file_line_info)
        .with_line_number(config.console_line_info)
        .with_target(config.file_target)
        .with_timer(timer)
        .with_writer(non_blocking_appender);
    // .with_filter(file_appender_targets);

    let targets = if config.target_filters.is_empty() {
        Targets::new().with_default(config.level_filter.clone())
    } else {
        Targets::from_iter(config.target_filters.clone())
    };

    Registry::default()
        .with(config.level_filter)
        .with(console_layer)
        .with(file_appender_layer)
        .with(targets)
        .with(ErrorLayer::default())
        .init();

    file_worker_guard
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    #[test]
    fn test_path() {
        println("/dir1/dir2/dir2/filename", "filename2.txt");
    }

    fn println(dir: impl AsRef<Path>, filename: impl AsRef<Path>) {
        let tmp = dir.as_ref().join(filename);
        println!("{:?}", tmp.as_path());
    }
}
