use std::borrow::Cow;
use std::fs;
use std::path::Path;

use rolling_file::{BasicRollingFileAppender, RollingConditionBasic};
use time::macros::format_description;
use time::UtcOffset;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_error::ErrorLayer;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::fmt::format::{DefaultFields, Format, Full};
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, Registry};

use self::tracing_file::TracingFileLayer;

mod tracing_file;

pub struct LogConfig<'a> {
    max_files:         usize,
    level_filter:      LevelFilter,
    target_filters:    Vec<(Cow<'a, str>, LevelFilter)>,
    console_enable:    bool,
    console_line_info: bool,
    console_target:    bool,
    file_enable:       bool,
    file_dir:          Cow<'a, Path>,
    file_name:         Cow<'a, str>,
    file_line_info:    bool,
    file_target:       bool,
    field_files:       Vec<Cow<'a, str>>,
}

impl Default for LogConfig<'_> {
    fn default() -> Self {
        Self {
            max_files:         9,
            level_filter:      LevelFilter::DEBUG,
            target_filters:    Vec::new(),
            console_enable:    true,
            console_line_info: true,
            console_target:    true,
            file_enable:       false,
            file_dir:          Default::default(),
            file_name:         "run.log".into(),
            file_line_info:    true,
            file_target:       true,
            field_files:       Vec::new(),
        }
    }
}

impl<'a> LogConfig<'a> {
    // #[deprecated(note = "Use LogConfig::default()")]
    // pub fn new(
    //     max_files: usize,
    //     level_filter: LevelFilter,
    //     console_enable: bool,
    //     console_line_info: bool,
    //     console_target: bool,
    //     file_enable: bool,
    //     file_line_info: bool,
    //     file_target: bool,
    // ) -> LogConfig {
    //     LogConfig {
    //         max_files,
    //         level_filter,
    //         console_enable,
    //         console_line_info,
    //         console_target,
    //         file_enable,
    //         file_line_info,
    //         file_target,
    //         target_filters: Vec::new(),
    //         log_files: Vec::new(),
    //     }
    // }

    pub fn with_max_files(self, max_files: usize) -> LogConfig<'a> {
        LogConfig { max_files, ..self }
    }

    pub fn with_level_filter(self, level_filter: LevelFilter) -> LogConfig<'a> {
        LogConfig {
            level_filter,
            ..self
        }
    }

    pub fn with_console_enable(self, console_enable: bool) -> LogConfig<'a> {
        LogConfig {
            console_enable,
            ..self
        }
    }

    pub fn with_console_line_info(self, console_line_info: bool) -> LogConfig<'a> {
        LogConfig {
            console_line_info,
            ..self
        }
    }

    pub fn with_console_target(self, console_target: bool) -> LogConfig<'a> {
        LogConfig {
            console_target,
            ..self
        }
    }

    pub fn with_file_enable(self, file_enable: bool) -> LogConfig<'a> {
        LogConfig {
            file_enable,
            ..self
        }
    }

    pub fn with_file_dir(self, dir: &'a str) -> LogConfig<'a> {
        LogConfig {
            file_dir: Cow::Borrowed(Path::new(dir)),
            ..self
        }
    }

    pub fn with_file_name(self, file_name: &'a str) -> LogConfig<'a> {
        LogConfig {
            file_name: file_name.into(),
            ..self
        }
    }

    pub fn with_file_line_info(self, file_line_info: bool) -> LogConfig<'a> {
        LogConfig {
            file_line_info,
            ..self
        }
    }

    pub fn with_file_target(self, file_target: bool) -> LogConfig<'a> {
        LogConfig {
            file_target,
            ..self
        }
    }

    pub fn with_field_files(self, field_files: &'a [&str]) -> LogConfig<'a> {
        LogConfig {
            field_files: field_files.iter().map(|v| (*v).into()).collect::<Vec<_>>(),
            ..self
        }
    }

    pub fn add_target(&mut self, target: &'a str) {
        self.target_filters.push((target.into(), self.level_filter));
    }
}

// linux多线程的环境下, 获取UtcOffset会出错
pub fn tracing_init(config: &LogConfig) -> Option<Vec<WorkerGuard>> {
    // https://time-rs.github.io/book/api/format-description.html
    let time_format =
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]");

    // 这个在linux下时间部分会变成<unknown time>
    // let timer = LocalTime::new(time_format);
    // let utc_offset = UtcOffset::current_local_offset().expect("should get local offset!");
    // 需要设置 (还未测试)
    // [build]
    // rustflags = ["--cfg unsound_local_offset"]

    let utc_offset = UtcOffset::from_hms(8, 0, 0).unwrap();
    let timer = OffsetTime::new(utc_offset, time_format);

    // // 控制台
    // let console_targets = Targets::new()
    // .with_target("sqlx::query", LevelFilter::OFF)
    // .with_target("mio::poll", LevelFilter::TRACE)
    // .not();

    let console_layer = if config.console_enable {
        let layer = fmt::layer()
            // .pretty()
            .with_ansi(true)
            .with_file(config.console_line_info)
            .with_line_number(config.console_line_info)
            .with_target(config.console_target)
            .with_timer(timer.clone());
        Some(layer)
    } else {
        None
    };

    // 文件
    // let timer = LocalTime::new(time_format);
    // 不用trace自带的文件生成
    // let file_appender = rolling::daily(directory, file_name);
    //

    // let directory = directory.as_ref();

    // let file_appender = BasicRollingFileAppender::new(
    //     directory.join(file_name),
    //     RollingConditionBasic::new().daily(),
    //     config.max_files,
    // )
    // .unwrap();

    // let (non_blocking_appender, file_worker_guard) = tracing_appender::non_blocking(file_appender);

    // let file_appender_layer = fmt::layer()
    //     .with_ansi(false)
    //     .with_file(config.file_line_info)
    //     .with_line_number(config.console_line_info)
    //     .with_target(config.file_target)
    //     .with_timer(timer)
    //     .with_writer(non_blocking_appender);

    let (file_append_layer, field_file_layer_vec, guard_vec) = if config.file_enable {
        let _ = fs::create_dir_all(config.file_dir.as_ref());
        let FileAppenderLayerWorkerGuard(file_appender_layer, worker_guard) =
            file_appender_layer_worker_guard(config.file_name.as_ref(), config, timer.clone());
        let mut guard_vec = vec![worker_guard];

        let field_file_layer_vec = if !config.field_files.is_empty() {
            let mut field_file_layer_vec = vec![];
            for log_file in config.field_files.iter() {
                let file_name = format!("{}.log", log_file);
                let FileAppenderLayerWorkerGuard(file_append_layer, worker_guard) =
                    file_appender_layer_worker_guard(file_name, config, timer.clone());
                let log_file_layer = TracingFileLayer::new(file_append_layer, "logfile", log_file);
                field_file_layer_vec.push(log_file_layer);
                guard_vec.push(worker_guard);
            }
            Some(field_file_layer_vec)
        } else {
            None
        };

        (
            Some(file_appender_layer),
            Some(field_file_layer_vec),
            Some(guard_vec),
        )
    } else {
        (None, None, None)
    };

    let targets = if config.target_filters.is_empty() {
        Targets::new().with_default(config.level_filter)
    } else {
        Targets::from_iter(config.target_filters.clone())
    };

    // XXX console_layer放到file_appender_layer和field_file_layer_vec前面, 会影响文件打印的内容.
    Registry::default()
        .with(config.level_filter)
        .with(file_append_layer)
        .with(field_file_layer_vec)
        .with(console_layer)
        .with(targets)
        // ErrorLayer 可以让 color-eyre 获取到 span 的信息
        .with(ErrorLayer::default())
        .init();

    guard_vec
}

struct FileAppenderLayerWorkerGuard<S, T>(
    Layer<S, DefaultFields, Format<Full, OffsetTime<T>>, NonBlocking>,
    WorkerGuard,
);

fn file_appender_layer_worker_guard<P, S, T>(
    file_name: P,
    config: &LogConfig,
    timer: OffsetTime<T>,
) -> FileAppenderLayerWorkerGuard<S, T>
where
    P: AsRef<Path>,
{
    let directory = config.file_dir.as_ref();
    let file_appender = BasicRollingFileAppender::new(
        directory.join(file_name),
        RollingConditionBasic::new().daily(),
        config.max_files,
    )
    .unwrap();

    let (non_blocking_appender, file_worker_guard) = tracing_appender::non_blocking(file_appender);

    let file_appender_layer = fmt::layer()
        .with_ansi(false)
        .with_file(config.file_line_info)
        .with_line_number(config.file_line_info)
        .with_target(config.file_target)
        .with_timer(timer)
        .with_writer(non_blocking_appender);
    FileAppenderLayerWorkerGuard(file_appender_layer, file_worker_guard)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tracing::level_filters::LevelFilter;
    use tracing::{info, span, Level};

    use super::{tracing_init, LogConfig};

    #[test]
    fn test_path() {
        println("/dir1/dir2/dir2/filename", "filename2.txt");
    }

    fn println(dir: impl AsRef<Path>, filename: impl AsRef<Path>) {
        let tmp = dir.as_ref().join(filename);
        println!("{:?}", tmp.as_path());
    }

    #[test]
    fn test_log() {
        let field_files = ["file1", "file2"];

        let log_config = LogConfig::default()
            .with_level_filter(LevelFilter::DEBUG)
            .with_file_enable(true)
            .with_file_dir("./_logs")
            .with_console_line_info(false)
            .with_field_files(&field_files)
            .with_file_line_info(false);

        let _worker_guard_vec = tracing_init(&log_config);

        info!(a = 100, "this is msg 1");
        info!("this is msg 2");
        info!("this is msg 3");

        span!(Level::DEBUG, "xxx", logfile = "file1").in_scope(|| {
            info!("this is msg 1 in file1");
            info!("this is msg 2 in file1");
            info!("this is msg 3 in file1");
        });

        span!(Level::DEBUG, "xxx", logfile = "file2").in_scope(|| {
            info!("this is msg 1 in file2");
            info!("this is msg 2 in file2");
            info!("this is msg 3 in file2");
        });
    }

    #[allow(unused)]
    #[derive(Debug)]
    struct Tmp {
        v1: i32,
        v2: i32,
        v3: i32,
    }

    impl Tmp {
        fn with_v1(self, v1: i32) -> Tmp {
            Tmp { v1, ..self }
        }
    }

    #[test]
    fn test_struct() {
        let tmp = Tmp {
            v1: 100,
            v2: 200,
            v3: 300,
        }
        .with_v1(1000);
        println!("{:?}", tmp);
    }
}
