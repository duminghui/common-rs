use once_cell::sync::Lazy;
use rayon::{ThreadPool, ThreadPoolBuilder};

mod contention_pool;
mod parser;
pub mod read;
mod splitfields;
mod utils;
pub mod write;

static POOL: Lazy<ThreadPool> = Lazy::new(|| {
    ThreadPoolBuilder::new()
        .num_threads(
            std::thread::available_parallelism()
                .unwrap_or(std::num::NonZeroUsize::new(1).unwrap())
                .get(),
        )
        .thread_name(move |i| format!("csv-{}", i))
        .build()
        .expect("could not spawn threads")
});
