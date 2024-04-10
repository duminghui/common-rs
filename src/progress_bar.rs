use std::future::Future;
use std::time::{Duration, Instant};

use indicatif::{HumanCount, HumanDuration, MultiProgress, ProgressBar, ProgressStyle};
use log::{error, info};
use rand::Rng;
use tokio::task::JoinHandle;

use crate::AResult;

fn progress_bar(len: u64) -> ProgressBar {
    let process_style = ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] ({pos}/{len}|{percent:>2}%)",
    )
    .unwrap();

    ProgressBar::new(len).with_style(process_style)
}

fn spinner() -> ProgressBar {
    let spinner_style =
        ProgressStyle::with_template("{spinner:.green} {prefix:.bold.dim} {wide_msg}")
            .unwrap()
            .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ");
    ProgressBar::new(0).with_style(spinner_style)
}

pub async fn parallel<T, F, FnOut, FnOutT>(
    par_flag: &str,
    data_vec: Vec<T>,
    parallel_limit: usize,
    progress_bar_share_prefix: &str,
    f: F,
) -> AResult<Vec<FnOutT>>
where
    T: std::fmt::Debug + Send + 'static,
    F: Fn(T, ProgressBar, ProgressBar) -> FnOut,
    F: Send + Sync + Clone + 'static,
    FnOut: Future<Output = AResult<FnOutT>> + Send,
    FnOutT: Send + 'static,
{
    let start = Instant::now();

    let data_len = data_vec.len();

    info!("{} [{}] 开始", par_flag, HumanCount(data_len as u64));

    if data_len == 0 {
        info!("结束: {} [{}] {:.3?}", par_flag, data_len, start.elapsed());
        return Ok(Vec::new());
    }

    let parallel_limit = if parallel_limit == 0 {
        1
    } else {
        parallel_limit
    };

    let parallel_limit = if data_len < parallel_limit {
        data_len
    } else {
        parallel_limit
    };

    let m = MultiProgress::new();

    let mut rng = rand::thread_rng();

    let pb_progress = m.add(progress_bar(data_len as u64));
    pb_progress.enable_steady_tick(Duration::from_millis(rng.gen_range(200..300)));

    let mut pb_task_vec = vec![pb_progress.clone()];

    let pb_idx_padding = format!("{}", parallel_limit).len();
    let idx_padding = format!("{}", data_len).len();

    for i in 1..=parallel_limit {
        let pb = m
            .add(spinner())
            .with_prefix(format!(
                "[{:pb_idx_padding$}][{:idx_padding$}/{:idx_padding$}]",
                i, "-", data_len
            ))
            .with_message("");
        pb.enable_steady_tick(Duration::from_millis(rng.gen_range(200..300)));
        let pb = m.add(pb);
        pb_task_vec.push(pb)
    }

    let pb_share = m
        .add(spinner())
        .with_prefix(format!("[{}]", progress_bar_share_prefix));
    pb_share.enable_steady_tick(Duration::from_millis(rng.gen_range(200..300)));
    pb_task_vec.push(pb_share.clone());

    let mut join_recv_handlers: Vec<JoinHandle<AResult<_>>> = Vec::with_capacity(parallel_limit);

    let (tx, rx) = async_channel::bounded::<(usize, T)>(1);

    let result_vec_cap = data_len / parallel_limit + 1;

    for task_idx in 0..parallel_limit {
        let pb_progress = pb_progress.clone();
        let task_idx = task_idx + 1;
        let pb_task = unsafe { pb_task_vec.get_unchecked(task_idx) }.clone();

        let pb_share = pb_share.clone();

        let f = f.clone();
        let rx = rx.clone();

        join_recv_handlers.push(tokio::spawn(async move {
            let mut result_vec = Vec::with_capacity(result_vec_cap);
            while let Ok((data_idx, data)) = rx.recv().await {
                pb_task.set_prefix(format!(
                    "[{:pb_idx_padding$}][{:idx_padding$}/{:idx_padding$}]",
                    task_idx, data_idx, data_len
                ));
                // exec
                let r = f(data, pb_task.clone(), pb_share.clone()).await?;
                result_vec.push(r);
                // m.println(&msg).unwrap();
                // pb.set_message(msg);
                pb_progress.inc(1);
                // 让线程能及时的分配出去
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }
            Ok(result_vec)
        }));
    }

    drop(rx);

    let par_flag_async = par_flag.to_string().clone();
    let join_send_handler = tokio::spawn(async move {
        for (data_idx, data) in data_vec.into_iter().enumerate() {
            if let Err(err) = tx.send((data_idx + 1, data)).await {
                error!("{} send error: {}", par_flag_async, err);
                break;
            }
        }
    });

    let mut result_vec = Vec::with_capacity(data_len);

    for handler in join_recv_handlers {
        let r = handler.await??;
        result_vec.extend(r)
    }

    join_send_handler.await?;

    pb_progress.finish_with_message("finish");

    for pb in pb_task_vec {
        m.remove(&pb);
    }

    // 把进度条从控制台删除
    // m.clear().unwrap();

    let elapsed = start.elapsed();
    info!(
        "{} [{}] 结束 {:.3?} {:#}",
        par_flag,
        HumanCount(data_len as u64),
        elapsed,
        HumanDuration(elapsed)
    );
    info!("==================");

    Ok(result_vec)
}
