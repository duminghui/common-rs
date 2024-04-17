use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_channel::Sender;
use log::{error, info};
use sqlx::MySqlPool;

use super::batch_exec::{BatchExec, SqlEntity};
use crate::AResult;

static MERGER: OnceLock<BatchExecMerger> = OnceLock::new();

#[derive(Debug)]
pub struct BatchExecMerger {
    sender: Sender<SqlEntity>,
}

impl BatchExecMerger {
    pub fn start_store_thread(pool: Arc<MySqlPool>, threshold: u16, tick_millis: u64) {
        let (sender, rx) = async_channel::unbounded::<SqlEntity>();
        tokio::spawn(async move {
            info!("[BatchExecMerger] Thrad start...");
            let mut interval = tokio::time::interval(Duration::from_millis(tick_millis));
            let mut batch_exec = BatchExec::new(pool, threshold);
            loop {
                tokio::select! {
                    Ok(entity) = rx.recv() => {
                        batch_exec.add(entity);
                        let exec_info = batch_exec.execute_threshold().await;
                        if let Err(err) = exec_info {
                            error!("[BatchExecMerger] err: {}", err);
                        }else {
                            let exec_info = exec_info.unwrap();
                            if exec_info.is_exec() {
                                info!("[BatchExecMerger] {}", exec_info);
                            }
                        }
                    }
                    _ =  interval.tick() => {
                        let exec_info = batch_exec.execute_all().await;
                        if let Err(err) = exec_info {
                            error!("[BatchExecMerger] err: {}", err);
                        }else {
                            let exec_info = exec_info.unwrap();
                            if exec_info.is_exec() {
                                info!("[BatchExecMerger] {}", exec_info);
                            }
                        }
                    }
                    else => break,
                }
            }

            error!("[BatchExecMerger] !!!!!! Thread End !!!!!!")
        });

        let merger = BatchExecMerger { sender };
        MERGER.set(merger).unwrap();
    }

    pub async fn add_sql_entity(entity: SqlEntity) -> AResult<()> {
        MERGER.get().unwrap().sender.send(entity).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::sleep;

    #[tokio::test]
    async fn test_3() {
        let (sender, rx) = async_channel::unbounded::<String>();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    Ok(msg) = rx.recv() => {
                        println!("get: {}",msg);
                    }
                    _  = interval.tick() => {
                        println!("tick");
                    }
                    else => {
                        break;
                    }
                }
            }
        });

        for i in 0..50 {
            sender.send(format!("{}", i)).await.unwrap();
            sleep(Duration::from_millis(200)).await;
        }
    }

    #[tokio::test]
    async fn test_2() {
        let mut interval1 = tokio::time::interval(Duration::from_millis(500));
        let mut interval2 = tokio::time::interval(Duration::from_millis(200));

        let mut count = 0;
        let count_stop = 50;

        loop {
            tokio::select! {
                biased;
                _ = interval1.tick() => {
                   count += 1;
                   println!("ticker 1: {}", count) ;
                   if count >= count_stop {
                        break;
                   }
                }
                _ = interval2.tick() => {
                   count += 1;
                   println!("ticker 2: {}", count) ;
                   if count >= count_stop {
                        break;
                   }
                }
                else => break,
            }
        }
        println!("xxxxxxxxxxxxx");
    }

    #[tokio::test]
    async fn test_1() {
        use tokio_stream::StreamExt;
        let mut stream1 = tokio_stream::iter(vec![1, 2, 3]);
        let mut stream2 = tokio_stream::iter(vec![4, 5, 6]);

        let mut values = vec![];

        loop {
            tokio::select! {
            // select! {
                Some(v) = stream1.next() => {
                    println!("stream1");
                    values.push(v)
                },
                Some(v) = stream2.next() => {
                    println!("stream2");
                    sleep(Duration::from_millis(1000)).await;
                    values.push(v);
                },
                else => break,
                // complete => break,
            }
        }

        values.sort();
        assert_eq!(&[1, 2, 3, 4, 5, 6], &values[..]);
    }
}
