use std::time::Duration;

use futures::Future;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::time::Instant;

#[derive(Debug)]
pub struct Timer {
    // stop_tx:  Option<oneshot::Sender<u8>>,
    stop_tx:  mpsc::Sender<()>,
    reset_tx: mpsc::Sender<Instant>,
}

impl Timer {
    // pub fn _new<F>(duration: Duration, mut f: F)
    // where
    //     F: 'static + FnMut() + Send,
    // {

    // }

    #[track_caller]
    pub fn new<F>(duration: Duration, f: F) -> Timer
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        // let (stop_tx, stop_rx) = oneshot::channel::<u8>();
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        let (reset_tx, mut reset_rx) = mpsc::channel::<Instant>(2);
        tokio::spawn(async move {
            // println!("timer spawn");
            let sleep = tokio::time::sleep(duration);
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    () = &mut sleep =>{
                        // println!("##: select in sleep");
                        f.await;
                        break;
                    }
                    Some(instant) = reset_rx.recv() =>{
                        sleep.as_mut().reset(instant);
                    }
                    _ = stop_rx.recv() =>{
                        // println!("##: select in stop rx");
                        break;
                    }
                }
            }
            // println!("##: timer is end");
        });
        Timer { stop_tx, reset_tx }
    }

    pub async fn stop(&mut self) {
        if let Err(err) = self.stop_tx.send(()).await {
            println!("#: Timer stop err: {}", err);
        }
    }

    /// 无法在结束后重置, 不实用.
    #[deprecated]
    pub async fn reset(&self, duration: Duration) -> Result<(), SendError<Instant>> {
        let instant = Instant::now() + duration;
        self.reset_tx.send(instant).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use chrono::Local;
    use tokio::time::sleep;

    use super::Timer;

    #[tokio::test]
    async fn test_timer() {
        println!("======: 1 {:?}", Instant::now());
        let a = "this is a string";
        let timer = Timer::new(Duration::from_secs(2), async move {
            println!("this is a function in timer, wait 1s");
            sleep(Duration::from_secs(1)).await;
            println!("this is a function in timer, end wait, {}", a);
        });
        println!("======: 2");
        sleep(Duration::from_secs(4)).await;
        drop(timer);
    }

    #[tokio::test]
    async fn test_timer_stop() {
        println!("======: 1 {:?}", Instant::now());
        let timer = Timer::new(Duration::from_secs(2), async {
            println!("this is a function")
        });
        println!("======: 2");
        let mut timer = timer;
        timer.stop().await;
        sleep(Duration::from_secs(3)).await
    }

    #[tokio::test]
    async fn test_timer_drop() {
        println!("======: 1 {:?}", Instant::now());
        let timer = Timer::new(Duration::from_secs(2), async {
            println!("this is a function")
        });
        println!("======: 2");
        drop(timer);
        sleep(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test_timer_2() {
        // 如果没有变量持有, 这两个timer生成后就会马上停止
        let timer1 = Timer::new(Duration::from_secs(2), async {
            println!("this is a function 1")
        });
        println!("####################");
        let timer2 = Timer::new(Duration::from_secs(2), async {
            println!("this is a function 2")
        });
        sleep(Duration::from_secs(3)).await;
        drop(timer1);
        drop(timer2);
    }

    struct Tmp {
        timer: Timer,
    }

    #[tokio::test]
    async fn test_timer_in_struct_stop() {
        let timer = Timer::new(Duration::from_secs(2), async {
            println!("this is a function")
        });
        let mut tmp = Tmp { timer };
        tmp.timer.stop().await;
        sleep(Duration::from_secs(1)).await;
        let mut a = tmp.timer;
        a.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_timer_in_hashmap() {
        println!("######################");
        let mut hmap = HashMap::new();
        let timer = Timer::new(Duration::from_secs(2), async {
            println!("this is a function 1")
        });
        println!("############ 1");
        hmap.insert("1", timer);
        let timer = Timer::new(Duration::from_secs(2), async {
            println!("this is a function 2")
        });
        println!("############ 2");
        hmap.insert("1", timer);
        println!("############ 3");
        // hmap.remove("1"); //, 从hmap中删除掉后, timer就停掉了.

        sleep(Duration::from_secs(3)).await;
        let timer = hmap.remove("1");
        println!("###: {:?}", timer);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_duration_zero() {
        println!("################");
        let hmap: Arc<Mutex<HashMap<i32, Timer>>> = Arc::default();
        let hmap_move = Arc::clone(&hmap);
        let timer = Timer::new(Duration::from_secs(0), async move {
            println!("1:{:?}", hmap_move.lock().unwrap());
            println!("this is a duration 0 timer");
        });
        println!("!!!!!! 1");
        {
            hmap.lock().unwrap().insert(1, timer);
        }
        println!("!!!!!! 2");
        println!("2: {:?}", hmap.lock().unwrap());
        println!("now 1: {}", Local::now().naive_local());

        tokio::spawn(async {
            println!("this is a spawn");
        });
        for i in 0..=1000000000 {
            if i % 100000000 == 0 {
                println!("#: {:?} {}", Instant::now(), i);
            }
        }
        println!("now 2: {}", Local::now().naive_local());
        sleep(Duration::from_millis(10)).await;
    }

    #[test]
    fn test_duration_zero_2() {
        // let rt = tokio::runtime::Builder::new_multi_thread()
        //     .enable_all()
        //     .build()
        //     .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            println!("################");
            let hmap: Arc<Mutex<HashMap<i32, Timer>>> = Arc::default();
            let hmap_move = Arc::clone(&hmap);
            let timer = Timer::new(Duration::from_secs(0), async move {
                println!("1:{:?}", hmap_move.lock().unwrap());
                println!("this is a duration 0 timer");
            });
            println!("!!!!!! 1");
            {
                hmap.lock().unwrap().insert(1, timer);
            }
            println!("!!!!!! 2");
            println!("2: {:?}", hmap.lock().unwrap());
            println!("now 1: {}", Local::now().naive_local());

            tokio::spawn(async {
                println!("this is a spawn");
            });
            for i in 0..=1000000000 {
                if i % 100000000 == 0 {
                    println!("#: {:?} {}", Instant::now(), i);
                }
            }
            println!("now 2: {}", Local::now().naive_local());
            sleep(Duration::from_millis(10)).await;
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(deprecated)]
    async fn test_timer_reset() {
        let now = Local::now().naive_local();
        println!("now 1:{}", now);
        let timer = Timer::new(Duration::from_secs(2), async {
            let now = Local::now().naive_local();
            println!("now 2:{}", now);
        });
        sleep(Duration::from_secs(1)).await;
        let now = Local::now().naive_local();
        println!("now 1-1:{}  2秒后打印", now);
        // 在这个时间一秒后再打印
        timer.reset(Duration::from_secs(2)).await.unwrap();
        timer.reset(Duration::from_secs(2)).await.unwrap();
        sleep(Duration::from_secs(3)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(deprecated)]
    async fn test_timer_reset_zero() {
        let now = Local::now().naive_local();
        println!("now 1:{}", now);
        let timer = Timer::new(Duration::from_secs(2), async {
            let now = Local::now().naive_local();
            println!("now 2:{}", now);
        });
        sleep(Duration::from_secs(1)).await;
        let now = Local::now().naive_local();
        println!("now 1-1:{}  0秒后打印", now);
        // 在这个时间一秒后再打印
        timer.reset(Duration::from_secs(0)).await.unwrap();
        sleep(Duration::from_secs(3)).await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_let_timer_modify() {
        let mut _timer = Timer::new(Duration::from_secs(2), async {
            println!("this is timer1");
        });
        _timer = Timer::new(Duration::from_secs(2), async {
            println!("this is timer2");
        });
        sleep(Duration::from_secs(3)).await;
    }
}
