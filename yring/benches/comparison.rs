use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const DURATION: Duration = Duration::from_secs(2);

fn yring_bench<T: Copy + Send + 'static>(cap: usize, batch_size: usize, val: T) -> f64 {
    let stop = Arc::new(AtomicBool::new(false));
    let (mut producer, mut consumer) = yring::spsc::<T>(cap);

    let stop2 = stop.clone();
    let sender = thread::spawn(move || {
        let mut pending = 0u64;
        while !stop2.load(Ordering::Relaxed) {
            if producer.push(val).is_ok() {
                pending += 1;
                if pending >= batch_size as u64 {
                    producer.flush();
                    pending = 0;
                }
            } else {
                producer.flush();
                thread::yield_now();
            }
        }
        producer.flush();
    });

    let start = Instant::now();
    let mut received = 0u64;
    while start.elapsed() < DURATION {
        if consumer.prefetch() > 0 {
            while consumer.pop().is_some() {
                received += 1;
            }
            consumer.release();
        } else {
            thread::yield_now();
        }
    }
    stop.store(true, Ordering::Relaxed);
    sender.join().unwrap();

    received as f64 / start.elapsed().as_secs_f64()
}

fn rtrb_per_item<T: Copy + Send + 'static>(cap: usize, val: T) -> f64 {
    let stop = Arc::new(AtomicBool::new(false));
    let (mut producer, mut consumer) = rtrb::RingBuffer::<T>::new(cap);

    let stop2 = stop.clone();
    let sender = thread::spawn(move || {
        while !stop2.load(Ordering::Relaxed) {
            if producer.push(val).is_err() {
                thread::yield_now();
            }
        }
    });

    let start = Instant::now();
    let mut received = 0u64;
    while start.elapsed() < DURATION {
        match consumer.pop() {
            Ok(_) => received += 1,
            Err(_) => thread::yield_now(),
        }
    }
    stop.store(true, Ordering::Relaxed);
    sender.join().unwrap();

    received as f64 / start.elapsed().as_secs_f64()
}

fn rtrb_chunked<T: Copy + Send + 'static>(cap: usize, batch_size: usize, val: T) -> f64 {
    let stop = Arc::new(AtomicBool::new(false));
    let (mut producer, mut consumer) = rtrb::RingBuffer::<T>::new(cap);

    let stop2 = stop.clone();
    let sender = thread::spawn(move || {
        while !stop2.load(Ordering::Relaxed) {
            match producer.write_chunk_uninit(batch_size) {
                Ok(chunk) => {
                    chunk.fill_from_iter(std::iter::repeat_n(val, batch_size));
                }
                Err(_) => thread::yield_now(),
            }
        }
    });

    let start = Instant::now();
    let mut received = 0u64;
    while start.elapsed() < DURATION {
        let avail = consumer.slots();
        if avail > 0 {
            if let Ok(chunk) = consumer.read_chunk(avail) {
                received += chunk.len() as u64;
                chunk.commit_all();
            }
        } else {
            thread::yield_now();
        }
    }
    stop.store(true, Ordering::Relaxed);
    sender.join().unwrap();

    received as f64 / start.elapsed().as_secs_f64()
}

fn crossbeam_bounded<T: Copy + Send + 'static>(cap: usize, val: T) -> f64 {
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = crossbeam_channel::bounded::<T>(cap);

    let stop2 = stop.clone();
    let sender = thread::spawn(move || {
        while !stop2.load(Ordering::Relaxed) {
            if tx.try_send(val).is_err() {
                thread::yield_now();
            }
        }
    });

    let start = Instant::now();
    let mut received = 0u64;
    while start.elapsed() < DURATION {
        match rx.try_recv() {
            Ok(_) => received += 1,
            Err(_) => thread::yield_now(),
        }
    }
    stop.store(true, Ordering::Relaxed);
    sender.join().unwrap();

    received as f64 / start.elapsed().as_secs_f64()
}

fn kanal_bounded<T: Copy + Send + 'static>(cap: usize, val: T) -> f64 {
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = kanal::bounded::<T>(cap);

    let stop2 = stop.clone();
    let sender = thread::spawn(move || {
        while !stop2.load(Ordering::Relaxed) {
            match tx.try_send(val) {
                Ok(true) => {}
                Ok(false) => thread::yield_now(),
                Err(_) => break,
            }
        }
    });

    let start = Instant::now();
    let mut received = 0u64;
    while start.elapsed() < DURATION {
        match rx.try_recv() {
            Ok(Some(_)) => received += 1,
            Ok(None) => thread::yield_now(),
            Err(_) => break,
        }
    }
    stop.store(true, Ordering::Relaxed);
    sender.join().unwrap();

    received as f64 / start.elapsed().as_secs_f64()
}

fn run_suite<T: Copy + Send + 'static>(label: &str, cap: usize, batch: usize, val: T) {
    println!("--- {label} ---");

    let m = |x: f64| x / 1_000_000.0;

    let r = yring_bench(cap, 1, val);
    println!("  yring          (batch=1  )  {:>7.1}M items/s", m(r));

    let r = yring_bench(cap, batch, val);
    println!(
        "  yring          (batch={batch:<3})  {:>7.1}M items/s",
        m(r)
    );

    let r = rtrb_per_item(cap, val);
    println!("  rtrb per-item              {:>7.1}M items/s", m(r));

    let r = rtrb_chunked(cap, batch, val);
    println!(
        "  rtrb chunked   (batch={batch:<3})  {:>7.1}M items/s",
        m(r)
    );

    let r = crossbeam_bounded(cap, val);
    println!("  crossbeam bounded          {:>7.1}M items/s", m(r));

    let r = kanal_bounded(cap, val);
    println!("  kanal bounded sync         {:>7.1}M items/s", m(r));

    println!();
}

fn main() {
    let cap = 1024;
    let batch = 64;

    println!("SPSC comparison (2s per config, cap={cap}, batch={batch})\n");

    run_suite("u64 (8 bytes)", cap, batch, 0u64);
    run_suite("[u8; 32] (32 bytes)", cap, batch, [0u8; 32]);
    run_suite("[u8; 64] (64 bytes)", cap, batch, [0u8; 64]);
    run_suite("[u8; 128] (128 bytes)", cap, batch, [0u8; 128]);
}
