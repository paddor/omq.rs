use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use super::io_pool::{IoThreadPool, fit_context_name};

fn named_pool(io_threads: usize, name: &str) -> Arc<IoThreadPool> {
    IoThreadPool::new_with_context_name(io_threads, Some(name.to_owned()))
}

/// A context that owns `io_threads` IO threads, and its pool.
fn owned_context(io_threads: usize) -> (super::Context, Arc<IoThreadPool>) {
    let context = super::Context::with_config(super::ContextConfig { io_threads });
    let pool = context.inner.io_pool_handle().pool.unwrap();
    (context, pool)
}

/// Assignment load per thread, control runtime first.
fn thread_loads(pool: &IoThreadPool) -> Vec<usize> {
    pool.threads
        .iter()
        .map(|thread| thread.load.load(super::Ordering::Relaxed))
        .collect()
}

fn thread_name(pool: &IoThreadPool, index: usize) -> String {
    let (name_tx, name_rx) = mpsc::channel();
    drop(pool.threads[index].handle.spawn(async move {
        name_tx
            .send(thread::current().name().unwrap().to_owned())
            .unwrap();
    }));

    name_rx.recv_timeout(Duration::from_secs(1)).unwrap()
}

#[cfg(unix)]
fn sigpipe_blocked() -> bool {
    use nix::sys::signal::{SigSet, Signal};

    SigSet::thread_get_mask().unwrap().contains(Signal::SIGPIPE)
}

#[cfg(unix)]
#[test]
fn io_threads_block_sigpipe() {
    let pool = named_pool(2, "sigpipe");
    for thread in &pool.threads {
        let (tx, rx) = mpsc::channel();
        let blocking_tx = tx.clone();

        drop(thread.handle.spawn(async move {
            tx.send(sigpipe_blocked()).unwrap();
        }));

        drop(thread.handle.spawn_blocking(move || {
            blocking_tx.send(sigpipe_blocked()).unwrap();
        }));

        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
    }

    pool.shutdown();
}

#[test]
fn single_io_thread_shares_control_runtime() {
    let pool = named_pool(1, "single");
    assert_eq!(pool.threads.len(), 1);
    assert_eq!(pool.thread_count(), 1);
    assert_eq!(pool.assign_thread(), 0);
    assert_eq!(thread_name(&pool, 0), "single/IO/0");
    pool.shutdown();
}

#[tokio::test]
async fn io_pool_distinguishes_owned_and_borrowed_runtimes() {
    let borrowed = super::Context::current();
    assert!(!borrowed.inner.io_pool_handle().owns_runtime());

    for io_threads in [1, 2] {
        let owned = super::Context::with_config(super::ContextConfig { io_threads });
        assert!(owned.inner.io_pool_handle().owns_runtime());
    }
}

#[test]
fn configured_count_excludes_control_runtime() {
    let pool = named_pool(3, "multi");

    assert_eq!(pool.threads.len(), 4);
    assert_eq!(pool.thread_count(), 3);
    assert_eq!(pool.assign_thread(), 0);
    assert_eq!(pool.assign_thread(), 1);
    assert_eq!(pool.assign_thread(), 2);

    assert_eq!(thread_name(&pool, 0), "multi/Ctrl");
    assert_eq!(thread_name(&pool, 1), "multi/IO/0");
    assert_eq!(thread_name(&pool, 2), "multi/IO/1");
    assert_eq!(thread_name(&pool, 3), "multi/IO/2");

    pool.shutdown();
}

#[test]
fn contexts_get_distinct_default_thread_names() {
    let first = IoThreadPool::new_with_context_name(1, None);
    let second = IoThreadPool::new_with_context_name(1, None);
    let first_name = thread_name(&first, 0);
    let second_name = thread_name(&second, 0);

    assert_ne!(first_name, second_name);
    assert!(first_name.starts_with("OMQ"));
    assert!(first_name.ends_with("/IO/0"));
    assert!(second_name.starts_with("OMQ"));
    assert!(second_name.ends_with("/IO/0"));

    first.shutdown();
    second.shutdown();
}

#[test]
fn long_context_names_preserve_thread_role() {
    let pool = named_pool(3, "orders-service");
    assert_eq!(fit_context_name("orders-service", 3), "orders-ser");
    assert_eq!(thread_name(&pool, 0), "orders-ser/Ctrl");
    assert_eq!(thread_name(&pool, 1), "orders-ser/IO/0");
    assert_eq!(thread_name(&pool, 3), "orders-ser/IO/2");
    pool.shutdown();
}

#[test]
fn eight_byte_context_name_keeps_its_full_prefix() {
    let pool = named_pool(2, "test/omq");
    assert_eq!(thread_name(&pool, 0), "test/omq/Ctrl");
    assert_eq!(thread_name(&pool, 1), "test/omq/IO/0");
    assert_eq!(thread_name(&pool, 2), "test/omq/IO/1");
    pool.shutdown();
}

#[test]
#[should_panic(expected = "context name must be non-empty and contain no NUL byte")]
fn explicit_context_name_rejects_nul() {
    let _ =
        super::Context::with_config_and_name(super::ContextConfig { io_threads: 1 }, "bad\0prefix");
}

#[tokio::test]
async fn socket_teardown_releases_inproc_io_assignments() {
    let (context, pool) = owned_context(2);
    let receiver = context.socket(crate::SocketType::Pull, crate::Options::default());
    let sender = context.socket(crate::SocketType::Push, crate::Options::default());
    let endpoint: crate::Endpoint = "inproc://io-assignment-release".parse().unwrap();

    receiver.bind(endpoint).await.unwrap();
    sender
        .connect("inproc://io-assignment-release")
        .await
        .unwrap();

    sender
        .send(crate::Message::single("assigned"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(thread_loads(&pool).iter().sum::<usize>(), 2);
    let mut sender_monitor = sender.monitor();
    let mut receiver_monitor = receiver.monitor();
    sender
        .close_with_linger(Some(Duration::ZERO))
        .await
        .unwrap();
    receiver
        .close_with_linger(Some(Duration::ZERO))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        for monitor in [&mut sender_monitor, &mut receiver_monitor] {
            while !matches!(monitor.recv().await.unwrap(), crate::MonitorEvent::Closed) {}
        }
    })
    .await
    .expect("socket teardown did not finish");

    assert_eq!(
        thread_loads(&pool),
        vec![0, 0, 0],
        "socket teardown leaked IO assignment load"
    );
}

#[tokio::test]
async fn raw_stream_reserves_a_data_lane_for_its_lifetime() {
    let (context, pool) = owned_context(2);
    let socket = context.socket(crate::SocketType::Stream, crate::Options::default());

    let endpoint = socket
        .bind("tcp://127.0.0.1:0".parse::<crate::Endpoint>().unwrap())
        .await
        .unwrap();
    let remote = tokio::net::TcpStream::connect(endpoint.to_string().trim_start_matches("tcp://"))
        .await
        .unwrap();
    socket.recv().await.unwrap();

    let loads = thread_loads(&pool);
    assert_eq!(loads[0], 0);
    assert_eq!(loads[1..].iter().sum::<usize>(), 1);

    drop(remote);
    socket
        .close_with_linger(Some(Duration::ZERO))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        while thread_loads(&pool).iter().any(|&load| load != 0) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn io_lease_releases_unpolled_aborted_and_panicked_tasks() {
    let (context, pool) = owned_context(2);
    let handle = context.inner.io_pool_handle();
    let load = || thread_loads(&pool).iter().sum::<usize>();

    let assignment = handle.reserve_thread();
    assert_eq!(load(), 1);

    let future = async move {
        let _assignment = assignment;
        std::future::pending::<()>().await;
    };

    drop(future);
    assert_eq!(load(), 0);

    let assignment = handle.reserve_thread();
    let task = handle.spawn_on(assignment.index(), async move {
        let _assignment = assignment;
        std::future::pending::<()>().await;
    });
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(load(), 0);

    let assignment = handle.reserve_thread();
    let task = handle.spawn_on(assignment.index(), async move {
        let _assignment = assignment;
        panic!("test IO driver panic");
    });
    assert!(task.await.unwrap_err().is_panic());
    assert_eq!(load(), 0);
}
