//! A proxy whose target is muted waits in the target's send instead of
//! polling. Own test binary: it measures process CPU time.
#![cfg(target_os = "linux")]

mod test_support;

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::{Context, Message, Options, SocketType, TrySendError};

fn cpu_time() -> Duration {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage fills the provided struct.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(rc, 0, "getrusage");
    // SAFETY: getrusage succeeded.
    let usage = unsafe { usage.assume_init() };
    let micros = |t: libc::timeval| t.tv_sec as u64 * 1_000_000 + t.tv_usec as u64;
    Duration::from_micros(micros(usage.ru_utime) + micros(usage.ru_stime))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_into_muted_nodrop_publisher_does_not_spin() {
    let ctx = Context::current();
    let options = Options::default().send_hwm(16).recv_hwm(16);
    let frontend = ctx.socket(SocketType::Pull, options.clone());
    let frontend_endpoint = frontend.bind("inproc://proxy-idle-frontend").await.unwrap();
    let backend = ctx.socket(
        SocketType::Pub,
        Options {
            xpub_nodrop: true,
            ..options.clone()
        },
    );
    let backend_endpoint = backend.bind(test_support::tcp_loopback(0)).await.unwrap();
    // A wire subscriber that never reads mutes the publisher.
    let subscriber = ctx.socket(SocketType::Sub, options.clone());
    subscriber.subscribe(Bytes::new()).await.unwrap();
    subscriber.connect(backend_endpoint).await.unwrap();
    backend
        .wait_subscribed(1, Duration::from_secs(5))
        .await
        .unwrap();
    let producer = ctx.socket(SocketType::Push, options);
    producer.connect(frontend_endpoint).await.unwrap();

    let proxy = tokio::spawn(omq_tokio::proxy::proxy(frontend, backend, None));
    let body = Bytes::from(vec![1; 64 * 1024]);
    // Fill until the proxy stops taking input: it then holds a pending send.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut full_since = None;
    loop {
        assert!(Instant::now() < deadline, "proxy never backpressured");
        match producer.try_send(Message::single(body.clone())) {
            Ok(()) => full_since = None,
            Err(TrySendError::Full(_)) => {
                let since = *full_since.get_or_insert_with(Instant::now);
                if since.elapsed() > Duration::from_millis(200) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(error) => panic!("producer send failed: {error}"),
        }
    }

    let idle = Duration::from_millis(300);
    let before = cpu_time();
    tokio::time::sleep(idle).await;
    let burned = cpu_time().saturating_sub(before);
    assert!(
        burned < idle / 10,
        "muted proxy burned {burned:?} CPU in {idle:?}"
    );

    // Reading resumes forwarding.
    tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
        .await
        .expect("subscriber receives after backpressure")
        .unwrap();
    proxy.abort();
}
