use super::*;
use crate::socket::monitor::MonitorPublisher;
use crate::transport::setup::Admission;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn raw_accepts_respect_both_caps_and_release_every_reservation_on_drop() {
    for (socket_limit, expected) in [(3, 3), (64, 32)] {
        let admission = Admission::new(socket_limit);
        let endpoint = "ws://127.0.0.1:0/".parse().unwrap();

        let mut listener = bind(
            &endpoint,
            None,
            AcceptSetup {
                admission: admission.clone(),
                timeout: Duration::from_secs(30),
                cancel: CancellationToken::new(),
                monitor: MonitorPublisher::new(omq_proto::options::DEFAULT_MONITOR_CAPACITY),
                io_pool: crate::context::IoPoolHandle::none(),
            },
            &omq_proto::Options::default(),
        )
        .await
        .unwrap();

        let address = listener.inner.local_addr().unwrap();
        let mut clients = Vec::new();

        for _ in 0..=expected {
            clients.push(TcpStream::connect(address).await.unwrap());
            let (stream, peer) = listener.inner.accept().await.unwrap();
            listener.start_setup(stream, peer);
        }

        assert_eq!(listener.queue.len(), expected);
        let free: Vec<_> = (expected..socket_limit)
            .map(|_| admission.try_acquire().unwrap())
            .collect();
        assert!(admission.try_acquire().is_none());
        drop(free);
        drop(listener);
        let released: Vec<_> = (0..socket_limit)
            .map(|_| admission.try_acquire().unwrap())
            .collect();
        assert_eq!(released.len(), socket_limit);
    }
}
