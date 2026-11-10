use super::*;
use tokio::io::AsyncReadExt;

async fn connection() -> (AnyConn, tokio::net::TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (stream, address) = listener.accept().await.unwrap();

    (
        AnyConn::ByteStream {
            stream: crate::socket::dispatch::AnyStream::Tcp(stream),
            peer_ident: omq_proto::monitor::PeerIdent::Socket(address),
            leftover: bytes::Bytes::new(),
            setup: None,
        },
        client,
    )
}

#[tokio::test]
async fn full_mailbox_closes_excess_transport_and_releases_its_admission() {
    let (tx, mut rx) = mpsc::channel(1);
    let admission = Admission::new(2);

    let task = ListenerTask {
        options: Arc::new(Options::default()),
        endpoint: "tcp://127.0.0.1:1".parse().unwrap(),
        cancel: CancellationToken::new(),
        tx,
        admission: Some(admission.clone()),
        monitor: MonitorPublisher::new(omq_proto::options::DEFAULT_MONITOR_CAPACITY),
    };

    let (mut first, _first_client) = connection().await;
    assert!(task.admit(&mut first));
    assert!(task.deliver(first));

    let (mut excess, mut excess_client) = connection().await;
    assert!(task.admit(&mut excess));
    assert!(admission.try_acquire().is_none());

    assert!(
        task.deliver(excess),
        "full mailbox must not stop the listener"
    );
    assert!(admission.try_acquire().is_some());

    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), excess_client.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );

    drop(rx.recv().await);
    let _first_slot = admission.try_acquire().unwrap();
    let _second_slot = admission.try_acquire().unwrap();
    assert!(admission.try_acquire().is_none());
}
