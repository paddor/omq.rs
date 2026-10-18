//! Multipart messages on the classic socket types.

use super::{DEADLINE, endpoint, options, receive, wait_fanout_ready};
use bytes::Bytes;
use omq_tokio::{
    Context, Endpoint, Message, Options, Payload, PayloadPool, Socket, SocketType,
    options::WorkloadProfile, shm_payload_pool,
};

/// Part sizes per message. Final frames use carrier copies, automatic storage,
/// or pool slots. Earlier frames of 4 KiB or more in total move the whole
/// message into one shared record; smaller ones use the carrier ring.
const SHAPES: &[&[usize]] = &[
    &[16],
    &[0, 0],
    &[0, 1024],
    &[5, 55],
    &[5, 56],
    &[3, 9, 4096],
    &[4095, 0],
    &[4096, 0],
    &[8 * 1024, 8 * 1024],
    &[0, 64 * 1024, 16, 1024],
    &[128 * 1024, 64],
    &[7, 128 * 1024],
    &[1, 2, 3, 4, 5, 1024 * 1024],
];

fn pool() -> PayloadPool {
    shm_payload_pool([(4096, 32), (128 * 1024, 32), (1024 * 1024, 4)]).unwrap()
}

fn pattern(seed: usize, part: usize, size: usize) -> Bytes {
    (0..size)
        .map(|index| (seed * 31 + part * 7 + index) as u8)
        .collect()
}

fn frame(bytes: Bytes, pool: Option<&PayloadPool>) -> Payload {
    match pool {
        Some(pool) if !bytes.is_empty() => pool
            .message(bytes.len(), |frame| frame.copy_from_slice(&bytes))
            .unwrap()
            .last_payload()
            .unwrap(),
        _ => Payload::from_bytes(bytes),
    }
}

/// Frames taken from the producer pool.
#[derive(Clone, Copy)]
enum Pooled {
    None,
    Last,
    All,
}

fn shape(seed: usize, sizes: &[usize], pool: &PayloadPool, pooled: Pooled) -> Message {
    let last = sizes.len() - 1;
    Message::multipart_payloads(sizes.iter().enumerate().map(|(part, size)| {
        let pooled = match pooled {
            Pooled::None => false,
            Pooled::Last => part == last,
            Pooled::All => true,
        };
        frame(pattern(seed, part, *size), pooled.then_some(pool))
    }))
}

/// Every shape with heap frames, a pooled final frame, and only pooled frames.
fn messages(pool: &PayloadPool) -> Vec<Message> {
    [Pooled::None, Pooled::Last, Pooled::All]
        .into_iter()
        .flat_map(|pooled| {
            SHAPES
                .iter()
                .enumerate()
                .map(move |(seed, sizes)| shape(seed, sizes, pool, pooled))
        })
        .collect()
}

async fn connected(
    context: &Context,
    bind: (SocketType, Options),
    connect: (SocketType, Options),
) -> (Socket, Socket) {
    let listener = context.socket(bind.0, bind.1);
    let address = listener.bind(endpoint()).await.unwrap();
    let connector = context.socket(connect.0, connect.1);
    connector.connect(address).await.unwrap();
    connector.wait_connected(1, DEADLINE).await.unwrap();
    listener.wait_connected(1, DEADLINE).await.unwrap();
    (listener, connector)
}

#[tokio::test]
async fn pipeline_pair_and_dealer_keep_every_multipart_shape() {
    let pool = pool();
    for (bind_type, connect_type) in [
        (SocketType::Pull, SocketType::Push),
        (SocketType::Pair, SocketType::Pair),
        (SocketType::Dealer, SocketType::Dealer),
    ] {
        let context = Context::new();
        let (receiver, sender) =
            connected(&context, (bind_type, options()), (connect_type, options())).await;
        for message in messages(&pool) {
            sender.send(message.clone()).await.unwrap();
            let got = receive(&receiver).await;
            assert_eq!(got, message);
            if bind_type != SocketType::Pull {
                receiver.send(got).await.unwrap();
                assert_eq!(receive(&sender).await, message);
            }
        }
        sender.close().await.unwrap();
        receiver.close().await.unwrap();
    }
}

#[tokio::test]
async fn pooled_envelope_frames_stay_shared_until_the_receiver_drops_them() {
    let pool = shm_payload_pool([(8192, 1)]).unwrap();
    let context = Context::new();
    let (receiver, sender) = connected(
        &context,
        (SocketType::Pull, options()),
        (SocketType::Push, options()),
    )
    .await;
    let envelope = pattern(1, 0, 8192);
    let message = Message::multipart_payloads([
        frame(envelope.clone(), Some(&pool)),
        Payload::from_static(b"body"),
    ]);
    sender.send(message).await.unwrap();
    let delivered = receive(&receiver).await;
    assert_eq!(delivered.part_slice(0), Some(envelope.as_ref()));
    assert_eq!(
        pool.available(),
        0,
        "the envelope frame is shared, not copied"
    );
    drop(delivered);
    tokio::time::timeout(DEADLINE, async {
        while pool.available() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    sender.close().await.unwrap();
    receiver.close().await.unwrap();
}

#[tokio::test]
async fn router_prepends_identities_and_routes_multipart_replies() {
    let pool = pool();
    let context = Context::new();
    let (router, dealer) = connected(
        &context,
        (SocketType::Router, options()),
        (
            SocketType::Dealer,
            options().identity(Bytes::from_static(b"dealer")),
        ),
    )
    .await;
    for message in messages(&pool) {
        dealer.send(message.clone()).await.unwrap();
        let mut request = receive(&router).await;
        assert_eq!(request.pop_front().unwrap(), b"dealer".as_slice());
        assert_eq!(request, message);
        router.send_to(b"dealer", request).await.unwrap();
        assert_eq!(receive(&dealer).await, message);
    }
    dealer.close().await.unwrap();
    router.close().await.unwrap();
}

#[tokio::test]
async fn request_reply_envelopes_survive_shm() {
    let pool = pool();
    let context = Context::new();
    // Latency profiles use different REQ and REP receive sinks.
    for profile in [WorkloadProfile::Throughput, WorkloadProfile::Latency] {
        let options = options().workload_profile(profile);
        let (rep, req) = connected(
            &context,
            (SocketType::Rep, options.clone()),
            (SocketType::Req, options.clone()),
        )
        .await;
        for message in messages(&pool) {
            req.send(message.clone()).await.unwrap();
            let request = receive(&rep).await;
            assert_eq!(request, message);
            rep.send(request).await.unwrap();
            assert_eq!(receive(&req).await, message);
        }
        req.close().await.unwrap();
        rep.close().await.unwrap();

        // ROUTER sees the REQ delimiter and must return it.
        let (router, req) = connected(
            &context,
            (SocketType::Router, options.clone()),
            (
                SocketType::Req,
                options.identity(Bytes::from_static(b"req")),
            ),
        )
        .await;
        for message in messages(&pool) {
            req.send(message.clone()).await.unwrap();
            let mut request = receive(&router).await;
            assert_eq!(request.pop_front().unwrap(), b"req".as_slice());
            assert_eq!(request.pop_front().unwrap(), b"".as_slice());
            assert_eq!(request, message);
            router
                .send_to(b"req", Message::with_prefix(Bytes::new(), request))
                .await
                .unwrap();
            assert_eq!(receive(&req).await, message);
        }
        req.close().await.unwrap();
        router.close().await.unwrap();
    }
}

fn topic(topic: &'static [u8], message: &Message) -> Message {
    Message::with_prefix(Bytes::from_static(topic), message.clone())
}

async fn publish_filtered(publisher: &Socket, subscribers: &[&Socket], pool: &PayloadPool) {
    let probe = topic(b"keep", &Message::from_slice(b"probe"));
    let fence = topic(b"keep", &Message::from_slice(b"fence"));
    wait_fanout_ready(publisher, subscribers, probe, fence).await;
    for message in messages(pool) {
        publisher.send(topic(b"drop", &message)).await.unwrap();
        publisher.send(topic(b"keep", &message)).await.unwrap();
        for subscriber in subscribers {
            let mut got = receive(subscriber).await;
            assert_eq!(got.pop_front().unwrap(), b"keep".as_slice());
            assert_eq!(got, message);
        }
    }
}

#[tokio::test]
async fn publishers_filter_multipart_topics_for_shm_and_ipc_subscribers() {
    let pool = pool();
    let context = Context::new();
    let nodrop = Options {
        xpub_nodrop: true,
        ..options()
    };
    let publisher = context.socket(SocketType::Pub, nodrop.clone());
    let shm = publisher.bind(endpoint()).await.unwrap();
    let ipc: Endpoint = format!(
        "ipc:///tmp/omq-shm-multipart-{}.sock",
        rand::random::<u64>()
    )
    .parse()
    .unwrap();
    let ipc = publisher.bind(ipc).await.unwrap();
    let mut subscribers = Vec::new();
    for address in [&shm, &shm, &ipc] {
        let subscriber = context.socket(SocketType::Sub, options());
        subscriber
            .subscribe(Bytes::from_static(b"keep"))
            .await
            .unwrap();
        subscriber.connect(address.clone()).await.unwrap();
        subscriber.wait_connected(1, DEADLINE).await.unwrap();
        subscribers.push(subscriber);
    }
    let borrowed: Vec<_> = subscribers.iter().collect();
    publish_filtered(&publisher, &borrowed, &pool).await;
    for subscriber in subscribers {
        subscriber.close().await.unwrap();
    }
    publisher.close().await.unwrap();

    let raw_publisher = context.socket(SocketType::XPub, nodrop);
    let address = raw_publisher.bind(endpoint()).await.unwrap();
    let raw_subscriber = context.socket(SocketType::XSub, options());
    raw_subscriber.connect(address).await.unwrap();
    raw_subscriber.wait_connected(1, DEADLINE).await.unwrap();
    raw_subscriber
        .send(Message::from_slice(b"\x01keep"))
        .await
        .unwrap();
    assert_eq!(
        receive(&raw_publisher).await,
        Message::from_slice(b"\x01keep")
    );
    publish_filtered(&raw_publisher, &[&raw_subscriber], &pool).await;
    raw_subscriber.close().await.unwrap();
    raw_publisher.close().await.unwrap();
}

/// Envelopes change every 16 messages and every seventh message has three
/// parts, so queued runs exercise copied and referenced batches.
fn queued(sequence: usize, pool: Option<&PayloadPool>) -> Message {
    const SIZES: [usize; 4] = [16, 300, 4096, 32 * 1024];
    let size = SIZES[sequence / 64 % SIZES.len()];
    let mut body = vec![0xa5; size];
    body[..8].copy_from_slice(&(sequence as u64).to_le_bytes());
    let mut parts = vec![Payload::from_bytes(Bytes::from(format!(
        "envelope-{}",
        sequence / 16
    )))];
    if sequence.is_multiple_of(7) {
        parts.push(Payload::from_static(b"extra"));
    }
    parts.push(frame(Bytes::from(body), pool));
    Message::multipart_payloads(parts)
}

#[tokio::test]
async fn queued_multipart_messages_keep_order_envelopes_and_identities() {
    const COUNT: usize = 1024;
    for (bind_type, connect_type) in [
        (SocketType::Pull, SocketType::Push),
        (SocketType::Router, SocketType::Dealer),
        (SocketType::Dealer, SocketType::Router),
    ] {
        let context = Context::new();
        let options = Options::default()
            .send_hwm(256)
            .recv_hwm(256)
            .router_mandatory(true);
        let (receiver, sender) = connected(
            &context,
            (
                bind_type,
                options.clone().identity(Bytes::from_static(b"receiver")),
            ),
            (
                connect_type,
                options.identity(Bytes::from_static(b"sender")),
            ),
        )
        .await;
        let pool = shm_payload_pool([(32 * 1024, 512)]).unwrap();
        let task = tokio::spawn(async move {
            for sequence in 0..COUNT {
                let mut message = receive(&receiver).await;
                if bind_type == SocketType::Router {
                    assert_eq!(message.pop_front().unwrap(), b"sender".as_slice());
                }
                assert_eq!(message, queued(sequence, None), "message {sequence}");
                if sequence.is_multiple_of(64) {
                    tokio::task::yield_now().await;
                }
            }
            receiver.close().await.unwrap();
        });
        for sequence in 0..COUNT {
            let pooled = (sequence / 32).is_multiple_of(2).then_some(&pool);
            let message = queued(sequence, pooled);
            if connect_type == SocketType::Router {
                sender.send_to(b"receiver", message).await.unwrap();
            } else {
                sender.send(message).await.unwrap();
            }
        }
        tokio::time::timeout(DEADLINE, task).await.unwrap().unwrap();
        sender.close().await.unwrap();
    }
}
