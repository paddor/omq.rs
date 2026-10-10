//! Allocation checks for the bounded pool and borrowed wire codec.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use omq_proto::dart;
use omq_tokio::diagnostics::dart_stats;
use omq_tokio::message::Message;
use omq_tokio::transport::dart::{DartIo, ReceiveBatch};
use omq_tokio::{Error, Options, PayloadPool, Socket, SocketType};

struct CountingAllocator;

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
    static TRACE_FIRST: Cell<bool> = const { Cell::new(false) };
}

fn allocated() {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value + 1));
            if value == 0 && TRACE_FIRST.with(|trace| trace.replace(false)) {
                count.set(None);
                eprintln!(
                    "first allocation: {}",
                    std::backtrace::Backtrace::force_capture()
                );
                count.set(Some(value + 1));
            }
        }
    });
}

async fn warm_runtime(task_count: usize) {
    // Initialize Tokio's deferred-waker storage for the socket and endpoint
    // tasks before counting steady-state allocations on this runtime thread.
    let tasks: Vec<_> = (0..task_count)
        .map(|_| tokio::spawn(tokio::task::yield_now()))
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
}

async fn radio_delivery(pool: &PayloadPool, radio: &Socket, dishes: &[Socket], size: usize) {
    let mut buffer = pool.try_buffer(1).unwrap();
    buffer.writable()[..size].fill(7);
    buffer.set_len(size).unwrap();
    radio
        .send(Message::with_prefix(
            bytes::Bytes::from_static(b"group"),
            buffer.into_message(),
        ))
        .await
        .unwrap();
    for dish in dishes {
        let received = dish.recv().await.unwrap();
        assert_eq!(received.part_slice(0), Some(b"group".as_slice()));
        assert_eq!(received.part_slice(1).unwrap().len(), size);
        assert!(
            received
                .part_slice(1)
                .unwrap()
                .iter()
                .all(|&byte| byte == 7)
        );
    }
}

#[tokio::test]
async fn native_radio_fanout_above_inline_target_capacity_does_not_allocate() {
    let mut counts = [[0; 2]; 2];
    for (policy, nodrop) in [false, true].into_iter().enumerate() {
        let pool = PayloadPool::new([(2048, 8192)]).unwrap();
        let radio = Socket::new(
            SocketType::Radio,
            Options {
                xpub_nodrop: nodrop,
                ..Options::default()
            },
        );
        let mut dishes = Vec::with_capacity(12);
        for _ in 0..12 {
            let dish = Socket::new(
                SocketType::Dish,
                Options::default().recv_payload_pool(PayloadPool::new([(2048, 8192)]).unwrap()),
            );
            dish.join(bytes::Bytes::from_static(b"group"))
                .await
                .unwrap();
            let endpoint = dish
                .bind("dart://127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            radio.connect(endpoint).await.unwrap();
            dishes.push(dish);
        }
        radio
            .wait_connected(12, std::time::Duration::from_secs(3))
            .await
            .unwrap();
        for dish in &dishes {
            dish.wait_connected(1, std::time::Duration::from_secs(3))
                .await
                .unwrap();
        }
        warm_runtime(2 * (dishes.len() + 1)).await;
        for (index, size) in [16, 1024].into_iter().enumerate() {
            for _ in 0..16 {
                radio_delivery(&pool, &radio, &dishes, size).await;
            }
            TRACE_FIRST.with(|trace| trace.set(std::env::var_os("OMQ_ALLOC_TRACE").is_some()));
            ALLOCATIONS.with(|count| count.set(Some(0)));
            for _ in 0..128 {
                radio_delivery(&pool, &radio, &dishes, size).await;
            }
            counts[policy][index] = ALLOCATIONS.with(|count| count.replace(None).unwrap());
        }
        radio.close().await.unwrap();
        for dish in dishes {
            dish.close().await.unwrap();
        }
    }
    assert_eq!(counts, [[0; 2]; 2], "12 DISH peers, lossy/nodrop, 16/1024B");
}

#[test]
fn native_blocking_send_and_parked_receive_do_not_allocate() {
    let context = omq_tokio::Context::with_name("hf-alloc");
    let sender = context.blocking_socket(SocketType::Channel, Options::default());
    let pool = PayloadPool::new([(2048, 8192)]).unwrap();
    let receiver = context.blocking_socket(SocketType::Channel, Options::default());
    sender
        .connect(
            receiver
                .bind("dart://127.0.0.1:0".parse().unwrap())
                .unwrap(),
        )
        .unwrap();
    sender
        .wait_connected(1, std::time::Duration::from_secs(3))
        .unwrap();
    receiver
        .wait_connected(1, std::time::Duration::from_secs(3))
        .unwrap();
    // Initialize the caller's cached waiter even when warmup deliveries are
    // already queued and never take the parking path.
    assert!(matches!(
        receiver.recv_timeout(std::time::Duration::from_millis(1)),
        Err(Error::Timeout)
    ));
    for iteration in 0..144 {
        if iteration == 16 {
            TRACE_FIRST.with(|trace| trace.set(std::env::var_os("OMQ_ALLOC_TRACE").is_some()));
            ALLOCATIONS.with(|count| count.set(Some(0)));
        }
        let mut buffer = pool.try_buffer(1).unwrap();
        buffer.writable()[..16].fill(7);
        buffer.set_len(16).unwrap();
        sender.send(buffer.into_message()).unwrap();
        let delivered = receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        assert_eq!(delivered.part_slice(0), Some([7; 16].as_slice()));
    }
    let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    sender.close().unwrap();
    receiver.close().unwrap();
    context.term();
    assert_eq!(count, 0, "native blocking send/receive after warmup");
}

#[test]
fn native_blocking_send_on_full_pipe_does_not_allocate() {
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    // Resume the IO thread even if an assertion fails while it is paused.
    struct Resume(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Resume {
        fn drop(&mut self) {
            let (resumed, wake) = &*self.0;
            *resumed.lock().unwrap() = true;
            wake.notify_one();
        }
    }

    let context = omq_tokio::Context::with_name("hf-full-alloc");
    let sender = context.blocking_socket(SocketType::Scatter, Options::default().send_hwm(1));
    let pool = PayloadPool::new([(2048, 8192)]).unwrap();
    let receiver = context.blocking_socket(SocketType::Gather, Options::default());
    sender
        .connect(
            receiver
                .bind("dart://127.0.0.1:0".parse().unwrap())
                .unwrap(),
        )
        .unwrap();
    for socket in [&sender, &receiver] {
        socket.wait_connected(1, Duration::from_secs(3)).unwrap();
    }
    let mut allocations = 0;
    for iteration in 0..18 {
        // Receipt, rather than popping the send ring, now frees HWM capacity.
        // Let the previous iteration's ACK retire before pausing its IO owner.
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while dart_stats(&sender).acknowledged < iteration * 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "ACK retirement stalled"
            );
            std::thread::yield_now();
        }
        let resume = Resume(Arc::new((Mutex::new(false), Condvar::new())));
        let pause = resume.0.clone();
        let (entered, paused) = std::sync::mpsc::sync_channel(0);
        context.handle().spawn(async move {
            let (resumed, wake) = &*pause;
            let mut guard = resumed.lock().unwrap();
            entered.send(()).unwrap();
            while !*guard {
                guard = wake.wait(guard).unwrap();
            }
        });
        paused.recv().unwrap();
        sender.try_send(message(&pool, 16)).unwrap();
        let body = match sender.try_send(message(&pool, 16)) {
            Err(omq_tokio::TrySendError::Full(body)) => body,
            result => panic!("paused full pipe returned {result:?}"),
        };
        std::thread::scope(|scope| {
            let release = resume.0.clone();
            scope.spawn(move || {
                std::thread::sleep(Duration::from_millis(2));
                let (resumed, wake) = &*release;
                *resumed.lock().unwrap() = true;
                wake.notify_one();
            });
            if iteration >= 2 {
                TRACE_FIRST.with(|trace| {
                    trace.set(iteration == 2 && std::env::var_os("OMQ_ALLOC_TRACE").is_some());
                });
                ALLOCATIONS.with(|count| count.set(Some(0)));
            }
            let result = sender.send(body);
            if iteration >= 2 {
                allocations += ALLOCATIONS.with(|count| count.replace(None).unwrap());
            }
            result.unwrap();
        });
        for _ in 0..2 {
            assert_eq!(
                receiver
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .byte_len(),
                16
            );
        }
    }
    sender.close().unwrap();
    receiver.close().unwrap();
    context.term();
    assert_eq!(
        allocations, 0,
        "native blocking sends on a full pipe after warmup"
    );
}

async fn radio_burst(pool: &PayloadPool, radio: &Socket, dishes: &[Socket]) {
    for value in 0..32u8 {
        let mut buffer = pool.try_buffer(1).unwrap();
        buffer.writable()[..16].fill(value);
        buffer.set_len(16).unwrap();
        radio
            .send(Message::with_prefix(
                bytes::Bytes::from_static(b"group"),
                buffer.into_message(),
            ))
            .await
            .unwrap();
    }
    for dish in dishes {
        let mut seen = [false; 32];
        for _ in 0..32 {
            let delivered = tokio::time::timeout(std::time::Duration::from_secs(2), dish.recv())
                .await
                .unwrap()
                .unwrap();
            let payload = delivered.part_slice(1).unwrap();
            assert_eq!(payload.len(), 16);
            let value = payload[0] as usize;
            assert!(!seen[value], "duplicate local publication");
            seen[value] = true;
        }
        assert!(seen.into_iter().all(|seen| seen));
    }
}

#[tokio::test]
async fn native_radio_nodrop_bursts_do_not_allocate() {
    let pool = PayloadPool::new([(2048, 8192)]).unwrap();
    let radio = Socket::new(
        SocketType::Radio,
        Options {
            xpub_nodrop: true,
            send_hwm: 1,
            ..Options::default()
        },
    );
    let mut dishes = Vec::with_capacity(12);
    for _ in 0..12 {
        let dish = Socket::new(
            SocketType::Dish,
            Options::default().recv_payload_pool(PayloadPool::new([(2048, 8192)]).unwrap()),
        );
        dish.join(bytes::Bytes::from_static(b"group"))
            .await
            .unwrap();
        radio
            .connect(
                dish.bind("dart://127.0.0.1:0".parse().unwrap())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        dishes.push(dish);
    }
    radio
        .wait_connected(12, std::time::Duration::from_secs(3))
        .await
        .unwrap();
    for dish in &dishes {
        dish.wait_connected(1, std::time::Duration::from_secs(3))
            .await
            .unwrap();
    }
    warm_runtime(2 * (dishes.len() + 1)).await;
    for _ in 0..2 {
        radio_burst(&pool, &radio, &dishes).await;
    }
    TRACE_FIRST.with(|trace| trace.set(std::env::var_os("OMQ_ALLOC_TRACE").is_some()));
    ALLOCATIONS.with(|count| count.set(Some(0)));
    for _ in 0..4 {
        radio_burst(&pool, &radio, &dishes).await;
    }
    let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    radio.close().await.unwrap();
    for dish in dishes {
        dish.close().await.unwrap();
    }
    assert_eq!(count, 0, "native nodrop queue-full bursts after warmup");
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocated();
        unsafe { System.realloc(pointer, layout, size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn message(pool: &PayloadPool, length: usize) -> Message {
    let mut buffer = pool.try_buffer(1).unwrap();
    buffer.writable()[..length].fill(7);
    buffer.set_len(length).unwrap();
    buffer.into_message()
}

async fn native_delivery(pool: &PayloadPool, sender: &Socket, receiver: &Socket, size: usize) {
    let mut buffer = pool.try_buffer(1).unwrap();
    buffer.writable()[..size].fill(7);
    buffer.set_len(size).unwrap();
    let body = buffer.into_message();
    match sender.socket_type() {
        SocketType::Peer => sender.send_to(b"receiver", body).await.unwrap(),
        SocketType::Radio => sender
            .send(Message::with_prefix(
                bytes::Bytes::from_static(b"group"),
                body,
            ))
            .await
            .unwrap(),
        _ => sender.send(body).await.unwrap(),
    }
    let delivered = receiver.recv().await.unwrap();
    let part = usize::from(matches!(
        receiver.socket_type(),
        SocketType::Peer | SocketType::Dish
    ));
    assert_eq!(delivered.part_slice(part).unwrap().len(), size);
    assert!(
        delivered
            .part_slice(part)
            .unwrap()
            .iter()
            .all(|&byte| byte == 7)
    );
}

#[tokio::test]
async fn native_socket_delivery_reuses_storage_without_allocating() {
    for (send, recv) in [
        (SocketType::Scatter, SocketType::Gather),
        (SocketType::Client, SocketType::Server),
        (SocketType::Channel, SocketType::Channel),
        (SocketType::Peer, SocketType::Peer),
        (SocketType::Radio, SocketType::Dish),
    ] {
        let pool = PayloadPool::new([(2048, 8192)]).unwrap();
        let sender = Socket::new(
            send,
            Options::default().identity(bytes::Bytes::from_static(b"sender")),
        );
        let receiver = Socket::new(
            recv,
            Options::default()
                .identity(bytes::Bytes::from_static(b"receiver"))
                .recv_payload_pool(PayloadPool::new([(2048, 8192)]).unwrap()),
        );
        if recv == SocketType::Dish {
            receiver
                .join(bytes::Bytes::from_static(b"group"))
                .await
                .unwrap();
        }
        let endpoint = receiver
            .bind("dart://127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        sender.connect(endpoint).await.unwrap();
        sender
            .wait_connected(1, std::time::Duration::from_secs(3))
            .await
            .unwrap();
        receiver
            .wait_connected(1, std::time::Duration::from_secs(3))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        for size in [16, 64, 256, 1024] {
            for _ in 0..16 {
                native_delivery(&pool, &sender, &receiver, size).await;
            }
            TRACE_FIRST.with(|trace| trace.set(std::env::var_os("OMQ_ALLOC_TRACE").is_some()));
            ALLOCATIONS.with(|count| count.set(Some(0)));
            for _ in 0..128 {
                native_delivery(&pool, &sender, &receiver, size).await;
            }
            let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());
            assert_eq!(count, 0, "native {send:?}/{recv:?}, {size} bytes");
        }
        sender.close().await.unwrap();
        receiver.close().await.unwrap();
    }
}

#[test]
fn pooled_server_routing_and_cloning_do_not_allocate() {
    let pool = PayloadPool::new([(2048, 1)]).unwrap();
    for size in [0, 16, 64, 256, 1024] {
        let body = message(&pool, size);
        let pointer = body.part_slice(0).unwrap().as_ptr();
        ALLOCATIONS.with(|count| count.set(Some(0)));
        let mut routed = body.with_routing_id(7);
        let clone = routed.clone();
        assert_eq!(routed.take_routing_id(), Some(7));
        assert_eq!(routed.part_slice(0).unwrap().as_ptr(), pointer);
        assert_eq!(clone.routing_id(), Some(7));
        drop(clone);
        drop(routed);
        let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());
        assert_eq!(count, 0, "SERVER routing and clone, {size} bytes");
        assert_eq!(pool.available(), 1);
    }
}

#[test]
fn pooled_peer_and_radio_prefixes_and_cloning_do_not_allocate() {
    let pool = PayloadPool::new([(2048, 1)]).unwrap();
    for size in [0, 16, 64, 256, 1024] {
        let body = message(&pool, size);
        let pointer = body.part_slice(0).unwrap().as_ptr();
        ALLOCATIONS.with(|count| count.set(Some(0)));
        let mut prefixed =
            Message::with_prefix(bytes::Bytes::from_static(b"identity-or-group"), body);
        let clone = prefixed.clone();
        assert_eq!(prefixed.len(), 2);
        assert_eq!(prefixed.part_slice(1).unwrap().as_ptr(), pointer);
        assert_eq!(
            prefixed.pop_front_payload().unwrap().as_slice(),
            b"identity-or-group"
        );
        assert_eq!(prefixed.part_slice(0).unwrap().as_ptr(), pointer);
        drop(clone);
        drop(prefixed);
        let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());
        assert_eq!(count, 0, "PEER/RADIO prefix and clone, {size} bytes");
        assert_eq!(pool.available(), 1);
    }
}

#[test]
fn buffer_acquisition_clone_freeze_drop_and_exhaustion_do_not_allocate() {
    let pool = PayloadPool::new([(2048, 4)]).unwrap();
    let mut messages: [Option<Message>; 4] = std::array::from_fn(|_| None);
    ALLOCATIONS.with(|count| count.set(Some(0)));
    for size in [0, 16, 64, 256, 1024] {
        for _ in 0..128 {
            for message in &mut messages {
                let mut buffer = pool.try_buffer(1).unwrap();
                buffer.writable()[..size].fill(7);
                buffer.set_len(size).unwrap();
                assert!(buffer.set_len(buffer.capacity() + 1).is_err());
                let owned = buffer.into_message();
                let clone = owned.clone();
                assert_eq!(clone.part_slice(0).unwrap().len(), size);
                drop(clone);
                *message = Some(owned);
            }
            assert!(pool.try_buffer(1).is_none());
            for message in &mut messages {
                drop(message.take());
            }
            assert_eq!(pool.available(), 4);
        }
    }
    let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(count, 0);
}

#[test]
fn borrowed_protocol_decode_and_encode_do_not_allocate() {
    let mut output = [0; dart::MAX_DATAGRAM];
    let ready = dart::Ready {
        socket_type: SocketType::Peer,
        identity: Some(b"peer"),
        reply_requested: true,
        session: 1,
        echo: 0,
        phase: dart::Phase::Hello,
    };
    ALLOCATIONS.with(|count| count.set(Some(0)));
    for _ in 0..128 {
        let length = dart::encode_ready(ready, &mut output).unwrap();
        assert_eq!(dart::decode_ready(&output[..length]), Some(ready));
        for group in [None, Some(b"group".as_slice())] {
            let length = dart::encode_data(&[7; 1024], group, &mut output).unwrap();
            let Some(dart::Datagram::Data(payload)) = dart::decode(&output[..length]) else {
                panic!()
            };
            let (_, body) = dart::data_body(payload, group.is_some()).unwrap();
            assert_eq!(body, &[7; 1024]);
        }
    }
    let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(count, 0);
}

#[tokio::test]
async fn reusable_udp_io_and_segmentation_do_not_allocate() {
    let receiver = DartIo::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
    let sender = DartIo::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
    let target = receiver.local_addr().unwrap();
    let mut batch = ReceiveBatch::new(receiver.gro_segments());
    let mut output = vec![0; 64_000].into_boxed_slice();
    let mut allocations = 0;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for size in [16, 64, 256, 1024] {
            let stride = size + dart::DATA_HEADER;
            let length = stride * 32;
            for packet in output[..length].chunks_mut(stride) {
                dart::encode_data(&[7; 1024][..size], None, packet).unwrap();
            }
            for iteration in 0..32 {
                let mut sent = 0;
                while sent < length {
                    sender.writable().await.unwrap();
                    ALLOCATIONS.with(|count| count.set(Some(0)));
                    let result =
                        sender.try_send_segments(target, None, None, &output[sent..length], stride);
                    let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());
                    if iteration > 0 {
                        allocations += count;
                    }
                    match result {
                        Ok(count) => sent += count * stride,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                        Err(error) => panic!("send: {error}"),
                    }
                }
                let mut delivered = 0;
                while delivered < 32 {
                    receiver.readable().await.unwrap();
                    ALLOCATIONS.with(|count| count.set(Some(0)));
                    let result = batch.try_receive(&receiver);
                    for datagram in batch.datagrams() {
                        assert_eq!(datagram.bytes.len(), stride);
                        delivered += 1;
                    }
                    let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());
                    if iteration > 0 {
                        allocations += count;
                    }
                    match result {
                        Ok(_) => assert_eq!(batch.rejected_buffers(), 0),
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                        Err(error) => panic!("receive: {error}"),
                    }
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(allocations, 0);
}

#[test]
fn bulk_pool_transfers_do_not_allocate() {
    let pool = PayloadPool::new([(1024, 64)]).unwrap();
    let mut buffers = Vec::with_capacity(64);
    let mut messages = Vec::with_capacity(64);
    ALLOCATIONS.with(|count| count.set(Some(0)));
    for _ in 0..128 {
        assert_eq!(pool.try_buffers_into(1, 64, &mut buffers), 64);
        for mut buffer in buffers.drain(..) {
            buffer.writable()[..16].fill(7);
            buffer.set_len(16).unwrap();
            messages.push(buffer.into_message());
        }
        assert_eq!(PayloadPool::recycle_many(&mut messages, 64), 64);
        assert_eq!(pool.available(), 64);
    }
    assert_eq!(ALLOCATIONS.with(|count| count.replace(None).unwrap()), 0);
}
