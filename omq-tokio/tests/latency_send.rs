//! TCP latency admission, allocation and partial-write regressions.
mod test_support;

use bytes::Bytes;
use omq_tokio::options::WorkloadProfile;
use omq_tokio::{Context, Message, Options, SocketType, TrySendError};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::Duration;

struct CountingAllocator;
thread_local! {
    static COUNT: Cell<Option<usize>> = const { Cell::new(None) };
}

fn allocated() {
    let _ = COUNT.try_with(|count| {
        if let Some(n) = count.get() {
            count.set(Some(n + 1));
        }
    });
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe {
            System.dealloc(ptr, layout);
        }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocated();
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const DEADLINE: Duration = Duration::from_secs(5);

fn options(identity: &'static [u8], profile: WorkloadProfile) -> Options {
    Options::default()
        .identity(Bytes::from_static(identity))
        .workload_profile(profile)
        .send_hwm(2)
        .recv_hwm(2)
        .send_buffer_size(4096)
        .recv_buffer_size(65536)
        .linger(Duration::ZERO)
}

#[tokio::test]
async fn full_identity_retry_allocates_nothing_and_preserves_original() {
    // Borrowed CT keeps driver work out of the synchronous measured section.
    for kind in [SocketType::Peer, SocketType::Router] {
        for profile in [WorkloadProfile::Throughput, WorkloadProfile::Latency] {
            let context = Context::current();
            let server = context.socket(kind, options(b"server", profile));
            let client = context.socket(kind, options(b"client", profile));
            let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
            client.connect(endpoint).await.unwrap();
            client.wait_connected(1, DEADLINE).await.unwrap();
            server.wait_connected(1, DEADLINE).await.unwrap();
            let mut message =
                Message::multipart([Bytes::from_static(b"server"), Bytes::from(vec![42; 65536])])
                    .with_routing_id(17);
            let expected = message.clone();
            let mut full = None;
            for _ in 0..512 {
                match client.try_send(message) {
                    Ok(()) => message = expected.clone(),
                    Err(TrySendError::Full(returned)) => {
                        full = Some(returned);
                        break;
                    }
                    other => panic!("unexpected send: {other:?}"),
                }
            }
            let mut message = full.expect("bounded outbound capacity");
            let identity_ptr = message.part_slice(0).unwrap().as_ptr();
            let body_ptr = message.part_slice(1).unwrap().as_ptr();
            COUNT.with(|count| count.set(Some(0)));
            for _ in 0..128 {
                message = match client.try_send(message) {
                    Err(TrySendError::Full(returned)) => returned,
                    other => panic!("retry must remain full: {other:?}"),
                };
            }
            let allocations = COUNT.with(|count| count.replace(None).unwrap());
            assert_eq!(allocations, 0, "{kind:?}/{profile:?}");
            assert_eq!(message, expected);
            assert_eq!(message.routing_id(), Some(17));
            assert_eq!(message.part_slice(0).unwrap().as_ptr(), identity_ptr);
            assert_eq!(message.part_slice(1).unwrap().as_ptr(), body_ptr);
            client.close().await.unwrap();
            server.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn latency_partial_tcp_writes_preserve_fifo() {
    for kind in [SocketType::Channel, SocketType::Peer, SocketType::Pair] {
        let context = Context::new();
        let server = context.socket(kind, options(b"server", WorkloadProfile::Latency));
        let client = context.socket(kind, options(b"client", WorkloadProfile::Latency));
        let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
        client.connect(endpoint).await.unwrap();
        client.wait_connected(1, DEADLINE).await.unwrap();
        server.wait_connected(1, DEADLINE).await.unwrap();
        let sending = tokio::spawn(async move {
            for sequence in 0u8..32 {
                let body =
                    Message::single(vec![sequence; if sequence % 2 == 0 { 65536 } else { 64 }]);
                let message = if kind == SocketType::Peer {
                    Message::with_prefix(Bytes::from_static(b"server"), body)
                } else {
                    body
                };
                client.send(message).await.unwrap();
            }
            client
        });
        // Let small kernel buffers force partial writes and HWM waits.
        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::time::timeout(Duration::from_secs(15), async {
            for sequence in 0u8..32 {
                let message = server.recv().await.unwrap();
                let body = message
                    .part_slice(usize::from(kind == SocketType::Peer))
                    .unwrap();
                assert_eq!(body.len(), if sequence % 2 == 0 { 65536 } else { 64 });
                assert!(body.iter().all(|byte| *byte == sequence));
            }
        })
        .await
        .unwrap();
        let client = sending.await.unwrap();
        client.close().await.unwrap();
        server.close().await.unwrap();
    }
}

#[tokio::test]
async fn warmed_latency_send_adds_no_allocation() {
    for kind in [SocketType::Channel, SocketType::Peer] {
        let context = Context::current();
        let server = context.socket(kind, options(b"server", WorkloadProfile::Latency));
        let client = context.socket(kind, options(b"client", WorkloadProfile::Latency));
        let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
        client.connect(endpoint).await.unwrap();
        client.wait_connected(1, DEADLINE).await.unwrap();
        server.wait_connected(1, DEADLINE).await.unwrap();
        let body = Message::single(Bytes::from_static(&[42; 64]));
        let request = if kind == SocketType::Peer {
            Message::with_prefix(Bytes::from_static(b"server"), body)
        } else {
            body
        };
        for iteration in 0..32 {
            let message = request.clone();
            COUNT.with(|count| count.set(Some(0)));
            let result = client.try_send(message);
            let allocations = COUNT.with(|count| count.replace(None).unwrap());
            result.unwrap();
            if iteration > 0 {
                assert_eq!(allocations, 0, "{kind:?}");
            }
            tokio::time::timeout(DEADLINE, server.recv())
                .await
                .unwrap()
                .unwrap();
        }
        client.close().await.unwrap();
        server.close().await.unwrap();
    }
}

#[tokio::test]
async fn latency_fallback_honors_small_send_hwm() {
    for kind in [SocketType::Pair, SocketType::Channel] {
        let context = Context::current();
        let server = context.socket(kind, options(b"server", WorkloadProfile::Latency));
        let client = context.socket(
            kind,
            options(b"client", WorkloadProfile::Latency)
                .send_hwm(1)
                .arena_threshold(0),
        );
        let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
        client.connect(endpoint).await.unwrap();
        client.wait_connected(1, DEADLINE).await.unwrap();
        server.wait_connected(1, DEADLINE).await.unwrap();
        let message = Message::single(Bytes::from(vec![42; 64]));
        let mut accepted = 0;
        // External payloads require the driver; the direct writer only writes
        // arena-only frames. This prevents completed kernel writes from being
        // counted as queued messages, regardless of TCP buffering behavior.
        // The borrowed runtime cannot drain during this synchronous loop, so
        // at most one slot message plus one admitted inbox message can remain.
        for _ in 0..128 {
            match client.try_send(message.clone()) {
                Ok(()) => accepted += 1,
                Err(TrySendError::Full(_)) => break,
                other => panic!("unexpected send result: {other:?}"),
            }
        }
        assert!(
            (1..=2).contains(&accepted),
            "{kind:?}: accepted {accepted} messages with HWM 1"
        );
        client.close().await.unwrap();
        server.close().await.unwrap();
    }
}
