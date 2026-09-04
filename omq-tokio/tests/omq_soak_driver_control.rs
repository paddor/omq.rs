#![cfg(feature = "soak")]
//! Soak: control-plane responsiveness while outbound writes are stalled.
//!
//! Each worker repeatedly fills a driver's data inbox, blocks its transport
//! after a tiny partial write, then sends control commands through the
//! separate control inbox. Close must preempt the pending write every time.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::engine::driver::DriverStream;
use omq_tokio::engine::{ConnectionDriver, PeerDriverCommand, PeerDriverData, PeerEvent};
use omq_tokio::proto::connection::{ConnectionConfig, Role};
use omq_tokio::proto::{Command, Connection, Event};
use omq_tokio::{Message, SocketType};
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

const DATA_BACKLOG: usize = 128;
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const CONTROL_DEADLINE: Duration = Duration::from_millis(250);

struct SoakDuplex(DuplexStream);

impl DriverStream for SoakDuplex {
    type Reader = tokio::io::ReadHalf<DuplexStream>;
    type Writer = tokio::io::WriteHalf<DuplexStream>;

    fn split(self, _fast_write: bool) -> (Self::Reader, Self::Writer) {
        tokio::io::split(self.0)
    }
}

#[test]
fn soak_driver_control_separation() {
    let duration = soak_common::soak_duration();
    let workers = std::env::var("OMQ_SOAK_DRIVER_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4)
        .clamp(1, 32);
    let monitor = soak_common::ResourceMonitor::start();
    let iterations = Arc::new(AtomicU64::new(0));
    let max_control_ns = Arc::new(AtomicU64::new(0));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers.max(2))
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let deadline = Instant::now() + duration;
        let mut tasks = JoinSet::new();
        for worker in 0..workers {
            let iterations = iterations.clone();
            let max_control_ns = max_control_ns.clone();
            tasks.spawn(async move {
                let mut local_iterations = 0u64;
                while Instant::now() < deadline {
                    let payload_len = if (worker as u64 + local_iterations).is_multiple_of(2) {
                        4 * 1024
                    } else {
                        1024 * 1024
                    };
                    let control_latency =
                        stalled_write_close_once(worker, local_iterations, payload_len).await;
                    iterations.fetch_add(1, Ordering::Relaxed);
                    max_control_ns.fetch_max(
                        u64::try_from(control_latency.as_nanos()).unwrap_or(u64::MAX),
                        Ordering::Relaxed,
                    );
                    local_iterations += 1;
                }
            });
        }

        let report_iterations = iterations.clone();
        let reporter = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                if Instant::now() >= deadline {
                    break;
                }
                eprintln!(
                    "[driver_control] iterations {}",
                    report_iterations.load(Ordering::Relaxed),
                );
            }
        });

        while let Some(result) = tasks.join_next().await {
            result.expect("driver-control soak worker panicked");
        }
        reporter.abort();
        let _ = reporter.await;
    });
    drop(runtime);

    let total = iterations.load(Ordering::Relaxed);
    let max_control = Duration::from_nanos(max_control_ns.load(Ordering::Relaxed));
    eprintln!("[driver_control] done: {total} iterations, max control latency {max_control:?}");
    assert!(total > 0, "driver-control soak did no work");
    assert!(
        max_control < CONTROL_DEADLINE,
        "control exceeded deadline: {max_control:?}",
    );

    monitor.stop().assert_no_leak("driver_control");
}

async fn stalled_write_close_once(worker: usize, iteration: u64, payload_len: usize) -> Duration {
    // 64-byte duplex capacity guarantees the first encoded message becomes a
    // partial write. The peer event queue then stops the receiving driver from
    // draining enough input to release that backpressure.
    let (server_stream, client_stream) = tokio::io::duplex(64);
    let server_connection = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
    let client_connection = Connection::new(
        ConnectionConfig::new(Role::Client, SocketType::Push)
            .identity(Bytes::from(format!("control-{worker}-{iteration}"))),
    );

    let (server_control_tx, server_control_rx) = mpsc::channel(4);
    let (client_control_tx, client_control_rx) = mpsc::channel(4);
    let (_server_data_tx, server_data_rx) = mpsc::channel(4);
    let (client_data_tx, client_data_rx) = mpsc::channel(DATA_BACKLOG);
    let (server_events_tx, mut server_events_rx) = mpsc::channel(1);
    let (client_events_tx, mut client_events_rx) = mpsc::channel(1);
    let server_cancel = CancellationToken::new();
    let client_cancel = CancellationToken::new();

    let server = ConnectionDriver::new(
        SoakDuplex(server_stream),
        server_connection,
        server_control_rx,
        server_events_tx,
        0,
        server_cancel.clone(),
    )
    .with_data_inbox(server_data_rx);
    let client = ConnectionDriver::new(
        SoakDuplex(client_stream),
        client_connection,
        client_control_rx,
        client_events_tx,
        1,
        client_cancel.clone(),
    )
    .with_data_inbox(client_data_rx);
    let server_task = tokio::spawn(server.run());
    let mut client_task = tokio::spawn(client.run());

    let ((), ()) = tokio::join!(
        wait_for_handshake(&mut server_events_rx),
        wait_for_handshake(&mut client_events_rx),
    );
    server_control_tx
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();
    client_control_tx
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();

    let payload = Bytes::from(vec![0xA5; payload_len]);
    for _ in 0..DATA_BACKLOG {
        client_data_tx
            .try_send(PeerDriverData::SendMessage(Message::single(
                payload.clone(),
            )))
            .expect("data inbox accepted less than its capacity");
    }
    tokio::time::timeout(IO_TIMEOUT, async {
        while client_data_tx.capacity() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("client driver did not start outbound work");
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert!(
        client_data_tx.capacity() < DATA_BACKLOG,
        "driver drained entire backlog before control arrived",
    );

    let control_started = Instant::now();
    client_control_tx
        .send(PeerDriverCommand::SendCommand(Command::Ping {
            ttl_deciseconds: 0,
            context: Bytes::copy_from_slice(&iteration.to_le_bytes()),
        }))
        .await
        .unwrap();
    client_control_tx
        .send(PeerDriverCommand::Close)
        .await
        .unwrap();
    let client_result = tokio::time::timeout(CONTROL_DEADLINE, &mut client_task)
        .await
        .expect("control was trapped behind stalled data")
        .expect("client driver task panicked");
    let control_latency = control_started.elapsed();
    client_result.expect("client driver failed during controlled close");

    server_cancel.cancel();
    server_task.abort();
    let _ = server_task.await;
    control_latency
}

async fn wait_for_handshake(events: &mut mpsc::Receiver<(u64, PeerEvent)>) {
    tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            match events.recv().await {
                Some((_, PeerEvent::Event(Event::HandshakeSucceeded { .. }))) => return,
                Some((_, PeerEvent::Event(_))) => {}
                Some((_, PeerEvent::Closed { error })) => {
                    panic!("driver closed before handshake: {error:?}")
                }
                None => panic!("driver event channel closed before handshake"),
            }
        }
    })
    .await
    .expect("driver handshake timed out");
}
