#![cfg(all(feature = "soak", feature = "zstd"))]
//! Soak: IO-lane PUB/SUB over zstd+tcp:// with fan-out dictionaries.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

const DECODED_SUBS: usize = 4;
const TRAINING_MSGS: u64 = 128;
const RAW_PAYLOAD_CHECKS: usize = 3;
const RAW_RECV_TIMEOUT: Duration = Duration::from_secs(15);
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const ZDICT_MAGIC: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];

fn zstd_tcp_ep(port: u16) -> Endpoint {
    Endpoint::ZstdTcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn tcp_from_zstd(ep: &Endpoint) -> Endpoint {
    match ep {
        Endpoint::ZstdTcp { host, port } => Endpoint::Tcp {
            host: host.clone(),
            port: *port,
        },
        other => panic!("expected zstd+tcp endpoint, got {other:?}"),
    }
}

fn zstd_options() -> Options {
    soak_common::soak_options()
        .compression_auto_train(true)
        .send_hwm(4096)
        .recv_hwm(4096)
}

async fn bind_zstd_pub(publisher: &Socket) -> (Endpoint, omq_tokio::MonitorStream) {
    let mut mon = publisher.monitor();
    publisher.bind(zstd_tcp_ep(0)).await.unwrap();
    loop {
        if let MonitorEvent::Listening {
            endpoint: Endpoint::ZstdTcp { port, .. },
        } = tokio::time::timeout(Duration::from_secs(5), mon.recv())
            .await
            .expect("publisher did not report listening")
            .unwrap()
        {
            return (zstd_tcp_ep(port), mon);
        }
    }
}

async fn wait_for_subscribes(mon: &mut omq_tokio::MonitorStream, n: usize) {
    let fut = async {
        let mut count = 0;
        while count < n {
            match mon.recv().await {
                Ok(MonitorEvent::SubscribeReceived { .. }) => count += 1,
                Ok(_) => {}
                Err(e) => panic!("monitor closed after {count}/{n} subscribes: {e:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("subscribes did not propagate");
}

fn payload(seq: u64) -> Bytes {
    Bytes::from(format!(
        "{{\"kind\":\"quote\",\"venue\":\"XNAS\",\"symbol\":\"OMQ\",\"seq\":{seq},\"bid\":101.25,\"ask\":101.27,\"depth\":[10125,10126,10127],\"pad\":\"{}\"}}",
        "A".repeat(256)
    ))
}

fn verify_message(message: &Message, previous: &mut Option<u64>) {
    assert_eq!(message.len(), 1, "unexpected multipart publication");
    let bytes = message.part_bytes(0).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let seq = value["seq"].as_u64().expect("publication sequence");
    assert_eq!(bytes, payload(seq), "corrupt publication {seq}");
    if let Some(last) = previous {
        assert!(
            seq > *last,
            "duplicate or reordered publication {seq} after {last}"
        );
    }
    *previous = Some(seq);
}

async fn assert_raw_dict_then_payloads(raw: &Socket) -> u64 {
    let dict = tokio::time::timeout(RAW_RECV_TIMEOUT, raw.recv())
        .await
        .expect("raw subscriber did not receive dictionary shipment")
        .unwrap();
    assert!(dict.part_slice(0).unwrap().starts_with(&ZDICT_MAGIC));
    let mut decoder = omq_proto::proto::transform::ZstdDecoder::new();
    assert!(decoder.decode(dict).unwrap().is_none());
    let mut previous = None;
    let mut arenas = 0;
    for idx in 0..RAW_PAYLOAD_CHECKS {
        let msg = tokio::time::timeout(RAW_RECV_TIMEOUT, raw.recv())
            .await
            .unwrap_or_else(|_| panic!("raw subscriber missed compressed payload {idx}"))
            .unwrap();
        let part = msg.part_slice(0).unwrap();
        let arena = part.starts_with(b"ZSTA");
        assert!(
            arena || part.starts_with(&ZSTD_MAGIC),
            "unexpected wire envelope"
        );
        if arena {
            assert_eq!(part[4], 1, "expected compressed arena");
            arenas += 1;
        }
        let message = decoder.decode(msg).unwrap().expect("duplicate dictionary");
        verify_message(&message, &mut previous);
        for _ in 1..omq_proto::proto::transform::arena::MAX_ARENA_MESSAGES {
            let Some(message) = decoder.poll_message() else {
                break;
            };
            verify_message(&message, &mut previous);
        }
        assert!(!decoder.has_pending_messages());
    }
    arenas
}

/// Counters shared by the publisher, decoded subscribers, and probe loop.
#[derive(Default)]
struct Progress {
    sent: AtomicU64,
    decoded: AtomicU64,
    raw_probes: AtomicU64,
    raw_arenas: AtomicU64,
    stop: AtomicBool,
}

async fn receive_publications(sub: Socket, progress: Arc<Progress>) {
    let mut previous = None;
    while !progress.stop.load(Ordering::Relaxed) {
        match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
            Ok(result) => {
                verify_message(&result.unwrap(), &mut previous);
                progress.decoded.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                // Silence is only acceptable once the publisher has stopped.
                assert!(
                    progress.stop.load(Ordering::Relaxed),
                    "decoded subscriber stalled"
                );
            }
        }
    }
}

/// Enough decoded SUBs to activate PUB lane workers, all subscribed to
/// everything before training starts.
async fn connect_decoded_subs(ep: &Endpoint, mon: &mut omq_tokio::MonitorStream) -> Vec<Socket> {
    let mut decoded_subs = Vec::with_capacity(DECODED_SUBS);
    for _ in 0..DECODED_SUBS {
        let sub = Socket::new(SocketType::Sub, zstd_options());
        sub.connect(ep.clone()).await.unwrap();
        sub.subscribe(Bytes::new()).await.unwrap();
        decoded_subs.push(sub);
    }
    wait_for_subscribes(mon, DECODED_SUBS).await;
    decoded_subs
}

/// Publishes the payload sequence after the training messages until stopped.
fn spawn_publisher(publisher: Socket, progress: Arc<Progress>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut seq = TRAINING_MSGS;
        while !progress.stop.load(Ordering::Relaxed) {
            publisher.send(Message::single(payload(seq))).await.unwrap();
            progress.sent.fetch_add(1, Ordering::Relaxed);
            seq += 1;
        }
    })
}

/// Attaches a raw TCP late subscriber that sees compressed wire frames and
/// checks its dictionary shipment and first payloads.
async fn probe_raw_subscriber(ep: &Endpoint, progress: &Progress) {
    let raw = Socket::new(SocketType::Sub, soak_common::soak_options().recv_hwm(64));
    raw.connect(tcp_from_zstd(ep)).await.unwrap();
    raw.subscribe(Bytes::new()).await.unwrap();

    let arenas = assert_raw_dict_then_payloads(&raw).await;
    progress.raw_arenas.fetch_add(arenas, Ordering::Relaxed);

    raw.close().await.unwrap();
    progress.raw_probes.fetch_add(1, Ordering::Relaxed);
}

fn assert_traffic(progress: &Progress) {
    assert!(
        progress.raw_probes.load(Ordering::Relaxed) > 0,
        "soak must attach at least one raw late subscriber"
    );
    assert!(
        progress.decoded.load(Ordering::Relaxed) > 0,
        "decoded subscribers must receive traffic"
    );

    let arenas = progress.raw_arenas.load(Ordering::Relaxed);
    assert!(
        arenas > 0,
        "raw probes did not observe any compression arenas"
    );
    eprintln!(
        "[pub_sub_zstd_dict] verified {arenas} arena carriers across {} late subscribers",
        progress.raw_probes.load(Ordering::Relaxed)
    );
}

#[test]
fn soak_pub_sub_zstd_dict_io_lane_fanout() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let progress = Arc::new(Progress::default());

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let publisher = Socket::new(SocketType::Pub, zstd_options());
        let (ep, mut mon) = bind_zstd_pub(&publisher).await;
        let decoded_subs = connect_decoded_subs(&ep, &mut mon).await;

        // Feed the auto-trainer so the fan-out dictionary exists early.
        for seq in 0..TRAINING_MSGS {
            publisher.send(Message::single(payload(seq))).await.unwrap();
            progress.sent.fetch_add(1, Ordering::Relaxed);
        }

        let mut decode_tasks = Vec::with_capacity(DECODED_SUBS);
        for sub in &decoded_subs {
            let task = receive_publications(sub.clone(), progress.clone());
            decode_tasks.push(tokio::spawn(task));
        }
        let send_task = spawn_publisher(publisher.clone(), progress.clone());

        let start = Instant::now();
        // Backdate the last probe so the first one runs immediately.
        let mut last_probe = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        let mut last_log = start;
        while start.elapsed() < duration {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if last_probe.elapsed() >= Duration::from_millis(500) {
                last_probe = Instant::now();
                probe_raw_subscriber(&ep, &progress).await;
            }

            if last_log.elapsed() >= Duration::from_secs(30) {
                let s = progress.sent.load(Ordering::Relaxed);
                let d = progress.decoded.load(Ordering::Relaxed);
                let r = progress.raw_probes.load(Ordering::Relaxed);
                eprintln!(
                    "[pub_sub_zstd_dict] {:.0}s, sent {s}, decoded {d}, raw_probes {r}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        progress.stop.store(true, Ordering::Relaxed);
        send_task.await.expect("publisher task failed");
        for task in decode_tasks {
            task.await.expect("subscriber task failed");
        }

        assert_traffic(&progress);
        for sub in decoded_subs {
            sub.close().await.unwrap();
        }
        publisher.close().await.unwrap();
    });

    let report = monitor.stop();
    report.assert_no_leak("pub_sub_zstd_dict");
}
