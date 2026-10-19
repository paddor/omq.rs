//! Draft sockets, SHM storage, and multiple-endpoint performance gates.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::{Message, PayloadPool, SocketType, TrySendError, blocking::Socket};

use super::affinity::Side;
use super::measurement::Window;
use super::{Case, SETTINGS, Transport, Workload as CaseWorkload, context, count_blocking_many};

const HWM: u32 = 1000;
const BATCH: usize = 256;
const GROUP: &[u8] = b"perf";

#[derive(Clone, Copy, Debug)]
enum Pattern {
    ScatterGather,
    RadioDish,
}

impl Pattern {
    fn label(self) -> &'static str {
        match self {
            Self::ScatterGather => "scattergather",
            Self::RadioDish => "radiodish",
        }
    }

    fn types(self) -> (SocketType, SocketType) {
        match self {
            Self::ScatterGather => (SocketType::Scatter, SocketType::Gather),
            Self::RadioDish => (SocketType::Radio, SocketType::Dish),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Workload {
    transport: Transport,
    pattern: Pattern,
    size: usize,
    sources: usize,
    destinations: usize,
    bulk: bool,
    pooled: bool,
    mixed: bool,
}

impl Workload {
    fn new(transport: Transport, pattern: Pattern, size: usize) -> Self {
        Self {
            transport,
            pattern,
            size,
            sources: 1,
            destinations: 1,
            bulk: false,
            pooled: false,
            mixed: false,
        }
    }
}

pub(super) fn cases(cases: &mut Vec<Case>) {
    for transport in [
        Transport::Tcp,
        Transport::Inproc,
        #[cfg(unix)]
        Transport::Ipc,
        #[cfg(target_os = "linux")]
        Transport::Shm,
        #[cfg(feature = "dart")]
        Transport::Dart,
    ] {
        add_size_cases(cases, transport);
        add_fan_cases(cases, transport);
        #[cfg(target_os = "linux")]
        if matches!(transport, Transport::Shm) {
            add_shm_cases(cases, transport);
        }
    }
}

/// One source and one destination at small sizes.
fn add_size_cases(cases: &mut Vec<Case>, transport: Transport) {
    for pattern in [Pattern::ScatterGather, Pattern::RadioDish] {
        for (size, suffix) in [(16, "16b"), (64, "64b"), (256, "256b")] {
            // DART SCATTER/GATHER is gated at 64 B only.
            #[cfg(feature = "dart")]
            if matches!(transport, Transport::Dart)
                && matches!(pattern, Pattern::ScatterGather)
                && size != 64
            {
                continue;
            }
            add(cases, suffix, Workload::new(transport, pattern, size));
        }
    }
}

/// 64 B fan-out (bulk SCATTER, RADIO) and fan-in (eight SCATTER sources).
fn add_fan_cases(cases: &mut Vec<Case>, transport: Transport) {
    let scatter = Workload::new(transport, Pattern::ScatterGather, 64);
    add(
        cases,
        "64b_4p_bulk",
        Workload {
            destinations: 4,
            bulk: true,
            ..scatter
        },
    );
    add(
        cases,
        "64b_8src",
        Workload {
            sources: 8,
            ..scatter
        },
    );
    add(
        cases,
        "64b_4p",
        Workload {
            destinations: 4,
            ..Workload::new(transport, Pattern::RadioDish, 64)
        },
    );
}

/// SHM only: mixed SHM/TCP sources, and large bodies with and without a
/// shared payload pool.
#[cfg(target_os = "linux")]
fn add_shm_cases(cases: &mut Vec<Case>, transport: Transport) {
    let scatter = Workload::new(transport, Pattern::ScatterGather, 64);
    add(
        cases,
        "64b_8src",
        Workload {
            sources: 8,
            mixed: true,
            ..scatter
        },
    );
    for pattern in [Pattern::ScatterGather, Pattern::RadioDish] {
        for (size, suffix) in [(4096, "4k"), (16 * 1024, "16k"), (1024 * 1024, "1m")] {
            let workload = Workload::new(transport, pattern, size);
            add(cases, suffix, workload);
            add(
                cases,
                suffix,
                Workload {
                    pooled: true,
                    ..workload
                },
            );
        }
    }
}

fn add(cases: &mut Vec<Case>, suffix: &str, workload: Workload) {
    cases.push(Case {
        name: format!(
            "{}{}{}{}_1io.{suffix}_msgs_s",
            if workload.mixed { "mixed_" } else { "" },
            workload.transport.prefix(),
            workload.pattern.label(),
            if workload.pooled { "_pool" } else { "" }
        ),
        workload: CaseWorkload::Draft(workload),
    });
}

pub(super) fn smoke_thresholds(thresholds: &mut HashMap<String, f64>) {
    for name in [
        "inproc_scattergather_1io.64b_4p_bulk_msgs_s",
        "radiodish_1io.64b_msgs_s",
    ] {
        thresholds.insert(name.into(), 100_000.0);
    }
    #[cfg(target_os = "linux")]
    for name in [
        "shm_scattergather_1io.64b_msgs_s",
        "shm_scattergather_1io.64b_4p_bulk_msgs_s",
        "shm_scattergather_1io.64b_8src_msgs_s",
        "mixed_shm_scattergather_1io.64b_8src_msgs_s",
        "shm_radiodish_1io.64b_msgs_s",
    ] {
        thresholds.insert(name.into(), 100_000.0);
    }
    #[cfg(target_os = "linux")]
    thresholds.insert("shm_scattergather_pool_1io.1m_msgs_s".into(), 100.0);
    #[cfg(feature = "dart")]
    thresholds.insert("dart_radiodish_1io.64b_msgs_s".into(), 100_000.0);
}

pub(super) fn measure(workload: Workload) -> f64 {
    let receiver_ctx = context(1, Side::Receiver);
    let sender_ctx = if matches!(workload.transport, Transport::Inproc) {
        receiver_ctx.clone()
    } else {
        context(1, Side::Sender)
    };
    let mut options = workload.transport.options().send_hwm(HWM).recv_hwm(HWM);
    options.xpub_nodrop = true;
    let (sender_type, receiver_type) = workload.pattern.types();
    let mut receivers = Vec::with_capacity(workload.destinations);
    let mut endpoints = Vec::with_capacity(workload.destinations);
    for _ in 0..workload.destinations {
        let receiver = receiver_ctx.blocking_socket(receiver_type, options.clone());
        if matches!(workload.pattern, Pattern::RadioDish) {
            receiver.join(Bytes::from_static(GROUP)).expect("DISH join");
        }
        endpoints.push(
            receiver
                .bind(workload.transport.endpoint())
                .expect("draft bind"),
        );
        if workload.mixed {
            endpoints.push(
                receiver
                    .bind(Transport::Tcp.endpoint())
                    .expect("mixed TCP bind"),
            );
        }
        receivers.push(receiver);
    }
    let senders: Vec<_> = (0..workload.sources)
        .map(|index| {
            let sender = sender_ctx.blocking_socket(sender_type, options.clone());
            for (endpoint_index, endpoint) in endpoints.iter().enumerate() {
                if workload.mixed && index % endpoints.len() != endpoint_index {
                    continue;
                }
                sender.connect(endpoint.clone()).expect("draft connect");
            }
            sender
                .wait_connected(workload.destinations, Duration::from_secs(5))
                .expect("draft sender ready");
            sender
        })
        .collect();
    for receiver in &receivers {
        receiver
            .wait_connected(workload.sources, Duration::from_secs(5))
            .expect("draft receiver ready");
    }
    if matches!(workload.pattern, Pattern::RadioDish) {
        wait_radio_ready(&senders[0], &receivers);
    }
    let rate = transfer(&senders, receivers, workload);
    sender_ctx.term();
    receiver_ctx.term();
    rate
}

fn wait_radio_ready(sender: &Socket, receivers: &[Socket]) {
    let probe = Message::with_group(Bytes::from_static(GROUP), Bytes::from_static(b"\0"));
    let mut ready = vec![false; receivers.len()];
    let deadline = Instant::now() + Duration::from_secs(5);
    while ready.iter().any(|ready| !ready) {
        assert!(Instant::now() < deadline, "DISH probe timeout");
        match sender.try_send(probe.clone()) {
            Ok(()) | Err(TrySendError::Full(_)) => {}
            Err(error) => panic!("DISH probe send: {error}"),
        }
        for (receiver, ready) in receivers.iter().zip(&mut ready) {
            match receiver.try_recv() {
                Ok(message) => {
                    assert_eq!(message, probe);
                    *ready = true;
                }
                Err(omq_tokio::Error::WouldBlock) => {}
                Err(error) => panic!("DISH probe receive: {error}"),
            }
        }
        thread::yield_now();
    }
}

fn transfer(senders: &[Socket], receivers: Vec<Socket>, workload: Workload) -> f64 {
    let (window_tx, window_rx) = mpsc::channel();
    let ready = Arc::new(Barrier::new(2));
    let receiver_ready = ready.clone();
    let reader = thread::spawn(move || {
        SETTINGS.affinity.pin(5);
        receiver_ready.wait();
        count_blocking_many(
            &receivers,
            window_rx.recv().expect("draft measurement window"),
        )
    });
    let pool = storage(workload);
    ready.wait();
    let window = Window::new(Instant::now(), SETTINGS.warmup, SETTINGS.measure);
    window_tx.send(window).expect("draft measurement start");
    let mut queued = VecDeque::with_capacity(BATCH);
    for (tag, deadline) in [(0, window.start), (super::MEASURED_TAG, window.end)] {
        let template = message(workload, None, tag);
        while Instant::now() < deadline {
            for sender in senders {
                if Instant::now() >= deadline {
                    break;
                }
                if workload.bulk {
                    queued.extend((0..BATCH).map(|_| template.clone()));
                    while !queued.is_empty() && Instant::now() < deadline {
                        match sender.try_send_many(&mut queued, BATCH) {
                            Ok(0) => thread::yield_now(),
                            Ok(_) => {}
                            Err(TrySendError::Full(message)) => {
                                queued.push_front(message);
                                thread::yield_now();
                            }
                            Err(error) => panic!("draft bulk send: {error}"),
                        }
                    }
                    queued.clear();
                } else {
                    super::send_blocking_batch_until_with(
                        sender,
                        || {
                            if pool.is_some() {
                                message(workload, pool.as_ref(), tag)
                            } else {
                                template.clone()
                            }
                        },
                        deadline,
                    );
                }
            }
        }
    }
    reader.join().expect("draft receiver").rate()
}

fn storage(workload: Workload) -> Option<PayloadPool> {
    if !workload.pooled {
        return None;
    }
    #[cfg(target_os = "linux")]
    if workload.pooled {
        let capacity = if workload.size >= 1024 * 1024 {
            256 * 1024 * 1024
        } else {
            64 * 1024 * 1024
        };
        return Some(
            omq_tokio::shm_payload_pool([(workload.size, capacity / workload.size)])
                .expect("SHM payload pool"),
        );
    }
    None
}

fn message(workload: Workload, pool: Option<&PayloadPool>, tag: u8) -> Message {
    let body = pool.map_or_else(
        || super::tagged_payload(workload.size, tag),
        |pool| {
            pool.message(workload.size, |bytes| bytes.fill(tag))
                .expect("pooled draft body")
        },
    );
    match workload.pattern {
        Pattern::ScatterGather => body,
        Pattern::RadioDish => Message::with_prefix(Bytes::from_static(GROUP), body),
    }
}
