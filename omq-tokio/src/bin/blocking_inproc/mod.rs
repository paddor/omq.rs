//! Two application threads using blocking send/recv, with no receive spin.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use omq_tokio::{Context, Endpoint, Message, Options, SocketType};

use super::StartStop;

pub(super) fn run(ctx: &Context, name: String, size: usize, duration: Duration, hwm: u32) {
    assert!(hwm > 0, "benchmark HWM must be positive");
    assert!(!duration.is_zero(), "benchmark duration must be positive");

    let options = Options::default().send_hwm(hwm).recv_hwm(hwm);
    let pull = ctx.blocking_socket(SocketType::Pull, options.clone());
    let push = ctx.blocking_socket(SocketType::Push, options);
    let endpoint = Endpoint::Inproc { name };

    pull.bind(endpoint.clone()).expect("pull bind");
    push.connect(endpoint).expect("push connect");
    push.wait_connected(1, Duration::from_secs(5))
        .expect("wait for inproc connection");

    let signals = StartStop::new();
    let payload = super::bench_payload(size);
    let sender = spawn_sender(push.clone(), payload, signals.clone());
    let StartStop { barrier, stop } = signals;

    let receiver_pull = pull.clone();
    let receiver_push = push;

    let receiver = std::thread::spawn(move || {
        barrier.wait();
        let before = Usage::read();
        let started = Instant::now();
        let mut count = 0_u64;

        loop {
            receiver_pull.recv().expect("blocking receive");
            count += 1;

            if count.is_multiple_of(256) && started.elapsed() >= duration {
                break;
            }
        }

        let elapsed = started.elapsed();
        let usage = Usage::read().zip(before).map(|(after, before)| Usage {
            cpu_seconds: after.cpu_seconds - before.cpu_seconds,
            context_switches: after.context_switches - before.context_switches,
        });
        stop.store(true, Ordering::Release);
        // A sender parked on a full ring must wake before we join it.
        receiver_push
            .close_with_linger(Some(Duration::ZERO))
            .expect("close push");

        (count, elapsed, usage)
    });

    let (count, elapsed, usage) = receiver.join().expect("receiver thread");
    sender.join().expect("sender thread");
    pull.close_with_linger(Some(Duration::ZERO))
        .expect("close pull");

    println!("{count} {:.9} {size}", elapsed.as_secs_f64());
    print_stats(hwm, usage.as_ref());
}

/// Blocking `send` until `stop`. A send error is only expected once the
/// receiver has stopped and closed the PUSH.
fn spawn_sender(
    sender_push: omq_tokio::blocking::Socket,
    payload: bytes::Bytes,
    signals: StartStop,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let StartStop {
            barrier: sender_barrier,
            stop: sender_stop,
        } = signals;
        sender_barrier.wait();

        if payload.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
            while !sender_stop.load(Ordering::Acquire) {
                if let Err(error) = sender_push.send(Message::from_slice(&payload)) {
                    assert!(sender_stop.load(Ordering::Acquire), "send failed: {error}");
                    break;
                }
            }
        } else {
            let message = Message::single(payload);
            while !sender_stop.load(Ordering::Acquire) {
                if let Err(error) = sender_push.send(message.clone()) {
                    assert!(sender_stop.load(Ordering::Acquire), "send failed: {error}");
                    break;
                }
            }
        }
    })
}

/// Second output line: HWM, the ring size it implies, and process usage.
fn print_stats(hwm: u32, usage: Option<&Usage>) {
    // Each direction's ring holds send_hwm + recv_hwm, rounded up to a power of two.
    let ring_capacity = (hwm as usize).saturating_mul(2).next_power_of_two();
    print!("blocking-stats hwm={hwm} ring_capacity={ring_capacity}");

    if let Some(usage) = usage {
        print!(
            " cpu_seconds={:.9} context_switches={}",
            usage.cpu_seconds, usage.context_switches
        );
    }

    println!();
}

struct Usage {
    cpu_seconds: f64,
    context_switches: u64,
}

impl Usage {
    #[cfg(unix)]
    #[expect(
        clippy::unnecessary_wraps,
        reason = "same counter interface as non-Unix platforms"
    )]
    fn read() -> Option<Self> {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes the writable output on success.
        assert_eq!(
            unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
            0
        );
        // SAFETY: getrusage succeeded and initialized every field read below.
        let usage = unsafe { usage.assume_init() };

        Some(Self {
            cpu_seconds: crate::bench_common::timeval_secs(&usage.ru_utime)
                + crate::bench_common::timeval_secs(&usage.ru_stime),
            context_switches: u64::try_from(usage.ru_nvcsw + usage.ru_nivcsw)
                .expect("context switches"),
        })
    }

    #[cfg(not(unix))]
    fn read() -> Option<Self> {
        None
    }
}
