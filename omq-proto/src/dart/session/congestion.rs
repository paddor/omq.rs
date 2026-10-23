//! Congestion window, RTT estimate, and send pacing.

use std::time::Duration;

use super::{INITIAL_RTT, MAX_DATAGRAM};

#[derive(Debug)]
pub(super) struct Congestion {
    /// Bytes sent and not yet acknowledged or declared lost.
    pub(super) flight: usize,
    pub(super) cwnd: usize,
    pub(super) ssthresh: usize,
    pub(super) recovery_end: u64,
    pub(super) rtt: Duration,
    pub(super) rttvar: Duration,
}

impl Congestion {
    pub(super) fn new() -> Self {
        Self {
            flight: 0,
            cwnd: 10 * MAX_DATAGRAM,
            ssthresh: usize::MAX,
            recovery_end: 0,
            rtt: INITIAL_RTT,
            rttvar: INITIAL_RTT / 2,
        }
    }

    /// Whether `bytes` more on top of `reserved` would exceed the window.
    pub(super) fn exceeds_window(&self, reserved: usize, bytes: usize) -> bool {
        self.flight.saturating_add(reserved).saturating_add(bytes) > self.cwnd
    }

    /// Smooth one RTT sample into `rtt` and `rttvar`.
    pub(super) fn sample_rtt(&mut self, sample: Duration) {
        self.rttvar = (self.rttvar * 3 + self.rtt.abs_diff(sample)) / 4;
        self.rtt = (self.rtt * 7 + sample) / 8;
    }

    /// Slow start below `ssthresh`, then about one datagram per window.
    pub(super) fn grow(&mut self, acknowledged_bytes: usize, max: usize) {
        let increment = if self.cwnd < self.ssthresh {
            acknowledged_bytes
        } else {
            (MAX_DATAGRAM * acknowledged_bytes / self.cwnd).max(1)
        };
        self.cwnd = self.cwnd.saturating_add(increment).min(max);
    }

    /// Halve the window. Recovery lasts until `cursor` is acknowledged.
    pub(super) fn back_off(&mut self, cursor: u64) {
        self.cwnd = (self.cwnd / 2).max(2 * MAX_DATAGRAM);
        self.ssthresh = self.cwnd;
        self.recovery_end = cursor;
    }

    /// Window bytes per second at the smoothed RTT.
    pub(super) fn rate(&self) -> u64 {
        (self.cwnd as f64 / self.rtt.as_secs_f64().max(0.000_001)) as u64
    }
}

#[derive(Debug, Default)]
pub(super) struct Pacing {
    pub(super) rate: Option<u64>,
    /// Datagram size `delay` was computed for.
    pub(super) bytes: usize,
    delay: Duration,
    /// Earliest time the next paced datagram may leave.
    pub(super) next_at: Duration,
}

impl Pacing {
    pub(super) fn set_rate(&mut self, rate: Option<u64>) {
        self.bytes = 0;
        self.rate = rate;
    }

    /// Push `next_at` past a datagram of `bytes` sent at `now`.
    pub(super) fn schedule(&mut self, now: Duration, bytes: usize) {
        let Some(rate) = self.rate else {
            return;
        };
        if self.bytes != bytes {
            self.bytes = bytes;
            self.delay = Duration::from_nanos((bytes as u64 * 1_000_000_000 / rate.max(1)).max(1));
        }
        self.next_at = self.next_at.max(now) + self.delay;
    }
}
