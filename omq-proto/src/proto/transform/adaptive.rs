//! Sender bottleneck of one Zstd adaptation window.
//!
//! zrip's `AdaptiveLevel` walks the levels. The connection driver measures
//! each window, and this module names what limited the sender. A level
//! that carries clearly less input than the best recent one names the
//! limit too: encoding when it is higher, the output when it is lower.

use std::ops::RangeInclusive;
use std::time::Duration;

use zrip::adaptive::{AdaptiveLevel, Bottleneck, CompressStats};

use super::OutputWindow;

/// zrip's level walk, and the input rate a step must keep.
#[derive(Clone, Debug)]
pub(super) struct LevelWalk {
    controller: AdaptiveLevel,
    /// The rate to keep: the best recent rate, and the level that reached it.
    base: Option<Pace>,
    /// Input that offloads finished at an earlier level.
    carried: u64,
}

impl LevelWalk {
    pub(super) fn new(range: RangeInclusive<i32>, start: i32) -> Self {
        Self {
            controller: AdaptiveLevel::new(range, start),
            base: None,
            carried: 0,
        }
    }

    pub(super) fn level(&self) -> i32 {
        self.controller.level()
    }

    /// Counts input that offloads compressed at an earlier level.
    pub(super) fn carry(&mut self, plain: u64) {
        self.carried += plain;
    }

    #[cfg(test)]
    pub(super) fn carried(&self) -> u64 {
        self.carried
    }

    /// Records one window compressed at `level` and returns the next level.
    pub(super) fn update(&mut self, level: i32, stats: CompressStats, output: OutputWindow) -> i32 {
        let plain = stats.plain + std::mem::take(&mut self.carried);
        let mut found = bottleneck(output);
        if let Some(rate) = rate(plain, output.elapsed) {
            match self.compare(Pace { level, rate }) {
                Verdict::Behind(limit) => found = Some(limit),
                // The step carried more input: keep it, whatever queued.
                Verdict::Ahead => found = None,
                Verdict::Even => {}
            }
        }
        self.controller.update(stats, found)
    }

    /// Compares a window with the base. A window at another level that
    /// falls more than 5% behind names the limit: encoding above the base,
    /// the output below it. A closed loop waits on every message, so this
    /// shows before any queue does. A window at another level at least as
    /// fast becomes the base. At the base level the best recent rate holds,
    /// decaying by 1/16 per window, so one slow window does not lower the bar.
    fn compare(&mut self, pace: Pace) -> Verdict {
        let Some(base) = self.base else {
            self.base = Some(pace);
            return Verdict::Even;
        };

        if pace.level == base.level {
            self.base = Some(Pace {
                rate: pace.rate.max(base.rate - base.rate / 16),
                ..pace
            });
            return Verdict::Even;
        }

        if pace.rate >= base.rate {
            self.base = Some(pace);
            return Verdict::Ahead;
        }

        if u128::from(pace.rate) * 100 >= u128::from(base.rate) * 95 {
            return Verdict::Even;
        }

        Verdict::Behind(if pace.level > base.level {
            Bottleneck::Cpu
        } else {
            Bottleneck::Output
        })
    }
}

/// How a window's input rate compares with the base.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    /// More than 5% slower, at another level: what limits it.
    Behind(Bottleneck),
    /// At least as fast, at another level.
    Ahead,
    /// At the base level, or within 5%.
    Even,
}

/// Input bytes per second one window carried at one level.
#[derive(Clone, Copy, Debug)]
struct Pace {
    level: i32,
    rate: u64,
}

fn rate(plain: u64, elapsed: Duration) -> Option<u64> {
    let nanos = elapsed.as_nanos();
    (nanos > 0)
        .then(|| u64::try_from(u128::from(plain) * 1_000_000_000 / nanos).unwrap_or(u64::MAX))
}

/// What clearly limited the sender during `output`, if anything.
fn bottleneck(output: OutputWindow) -> Option<Bottleneck> {
    let elapsed = output.elapsed.as_nanos();
    let blocked = output.blocked.as_nanos();

    if elapsed == 0 || output.batches == 0 {
        return None;
    }

    // The transport refused bytes for at least 1/16 of the window.
    if blocked > 0 && blocked * 16 >= elapsed {
        return Some(Bottleneck::Output);
    }

    // The input queue filled up, or more than 1/8 of the batches stopped at
    // their limit with input left.
    if output.input_full > 0 || u64::from(output.overflowed) * 8 > u64::from(output.batches) {
        return (blocked * 64 < elapsed).then_some(Bottleneck::Cpu);
    }

    // Encoding kept up with the input. A saturated link can queue in the
    // kernel send buffer without refusing bytes, so spare time goes to ratio.
    Some(Bottleneck::Input)
}

#[cfg(test)]
mod tests;
