//! Sender bottleneck of one Zstd adaptation window.
//!
//! zrip's `AdaptiveLevel` walks the levels. The connection driver measures
//! each window, and this module names what limited the sender. A level
//! that carries clearly less input than the best recent one names the
//! limit too: encoding when it is higher, the output when it is lower.
//! Now and then one window runs at a fast safe level; when that carries
//! more input than the walk's level did recently, the walk starts over there.

use std::ops::RangeInclusive;
use std::time::Duration;

use zrip::adaptive::{AdaptiveLevel, Bottleneck, CompressStats};

use super::OutputWindow;

/// Level the walk falls back to: fast, with a fair ratio.
const SAFE_LEVEL: i32 = -4;
/// Windows before the first check of the safe level, and before the next
/// one once the walk settles at another level or rate.
const FIRST_CHECK: u64 = 16;
/// Windows between checks, doubling from the first to the second while
/// checks keep the walk where it is.
const CHECK_WINDOWS: (u64, u64) = (64, 256);
/// Windows the base holds without a window at its level.
const BASE_WINDOWS: u64 = 32;

/// zrip's level walk, and the input rate a step must keep.
#[derive(Clone, Debug)]
pub(super) struct LevelWalk {
    controller: AdaptiveLevel,
    range: RangeInclusive<i32>,
    /// The fallback level within `range`.
    safe: i32,
    /// The rate to keep: the best recent rate, and the level that reached it.
    base: Option<Pace>,
    /// The best recent rate at the level of the last window.
    current: Option<Pace>,
    check: Check,
    /// Input that offloads finished at an earlier level.
    carried: u64,
    /// Windows seen so far.
    window: u64,
}

impl LevelWalk {
    pub(super) fn new(range: RangeInclusive<i32>, start: i32) -> Self {
        Self {
            controller: AdaptiveLevel::new(range.clone(), start),
            safe: AdaptiveLevel::new(range.clone(), SAFE_LEVEL).level(),
            range,
            base: None,
            current: None,
            check: Check::new(),
            carried: 0,
            window: 0,
        }
    }

    pub(super) fn level(&self) -> i32 {
        if self.check.running {
            self.safe
        } else {
            self.controller.level()
        }
    }

    /// Counts input that offloads compressed at an earlier level.
    pub(super) fn carry(&mut self, plain: u64) {
        self.carried = self.carried.saturating_add(plain);
    }

    #[cfg(test)]
    pub(super) fn carried(&self) -> u64 {
        self.carried
    }

    /// Records one window compressed at `level` and returns the next level.
    pub(super) fn update(&mut self, level: i32, stats: CompressStats, output: OutputWindow) -> i32 {
        self.window += 1;
        let plain = stats
            .plain
            .saturating_add(std::mem::take(&mut self.carried));
        let rate = rate(plain, output.elapsed);
        if std::mem::take(&mut self.check.running) {
            self.finish_check(level, rate);
            return self.level();
        }

        let mut found = bottleneck(output);
        if let Some(rate) = rate {
            let pace = Pace {
                level,
                rate,
                window: self.window,
            };
            self.current = Some(decayed(self.current, pace));
            match self.compare(pace) {
                Verdict::Behind(limit) => found = Some(limit),
                // The step carried more input: keep it, whatever queued.
                Verdict::Ahead => found = None,
                Verdict::Even => {}
            }
        }

        let next = self.controller.update(stats, found);
        if level == self.safe {
            self.check.due = self.window + self.check.every;
        } else if next == level
            && let Some(current) = self.current
        {
            self.check.running = self.check.is_due(self.window, current);
        }
        self.level()
    }

    /// Falls back to the safe level if its window carried more input than
    /// the walk's level did.
    fn finish_check(&mut self, level: i32, rate: Option<u64>) {
        let Some(current) = self.current else {
            return;
        };

        match rate {
            Some(rate) if level == self.safe && rate > current.rate => {
                self.check.done(self.window, None);
                let pace = Pace {
                    level,
                    rate,
                    window: self.window,
                };
                self.controller = AdaptiveLevel::new(self.range.clone(), self.safe);
                self.base = Some(pace);
                self.current = Some(pace);
            }
            _ => self.check.done(self.window, Some(current)),
        }
    }

    /// Compares a window with the base. A window at another level that
    /// falls more than 5% behind names the limit: encoding above the base,
    /// the output below it. A closed loop waits on every message, so this
    /// shows before any queue does. A window at another level at least as
    /// fast becomes the base. At the base level the best recent rate holds,
    /// decaying by 1/16 per window, so one slow window does not lower the
    /// bar. A base not measured for `BASE_WINDOWS` windows gives way.
    fn compare(&mut self, pace: Pace) -> Verdict {
        let base = match self.base {
            Some(base) if pace.window - base.window < BASE_WINDOWS => base,
            _ => {
                self.base = Some(pace);
                return Verdict::Even;
            }
        };

        if pace.level == base.level {
            self.base = Some(decayed(Some(base), pace));
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

/// One window at the safe level now and then.
#[derive(Clone, Copy, Debug)]
struct Check {
    /// Window after which the next check runs.
    due: u64,
    /// Windows between checks.
    every: u64,
    /// The next window runs at the safe level.
    running: bool,
    /// The walk's level and rate when the last check kept it.
    kept: Option<Pace>,
}

impl Check {
    const fn new() -> Self {
        Self {
            due: FIRST_CHECK,
            every: CHECK_WINDOWS.0,
            running: false,
            kept: None,
        }
    }

    /// Whether a check is due at `window` for a walk settled at `current`.
    /// A walk that settled at another level or rate since the last check
    /// gets checked soon.
    fn is_due(&mut self, window: u64, current: Pace) -> bool {
        if self.kept.is_some_and(|kept| moved(kept, current)) {
            self.every = CHECK_WINDOWS.0;
            self.due = self.due.min(window + FIRST_CHECK);
            self.kept = None;
        }
        window >= self.due
    }

    /// Schedules the next check after one at `window` that kept the walk
    /// at `kept`, or fell back.
    fn done(&mut self, window: u64, kept: Option<Pace>) {
        self.every = if kept.is_some() {
            (self.every * 2).min(CHECK_WINDOWS.1)
        } else {
            CHECK_WINDOWS.0
        };
        self.due = window + self.every;
        self.kept = kept;
    }
}

/// Whether the walk moved to another level, or its rate by more than 1/4.
fn moved(kept: Pace, current: Pace) -> bool {
    let (was, now) = (u128::from(kept.rate), u128::from(current.rate));
    kept.level != current.level || now * 4 < was * 3 || now * 3 > was * 4
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
    window: u64,
}

/// `pace`, or the best recent rate at its level decayed by 1/16.
fn decayed(best: Option<Pace>, pace: Pace) -> Pace {
    match best {
        Some(best) if best.level == pace.level => Pace {
            rate: pace.rate.max(best.rate - best.rate / 16),
            ..pace
        },
        _ => pace,
    }
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
