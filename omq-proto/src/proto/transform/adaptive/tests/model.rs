//! A simulated sender for `LevelWalk`: zrip's measured speed and ratio per
//! level, a link, and a closed or open input loop. Each window reports what
//! OMQ's connection driver would measure, with optional noise and lies.

#![expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "the model computes rates in f64"
)]

use std::time::Duration;

use zrip::adaptive::{CompressStats, MIN_WINDOW_BYTES};

use super::super::{LevelWalk, OutputWindow};

/// The levels of the default range.
pub(super) const LEVELS: [i32; 11] = [-7, -6, -5, -4, -3, -2, -1, 1, 2, 3, 4];
/// Seconds per window at least, as `ADAPT_WINDOW`.
const WINDOW: f64 = 0.1;
/// Unsent bytes the kernel takes before a write blocks.
const KERNEL_BUFFER: f64 = 900e3;
/// Bytes after which a batch stops.
const BATCH: f64 = 64e3;

/// zrip on one kind of message: one thread, i7-8700B.
#[derive(Clone, Copy, Debug)]
pub(super) struct Corpus {
    /// Bytes per message.
    pub message: f64,
    /// Input MB/s per level.
    pub speed: [f64; 11],
    /// Input bytes per compressed byte, per level.
    pub ratio: [f64; 11],
}

/// smg `EngineCoreRequest`s of 32k Qwen2.5 tokens, msgpack.
pub(super) const PROSE: Corpus = Corpus {
    message: 93_523.0,
    speed: [
        546., 553., 551., 548., 545., 545., 232., 571., 565., 96., 84.,
    ],
    ratio: [
        1.434, 1.434, 1.434, 1.435, 1.435, 1.462, 1.556, 1.459, 1.461, 1.711, 1.786,
    ],
};
pub(super) const CODE: Corpus = Corpus {
    message: 90_665.0,
    speed: [
        552., 557., 374., 315., 312., 245., 235., 216., 211., 124., 120.,
    ],
    ratio: [
        1.412, 1.425, 1.691, 1.831, 1.897, 2.189, 2.371, 2.379, 2.396, 2.576, 2.636,
    ],
};
pub(super) const DOCS: Corpus = Corpus {
    message: 86_100.0,
    speed: [
        547., 541., 447., 404., 391., 318., 232., 243., 238., 131., 99.,
    ],
    ratio: [
        1.406, 1.436, 1.518, 1.536, 1.540, 1.601, 1.962, 1.825, 1.835, 2.015, 2.201,
    ],
};
/// OMQ's benchmark corpus, 1 KiB messages (zrip's own model).
pub(super) const SMALL: Corpus = Corpus {
    message: 1024.0,
    speed: [
        286., 298., 234., 226., 221., 225., 220., 197., 197., 154., 154.,
    ],
    ratio: [
        1.86, 2.55, 2.89, 2.85, 2.94, 3.22, 3.65, 3.51, 3.51, 3.36, 3.37,
    ],
};

/// Where the input comes from.
#[derive(Clone, Copy, Debug)]
pub(super) enum Load {
    /// `streams` senders each send one message and wait for its reply,
    /// which comes `think` seconds after the message left the link.
    Closed { streams: u32, think: f64 },
    /// Input offered at bytes per second; `None` floods.
    Open(Option<f64>),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Plant {
    pub corpus: Corpus,
    /// Wire bytes per second; `None` is unlimited.
    pub link: Option<f64>,
    pub load: Load,
    /// Threads that compress.
    pub encoders: u32,
    /// Share of a core each thread gets.
    pub cpu: f64,
    /// Relative error of each window's measurements.
    pub noise: f64,
    /// Chance that a window reports random signals.
    pub lies: f64,
}

/// What a level sustains.
#[derive(Clone, Copy, Debug)]
pub(super) struct Steady {
    /// Input bytes per second.
    pub rate: f64,
    /// Share of the time the writer blocks.
    blocked: f64,
    /// Share of batches that stop at their limit with input left.
    backlog: f64,
    /// Whether input waits in a full queue.
    input_full: bool,
}

impl Plant {
    pub(super) const fn new(corpus: Corpus, link: Option<f64>, load: Load) -> Self {
        Self {
            corpus,
            link,
            load,
            encoders: 1,
            cpu: 1.0,
            noise: 0.0,
            lies: 0.0,
        }
    }

    fn speed(&self, index: usize) -> f64 {
        self.corpus.speed[index] * 1e6 * self.cpu
    }

    fn carry(&self, index: usize) -> f64 {
        self.link
            .map_or(f64::INFINITY, |link| link * self.corpus.ratio[index])
    }

    /// Share of batches that leave input queued at encoder use `used`.
    fn backlog(&self, used: f64) -> f64 {
        let batch = (BATCH / self.corpus.message).max(1.0);
        used.clamp(0.0, 1.0).powf(f64::from(self.encoders) * batch)
    }

    /// The steady state at ladder `index`.
    pub(super) fn steady(&self, index: usize) -> Steady {
        match self.load {
            Load::Closed { streams, think } => self.closed(index, streams, think),
            Load::Open(offered) => self.open(index, offered.unwrap_or(f64::INFINITY)),
        }
    }

    /// Mean value analysis: encoders (Seidmann's approximation for several)
    /// and the link queue, the reply wait is a delay.
    fn closed(&self, index: usize, streams: u32, think: f64) -> Steady {
        let message = self.corpus.message;
        let wire = message / self.corpus.ratio[index];
        let encode = message / self.speed(index);
        let threads = f64::from(self.encoders);
        let (queued, delay) = (encode / threads, encode * (threads - 1.0) / threads);
        let send = self.link.map_or(0.0, |link| wire / link);

        let (mut at_encoder, mut at_link, mut throughput) = (0.0, 0.0, 0.0);
        for n in 1..=streams {
            let encoding = queued * (1.0 + at_encoder);
            let sending = send * (1.0 + at_link);
            throughput = f64::from(n) / (think + delay + encoding + sending);
            at_encoder = throughput * encoding;
            at_link = throughput * sending;
        }

        let unsent = at_link * wire;
        let blocked = if unsent > KERNEL_BUFFER {
            throughput * send * (1.0 - KERNEL_BUFFER / unsent)
        } else {
            0.0
        };
        Steady {
            rate: throughput * message,
            blocked,
            backlog: self.backlog(throughput * queued),
            input_full: false,
        }
    }

    fn open(&self, index: usize, offered: f64) -> Steady {
        let encode = self.speed(index) * f64::from(self.encoders);
        let carry = self.carry(index);
        let rate = offered.min(encode).min(carry);
        if rate >= offered {
            return Steady {
                rate,
                blocked: 0.0,
                backlog: self.backlog(rate / encode),
                input_full: false,
            };
        }

        let blocked = if carry < encode {
            1.0 - carry / encode
        } else {
            0.0
        };
        Steady {
            rate,
            blocked,
            backlog: 1.0,
            input_full: true,
        }
    }

    /// The best rate any fixed level sustains, and its level.
    pub(super) fn best(&self) -> (i32, f64) {
        (0..LEVELS.len())
            .map(|index| (LEVELS[index], self.steady(index).rate))
            .fold(
                (0, 0.0),
                |best, entry| {
                    if entry.1 > best.1 { entry } else { best }
                },
            )
    }

    pub(super) fn rate_at(&self, level: i32) -> f64 {
        self.steady(index(level)).rate
    }

    /// One window at `level`, as the connection driver measures it.
    pub(super) fn window(&self, level: i32, rng: &mut Rng) -> (CompressStats, OutputWindow) {
        let index = index(level);
        let steady = self.steady(index);
        let rate = steady.rate * rng.around(self.noise);
        let elapsed = WINDOW.max(MIN_WINDOW_BYTES as f64 / rate);
        let plain = rate * elapsed;
        let busy = plain / self.speed(index) * rng.around(self.noise);
        let stats = CompressStats {
            plain: plain as u64,
            compressed: (plain / self.corpus.ratio[index]) as u64,
            busy: Duration::from_secs_f64(busy),
        };

        let batches = (plain / self.corpus.message.max(BATCH)).ceil().max(1.0);
        let mut output = OutputWindow {
            elapsed: Duration::from_secs_f64(elapsed),
            blocked: Duration::from_secs_f64(
                (elapsed * steady.blocked * rng.around(self.noise)).min(elapsed),
            ),
            batches: batches as u32,
            overflowed: (batches * (steady.backlog * rng.around(self.noise)).min(1.0)) as u32,
            input_full: if steady.input_full { batches as u32 } else { 0 },
        };
        if rng.unit() < self.lies {
            output.blocked = Duration::from_secs_f64(elapsed * rng.unit() * rng.unit());
            output.overflowed = (batches * rng.unit()) as u32;
            output.input_full = u32::from(rng.unit() < 0.3);
        }
        (stats, output)
    }
}

pub(super) fn index(level: i32) -> usize {
    LEVELS.iter().position(|l| *l == level).unwrap()
}

/// Levels the walk picks over `windows` windows of `plant`.
pub(super) fn run(walk: &mut LevelWalk, plant: &Plant, windows: usize, rng: &mut Rng) -> Vec<i32> {
    (0..windows)
        .map(|_| {
            let level = walk.level();
            let (stats, output) = plant.window(level, rng);
            walk.update(level, stats, output);
            level
        })
        .collect()
}

/// Mean steady rate of the levels picked, against the best fixed level.
pub(super) fn efficiency(plant: &Plant, levels: &[i32]) -> f64 {
    let sum: f64 = levels.iter().map(|level| plant.rate_at(*level)).sum();
    sum / levels.len() as f64 / plant.best().1
}

/// splitmix64.
pub(super) struct Rng(u64);

impl Rng {
    pub(super) const fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(super) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `0..1`.
    pub(super) fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Uniform in `1 - error..=1 + error`.
    pub(super) fn around(&mut self, error: f64) -> f64 {
        1.0 + error * (self.unit() * 2.0 - 1.0)
    }

    /// Log-uniform in `low..high`.
    pub(super) fn log(&mut self, low: f64, high: f64) -> f64 {
        low * (high / low).powf(self.unit())
    }

    pub(super) fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[(self.next() % items.len() as u64) as usize]
    }
}

/// Speed falls and ratio rises with the level, each with dips.
pub(super) fn random_corpus(rng: &mut Rng) -> Corpus {
    let mut speed = [0.0; 11];
    let mut ratio = [0.0; 11];
    speed[0] = rng.log(150.0, 900.0);
    ratio[0] = rng.log(1.05, 3.0);
    for i in 1..11 {
        speed[i] = speed[i - 1] * (0.55 + rng.unit() * 0.55);
        ratio[i] = ratio[i - 1] * (0.93 + rng.unit() * 0.22);
    }
    Corpus {
        message: rng.pick(&[1024.0, 16_384.0, 90_000.0, 1_000_000.0]),
        speed,
        ratio,
    }
}

pub(super) fn random_plant(rng: &mut Rng) -> Plant {
    let corpus = match rng.next() % 5 {
        0 => PROSE,
        1 => CODE,
        2 => DOCS,
        3 => SMALL,
        _ => random_corpus(rng),
    };
    let link = match rng.next() % 5 {
        0 => None,
        1 => Some(1.25e9),
        2 => Some(125e6),
        3 => Some(12.5e6),
        _ => Some(rng.log(5e6, 2e9)),
    };
    let load = if rng.unit() < 0.7 {
        Load::Closed {
            streams: rng.pick(&[1, 2, 4, 8, 16, 64, 256]),
            think: rng.pick(&[50e-6, 200e-6, 1e-3, 5e-3, 20e-3]),
        }
    } else {
        Load::Open(if rng.unit() < 0.3 {
            None
        } else {
            Some(rng.log(1e6, 2e9))
        })
    };
    Plant {
        encoders: rng.pick(&[1, 2, 4]),
        cpu: rng.pick(&[1.0, 0.5, 0.25]),
        noise: rng.pick(&[0.0, 0.05, 0.1, 0.2, 0.3]),
        lies: rng.pick(&[0.0, 0.05, 0.2]),
        ..Plant::new(corpus, link, load)
    }
}

/// Mean steady rate of the levels picked, against `level`'s.
pub(super) fn against(plant: &Plant, levels: &[i32], level: i32) -> f64 {
    let sum: f64 = levels.iter().map(|level| plant.rate_at(*level)).sum();
    sum / levels.len() as f64 / plant.rate_at(level)
}
