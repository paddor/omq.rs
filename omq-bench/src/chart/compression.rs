use plotters::prelude::RGBColor;

use super::compression_load;
use super::pushpull_compression::{self, CompressionChart, Series, Source};

pub(super) const LZ4: Source = Source {
    cache_file: "results_pushpull_lz4.jsonl",
    pattern: "pushpull_lz4",
    level_range: None,
    warmup_seconds: 0.5,
    arena_bytes: 16384,
};

pub(super) const ZSTD: Source = Source {
    cache_file: "results_pushpull_zstd.jsonl",
    pattern: "pushpull_zstd",
    level_range: Some([-7, 4]),
    // The adaptive level settles within about 1 s.
    warmup_seconds: 1.0,
    arena_bytes: 32768,
};

const TCP_COLOR: RGBColor = RGBColor(250, 204, 21);
const LZ4_COLOR: RGBColor = RGBColor(96, 165, 250);
const LZ4_DICT_COLOR: RGBColor = RGBColor(167, 139, 250);
const ZSTD_COLOR: RGBColor = RGBColor(248, 113, 113);
const ZSTD_DICT_COLOR: RGBColor = RGBColor(251, 146, 60);

const SIZES: &[u64] = &[16, 32, 64, 256, 1024, 4096, 16384, 65536, 262_144];

pub(super) const SATURATION: CompressionChart = CompressionChart {
    output_file: "compression_tcp.svg",
    title: "PUSH/PULL LZ4 and Zstd (adaptive L-7..4), varied binary/JSON, netem, 1 IO",
    series: &[
        Series {
            key: "tcp",
            label: "tcp (no compression)",
            color: TCP_COLOR,
            source: &LZ4,
        },
        Series {
            key: "lz4+tcp",
            label: "lz4+tcp",
            color: LZ4_COLOR,
            source: &LZ4,
        },
        Series {
            key: "zstd+tcp",
            label: "zstd+tcp",
            color: ZSTD_COLOR,
            source: &ZSTD,
        },
    ],
    sizes: SIZES,
    panels: &[
        (1000, "1 Gbps netem link"),
        (100, "100 Mbps netem link"),
        (10, "10 Mbps netem link"),
    ],
};

pub(super) const FIXED_LOAD: CompressionChart = CompressionChart {
    output_file: "compression_fixed_load.svg",
    title: "PUSH/PULL LZ4 and Zstd at fixed send rates, varied binary/JSON, loopback, 1 IO",
    series: &[
        Series {
            key: "lz4+tcp",
            label: "lz4+tcp",
            color: LZ4_COLOR,
            source: &LZ4,
        },
        Series {
            key: "lz4+tcp+dict",
            label: "lz4+tcp + 2 KiB dict",
            color: LZ4_DICT_COLOR,
            source: &LZ4,
        },
        Series {
            key: "zstd+tcp",
            label: "zstd+tcp",
            color: ZSTD_COLOR,
            source: &ZSTD,
        },
        Series {
            key: "zstd+tcp+dict",
            label: "zstd+tcp + 2 KiB dict",
            color: ZSTD_DICT_COLOR,
            source: &ZSTD,
        },
    ],
    sizes: SIZES,
    panels: &[
        (1000, "1k msg/s"),
        (10_000, "10k msg/s"),
        (100_000, "100k msg/s"),
        (1_000_000, "1M msg/s"),
    ],
};

pub(crate) fn generate() {
    pushpull_compression::generate(&SATURATION);
    compression_load::generate(&FIXED_LOAD);
}
