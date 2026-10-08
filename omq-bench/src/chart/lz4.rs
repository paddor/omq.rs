use plotters::prelude::RGBColor;

use super::pushpull_compression::{CompressionChart, Series, generate as generate_chart};

const SERIES: &[Series] = &[
    Series {
        key: "tcp",
        label: "tcp (no compression)",
        color: RGBColor(250, 204, 21),
    },
    Series {
        key: "lz4+tcp",
        label: "lz4+tcp",
        color: RGBColor(96, 165, 250),
    },
    Series {
        key: "lz4+tcp+dict",
        label: "lz4+tcp + dict",
        color: RGBColor(167, 139, 250),
    },
];

const CHART: CompressionChart = CompressionChart {
    cache_file: "results_pushpull_lz4.jsonl",
    pattern_prefix: "pushpull_lz4",
    dict_pattern: "pushpull_lz4_dict",
    dict_series_key: "lz4+tcp+dict",
    output_file: "lz4_tcp.svg",
    title: "PUSH/PULL LZ4, JSON payloads, 2 KiB dict, netem, 1 IO",
    series: SERIES,
    compression_level: None,
};

pub(crate) fn generate() {
    generate_chart(&CHART);
}
