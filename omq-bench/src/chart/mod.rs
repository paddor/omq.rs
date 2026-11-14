mod common;
mod comparison;
mod compression;
mod compression_load;
mod dart;
mod fanio;
mod lossy;
mod main_tcp;
mod pubsub;
mod pushpull_compression;
mod quic;
mod rate_limit;
mod shm;

pub(crate) use common::fmt_size;

use crate::cli::ChartSub;

/// Chart sets a bare `chart` command generates, in this order.
const ALL: [ChartSub; 10] = [
    ChartSub::Main,
    ChartSub::Comparison,
    ChartSub::Pubsub,
    ChartSub::Shm {
        cached_baselines: false,
    },
    ChartSub::Fanio,
    ChartSub::Quic,
    ChartSub::Dart,
    ChartSub::Lossy,
    ChartSub::Compression,
    ChartSub::RateLimit,
];

/// Generates the chart set `sub`, or every set in `ALL` order without one.
pub(crate) fn generate(sub: Option<ChartSub>) {
    match sub {
        Some(sub) => generate_set(&sub),
        None => ALL.iter().for_each(generate_set),
    }
}

fn generate_set(sub: &ChartSub) {
    match sub {
        ChartSub::Shm { cached_baselines } => {
            shm::generate(*cached_baselines).expect("generate SHM charts");
        }
        ChartSub::Main => main_tcp::generate(),
        ChartSub::Comparison => comparison::generate(),
        ChartSub::Pubsub => pubsub::generate(),
        ChartSub::Fanio => fanio::generate(),
        ChartSub::Quic => quic::generate(),
        ChartSub::Dart => dart::generate(),
        ChartSub::Lossy => lossy::generate(),
        ChartSub::Compression => compression::generate(),
        ChartSub::RateLimit => rate_limit::generate(),
    }
}

#[cfg(test)]
mod tests;
