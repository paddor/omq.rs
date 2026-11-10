//! Benchmark runner and SVG chart generator for omq.rs.

mod bench;
mod chart;
mod cli;
mod coord;
mod jsonl;
mod parse;
mod process;
mod tls;

use clap::Parser;
use cli::{Command, RunSub};

fn main() {
    let cli = cli::Cli::parse();
    process::install_reaper();

    let result = std::panic::catch_unwind(|| match cli.command {
        Command::Run { sub } => match sub {
            RunSub::Comparisons(args) => bench::comparisons::run(&args),
            RunSub::PushpullLz4(args) => bench::pushpull_lz4::run(args),
            RunSub::PushpullZstd(args) => bench::pushpull_zstd::run(args),
            RunSub::PushpullRateLimit(args) => bench::pushpull_rate_limit::run(&args),
            RunSub::Compression(args) => bench::compression::run(args),
            RunSub::Dart(args) => bench::datagram::dart::run(args),
            RunSub::AeronDart(args) => bench::datagram::aeron::run(args),
            RunSub::QuinnDatagram(args) => bench::datagram::quinn::run(args),
        },
        Command::Chart { sub } => chart::generate(sub),
    });

    process::reap_all();

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
