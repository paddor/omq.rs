mod bench;
mod chart;
mod cli;
mod coord;
mod jsonl;
mod parse;
mod process;
mod tls;

use clap::Parser;
use cli::{ChartSub, Command, RunSub};

fn main() {
    let cli = cli::Cli::parse();
    process::install_reaper();

    let result = std::panic::catch_unwind(|| match cli.command {
        Command::Run { sub } => match sub {
            RunSub::Comparisons(args) => bench::comparisons::run(&args),
            RunSub::PushpullLz4(args) => bench::pushpull_lz4::run(args),
            RunSub::PushpullZstd(args) => bench::pushpull_zstd::run(args),
            RunSub::Compression(args) => bench::compression::run(args),
            RunSub::Dart(args) => bench::datagram::dart::run(args),
            RunSub::AeronDart(args) => bench::datagram::aeron::run(args),
            RunSub::QuinnDatagram(args) => bench::datagram::quinn::run(args),
        },
        Command::Chart { sub } => match sub {
            Some(ChartSub::Main) => chart::main_tcp::generate(),
            Some(ChartSub::Comparison) => chart::comparison::generate(),
            Some(ChartSub::Pubsub) => chart::pubsub::generate(),
            Some(ChartSub::Fanio) => chart::fanio::generate(),
            Some(ChartSub::Quic) => chart::quic::generate(),
            Some(ChartSub::Dart) => chart::dart::generate(),
            Some(ChartSub::Lz4) => chart::lz4::generate(),
            Some(ChartSub::Zstd) => chart::zstd::generate(),
            None => {
                chart::main_tcp::generate();
                chart::comparison::generate();
                chart::pubsub::generate();
                chart::fanio::generate();
                chart::quic::generate();
                chart::dart::generate();
                chart::lz4::generate();
                chart::zstd::generate();
            }
        },
    });

    process::reap_all();

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
