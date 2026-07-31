mod bench;
mod chart;
mod cli;
mod coord;
mod jsonl;
mod parse;
mod process;

use clap::Parser;
use cli::{ChartSub, Command, RunSub};

fn main() {
    let cli = cli::Cli::parse();
    process::install_reaper();

    let result = std::panic::catch_unwind(|| match cli.command {
        Command::Run { sub } => match sub {
            RunSub::Comparisons(args) => bench::comparisons::run(args),
            RunSub::PushpullLz4(args) => bench::pushpull_lz4::run(args),
            RunSub::PushpullZstd(args) => bench::pushpull_zstd::run(args),
            RunSub::Compression(args) => bench::compression::run(args),
        },
        Command::Chart { sub } => match sub {
            Some(ChartSub::Main) => chart::main_tcp::generate(),
            Some(ChartSub::Comparison) => chart::comparison::generate(),
            Some(ChartSub::Pubsub) => chart::pubsub::generate(),
            Some(ChartSub::Fanio) => chart::fanio::generate(),
            Some(ChartSub::Lz4) => chart::lz4::generate(),
            None => {
                chart::main_tcp::generate();
                chart::comparison::generate();
                chart::pubsub::generate();
                chart::fanio::generate();
                chart::lz4::generate();
            }
        },
    });

    process::reap_all();

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
