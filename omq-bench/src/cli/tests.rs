use super::{Cli, Command, RunSub};
use clap::{CommandFactory, Parser};

fn run(args: &[&str]) -> RunSub {
    let cli = Cli::try_parse_from([&["omq_bench", "run"], args].concat()).unwrap();
    let Command::Run { sub } = cli.command else {
        panic!("not a run command");
    };

    sub
}

#[test]
fn command_definition_is_valid() {
    Cli::command().debug_assert();
}

#[test]
fn compression_commands_keep_their_own_defaults() {
    let RunSub::PushpullLz4(lz4) = run(&["pushpull-lz4"]) else {
        panic!("not pushpull-lz4");
    };

    assert_eq!(lz4.common.transports, "tcp,lz4+tcp");
    assert!((lz4.common.link.warmup_seconds - 0.5).abs() < f64::EPSILON);

    let RunSub::PushpullZstd(zstd) = run(&["pushpull-zstd"]) else {
        panic!("not pushpull-zstd");
    };

    assert_eq!(zstd.common.transports, "zstd+tcp");
    assert!((zstd.common.link.warmup_seconds - 1.0).abs() < f64::EPSILON);
    assert_eq!(zstd.level_range, [-7, 4]);
}

#[test]
fn grouped_comparison_flags_keep_their_names() {
    let RunSub::Comparisons(args) = run(&[
        "comparisons",
        "--curve",
        "--curve-peers=8",
        "--no-pubsub",
        "--pubsub-receiver-workers=2",
        "--latency-iterations=10",
        "--fanin",
        "--fanout-peers=2,3",
    ]) else {
        panic!("not comparisons");
    };

    assert!(args.curve.enabled);
    assert_eq!(args.curve.curve_peers, 8);
    assert!(args.pubsub.no_pubsub);
    assert_eq!(args.pubsub.pubsub_receiver_workers, Some(2));
    assert_eq!(args.latency.latency_iterations, 10);
    assert_eq!(args.latency.latency_warmup, 500);
    assert!(args.fanio.fanin && !args.fanio.fanout);
    assert_eq!(args.fanio.fanout_peers, [2, 3]);
}
