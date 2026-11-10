use clap::Parser;

use super::*;
use crate::cli::{Cli, Command, RunSub};

fn dart_args(extra: &[&str]) -> DartArgs {
    let cli =
        Cli::try_parse_from([&["omq_bench", "run", "dart", "--binary=dart-peer"], extra].concat())
            .unwrap();
    let Command::Run {
        sub: RunSub::Dart(args),
    } = cli.command
    else {
        panic!("not a dart run");
    };

    args
}

#[test]
fn args_accept_the_defaults() {
    validate_args(&dart_args(&[]));
}

#[test]
#[should_panic(expected = "--window-messages must be a power of two up to 65536")]
fn args_reject_an_uneven_window() {
    validate_args(&dart_args(&["--window-messages=300"]));
}

#[test]
#[should_panic(expected = "--runtime current needs --kind latency")]
fn args_reject_throughput_on_a_current_thread_runtime() {
    validate_args(&dart_args(&[
        "--runtime=current",
        "--spin=0",
        "--io-spin=0",
    ]));
}

#[test]
fn gates_accept_the_default_workload() {
    validate_gates(&dart_args(&["--check-gates"]));
}

#[test]
#[should_panic(expected = "--check-gates needs --window-messages 256")]
fn gates_name_the_mismatched_flag() {
    validate_gates(&dart_args(&["--check-gates", "--window-messages=512"]));
}

#[test]
#[should_panic(expected = "--check-gates excludes --latency-samples")]
fn gates_reject_latency_samples() {
    validate_gates(&dart_args(&["--check-gates", "--latency-samples=samples"]));
}
