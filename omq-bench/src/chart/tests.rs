use std::collections::HashSet;
use std::mem::discriminant;

use clap::CommandFactory;

use super::ALL;
use crate::cli::Cli;

#[test]
fn bare_chart_command_generates_every_chart_set_once() {
    let cli = Cli::command();
    let chart = cli.find_subcommand("chart").unwrap();
    let distinct: HashSet<_> = ALL.iter().map(discriminant).collect();

    assert_eq!(distinct.len(), ALL.len());
    assert_eq!(ALL.len(), chart.get_subcommands().count());
}
