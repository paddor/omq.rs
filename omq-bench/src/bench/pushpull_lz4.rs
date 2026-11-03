use super::pushpull_compression::{self, Config};
use crate::cli::PushpullLz4Args;

pub(crate) fn run(args: PushpullLz4Args) {
    pushpull_compression::run(&Config {
        feature: "lz4",
        common: args.common,
        level: None,
        level_range: None,
    });
}
