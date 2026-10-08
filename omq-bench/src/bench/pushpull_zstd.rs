use super::pushpull_compression::{self, Config};
use crate::cli::PushpullZstdArgs;

pub(crate) fn run(args: PushpullZstdArgs) {
    pushpull_compression::run(&Config {
        feature: "zstd",
        transports: args.transports,
        sizes: args.sizes,
        duration: args.duration,
        rounds: args.rounds,
        quick: args.quick,
        dict_sizes: args.dict_sizes,
        level: args.level,
        link: args.link,
    });
}
