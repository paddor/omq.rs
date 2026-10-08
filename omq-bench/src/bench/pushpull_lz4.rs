use super::pushpull_compression::{self, Config};
use crate::cli::PushpullLz4Args;

pub(crate) fn run(args: PushpullLz4Args) {
    pushpull_compression::run(&Config {
        feature: "lz4",
        transports: args.transports,
        sizes: args.sizes,
        duration: args.duration,
        rounds: args.rounds,
        quick: args.quick,
        dict_sizes: args.dict_sizes,
        level: None,
        link: args.link,
    });
}
