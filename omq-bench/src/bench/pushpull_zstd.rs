use super::pushpull_compression::{self, Config};
use crate::cli::PushpullZstdArgs;

pub(crate) fn run(args: PushpullZstdArgs) {
    pushpull_compression::run(&Config {
        feature: "zstd",
        common: args.common,
        level: args.level,
        level_range: args.level.is_none().then(|| {
            assert_eq!(args.level_range.len(), 2, "--level-range takes MIN,MAX");
            [args.level_range[0], args.level_range[1]]
        }),
    });
}
