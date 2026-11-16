//! Compile the PHP stream shim against the PHP headers `php-config` reports.

use std::process::Command;

fn main() {
    let php_config = std::env::var("PHP_CONFIG").unwrap_or_else(|_| "php-config".into());
    println!("cargo:rerun-if-env-changed=PHP_CONFIG");
    println!("cargo:rerun-if-changed=src/fd_stream.c");

    let output = Command::new(&php_config)
        .arg("--includes")
        .output()
        .unwrap_or_else(|error| panic!("failed to run {php_config}: {error}"));
    assert!(output.status.success(), "{php_config} --includes failed");
    let includes = String::from_utf8(output.stdout).expect("php-config output is UTF-8");

    let mut build = cc::Build::new();
    build.file("src/fd_stream.c").warnings(false);
    for flag in includes.split_whitespace() {
        if let Some(dir) = flag.strip_prefix("-I") {
            build.include(dir);
        }
    }

    build.compile("omq_php_fd_stream");

    // The extension takes the PHP API from the PHP binary that loads it.
    // macOS's linker rejects those undefined symbols unless told so.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-undefined,dynamic_lookup");
    }
}
