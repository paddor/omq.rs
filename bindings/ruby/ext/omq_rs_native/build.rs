use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=RUBY");
    println!("cargo:rustc-check-cfg=cfg(ruby_engine, values(\"mri\", \"truffleruby\"))");

    let ruby = std::env::var("RUBY").unwrap_or_else(|_| "ruby".to_string());
    let output = Command::new(&ruby)
        .arg("-rrbconfig")
        .arg("-e")
        .arg("print RbConfig::CONFIG.fetch('ruby_install_name')")
        .output()
        .unwrap_or_else(|err| panic!("failed to run {ruby}: {err}"));

    assert!(
        output.status.success(),
        "failed to query Ruby engine with {ruby}"
    );

    match String::from_utf8_lossy(&output.stdout).trim() {
        "truffleruby" => println!("cargo:rustc-cfg=ruby_engine=\"truffleruby\""),
        _ => println!("cargo:rustc-cfg=ruby_engine=\"mri\""),
    }
}
