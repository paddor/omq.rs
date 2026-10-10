#[cfg(not(feature = "mom-bench"))]
fn main() {}

#[cfg(feature = "mom-bench")]
fn main() {
    println!("cargo:rerun-if-changed=proto/bench.proto");
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("find vendored protoc");
    // SAFETY: This build script has not started any threads.
    unsafe { std::env::set_var("PROTOC", protoc) };
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/bench.proto"], &["proto"])
        .expect("compile gRPC proto");
}
