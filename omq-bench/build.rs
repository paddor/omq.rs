fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("find vendored protoc");
    unsafe { std::env::set_var("PROTOC", protoc) };
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/bench.proto"], &["proto"])
        .expect("compile gRPC proto");
}
