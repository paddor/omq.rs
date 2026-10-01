//! Self-signed TLS credentials for encrypted benchmark transports.

use std::path::PathBuf;

/// Writes a fresh `localhost` certificate and key under the cache directory
/// and passes them to every spawned peer through `OMQ_BENCH_TLS_*`.
pub(crate) fn install_bench_credentials() {
    let dir = crate::jsonl::cache_dir().join("bench-tls");
    std::fs::create_dir_all(&dir).expect("create benchmark TLS directory");
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .expect("generate benchmark certificate");
    let cert = write(&dir, "cert.pem", certified.cert.pem().as_bytes());
    let key = write(
        &dir,
        "key.pem",
        certified.signing_key.serialize_pem().as_bytes(),
    );
    crate::process::set_peer_env(vec![
        ("OMQ_BENCH_TLS_CERT_FILE".into(), cert.clone()),
        ("OMQ_BENCH_TLS_KEY_FILE".into(), key),
        ("OMQ_BENCH_TLS_TRUST_FILE".into(), cert),
        ("OMQ_BENCH_TLS_NAME".into(), "localhost".into()),
    ]);
}

fn write(dir: &std::path::Path, name: &str, contents: &[u8]) -> String {
    let path: PathBuf = dir.join(name);
    std::fs::write(&path, contents).expect("write benchmark TLS file");
    path.to_str().expect("UTF-8 cache path").to_string()
}
