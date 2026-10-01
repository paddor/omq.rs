//! Verified WSS credentials for the existing separate-process benchmark peer.

use omq_tokio::Options;

pub(super) fn configure(options: &mut Options) {
    if let Some(cert) = read_pem("OMQ_BENCH_TLS_CERT_FILE") {
        options.wss_tls.server_cert_pem = Some(cert);
    }
    if let Some(key) = read_pem("OMQ_BENCH_TLS_KEY_FILE") {
        options.wss_tls.server_key_pem = Some(key);
    }
    if let Some(trust) = read_pem("OMQ_BENCH_TLS_TRUST_FILE") {
        options.wss_tls.trust_pem = Some(trust);
        options.wss_tls.trust_system = false;
    }
    if let Ok(name) = std::env::var("OMQ_BENCH_TLS_NAME") {
        options.wss_tls.hostname = Some(name);
    }
}

fn read_pem(name: &str) -> Option<Vec<u8>> {
    std::env::var_os(name).map(|path| std::fs::read(path).expect("read benchmark TLS PEM"))
}
