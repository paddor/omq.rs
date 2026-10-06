//! Verified WSS/QUIC credentials for the separate-process benchmark peer.

use omq_tokio::Options;

/// UDP socket buffer size for QUIC peers. Many connections share one UDP
/// socket per listener or IO thread, and loopback bursts overflow the
/// default 208 KiB receive buffer. Linux caps it at `net.core.rmem_max`.
#[cfg(feature = "quic")]
const QUIC_SOCKET_BUFFER: usize = 4 * 1024 * 1024;

#[cfg(feature = "quic")]
static QUIC_ENDPOINT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Records whether this peer's endpoint argument is QUIC, so only QUIC runs
/// get larger socket buffers.
pub(super) fn set_endpoint(arg: Option<&str>) {
    #[cfg(feature = "quic")]
    QUIC_ENDPOINT
        .set(arg.is_some_and(|a| a.starts_with("quic://")))
        .expect("endpoint set once");
    #[cfg(not(feature = "quic"))]
    let _ = arg;
}

pub(super) fn configure(options: &mut Options) {
    #[cfg(feature = "ws")]
    configure_wss(options);
    #[cfg(feature = "quic")]
    configure_quic(options);
}

#[cfg(feature = "quic")]
fn configure_quic(options: &mut Options) {
    options.quic.server_cert_pem = read_pem("OMQ_BENCH_TLS_CERT_FILE");
    options.quic.server_key_pem = read_pem("OMQ_BENCH_TLS_KEY_FILE");
    if let Some(trust) = read_pem("OMQ_BENCH_TLS_TRUST_FILE") {
        options.quic.trust_pem = Some(trust);
        options.quic.trust_system = false;
    }
    if let Ok(name) = std::env::var("OMQ_BENCH_TLS_NAME") {
        options.quic.server_name = Some(name);
    }
    if QUIC_ENDPOINT.get().copied().unwrap_or(false) {
        options.recv_buffer_size = Some(QUIC_SOCKET_BUFFER);
        options.send_buffer_size = Some(QUIC_SOCKET_BUFFER);
    }
    if let Ok(window) = std::env::var("OMQ_BENCH_QUIC_STREAM_WINDOW") {
        options.quic.stream_window = window.parse().expect("OMQ_BENCH_QUIC_STREAM_WINDOW");
    }
}

#[cfg(feature = "ws")]
fn configure_wss(options: &mut Options) {
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
