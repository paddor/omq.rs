//! Quinn endpoint and transport configuration for the raw OMQ profile.

use std::sync::Arc;

use omq_proto::options::QuicOptions;
use omq_proto::{Error, Result};

use super::ALPN;

/// Data-stream credit plus a liveness reserve. Unread data can hold at most
/// one stream window of connection credit, so liveness records keep the
/// remaining reserve. The reserve exceeds one eighth of the connection
/// window, which is Quinn's `MAX_DATA` update threshold. The same total
/// caps unacknowledged send storage, leaving room for liveness writes.
pub(super) fn transport(options: &QuicOptions, server: bool) -> quinn::TransportConfig {
    let stream = options.stream_window;
    let connection = u64::from(stream) + u64::from(options.liveness_reserve());
    let mut config = quinn::TransportConfig::default();
    config
        .stream_receive_window(quinn::VarInt::from_u32(stream))
        .receive_window(quinn::VarInt::from_u64(connection).expect("bounded window"))
        .send_window(connection)
        .max_concurrent_bidi_streams(quinn::VarInt::from_u32(if server { 2 } else { 0 }))
        .max_concurrent_uni_streams(quinn::VarInt::from_u32(0))
        .datagram_receive_buffer_size(None)
        .datagram_send_buffer_size(0)
        .max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(options.idle_timeout).expect("validated idle timeout"),
        ))
        .keep_alive_interval(Some(options.keep_alive_interval))
        .congestion_controller_factory(Arc::new(quinn::congestion::CubicConfig::default()));
    config
}

/// Largest UDP payload an endpoint accepts. Ordinary Ethernet MTUs carry at
/// most 1472 bytes of UDP payload; path MTU discovery stops at 1452. Quinn
/// sizes each endpoint's receive buffer from this value times GRO segments,
/// so the 64-KiB default costs about 4 MiB per endpoint.
pub(super) const MAX_UDP_PAYLOAD: u16 = 1500;

pub(super) fn endpoint() -> quinn::EndpointConfig {
    let mut config = quinn::EndpointConfig::default();
    config
        .max_udp_payload_size(MAX_UDP_PAYLOAD)
        .expect("valid UDP payload size");
    config
}

pub(super) fn server(options: &QuicOptions, handshakes: u32) -> Result<quinn::ServerConfig> {
    let cert = options
        .server_cert_pem
        .as_deref()
        .ok_or_else(|| Error::Config("quic:// bind requires quic.server_cert_pem".into()))?;
    let key = options
        .server_key_pem
        .as_deref()
        .ok_or_else(|| Error::Config("quic:// bind requires quic.server_key_pem".into()))?;
    let mut tls = crate::transport::tls::server_config(cert, key)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.max_early_data_size = 0;
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
        .map_err(|e| Error::Config(format!("QUIC TLS config: {e}")))?;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config
        .transport_config(Arc::new(transport(options, true)))
        .migration(false)
        // Pending Initials beyond this are dropped; clients retransmit.
        .max_incoming(handshakes as usize)
        .incoming_buffer_size(64 * 1024)
        .incoming_buffer_size_total(u64::from(handshakes) * 64 * 1024);
    Ok(config)
}

pub(super) fn client(options: &QuicOptions) -> Result<quinn::ClientConfig> {
    let mut tls = crate::transport::tls::verified_client_config(
        options.trust_system,
        options.trust_pem.as_deref(),
    )?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.enable_early_data = false;
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
        .map_err(|e| Error::Config(format!("QUIC TLS config: {e}")))?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport(options, false)));
    Ok(config)
}

/// Reject a completed handshake that did not negotiate `expected`.
pub(super) fn check_alpn(connection: &quinn::Connection, expected: &[u8]) -> Result<()> {
    let negotiated = connection
        .handshake_data()
        .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
        .and_then(|data| data.protocol);
    if negotiated.as_deref() == Some(expected) {
        Ok(())
    } else {
        Err(Error::HandshakeFailed(format!(
            "QUIC peer did not negotiate {}",
            String::from_utf8_lossy(expected)
        )))
    }
}
