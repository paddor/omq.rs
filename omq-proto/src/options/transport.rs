//! Settings for the DART, QUIC, and WebSocket transports.

#[cfg(any(feature = "dart", feature = "quic"))]
use std::time::Duration;

/// Network congestion policy for reliable DART.
#[cfg(feature = "dart")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DartCongestion {
    /// Loss-based congestion control, validated ECN feedback, and pacing.
    #[default]
    Adaptive,
    /// Fixed receiver window for provisioned networks. Repairs remain reliable.
    Lan,
}

/// ECN is enabled only with a validated adaptive feedback path.
#[cfg(feature = "dart")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DartEcn {
    /// Validate ECN feedback before using it for adaptive congestion control.
    #[default]
    Auto,
    /// Disable outgoing ECN marking and feedback validation.
    Disabled,
}

/// Settings for reliable, ordered, bounded `dart://` messages.
#[cfg(feature = "dart")]
#[derive(Clone, Copy, Debug)]
pub struct DartOptions {
    /// Maximum admitted peers across the socket. Default 1024.
    pub max_ready_peers: usize,
    /// UDP polling budget on the IO runtime. Default zero, at most 50 microseconds.
    /// [`Duration::MAX`] polls continuously, including while idle. Endpoint
    /// turns remain bounded so other runtime tasks and controls make progress.
    pub io_spin: Duration,
    /// Automatic or disabled ECN. LAN mode never marks outgoing traffic.
    pub ecn: DartEcn,
    /// Network congestion policy. Default adaptive.
    pub congestion: DartCongestion,
    /// Preallocated receive and retention slots per peer. Default 256;
    /// a power of two between 1 and 65536. Independent of body pool capacity.
    pub window_messages: usize,
    /// Optional wire-byte rate cap per peer. Zero is invalid.
    pub max_send_rate: Option<u64>,
}

#[cfg(feature = "dart")]
impl Default for DartOptions {
    fn default() -> Self {
        Self {
            max_ready_peers: 1024,
            io_spin: Duration::ZERO,
            ecn: DartEcn::Auto,
            congestion: DartCongestion::Adaptive,
            window_messages: 256,
            max_send_rate: None,
        }
    }
}

#[cfg(feature = "dart")]
impl DartOptions {
    pub(super) fn validate(self) -> crate::error::Result<()> {
        if self.max_ready_peers == 0 {
            return Err(crate::error::Error::Config(
                "dart.max_ready_peers must be nonzero".into(),
            ));
        }
        if !self.window_messages.is_power_of_two()
            || self.window_messages > 65536
            || self.max_send_rate == Some(0)
        {
            return Err(crate::error::Error::Config(
                "DART needs a power-of-two window <=65536 and a nonzero rate cap".into(),
            ));
        }
        if self.io_spin > Duration::from_micros(50) && self.io_spin != Duration::MAX {
            return Err(crate::error::Error::Config(
                "dart.io_spin must be at most 50 microseconds or Duration::MAX".into(),
            ));
        }
        Ok(())
    }
}

/// Settings for OMQ over QUIC (`quic://`).
///
/// Bind requires a server certificate and key. Connect always verifies the
/// server certificate against `trust_pem` and/or the platform store; there
/// is no accept-any-certificate mode. Mutual TLS is not implemented.
///
/// The data stream receive window bounds unread data per peer. The
/// connection window adds a reserve (one quarter of the stream window, at
/// least 64 KiB) so carrier liveness records keep flowing while the data
/// stream is blocked by local receive backpressure.
#[cfg(feature = "quic")]
#[derive(Clone)]
pub struct QuicOptions {
    /// PEM-encoded server certificate chain for bind.
    pub server_cert_pem: Option<Vec<u8>>,
    /// PEM-encoded server private key for bind.
    pub server_key_pem: Option<Vec<u8>>,
    /// PEM-encoded trust anchors for connect.
    pub trust_pem: Option<Vec<u8>>,
    /// Trust the platform certificate store for connect. Default true.
    pub trust_system: bool,
    /// Verified server name override for connect, for example when the
    /// endpoint uses an IP address and the certificate names a host.
    pub server_name: Option<String>,
    /// Per-stream receive window in bytes. Default 1 MiB. Range 16 KiB to
    /// 256 MiB. Each admitted peer can hold this much unread data plus the
    /// liveness reserve.
    pub stream_window: u32,
    /// QUIC idle timeout. Default 10 s. A transport without any packets
    /// for this long is closed and reconnects normally.
    pub idle_timeout: Duration,
    /// QUIC keepalive interval. Default 2 s; must be below `idle_timeout`.
    /// Transport keepalive only; carrier liveness follows the heartbeat
    /// options.
    pub keep_alive_interval: Duration,
    /// Maximum ready QUIC connections across this socket's endpoints,
    /// raw and WebTransport together. Default 1024; must be nonzero.
    pub max_ready_peers: usize,
    /// WebTransport resource path on `quic://` listeners. Default `/omq`.
    /// Browsers connect to `https://host:port/path`.
    #[cfg(feature = "webtransport")]
    pub webtransport_path: String,
    /// Allowed browser origins for `quic://` listeners, for example
    /// `https://app.example.com`. Empty by default: CONNECT requests carrying
    /// Origin are rejected. Native clients may omit Origin. Origin is not
    /// authentication. Requires the `webtransport` feature.
    #[cfg(feature = "webtransport")]
    pub allowed_origins: Vec<String>,
}

#[cfg(feature = "quic")]
impl std::fmt::Debug for QuicOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("QuicOptions");
        debug.field("server_cert_pem", &self.server_cert_pem);
        debug.field(
            "server_key_pem",
            &self.server_key_pem.as_ref().map(|_| "<redacted>"),
        );
        debug.field("trust_pem", &self.trust_pem);
        debug.field("trust_system", &self.trust_system);
        debug.field("server_name", &self.server_name);
        debug.field("stream_window", &self.stream_window);
        debug.field("idle_timeout", &self.idle_timeout);
        debug.field("keep_alive_interval", &self.keep_alive_interval);
        debug.field("max_ready_peers", &self.max_ready_peers);
        #[cfg(feature = "webtransport")]
        debug.field("webtransport_path", &self.webtransport_path);
        #[cfg(feature = "webtransport")]
        debug.field("allowed_origins", &self.allowed_origins);
        debug.finish()
    }
}

#[cfg(feature = "quic")]
impl QuicOptions {
    /// Smallest accepted stream window.
    pub const MIN_STREAM_WINDOW: u32 = 16 * 1024;
    /// Largest accepted stream window.
    pub const MAX_STREAM_WINDOW: u32 = 256 * 1024 * 1024;

    /// Connection-level reserve above the data stream window.
    #[must_use]
    pub fn liveness_reserve(&self) -> u32 {
        (self.stream_window / 4).max(64 * 1024)
    }

    pub(super) fn validate(&self) -> crate::error::Result<()> {
        if !(Self::MIN_STREAM_WINDOW..=Self::MAX_STREAM_WINDOW).contains(&self.stream_window) {
            return Err(crate::error::Error::Config(format!(
                "quic.stream_window {} outside {}..={}",
                self.stream_window,
                Self::MIN_STREAM_WINDOW,
                Self::MAX_STREAM_WINDOW
            )));
        }
        if self.keep_alive_interval.is_zero() || self.keep_alive_interval >= self.idle_timeout {
            return Err(crate::error::Error::Config(
                "quic.keep_alive_interval must be nonzero and below quic.idle_timeout".into(),
            ));
        }
        if self.idle_timeout > Duration::from_hours(1) {
            return Err(crate::error::Error::Config(
                "quic.idle_timeout must not exceed one hour".into(),
            ));
        }
        if self.max_ready_peers == 0 {
            return Err(crate::error::Error::Config(
                "quic.max_ready_peers must be greater than zero".into(),
            ));
        }
        #[cfg(feature = "webtransport")]
        crate::proto::web_address::validate_ws_address(
            &crate::endpoint::Host::Name("localhost".into()),
            &self.webtransport_path,
        )
        .map_err(|_| crate::error::Error::Config("invalid quic.webtransport_path".into()))?;
        #[cfg(feature = "webtransport")]
        for origin in &self.allowed_origins {
            crate::proto::web_address::normalize_ws_origin(origin).map_err(|_| {
                crate::error::Error::Config(format!("invalid quic.allowed_origins entry {origin}"))
            })?;
        }
        Ok(())
    }
}

#[cfg(feature = "quic")]
impl Default for QuicOptions {
    fn default() -> Self {
        Self {
            server_cert_pem: None,
            server_key_pem: None,
            trust_pem: None,
            trust_system: true,
            server_name: None,
            stream_window: 1024 * 1024,
            idle_timeout: Duration::from_secs(10),
            keep_alive_interval: Duration::from_secs(2),
            max_ready_peers: 1024,
            #[cfg(feature = "webtransport")]
            webtransport_path: "/omq".into(),
            #[cfg(feature = "webtransport")]
            allowed_origins: Vec::new(),
        }
    }
}

/// WebSocket connection policy. Native clients may omit Origin; a present
/// Origin must match a listener's explicit HTTP(S) origin allowlist.
#[cfg(feature = "ws")]
#[derive(Clone, Debug)]
pub struct WsOptions {
    /// Allowed browser origins, for example `https://app.example.com`.
    /// Empty by default: requests carrying Origin are rejected. Matching uses
    /// scheme, normalized host, and port; no suffix or wildcard matching.
    /// Origin is not authentication, and native clients can forge it.
    pub allowed_origins: Vec<String>,

    /// Maximum ready WS/WSS connections across this socket's endpoints.
    /// Defaults to 1024; must be nonzero. Other transports do not consume
    /// this limit. Stricter socket-type limits still apply. A replacement
    /// identity may hand over an existing route without an extra ready slot.
    /// Pending handshakes have their separate admission limit.
    pub max_ready_peers: usize,
}

#[cfg(feature = "ws")]
impl Default for WsOptions {
    fn default() -> Self {
        Self {
            allowed_origins: Vec::new(),
            max_ready_peers: 1024,
        }
    }
}

/// TLS configuration for WSS endpoints. This covers server certificates
/// and client-side server certificate validation only. Mutual TLS/client
/// certificate authentication is not implemented.
#[cfg(feature = "ws")]
#[derive(Clone)]
pub struct WssTls {
    /// PEM-encoded server certificate chain for WSS bind.
    pub server_cert_pem: Option<Vec<u8>>,
    /// PEM-encoded server private key for WSS bind.
    pub server_key_pem: Option<Vec<u8>>,
    /// PEM-encoded trust anchors for WSS connect.
    pub trust_pem: Option<Vec<u8>>,
    /// Override server name used for WSS certificate verification.
    pub hostname: Option<String>,
    /// Trust the platform certificate store for WSS connect.
    pub trust_system: bool,
    /// Accept invalid server certificates on connect (for testing).
    pub accept_invalid_certs: bool,
}

#[cfg(feature = "ws")]
impl std::fmt::Debug for WssTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WssTls")
            .field("server_cert_pem", &self.server_cert_pem)
            .field(
                "server_key_pem",
                &self.server_key_pem.as_ref().map(|_| "<redacted>"),
            )
            .field("trust_pem", &self.trust_pem)
            .field("hostname", &self.hostname)
            .field("trust_system", &self.trust_system)
            .field("accept_invalid_certs", &self.accept_invalid_certs)
            .finish()
    }
}

#[cfg(feature = "ws")]
impl Default for WssTls {
    fn default() -> Self {
        Self {
            server_cert_pem: None,
            server_key_pem: None,
            trust_pem: None,
            hostname: None,
            trust_system: true,
            accept_invalid_certs: false,
        }
    }
}
