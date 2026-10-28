//! Socket options overlay and `zmq_setsockopt` / `zmq_getsockopt`.
//!
//! Both entry points dispatch through per-category handlers. The match
//! arms inside them are intentionally repetitive: each option is
//! self-contained and easy to audit. Extracting duration-conversion
//! helpers was considered and rejected as not worth the indirection.
#![expect(clippy::cast_possible_wrap)]

use std::time::Duration;

use bytes::Bytes;
use omq_tokio::MechanismSetup;
use omq_tokio::options::{KeepAlive, OnMute, ReconnectPolicy, WorkloadProfile};

use crate::socket::DEFAULT_HWM;

#[cfg(feature = "quic")]
mod quic;

/// Lock the overlay or return `ETERM` from the enclosing function. `.into()`
/// lets the same macro serve `c_int` and `Option<c_int>` returns.
macro_rules! lock_overlay {
    ($sock:expr) => {
        match $sock.overlay.lock() {
            Ok(g) => g,
            Err(_) => return crate::error::fail(crate::error::ETERM).into(),
        }
    };
}

// Declared after `lock_overlay!`, which these modules use.
mod codes;
mod get;
mod raw;
mod set;
mod set_security;

use codes::DEFAULT_HANDSHAKE_IVL_MS;
pub use get::zmq_getsockopt;
pub use set::zmq_setsockopt;
pub use set_security::{OmqPlainCredential, omq_socket_set_plain_server_credentials};

#[derive(Clone, Debug)]
#[expect(clippy::struct_excessive_bools)]
pub(crate) struct SocketOverlay {
    pub send_hwm: Option<u32>,
    pub recv_hwm: Option<u32>,
    pub linger: LingerSetting,
    pub identity: Bytes,
    pub router_mandatory: bool,
    pub reconnect_ivl: Option<Duration>,
    pub reconnect_ivl_max: Option<Duration>,
    pub heartbeat_ivl: Option<Duration>,
    pub heartbeat_ttl: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub handshake_ivl: Option<Duration>,
    pub max_message_size: Option<usize>,
    pub arena_threshold: Option<usize>,
    pub conflate: bool,
    pub tcp_keepalive: i32,
    pub tcp_keepalive_cnt: Option<u32>,
    pub tcp_keepalive_idle: Option<Duration>,
    pub tcp_keepalive_intvl: Option<Duration>,
    /// `ZMQ_TCP_MAXRT` in milliseconds; 0 keeps the system default.
    pub tcp_maxrt: i32,
    pub mechanism: MechanismOverlay,
    pub zap_domain: String,
    pub zap_enforce_domain: bool,
    pub sndbuf: Option<usize>,
    pub rcvbuf: Option<usize>,
    pub xpub_verbose: bool,
    pub on_mute: OnMute,
    pub compression_level: Option<i32>,
    pub compression_dict: Option<Bytes>,
    pub compression_auto_train: bool,
    pub workload_profile: Option<WorkloadProfile>,
    pub ipv6: bool,
    pub backlog: i32,
    pub immediate: bool,
    pub connect_timeout: i32,
    pub probe_router: bool,
    pub req_correlate: bool,
    pub req_relaxed: bool,
    pub xpub_nodrop: bool,
    pub reconnect_stop: i32,
    pub wss_key_pem: Option<SecretBytes>,
    pub wss_cert_pem: Option<Vec<u8>>,
    pub wss_trust_pem: Option<Vec<u8>>,
    pub wss_hostname: Option<String>,
    pub wss_trust_system: bool,
    pub ws_allowed_origins: Vec<String>,
    pub ws_max_ready_peers: i32,
    #[cfg(feature = "quic")]
    pub quic: QuicOverlay,
}

/// Keep the overlay's derived Debug without exposing a WSS private key.
#[derive(Clone)]
pub(crate) struct SecretBytes(Vec<u8>);

impl std::fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

/// `OMQ_QUIC_*` settings; fixed when the backend socket materializes.
#[cfg(feature = "quic")]
#[derive(Clone)]
pub(crate) struct QuicOverlay {
    pub cert_pem: Option<Vec<u8>>,
    pub key_pem: Option<Vec<u8>>,
    pub trust_pem: Option<Vec<u8>>,
    pub server_name: Option<String>,
    pub trust_system: bool,
    pub stream_window: i32,
    #[cfg(feature = "webtransport")]
    pub allowed_origins: Vec<String>,
    #[cfg(feature = "webtransport")]
    pub webtransport_path: String,
    pub max_ready_peers: i32,
}

#[cfg(feature = "quic")]
impl std::fmt::Debug for QuicOverlay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("QuicOverlay");
        debug.field("cert_pem", &self.cert_pem);
        debug.field("key_pem", &self.key_pem.as_ref().map(|_| "<redacted>"));
        debug.field("trust_pem", &self.trust_pem);
        debug.field("server_name", &self.server_name);
        debug.field("trust_system", &self.trust_system);
        debug.field("stream_window", &self.stream_window);
        #[cfg(feature = "webtransport")]
        debug.field("allowed_origins", &self.allowed_origins);
        #[cfg(feature = "webtransport")]
        debug.field("webtransport_path", &self.webtransport_path);
        debug.field("max_ready_peers", &self.max_ready_peers);
        debug.finish()
    }
}

#[cfg(feature = "quic")]
impl Default for QuicOverlay {
    fn default() -> Self {
        let defaults = omq_tokio::options::QuicOptions::default();
        Self {
            cert_pem: None,
            key_pem: None,
            trust_pem: None,
            server_name: None,
            trust_system: true,
            stream_window: i32::try_from(defaults.stream_window).unwrap_or(i32::MAX),
            #[cfg(feature = "webtransport")]
            allowed_origins: Vec::new(),
            #[cfg(feature = "webtransport")]
            webtransport_path: defaults.webtransport_path,
            max_ready_peers: i32::try_from(defaults.max_ready_peers).unwrap_or(i32::MAX),
        }
    }
}

#[cfg(feature = "quic")]
impl QuicOverlay {
    fn options(&self) -> omq_tokio::options::QuicOptions {
        omq_tokio::options::QuicOptions {
            server_cert_pem: self.cert_pem.clone(),
            server_key_pem: self.key_pem.clone(),
            trust_pem: self.trust_pem.clone(),
            trust_system: self.trust_system,
            server_name: self.server_name.clone(),
            stream_window: u32::try_from(self.stream_window).expect("validated window"),
            max_ready_peers: usize::try_from(self.max_ready_peers).expect("validated limit"),
            #[cfg(feature = "webtransport")]
            allowed_origins: self.allowed_origins.clone(),
            #[cfg(feature = "webtransport")]
            webtransport_path: self.webtransport_path.clone(),
            ..omq_tokio::options::QuicOptions::default()
        }
    }
}

impl Default for SocketOverlay {
    fn default() -> Self {
        Self {
            send_hwm: None,
            recv_hwm: None,
            linger: LingerSetting::Unset,
            identity: Bytes::new(),
            router_mandatory: false,
            reconnect_ivl: Some(Duration::from_millis(100)),
            reconnect_ivl_max: None,
            heartbeat_ivl: None,
            heartbeat_ttl: None,
            heartbeat_timeout: None,
            handshake_ivl: None,
            max_message_size: None,
            arena_threshold: None,
            conflate: false,
            tcp_keepalive: 0,
            tcp_keepalive_cnt: None,
            tcp_keepalive_idle: None,
            tcp_keepalive_intvl: None,
            tcp_maxrt: 0,
            mechanism: MechanismOverlay::Null,
            zap_domain: String::new(),
            zap_enforce_domain: false,
            sndbuf: None,
            rcvbuf: None,
            xpub_verbose: false,
            on_mute: OnMute::Block,
            compression_level: None,
            compression_dict: None,
            compression_auto_train: false,
            workload_profile: None,
            ipv6: false,
            backlog: 0,
            immediate: false,
            connect_timeout: 0,
            probe_router: false,
            req_correlate: false,
            req_relaxed: false,
            xpub_nodrop: false,
            reconnect_stop: 0,
            wss_key_pem: None,
            wss_cert_pem: None,
            wss_trust_pem: None,
            wss_hostname: None,
            wss_trust_system: true,
            ws_allowed_origins: Vec::new(),
            ws_max_ready_peers: 1024,
            #[cfg(feature = "quic")]
            quic: QuicOverlay::default(),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) enum MechanismOverlay {
    #[default]
    Null,
    PlainServer,
    PlainServerCredentials {
        credentials: Vec<(String, String)>,
    },
    PlainClient {
        username: String,
        password: String,
    },
    CurveServer {
        secret_key: [u8; 32],
    },
    CurveClient {
        public_key: [u8; 32],
        secret_key: [u8; 32],
        server_key: [u8; 32],
    },
}

impl std::fmt::Debug for MechanismOverlay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("Null"),
            Self::PlainServer => f.write_str("PlainServer"),
            Self::PlainServerCredentials { credentials } => f
                .debug_struct("PlainServerCredentials")
                .field("count", &credentials.len())
                .finish(),
            Self::PlainClient { username, .. } => f
                .debug_struct("PlainClient")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::CurveServer { .. } => f
                .debug_struct("CurveServer")
                .field("secret_key", &"<redacted>")
                .finish(),
            Self::CurveClient {
                public_key,
                server_key,
                ..
            } => f
                .debug_struct("CurveClient")
                .field("public_key", public_key)
                .field("secret_key", &"<redacted>")
                .field("server_key", server_key)
                .finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) enum LingerSetting {
    #[default]
    Unset,
    Forever,
    Finite(Duration),
}

impl SocketOverlay {
    pub(crate) fn effective_linger(&self) -> Option<Duration> {
        match self.linger {
            LingerSetting::Unset | LingerSetting::Forever => None,
            LingerSetting::Finite(d) => Some(d),
        }
    }

    fn linger_ms(&self) -> i32 {
        match self.linger {
            LingerSetting::Unset | LingerSetting::Forever => -1,
            LingerSetting::Finite(d) => d.as_millis() as i32,
        }
    }

    fn keepalive(&self) -> KeepAlive {
        match self.tcp_keepalive {
            1 => KeepAlive::Enabled {
                idle: self.tcp_keepalive_idle.unwrap_or(Duration::from_mins(1)),
                intvl: self.tcp_keepalive_intvl.unwrap_or(Duration::from_secs(10)),
                cnt: self.tcp_keepalive_cnt.unwrap_or(6),
            },
            0 => KeepAlive::Disabled,
            _ => KeepAlive::Default,
        }
    }

    fn tcp_user_timeout(&self) -> Option<Duration> {
        u64::try_from(self.tcp_maxrt)
            .ok()
            .filter(|&ms| ms > 0)
            .map(Duration::from_millis)
    }

    pub(crate) fn to_options(&self) -> omq_tokio::Options {
        omq_tokio::Options {
            send_hwm: self.send_hwm.unwrap_or(DEFAULT_HWM as u32),
            recv_hwm: self.recv_hwm.unwrap_or(DEFAULT_HWM as u32),
            linger: self.effective_linger(),
            identity: self.identity.clone(),
            max_message_size: self.max_message_size,
            arena_threshold: self.arena_threshold,
            router_mandatory: self.router_mandatory,
            heartbeat_interval: self.heartbeat_ivl,
            heartbeat_ttl: self.heartbeat_ttl,
            heartbeat_timeout: self.heartbeat_timeout,
            handshake_timeout: self.handshake_timeout(),
            conflate: self.conflate,
            tcp_keepalive: self.keepalive(),
            tcp_user_timeout: self.tcp_user_timeout(),
            reconnect: self.reconnect_policy(),
            send_buffer_size: self.sndbuf,
            recv_buffer_size: self.rcvbuf,
            mechanism: self.mechanism_setup(),
            on_mute: self.on_mute,
            compression_level: self.compression_level,
            compression_dict: self.compression_dict.clone(),
            compression_auto_train: self.compression_auto_train,
            workload_profile: self.workload_profile,
            xpub_nodrop: self.xpub_nodrop,
            reconnect_stop_conn_refused: (self.reconnect_stop & 1) != 0,
            ws: self.ws_options(),
            wss_tls: self.wss_tls(),
            #[cfg(feature = "quic")]
            quic: self.quic.options(),
            ..Default::default()
        }
    }

    fn reconnect_policy(&self) -> ReconnectPolicy {
        match (self.reconnect_ivl, self.reconnect_ivl_max) {
            (None, _) => ReconnectPolicy::Disabled,
            (Some(min), None) => ReconnectPolicy::Fixed(min),
            (Some(min), Some(max)) => ReconnectPolicy::Exponential { min, max },
        }
    }

    /// A positive `ZMQ_CONNECT_TIMEOUT` also caps the handshake; the shorter limit wins.
    fn handshake_timeout(&self) -> Option<Duration> {
        match (self.handshake_ivl, self.connect_timeout) {
            (Some(h), t) if t > 0 => Some(h.min(Duration::from_millis(t as u64))),
            (None, t) if t > 0 => Some(Duration::from_millis(t as u64)),
            (None, _) => Some(Duration::from_millis(DEFAULT_HANDSHAKE_IVL_MS as u64)),
            (h, _) => h,
        }
    }

    fn mechanism_setup(&self) -> MechanismSetup {
        match &self.mechanism {
            MechanismOverlay::Null => MechanismSetup::Null,
            MechanismOverlay::PlainServer => MechanismSetup::PlainServer {
                // Replaced with the context's ZAP authenticator during socket
                // materialization. Keep detached overlays fail-closed.
                authenticator: omq_tokio::Authenticator::new(|_| false),
            },
            MechanismOverlay::PlainServerCredentials { credentials } => {
                MechanismSetup::PlainServer {
                    authenticator: omq_tokio::Authenticator::plain_credentials(credentials.clone()),
                }
            }
            MechanismOverlay::PlainClient { username, password } => MechanismSetup::PlainClient {
                username: username.clone(),
                password: password.clone(),
            },
            MechanismOverlay::CurveServer { secret_key } => curve_server_setup(secret_key),
            MechanismOverlay::CurveClient {
                public_key,
                secret_key,
                server_key,
            } => MechanismSetup::CurveClient {
                our_keypair: omq_tokio::CurveKeypair {
                    secret: omq_tokio::CurveSecretKey::from_bytes(*secret_key),
                    public: omq_tokio::CurvePublicKey::from_bytes(*public_key),
                },
                server_public: omq_tokio::CurvePublicKey::from_bytes(*server_key),
            },
        }
    }

    fn ws_options(&self) -> omq_tokio::options::WsOptions {
        omq_tokio::options::WsOptions {
            allowed_origins: self.ws_allowed_origins.clone(),
            max_ready_peers: usize::try_from(self.ws_max_ready_peers)
                .expect("validated WS peer limit"),
        }
    }

    fn wss_tls(&self) -> omq_tokio::options::WssTls {
        omq_tokio::options::WssTls {
            server_cert_pem: self.wss_cert_pem.clone(),
            server_key_pem: self.wss_key_pem.as_ref().map(|key| key.0.clone()),
            trust_pem: self.wss_trust_pem.clone(),
            hostname: self.wss_hostname.clone(),
            trust_system: self.wss_trust_system,
            accept_invalid_certs: false,
        }
    }
}

/// libzmq stores only the CURVE server secret key; derive the public half from it.
fn curve_server_setup(secret_key: &[u8; 32]) -> MechanismSetup {
    let sec = omq_tokio::CurveSecretKey::from_bytes(*secret_key);
    let pubk = sec.derive_public();

    MechanismSetup::CurveServer {
        our_keypair: omq_tokio::CurveKeypair {
            secret: sec,
            public: pubk,
        },
        options: omq_tokio::CurveServerOptions::default(),
    }
}

#[cfg(test)]
mod tests;
