//! Socket options: typed builder.
//!
//! Defaults differ from libzmq in two places: per-socket HWM semantics
//! and conflate restricted to `FanOut` patterns.

use std::time::Duration;

use bytes::Bytes;

use crate::proto::mechanism::MechanismSetup;
#[cfg(any(feature = "curve", feature = "blake3zmq"))]
use crate::proto::mechanism::{Authenticator, MechanismPeerInfo};
#[cfg(feature = "curve")]
use crate::proto::mechanism::{CurveKeypair, CurvePublicKey, CurveSecretKey};
#[cfg(feature = "blake3zmq")]
use crate::proto::mechanism::{Blake3ZmqKeypair, Blake3ZmqPublicKey};
/// Upper bound for `Options::compression_dict`. Matches the smaller
/// of the two dict-size limits across the supported compression
/// transports - the lz4 RFC's 64 KiB minus 4 (sentinel + ZDICT
/// magic). Inlined as a const so the `compression_dict` setter
/// works regardless of which compression features are enabled.
const COMPRESSION_DICT_MAX: usize = 64 * 1024 - 4;

/// Per-socket configuration.
#[derive(Clone, Debug)]
pub struct Options {
    /// Send-side high-water mark, total for the socket. `None` = unbounded.
    pub send_hwm: Option<u32>,

    /// Receive-side high-water mark, total for the socket. `None` = unbounded.
    pub recv_hwm: Option<u32>,

    /// Time to wait on close for the send queue to drain.
    /// `None` = wait forever. `Some(Duration::ZERO)` = drop immediately.
    pub linger: Option<Duration>,

    /// Identity used for ROUTER / DEALER / SERVER / PEER routing. Empty = auto.
    pub identity: Bytes,

    /// Reconnection policy after a lost connection.
    pub reconnect: ReconnectPolicy,

    /// ZMTP PING interval. `None` = heartbeats disabled.
    pub heartbeat_interval: Option<Duration>,

    /// TTL announced in PING (peer's how-long-to-wait hint). `None` = omit.
    pub heartbeat_ttl: Option<Duration>,

    /// Close the connection if no traffic received within this window.
    /// Defaults to `heartbeat_interval` when unset.
    pub heartbeat_timeout: Option<Duration>,

    /// Max time allowed to complete the ZMTP handshake.
    pub handshake_timeout: Option<Duration>,

    /// Reject incoming messages larger than this. `None` = no limit.
    pub max_message_size: Option<usize>,

    /// Conflate: keep only the latest message per subscriber. Applies to
    /// `FanOut` patterns only (PUB/XPUB/RADIO). Ignored elsewhere.
    pub conflate: bool,

    /// ROUTER: fail `send` with `Error::Unroutable` for unknown identities.
    pub router_mandatory: bool,

    /// Behaviour when the socket's send HWM is reached.
    pub on_mute: OnMute,

    /// TCP keepalive policy. Applied to every accepted / dialed TCP
    /// stream after connect. Ignored on non-TCP transports
    /// (`inproc://`, `ipc://`, `udp://`).
    pub tcp_keepalive: KeepAlive,

    /// Active security mechanism. Defaults to `Null` (no encryption).
    pub mechanism: MechanismConfig,

    /// Outbound compression dictionary. Used by `lz4+tcp://` (and, when it
    /// lands, `zstd+tcp://`); ignored on plain transports. The dict is
    /// shipped to the peer once per connection; subsequent parts are
    /// compressed against it. Must be 1..=8192 bytes.
    pub compression_dict: Option<Bytes>,

    /// Zstd auto-trained dictionaries (RFC §6.5). Defaults to **on**
    /// - when neither `compression_dict` nor any other dict source
    ///   is configured on a `zstd+tcp://` connection, the encoder
    ///   samples the first 1000 outbound messages or 100 KiB total
    ///   plaintext (whichever fires first), trains an 8 KiB dict, and
    ///   ships it. After that the per-frame compression threshold
    ///   drops from 512 B to 64 B and small messages start riding the
    ///   dict. Setting `compression_dict` overrides - auto-train is
    ///   silently disabled when a static dict is supplied.
    ///   Ignored by `lz4+tcp://` (LZ4 has no standard trainer).
    ///   Set to `false` to suppress training (e.g. tests that need a
    ///   deterministic wire shape).
    pub compression_auto_train: bool,
}

/// Security-mechanism configuration. NULL is the default; CURVE is
/// available behind the default-on `curve` feature; BLAKE3ZMQ behind
/// the default-off `blake3zmq` feature.
#[derive(Clone, Debug, Default)]
pub enum MechanismConfig {
    /// NULL: no encryption, no peer authentication.
    #[default]
    Null,
    /// CURVE server side: this socket accepts incoming CURVE clients
    /// authenticated against `our_keypair.public`. `authenticator`
    /// (if set) is invoked after vouch verification with the peer's
    /// long-term public key.
    #[cfg(feature = "curve")]
    CurveServer {
        our_keypair: CurveKeypair,
        authenticator: Option<Authenticator>,
    },
    /// CURVE client side: this socket connects to a server identified by
    /// `server_public`, authenticating with `our_keypair`.
    #[cfg(feature = "curve")]
    CurveClient {
        our_keypair: CurveKeypair,
        server_public: CurvePublicKey,
    },
    /// BLAKE3ZMQ server side. Non-standard, omq-to-omq only. Available
    /// behind the `blake3zmq` feature. The cookie keyring is shared
    /// across every server-side connection on this Socket so its
    /// 30-second rotation timeline (RFC §9.2) doesn't reset per
    /// connection.
    #[cfg(feature = "blake3zmq")]
    Blake3ZmqServer {
        our_keypair: Blake3ZmqKeypair,
        cookie_keyring: std::sync::Arc<crate::proto::mechanism::blake3zmq::CookieKeyring>,
        authenticator: Option<Authenticator>,
    },
    /// BLAKE3ZMQ client side. Available behind the `blake3zmq` feature.
    #[cfg(feature = "blake3zmq")]
    Blake3ZmqClient {
        our_keypair: Blake3ZmqKeypair,
        server_public: Blake3ZmqPublicKey,
    },
}

impl MechanismConfig {
    /// Wire-level mechanism name advertised in the greeting.
    pub fn wire_name(&self) -> &'static [u8] {
        match self {
            Self::Null => b"NULL",
            #[cfg(feature = "curve")]
            Self::CurveServer { .. } | Self::CurveClient { .. } => b"CURVE",
            #[cfg(feature = "blake3zmq")]
            Self::Blake3ZmqServer { .. } | Self::Blake3ZmqClient { .. } => b"BLAKE3",
        }
    }

    /// Access the BLAKE3ZMQ server's cookie keyring so callers can
    /// configure its rotation interval or share it across multiple
    /// Sockets. `None` for non-BLAKE3ZMQ-server configs.
    #[cfg(feature = "blake3zmq")]
    pub fn blake3zmq_cookie_keyring(
        &self,
    ) -> Option<&std::sync::Arc<crate::proto::mechanism::blake3zmq::CookieKeyring>> {
        match self {
            Self::Blake3ZmqServer { cookie_keyring, .. } => Some(cookie_keyring),
            _ => None,
        }
    }

    #[cfg(feature = "curve")]
    pub fn is_curve(&self) -> bool {
        matches!(self, Self::CurveServer { .. } | Self::CurveClient { .. })
    }

    #[cfg(feature = "curve")]
    pub fn curve_secret(&self) -> Option<&CurveSecretKey> {
        match self {
            Self::CurveServer { our_keypair, .. } | Self::CurveClient { our_keypair, .. } => {
                Some(&our_keypair.secret)
            }
            Self::Null => None,
            #[cfg(feature = "blake3zmq")]
            Self::Blake3ZmqServer { .. } | Self::Blake3ZmqClient { .. } => None,
        }
    }

    /// Translate to the codec-layer [`MechanismSetup`] consumed by
    /// `Connection::new`.
    pub fn to_setup(&self) -> MechanismSetup {
        match self {
            Self::Null => MechanismSetup::Null,
            #[cfg(feature = "curve")]
            Self::CurveServer { our_keypair, authenticator } => MechanismSetup::CurveServer {
                keypair: our_keypair.clone(),
                authenticator: authenticator.clone(),
            },
            #[cfg(feature = "curve")]
            Self::CurveClient { our_keypair, server_public } => MechanismSetup::CurveClient {
                keypair: our_keypair.clone(),
                server_public: *server_public,
            },
            #[cfg(feature = "blake3zmq")]
            Self::Blake3ZmqServer { our_keypair, cookie_keyring, authenticator } => {
                MechanismSetup::Blake3ZmqServer {
                    keypair: our_keypair.clone(),
                    cookie_keyring: cookie_keyring.clone(),
                    authenticator: authenticator.clone(),
                }
            }
            #[cfg(feature = "blake3zmq")]
            Self::Blake3ZmqClient { our_keypair, server_public } => {
                MechanismSetup::Blake3ZmqClient {
                    keypair: our_keypair.clone(),
                    server_public: *server_public,
                }
            }
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            send_hwm: Some(1000),
            recv_hwm: Some(1000),
            linger: Some(Duration::ZERO),
            identity: Bytes::new(),
            reconnect: ReconnectPolicy::default(),
            heartbeat_interval: None,
            heartbeat_ttl: None,
            heartbeat_timeout: None,
            handshake_timeout: Some(Duration::from_secs(30)),
            max_message_size: None,
            conflate: false,
            router_mandatory: false,
            on_mute: OnMute::Block,
            tcp_keepalive: KeepAlive::default(),
            mechanism: MechanismConfig::Null,
            compression_dict: None,
            compression_auto_train: true,
        }
    }
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn send_hwm(mut self, hwm: u32) -> Self {
        self.send_hwm = Some(hwm);
        self
    }

    #[must_use]
    pub fn recv_hwm(mut self, hwm: u32) -> Self {
        self.recv_hwm = Some(hwm);
        self
    }

    #[must_use]
    pub fn unbounded_send(mut self) -> Self {
        self.send_hwm = None;
        self
    }

    #[must_use]
    pub fn unbounded_recv(mut self) -> Self {
        self.recv_hwm = None;
        self
    }

    #[must_use]
    pub fn linger(mut self, d: Duration) -> Self {
        self.linger = Some(d);
        self
    }

    #[must_use]
    pub fn linger_forever(mut self) -> Self {
        self.linger = None;
        self
    }

    #[must_use]
    pub fn identity(mut self, id: impl Into<Bytes>) -> Self {
        self.identity = id.into();
        self
    }

    #[must_use]
    pub fn reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    #[must_use]
    pub fn heartbeat_interval(mut self, d: Duration) -> Self {
        self.heartbeat_interval = Some(d);
        self
    }

    #[must_use]
    pub fn heartbeat_ttl(mut self, d: Duration) -> Self {
        self.heartbeat_ttl = Some(d);
        self
    }

    #[must_use]
    pub fn heartbeat_timeout(mut self, d: Duration) -> Self {
        self.heartbeat_timeout = Some(d);
        self
    }

    #[must_use]
    pub fn handshake_timeout(mut self, d: Duration) -> Self {
        self.handshake_timeout = Some(d);
        self
    }

    #[must_use]
    pub fn max_message_size(mut self, n: usize) -> Self {
        self.max_message_size = Some(n);
        self
    }

    #[must_use]
    pub fn conflate(mut self, c: bool) -> Self {
        self.conflate = c;
        self
    }

    #[must_use]
    pub fn router_mandatory(mut self, m: bool) -> Self {
        self.router_mandatory = m;
        self
    }

    #[must_use]
    pub fn on_mute(mut self, m: OnMute) -> Self {
        self.on_mute = m;
        self
    }

    #[must_use]
    pub fn tcp_keepalive(mut self, k: KeepAlive) -> Self {
        self.tcp_keepalive = k;
        self
    }

    /// Configure this socket as a CURVE server with the given long-term
    /// keypair. Incoming clients must present the matching server public
    /// key during their handshake. Use [`Self::curve_authenticator`] to
    /// add a per-client admission callback.
    #[cfg(feature = "curve")]
    #[must_use]
    pub fn curve_server(mut self, our_keypair: CurveKeypair) -> Self {
        self.mechanism = MechanismConfig::CurveServer {
            our_keypair,
            authenticator: None,
        };
        self
    }


    /// Configure this socket as a CURVE client targeting `server_public`.
    #[cfg(feature = "curve")]
    #[must_use]
    pub fn curve_client(
        mut self,
        our_keypair: CurveKeypair,
        server_public: CurvePublicKey,
    ) -> Self {
        self.mechanism = MechanismConfig::CurveClient {
            our_keypair,
            server_public,
        };
        self
    }

    /// Configure this socket as a BLAKE3ZMQ server. Non-standard,
    /// omq-to-omq only - peers must also be `blake3zmq`-built.
    /// A fresh cookie keyring with the default rotation interval
    /// (~30 s) is created. Reach in via
    /// [`MechanismConfig::blake3zmq_cookie_keyring`] to configure or
    /// share it. Use [`Self::blake3zmq_authenticator`] to add a
    /// per-client admission callback.
    #[cfg(feature = "blake3zmq")]
    #[must_use]
    pub fn blake3zmq_server(mut self, our_keypair: Blake3ZmqKeypair) -> Self {
        self.mechanism = MechanismConfig::Blake3ZmqServer {
            our_keypair,
            cookie_keyring: std::sync::Arc::new(
                crate::proto::mechanism::blake3zmq::CookieKeyring::new(),
            ),
            authenticator: None,
        };
        self
    }

    /// Install a server-side authenticator. Called once per handshake
    /// after the underlying mechanism has cryptographically verified
    /// the peer (CURVE: vouch decrypt; BLAKE3ZMQ: vouch decrypt).
    /// The callback receives the peer's long-term public key plus a
    /// tag identifying which mechanism produced it. Return `false` to
    /// reject the client; the handshake aborts.
    ///
    /// Works for both CURVE and BLAKE3ZMQ server configurations.
    /// Panics if the current mechanism is not a server configuration
    /// of an encrypting mechanism.
    #[cfg(any(feature = "curve", feature = "blake3zmq"))]
    #[must_use]
    pub fn authenticator<F>(mut self, f: F) -> Self
    where
        F: Fn(&MechanismPeerInfo) -> bool + Send + Sync + 'static,
    {
        let auth = Authenticator::new(f);
        match &mut self.mechanism {
            #[cfg(feature = "curve")]
            MechanismConfig::CurveServer { authenticator, .. } => {
                *authenticator = Some(auth);
            }
            #[cfg(feature = "blake3zmq")]
            MechanismConfig::Blake3ZmqServer { authenticator, .. } => {
                *authenticator = Some(auth);
            }
            _ => panic!("authenticator requires a server-side encrypting mechanism"),
        }
        self
    }

    /// Configure this socket as a BLAKE3ZMQ client targeting
    /// `server_public`. Non-standard, omq-to-omq only.
    #[cfg(feature = "blake3zmq")]
    #[must_use]
    pub fn blake3zmq_client(
        mut self,
        our_keypair: Blake3ZmqKeypair,
        server_public: Blake3ZmqPublicKey,
    ) -> Self {
        self.mechanism = MechanismConfig::Blake3ZmqClient {
            our_keypair,
            server_public,
        };
        self
    }

    /// Set the outbound compression dictionary. Used by compression
    /// transports (`lz4+tcp://`, future `zstd+tcp://`). Panics if the dict
    /// is empty or larger than 8192 bytes (`omq-lz4` RFC §6.2).
    #[must_use]
    pub fn compression_dict(mut self, dict: impl Into<Bytes>) -> Self {
        let dict = dict.into();
        assert!(
            !dict.is_empty() && dict.len() <= COMPRESSION_DICT_MAX,
            "compression dict must be 1..={COMPRESSION_DICT_MAX} bytes, got {}",
            dict.len()
        );
        self.compression_dict = Some(dict);
        self
    }

    /// Toggle Zstd auto-trained dictionaries (`zstd+tcp://` only).
    /// On by default; pass `false` to suppress training. See
    /// [`Options::compression_auto_train`] for semantics.
    #[must_use]
    pub fn compression_auto_train(mut self, enabled: bool) -> Self {
        self.compression_auto_train = enabled;
        self
    }
}

impl From<Bytes> for Options {
    /// Convenience: build options with a given identity, defaults for the rest.
    fn from(identity: Bytes) -> Self {
        Self::default().identity(identity)
    }
}

/// Reconnection policy applied after a lost connection on `connect()` sockets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconnectPolicy {
    /// No reconnect; the connection is dropped permanently on failure.
    Disabled,
    /// Retry at a constant interval.
    Fixed(Duration),
    /// Exponential backoff between `min` and `max`, doubling on each retry.
    Exponential { min: Duration, max: Duration },
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        // Constant 100ms matches libzmq's `ZMQ_RECONNECT_IVL` default.
        // Users who want exponential backoff opt in via
        // `Options::reconnect(ReconnectPolicy::Exponential { .. })`.
        Self::Fixed(Duration::from_millis(100))
    }
}

/// What to do when the send HWM is reached and a new message arrives.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnMute {
    /// Block the sender until room is available.
    #[default]
    Block,
    /// Drop the incoming message silently.
    DropNewest,
    /// Drop the oldest queued message, then enqueue the new one.
    DropOldest,
}

/// TCP keepalive policy. `Default` leaves the OS defaults alone (matches
/// libzmq's `ZMQ_TCP_KEEPALIVE = -1`); `Disabled` clears `SO_KEEPALIVE`;
/// `Enabled` sets `SO_KEEPALIVE` and pins the three timing knobs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KeepAlive {
    /// OS defaults; nothing applied to the socket.
    #[default]
    Default,
    /// Explicitly disable `SO_KEEPALIVE`.
    Disabled,
    /// Enable `SO_KEEPALIVE` and set the timing triplet.
    Enabled {
        /// Idle time before the first probe is sent (`TCP_KEEPIDLE`).
        idle: Duration,
        /// Interval between probes (`TCP_KEEPINTVL`).
        intvl: Duration,
        /// Failed probes before declaring the connection dead (`TCP_KEEPCNT`).
        cnt: u32,
    },
}

impl KeepAlive {
    /// Apply this keepalive policy to a connected TCP socket. Used by
    /// both `omq-tokio` and `omq-compio` after `connect`/`accept` so the
    /// option is in effect for the connection's lifetime.
    pub fn apply<S: std::os::fd::AsFd>(&self, sock: &S) -> std::io::Result<()> {
        let sref = socket2::SockRef::from(sock);
        match self {
            KeepAlive::Default => Ok(()),
            KeepAlive::Disabled => sref.set_keepalive(false),
            KeepAlive::Enabled { idle, intvl, cnt } => {
                let ka = socket2::TcpKeepalive::new()
                    .with_time(*idle)
                    .with_interval(*intvl)
                    .with_retries(*cnt);
                sref.set_tcp_keepalive(&ka)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_per_socket_hwm_block() {
        let o = Options::default();
        assert_eq!(o.send_hwm, Some(1000));
        assert_eq!(o.recv_hwm, Some(1000));
        assert_eq!(o.linger, Some(Duration::ZERO));
        assert_eq!(o.handshake_timeout, Some(Duration::from_secs(30)));
        assert_eq!(o.heartbeat_interval, None);
        assert_eq!(o.max_message_size, None);
        assert_eq!(o.tcp_keepalive, KeepAlive::Default);
        assert!(!o.conflate);
        assert!(!o.router_mandatory);
        assert_eq!(o.on_mute, OnMute::Block);
    }

    #[test]
    fn tcp_keepalive_builder() {
        let o = Options::new().tcp_keepalive(KeepAlive::Disabled);
        assert_eq!(o.tcp_keepalive, KeepAlive::Disabled);
        let o = Options::new().tcp_keepalive(KeepAlive::Enabled {
            idle: Duration::from_secs(30),
            intvl: Duration::from_secs(5),
            cnt: 3,
        });
        match o.tcp_keepalive {
            KeepAlive::Enabled { idle, intvl, cnt } => {
                assert_eq!(idle, Duration::from_secs(30));
                assert_eq!(intvl, Duration::from_secs(5));
                assert_eq!(cnt, 3);
            }
            _ => panic!("expected Enabled"),
        }
    }

    #[test]
    fn reconnect_default_fixed_100ms() {
        assert_eq!(
            ReconnectPolicy::default(),
            ReconnectPolicy::Fixed(Duration::from_millis(100))
        );
    }

    #[test]
    fn builder_chaining() {
        let o = Options::new()
            .send_hwm(42)
            .recv_hwm(99)
            .linger(Duration::from_secs(5))
            .identity("router-id")
            .heartbeat_interval(Duration::from_secs(1))
            .max_message_size(1024)
            .conflate(true)
            .router_mandatory(true)
            .on_mute(OnMute::DropNewest);
        assert_eq!(o.send_hwm, Some(42));
        assert_eq!(o.recv_hwm, Some(99));
        assert_eq!(o.linger, Some(Duration::from_secs(5)));
        assert_eq!(o.identity, &b"router-id"[..]);
        assert_eq!(o.heartbeat_interval, Some(Duration::from_secs(1)));
        assert_eq!(o.max_message_size, Some(1024));
        assert!(o.conflate);
        assert!(o.router_mandatory);
        assert_eq!(o.on_mute, OnMute::DropNewest);
    }

    #[test]
    fn unbounded_queues() {
        let o = Options::new().unbounded_send().unbounded_recv();
        assert_eq!(o.send_hwm, None);
        assert_eq!(o.recv_hwm, None);
    }

    #[test]
    fn linger_forever() {
        let o = Options::new().linger_forever();
        assert_eq!(o.linger, None);
    }

    #[test]
    fn from_bytes_sets_identity() {
        let o: Options = Bytes::from_static(b"id").into();
        assert_eq!(o.identity, &b"id"[..]);
        assert_eq!(o.send_hwm, Some(1000));
    }
}
