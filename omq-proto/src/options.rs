//! Socket options: typed builder.
//!
//! Defaults differ from libzmq in a few places. Native OMQ linger defaults to
//! zero, while libzmq `ZMQ_LINGER` defaults to forever. Native OMQ
//! `send_hwm`/`recv_hwm` are message-count caps, not byte caps. Native OMQ
//! applies `send_hwm` per outbound pipe; it is not a single socket-wide byte
//! or message budget.

use std::time::Duration;

use bytes::Bytes;

use crate::proto::mechanism::MechanismSetup;
#[cfg(feature = "plain")]
use crate::proto::mechanism::{Authenticator, MechanismPeerInfo};
#[cfg(feature = "curve")]
use crate::proto::mechanism::{CurveKeypair, CurvePublicKey, CurveServerOptions};
use crate::socket_ref::SocketRef;
/// Upper bound for `Options::compression_dict`. Compression transports cap
/// dictionaries at 8 KiB. Inlined as a const so the `compression_dict`
/// setter works regardless of which compression features are enabled.
const COMPRESSION_DICT_MAX: usize = 8 * 1024;

/// Default cap for byte-stream peers that are accepted but have not
/// completed the ZMTP handshake.
pub const DEFAULT_MAX_PENDING_HANDSHAKES: usize = 128;

/// Default per-`FrameBuffer` arena threshold.
pub const DEFAULT_ARENA_THRESHOLD: usize = crate::frame_buffer::ARENA_THRESHOLD;

/// Token-bucket message rate limit.
///
/// `messages_per_second` controls refill speed. `burst` is the maximum token
/// capacity. Exceeding either receive limit closes the offending connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageRateLimit {
    /// Sustained complete messages allowed per second.
    pub messages_per_second: u32,
    /// Maximum immediate message burst.
    pub burst: u32,
}

impl MessageRateLimit {
    /// Create a message rate limit.
    #[must_use]
    pub const fn new(messages_per_second: u32, burst: u32) -> Self {
        Self {
            messages_per_second,
            burst,
        }
    }
}

/// Per-socket configuration.
///
/// # Compatibility warnings
///
/// Native OMQ does not copy every libzmq socket default or HWM detail:
///
/// - `linger` defaults to zero. libzmq `ZMQ_LINGER` defaults to forever.
/// - `send_hwm` and `recv_hwm` count complete messages, not bytes.
/// - Native `send_hwm` is not an exact total queued-message cap. Connect-side
///   pre-ready pipes, per-peer pipes, fan-out lane rings, and transmit slots
///   are separate buffers.
/// - Native round-robin sends (`PUSH`, `DEALER`, `REQ`, `CLIENT`, `SCATTER`)
///   with a bound endpoint and no ready pipe mute like libzmq: blocking
///   `send()` waits and `try_send()` returns `Full`.
/// - The same socket types with a `connect()` endpoint allocate a pre-ready
///   pipe at `connect()` time. Sends may queue there before the peer reaches
///   READY. Native OMQ has no `ZMQ_IMMEDIATE` option to disable that queue.
const ZSTD_LEVEL_MIN: i32 = -8;
const ZSTD_LEVEL_MAX: i32 = 4;

// Compression fields (compression_dict through compression_offload_threshold)
// could be grouped into a sub-struct, but the public API change would touch
// every backend file that accesses them.
#[derive(Clone, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct Options {
    /// Scheduling profile for this socket.
    ///
    /// `None` selects the socket-type default: REQ and REP use the
    /// latency profile; all other socket types use the throughput profile.
    /// For ping-pong SERVER/CLIENT or ROUTER/DEALER workloads, set this
    /// explicitly on both endpoints.
    pub workload_profile: Option<WorkloadProfile>,

    /// Send-side high-water mark as a message count.
    ///
    /// This is not a byte cap. One 16-byte message and one 16-MiB message
    /// each consume one HWM slot. Native OMQ applies this per outbound pipe:
    /// connect-side pre-ready pipes, materialized peer pipes, and fan-out lane
    /// rings each have their own HWM. Effective socket-wide queued capacity
    /// can therefore exceed this value when multiple pipes or transmit slots
    /// exist. `omq-libzmq` exposes this as `ZMQ_SNDHWM`.
    pub send_hwm: u32,

    /// Receive-side high-water mark as a message count.
    pub recv_hwm: u32,

    /// Per-connection receive token bucket. `None` disables it.
    ///
    /// The tokio byte-stream backend counts complete application messages
    /// after decoding. Exceeding the burst closes that peer connection. Inproc
    /// and UDP transports do not use this limit.
    pub recv_rate_limit: Option<MessageRateLimit>,

    /// Aggregate receive token bucket per remote IP. `None` disables it.
    ///
    /// Buckets are shared by every TCP/WS connection owned by this socket,
    /// including connections on different endpoints. IPC, inproc, and UDP
    /// peers have no remote TCP/WS IP and are not charged against this limit.
    pub recv_ip_rate_limit: Option<MessageRateLimit>,

    /// Time to wait on close for the send queue to drain.
    ///
    /// Native OMQ defaults to `Some(Duration::ZERO)`: close/drop discard
    /// unsent queued messages immediately. This intentionally differs from
    /// libzmq, where `ZMQ_LINGER` defaults to `-1` (forever). `omq-libzmq`
    /// maps its C default back to forever for compatibility.
    ///
    /// `None` waits forever. `Some(Duration::ZERO)` drops immediately.
    /// Finite non-zero values keep bind/connect endpoints alive until queued
    /// sends drain or the deadline expires.
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
    ///
    /// Encrypted mechanisms require a timeout. Longer values give slow peers
    /// more time to finish authentication, but also let stalled or malicious
    /// peers hold pending-handshake slots longer.
    pub handshake_timeout: Option<Duration>,

    /// Maximum byte-stream peers allowed to sit in the ZMTP handshake state
    /// at once. The tokio backend applies this before spawning a peer driver
    /// for newly accepted TCP/IPC connections.
    ///
    /// Lower values reduce memory/task pressure from unauthenticated peers,
    /// but can reject legitimate connection bursts while the cap is full.
    /// Higher values admit larger bursts, at the cost of more pre-auth
    /// resource use. Completed handshakes leave this pool immediately; timed
    /// out or failed handshakes release their slot when the peer is closed.
    pub max_pending_handshakes: usize,

    /// Reject incoming messages larger than this. Accounting includes payload
    /// bytes plus one internal payload slot per part. `None` = no limit.
    pub max_message_size: Option<usize>,

    /// Conflate: keep only the latest message per subscriber. Applies to
    /// `FanOut` patterns only (PUB/XPUB/RADIO). Ignored elsewhere.
    pub conflate: bool,

    /// ROUTER: fail `send` with `Error::Unroutable` for unknown identities.
    pub router_mandatory: bool,

    /// Behavior when the socket's send HWM is reached.
    ///
    /// Fan-out sockets (`PUB`, `XPUB`, `RADIO`) are always lossy on mute:
    /// this setting is ignored and they drop newest unless `xpub_nodrop`
    /// is set.
    ///
    /// Native bound no-peer round-robin sends mute immediately. Connected
    /// no-peer round-robin sends queue into their connect-side pre-ready pipe
    /// until that pipe reaches `send_hwm`, then this policy applies.
    pub on_mute: OnMute,

    /// TCP keepalive policy. Applied to every accepted / dialed TCP
    /// stream after connect. Ignored on non-TCP transports
    /// (`inproc://`, `ipc://`, `udp://`).
    pub tcp_keepalive: KeepAlive,

    /// `SO_RCVBUF` size in bytes. Applied to every TCP/IPC stream after
    /// connect/accept. `None` leaves the OS default. Larger values
    /// reduce the number of kernel-to-userspace round-trips for large
    /// messages.
    pub recv_buffer_size: Option<usize>,

    /// `SO_SNDBUF` size in bytes. Applied to every TCP/IPC stream after
    /// connect/accept. `None` leaves the OS default.
    pub send_buffer_size: Option<usize>,

    /// Active security mechanism. Defaults to `Null` (no encryption).
    pub mechanism: MechanismSetup,

    /// Outbound compression dictionary. Used by compression transports;
    /// ignored on plain transports. The dict is shipped to the peer once per
    /// connection; subsequent parts are compressed against it.
    /// Must be 1..=8192 bytes.
    pub compression_dict: Option<Bytes>,

    /// Auto-trained dictionaries. Defaults to off.
    /// When no `compression_dict` is configured on a compression
    /// connection, the encoder feeds outbound message parts to a
    /// dict trainer until it saturates, then trains a dict (capacity controlled by
    /// `compression_dict_capacity`, default 2 KiB) and ships it.
    /// After that the per-part compression threshold drops from
    /// 512 B to 64 B and small messages ride the dict.
    /// Setting `compression_dict` overrides: auto-train is silently
    /// disabled when a static dict is supplied.
    /// Default: `false`. Enable for workloads with small structured
    /// records (JSON, protobuf) where dictionary compression can
    /// achieve 8-24x compression ratios on sub-1 KiB messages.
    pub compression_auto_train: bool,

    /// Minimum payload size (bytes) before compression is attempted.
    /// Messages smaller than this are sent uncompressed regardless of
    /// dict presence. `None` uses the built-in defaults (which vary by
    /// transport and dict presence). Useful on high-bandwidth links
    /// where compressing tiny messages wastes CPU.
    pub compression_threshold: Option<usize>,

    /// Compression level for `zstd+tcp://`. `None` uses the transport
    /// default. Supported zrip levels are -8..=4; level 0 maps to zrip's
    /// library default (currently level 1). Ignored by `lz4+tcp://`.
    pub compression_level: Option<i32>,

    /// Auto-train dict capacity in bytes. Controls the maximum size of
    /// the dictionary produced by auto-training. Default: 2048.
    /// Ignored when `compression_dict` is set.
    pub compression_dict_capacity: Option<usize>,

    /// Maximum dictionary size (bytes) accepted from a peer. Dicts
    /// larger than this are rejected. Default: 8192 for compression transports.
    pub max_recv_dict_size: Option<usize>,

    /// Minimum message size (bytes) before compression is offloaded to
    /// a background thread (tokio backend only). Messages smaller than
    /// this are compressed inline on the driver task. `None` disables
    /// offloading entirely. Default: `Some(8192)`.
    pub compression_offload_threshold: Option<usize>,

    /// Switch the recv path to a sized one-shot read for any inbound
    /// frame whose wire payload is at least this many bytes.
    ///
    /// On `omq-tokio` this threshold triggers a fast path that reads
    /// large payloads into a single pre-sized buffer instead of
    /// accumulating fixed-size reads through the codec. Medium-large
    /// payloads may use bounded pooled buffers; larger payloads use
    /// one-shot owned buffers.
    pub large_message_threshold: Option<usize>,

    /// Payload size at which the encoder switches from contiguous arena
    /// copies to zero-copy gather-write. Messages smaller than this are
    /// appended into a shared arena buffer (one iovec per batch); larger
    /// messages produce per-frame iovecs referencing the original `Bytes`
    /// payload.
    ///
    /// `None` uses the default (`ARENA_THRESHOLD`, 4 KiB). Raise this
    /// when payloads are owned by an external runtime (e.g. Python
    /// refcounted objects) where the gather path's per-chunk refcount
    /// traffic is more expensive than a flat memcpy.
    pub arena_threshold: Option<usize>,

    /// Maximum encoded bytes buffered in a per-peer transmit slot before
    /// `try_encode` returns `Full` and the message falls back to the
    /// actor inbox. `None` uses the default (2 MiB). Larger values
    /// allow more batching at the cost of memory per peer.
    pub transmit_slot_cap: Option<usize>,

    /// `XPUB_NODROP`: when true, PUB/XPUB `try_send` returns `Full`
    /// instead of silently dropping the message when any subscriber's
    /// transmit slot is at capacity.
    pub xpub_nodrop: bool,

    /// Stop reconnecting on `ECONNREFUSED` (`ZMQ_RECONNECT_STOP`).
    pub reconnect_stop_conn_refused: bool,

    /// TLS configuration for `wss://` endpoints. Ignored for non-WSS
    /// transports. Requires the `ws` feature.
    #[cfg(feature = "ws")]
    pub wss_tls: WssTls,
}

/// TLS configuration for WSS endpoints. This covers server certificates
/// and client-side server certificate validation only. Mutual TLS/client
/// certificate authentication is not implemented.
#[cfg(feature = "ws")]
#[derive(Clone, Debug)]
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

/// Backward-compatible alias. [`MechanismSetup`] is the canonical type.
pub type MechanismConfig = MechanismSetup;

impl Default for Options {
    fn default() -> Self {
        Self {
            workload_profile: None,
            send_hwm: 1000,
            recv_hwm: 1000,
            recv_rate_limit: None,
            recv_ip_rate_limit: None,
            linger: Some(Duration::ZERO),
            identity: Bytes::new(),
            reconnect: ReconnectPolicy::default(),
            heartbeat_interval: None,
            heartbeat_ttl: None,
            heartbeat_timeout: None,
            handshake_timeout: Some(Duration::from_secs(30)),
            max_pending_handshakes: DEFAULT_MAX_PENDING_HANDSHAKES,
            max_message_size: None,
            conflate: false,
            router_mandatory: false,
            on_mute: OnMute::Block,
            tcp_keepalive: KeepAlive::default(),
            recv_buffer_size: None,
            send_buffer_size: None,
            mechanism: MechanismSetup::Null,
            compression_dict: None,
            compression_auto_train: false,
            compression_threshold: None,
            compression_level: None,
            compression_dict_capacity: None,
            max_recv_dict_size: None,
            compression_offload_threshold: Some(8192),
            large_message_threshold: Some(128 * 1024),
            arena_threshold: None,
            transmit_slot_cap: None,
            xpub_nodrop: false,
            reconnect_stop_conn_refused: false,
            #[cfg(feature = "ws")]
            wss_tls: WssTls::default(),
        }
    }
}

/// ZMTP PING encodes TTL as tenths of a second in a `u16`.
const MAX_HEARTBEAT_TTL_MS: u128 = 6_553_500;

impl Options {
    /// Create options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Select the scheduling profile used by this socket's I/O driver.
    #[must_use]
    pub fn workload_profile(mut self, profile: WorkloadProfile) -> Self {
        self.workload_profile = Some(profile);
        self
    }

    /// Check ZMTP protocol limits that would cause hard-to-debug wire
    /// failures if violated. Called from `Socket::new` in both backends.
    pub fn validate(&self) -> crate::error::Result<()> {
        let id_len = self.identity.len();
        if id_len > 255 {
            return Err(crate::error::Error::Config(format!(
                "identity length {id_len} exceeds ZMTP limit of 255 bytes"
            )));
        }
        if let Some(ttl) = self.heartbeat_ttl
            && ttl.as_millis() > MAX_HEARTBEAT_TTL_MS
        {
            return Err(crate::error::Error::Config(format!(
                "heartbeat_ttl {ttl:?} exceeds ZMTP maximum of 6553.5s"
            )));
        }
        if self.max_pending_handshakes == 0 {
            return Err(crate::error::Error::Config(
                "max_pending_handshakes must be greater than zero".into(),
            ));
        }
        for (name, limit) in [
            ("recv_rate_limit", self.recv_rate_limit),
            ("recv_ip_rate_limit", self.recv_ip_rate_limit),
        ] {
            if let Some(limit) = limit
                && (limit.messages_per_second == 0 || limit.burst == 0)
            {
                return Err(crate::error::Error::Config(format!(
                    "{name} rate and burst must be greater than zero"
                )));
            }
        }
        if self.handshake_timeout.is_none() && self.mechanism.has_frame_transform() {
            return Err(crate::error::Error::Config(
                "encrypted mechanisms require handshake_timeout".into(),
            ));
        }
        if let Some(ref dict) = self.compression_dict
            && (dict.is_empty() || dict.len() > COMPRESSION_DICT_MAX)
        {
            return Err(crate::error::Error::Config(format!(
                "compression dict must be 1..={COMPRESSION_DICT_MAX} bytes, got {}",
                dict.len()
            )));
        }
        if let Some(level) = self.compression_level
            && !(ZSTD_LEVEL_MIN..=ZSTD_LEVEL_MAX).contains(&level)
        {
            return Err(crate::error::Error::Config(format!(
                "zstd compression level must be {ZSTD_LEVEL_MIN}..={ZSTD_LEVEL_MAX}, got {level}",
            )));
        }
        #[cfg(feature = "plain")]
        if let MechanismSetup::PlainClient {
            ref username,
            ref password,
        } = self.mechanism
        {
            if username.len() > 255 {
                return Err(crate::error::Error::Config(format!(
                    "PLAIN username length {} exceeds 255-byte limit",
                    username.len()
                )));
            }
            if password.len() > 255 {
                return Err(crate::error::Error::Config(format!(
                    "PLAIN password length {} exceeds 255-byte limit",
                    password.len()
                )));
            }
        }
        #[cfg(feature = "curve")]
        if let MechanismSetup::CurveServer { ref options, .. } = self.mechanism
            && options.cookie_lifetime.is_zero()
        {
            return Err(crate::error::Error::Config(
                "CURVE cookie lifetime must be greater than zero".into(),
            ));
        }
        Ok(())
    }

    /// Set send-side HWM as a message count.
    ///
    /// This is not a byte limit. Large messages count the same as small
    /// messages. Native OMQ may hold more than this value across multiple
    /// connect-side pre-ready pipes, per-peer pipes, fan-out lane rings, and
    /// transmit slots.
    #[must_use]
    pub fn send_hwm(mut self, hwm: u32) -> Self {
        self.send_hwm = hwm;
        self
    }

    #[must_use]
    /// Set receive-side HWM as a message count.
    ///
    /// This bounds complete messages queued for application receive. It is
    /// not a byte limit.
    pub fn recv_hwm(mut self, hwm: u32) -> Self {
        self.recv_hwm = hwm;
        self
    }

    /// Set the per-connection receive message rate and burst.
    #[must_use]
    pub fn recv_rate_limit(mut self, messages_per_second: u32, burst: u32) -> Self {
        self.recv_rate_limit = Some(MessageRateLimit::new(messages_per_second, burst));
        self
    }

    /// Set the aggregate receive message rate and burst per remote IP.
    #[must_use]
    pub fn recv_ip_rate_limit(mut self, messages_per_second: u32, burst: u32) -> Self {
        self.recv_ip_rate_limit = Some(MessageRateLimit::new(messages_per_second, burst));
        self
    }

    /// Set close linger to a finite duration.
    ///
    /// `Duration::ZERO` means drop queued outbound messages immediately.
    /// Non-zero values keep endpoints alive so queued sends can drain to
    /// existing or late peers before the deadline.
    #[must_use]
    pub fn linger(mut self, d: Duration) -> Self {
        self.linger = Some(d);
        self
    }

    /// Wait forever for queued outbound messages to drain on close/drop.
    ///
    /// This can wait forever if queued messages have no peer and no peer ever
    /// arrives. Use finite linger for services that need bounded shutdown.
    #[must_use]
    pub fn linger_forever(mut self) -> Self {
        self.linger = None;
        self
    }

    #[must_use]
    /// Set the ZMTP identity advertised during handshake.
    pub fn identity(mut self, id: impl Into<Bytes>) -> Self {
        self.identity = id.into();
        self
    }

    #[must_use]
    /// Set reconnect behavior for connect-side peers.
    pub fn reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    #[must_use]
    /// Stop reconnecting when the remote side refuses the connection.
    pub fn reconnect_stop_conn_refused(mut self, stop: bool) -> Self {
        self.reconnect_stop_conn_refused = stop;
        self
    }

    #[must_use]
    /// Set interval between heartbeat PING commands.
    pub fn heartbeat_interval(mut self, d: Duration) -> Self {
        self.heartbeat_interval = Some(d);
        self
    }

    #[must_use]
    /// Set heartbeat TTL advertised to peers.
    pub fn heartbeat_ttl(mut self, d: Duration) -> Self {
        self.heartbeat_ttl = Some(d);
        self
    }

    #[must_use]
    /// Set max time to wait for peer heartbeat traffic before disconnecting.
    pub fn heartbeat_timeout(mut self, d: Duration) -> Self {
        self.heartbeat_timeout = Some(d);
        self
    }

    /// Set max time allowed to complete the ZMTP handshake.
    ///
    /// For encrypted mechanisms this also controls how long a stalled peer can
    /// occupy one `max_pending_handshakes` slot.
    #[must_use]
    pub fn handshake_timeout(mut self, d: Duration) -> Self {
        self.handshake_timeout = Some(d);
        self
    }

    /// Set max simultaneous inbound byte-stream handshakes.
    ///
    /// This caps pre-auth TCP/IPC resource use. If full, new accepted peers
    /// are rejected before a peer driver is spawned and monitors receive
    /// `HandshakeFailed`.
    #[must_use]
    pub fn max_pending_handshakes(mut self, n: usize) -> Self {
        self.max_pending_handshakes = n;
        self
    }

    #[must_use]
    /// Set max allowed size for one complete message.
    pub fn max_message_size(mut self, n: usize) -> Self {
        self.max_message_size = Some(n);
        self
    }

    #[must_use]
    /// Keep only the most recent inbound message.
    pub fn conflate(mut self, c: bool) -> Self {
        self.conflate = c;
        self
    }

    #[must_use]
    /// Require ROUTER sends to target a known peer.
    pub fn router_mandatory(mut self, m: bool) -> Self {
        self.router_mandatory = m;
        self
    }

    #[must_use]
    /// Set behavior when send HWM mutes the socket or peer.
    pub fn on_mute(mut self, m: OnMute) -> Self {
        self.on_mute = m;
        self
    }

    #[must_use]
    /// Set TCP keepalive behavior.
    pub fn tcp_keepalive(mut self, k: KeepAlive) -> Self {
        self.tcp_keepalive = k;
        self
    }

    #[must_use]
    /// Set OS receive buffer size for stream transports.
    pub fn recv_buffer_size(mut self, bytes: usize) -> Self {
        self.recv_buffer_size = Some(bytes);
        self
    }

    #[must_use]
    /// Set OS send buffer size for stream transports.
    pub fn send_buffer_size(mut self, bytes: usize) -> Self {
        self.send_buffer_size = Some(bytes);
        self
    }

    /// Set the wire-payload size at which the recv path switches to a
    /// sized one-shot read. See the field-level docs on
    /// [`large_message_threshold`](Self::large_message_threshold) for
    /// the trade-offs. Pass `0` to fall back to the multi-shot path
    /// for every frame; the threshold is treated as `usize::MAX` in
    /// that case.
    #[must_use]
    pub fn large_message_threshold(mut self, n: usize) -> Self {
        self.large_message_threshold = if n == 0 { None } else { Some(n) };
        self
    }

    /// Disable the one-shot recv switch entirely; the multi-shot path
    /// is used for every inbound frame regardless of size.
    #[must_use]
    pub fn disable_large_message_path(mut self) -> Self {
        self.large_message_threshold = None;
        self
    }

    /// Set the per-`FrameBuffer` arena threshold. Messages smaller than
    /// this are copied into a contiguous arena buffer; larger ones use
    /// zero-copy gather-write. `0` forces gather-write for every
    /// non-empty message. Default: 4 KiB.
    #[must_use]
    pub fn arena_threshold(mut self, bytes: usize) -> Self {
        self.arena_threshold = Some(bytes);
        self
    }

    /// Restore the default per-`FrameBuffer` arena threshold.
    #[must_use]
    pub fn default_arena_threshold(mut self) -> Self {
        self.arena_threshold = None;
        self
    }

    /// Set the per-peer transmit-slot capacity in bytes. Default: 2 MiB.
    #[must_use]
    pub fn transmit_slot_cap(mut self, bytes: usize) -> Self {
        self.transmit_slot_cap = Some(bytes);
        self
    }

    /// Configure this socket as a CURVE server with default CURVE
    /// server options.
    #[cfg(feature = "curve")]
    #[must_use]
    pub fn curve_server(self, our_keypair: CurveKeypair) -> Self {
        self.curve_server_with_options(our_keypair, CurveServerOptions::default())
    }

    /// Configure this socket as a CURVE server with explicit CURVE
    /// server options. Incoming clients must present the matching
    /// server public key during their handshake.
    #[cfg(feature = "curve")]
    #[must_use]
    pub fn curve_server_with_options(
        mut self,
        our_keypair: CurveKeypair,
        options: CurveServerOptions,
    ) -> Self {
        self.mechanism = MechanismSetup::CurveServer {
            our_keypair,
            options,
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
        self.mechanism = MechanismSetup::CurveClient {
            our_keypair,
            server_public,
        };
        self
    }

    /// Configure this socket as a PLAIN server (RFC 24). The
    /// authenticator receives [`MechanismPeerInfo`] with `username`
    /// and `password` populated; return `true` to admit the client.
    /// No encryption is applied; use on trusted networks only.
    #[cfg(feature = "plain")]
    #[must_use]
    pub fn plain_server<F>(mut self, f: F) -> Self
    where
        F: Fn(&MechanismPeerInfo) -> bool + Send + Sync + 'static,
    {
        self.mechanism = MechanismSetup::PlainServer {
            authenticator: Authenticator::new(f),
        };
        self
    }

    /// Configure this socket as a PLAIN client with the given
    /// credentials. The server's authenticator decides admission.
    #[cfg(feature = "plain")]
    #[must_use]
    pub fn plain_client(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.mechanism = MechanismSetup::PlainClient {
            username: username.into(),
            password: password.into(),
        };
        self
    }

    /// Set the outbound compression dictionary. Used by compression transports.
    /// Validated by [`Options::validate`]: must be 1..=8192 bytes.
    /// Disables auto-training when set.
    #[must_use]
    pub fn compression_dict(mut self, dict: impl Into<Bytes>) -> Self {
        self.compression_dict = Some(dict.into());
        self
    }

    /// Enable auto-trained dictionaries for compression transports.
    /// Off by default. See [`Options::compression_auto_train`] for
    /// semantics.
    #[must_use]
    pub fn compression_auto_train(mut self, enabled: bool) -> Self {
        self.compression_auto_train = enabled;
        self
    }

    /// Override the minimum payload size for compression. Messages
    /// smaller than `threshold` bytes are sent uncompressed. Useful
    /// on high-bandwidth links where compressing tiny messages wastes
    /// CPU without meaningful wire savings.
    #[must_use]
    pub fn compression_threshold(mut self, threshold: usize) -> Self {
        self.compression_threshold = Some(threshold);
        self
    }

    /// Set the `zstd+tcp://` compression level.
    ///
    /// Supported zrip levels are -8..=4. Level 0 maps to zrip's library
    /// default, currently level 1. Ignored by LZ4 compression.
    #[must_use]
    pub fn compression_level(mut self, level: i32) -> Self {
        self.compression_level = Some(level);
        self
    }

    /// Set the auto-train dictionary capacity in bytes
    /// (default 2048). Ignored when `compression_dict` is set.
    #[must_use]
    pub fn compression_dict_capacity(mut self, capacity: usize) -> Self {
        self.compression_dict_capacity = Some(capacity);
        self
    }

    /// Set the maximum dictionary size accepted from a peer.
    /// Dicts larger than this are rejected at decode time. Transport hard caps
    /// still apply.
    #[must_use]
    pub fn max_recv_dict_size(mut self, max: usize) -> Self {
        self.max_recv_dict_size = Some(max);
        self
    }

    /// Minimum message size before compression is offloaded to a
    /// background thread (tokio backend only). `None` disables offloading.
    #[must_use]
    pub fn compression_offload_threshold(mut self, threshold: Option<usize>) -> Self {
        self.compression_offload_threshold = threshold;
        self
    }
}

/// Scheduling tradeoff for a socket's I/O driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkloadProfile {
    /// Prefer batching and throughput.
    Throughput,
    /// Prefer promptly handing messages to the application.
    Latency,
}

impl From<Bytes> for Options {
    /// Convenience: build options with a given identity, defaults for the rest.
    fn from(identity: Bytes) -> Self {
        Self::default().identity(identity)
    }
}

/// Reconnection policy applied after a lost connection on `connect()` sockets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReconnectPolicy {
    /// No reconnect; the connection is dropped permanently on failure.
    Disabled,
    /// Retry at a constant interval.
    Fixed(Duration),
    /// Exponential backoff, doubling on each retry.
    Exponential {
        /// Initial retry interval.
        min: Duration,
        /// Maximum retry interval.
        max: Duration,
    },
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        // Constant 100ms matches libzmq's `ZMQ_RECONNECT_IVL` default.
        // Users who want exponential backoff opt in via
        // `Options::reconnect(ReconnectPolicy::Exponential { .. })`.
        Self::Fixed(Duration::from_millis(100))
    }
}

/// What to do when native send HWM is reached and a new message arrives.
///
/// Native bound no-peer round-robin sockets mute immediately. Connected
/// no-peer round-robin sockets queue into a pre-ready pipe until `send_hwm`
/// is reached, then apply this policy. Native OMQ has no `ZMQ_IMMEDIATE`
/// option; `omq-libzmq` implements `ZMQ_IMMEDIATE` at the C layer.
///
/// `PUB`, `XPUB`, and `RADIO` honor `DropOldest` for per-peer fan-out
/// queues. Other fan-out sockets keep the native drop-newest behavior unless
/// `xpub_nodrop` asks them to wait.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum OnMute {
    /// Block the sender until room is available.
    ///
    /// Ignored by fan-out sockets (`PUB`, `XPUB`, `RADIO`), which drop on
    /// mute unless `xpub_nodrop` is set.
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
#[non_exhaustive]
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

impl Options {
    /// Apply `SO_RCVBUF` and `SO_SNDBUF` to a connected socket.
    pub fn apply_socket_buffers<S: SocketRef>(&self, sock: &S) -> std::io::Result<()> {
        let sref = sock.as_socket_ref();
        if let Some(n) = self.recv_buffer_size {
            sref.set_recv_buffer_size(n)?;
        }
        if let Some(n) = self.send_buffer_size {
            sref.set_send_buffer_size(n)?;
        }
        Ok(())
    }
}

impl KeepAlive {
    /// Apply this keepalive policy to a connected TCP socket after
    /// `connect`/`accept` so the option is in effect for the
    /// connection's lifetime.
    pub fn apply<S: SocketRef>(&self, sock: &S) -> std::io::Result<()> {
        let sref = sock.as_socket_ref();
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
        assert_eq!(o.send_hwm, 1000);
        assert_eq!(o.recv_hwm, 1000);
        assert_eq!(o.recv_rate_limit, None);
        assert_eq!(o.recv_ip_rate_limit, None);
        assert_eq!(o.linger, Some(Duration::ZERO));
        assert_eq!(o.handshake_timeout, Some(Duration::from_secs(30)));
        assert_eq!(o.max_pending_handshakes, DEFAULT_MAX_PENDING_HANDSHAKES);
        assert_eq!(o.heartbeat_interval, None);
        assert_eq!(o.max_message_size, None);
        assert_eq!(o.tcp_keepalive, KeepAlive::Default);
        assert!(!o.conflate);
        assert!(!o.router_mandatory);
        assert_eq!(o.compression_level, None);
        assert_eq!(o.on_mute, OnMute::Block);
        assert_eq!(o.large_message_threshold, Some(128 * 1024));
    }

    #[test]
    fn native_default_linger_is_zero() {
        // Native OMQ intentionally differs from libzmq here: async socket
        // close should not wait forever unless the user asks for it.
        assert_eq!(Options::default().linger, Some(Duration::ZERO));
    }

    #[test]
    fn rejects_zero_pending_handshake_cap() {
        let o = Options {
            max_pending_handshakes: 0,
            ..Options::default()
        };
        assert!(o.validate().is_err());
    }

    #[test]
    fn validates_receive_rate_limits() {
        let valid = Options::new()
            .recv_rate_limit(1_000, 2_000)
            .recv_ip_rate_limit(5_000, 10_000);
        assert!(valid.validate().is_ok());
        assert_eq!(
            valid.recv_rate_limit,
            Some(MessageRateLimit::new(1_000, 2_000))
        );
        assert!(Options::new().recv_rate_limit(0, 1).validate().is_err());
        assert!(Options::new().recv_rate_limit(1, 0).validate().is_err());
        assert!(Options::new().recv_ip_rate_limit(0, 1).validate().is_err());
        assert!(Options::new().recv_ip_rate_limit(1, 0).validate().is_err());
    }

    #[test]
    fn validates_zstd_compression_level() {
        assert!(Options::new().compression_level(1).validate().is_ok());
        assert!(Options::new().compression_level(-8).validate().is_ok());
        assert!(Options::new().compression_level(4).validate().is_ok());
        assert!(Options::new().compression_level(5).validate().is_err());
        assert!(Options::new().compression_level(-9).validate().is_err());
    }

    #[cfg(feature = "curve")]
    #[test]
    fn curve_requires_handshake_timeout() {
        let mut o = Options::default().curve_server(CurveKeypair::generate());
        o.handshake_timeout = None;
        assert!(o.validate().is_err());

        let server_kp = CurveKeypair::generate();
        let mut o = Options::default().curve_client(CurveKeypair::generate(), server_kp.public);
        o.handshake_timeout = None;
        assert!(o.validate().is_err());
    }

    #[test]
    fn large_message_threshold_setters() {
        assert_eq!(
            Options::new()
                .large_message_threshold(64 * 1024)
                .large_message_threshold,
            Some(64 * 1024),
        );
        assert_eq!(
            Options::new()
                .large_message_threshold(0)
                .large_message_threshold,
            None,
        );
        assert_eq!(
            Options::new()
                .disable_large_message_path()
                .large_message_threshold,
            None,
        );
    }

    #[test]
    fn arena_threshold_setters() {
        assert_eq!(
            Options::new().arena_threshold(2048).arena_threshold,
            Some(2048)
        );
        assert_eq!(Options::new().arena_threshold(0).arena_threshold, Some(0));
        assert_eq!(
            Options::new()
                .arena_threshold(2048)
                .default_arena_threshold()
                .arena_threshold,
            None,
        );
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
            .workload_profile(WorkloadProfile::Latency)
            .send_hwm(42)
            .recv_hwm(99)
            .linger(Duration::from_secs(5))
            .identity("router-id")
            .heartbeat_interval(Duration::from_secs(1))
            .max_message_size(1024)
            .conflate(true)
            .compression_level(1)
            .router_mandatory(true)
            .on_mute(OnMute::DropNewest);
        assert_eq!(o.send_hwm, 42);
        assert_eq!(o.workload_profile, Some(WorkloadProfile::Latency));
        assert_eq!(o.recv_hwm, 99);
        assert_eq!(o.linger, Some(Duration::from_secs(5)));
        assert_eq!(o.identity, &b"router-id"[..]);
        assert_eq!(o.heartbeat_interval, Some(Duration::from_secs(1)));
        assert_eq!(o.max_message_size, Some(1024));
        assert!(o.conflate);
        assert_eq!(o.compression_level, Some(1));
        assert!(o.router_mandatory);
        assert_eq!(o.on_mute, OnMute::DropNewest);
    }

    #[test]
    fn workload_profile_defaults_to_socket_type_selection() {
        assert_eq!(Options::default().workload_profile, None);
        assert_eq!(
            Options::new()
                .workload_profile(WorkloadProfile::Throughput)
                .workload_profile,
            Some(WorkloadProfile::Throughput)
        );
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
        assert_eq!(o.send_hwm, 1000);
    }
}
