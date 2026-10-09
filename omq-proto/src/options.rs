//! Socket options: typed builder.
//!
//! Defaults differ from libzmq in a few places. Native OMQ linger defaults to
//! zero, while libzmq `ZMQ_LINGER` defaults to forever. Native OMQ
//! `send_hwm`/`recv_hwm` are message-count caps, not byte caps. Native OMQ
//! applies `send_hwm` per outbound pipe; it is not a single socket-wide byte
//! or message budget.
//! Setup defaults to 10 seconds in Rust and all bindings, compared with libzmq's
//! 30-second `ZMQ_HANDSHAKE_IVL`.

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

/// Default deadline for one connection setup attempt, from DNS through READY.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

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

/// Complete codec-parameter snapshot for one bind/connect operation.
///
/// The endpoint selects the codec kind. These parameters replace the socket's
/// five compression settings; `None` selects the codec default, not inheritance.
/// Construct from `&Options` to retain defaults before changing selected fields.
/// Mechanism and decoder size limits come from the socket configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompressionOptions {
    /// Static outbound dictionary. Takes precedence over auto-training.
    pub dict: Option<Bytes>,
    /// Train one dictionary when no static dictionary is supplied.
    pub auto_train: bool,
    /// Minimum part size to attempt compression.
    pub threshold: Option<usize>,
    /// Zstd compression level; ignored by LZ4.
    pub level: Option<i32>,
    /// Training dictionary capacity.
    pub dict_capacity: Option<usize>,
}

impl From<&Options> for CompressionOptions {
    fn from(options: &Options) -> Self {
        Self {
            dict: options.compression_dict.clone(),
            auto_train: options.compression_auto_train,
            threshold: options.compression_threshold,
            level: options.compression_level,
            dict_capacity: options.compression_dict_capacity,
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
    /// This profile does not enable receive spinning; see [`Self::recv_spin`].
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

    /// Maximum busy-wait time before each park in native blocking receives.
    ///
    /// Defaults to zero for every socket type, independently of
    /// [`Self::workload_profile`]. Applies to single-message receives and the
    /// first message of a bulk receive, including timeout and cancelable calls.
    /// Timeouts and cancellation stop the spin early. Async and nonblocking
    /// receives do not spin.
    /// [`Duration::MAX`] polls continuously until delivery, close, cancellation,
    /// or timeout. This consumes an application CPU even while idle.
    ///
    /// Spinning can reduce wakeup latency when application and IO threads have
    /// separate CPU resources, but consumes CPU and can worsen latency when
    /// those threads compete for a CPU. For example, opt in with
    /// `Options::default().recv_spin(Duration::from_micros(50))`.
    pub recv_spin: Duration,

    /// Allow receive batching to relax cross-connection message ordering.
    ///
    /// Defaults to false. Currently applies to bulk receives on PULL, GATHER,
    /// SUB, and XSUB; single-message receives are unchanged. Bursts are bounded
    /// internally, but their size and scheduling are implementation details.
    /// Per-connection FIFO and multipart atomicity are preserved. This does
    /// not wait for additional messages to fill a batch.
    pub recv_batching: bool,

    /// Optional native byte-stream multipart frame-table cache. Shared across
    /// this socket's connections; payload bytes and in-flight messages are not
    /// bounded by this cache. Exhaustion allocates normally. Default: disabled.
    pub recv_message_pool: Option<crate::message::MessagePool>,

    /// Optional application-selected receive payload storage. Shared across
    /// connections; inline parts skip checkout and exhaustion uses owned
    /// storage. Default: disabled. Frozen before the first bind/connect.
    pub recv_payload_pool: Option<crate::PayloadPool>,

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

    /// Reconnection policy after a lost connection. A peer's fatal handshake
    /// ERROR stops automatic retries regardless of this policy.
    pub reconnect: ReconnectPolicy,

    /// ZMTP PING interval. `None` = heartbeats disabled.
    pub heartbeat_interval: Option<Duration>,

    /// TTL announced in PING (peer's how-long-to-wait hint). `None` = omit.
    pub heartbeat_ttl: Option<Duration>,

    /// Close the connection if no traffic received within this window.
    /// Defaults to `heartbeat_interval` when unset.
    pub heartbeat_timeout: Option<Duration>,

    /// Max time for one connection setup attempt. Default 10 seconds. One
    /// deadline covers DNS, dialing, TLS/HTTP when applicable, and ZMTP
    /// authentication through READY. Reconnect attempts each get a fresh budget.
    ///
    /// Encrypted mechanisms and WS/WSS require a finite timeout. QUIC uses
    /// the default when unset. Longer values
    /// give slow peers more time to finish authentication, but also let malicious
    /// peers hold pending-handshake slots longer.
    pub handshake_timeout: Option<Duration>,

    /// Maximum pending byte-stream handshakes per socket. Includes accepted
    /// TCP/IPC connections, WS/WSS before TLS/HTTP, and outbound attempts.
    /// The tokio backend reserves admission before spawning a peer driver,
    /// including before DNS for named outbound connections. WS/WSS also
    /// allows at most 32 pending peers per listener. Named bind/connect API
    /// operations have a separate 128-job cap while awaiting DNS/admission.
    ///
    /// Lower values reduce memory/task pressure from unauthenticated peers,
    /// but can reject legitimate connection bursts while the cap is full.
    /// Higher values admit larger bursts, at the cost of more pre-auth
    /// resource use. Completed handshakes leave this pool immediately; timed
    /// out or failed handshakes release their slot when the peer is closed.
    pub max_pending_handshakes: usize,

    /// Reject incoming messages larger than this. Accounting includes payload
    /// bytes plus one internal payload slot per part. `None` = no limit.
    /// For compression transports this is the decoded size. Framing has a
    /// separate finite allowance for codec overhead and dictionary setup;
    /// command limits and dictionary protocol ceilings remain independent.
    /// Count HWM and assembly limits do not bound aggregate retained bytes;
    /// there is no separate WS message-size default or socket byte ledger.
    pub max_message_size: Option<usize>,

    /// Conflate: keep only the latest message per subscriber. Applies to
    /// `FanOut` patterns only (PUB/XPUB/RADIO). Ignored elsewhere.
    pub conflate: bool,

    /// ROUTER: fail `send` with `Error::Unroutable` for unknown identities.
    /// Full destination queues apply send backpressure; `try_send` returns
    /// `TrySendError::Full`. With this disabled, unknown destinations and
    /// full destination queues silently drop complete messages.
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
    ///
    /// QUIC applies it to UDP sockets, which carry many connections: a
    /// listener's socket at bind, and the connector socket each IO thread
    /// shares, which keeps the largest size its connections requested.
    /// Loopback or LAN bursts from many QUIC peers can overflow the OS
    /// default and cause packet loss.
    ///
    /// Best effort: Linux caps the size at `net.core.rmem_max`; macOS and
    /// BSDs reject sizes above `kern.ipc.maxsockbuf` and keep the old size.
    pub recv_buffer_size: Option<usize>,

    /// `SO_SNDBUF` size in bytes. Applied to every TCP/IPC stream after
    /// connect/accept, and to QUIC UDP sockets like `recv_buffer_size`.
    /// `None` leaves the OS default. Linux caps the size at
    /// `net.core.wmem_max`.
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

    /// Browser-origin policy for WS/WSS listeners. Requires the `ws` feature.
    #[cfg(feature = "ws")]
    pub ws: WsOptions,

    /// TLS and transport settings for `quic://` endpoints. Requires the
    /// `quic` feature.
    #[cfg(feature = "quic")]
    pub quic: QuicOptions,

    /// Bounded datagram transport settings. Requires the `dart` feature.
    #[cfg(feature = "dart")]
    pub dart: DartOptions,
}

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
    #[default]
    Auto,
    Disabled,
}

/// Settings for reliable, ordered, bounded `dart://` messages.
#[cfg(feature = "dart")]
#[derive(Clone, Copy, Debug)]
pub struct DartOptions {
    /// Maximum admitted peers across the socket. Default 1024.
    pub max_ready_peers: usize,
    /// Busy wait before readiness waiting. Default zero, maximum 50 us.
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
    fn validate(self) -> crate::error::Result<()> {
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
    /// Maximum ready QUIC connections across this socket's endpoints.
    /// Default 1024; must be nonzero.
    pub max_ready_peers: usize,
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

    fn validate(&self) -> crate::error::Result<()> {
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

/// Backward-compatible alias. [`MechanismSetup`] is the canonical type.
pub type MechanismConfig = MechanismSetup;

impl Default for Options {
    fn default() -> Self {
        Self {
            workload_profile: None,
            send_hwm: 1000,
            recv_hwm: 1000,
            recv_spin: Duration::ZERO,
            recv_batching: false,
            recv_message_pool: None,
            recv_payload_pool: None,
            recv_rate_limit: None,
            recv_ip_rate_limit: None,
            linger: Some(Duration::ZERO),
            identity: Bytes::new(),
            reconnect: ReconnectPolicy::default(),
            heartbeat_interval: None,
            heartbeat_ttl: None,
            heartbeat_timeout: None,
            handshake_timeout: Some(DEFAULT_HANDSHAKE_TIMEOUT),
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
            #[cfg(feature = "quic")]
            quic: QuicOptions::default(),
            #[cfg(feature = "dart")]
            dart: DartOptions::default(),
            #[cfg(feature = "ws")]
            wss_tls: WssTls::default(),
            #[cfg(feature = "ws")]
            ws: WsOptions::default(),
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
        if self.identity.first() == Some(&0) {
            return Err(crate::error::Error::Config(
                "identity must not start with a zero byte".into(),
            ));
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
        #[cfg(feature = "quic")]
        self.quic.validate()?;
        #[cfg(feature = "dart")]
        self.dart.validate()?;
        #[cfg(feature = "ws")]
        if self.ws.max_ready_peers == 0 {
            return Err(crate::error::Error::Config(
                "ws.max_ready_peers must be greater than zero".into(),
            ));
        }
        self.validate_recv_rate_limits()?;
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
            if username.len() > 255 || !username.bytes().all(|byte| byte.is_ascii_graphic()) {
                return Err(crate::error::Error::Config(format!(
                    "PLAIN username must contain at most 255 ASCII VCHAR bytes, got {} bytes",
                    username.len(),
                )));
            }
            if password.len() > 255 || !password.bytes().all(|byte| byte.is_ascii_graphic()) {
                return Err(crate::error::Error::Config(format!(
                    "PLAIN password must contain at most 255 ASCII VCHAR bytes, got {} bytes",
                    password.len(),
                )));
            }
        }
        #[cfg(feature = "curve")]
        if let MechanismSetup::CurveServer { our_keypair, .. }
        | MechanismSetup::CurveClient { our_keypair, .. } = &self.mechanism
            && our_keypair.secret.derive_public() != our_keypair.public
        {
            return Err(crate::error::Error::Config(
                "CURVE public key does not match secret key".into(),
            ));
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

    fn validate_recv_rate_limits(&self) -> crate::error::Result<()> {
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

    /// Set the busy-wait budget before parking in native blocking receives.
    /// Zero disables spinning. See [`Self::recv_spin`] for scope and tradeoffs.
    #[must_use]
    pub fn recv_spin(mut self, budget: Duration) -> Self {
        self.recv_spin = budget;
        self
    }

    /// Allow bounded per-connection bursts when receiving batches.
    /// See [`Self::recv_batching`] for supported socket types and semantics.
    #[must_use]
    pub fn recv_batching(mut self, enabled: bool) -> Self {
        self.recv_batching = enabled;
        self
    }

    /// Recycle native byte-stream receive frame tables through this bounded pool.
    #[must_use]
    pub fn recv_message_pool(mut self, pool: crate::message::MessagePool) -> Self {
        self.recv_message_pool = Some(pool);
        self
    }

    /// Set explicit receive payload size classes, shared across connections.
    /// Inline bodies skip checkout; exhaustion and oversized bodies use owned
    /// allocations. Inproc transfers existing payload owners unchanged.
    #[must_use]
    pub fn recv_payload_pool(mut self, pool: crate::PayloadPool) -> Self {
        self.recv_payload_pool = Some(pool);
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
    /// Set OS receive buffer size for TCP/IPC streams and QUIC UDP sockets.
    pub fn recv_buffer_size(mut self, bytes: usize) -> Self {
        self.recv_buffer_size = Some(bytes);
        self
    }

    #[must_use]
    /// Set OS send buffer size for TCP/IPC streams and QUIC UDP sockets.
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
    /// PLAIN adds no encryption; use it over a trusted or encrypted transport.
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

    /// Configure this socket as a PLAIN server accepting exact, case-sensitive
    /// username/password pairs.
    ///
    /// An empty allowlist denies every client. PLAIN provides authentication,
    /// not encryption. Use it only over a trusted or separately encrypted
    /// transport.
    #[cfg(feature = "plain")]
    #[must_use]
    pub fn plain_server_credentials<I, U, P>(mut self, credentials: I) -> Self
    where
        I: IntoIterator<Item = (U, P)>,
        U: Into<String>,
        P: Into<String>,
    {
        self.mechanism = MechanismSetup::PlainServer {
            authenticator: Authenticator::plain_credentials(credentials),
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
/// Receiving a fatal handshake ERROR stops automatic retries under every policy.
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
    #[test]
    fn identity_limits_and_reserved_prefix() {
        use bytes::Bytes;
        for valid in [
            Bytes::new(),
            Bytes::from_static(b"valid\x00suffix"),
            Bytes::from(vec![b'x'; 255]),
        ] {
            assert!(super::Options::default().identity(valid).validate().is_ok());
        }
        for invalid in [
            Bytes::from_static(b"\x00reserved"),
            Bytes::from(vec![b'x'; 256]),
        ] {
            assert!(
                super::Options::default()
                    .identity(invalid)
                    .validate()
                    .is_err()
            );
        }
    }
    use super::*;

    #[cfg(feature = "dart")]
    #[test]
    fn dart_defaults_are_bounded_and_spinning_is_explicit() {
        let mut options = Options::default();
        assert_eq!(options.dart.max_ready_peers, 1024);
        assert_eq!(options.dart.io_spin, Duration::ZERO);
        assert_eq!(options.dart.ecn, DartEcn::Auto);
        assert_eq!(options.dart.congestion, DartCongestion::Adaptive);
        assert_eq!(options.dart.window_messages, 256);
        assert_eq!(options.dart.max_send_rate, None);
        options.dart.io_spin = Duration::from_micros(50);
        assert!(options.validate().is_ok());
        options.dart.io_spin += Duration::from_nanos(1);
        assert!(options.validate().is_err());
        options.dart.io_spin = Duration::ZERO;
        options.dart.max_ready_peers = 0;
        assert!(options.validate().is_err());
        options.dart.max_ready_peers = 1;
        for window in [0, 3, 65_537] {
            options.dart.window_messages = window;
            assert!(options.validate().is_err());
        }
        options.dart.window_messages = 65_536;
        assert!(options.validate().is_ok());
        options.dart.max_send_rate = Some(0);
        assert!(options.validate().is_err());
    }

    #[cfg(feature = "quic")]
    #[test]
    fn debug_redacts_quic_private_key() {
        let mut options = Options::default();
        options.quic.server_key_pem = Some(b"private-key-sentinel".to_vec());
        let debug = format!("{options:?}");
        assert!(debug.contains("server_key_pem: Some(\"<redacted>\")"));
        assert!(!debug.contains("private-key-sentinel"));
    }

    #[cfg(feature = "ws")]
    #[test]
    fn debug_redacts_wss_private_key() {
        let mut options = Options::default();
        options.wss_tls.server_key_pem = Some(b"private-key-sentinel".to_vec());
        let debug = format!("{options:?}");
        assert!(debug.contains("server_key_pem: Some(\"<redacted>\")"));
        assert!(!debug.contains("private-key-sentinel"));
    }

    #[cfg(feature = "plain")]
    #[test]
    fn debug_redacts_plain_client_password() {
        let options = Options::default().plain_client("alice", "password-sentinel");
        let debug = format!("{options:?}");
        assert!(debug.contains("password: \"<redacted>\""));
        assert!(!debug.contains("password-sentinel"));
    }

    #[cfg(feature = "plain")]
    #[test]
    fn fixed_plain_credentials_match_exactly() {
        let options =
            Options::default().plain_server_credentials([("alice", "secret"), ("bob", "hunter2")]);
        let MechanismSetup::PlainServer { authenticator } = options.mechanism else {
            panic!("expected PLAIN server");
        };
        let peer = |username: &str, password: &str| MechanismPeerInfo {
            mechanism: crate::proto::MechanismName::PLAIN,
            public_key: [0; 32],
            identity: None,
            peer_address: None,
            username: Some(username.to_owned()),
            password: Some(password.to_owned()),
        };

        assert_eq!(
            authenticator.authenticate(&peer("alice", "secret")).status,
            crate::AuthenticationStatus::Success
        );
        assert_eq!(
            authenticator.authenticate(&peer("bob", "hunter2")).status,
            crate::AuthenticationStatus::Success
        );
        assert_eq!(
            authenticator
                .authenticate(&peer("mallory", "secret"))
                .status,
            crate::AuthenticationStatus::Denied
        );
    }

    #[cfg(feature = "plain")]
    #[test]
    fn plain_client_credentials_require_rfc_24_vchar() {
        assert!(Options::default().plain_client("", "").validate().is_ok());
        assert!(
            Options::default()
                .plain_client("alice", "!secret~")
                .validate()
                .is_ok()
        );
        for (username, password) in [
            ("has space", "secret"),
            ("alice", "line\nbreak"),
            ("alice", "\u{e9}"),
            (&"x".repeat(256), "secret"),
        ] {
            assert!(
                Options::default()
                    .plain_client(username, password)
                    .validate()
                    .is_err()
            );
        }
    }

    #[test]
    fn defaults_are_per_socket_hwm_block() {
        let o = Options::default();
        assert_eq!(o.send_hwm, 1000);
        assert_eq!(o.recv_hwm, 1000);
        assert!(!o.recv_batching);
        assert_eq!(o.recv_rate_limit, None);
        assert_eq!(o.recv_ip_rate_limit, None);
        assert_eq!(o.linger, Some(Duration::ZERO));
        assert_eq!(o.handshake_timeout, Some(Duration::from_secs(10)));
        assert_eq!(o.max_pending_handshakes, DEFAULT_MAX_PENDING_HANDSHAKES);
        assert_eq!(o.heartbeat_interval, None);
        assert_eq!(o.max_message_size, None);
        assert_eq!(o.tcp_keepalive, KeepAlive::Default);
        assert!(!o.conflate);
        assert!(!o.router_mandatory);
        assert_eq!(o.compression_level, None);
        assert_eq!(o.on_mute, OnMute::Block);
        assert_eq!(o.large_message_threshold, Some(128 * 1024));
        #[cfg(feature = "quic")]
        {
            assert_eq!(o.quic.idle_timeout, Duration::from_secs(10));
            assert_eq!(o.quic.keep_alive_interval, Duration::from_secs(2));
        }
    }

    #[cfg(feature = "ws")]
    #[test]
    fn ws_ready_peer_limit_is_finite_and_cannot_be_disabled() {
        let mut options = Options::default();
        assert_eq!(options.ws.max_ready_peers, 1024);
        options.ws.max_ready_peers = 0;
        assert!(options.validate().is_err());
        options.ws.max_ready_peers = 1;
        assert!(options.validate().is_ok());
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

    #[cfg(feature = "curve")]
    #[test]
    fn rejects_mismatched_curve_keypairs() {
        let valid = CurveKeypair::generate();
        let mut bad_public = *valid.public.as_bytes();
        bad_public[0] ^= 1;
        let mismatched = CurveKeypair {
            public: CurvePublicKey::from_bytes(bad_public),
            secret: valid.secret,
        };

        let server_error = Options::default()
            .curve_server(mismatched.clone())
            .validate()
            .unwrap_err();
        assert_eq!(
            server_error.to_string(),
            "invalid configuration: CURVE public key does not match secret key"
        );

        let client_error = Options::default()
            .curve_client(mismatched, CurveKeypair::generate().public)
            .validate()
            .unwrap_err();
        assert_eq!(
            client_error.to_string(),
            "invalid configuration: CURVE public key does not match secret key"
        );
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
            .recv_spin(Duration::from_micros(50))
            .recv_batching(true)
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
        assert_eq!(o.recv_spin, Duration::from_micros(50));
        assert!(o.recv_batching);
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
        assert_eq!(Options::default().recv_spin, Duration::ZERO);
        assert_eq!(
            Options::default()
                .workload_profile(WorkloadProfile::Latency)
                .recv_spin,
            Duration::ZERO
        );
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
