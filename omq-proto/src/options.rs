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
use crate::subscription::{SubscriptionAdmission, SubscriptionRequest};

mod compression;
#[cfg(any(feature = "plain", feature = "curve"))]
mod security;
mod tcp;
#[cfg(any(feature = "dart", feature = "quic", feature = "ws"))]
mod transport;
mod validate;

pub use compression::CompressionOptions;
pub use tcp::KeepAlive;
#[cfg(feature = "quic")]
pub use transport::QuicOptions;
#[cfg(feature = "dart")]
pub use transport::{DartCongestion, DartEcn, DartOptions};
#[cfg(feature = "ws")]
pub use transport::{WsOptions, WssTls};

/// Default cap for byte-stream peers that are accepted but have not
/// completed the ZMTP handshake.
pub const DEFAULT_MAX_PENDING_HANDSHAKES: usize = 128;

/// Default deadline for one connection setup attempt, from DNS through READY.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default per-`FrameBuffer` arena threshold.
pub const DEFAULT_ARENA_THRESHOLD: usize = crate::frame_buffer::ARENA_THRESHOLD;

/// Default socket-wide cap for automatically allocated outbound SHM regions.
pub const DEFAULT_SHM_MAX_MEMORY: usize = 64 * 1024 * 1024;

/// Default PEER per-connection queue byte budget and single-message limit.
pub const DEFAULT_PEER_MAX_BYTES: usize = 16 * 1024 * 1024;

/// Default per-socket monitor event queue capacity.
pub const DEFAULT_MONITOR_CAPACITY: usize = 256;

/// Largest message queue allocated for one HWM. Queues preallocate their
/// capacity, so larger HWM values behave as this limit.
pub const MAX_QUEUE_CAPACITY: usize = 1 << 20;

/// Queue capacity in messages for `hwm`, clamped to `1..=MAX_QUEUE_CAPACITY`.
#[must_use]
pub fn queue_capacity(hwm: u32) -> usize {
    usize::try_from(hwm)
        .unwrap_or(MAX_QUEUE_CAPACITY)
        .clamp(1, MAX_QUEUE_CAPACITY)
}

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
    /// On owned runtimes, IPC and SHM latency drivers briefly keep the reactor
    /// polling after traffic, consuming additional IO CPU. Blocking receives
    /// spin only when [`Self::recv_spin`] is enabled.
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

    /// Native PEER queue capacity per sending handle and destination. Default 64.
    /// Uses the smaller of this and [`Self::send_hwm`] (at least one), rounded
    /// up to a power of two. The shared per-connection `send_hwm` still applies.
    /// Must be positive and at most `1 << (usize::BITS - 2)`.
    pub peer_send_queue_capacity: usize,

    /// Native PEER maximum sending handles per destination. Default 64.
    /// Retired queues count until drained; sends wait for registration capacity.
    /// Must be positive and less than `usize::MAX`.
    pub peer_max_senders: usize,

    /// Native PEER maximum receive peers per socket. Default 128.
    /// Retired queues count until drained. New connections are rejected while
    /// full; existing connections remain usable. Must be positive and less than
    /// `usize::MAX`.
    pub peer_max_peers: usize,

    /// Native PEER retained messages per receiving connection. Default 8192.
    /// Includes claimed and held messages, independently of [`Self::recv_hwm`].
    /// Must be positive. A full budget pauses that connection's receive path.
    pub peer_recv_max_messages: usize,

    /// Native PEER outbound queue byte budget per connection, shared by clones.
    /// Counts payload bytes and one payload slot per part, excluding identity.
    /// `None` uses the larger of 16 MiB and [`Self::max_message_size`]. An
    /// explicit nonzero budget is used exactly; oversized sends are rejected.
    /// Driver buffers and messages already dequeued for transmission are excluded.
    pub peer_send_max_bytes: Option<usize>,

    /// Native PEER receive queue byte budget per connection, including claimed
    /// and held messages. Counts retained backing allocations and frame tables.
    /// `None` uses the larger of 16 MiB and [`Self::max_message_size`]. An
    /// explicit nonzero budget is used exactly; an oversized message closes
    /// that connection. Decoder storage and one pending message are excluded.
    /// This is independent of the single-message limit [`Self::max_message_size`].
    pub peer_recv_max_bytes: Option<usize>,

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
    /// connections; supplies destinations without copying decoded bodies.
    /// Inline parts skip checkout and exhaustion uses owned storage.
    /// Default: disabled. Frozen before the first bind/connect.
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

    /// Maximum pending byte-stream handshakes per socket, counted separately
    /// for inbound peers (accepted TCP/IPC connections, WS/WSS before
    /// TLS/HTTP) and outbound attempts, so inbound load cannot stall
    /// reconnects. One remote host (an IPv4 address or IPv6 /64, loopback
    /// exempt) may hold at most an eighth of the inbound slots.
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
    /// bytes plus one internal payload slot per part. `None` = no limit,
    /// except native PEER sockets which default to 16 MiB.
    /// For compression transports this is the decoded size. Framing has a
    /// separate finite allowance for codec overhead and dictionary setup;
    /// command limits and dictionary protocol ceilings remain independent.
    /// Count HWM and assembly limits do not bound aggregate retained bytes;
    /// there is no separate WS message-size default or socket byte ledger.
    pub max_message_size: Option<usize>,

    /// Maximum automatic outbound SHM mapping bytes across this socket's peers
    /// and fan-out lanes, including headers and alignment padding. Defaults to 64 MiB.
    /// `None` disables this cap; `Some(0)` uses the shared carrier for ordinary
    /// bodies without allocating automatic SHM payload storage.
    ///
    /// Unavailable storage uses carrier delivery rather than failing or waiting
    /// forever for a body larger than the cap. Explicit producer pools, imported
    /// mappings, completion/carrier tables, and ordinary heap queues are excluded.
    /// Retained owners and leases keep their automatic region bytes charged.
    /// Ignored by other transports.
    pub shm_max_memory: Option<usize>,

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

    /// `TCP_USER_TIMEOUT` for TCP streams on Linux and Android: how long sent
    /// data may stay unacknowledged, or queued data unsent, before the kernel
    /// drops the connection. Off by default.
    ///
    /// Heartbeats cannot time out a peer while our own writes are stalled,
    /// so without this a dead host behind a full send buffer is dropped only
    /// at the kernel retry limit (about 15 minutes). This timeout also drops
    /// a live peer that stops reading for this long, losing the data in the
    /// kernel send buffer. The connection then reconnects like any other
    /// peer loss. `None` or zero keeps the system default. Ignored on other
    /// platforms and non-TCP transports. libzmq: `ZMQ_TCP_MAXRT`.
    pub tcp_user_timeout: Option<Duration>,

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

    /// Minimum part size (bytes) before compression is attempted, or aggregate
    /// frame bytes for a compression arena. Smaller compression units are sent raw.
    /// `None` uses defaults that depend on the codec and dictionary presence.
    pub compression_threshold: Option<usize>,

    /// Fixed compression level for `zstd+tcp://`. `None` adapts the level
    /// within `compression_level_range`. Supported zrip levels are -8..=4;
    /// level 0 maps to zrip's library default (currently level 1). Ignored by
    /// `lz4+tcp://`.
    pub compression_level: Option<i32>,

    /// Adaptive level range for `zstd+tcp://` when `compression_level` is
    /// unset. `None` adapts within -7..=4. Each connection raises the level
    /// while the transport limits throughput or input is idle, and lowers it
    /// while compression limits throughput. Ignored by `lz4+tcp://`.
    pub compression_level_range: Option<(i32, i32)>,

    /// Auto-train dict capacity in bytes. Controls the maximum size of
    /// the dictionary produced by auto-training. Default: 2048.
    /// Ignored when `compression_dict` is set.
    pub compression_dict_capacity: Option<usize>,

    /// Maximum dictionary size (bytes) accepted from a peer. Dicts
    /// larger than this are rejected. Default: 8192 for compression transports.
    pub max_recv_dict_size: Option<usize>,

    /// Minimum message size (bytes) before compression is offloaded to
    /// a background thread (tokio backend only). Smaller messages, and
    /// the last large message of a batch, are compressed inline on the
    /// driver task. `None` disables offloading entirely. Default:
    /// `Some(8192)`.
    pub compression_offload_threshold: Option<usize>,

    /// Switch the recv path to a sized one-shot read for any inbound
    /// frame whose wire payload is at least this many bytes.
    ///
    /// On `omq-tokio` this threshold triggers a fast path that reads
    /// large payloads into a single pre-sized buffer instead of
    /// accumulating fixed-size reads through the codec. Medium-large
    /// payloads may use bounded pooled buffers; larger payloads use
    /// one-shot owned buffers. Explicit receive pools also supply destinations
    /// for incomplete smaller bodies when this path is enabled.
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

    /// Monitor event queue capacity, shared by all monitors of this socket.
    /// Default 256. A monitor that falls further behind observes `Lagged`
    /// and resumes at the oldest retained event. Slots are allocated on the
    /// first `monitor()` call and keep their last event alive until
    /// overwritten. Must be in `1..=MAX_QUEUE_CAPACITY`.
    pub monitor_capacity: usize,

    /// PUB/XPUB: admission callback for each SUBSCRIBE from a peer. `None`
    /// admits every subscription. Ignored by other socket types.
    /// See [`SubscriptionAdmission`] for what a rejection skips.
    pub subscription_admission: Option<SubscriptionAdmission>,

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

/// Backward-compatible alias. [`MechanismSetup`] is the canonical type.
pub type MechanismConfig = MechanismSetup;

impl Default for Options {
    fn default() -> Self {
        Self {
            workload_profile: None,
            send_hwm: 1000,
            recv_hwm: 1000,
            peer_send_queue_capacity: 64,
            peer_max_senders: 64,
            peer_max_peers: 128,
            peer_recv_max_messages: 8192,
            peer_send_max_bytes: None,
            peer_recv_max_bytes: None,
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
            shm_max_memory: Some(DEFAULT_SHM_MAX_MEMORY),
            conflate: false,
            router_mandatory: false,
            on_mute: OnMute::Block,
            tcp_keepalive: KeepAlive::default(),
            tcp_user_timeout: None,
            recv_buffer_size: None,
            send_buffer_size: None,
            mechanism: MechanismSetup::Null,
            compression_dict: None,
            compression_auto_train: false,
            compression_threshold: None,
            compression_level: None,
            compression_level_range: None,
            compression_dict_capacity: None,
            max_recv_dict_size: None,
            compression_offload_threshold: Some(8192),
            large_message_threshold: Some(128 * 1024),
            arena_threshold: None,
            transmit_slot_cap: None,
            xpub_nodrop: false,
            reconnect_stop_conn_refused: false,
            monitor_capacity: DEFAULT_MONITOR_CAPACITY,
            subscription_admission: None,
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

    /// Send queue capacity in messages, from `send_hwm` via [`queue_capacity`].
    #[must_use]
    pub fn send_queue_capacity(&self) -> usize {
        queue_capacity(self.send_hwm)
    }

    /// Receive queue capacity in messages, from `recv_hwm` via [`queue_capacity`].
    #[must_use]
    pub fn recv_queue_capacity(&self) -> usize {
        queue_capacity(self.recv_hwm)
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

    /// Set the native PEER queue capacity per sending handle and destination.
    /// See [`Self::peer_send_queue_capacity`] for rounding and shared limits.
    #[must_use]
    pub fn peer_send_queue_capacity(mut self, messages: usize) -> Self {
        self.peer_send_queue_capacity = messages;
        self
    }

    /// Set the native PEER sending-handle limit per destination.
    /// See [`Self::peer_max_senders`] for retired queue accounting.
    #[must_use]
    pub fn peer_max_senders(mut self, senders: usize) -> Self {
        self.peer_max_senders = senders;
        self
    }

    /// Set the native PEER receive-peer limit per socket.
    /// See [`Self::peer_max_peers`] for connection admission behavior.
    #[must_use]
    pub fn peer_max_peers(mut self, peers: usize) -> Self {
        self.peer_max_peers = peers;
        self
    }

    /// Set the native PEER retained-message budget per receiving connection.
    /// See [`Self::peer_recv_max_messages`] for claim and HWM accounting.
    #[must_use]
    pub fn peer_recv_max_messages(mut self, messages: usize) -> Self {
        self.peer_recv_max_messages = messages;
        self
    }

    /// Set the native PEER outbound byte budget for each connection.
    /// See [`Self::peer_send_max_bytes`] for accounting and defaults.
    #[must_use]
    pub fn peer_send_max_bytes(mut self, bytes: usize) -> Self {
        self.peer_send_max_bytes = Some(bytes);
        self
    }

    /// Set the native PEER receive byte budget for each connection.
    /// See [`Self::peer_recv_max_bytes`] for accounting and defaults.
    #[must_use]
    pub fn peer_recv_max_bytes(mut self, bytes: usize) -> Self {
        self.peer_recv_max_bytes = Some(bytes);
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
    /// Pools supply destinations for incomplete bodies, fragmentation, and
    /// decoders that support fixed storage. Contiguous buffered bodies keep their
    /// byte views. Inline bodies skip checkout; exhaustion and oversized bodies
    /// use owned allocations. Inproc transfers payload owners unchanged.
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

    /// Cap automatically allocated outbound SHM across all peers of this socket.
    /// See [`Self::shm_max_memory`] for scope and carrier fallback behavior.
    #[must_use]
    pub fn shm_max_memory(mut self, bytes: usize) -> Self {
        self.shm_max_memory = Some(bytes);
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
    /// Set `TCP_USER_TIMEOUT` for TCP streams. See the
    /// [`tcp_user_timeout`](Self::tcp_user_timeout) field for the trade-off.
    pub fn tcp_user_timeout(mut self, timeout: Duration) -> Self {
        self.tcp_user_timeout = Some(timeout);
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

    /// Set the monitor event queue capacity. See [`Self::monitor_capacity`].
    #[must_use]
    pub fn monitor_capacity(mut self, events: usize) -> Self {
        self.monitor_capacity = events;
        self
    }

    /// Admit or reject each SUBSCRIBE a PUB/XPUB socket receives.
    /// See [`Self::subscription_admission`].
    #[must_use]
    pub fn subscription_admission<F>(mut self, f: F) -> Self
    where
        F: Fn(&SubscriptionRequest<'_>) -> bool + Send + Sync + 'static,
    {
        self.subscription_admission = Some(SubscriptionAdmission::new(f));
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

#[cfg(test)]
mod tests;
