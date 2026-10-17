//! Per-connection driver: one tokio task per live peer connection.
//!
//! Queue-space and event-mailbox waits remain under the main select alongside
//! local control, reverse writes, cancellation, and deadlines. Large payload
//! reads retain their destination but read at most 64 KiB per selected operation.
//! Control drains stop at 64 commands or 64 KiB and poll writes before another
//! turn; receive backpressure suspends heartbeat silence accounting.
//!
//! Drain carries the socket's original linger deadline through accepted batches,
//! offloads, arenas, slots, partial writes, WS CLOSE 1000, and writer/TLS shutdown.
//! A missing CLOSE reply cannot restart that clock. Unlimited linger may wait
//! indefinitely; peer-initiated WS close gets a ten-second reply-flush ceiling,
//! tightened by a later socket close. Transport completion does not acknowledge
//! remote application delivery.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use omq_proto::MessageRateLimit;
use omq_proto::message::Message;
use omq_proto::proto::transform::{MessageDecoder, MessageEncoder};
use omq_proto::proto::{Command, Connection, Event};

use super::actor_output::PeerOutput;
use super::compression_pool::CompressionPool;
use super::peer_completion::CompletionProgress;
use super::rate_limit::SharedIpRateLimiter;
use super::send_pipe::{SendPipeConsumer, SendPipeProducerHandle};
use super::transmit_slot::PeerTransmitSlot;

mod activation;
mod builder;
mod control;
mod outbound;
mod output;
mod read;
mod receive;
mod run;
mod shm;
mod stream;
mod write;

pub(crate) use receive::ReceiveProfile;
pub use stream::{ChunkWrite, DriverStream, DriverWrite};

const OUTBOUND_BATCH_TIME: Duration = Duration::from_millis(1);
const BATCH_TIME_CHECK_MSGS: usize = 32;
const IPC_LATENCY_POLL_WINDOW: Duration = Duration::from_micros(50);
pub use super::recv_sink::{
    AuthenticatedRecvItem, AuthenticatedRecvSink, RecvSink, RecvSinkConfig, RepRecvSink,
    ServerRecvSink, YringSink,
};

/// Driver-level timing configuration: handshake deadline, heartbeat
/// cadence, idle-close timeout.
#[derive(Debug, Clone, Copy, Default)]
pub struct PeerDriverConfig {
    /// Close the connection if the ZMTP handshake doesn't finish within
    /// this window. `None` = no deadline.
    pub handshake_timeout: Option<Duration>,
    /// PING cadence. `None` disables heartbeat.
    pub heartbeat_interval: Option<Duration>,
    /// Close the connection if nothing has been received for this long.
    /// Defaults to `heartbeat_interval` when unset and heartbeat is on.
    pub heartbeat_timeout: Option<Duration>,
    /// `TTL` field of outgoing PING (peer-hint for when to assume dead).
    pub heartbeat_ttl: Option<Duration>,
    /// Recv frames whose payload exceeds this threshold directly into
    /// a pre-sized owned buffer, bypassing the fixed
    /// `read_buf` -> `Connection` buffering path. `0` disables.
    pub large_message_threshold: usize,
    /// Hard per-connection receive message rate limit.
    pub recv_rate_limit: Option<MessageRateLimit>,
}

/// Commands accepted by a running [`ConnectionDriver`].
#[derive(Debug)]
pub enum PeerDriverCommand {
    /// Allow application messages to flow after the socket actor has accepted
    /// this peer as ready.
    ActivateDataPlane,
    /// Install a socket-selected receive sink before activating application
    /// traffic. Used for PEER receive queues after handshake.
    ActivateWithRecvSink(RecvSink),
    /// Queue a ZMTP command for send (SUBSCRIBE, CANCEL, JOIN, LEAVE, ...).
    SendCommand(Command),
    /// Finish accepted output before shutting down the transport. The deadline
    /// belongs to the socket close operation and must not restart per stage.
    DrainAndClose {
        /// Absolute linger deadline; `None` waits indefinitely.
        deadline: Option<Instant>,
    },
    /// Stop immediately, including any staged output.
    Close,
}

/// Data-plane work accepted by a running [`ConnectionDriver`].
#[derive(Debug)]
pub enum PeerDriverData {
    /// Queue an application message for send.
    SendMessage(Message),
    /// Pre-encoded wire bytes. Pushed directly into the transmit buffer,
    /// skipping per-message encoding for callers that already have shared
    /// wire chunks.
    SendEncoded(std::sync::Arc<smallvec::SmallVec<[bytes::Bytes; 4]>>),
}

/// Handle returned to callers after spawning a driver. `inbox` delivers
/// commands into the driver; `cancel` requests early teardown.
#[derive(Debug, Clone)]
pub struct PeerDriverHandle {
    /// Control-plane commands. Never carries application data.
    pub inbox: mpsc::Sender<PeerDriverCommand>,
    /// Fallback data plane for peers without a send pipe or transmit slot.
    pub data_inbox: mpsc::Sender<PeerDriverData>,
    /// Request immediate driver teardown.
    pub cancel: CancellationToken,
    pub(crate) transmit_slot: Option<Arc<PeerTransmitSlot>>,
    pub(crate) direct_tcp_writer: Option<Arc<crate::socket::dispatch::DirectTcpWriter>>,
    pub(crate) send_pipe: Option<SendPipeProducerHandle>,
    /// Direct route into an inproc peer's receive queue.
    pub(crate) inproc: Option<crate::transport::inproc::InprocSender>,
}

#[derive(Debug, Clone)]
pub(crate) struct ActorPeerDriverHandle {
    /// Control-plane commands. Never carries application data.
    pub inbox: super::control_inbox::Sender,
    /// Fallback data plane for peers without a send pipe or transmit slot.
    pub data_inbox: super::data_inbox::Sender,
    pub cancel: CancellationToken,
    pub(crate) transmit_slot: Option<Arc<PeerTransmitSlot>>,
    pub(crate) direct_tcp_writer: Option<Arc<crate::socket::dispatch::DirectTcpWriter>>,
    pub(crate) send_pipe: Option<SendPipeProducerHandle>,
    /// Direct route into an inproc peer's receive queue.
    pub(crate) inproc: Option<crate::transport::inproc::InprocSender>,
}

impl From<PeerDriverHandle> for ActorPeerDriverHandle {
    fn from(handle: PeerDriverHandle) -> Self {
        Self {
            inbox: handle.inbox.into(),
            data_inbox: super::data_inbox::Sender::Legacy(handle.data_inbox),
            cancel: handle.cancel,
            transmit_slot: handle.transmit_slot,
            direct_tcp_writer: handle.direct_tcp_writer,
            send_pipe: handle.send_pipe,
            inproc: handle.inproc,
        }
    }
}

/// Parsed ZMTP events and the final closure signal for standalone drivers.
/// Socket-owned drivers reserve a separate lifecycle slot and publish protocol
/// events independently of their bounded application-data lane.
#[derive(Debug)]
pub enum PeerEvent {
    /// A decoded protocol event.
    Event(Event),
    /// The driver terminated.
    Closed {
        /// Failure description, or `None` for orderly shutdown.
        error: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriverStep {
    Continue,
    Yield,
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreActivationStep {
    Continue,
    Activate,
    Close,
}

/// A single-connection driver: reads bytes from the stream, feeds the
/// `Connection` state machine, forwards events out, accepts commands in,
/// writes bytes produced by the connection.
#[derive(Debug)]
pub struct ConnectionDriver<T>
where
    T: DriverStream,
{
    stream: T,
    network: crate::network_limit::Lane,
    connection: Connection,
    inbox: super::control_inbox::Receiver,
    data_inbox: Option<super::data_inbox::Receiver>,
    /// Application-data output: one socket-owned fanring producer, or the
    /// caller's combined Tokio queue for a standalone driver.
    peer_out: PeerOutput,
    notify_xpub: bool,
    /// Socket-owned drivers publish codec protocol events independently of
    /// the bounded application-data mailbox. Standalone drivers keep their
    /// supplied combined channel and its event ordering.
    peer_control: Option<mpsc::Sender<(u64, PeerEvent)>>,
    completion: CompletionProgress,
    peer_id: u64,
    cancel: CancellationToken,
    config: PeerDriverConfig,
    setup_deadline: Option<Instant>,
    setup_cancel: Option<CancellationToken>,
    /// Send-side message encoder (`lz4+tcp://`).
    encoder: Option<MessageEncoder>,
    /// Receive-side message decoder. Symmetric to `encoder`.
    decoder: Option<MessageDecoder>,
    /// Direct recv channel. When set, inbound `Event::Message` frames are
    /// pushed straight into the user-facing recv channel without going through
    /// the `SocketDriver` actor's event loop. Only set for socket types where
    /// the recv path is a plain fair-queue delivery with no per-type
    /// post-processing (no `TypeState::post_recv`, no identity-prefix).
    recv_direct: Option<RecvSink>,
    recv_parts: Option<usize>,
    /// Shared pool of raw compression contexts for offloading large-message
    /// compression to blocking threads.
    compression_pool: Option<Arc<CompressionPool>>,
    /// Minimum message `byte_len` to trigger compression offloading.
    offload_threshold: usize,
    /// Per-peer encode slot: the socket handle encodes ZMTP frames into
    /// this slot's `FrameBuffer`, and the driver flushes them to the
    /// wire.
    transmit_slot: Option<Arc<PeerTransmitSlot>>,
    send_pipe_rx: Option<SendPipeConsumer>,
    arena_threshold: usize,
    arena_cap: usize,
    receive_profile: ReceiveProfile,
    /// Keep owned IPC latency drivers runnable briefly after wire progress.
    latency_polling: bool,
    decoded_payload_pool: Option<omq_proto::PayloadPool>,
    recv_ip_rate_limiter: Option<(Arc<SharedIpRateLimiter>, IpAddr)>,
    socket_close_state: Option<Arc<crate::socket::recv::SharedRecvPipe>>,
    #[cfg(target_os = "linux")]
    shm: Option<crate::transport::shm::ShmState>,
}

#[cfg(test)]
mod tests;
