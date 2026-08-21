//! Per-connection driver: one tokio task per live peer connection.

use std::collections::VecDeque;
use std::io;
use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use futures::stream::FuturesOrdered;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::transform::{MessageDecoder, MessageEncoder, TransformedOut};
use omq_proto::proto::{Command, Connection, Event};
use omq_proto::{MessageRateLimit, WorkloadProfile};

use super::compression_pool::CompressionPool;
use super::rate_limit::{SharedIpRateLimiter, TokenBucket};
use super::send_pipe::{SendPipeConsumer, SendPipeProducerHandle};
use super::signal::StateSignal;
use super::transmit_slot::PeerTransmitSlot;
use crate::routing::RepEnvelope;
use crate::socket::dispatch::{AnyReadHalf, AnyStream, AnyWriteHalf};
use crate::socket::recv::RecvItem;
use omq_proto::flow::{DrainBudget, max_batch_bytes};
use omq_proto::frame_buffer::FrameBuffer;

const RECV_SMALL_MSG: usize = 1024;
const RECV_MEDIUM_MSG: usize = 4096;
const RECV_SMALL_BYTES: usize = 64 * 1024;
const RECV_MEDIUM_BYTES: usize = 1024 * 1024;
const RECV_LARGE_BYTES: usize = 1024 * 1024;
const RECV_MEDIUM_TIME: Duration = Duration::from_micros(200);
const RECV_LARGE_TIME: Duration = Duration::from_micros(200);
const RECV_POOL_MAX_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const RECV_POOL_MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReceiveProfile {
    Latency,
    LatencyReq,
    Throughput,
}

/// Stream abstraction allowing production TCP streams to use owned halves.
pub trait DriverStream: Sized {
    type Reader: AsyncRead + Send + Unpin + 'static;
    type Writer: AsyncWrite + Send + Unpin + 'static;

    fn split(self, fast_write: bool) -> (Self::Reader, Self::Writer);
}

impl DriverStream for AnyStream {
    type Reader = AnyReadHalf;
    type Writer = AnyWriteHalf;

    fn split(self, fast_write: bool) -> (Self::Reader, Self::Writer) {
        AnyStream::split(self, fast_write)
    }
}

impl DriverStream for tokio::net::TcpStream {
    type Reader = tokio::net::tcp::OwnedReadHalf;
    type Writer = tokio::net::tcp::OwnedWriteHalf;

    fn split(self, _fast_write: bool) -> (Self::Reader, Self::Writer) {
        self.into_split()
    }
}

impl ReceiveProfile {
    pub(crate) fn from_workload_for_socket(
        profile: WorkloadProfile,
        socket_type: omq_proto::SocketType,
    ) -> Self {
        match (profile, socket_type) {
            (WorkloadProfile::Latency, omq_proto::SocketType::Req) => Self::LatencyReq,
            (WorkloadProfile::Latency, _) => Self::Latency,
            (WorkloadProfile::Throughput, _) => Self::Throughput,
        }
    }

    fn budget(self, msg_bytes: usize) -> DrainBudget {
        match self {
            Self::Latency | Self::LatencyReq => DrainBudget::new(1, 16 * 1024),
            Self::Throughput => {
                let (max_msgs, max_bytes) = if msg_bytes <= RECV_SMALL_MSG {
                    (256, RECV_SMALL_BYTES)
                } else if msg_bytes <= RECV_MEDIUM_MSG {
                    (256, RECV_MEDIUM_BYTES)
                } else {
                    (256, RECV_LARGE_BYTES)
                };
                DrainBudget::new(max_msgs, max_bytes)
            }
        }
    }

    fn time(self, msg_bytes: usize) -> Option<Duration> {
        match self {
            Self::Latency | Self::LatencyReq => None,
            // The small profile already has tight message/byte bounds. Avoid
            // clock reads here; this is the hot path for tiny messages.
            Self::Throughput if msg_bytes <= RECV_SMALL_MSG => None,
            Self::Throughput if msg_bytes <= RECV_MEDIUM_MSG => Some(RECV_MEDIUM_TIME),
            Self::Throughput => Some(RECV_LARGE_TIME),
        }
    }
}

/// Where the driver routes decoded inbound messages.
///
/// `Channel`: push into the shared recv pipe (yring + Mutex).
/// `Yring`: direct push to a per-peer lock-free SPSC ring + external
/// signal, used by omq-libzmq for direct delivery.
#[allow(private_interfaces)]
pub enum RecvSink {
    Channel(Arc<crate::socket::recv::SharedRecvPipe>),
    Yring(YringSink),
    Conflate(Arc<crate::socket::recv::ConflateRecvSlot>),
    Rep(RepRecvSink),
    Server(ServerRecvSink),
}

/// REP's latency receive path: perform identity/envelope handling in the
/// connection driver, before the message reaches the socket actor.
#[derive(Debug)]
pub struct RepRecvSink {
    sink: Box<RecvSink>,
    pending: std::sync::Arc<std::sync::Mutex<VecDeque<(u64, RepEnvelope)>>>,
    peer_id: u64,
}

/// SERVER's direct receive path: attach the connection's opaque routing ID
/// without constructing an identity frame or multipart message.
#[derive(Debug)]
pub struct ServerRecvSink {
    sink: Box<RecvSink>,
    routing_id: u32,
}

/// Yring-based recv sink. Pushes decoded messages directly into a
/// lock-free SPSC ring and signals the consumer via a callback on
/// empty-to-non-empty transitions.
#[allow(private_interfaces)]
pub struct YringSink {
    pub producer: yring::Producer<RecvItem>,
    pub signal: Box<dyn Fn() + Send + Sync>,
    pub space: Arc<StateSignal>,
}

/// Shared config for creating and recycling [`RecvSink::Yring`] instances.
/// The actor refills `slot` with a fresh yring pair on peer disconnect;
/// the external consumer picks up the new consumer from
/// `pending_consumer`.
pub struct RecvSinkConfig {
    slot: std::sync::Mutex<Option<RecvSink>>,
    pending_consumer: std::sync::Mutex<Option<yring::Consumer<RecvItem>>>,
    signal: Arc<dyn Fn() + Send + Sync>,
    space: Arc<StateSignal>,
    cap: usize,
}

impl std::fmt::Debug for RecvSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecvSinkConfig")
            .field("cap", &self.cap)
            .finish_non_exhaustive()
    }
}

impl RecvSinkConfig {
    pub fn new(
        initial_sink: RecvSink,
        signal: Arc<dyn Fn() + Send + Sync>,
        space: Arc<StateSignal>,
        cap: usize,
    ) -> Self {
        Self {
            slot: std::sync::Mutex::new(Some(initial_sink)),
            pending_consumer: std::sync::Mutex::new(None),
            signal,
            space,
            cap,
        }
    }

    /// Create a fresh yring pair. Puts the `RecvSink` in `slot` and the
    /// consumer in `pending_consumer`. No-op if the slot already contains
    /// a sink.
    pub fn refill_sink(&self) {
        let mut guard = self.slot.lock().unwrap();
        if guard.is_some() {
            return;
        }
        let (prod, cons) = yring::spsc(self.cap);
        let f = self.signal.clone();
        *guard = Some(RecvSink::Yring(YringSink {
            producer: prod,
            signal: Box::new(move || f()),
            space: self.space.clone(),
        }));
        *self.pending_consumer.lock().unwrap() = Some(cons);
    }

    pub fn take_sink(&self) -> Option<RecvSink> {
        self.slot.lock().unwrap().take()
    }

    #[allow(private_interfaces)]
    pub fn try_take_pending_consumer(&self) -> Option<yring::Consumer<RecvItem>> {
        self.pending_consumer.try_lock().ok()?.take()
    }

    pub fn notify_space(&self) {
        self.space.notify_changed();
    }
}

impl std::fmt::Debug for RecvSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Channel(pipe) => f.debug_tuple("Channel").field(pipe).finish(),
            Self::Yring(y) => f
                .debug_struct("Yring")
                .field("producer", &y.producer)
                .finish_non_exhaustive(),
            Self::Conflate(_) => f.debug_tuple("Conflate").finish_non_exhaustive(),
            Self::Rep(_) => f.debug_tuple("Rep").finish_non_exhaustive(),
            Self::Server(server) => f
                .debug_struct("Server")
                .field("routing_id", &server.routing_id)
                .finish_non_exhaustive(),
        }
    }
}

impl std::fmt::Debug for YringSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("YringSink")
            .field("producer", &self.producer)
            .finish_non_exhaustive()
    }
}

impl YringSink {
    #[inline]
    fn flush_and_signal(&mut self) {
        self.producer.flush();
        (self.signal)();
    }

    #[inline]
    fn flush_pending(&mut self, pending: &mut bool) {
        if *pending {
            self.flush_and_signal();
            *pending = false;
        }
    }

    async fn send_deferred(&mut self, m: Message, pending: &mut bool) -> bool {
        let mut item = RecvItem::new(m);
        loop {
            match self.producer.push(item) {
                Ok(()) => {
                    *pending = true;
                    return true;
                }
                Err(returned) => {
                    item = returned;
                    self.flush_pending(pending);
                }
            }
            if self.producer.is_consumer_dropped() {
                return false;
            }
            let seen = self.space.generation();
            let changed = self.space.changed_after(seen);
            tokio::pin!(changed);
            match self.producer.push(item) {
                Ok(()) => {
                    *pending = true;
                    return true;
                }
                Err(returned) => {
                    item = returned;
                    tokio::select! {
                        biased;
                        () = changed => {}
                        () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                    }
                }
            }
        }
    }
}

impl RecvSink {
    pub(crate) fn rep(
        sink: RecvSink,
        pending: std::sync::Arc<std::sync::Mutex<VecDeque<(u64, RepEnvelope)>>>,
        peer_id: u64,
    ) -> Self {
        Self::Rep(RepRecvSink {
            sink: Box::new(sink),
            pending,
            peer_id,
        })
    }

    pub(crate) fn server(sink: RecvSink, routing_id: u32) -> Self {
        Self::Server(ServerRecvSink {
            sink: Box::new(sink),
            routing_id,
        })
    }

    fn is_yring(&self) -> bool {
        match self {
            Self::Yring(_) => true,
            Self::Server(server) => server.sink.is_yring(),
            _ => false,
        }
    }

    /// Non-blocking push. Returns the message back if the yring is full.
    /// Channel variant always succeeds (awaits space).
    pub(crate) async fn try_send(&mut self, m: Message) -> Option<Message> {
        if let Self::Server(server) = self {
            let routed = m.with_routing_id(server.routing_id);
            let _ = server.sink.send_plain(routed).await;
            return None;
        }
        self.try_send_plain(m).await
    }

    async fn try_send_plain(&mut self, m: Message) -> Option<Message> {
        match self {
            Self::Channel(pipe) => {
                let _ = pipe.send(m).await;
                None
            }
            Self::Yring(sink) => match sink.producer.push(RecvItem::new(m)) {
                Ok(()) => {
                    sink.flush_and_signal();
                    None
                }
                Err(returned) => Some(returned.message),
            },
            Self::Conflate(slot) => {
                let _ = slot.send_latest(m);
                None
            }
            Self::Rep(_) => unreachable!("REP uses the blocking direct path"),
            Self::Server(_) => unreachable!("nested SERVER sink"),
        }
    }

    async fn send_plain(&mut self, m: Message) -> bool {
        match self {
            Self::Channel(pipe) => pipe.send(m).await.is_ok(),
            Self::Yring(sink) => {
                let mut msg = m;
                loop {
                    if let Err(returned) = sink.producer.push(RecvItem::new(msg)) {
                        msg = returned.message;
                    } else {
                        sink.flush_and_signal();
                        return true;
                    }
                    if sink.producer.is_consumer_dropped() {
                        return false;
                    }
                    let seen = sink.space.generation();
                    let changed = sink.space.changed_after(seen);
                    tokio::pin!(changed);
                    if let Err(returned) = sink.producer.push(RecvItem::new(msg)) {
                        msg = returned.message;
                        tokio::select! {
                            biased;
                            () = changed => {}
                            () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                        }
                        continue;
                    }
                    // Field-level borrows: notified holds sink.space,
                    // but producer and signal are disjoint fields.
                    sink.producer.flush();
                    (sink.signal)();
                    return true;
                }
            }
            Self::Conflate(slot) => slot.send_latest(m),
            Self::Rep(_) | Self::Server(_) => unreachable!("wrapped sink uses routed send"),
        }
    }

    async fn send(&mut self, m: Message) -> bool {
        if let Self::Rep(rep) = self {
            let Some((envelope, body)) = crate::routing::split_rep_request(&m) else {
                return true;
            };
            rep.pending
                .lock()
                .expect("rep pending")
                .push_back((rep.peer_id, envelope));
            return rep.sink.send_plain(body).await;
        }
        if let Self::Server(server) = self {
            let routed = m.with_routing_id(server.routing_id);
            return server.sink.send_plain(routed).await;
        }
        self.send_plain(m).await
    }

    async fn send_with_flush_mode(
        &mut self,
        m: Message,
        defer_yring_flush: bool,
        pending_yring_flush: &mut bool,
    ) -> bool {
        if defer_yring_flush {
            if let Self::Yring(sink) = self {
                return sink.send_deferred(m, pending_yring_flush).await;
            }
            if let Self::Server(server) = self {
                let routed = m.with_routing_id(server.routing_id);
                if let Self::Yring(sink) = server.sink.as_mut() {
                    return sink.send_deferred(routed, pending_yring_flush).await;
                }
                return server.sink.send_plain(routed).await;
            }
        }
        self.send(m).await
    }

    fn flush_deferred(&mut self, pending_yring_flush: &mut bool) {
        if let Self::Yring(sink) = self {
            sink.flush_pending(pending_yring_flush);
        } else if let Self::Server(server) = self
            && let Self::Yring(sink) = server.sink.as_mut()
        {
            sink.flush_pending(pending_yring_flush);
        }
    }
}

/// Batch-encode messages into `FrameBuffer`. Two modes:
///
/// **Direct** (no encoder or offloading disabled): encode each message
/// into `FrameBuffer` inline.
///
/// **Pipelined** (encoder present, offloading enabled): each message
/// enters `FuturesOrdered` as either `spawn_blocking` (large) or
/// `ready()` (small). After the batch loop, drain completed futures
/// front-to-back into EQ.
///
/// Does NOT flush to the writer. Call [`flush_all`] afterwards.
#[expect(clippy::too_many_arguments)]
async fn batch_encode(
    first: &Message,
    mut try_recv: impl FnMut() -> Option<Message>,
    max_msgs: usize,
    encoder: &mut Option<MessageEncoder>,
    connection: &mut Connection,
    eq: &mut FrameBuffer,
    passthrough: Option<&(Bytes, usize)>,
    pool: Option<&Arc<CompressionPool>>,
    threshold: usize,
    pipeline: &mut OffloadPipeline,
) -> Result<usize> {
    let use_pipeline = threshold > 0
        && encoder.as_ref().is_some_and(MessageEncoder::can_offload)
        && pool.is_some();
    if use_pipeline {
        submit_to_pipeline(
            first,
            encoder.as_mut().unwrap(),
            pool.unwrap(),
            threshold,
            pipeline,
        );
    } else {
        encode_msg(first, encoder, connection, eq, passthrough)?;
    }
    let mut count = 1usize;
    let mut bytes = first.byte_len();
    while count < max_msgs && bytes < max_batch_bytes() {
        match try_recv() {
            Some(next) => {
                bytes += next.byte_len();
                if use_pipeline {
                    submit_to_pipeline(
                        &next,
                        encoder.as_mut().unwrap(),
                        pool.unwrap(),
                        threshold,
                        pipeline,
                    );
                } else {
                    encode_msg(&next, encoder, connection, eq, passthrough)?;
                }
                count += 1;
            }
            None => break,
        }
    }
    if use_pipeline {
        drain_pipeline(pipeline, pool, connection, eq).await?;
    }
    Ok(count)
}

const READ_BUF_INITIAL_LATENCY: usize = 4 * 1024;
const READ_BUF_INITIAL_THROUGHPUT: usize = 4 * 1024;
const READ_BUF_MAX: usize = 128 * 1024;
const READ_BUF_GROW_FULL_READS: usize = 2;

use crate::routing::OUTBOUND_BATCH_MAX_MSGS;

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
    /// Queue an application message for send.
    SendMessage(Message),
    /// Pre-encoded wire bytes. Pushed directly into the transmit buffer,
    /// skipping per-message encoding for callers that already have shared
    /// wire chunks.
    SendEncoded(std::sync::Arc<smallvec::SmallVec<[bytes::Bytes; 4]>>),
    /// Queue a ZMTP command for send (SUBSCRIBE, CANCEL, JOIN, LEAVE, ...).
    SendCommand(Command),
    /// Initiate clean shutdown.
    Close,
}

/// Handle returned to callers after spawning a driver. `inbox` delivers
/// commands into the driver; `cancel` requests early teardown.
#[derive(Debug, Clone)]
pub struct PeerDriverHandle {
    pub inbox: mpsc::Sender<PeerDriverCommand>,
    pub cancel: CancellationToken,
    pub(crate) transmit_slot: Option<Arc<PeerTransmitSlot>>,
    pub(crate) direct_tcp_writer: Option<Arc<crate::socket::dispatch::DirectTcpWriter>>,
    pub(crate) send_pipe: Option<SendPipeProducerHandle>,
}

/// What a [`ConnectionDriver`] writes to its shared peer-event
/// channel: either a parsed ZMTP `Event` or a final `Closed` signal
/// emitted just before the driver task exits. Replaces the old
/// per-connection shim task that wrapped Events into the
/// `SocketDriver`'s `InternalEvent::PeerEvent` / `PeerClosed`.
#[derive(Debug)]
pub enum PeerEvent {
    Event(Event),
    Closed { error: Option<String> },
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

struct OutboundState {
    encoder: Option<MessageEncoder>,
    passthrough: Option<(Bytes, usize)>,
    compression_pool: Option<Arc<CompressionPool>>,
    offload_threshold: usize,
    offload_pipeline: OffloadPipeline,
}

impl std::fmt::Debug for OutboundState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutboundState")
            .field("has_encoder", &self.encoder.is_some())
            .field("has_passthrough", &self.passthrough.is_some())
            .field("has_compression_pool", &self.compression_pool.is_some())
            .field("offload_threshold", &self.offload_threshold)
            .field("offload_pipeline_len", &self.offload_pipeline.len())
            .finish()
    }
}

impl OutboundState {
    fn new(
        encoder: Option<MessageEncoder>,
        compression_pool: Option<Arc<CompressionPool>>,
        offload_threshold: usize,
    ) -> Self {
        let passthrough = encoder.as_ref().and_then(MessageEncoder::passthrough_info);
        Self {
            encoder,
            passthrough,
            compression_pool,
            offload_threshold,
            offload_pipeline: FuturesOrdered::new(),
        }
    }

    async fn batch_encode(
        &mut self,
        first: &Message,
        try_recv: impl FnMut() -> Option<Message>,
        max_msgs: usize,
        connection: &mut Connection,
        eq: &mut FrameBuffer,
    ) -> Result<usize> {
        let Self {
            encoder,
            passthrough,
            compression_pool,
            offload_threshold,
            offload_pipeline,
        } = self;
        batch_encode(
            first,
            try_recv,
            max_msgs,
            encoder,
            connection,
            eq,
            passthrough.as_ref(),
            compression_pool.as_ref(),
            *offload_threshold,
            offload_pipeline,
        )
        .await
    }

    fn has_pending_offload(&self) -> bool {
        !self.offload_pipeline.is_empty()
    }

    async fn next_offload(&mut self) -> Option<(Option<MessageEncoder>, Result<TransformedOut>)> {
        use futures::StreamExt;
        self.offload_pipeline.next().await
    }

    fn drain_offload_result(
        &mut self,
        pool_enc: Option<MessageEncoder>,
        frames: Result<TransformedOut>,
        connection: &Connection,
        eq: &mut FrameBuffer,
    ) -> Result<()> {
        drain_offload_result(
            pool_enc,
            frames,
            self.compression_pool.as_ref(),
            connection,
            eq,
        )
    }
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
    connection: Connection,
    inbox: mpsc::Receiver<PeerDriverCommand>,
    /// Shared multi-producer channel feeding the `SocketDriver`'s
    /// per-peer event loop. Each entry is tagged with the `peer_id`
    /// this driver was assigned; the receiver dispatches on that.
    peer_out: mpsc::Sender<(u64, PeerEvent)>,
    peer_id: u64,
    cancel: CancellationToken,
    config: PeerDriverConfig,
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
    recv_ip_rate_limiter: Option<(Arc<SharedIpRateLimiter>, IpAddr)>,
}

impl<T> ConnectionDriver<T>
where
    T: DriverStream,
{
    pub fn new(
        stream: T,
        connection: Connection,
        inbox: mpsc::Receiver<PeerDriverCommand>,
        peer_out: mpsc::Sender<(u64, PeerEvent)>,
        peer_id: u64,
        cancel: CancellationToken,
    ) -> Self {
        Self::with_config(
            stream,
            connection,
            inbox,
            peer_out,
            peer_id,
            cancel,
            PeerDriverConfig::default(),
        )
    }

    pub fn with_config(
        stream: T,
        connection: Connection,
        inbox: mpsc::Receiver<PeerDriverCommand>,
        peer_out: mpsc::Sender<(u64, PeerEvent)>,
        peer_id: u64,
        cancel: CancellationToken,
        config: PeerDriverConfig,
    ) -> Self {
        Self {
            stream,
            connection,
            inbox,
            peer_out,
            peer_id,
            cancel,
            config,
            encoder: None,
            decoder: None,
            recv_direct: None,
            compression_pool: None,
            offload_threshold: 0,
            transmit_slot: None,
            send_pipe_rx: None,
            arena_threshold: omq_proto::frame_buffer::ARENA_THRESHOLD,
            arena_cap: omq_proto::frame_buffer::ARENA_INITIAL_CAP,
            receive_profile: ReceiveProfile::Throughput,
            recv_ip_rate_limiter: None,
        }
    }

    /// Install the send-side encoder. Used by compression transports.
    #[must_use]
    pub fn with_encoder(mut self, encoder: MessageEncoder) -> Self {
        self.encoder = Some(encoder);
        self
    }

    /// Install the receive-side decoder. Used by compression transports.
    #[must_use]
    pub fn with_decoder(mut self, decoder: MessageDecoder) -> Self {
        self.decoder = Some(decoder);
        self
    }

    /// Install the compression offload pool and threshold.
    #[must_use]
    pub(crate) fn with_compression_pool(
        mut self,
        pool: Arc<CompressionPool>,
        threshold: usize,
    ) -> Self {
        self.compression_pool = Some(pool);
        self.offload_threshold = threshold;
        self
    }

    /// Install a direct recv channel. When set, inbound `Event::Message`
    /// frames are pushed straight into the user-facing recv channel, bypassing
    /// the `SocketDriver` actor's event loop. Only valid for socket types
    /// whose recv path is a plain fair-queue delivery with no per-type
    /// post-processing.
    #[must_use]
    pub(crate) fn with_recv_direct(
        mut self,
        pipe: Arc<crate::socket::recv::SharedRecvPipe>,
    ) -> Self {
        self.recv_direct = Some(RecvSink::Channel(pipe));
        self
    }

    /// Install a custom recv sink. The driver pushes decoded messages
    /// into this sink instead of the internal `async_channel`.
    #[must_use]
    pub fn with_recv_sink(mut self, sink: RecvSink) -> Self {
        self.recv_direct = Some(sink);
        self
    }

    /// Install a per-peer encode slot. The socket handle encodes ZMTP
    /// frames into this slot, and the driver flushes them to the wire
    /// via the `data_signal` select arm.
    #[must_use]
    pub(crate) fn with_transmit_slot(mut self, slot: Arc<PeerTransmitSlot>) -> Self {
        self.transmit_slot = Some(slot);
        self
    }

    /// Install a per-peer send pipe. The public socket handle pushes raw
    /// messages into the sender; this driver drains and encodes locally.
    #[must_use]
    pub(crate) fn with_send_pipe(mut self, rx: SendPipeConsumer) -> Self {
        self.send_pipe_rx = Some(rx);
        self
    }

    #[must_use]
    pub(crate) fn with_arena_threshold(mut self, threshold: usize) -> Self {
        self.arena_threshold = threshold;
        self
    }

    #[must_use]
    pub(crate) fn with_arena_cap(mut self, cap: usize) -> Self {
        self.arena_cap = cap;
        self
    }

    pub(crate) fn with_receive_profile(mut self, profile: ReceiveProfile) -> Self {
        self.receive_profile = profile;
        self
    }

    #[must_use]
    pub(crate) fn with_ip_rate_limiter(
        mut self,
        limiter: Arc<SharedIpRateLimiter>,
        ip: IpAddr,
    ) -> Self {
        self.recv_ip_rate_limiter = Some((limiter, ip));
        self
    }

    /// Re-register the stream with the current thread's reactor. Call
    /// at the top of a future spawned on the target IO thread so the
    /// fd is polled by that thread, not the one that accepted/connected.
    pub(crate) fn migrate_stream(mut self) -> io::Result<Self>
    where
        T: crate::socket::dispatch::Migratable,
    {
        self.stream = self.stream.migrate()?;
        Ok(self)
    }

    /// Run the driver to completion. Returns:
    /// - `Ok(())` on clean shutdown (peer EOF, canceled, `Close` command,
    ///   inbox dropped).
    /// - `Err(_)` on protocol violations, I/O errors, or connection errors.
    ///
    /// In every exit path (success or error) the driver sends one final
    /// `PeerEvent::Closed` on the shared peer-event channel so the
    /// `SocketDriver` can clean up its peer entry.
    pub async fn run(self) -> Result<()> {
        let peer_out = self.peer_out.clone();
        let peer_id = self.peer_id;
        let result = self.run_inner_body().await;
        let error = result.as_ref().err().map(close_error_reason);
        let _ = peer_out.send((peer_id, PeerEvent::Closed { error })).await;
        result
    }

    #[expect(clippy::too_many_lines)]
    async fn run_inner_body(self) -> Result<()> {
        let Self {
            stream,
            mut connection,
            mut inbox,
            peer_out,
            peer_id,
            cancel,
            config,
            encoder,
            mut decoder,
            mut recv_direct,
            compression_pool,
            offload_threshold,
            transmit_slot,
            mut send_pipe_rx,
            arena_threshold,
            arena_cap,
            receive_profile,
            recv_ip_rate_limiter,
        } = self;
        let mut recv_rate_limiter = config
            .recv_rate_limit
            .map(|limit| TokenBucket::new(limit, Instant::now()));
        let mut outbound = OutboundState::new(encoder, compression_pool, offload_threshold);
        let latency_profile = !matches!(receive_profile, ReceiveProfile::Throughput);
        let (mut reader, mut writer) = stream.split(latency_profile);
        let mut read_buf_target = if latency_profile {
            READ_BUF_INITIAL_LATENCY
        } else {
            READ_BUF_INITIAL_THROUGHPUT
        };
        let mut read_buf_full_reads = 0usize;
        let mut read_buf = BytesMut::with_capacity(read_buf_target);
        let recv_pool = RecvBufPool::new();
        let mut eq = FrameBuffer::with_config_lazy(arena_threshold, arena_cap);
        let mut drain_buf: Vec<Bytes> = Vec::new();
        let mut arena_buf: Vec<u8> = Vec::new();
        let mut pipe_batch: Vec<Message> = Vec::new();
        let mut last_input = Instant::now();
        let mut handshake_deadline: Option<Instant> = config
            .handshake_timeout
            .and_then(|d| last_input.checked_add(d));
        let hb_interval = config.heartbeat_interval;
        let hb_timeout = config
            .heartbeat_timeout
            .or(config.heartbeat_interval)
            .unwrap_or(Duration::MAX);
        let hb_ttl_deciseconds = config
            .heartbeat_ttl
            .and_then(|d| u16::try_from(d.as_millis() / 100).ok())
            .unwrap_or(0);
        let mut hb_deadline = hb_interval.and_then(|d| Instant::now().checked_add(d));
        let mut hb_ping_sent = false;

        loop {
            if handshake_deadline.is_some() && connection.is_ready() {
                handshake_deadline = None;
            }

            if !emit_connection_events(&mut connection, &peer_out, peer_id).await {
                return Ok(());
            }

            let want_write = connection.has_pending_transmit() || !eq.is_empty();

            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    if let Some(ref slot) = transmit_slot {
                        slot.mark_dead();
                    }
                    return Ok(());
                }

                () = sleep_until_opt(handshake_deadline), if handshake_deadline.is_some() => {
                    return Err(Error::HandshakeFailed("handshake timeout".into()));
                }

                res = reader.read_buf(&mut read_buf), if !connection.is_ready() => {
                    let n = res?;
                    if n == 0 {
                        mark_peer_dead(transmit_slot.as_deref());
                        cancel.cancel();
                        inbox.close();
                        return Ok(());
                    }
                    read_stream_input(
                        n,
                        &mut reader,
                        &mut connection,
                        &mut read_buf,
                        &mut read_buf_target,
                        &mut read_buf_full_reads,
                        &config,
                        &mut last_input,
                        &recv_pool,
                        &peer_out,
                        peer_id,
                    ).await?;
                }

                res = async {
                    flush_frame_buffer(&mut writer, &mut eq, &mut drain_buf).await?;
                    flush_once(&mut writer, &mut connection).await
                }, if want_write => {
                    res?;
                }

                cmd = inbox.recv() => {
                    match handle_pre_activation_inbox_command(
                        cmd,
                        &mut connection,
                    )? {
                        PreActivationStep::Continue => {}
                        PreActivationStep::Activate => break,
                        PreActivationStep::Close => {
                            drain_writes(&mut writer, &mut connection).await.ok();
                            return Ok(());
                        }
                    }
                }
            }
        }

        enable_transmit_slot_after_handshake(transmit_slot.as_deref(), &connection);
        loop {
            if !emit_connection_events(&mut connection, &peer_out, peer_id).await {
                return Ok(());
            }
            match drain_decoded_messages(
                &mut connection,
                &mut decoder,
                receive_profile,
                &mut recv_direct,
                &peer_out,
                peer_id,
                ReceiveRateLimiters {
                    connection: &mut recv_rate_limiter,
                    ip: recv_ip_rate_limiter.as_ref(),
                },
            )
            .await?
            {
                DriverStep::Continue => {}
                DriverStep::Yield => {
                    tokio::task::yield_now().await;
                    continue;
                }
                DriverStep::Close => return Ok(()),
            }

            let want_write = connection.has_pending_transmit() || !eq.is_empty();

            // Latency-routed sends are encoded into the wire slot by
            // the caller. Drain that already-queued work before polling the
            // reader, avoiding an extra zero-time reactor roundtrip.
            if latency_profile && transmit_slot.as_ref().is_some_and(|slot| !slot.is_empty()) {
                drain_transmit_slot(
                    transmit_slot.as_ref().unwrap(),
                    &mut drain_buf,
                    &mut arena_buf,
                    &mut writer,
                )
                .await?;
                continue;
            }

            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    if let Some(ref slot) = transmit_slot {
                        slot.mark_dead();
                    }
                    return Ok(());
                }

                // Latency-routed sends are written by the socket handle into
                // the slot. Poll this wakeup before the reader: otherwise a
                // reply can cause an unnecessary zero-time reactor poll
                // before the next request is written.
                () = async {
                    transmit_slot.as_ref().unwrap().data_signal.ready().await;
                }, if latency_profile && transmit_slot.as_ref().is_some_and(|s| {
                    s.handshake_done.load(Ordering::Acquire)
                }) => {
                    drain_transmit_slot(
                        transmit_slot.as_ref().unwrap(), &mut drain_buf,
                        &mut arena_buf, &mut writer,
                    ).await?;
                }

                res = reader.read_buf(&mut read_buf), if !latency_profile || inbox.is_empty() => {
                    let n = res?;
                    if n == 0 {
                        mark_peer_dead(transmit_slot.as_deref());
                        cancel.cancel();
                        inbox.close();
                        return Ok(());
                    }
                    read_stream_input(
                        n,
                        &mut reader,
                        &mut connection,
                        &mut read_buf,
                        &mut read_buf_target,
                        &mut read_buf_full_reads,
                        &config,
                        &mut last_input,
                        &recv_pool,
                        &peer_out,
                        peer_id,
                    ).await?;
                }

                // Drain completed offloaded compressions and flush.
                Some((pool_enc, frames)) = outbound.next_offload(), if outbound.has_pending_offload() => {
                    outbound.drain_offload_result(pool_enc, frames, &connection, &mut eq)?;
                    flush_all(&mut writer, &mut eq, &mut drain_buf, &mut connection).await?;
                }

                res = async {
                    flush_frame_buffer(&mut writer, &mut eq, &mut drain_buf).await?;
                    flush_once(&mut writer, &mut connection).await
                }, if want_write => {
                    res?;
                }

                cmd = inbox.recv() => {
                    if handle_inbox_command(
                        cmd,
                        &mut inbox,
                        &mut outbound,
                        &mut connection,
                        &mut eq,
                        &mut drain_buf,
                        &mut writer,
                    ).await? == DriverStep::Close {
                        drain_writes(&mut writer, &mut connection).await.ok();
                        return Ok(());
                    }
                },

                // Wire-slot arm: the socket handle encoded ZMTP frames
                // into the per-peer PeerTransmitSlot. Drain and write
                // directly, bypassing the local FrameBuffer.
                () = async {
                    transmit_slot.as_ref().unwrap().data_signal.ready().await;
                }, if !latency_profile && transmit_slot.as_ref().is_some_and(|s| {
                    s.handshake_done.load(Ordering::Acquire)
                }) => {
                    drain_transmit_slot(
                        transmit_slot.as_ref().unwrap(), &mut drain_buf,
                        &mut arena_buf, &mut writer,
                    ).await?;
                },

                // Per-peer send pipe: active round-robin pushes raw
                // messages to this driver, which encodes and writes locally.
                () = async {
                    send_pipe_rx.as_ref().unwrap().ready().await;
                }, if send_pipe_rx.is_some() => {
                    match handle_send_pipe_ready(
                        &mut send_pipe_rx,
                        &mut pipe_batch,
                        &mut outbound,
                        &mut connection,
                        &mut eq,
                        &mut drain_buf,
                        &mut writer,
                    ).await? {
                        DriverStep::Continue | DriverStep::Yield => {}
                        DriverStep::Close => {
                            drain_writes(&mut writer, &mut connection).await.ok();
                            return Ok(());
                        }
                    }
                },

                // Heartbeat tick: enabled only post-handshake when
                // `heartbeat_interval` is set. Uses a persistent pinned
                // sleep so the safety-net timeout doesn't reset it.
                //
                // Only check the timeout after at least one PING has
                // been sent: on unidirectional sockets (PUSH, PUB) the
                // peer has no data to send, so last_input stays at
                // handshake time until the first PONG arrives.
                () = sleep_until_opt(hb_deadline), if hb_deadline.is_some() => {
                    if hb_ping_sent && last_input.elapsed() > hb_timeout {
                        return Err(Error::Timeout);
                    }
                    let ping = Command::Ping {
                        ttl_deciseconds: hb_ttl_deciseconds,
                        context: Bytes::new(),
                    };
                    let _ = connection.send_command(&ping);
                    hb_ping_sent = true;
                    hb_deadline = hb_interval.and_then(|d| Instant::now().checked_add(d));
                }

            }
        }
    }
}

fn close_error_reason(err: &Error) -> String {
    match err {
        Error::HandshakeFailed(reason) => reason.clone(),
        other => other.to_string(),
    }
}

async fn emit_connection_events(
    connection: &mut Connection,
    peer_out: &mpsc::Sender<(u64, PeerEvent)>,
    peer_id: u64,
) -> bool {
    while let Some(ev) = connection.poll_event() {
        if peer_out
            .send((peer_id, PeerEvent::Event(ev)))
            .await
            .is_err()
        {
            return false;
        }
    }
    true
}

async fn emit_connection_events_best_effort(
    connection: &mut Connection,
    peer_out: &mpsc::Sender<(u64, PeerEvent)>,
    peer_id: u64,
) {
    while let Some(ev) = connection.poll_event() {
        let _ = peer_out.send((peer_id, PeerEvent::Event(ev))).await;
    }
}

struct ReceiveRateLimiters<'a> {
    connection: &'a mut Option<TokenBucket>,
    ip: Option<&'a (Arc<SharedIpRateLimiter>, IpAddr)>,
}

async fn drain_decoded_messages(
    connection: &mut Connection,
    decoder: &mut Option<MessageDecoder>,
    receive_profile: ReceiveProfile,
    recv_direct: &mut Option<RecvSink>,
    peer_out: &mpsc::Sender<(u64, PeerEvent)>,
    peer_id: u64,
    rate_limiters: ReceiveRateLimiters<'_>,
) -> Result<DriverStep> {
    let recv_batch_start = Instant::now();
    let mut recv_budget = None;
    let mut recv_batch_time = None;
    let defer_yring_flush = decoder.is_none()
        && matches!(receive_profile, ReceiveProfile::Throughput)
        && recv_direct.as_ref().is_some_and(RecvSink::is_yring);
    let mut pending_yring_flush = false;
    while let Some(m) = connection.poll_message() {
        let m = match decoder.as_mut() {
            Some(dec) => match dec.decode(m)? {
                Some(plain) => plain,
                None => continue,
            },
            None => m,
        };
        let rate_limited = rate_limiters
            .connection
            .as_mut()
            .is_some_and(|limiter| !limiter.allow(recv_batch_start))
            || rate_limiters
                .ip
                .is_some_and(|(limiter, ip)| !limiter.allow(*ip, recv_batch_start));
        if rate_limited {
            flush_deferred_recv(recv_direct, &mut pending_yring_flush);
            return Err(Error::ReceiveRateLimitExceeded);
        }
        let msg_bytes = m.byte_len();
        let budget = recv_budget.get_or_insert_with(|| {
            recv_batch_time = receive_profile.time(msg_bytes);
            receive_profile.budget(msg_bytes)
        });
        if !route_message(
            m,
            recv_direct,
            peer_out,
            peer_id,
            defer_yring_flush,
            &mut pending_yring_flush,
        )
        .await
        {
            flush_deferred_recv(recv_direct, &mut pending_yring_flush);
            return Ok(DriverStep::Close);
        }
        let budget_remains = budget.account(msg_bytes);
        let time_check = budget.msgs().is_multiple_of(32);
        if !budget_remains
            || (time_check
                && recv_batch_time.is_some_and(|limit| recv_batch_start.elapsed() >= limit))
        {
            flush_deferred_recv(recv_direct, &mut pending_yring_flush);
            return Ok(DriverStep::Yield);
        }
    }
    flush_deferred_recv(recv_direct, &mut pending_yring_flush);
    Ok(DriverStep::Continue)
}

fn flush_deferred_recv(recv_direct: &mut Option<RecvSink>, pending_yring_flush: &mut bool) {
    if let Some(sink) = recv_direct {
        sink.flush_deferred(pending_yring_flush);
    }
}

fn enable_transmit_slot_after_handshake(slot: Option<&PeerTransmitSlot>, connection: &Connection) {
    if let Some(slot) = slot
        && connection.is_ready()
        && !slot.handshake_done.load(Ordering::Relaxed)
        && !connection.has_frame_transform()
    {
        slot.handshake_done.store(true, Ordering::Release);
    }
}

fn mark_peer_dead(slot: Option<&PeerTransmitSlot>) {
    if let Some(slot) = slot {
        slot.mark_dead();
    }
}

#[expect(clippy::too_many_arguments)]
async fn read_stream_input<R: AsyncRead + Unpin>(
    n: usize,
    reader: &mut R,
    connection: &mut Connection,
    read_buf: &mut BytesMut,
    read_buf_target: &mut usize,
    read_buf_full_reads: &mut usize,
    config: &PeerDriverConfig,
    last_input: &mut Instant,
    recv_pool: &Arc<RecvBufPool>,
    peer_out: &mpsc::Sender<(u64, PeerEvent)>,
    peer_id: u64,
) -> Result<()> {
    *last_input = Instant::now();
    if n >= *read_buf_target && *read_buf_target < READ_BUF_MAX {
        *read_buf_full_reads += 1;
        if *read_buf_full_reads >= READ_BUF_GROW_FULL_READS {
            *read_buf_target = (*read_buf_target * 2).min(READ_BUF_MAX);
            *read_buf_full_reads = 0;
        }
    } else {
        *read_buf_full_reads = 0;
    }

    let chunk = read_buf.split().freeze();
    read_buf.reserve(read_buf_target.saturating_sub(read_buf.capacity()));
    if let Err(e) = connection.handle_input(chunk) {
        emit_connection_events_best_effort(connection, peer_out, peer_id).await;
        return Err(e);
    }
    handle_large_messages(connection, reader, config, last_input, recv_pool).await
}

fn handle_pre_activation_inbox_command(
    cmd: Option<PeerDriverCommand>,
    connection: &mut Connection,
) -> Result<PreActivationStep> {
    match cmd {
        Some(PeerDriverCommand::ActivateDataPlane) => Ok(PreActivationStep::Activate),
        Some(PeerDriverCommand::SendCommand(c)) => {
            connection.send_command(&c)?;
            Ok(PreActivationStep::Continue)
        }
        Some(PeerDriverCommand::Close) | None => Ok(PreActivationStep::Close),
        Some(PeerDriverCommand::SendMessage(_) | PeerDriverCommand::SendEncoded(_)) => Err(
            Error::Protocol("peer data command before activation".into()),
        ),
    }
}

async fn handle_inbox_command<W: AsyncWrite + Unpin>(
    cmd: Option<PeerDriverCommand>,
    inbox: &mut mpsc::Receiver<PeerDriverCommand>,
    outbound: &mut OutboundState,
    connection: &mut Connection,
    eq: &mut FrameBuffer,
    drain_buf: &mut Vec<Bytes>,
    writer: &mut W,
) -> Result<DriverStep> {
    match cmd {
        Some(PeerDriverCommand::ActivateDataPlane) => Ok(DriverStep::Continue),
        Some(PeerDriverCommand::SendMessage(first)) => {
            // TODO: Give driver control commands an explicit msg/byte/time
            // budget. Current mixed inbox batches data first, then handles
            // controls found after the batch.
            let mut closing = false;
            let mut deferred: SmallVec<[PeerDriverCommand; 4]> = SmallVec::new();
            outbound
                .batch_encode(
                    &first,
                    || match inbox.try_recv() {
                        Ok(PeerDriverCommand::SendMessage(m)) => Some(m),
                        Ok(cmd) => {
                            deferred.push(cmd);
                            None
                        }
                        Err(_) => None,
                    },
                    OUTBOUND_BATCH_MAX_MSGS,
                    connection,
                    eq,
                )
                .await?;
            for cmd in deferred {
                match cmd {
                    PeerDriverCommand::ActivateDataPlane => {}
                    PeerDriverCommand::SendEncoded(chunks) => {
                        eq.push_shared_chunks(&chunks);
                    }
                    PeerDriverCommand::SendCommand(c) => {
                        connection.send_command(&c)?;
                    }
                    PeerDriverCommand::Close => closing = true,
                    PeerDriverCommand::SendMessage(_) => unreachable!(),
                }
            }
            flush_all(writer, eq, drain_buf, connection).await?;
            if closing {
                return Ok(DriverStep::Close);
            }
            Ok(DriverStep::Continue)
        }
        Some(PeerDriverCommand::SendEncoded(chunks)) => {
            eq.push_shared_chunks(&chunks);
            flush_frame_buffer(writer, eq, drain_buf).await?;
            Ok(DriverStep::Continue)
        }
        Some(PeerDriverCommand::SendCommand(c)) => {
            connection.send_command(&c)?;
            Ok(DriverStep::Continue)
        }
        Some(PeerDriverCommand::Close) | None => Ok(DriverStep::Close),
    }
}

async fn handle_send_pipe_ready<W: AsyncWrite + Unpin>(
    send_pipe_rx: &mut Option<SendPipeConsumer>,
    pipe_batch: &mut Vec<Message>,
    outbound: &mut OutboundState,
    connection: &mut Connection,
    eq: &mut FrameBuffer,
    drain_buf: &mut Vec<Bytes>,
    writer: &mut W,
) -> Result<DriverStep> {
    let rx = send_pipe_rx.as_mut().expect("send pipe select guard");
    let drained = rx.drain_into(
        pipe_batch,
        crate::routing::OUTBOUND_BATCH_MAX_MSGS,
        max_batch_bytes(),
    );
    if drained == 0 {
        if rx.is_disconnected() {
            return Ok(DriverStep::Close);
        }
        return Ok(DriverStep::Yield);
    }
    drain_send_pipe_batch(pipe_batch, outbound, connection, eq, drain_buf, writer).await?;
    if send_pipe_rx
        .as_ref()
        .expect("send pipe select guard")
        .is_disconnected()
    {
        return Ok(DriverStep::Close);
    }
    Ok(DriverStep::Continue)
}

/// Sleep until an `Option<Instant>`. Returns immediately if `None`, which
/// paired with a select `if` guard means this branch won't fire.
async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(t) => tokio::time::sleep_until(t.into()).await,
        None => std::future::pending::<()>().await,
    }
}

/// Flush `FrameBuffer` to the writer, then drain any pending connection
/// transmits (command frames queued during encoding).
async fn flush_all<W: AsyncWrite + Unpin>(
    writer: &mut W,
    eq: &mut FrameBuffer,
    drain_buf: &mut Vec<Bytes>,
    connection: &mut Connection,
) -> io::Result<()> {
    flush_frame_buffer(writer, eq, drain_buf).await?;
    while connection.has_pending_transmit() {
        flush_once(writer, connection).await?;
    }
    Ok(())
}

/// Drain the per-peer [`PeerTransmitSlot`] and write directly to the wire.
async fn drain_transmit_slot<W: AsyncWrite + Unpin>(
    slot: &PeerTransmitSlot,
    drain_buf: &mut Vec<Bytes>,
    arena_buf: &mut Vec<u8>,
    writer: &mut W,
) -> io::Result<()> {
    // NOTE: copy arena bytes into a reusable owned buffer before awaiting the
    // write. The slot mutex guards the arena borrow, so writing directly from
    // arena_bytes() would hold the lock across `.await`.
    // Fast path: all content is in the FrameBuffer arena (inline messages).
    // Preserve the arena capacity while releasing the slot lock for IO.
    arena_buf.clear();
    if let Some(drain) = slot.try_drain_arena_only(arena_buf) {
        if !arena_buf.is_empty() {
            writer.write_all(arena_buf).await?;
        }
        if drain.space_available {
            slot.space_available.notify_changed();
        }
        return Ok(());
    }

    let mut budget = DrainBudget::WIRE_DRAIN;
    loop {
        drain_buf.clear();
        let drain = slot.drain(drain_buf, 1024);
        if drain_buf.is_empty() {
            break;
        }
        let chunk_bytes: usize = drain_buf.iter().map(Bytes::len).sum();
        write_chunks(writer, drain_buf).await?;
        if drain.space_available {
            slot.space_available.notify_changed();
        }
        if !budget.account(chunk_bytes) {
            slot.data_signal.reschedule();
            break;
        }
    }
    Ok(())
}

async fn drain_send_pipe_batch<W: AsyncWrite + Unpin>(
    batch: &mut Vec<Message>,
    outbound: &mut OutboundState,
    connection: &mut Connection,
    eq: &mut FrameBuffer,
    drain_buf: &mut Vec<Bytes>,
    writer: &mut W,
) -> Result<()> {
    batch.reverse();
    while let Some(first) = batch.pop() {
        outbound
            .batch_encode(
                &first,
                || batch.pop(),
                OUTBOUND_BATCH_MAX_MSGS,
                connection,
                eq,
            )
            .await?;
        flush_all(writer, eq, drain_buf, connection).await?;
    }
    Ok(())
}

#[derive(Debug)]
struct RecvBufPool {
    inner: std::sync::Mutex<RecvBufPoolInner>,
}

#[derive(Debug, Default)]
struct RecvBufPoolInner {
    buffers: Vec<BytesMut>,
    retained_bytes: usize,
}

impl RecvBufPool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: std::sync::Mutex::new(RecvBufPoolInner::default()),
        })
    }

    fn take(&self, capacity: usize) -> BytesMut {
        let mut pool = self.inner.lock().expect("recv buf pool");
        if let Some(mut buf) = pool.buffers.pop() {
            pool.retained_bytes = pool.retained_bytes.saturating_sub(buf.capacity());
            if buf.capacity() < capacity {
                buf.reserve(capacity - buf.capacity());
            }
            buf.clear();
            return buf;
        }
        BytesMut::with_capacity(capacity)
    }

    fn give(&self, mut buf: BytesMut) {
        let capacity = buf.capacity();
        if capacity > RECV_POOL_MAX_BUFFER_BYTES {
            return;
        }
        buf.clear();
        let mut pool = self.inner.lock().expect("recv buf pool");
        if pool.retained_bytes.saturating_add(capacity) <= RECV_POOL_MAX_RETAINED_BYTES {
            pool.retained_bytes += capacity;
            pool.buffers.push(buf);
        }
    }

    fn wrap(self: &Arc<Self>, buf: BytesMut) -> Bytes {
        Bytes::from_owner(PooledRecvBuf {
            buf,
            pool: Arc::downgrade(self),
        })
    }
}

struct PooledRecvBuf {
    buf: BytesMut,
    pool: Weak<RecvBufPool>,
}

impl AsRef<[u8]> for PooledRecvBuf {
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl Drop for PooledRecvBuf {
    fn drop(&mut self) {
        let buf = std::mem::take(&mut self.buf);
        if let Some(pool) = self.pool.upgrade() {
            pool.give(buf);
        }
    }
}

/// Read large frames directly into pooled owned buffers (bypasses the fixed
/// `read_buf` -> `Connection` buffering path).
async fn handle_large_messages<R: AsyncRead + Unpin>(
    connection: &mut Connection,
    reader: &mut R,
    config: &PeerDriverConfig,
    last_input: &mut Instant,
    recv_pool: &Arc<RecvBufPool>,
) -> Result<()> {
    #[cfg(feature = "ws")]
    let skip_large = connection.is_ws();
    #[cfg(not(feature = "ws"))]
    let skip_large = false;
    if config.large_message_threshold == 0 || connection.has_frame_transform() || skip_large {
        return Ok(());
    }
    while let Some(info) = connection.peek_next_frame_payload_size()? {
        if info.payload_len < config.large_message_threshold {
            break;
        }
        let Some((plen, prefix)) = connection.begin_supplied_payload_with_prefix() else {
            break;
        };
        let payload = if plen <= RECV_POOL_MAX_BUFFER_BYTES {
            let mut buf = recv_pool.take(plen);
            buf.extend_from_slice(prefix.as_slice());
            if buf.len() < plen {
                read_exact_buf(reader, &mut buf, plen).await?;
            }
            debug_assert_eq!(buf.len(), plen);
            recv_pool.wrap(buf)
        } else {
            let mut buf = BytesMut::with_capacity(plen);
            buf.extend_from_slice(prefix.as_slice());
            if buf.len() < plen {
                read_exact_buf(reader, &mut buf, plen).await?;
            }
            debug_assert_eq!(buf.len(), plen);
            buf.freeze()
        };
        *last_input = Instant::now();
        connection.supply_payload(payload)?;
    }
    Ok(())
}

async fn read_exact_buf<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut BytesMut,
    target_len: usize,
) -> io::Result<()> {
    while buf.len() < target_len {
        let remaining = target_len - buf.len();
        let mut limited = (&mut *buf).limit(remaining);
        if reader.read_buf(&mut limited).await? == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
    }
    Ok(())
}

type OffloadPipeline = FuturesOrdered<
    std::pin::Pin<
        Box<
            dyn std::future::Future<Output = (Option<MessageEncoder>, Result<TransformedOut>)>
                + Send,
        >,
    >,
>;

/// Submit one message to the offload pipeline. Large messages (above
/// `threshold`) get `spawn_blocking` via a pool encoder; small messages
/// and pool-exhausted fallbacks are encoded inline on the driver thread.
#[allow(unused_variables)]
fn submit_to_pipeline(
    msg: &Message,
    encoder: &mut MessageEncoder,
    pool: &Arc<CompressionPool>,
    threshold: usize,
    pipeline: &mut OffloadPipeline,
) {
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    if msg.byte_len() >= threshold
        && let Some(mut pool_enc) = pool.try_take(encoder)
    {
        let msg = msg.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let result = pool_enc.encode(&msg);
            (Some(pool_enc), result)
        });
        pipeline.push_back(Box::pin(async move {
            match handle.await {
                Ok(pair) => pair,
                Err(_) => (
                    None,
                    Err(Error::Protocol("compression offload task panicked".into())),
                ),
            }
        }));
        return;
    }
    let result = encoder.encode(msg);
    pipeline.push_back(Box::pin(futures::future::ready((None, result))));
}

/// Drain all completed futures from the pipeline into `FrameBuffer`.
async fn drain_pipeline(
    pipeline: &mut OffloadPipeline,
    pool: Option<&Arc<CompressionPool>>,
    connection: &Connection,
    eq: &mut FrameBuffer,
) -> Result<()> {
    use futures::StreamExt;
    while let Some((pool_enc, frames)) = pipeline.next().await {
        drain_offload_result(pool_enc, frames, pool, connection, eq)?;
    }
    Ok(())
}

#[allow(unused_variables, clippy::needless_pass_by_value)]
fn drain_offload_result(
    pool_enc: Option<MessageEncoder>,
    frames: Result<TransformedOut>,
    pool: Option<&Arc<CompressionPool>>,
    connection: &Connection,
    eq: &mut FrameBuffer,
) -> Result<()> {
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    if let (Some(enc), Some(pool)) = (pool_enc, pool) {
        pool.put(enc);
    }
    #[cfg(feature = "ws")]
    let ws = connection.is_ws().then(|| {
        matches!(
            connection.ws_role(),
            Some(omq_proto::proto::connection::WsRole::Client)
        )
    });
    for wire in frames? {
        #[cfg(feature = "ws")]
        if let Some(masked) = ws {
            eq.frame_ws(&wire, masked);
            continue;
        }
        eq.frame(&wire);
    }
    Ok(())
}

/// Encode one message into `FrameBuffer`. When a compression encoder
/// is active, the message is transformed first; the resulting wire
/// message(s) are then framed into EQ. When no encoder is present the
/// message is framed directly. Sub-threshold messages on compression
/// transports take a sentinel-prefix fast path that avoids the encoder
/// entirely.
///
/// The only path that still goes through `connection.send_message` is when a
/// frame-level transform (CURVE) is active, since those
/// encrypt at the ZMTP frame layer and need the connection's internal state.
fn encode_msg(
    msg: &Message,
    encoder: &mut Option<MessageEncoder>,
    connection: &mut Connection,
    eq: &mut FrameBuffer,
    passthrough: Option<&(Bytes, usize)>,
) -> Result<()> {
    #[cfg(feature = "ws")]
    if connection.is_ws() && !connection.has_frame_transform() {
        let masked = matches!(
            connection.ws_role(),
            Some(omq_proto::proto::connection::WsRole::Client)
        );
        if let Some(enc) = encoder.as_mut() {
            for wire in enc.encode(msg)? {
                eq.frame_ws(&wire, masked);
            }
        } else {
            eq.frame_ws(msg, masked);
        }
        return Ok(());
    }
    if connection.has_frame_transform() {
        if let Some(enc) = encoder.as_mut() {
            for wire in enc.encode(msg)? {
                connection.send_message(&wire)?;
            }
        } else {
            connection.send_message(msg)?;
        }
        return Ok(());
    }
    if let Some((sentinel, threshold)) = passthrough
        && msg.iter().all(|b| b.len() < *threshold)
    {
        eq.frame_prefixed(sentinel, msg);
    } else if let Some(enc) = encoder.as_mut() {
        for wire in enc.encode(msg)? {
            eq.frame(&wire);
        }
    } else {
        eq.frame(msg);
    }
    Ok(())
}

/// Route a decoded message to `recv_direct` or through the actor.
/// Returns `true` if sent, `false` if the receiving channel closed.
async fn route_message(
    m: Message,
    recv_direct: &mut Option<RecvSink>,
    peer_out: &mpsc::Sender<(u64, PeerEvent)>,
    peer_id: u64,
    defer_yring_flush: bool,
    pending_yring_flush: &mut bool,
) -> bool {
    match recv_direct {
        Some(sink) => {
            sink.send_with_flush_mode(m, defer_yring_flush, pending_yring_flush)
                .await
        }
        None => peer_out
            .send((peer_id, PeerEvent::Event(Event::Message(m))))
            .await
            .is_ok(),
    }
}

/// Flush the `FrameBuffer` to the writer. Drains chunks into a
/// reusable `Vec<Bytes>`, builds `IoSlice` refs, and does one
/// `write_vectored`. On partial write, unwritten chunks are restored
/// to the queue front.
pub(crate) async fn flush_frame_buffer<W>(
    writer: &mut W,
    eq: &mut FrameBuffer,
    drain_buf: &mut Vec<Bytes>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if eq.has_arena_only() {
        loop {
            let len = eq.arena_bytes().len();
            if len == 0 {
                return Ok(());
            }
            let n = {
                let data = eq.arena_bytes();
                writer.write_vectored(&[io::IoSlice::new(data)]).await?
            };
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
            }
            eq.advance_arena(n);
        }
    }

    loop {
        drain_buf.clear();
        eq.drain(drain_buf, 1024);
        if drain_buf.is_empty() {
            return Ok(());
        }
        let total: usize = drain_buf.iter().map(Bytes::len).sum();
        let iovecs: SmallVec<[io::IoSlice<'_>; 64]> =
            drain_buf.iter().map(|b| io::IoSlice::new(b)).collect();
        let n = writer.write_vectored(&iovecs).await?;
        drop(iovecs);
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
        }
        if n < total {
            let drained = std::mem::take(drain_buf);
            eq.put_back_unwritten(drained, n);
        }
    }
}

async fn write_chunks<W>(writer: &mut W, chunks: &mut Vec<Bytes>) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut remaining: usize = chunks.iter().map(Bytes::len).sum();
    while remaining > 0 {
        let iovecs: SmallVec<[io::IoSlice<'_>; 64]> =
            chunks.iter().map(|b| io::IoSlice::new(b)).collect();
        let n = writer.write_vectored(&iovecs).await?;
        drop(iovecs);
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
        }
        remaining -= n;
        if remaining == 0 {
            chunks.clear();
        } else {
            let mut skip = n;
            let mut first_kept = 0;
            for (i, chunk) in chunks.iter().enumerate() {
                if skip >= chunk.len() {
                    skip -= chunk.len();
                    first_kept = i + 1;
                } else {
                    break;
                }
            }
            chunks.drain(..first_kept);
            if skip > 0 && !chunks.is_empty() {
                chunks[0] = chunks[0].slice(skip..);
            }
        }
    }
    Ok(())
}

/// One write attempt. Uses `write_vectored` so multi-chunk frame
/// payloads (compression sentinels, CURVE nonces, etc.) hit the kernel
/// as a single gather-write - no userspace memcpy. Partial writes are
/// fine; we loop and try again.
async fn flush_once<W>(writer: &mut W, connection: &mut Connection) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let chunks = connection.transmit_chunks_capped(128);
    if chunks.is_empty() {
        return Ok(());
    }
    let n = writer.write_vectored(&chunks).await?;
    drop(chunks);
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
    }
    connection.advance_transmit(n);
    Ok(())
}

/// Best-effort flush of remaining outbound bytes on shutdown.
async fn drain_writes<W>(writer: &mut W, connection: &mut Connection) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    while connection.has_pending_transmit() {
        flush_once(writer, connection).await?;
    }
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{DuplexStream, ReadBuf};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use omq_proto::proto::connection::{ConnectionConfig, Role};
    use omq_proto::proto::{Event, SocketType};

    impl DriverStream for DuplexStream {
        type Reader = tokio::io::ReadHalf<Self>;
        type Writer = tokio::io::WriteHalf<Self>;

        fn split(self, _fast_write: bool) -> (Self::Reader, Self::Writer) {
            tokio::io::split(self)
        }
    }

    #[derive(Debug)]
    struct ChoppyDuplex {
        inner: DuplexStream,
        read_cap: usize,
        write_cap: usize,
    }

    impl ChoppyDuplex {
        fn new(inner: DuplexStream, read_cap: usize, write_cap: usize) -> Self {
            Self {
                inner,
                read_cap: read_cap.max(1),
                write_cap: write_cap.max(1),
            }
        }
    }

    impl DriverStream for ChoppyDuplex {
        type Reader = ChoppyReader<tokio::io::ReadHalf<DuplexStream>>;
        type Writer = ChoppyWriter<tokio::io::WriteHalf<DuplexStream>>;

        fn split(self, _fast_write: bool) -> (Self::Reader, Self::Writer) {
            let (reader, writer) = tokio::io::split(self.inner);
            (
                ChoppyReader {
                    inner: reader,
                    cap: self.read_cap,
                },
                ChoppyWriter {
                    inner: writer,
                    cap: self.write_cap,
                },
            )
        }
    }

    #[derive(Debug)]
    struct ChoppyReader<R> {
        inner: R,
        cap: usize,
    }

    impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for ChoppyReader<R> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            let mut scratch = vec![0u8; self.cap.min(buf.remaining())];
            let mut limited = ReadBuf::new(&mut scratch);
            match Pin::new(&mut self.inner).poll_read(cx, &mut limited) {
                Poll::Ready(Ok(())) => {
                    buf.put_slice(limited.filled());
                    Poll::Ready(Ok(()))
                }
                other => other,
            }
        }
    }

    #[derive(Debug)]
    struct ChoppyWriter<W> {
        inner: W,
        cap: usize,
    }

    impl<W: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for ChoppyWriter<W> {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let n = self.cap.min(buf.len());
            Pin::new(&mut self.inner).poll_write(cx, &buf[..n])
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let mut remaining = self.cap;
            let mut limited: SmallVec<[io::IoSlice<'_>; 64]> = SmallVec::new();
            for buf in bufs {
                if remaining == 0 {
                    break;
                }
                let n = remaining.min(buf.len());
                if n > 0 {
                    limited.push(io::IoSlice::new(&buf[..n]));
                    remaining -= n;
                }
            }
            Pin::new(&mut self.inner).poll_write_vectored(cx, &limited)
        }

        fn is_write_vectored(&self) -> bool {
            self.inner.is_write_vectored()
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    #[test]
    fn latency_receive_profile_drains_one_message_without_timer() {
        for profile in [ReceiveProfile::Latency, ReceiveProfile::LatencyReq] {
            let mut budget = profile.budget(16);
            assert!(!budget.exhausted());
            assert!(!budget.account(16));
            assert!(budget.exhausted());
            assert_eq!(profile.time(16), None);
        }
    }

    #[test]
    fn yring_sink_signals_every_flush_even_when_nonempty() {
        let (producer, _consumer) = yring::spsc(4);
        let signals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let signals_for_sink = signals.clone();
        let mut sink = YringSink {
            producer,
            signal: Box::new(move || {
                signals_for_sink.fetch_add(1, Ordering::Relaxed);
            }),
            space: Arc::new(StateSignal::new()),
        };

        assert!(matches!(
            sink.producer.push(RecvItem::new(Message::single("a"))),
            Ok(())
        ));
        sink.flush_and_signal();
        assert!(matches!(
            sink.producer.push(RecvItem::new(Message::single("b"))),
            Ok(())
        ));
        sink.flush_and_signal();

        assert_eq!(signals.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn recv_buf_pool_reuses_bounded_buffers() {
        let pool = RecvBufPool::new();
        let mut buf = pool.take(256 * 1024);
        buf.extend_from_slice(&[7; 1024]);
        let capacity = buf.capacity();
        pool.give(buf);

        assert_eq!(pool.inner.lock().unwrap().buffers.len(), 1);
        let reused = pool.take(128 * 1024);
        assert_eq!(reused.len(), 0);
        assert!(reused.capacity() >= capacity);
        assert_eq!(pool.inner.lock().unwrap().retained_bytes, 0);
    }

    #[test]
    fn recv_buf_pool_does_not_retain_huge_buffers() {
        let pool = RecvBufPool::new();
        pool.give(BytesMut::with_capacity(RECV_POOL_MAX_BUFFER_BYTES + 1));
        let guard = pool.inner.lock().unwrap();
        assert_eq!(guard.buffers.len(), 0);
        assert_eq!(guard.retained_bytes, 0);
    }

    #[test]
    fn recv_buf_pool_caps_total_retained_bytes() {
        const BUFFER_BYTES: usize = 1024 * 1024;

        let pool = RecvBufPool::new();
        let buffer_count = (RECV_POOL_MAX_RETAINED_BYTES / BUFFER_BYTES) + 2;
        for _ in 0..buffer_count {
            pool.give(BytesMut::with_capacity(BUFFER_BYTES));
        }
        let guard = pool.inner.lock().unwrap();
        assert!(guard.retained_bytes <= RECV_POOL_MAX_RETAINED_BYTES);
        assert_eq!(
            guard.buffers.len(),
            RECV_POOL_MAX_RETAINED_BYTES / BUFFER_BYTES
        );
    }

    #[test]
    fn recv_buf_pool_waits_for_last_bytes_clone() {
        let pool = RecvBufPool::new();
        let mut buf = pool.take(4 * 1024 * 1024);
        buf.extend_from_slice(b"payload");
        let payload = pool.wrap(buf);
        let clone = payload.clone();

        drop(payload);
        assert_eq!(pool.inner.lock().unwrap().buffers.len(), 0);

        drop(clone);
        let guard = pool.inner.lock().unwrap();
        assert_eq!(guard.buffers.len(), 1);
        assert!(guard.retained_bytes >= 4 * 1024 * 1024);
    }

    #[test]
    fn recv_buf_pool_does_not_outlive_connection_owner() {
        let pool = RecvBufPool::new();
        let weak = Arc::downgrade(&pool);
        let payload = pool.wrap(pool.take(4 * 1024 * 1024));

        drop(pool);
        assert!(weak.upgrade().is_none());

        drop(payload);
    }

    #[test]
    fn recv_buf_pool_accepts_concurrent_last_owner_drops() {
        const BUFFER_COUNT: usize = 16;
        const BUFFER_BYTES: usize = 1024 * 1024;

        let pool = RecvBufPool::new();
        let payloads = (0..BUFFER_COUNT)
            .map(|_| pool.wrap(pool.take(BUFFER_BYTES)))
            .collect::<Vec<_>>();

        std::thread::scope(|scope| {
            for payload in payloads {
                scope.spawn(move || drop(payload));
            }
        });

        let guard = pool.inner.lock().unwrap();
        assert_eq!(guard.buffers.len(), BUFFER_COUNT);
        assert_eq!(guard.retained_bytes, BUFFER_COUNT * BUFFER_BYTES);
    }

    #[derive(Debug)]
    struct PartialVectoredWriter {
        out: Vec<u8>,
        first_cap: usize,
        next_cap: usize,
        writes: usize,
    }

    impl PartialVectoredWriter {
        fn new(first_cap: usize, next_cap: usize) -> Self {
            Self {
                out: Vec::new(),
                first_cap,
                next_cap,
                writes: 0,
            }
        }
    }

    impl tokio::io::AsyncWrite for PartialVectoredWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let cap = if self.writes == 0 {
                self.first_cap
            } else {
                self.next_cap
            };
            let n = cap.min(buf.len());
            if n > 0 {
                self.out.extend_from_slice(&buf[..n]);
            }
            self.writes += 1;
            Poll::Ready(Ok(n))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let total = bufs.iter().map(|buf| buf.len()).sum::<usize>();
            let cap = if self.writes == 0 {
                self.first_cap
            } else {
                self.next_cap
            };
            let mut remaining = cap.min(total);
            for buf in bufs {
                if remaining == 0 {
                    break;
                }
                let n = remaining.min(buf.len());
                self.out.extend_from_slice(&buf[..n]);
                remaining -= n;
            }
            self.writes += 1;
            Poll::Ready(Ok(cap.min(total)))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Debug)]
    struct ScriptedVectoredWriter {
        out: Vec<u8>,
        caps: VecDeque<usize>,
        fallback_cap: usize,
    }

    impl ScriptedVectoredWriter {
        fn new(caps: impl IntoIterator<Item = usize>, fallback_cap: usize) -> Self {
            Self {
                out: Vec::new(),
                caps: caps.into_iter().collect(),
                fallback_cap,
            }
        }
    }

    impl tokio::io::AsyncWrite for ScriptedVectoredWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let cap = self.caps.pop_front().unwrap_or(self.fallback_cap).max(1);
            let n = cap.min(buf.len());
            self.out.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let total = bufs.iter().map(|buf| buf.len()).sum::<usize>();
            let cap = self.caps.pop_front().unwrap_or(self.fallback_cap).max(1);
            let mut remaining = cap.min(total);
            for buf in bufs {
                if remaining == 0 {
                    break;
                }
                let n = remaining.min(buf.len());
                self.out.extend_from_slice(&buf[..n]);
                remaining -= n;
            }
            Poll::Ready(Ok(cap.min(total)))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Debug)]
    struct ScriptedReader {
        data: Bytes,
        pos: usize,
        chunks: VecDeque<usize>,
    }

    impl ScriptedReader {
        fn new(data: Bytes, chunks: impl IntoIterator<Item = usize>) -> Self {
            Self {
                data,
                pos: 0,
                chunks: chunks.into_iter().collect(),
            }
        }
    }

    impl tokio::io::AsyncRead for ScriptedReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.pos >= self.data.len() {
                return Poll::Ready(Ok(()));
            }
            let chunk_cap = self.chunks.pop_front().unwrap_or(usize::MAX);
            let n = chunk_cap
                .min(buf.remaining())
                .min(self.data.len() - self.pos);
            let end = self.pos + n;
            buf.put_slice(&self.data[self.pos..end]);
            self.pos = end;
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Debug)]
    struct UninitProbeReader {
        inner: ScriptedReader,
    }

    impl tokio::io::AsyncRead for UninitProbeReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            assert!(
                buf.initialized().is_empty(),
                "large recv path must fill uninitialized spare capacity"
            );
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    #[tokio::test]
    async fn flush_frame_buffer_preserves_large_payload_after_partial_write() {
        const MSG_SIZE: usize = 1024 * 1024;
        let payload = (0..MSG_SIZE).map(|i| (i & 0xFF) as u8).collect::<Vec<_>>();
        let mut eq = FrameBuffer::one_shot();
        eq.frame(&Message::single(Bytes::from(payload.clone())));

        let mut drain_buf = Vec::new();
        let mut writer = PartialVectoredWriter::new(9 + 632_554, 65_537);
        flush_frame_buffer(&mut writer, &mut eq, &mut drain_buf)
            .await
            .unwrap();

        assert!(eq.is_empty());
        assert_eq!(writer.out.len(), 9 + MSG_SIZE);
        assert_eq!(writer.out[0], 0x02);
        assert_eq!(
            u64::from_be_bytes(writer.out[1..9].try_into().unwrap()),
            MSG_SIZE as u64
        );
        assert_eq!(&writer.out[9..], &payload);
    }

    #[tokio::test]
    async fn flush_frame_buffer_preserves_large_payloads_after_partial_write_matrix() {
        const MSG_SIZE: usize = 1024 * 1024;
        const CAPS: &[(usize, usize)] = &[
            (1, 16_384),
            (8, 16_384),
            (9, 16_384),
            (9 + 632_554, 65_537),
            (9 + 632_558, 65_537),
            (9 + 632_562, 65_537),
            (9 + MSG_SIZE - 4, 4_097),
            (9 + MSG_SIZE, 4_097),
        ];

        let payloads = (0..3)
            .map(|seq| patterned_payload(MSG_SIZE, seq))
            .collect::<Vec<_>>();
        let mut expected = Vec::new();
        for payload in &payloads {
            expected.push(0x02);
            expected.extend_from_slice(&(MSG_SIZE as u64).to_be_bytes());
            expected.extend_from_slice(payload);
        }

        for &(first_cap, next_cap) in CAPS {
            let mut eq = FrameBuffer::one_shot();
            for payload in &payloads {
                eq.frame(&Message::single(Bytes::copy_from_slice(payload)));
            }

            let mut drain_buf = Vec::new();
            let mut writer = PartialVectoredWriter::new(first_cap, next_cap);
            flush_frame_buffer(&mut writer, &mut eq, &mut drain_buf)
                .await
                .unwrap();

            assert!(eq.is_empty());
            assert_eq!(
                writer.out, expected,
                "first_cap={first_cap}, next_cap={next_cap}"
            );
        }
    }

    #[tokio::test]
    async fn flush_frame_buffer_preserves_large_payloads_after_scripted_partial_writes() {
        const MSG_SIZE: usize = 1024 * 1024;
        let payloads = (0..4)
            .map(|seq| patterned_payload(MSG_SIZE, seq))
            .collect::<Vec<_>>();
        let mut expected = Vec::new();
        for payload in &payloads {
            expected.push(0x02);
            expected.extend_from_slice(&(MSG_SIZE as u64).to_be_bytes());
            expected.extend_from_slice(payload);
        }

        let mut eq = FrameBuffer::one_shot();
        for payload in &payloads {
            eq.frame(&Message::single(Bytes::copy_from_slice(payload)));
        }

        let caps = [
            1,
            8,
            9,
            31,
            4_095,
            4_097,
            65_535,
            65_537,
            9 + 632_554,
            9 + 632_558,
            9 + 632_562,
            131_071,
            262_147,
        ];
        let mut drain_buf = Vec::new();
        let mut writer = ScriptedVectoredWriter::new(caps, 17_003);
        flush_frame_buffer(&mut writer, &mut eq, &mut drain_buf)
            .await
            .unwrap();

        assert!(eq.is_empty());
        assert_eq!(writer.out, expected);
    }

    #[tokio::test]
    async fn flush_frame_buffer_matches_reference_under_random_partial_writes() {
        const LENGTHS: &[usize] = &[
            16,
            62,
            63,
            254,
            255,
            256,
            4095,
            4096,
            8191,
            65_535,
            65_536,
            131_073,
            632_558,
            1024 * 1024,
        ];

        for case in 0..48u64 {
            let mut seed = 0xA5A5_5A5A_D3C1_BEEF ^ case;
            let mut eq = FrameBuffer::one_shot();
            let mut expected = Vec::new();
            for seq in 0..12u64 {
                let len = LENGTHS[next_random(&mut seed) % LENGTHS.len()];
                let payload = patterned_payload(len, (case << 8) | seq);
                push_expected_single_frame(&mut expected, &payload);
                eq.frame(&Message::single(Bytes::from(payload)));
            }

            let caps = (0..192)
                .map(|_| match next_random(&mut seed) % 12 {
                    0 => 1,
                    1 => 2,
                    2 => 8,
                    3 => 9,
                    4 => 17,
                    5 => 4_095,
                    6 => 4_097,
                    7 => 65_535,
                    8 => 65_537,
                    9 => 9 + 632_558,
                    10 => 9 + 1024 * 1024 - 4,
                    _ => (next_random(&mut seed) % 262_147) + 1,
                })
                .collect::<Vec<_>>();

            let mut drain_buf = Vec::new();
            let mut writer = ScriptedVectoredWriter::new(caps, 37_111);
            flush_frame_buffer(&mut writer, &mut eq, &mut drain_buf)
                .await
                .unwrap();

            assert!(eq.is_empty(), "case={case}");
            assert_eq!(writer.out, expected, "case={case}");
        }
    }

    #[tokio::test]
    async fn reused_lazy_frame_buffer_matches_reference_under_partial_writes() {
        const LENGTHS: &[usize] = &[4_095, 4_096, 65_537, 632_558, 1024 * 1024];
        let caps = [1, 8, 9, 4_097, 65_537, 9 + 632_554, 9 + 632_558, 262_147];
        let mut eq = FrameBuffer::with_config_lazy(
            omq_proto::frame_buffer::ARENA_THRESHOLD,
            omq_proto::frame_buffer::ARENA_INITIAL_CAP,
        );
        let mut drain_buf = Vec::new();
        let mut writer = ScriptedVectoredWriter::new(caps, 23_011);
        let mut expected = Vec::new();

        for seq in 0..64u64 {
            let len = LENGTHS[seq as usize % LENGTHS.len()];
            let payload = patterned_payload(len, seq);
            push_expected_single_frame(&mut expected, &payload);
            eq.frame(&Message::single(Bytes::from(payload)));
            flush_frame_buffer(&mut writer, &mut eq, &mut drain_buf)
                .await
                .unwrap();
            assert!(eq.is_empty(), "seq={seq}");
        }

        assert_eq!(writer.out, expected);
    }

    #[tokio::test]
    async fn write_chunks_preserves_large_payloads_after_scripted_partial_writes() {
        const MSG_SIZE: usize = 1024 * 1024;
        let payloads = (0..3)
            .map(|seq| patterned_payload(MSG_SIZE, seq))
            .collect::<Vec<_>>();
        let mut expected = Vec::new();
        let mut chunks = Vec::new();
        for payload in &payloads {
            let mut header = Vec::with_capacity(9);
            header.push(0x02);
            header.extend_from_slice(&(MSG_SIZE as u64).to_be_bytes());
            expected.extend_from_slice(&header);
            expected.extend_from_slice(payload);
            chunks.push(Bytes::from(header));
            chunks.push(Bytes::copy_from_slice(payload));
        }

        let caps = [
            1,
            8,
            9,
            17,
            4_097,
            9 + 632_558,
            65_537,
            262_147,
            9 + MSG_SIZE - 4,
        ];
        let mut writer = ScriptedVectoredWriter::new(caps, 23_011);
        write_chunks(&mut writer, &mut chunks).await.unwrap();

        assert!(chunks.is_empty());
        assert_eq!(writer.out, expected);
    }

    #[tokio::test]
    async fn large_message_direct_read_preserves_buffered_prefix() {
        const MSG_SIZE: usize = 1024 * 1024;
        const PREFIX_PAYLOAD_BYTES: usize = 632_558;

        let (mut push, mut pull) = ready_push_pull_connections();
        let payload = (0..MSG_SIZE).map(|i| (i & 0xFF) as u8).collect::<Vec<_>>();
        push.send_message(&Message::single(Bytes::from(payload.clone())))
            .unwrap();
        let wire = Bytes::from(drain_transmit(&mut push));
        let prefix_wire_bytes = 9 + PREFIX_PAYLOAD_BYTES;

        pull.handle_input(wire.slice(..prefix_wire_bytes)).unwrap();
        let mut reader = ScriptedReader::new(
            wire.slice(prefix_wire_bytes..),
            [3, 1, 65_537, 8_191, 262_147],
        );
        let config = PeerDriverConfig {
            large_message_threshold: 128 * 1024,
            ..PeerDriverConfig::default()
        };
        let mut last_input = Instant::now();

        handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
            .await
            .unwrap();

        let msg = pull.poll_message().expect("large message decoded");
        assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
        assert_eq!(reader.pos, reader.data.len());
    }

    #[tokio::test]
    async fn large_message_direct_read_preserves_fragmented_buffered_prefix() {
        const MSG_SIZE: usize = 1024 * 1024;
        const PREFIX_PAYLOAD_BYTES: usize = 632_558;

        let (mut push, mut pull) = ready_push_pull_connections();
        let payload = patterned_payload(MSG_SIZE, 42);
        push.send_message(&Message::single(Bytes::from(payload.clone())))
            .unwrap();
        let wire = Bytes::from(drain_transmit(&mut push));
        let prefix_wire_bytes = 9 + PREFIX_PAYLOAD_BYTES;

        pull.handle_input(wire.slice(..5)).unwrap();
        pull.handle_input(wire.slice(5..17)).unwrap();
        pull.handle_input(wire.slice(17..prefix_wire_bytes))
            .unwrap();

        let mut reader = ScriptedReader::new(
            wire.slice(prefix_wire_bytes..),
            [4, 3, 1, 65_537, 8_191, 262_147],
        );
        let config = PeerDriverConfig {
            large_message_threshold: 128 * 1024,
            ..PeerDriverConfig::default()
        };
        let mut last_input = Instant::now();

        handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
            .await
            .unwrap();

        let msg = pull.poll_message().expect("large message decoded");
        assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
        assert_eq!(reader.pos, reader.data.len());
    }

    #[tokio::test]
    async fn large_message_direct_read_preserves_many_chunk_buffered_prefix() {
        const MSG_SIZE: usize = 1024 * 1024;
        const PREFIX_PAYLOAD_BYTES: usize = 632_558;

        let (mut push, mut pull) = ready_push_pull_connections();
        let payload = patterned_payload(MSG_SIZE, 43);
        push.send_message(&Message::single(Bytes::from(payload.clone())))
            .unwrap();
        let wire = Bytes::from(drain_transmit(&mut push));
        let prefix_wire_bytes = 9 + PREFIX_PAYLOAD_BYTES;

        feed_input_in_chunks(
            &mut pull,
            &wire,
            prefix_wire_bytes,
            [4096, 8192, 16_384, 32_768, 65_536, 131_072],
        );

        let mut reader = ScriptedReader::new(
            wire.slice(prefix_wire_bytes..),
            [4, 3, 1, 65_537, 8_191, 262_147],
        );
        let config = PeerDriverConfig {
            large_message_threshold: 128 * 1024,
            ..PeerDriverConfig::default()
        };
        let mut last_input = Instant::now();

        handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
            .await
            .unwrap();

        let msg = pull.poll_message().expect("large message decoded");
        assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
        assert_eq!(reader.pos, reader.data.len());
    }

    #[tokio::test]
    async fn large_message_direct_read_uses_uninitialized_spare_capacity() {
        const MSG_SIZE: usize = RECV_POOL_MAX_BUFFER_BYTES + 1024;
        const PREFIX_PAYLOAD_BYTES: usize = 64;

        let (mut push, mut pull) = ready_push_pull_connections();
        let payload = patterned_payload(MSG_SIZE, 45);
        push.send_message(&Message::single(Bytes::from(payload.clone())))
            .unwrap();
        let wire = Bytes::from(drain_transmit(&mut push));
        let prefix_wire_bytes = 9 + PREFIX_PAYLOAD_BYTES;

        pull.handle_input(wire.slice(..prefix_wire_bytes)).unwrap();
        let mut reader = UninitProbeReader {
            inner: ScriptedReader::new(wire.slice(prefix_wire_bytes..), [128, 4093, 65_537]),
        };
        let config = PeerDriverConfig {
            large_message_threshold: 128 * 1024,
            ..PeerDriverConfig::default()
        };
        let mut last_input = Instant::now();

        handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
            .await
            .unwrap();

        let msg = pull.poll_message().expect("large message decoded");
        assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn large_message_direct_read_returns_unexpected_eof_on_short_payload() {
        const MSG_SIZE: usize = 1024 * 1024;
        const PREFIX_PAYLOAD_BYTES: usize = 64;

        let (mut push, mut pull) = ready_push_pull_connections();
        let payload = patterned_payload(MSG_SIZE, 44);
        push.send_message(&Message::single(Bytes::from(payload)))
            .unwrap();
        let wire = Bytes::from(drain_transmit(&mut push));
        let prefix_wire_bytes = 9 + PREFIX_PAYLOAD_BYTES;

        pull.handle_input(wire.slice(..prefix_wire_bytes)).unwrap();
        let short_end = prefix_wire_bytes + 1024;
        let mut reader = ScriptedReader::new(wire.slice(prefix_wire_bytes..short_end), [128]);
        let config = PeerDriverConfig {
            large_message_threshold: 128 * 1024,
            ..PeerDriverConfig::default()
        };
        let mut last_input = Instant::now();

        let err = handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
            .await
            .expect_err("short payload must fail");

        assert!(matches!(
            err,
            Error::Io(ref e) if e.kind() == io::ErrorKind::UnexpectedEof
        ));
    }

    #[tokio::test]
    async fn large_message_direct_read_preserves_repeated_payloads() {
        const MSG_SIZE: usize = 1024 * 1024;
        const PREFIX_PAYLOAD_BYTES: usize = 632_558;

        let (mut push, mut pull) = ready_push_pull_connections();
        let config = PeerDriverConfig {
            large_message_threshold: 128 * 1024,
            ..PeerDriverConfig::default()
        };
        let mut last_input = Instant::now();

        for seq in 0..2 {
            let payload = patterned_payload(MSG_SIZE, seq);
            push.send_message(&Message::single(Bytes::from(payload.clone())))
                .unwrap();
            let wire = Bytes::from(drain_transmit(&mut push));
            let prefix_wire_bytes = 9 + PREFIX_PAYLOAD_BYTES;

            pull.handle_input(wire.slice(..prefix_wire_bytes)).unwrap();
            let mut reader = ScriptedReader::new(
                wire.slice(prefix_wire_bytes..),
                [3, 1, 65_537, 8_191, 262_147],
            );
            handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
                .await
                .unwrap();

            let msg = pull.poll_message().expect("large message decoded");
            assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
        }
    }

    #[tokio::test]
    async fn large_message_direct_read_small_payload_smoke() {
        const MSG_SIZE: usize = 8 * 1024;
        const PREFIX_CASES: &[usize] = &[0, 4, 4097, MSG_SIZE];

        let (mut push, mut pull) = ready_push_pull_connections();
        let config = PeerDriverConfig {
            large_message_threshold: 1024,
            ..PeerDriverConfig::default()
        };
        let mut last_input = Instant::now();

        for (seq, &prefix_payload_bytes) in PREFIX_CASES.iter().enumerate() {
            let payload = patterned_payload(MSG_SIZE, seq as u64);
            push.send_message(&Message::single(Bytes::from(payload.clone())))
                .unwrap();
            let wire = Bytes::from(drain_transmit(&mut push));
            let prefix_wire_bytes = 9 + prefix_payload_bytes;

            feed_fragmented_input(&mut pull, &wire, prefix_wire_bytes);
            let mut reader = ScriptedReader::new(wire.slice(prefix_wire_bytes..), [1, 7, 31, 257]);
            handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
                .await
                .unwrap();

            let msg = pull.poll_message().expect("large message decoded");
            assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
            assert_eq!(reader.pos, reader.data.len());
        }
    }

    #[tokio::test]
    async fn large_message_direct_read_survives_prefix_boundary_matrix() {
        const MSG_SIZE: usize = 1024 * 1024;
        const PREFIX_CASES: &[usize] = &[
            0,
            1,
            4,
            8,
            9,
            17,
            255,
            256,
            4095,
            4096,
            65_535,
            65_536,
            128 * 1024 - 4,
            128 * 1024,
            128 * 1024 + 4,
            632_554,
            632_558,
            632_562,
            MSG_SIZE - 1,
            MSG_SIZE,
        ];

        let (mut push, mut pull) = ready_push_pull_connections();
        let config = PeerDriverConfig {
            large_message_threshold: 128 * 1024,
            ..PeerDriverConfig::default()
        };
        let mut last_input = Instant::now();

        for (seq, &prefix_payload_bytes) in PREFIX_CASES.iter().enumerate() {
            let payload = patterned_payload(MSG_SIZE, seq as u64);
            push.send_message(&Message::single(Bytes::from(payload.clone())))
                .unwrap();
            let wire = Bytes::from(drain_transmit(&mut push));
            let prefix_wire_bytes = 9 + prefix_payload_bytes;

            feed_fragmented_input(&mut pull, &wire, prefix_wire_bytes);
            let mut reader = ScriptedReader::new(
                wire.slice(prefix_wire_bytes..),
                [1 + (seq % 7), 3, 31, 4093, 65_537, 131_071, 262_147],
            );

            handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
                .await
                .unwrap();

            let msg = pull.poll_message().expect("large message decoded");
            assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
            assert_eq!(reader.pos, reader.data.len());
        }
    }

    #[tokio::test]
    async fn large_message_direct_read_matches_mixed_chunked_wire_reference() {
        const LENGTHS: &[usize] = &[
            16,
            255,
            256,
            4095,
            4096,
            65_536,
            128 * 1024 + 1,
            632_558,
            1024 * 1024,
        ];
        const CASES: u64 = 24;

        for case in 0..CASES {
            let mut seed = 0x5151_F00D_ABCD_1234 ^ case;
            let (mut push, mut pull) = ready_push_pull_connections();
            let mut expected = Vec::new();
            for seq in 0..18u64 {
                let len = LENGTHS[next_random(&mut seed) % LENGTHS.len()];
                let payload = patterned_payload(len, (case << 8) | seq);
                push.send_message(&Message::single(Bytes::from(payload.clone())))
                    .unwrap();
                expected.push(payload);
            }
            let wire = Bytes::from(drain_transmit(&mut push));
            let config = PeerDriverConfig {
                large_message_threshold: 128 * 1024,
                ..PeerDriverConfig::default()
            };
            let mut last_input = Instant::now();
            let mut cursor = 0usize;
            let mut got = Vec::new();

            while cursor < wire.len() {
                let chunk_len = match next_random(&mut seed) % 10 {
                    0 => 1,
                    1 => 2,
                    2 => 9,
                    3 => 17,
                    4 => 4096,
                    5 => 65_536,
                    6 => 128 * 1024,
                    7 => 128 * 1024 + 4,
                    8 => 9 + 632_558,
                    _ => (next_random(&mut seed) % (128 * 1024)) + 1,
                };
                let end = cursor.saturating_add(chunk_len).min(wire.len());
                pull.handle_input(wire.slice(cursor..end)).unwrap();
                cursor = end;

                let read_caps = [
                    1 + (next_random(&mut seed) % 7),
                    31,
                    4093,
                    65_537,
                    131_071,
                    262_147,
                ];
                let mut reader = ScriptedReader::new(wire.slice(cursor..), read_caps);
                handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
                    .await
                    .unwrap();
                cursor += reader.pos;

                while let Some(msg) = pull.poll_message() {
                    got.push(msg.part_bytes(0).unwrap().to_vec());
                }
            }

            while let Some(msg) = pull.poll_message() {
                got.push(msg.part_bytes(0).unwrap().to_vec());
            }
            assert_eq!(got, expected, "case={case}");
        }
    }

    #[tokio::test]
    async fn yring_sink_deferred_send_signals_once_per_flush() {
        let (producer, mut consumer) = yring::spsc(4);
        let signals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let signals_for_sink = signals.clone();
        let mut sink = YringSink {
            producer,
            signal: Box::new(move || {
                signals_for_sink.fetch_add(1, Ordering::Relaxed);
            }),
            space: Arc::new(StateSignal::new()),
        };
        let mut pending = false;

        assert!(sink.send_deferred(Message::single("a"), &mut pending).await);
        assert!(sink.send_deferred(Message::single("b"), &mut pending).await);
        assert_eq!(signals.load(Ordering::Relaxed), 0);
        assert_eq!(consumer.prefetch(), 0);

        sink.flush_pending(&mut pending);
        assert_eq!(signals.load(Ordering::Relaxed), 1);
        assert_eq!(consumer.prefetch(), 2);
    }

    /// Adapter: pull `(u64, PeerEvent::Event)` off the shared peer-out
    /// channel and yield bare `Event` values, matching the older
    /// per-side events channel shape the tests were written
    /// against. `PeerEvent::Closed` ends the stream (returns None).
    pub(super) struct EventAdapter {
        rx: mpsc::Receiver<(u64, PeerEvent)>,
    }

    impl EventAdapter {
        pub(super) async fn recv(&mut self) -> Option<Event> {
            match self.rx.recv().await? {
                (_, PeerEvent::Event(e)) => Some(e),
                (_, PeerEvent::Closed { .. }) => None,
            }
        }
    }

    /// Spin up two drivers connected via an in-memory duplex pair,
    /// return handles + event rxes. The connection driver is generic
    /// over T: AsyncRead+AsyncWrite, so a `tokio::io::duplex` pair
    /// is the simplest way to test it without involving the inproc
    /// transport (which since the inproc fast-path landed bypasses
    /// the connection entirely).
    #[expect(clippy::unused_async)]
    async fn inproc_pair(
        _name: &str,
    ) -> (
        PeerDriverHandle,
        EventAdapter,
        PeerDriverHandle,
        EventAdapter,
    ) {
        let (server_stream, client_stream) = tokio::io::duplex(64 * 1024);

        let server_connection =
            Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let client_connection = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push)
                .identity(Bytes::from_static(b"c")),
        );

        let (s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
        let (c_inbox_tx, c_inbox_rx) = mpsc::channel(16);
        let (s_evt_tx, s_evt_rx) = mpsc::channel(16);
        let (c_evt_tx, c_evt_rx) = mpsc::channel(16);
        let s_cancel = CancellationToken::new();
        let c_cancel = CancellationToken::new();

        let s_driver = ConnectionDriver::new(
            server_stream,
            server_connection,
            s_inbox_rx,
            s_evt_tx,
            0,
            s_cancel.clone(),
        );
        let c_driver = ConnectionDriver::new(
            client_stream,
            client_connection,
            c_inbox_rx,
            c_evt_tx,
            0,
            c_cancel.clone(),
        );

        tokio::spawn(async move { s_driver.run().await });
        tokio::spawn(async move { c_driver.run().await });

        (
            PeerDriverHandle {
                inbox: c_inbox_tx,
                cancel: c_cancel,
                transmit_slot: None,
                direct_tcp_writer: None,
                send_pipe: None,
            },
            EventAdapter { rx: c_evt_rx },
            PeerDriverHandle {
                inbox: s_inbox_tx,
                cancel: s_cancel,
                transmit_slot: None,
                direct_tcp_writer: None,
                send_pipe: None,
            },
            EventAdapter { rx: s_evt_rx },
        )
    }

    #[tokio::test]
    async fn handshake_completes_over_inproc() {
        let (_client, mut client_events, _server, mut server_events) =
            inproc_pair("drv-handshake").await;

        let c = client_events.recv().await.unwrap();
        let s = server_events.recv().await.unwrap();
        assert!(matches!(c, Event::HandshakeSucceeded { .. }));
        assert!(matches!(s, Event::HandshakeSucceeded { .. }));
    }

    #[tokio::test]
    async fn message_roundtrip_over_inproc() {
        let (client, mut client_events, server, mut server_events) = inproc_pair("drv-msg").await;
        client_events.recv().await.unwrap();
        server_events.recv().await.unwrap();
        client
            .inbox
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        server
            .inbox
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();

        client
            .inbox
            .send(PeerDriverCommand::SendMessage(Message::single("hello")))
            .await
            .unwrap();

        let ev = server_events.recv().await.unwrap();
        match ev {
            Event::Message(m) => {
                assert_eq!(m.part_bytes(0).unwrap(), &b"hello"[..]);
            }
            _ => panic!("unexpected {ev:?}"),
        }
    }

    #[tokio::test]
    async fn send_pipe_to_yring_preserves_large_payload_under_partial_io() {
        let (server_stream, client_stream) = tokio::io::duplex(16 * 1024);
        send_pipe_to_yring_large_payload_harness(server_stream, client_stream, 32).await;
    }

    #[tokio::test]
    async fn send_pipe_to_yring_preserves_large_payload_under_choppy_io() {
        let (server_stream, client_stream) = tokio::io::duplex(16 * 1024);
        let server_stream = ChoppyDuplex::new(server_stream, 17_003, 7_919);
        let client_stream = ChoppyDuplex::new(client_stream, 23_011, 65_537);
        send_pipe_to_yring_large_payload_harness(server_stream, client_stream, 24).await;
    }

    #[expect(clippy::too_many_lines)]
    async fn send_pipe_to_yring_large_payload_harness<S, C>(
        server_stream: S,
        client_stream: C,
        msgs: usize,
    ) where
        S: DriverStream + Send + 'static,
        C: DriverStream + Send + 'static,
    {
        const MSG_SIZE: usize = 1024 * 1024;
        let server_connection =
            Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let client_connection = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push)
                .identity(Bytes::from_static(b"c")),
        );

        let (mut send_pipe_tx, send_pipe_rx) = crate::engine::send_pipe(4);
        let (recv_producer, mut recv_consumer) = yring::spsc(4);
        let recv_signals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recv_signals_for_sink = recv_signals.clone();
        let recv_space = Arc::new(StateSignal::new());

        let (s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
        let (c_inbox_tx, c_inbox_rx) = mpsc::channel(16);
        let (s_evt_tx, s_evt_rx) = mpsc::channel(16);
        let (c_evt_tx, c_evt_rx) = mpsc::channel(16);
        let mut s_evt_rx = EventAdapter { rx: s_evt_rx };
        let mut c_evt_rx = EventAdapter { rx: c_evt_rx };
        let s_cancel = CancellationToken::new();
        let c_cancel = CancellationToken::new();

        let server = ConnectionDriver::with_config(
            server_stream,
            server_connection,
            s_inbox_rx,
            s_evt_tx,
            0,
            s_cancel.clone(),
            PeerDriverConfig {
                large_message_threshold: 128 * 1024,
                ..PeerDriverConfig::default()
            },
        )
        .with_recv_sink(RecvSink::Yring(YringSink {
            producer: recv_producer,
            signal: Box::new(move || {
                recv_signals_for_sink.fetch_add(1, Ordering::Relaxed);
            }),
            space: recv_space.clone(),
        }));
        let client = ConnectionDriver::new(
            client_stream,
            client_connection,
            c_inbox_rx,
            c_evt_tx,
            0,
            c_cancel.clone(),
        )
        .with_send_pipe(send_pipe_rx);

        let server_task = tokio::spawn(async move { server.run().await });
        let client_task = tokio::spawn(async move { client.run().await });

        c_evt_rx.recv().await.unwrap();
        s_evt_rx.recv().await.unwrap();
        c_inbox_tx
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        s_inbox_tx
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();

        let mut next_recv = 0usize;
        for seq in 0..msgs {
            let payload = patterned_payload(MSG_SIZE, seq as u64);
            let mut msg = Message::single(payload);
            loop {
                match send_pipe_tx.try_send(msg) {
                    Ok(()) => break,
                    Err(crate::engine::SendPipeError::Full(returned)) => {
                        msg = returned;
                        drain_large_messages_until(
                            &mut recv_consumer,
                            &recv_space,
                            MSG_SIZE,
                            &mut next_recv,
                            seq,
                            false,
                        )
                        .await;
                        tokio::task::yield_now().await;
                    }
                    Err(crate::engine::SendPipeError::Closed(_)) => panic!("send pipe closed"),
                }
            }
        }

        drain_large_messages_until(
            &mut recv_consumer,
            &recv_space,
            MSG_SIZE,
            &mut next_recv,
            msgs,
            true,
        )
        .await;
        c_cancel.cancel();
        s_cancel.cancel();
        let client_result = tokio::time::timeout(Duration::from_secs(5), client_task)
            .await
            .expect("client driver did not stop")
            .expect("client driver task panicked");
        let server_result = tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server driver did not stop")
            .expect("server driver task panicked");
        client_result.expect("client driver failed");
        server_result.expect("server driver failed");
    }

    #[tokio::test]
    async fn cancel_stops_driver() {
        let (client, _client_events, _server, _server_events) = inproc_pair("drv-cancel").await;
        client.cancel.cancel();
        // The driver should exit; confirm by closing its inbox and checking
        // a subsequent send fails.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let res = client.inbox.send(PeerDriverCommand::Close).await;
        assert!(res.is_err(), "inbox should be closed after driver exit");
    }

    #[tokio::test]
    async fn handshake_completes_over_tcp() {
        use crate::transport::{Listener as _, TcpTransport, Transport as _};
        use omq_proto::endpoint::{Endpoint, Host};
        use std::net::{IpAddr, Ipv4Addr};

        let bind_ep = Endpoint::Tcp {
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port: 0,
        };
        let mut listener = TcpTransport::bind(&bind_ep).await.unwrap();
        let local = listener.local_endpoint().clone();
        let Endpoint::Tcp { port, .. } = local else {
            panic!()
        };

        let connect_ep = Endpoint::Tcp {
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port,
        };
        let connect_task = tokio::spawn(async move { TcpTransport::connect(&connect_ep).await });

        let (server_stream, _peer) = listener.accept().await.unwrap();
        let client_stream = connect_task.await.unwrap().unwrap();

        let server_connection =
            Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let client_connection =
            Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));

        let (c_inbox_tx, c_inbox_rx) = mpsc::channel(16);
        let (s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
        let (c_evt_tx, c_evt_rx) = mpsc::channel(16);
        let (s_evt_tx, s_evt_rx) = mpsc::channel(16);
        let mut c_evt_rx = EventAdapter { rx: c_evt_rx };
        let mut s_evt_rx = EventAdapter { rx: s_evt_rx };

        let s = ConnectionDriver::new(
            server_stream,
            server_connection,
            s_inbox_rx,
            s_evt_tx,
            0,
            CancellationToken::new(),
        );
        let c = ConnectionDriver::new(
            client_stream,
            client_connection,
            c_inbox_rx,
            c_evt_tx,
            0,
            CancellationToken::new(),
        );
        tokio::spawn(async move { s.run().await });
        tokio::spawn(async move { c.run().await });

        let _ = c_inbox_tx; // keep inbox open
        let _ = s_inbox_tx;

        match c_evt_rx.recv().await.unwrap() {
            Event::HandshakeSucceeded { .. } => {}
            other => panic!("unexpected {other:?}"),
        }
        match s_evt_rx.recv().await.unwrap() {
            Event::HandshakeSucceeded { .. } => {}
            other => panic!("unexpected {other:?}"),
        }
    }

    /// When READY + ERROR arrive in the same TCP read, `handle_input`
    /// processes READY (queuing `HandshakeSucceeded`) then returns `Err`
    /// on ERROR. The driver must drain pending events before
    /// propagating the error so `HandshakeSucceeded` is not lost.
    #[tokio::test]
    async fn coalesced_ready_and_error_still_emits_handshake_succeeded() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (server_stream, mut client_stream) = tokio::io::duplex(64 * 1024);

        // Server driver on one end of the duplex.
        let server_connection =
            Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let (_s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
        let (s_evt_tx, mut s_evt_rx) = mpsc::channel::<(u64, PeerEvent)>(16);
        let s_driver = ConnectionDriver::new(
            server_stream,
            server_connection,
            s_inbox_rx,
            s_evt_tx,
            0,
            CancellationToken::new(),
        );
        tokio::spawn(async move { s_driver.run().await });

        // Manual client: use a connection to generate correct wire bytes.
        let mut client_connection = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push)
                .identity(Bytes::from_static(b"x")),
        );

        // Write client greeting.
        let greeting = drain_transmit(&mut client_connection);
        client_stream.write_all(&greeting).await.unwrap();

        // Read server greeting + READY from the duplex and feed to
        // client connection until it reaches Ready state.
        let mut buf = vec![0u8; 4096];
        while !client_connection.is_ready() {
            let n = client_stream.read(&mut buf).await.unwrap();
            assert!(n > 0, "server closed before handshake");
            client_connection
                .handle_input(Bytes::copy_from_slice(&buf[..n]))
                .unwrap();
        }

        // Client connection has produced READY. Also encode ERROR.
        let ready_bytes = drain_transmit(&mut client_connection);
        client_connection
            .send_command(&Command::Error {
                reason: "boom".into(),
            })
            .unwrap();
        let error_bytes = drain_transmit(&mut client_connection);

        // Write READY + ERROR in a single write so the server driver
        // reads them in one handle_input call.
        let mut combined = Vec::with_capacity(ready_bytes.len() + error_bytes.len());
        combined.extend_from_slice(&ready_bytes);
        combined.extend_from_slice(&error_bytes);
        client_stream.write_all(&combined).await.unwrap();

        // Collect all events from the server driver.
        let mut events = Vec::new();
        while let Some((_, out)) = s_evt_rx.recv().await {
            let is_closed = matches!(out, PeerEvent::Closed { .. });
            events.push(out);
            if is_closed {
                break;
            }
        }

        assert!(
            events
                .iter()
                .any(|e| matches!(e, PeerEvent::Event(Event::HandshakeSucceeded { .. }))),
            "HandshakeSucceeded must not be lost when coalesced with \
             a post-handshake protocol error; got: {events:?}",
        );
    }

    fn drain_transmit(connection: &mut Connection) -> Vec<u8> {
        let mut out = Vec::new();
        while connection.has_pending_transmit() {
            let len_before = out.len();
            for chunk in connection.transmit_chunks_capped(128) {
                out.extend_from_slice(&chunk);
            }
            connection.advance_transmit(out.len() - len_before);
        }
        out
    }

    fn ready_push_pull_connections() -> (Connection, Connection) {
        let mut push = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push)
                .identity(Bytes::from_static(b"c")),
        );
        let mut pull = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        for _ in 0..10 {
            let push_out = drain_transmit(&mut push);
            let pull_out = drain_transmit(&mut pull);
            if push_out.is_empty() && pull_out.is_empty() {
                break;
            }
            if !push_out.is_empty() {
                pull.handle_input(Bytes::from(push_out)).unwrap();
            }
            if !pull_out.is_empty() {
                push.handle_input(Bytes::from(pull_out)).unwrap();
            }
        }
        assert!(push.is_ready());
        assert!(pull.is_ready());
        (push, pull)
    }

    fn feed_fragmented_input(connection: &mut Connection, wire: &Bytes, end: usize) {
        let mut start = 0;
        for boundary in [5, 17, 4093, 65_541, end] {
            let boundary = boundary.min(end);
            if boundary > start {
                connection
                    .handle_input(wire.slice(start..boundary))
                    .unwrap();
                start = boundary;
            }
        }
        if start < end {
            connection.handle_input(wire.slice(start..end)).unwrap();
        }
    }

    async fn handle_large_messages_test<R: AsyncRead + Unpin>(
        connection: &mut Connection,
        reader: &mut R,
        config: &PeerDriverConfig,
        last_input: &mut Instant,
    ) -> Result<()> {
        let recv_pool = RecvBufPool::new();
        handle_large_messages(connection, reader, config, last_input, &recv_pool).await
    }

    fn feed_input_in_chunks(
        connection: &mut Connection,
        wire: &Bytes,
        end: usize,
        chunks: impl IntoIterator<Item = usize>,
    ) {
        let chunks = chunks.into_iter().collect::<Vec<_>>();
        let mut start = 0;
        let mut index = 0;
        while start < end {
            let size = chunks[index % chunks.len()];
            index += 1;
            let next = start.saturating_add(size).min(end);
            connection.handle_input(wire.slice(start..next)).unwrap();
            start = next;
        }
    }

    fn patterned_payload(len: usize, seq: u64) -> Vec<u8> {
        let mut payload = vec![0u8; len];
        payload[..8].copy_from_slice(&0xDEAD_BEEF_CAFE_F00Du64.to_le_bytes());
        payload[8..16].copy_from_slice(&seq.to_le_bytes());
        let mask = (seq as u8).wrapping_mul(7) ^ ((seq >> 8) as u8).wrapping_mul(3);
        for (i, byte) in payload.iter_mut().enumerate().skip(16) {
            let mut expected = (i as u8).wrapping_mul(31);
            expected ^= ((i >> 8) as u8).wrapping_mul(17);
            expected ^= ((i >> 16) as u8).wrapping_mul(13);
            *byte = expected ^ mask;
        }
        payload
    }

    fn push_expected_single_frame(out: &mut Vec<u8>, payload: &[u8]) {
        if payload.len() > 255 {
            out.push(0x02);
            out.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        } else {
            out.push(0);
            out.push(payload.len() as u8);
        }
        out.extend_from_slice(payload);
    }

    fn next_random(seed: &mut u64) -> usize {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed as usize
    }

    async fn drain_large_messages_until(
        consumer: &mut yring::Consumer<RecvItem>,
        space: &StateSignal,
        msg_size: usize,
        next_recv: &mut usize,
        target: usize,
        wait: bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while *next_recv < target {
            if consumer.prefetch() == 0 && consumer.is_empty() {
                if !wait || Instant::now() >= deadline {
                    break;
                }
                tokio::task::yield_now().await;
                continue;
            }
            let mut released = false;
            while let Some(item) = consumer.pop() {
                released = true;
                let data = item.message.part_bytes(0).unwrap();
                assert_eq!(
                    data.as_ref(),
                    patterned_payload(msg_size, *next_recv as u64)
                );
                *next_recv += 1;
            }
            if released {
                consumer.release();
                space.notify_changed();
            }
        }
        if wait {
            assert!(
                *next_recv >= target,
                "received {} large messages, expected {target}",
                *next_recv,
            );
        }
    }
}
