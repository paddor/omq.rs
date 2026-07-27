//! Per-connection driver: one tokio task per live peer connection.

use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use futures::stream::FuturesOrdered;
use omq_proto::WorkloadProfile;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::transform::{MessageDecoder, MessageEncoder, TransformedOut};
use omq_proto::proto::{Command, Connection, Event};

use super::compression_pool::CompressionPool;
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
    Rep(RepRecvSink),
}

/// REP's latency receive path: perform identity/envelope handling in the
/// connection driver, before the message reaches the socket actor.
#[derive(Debug)]
pub struct RepRecvSink {
    sink: Box<RecvSink>,
    pending: std::sync::Arc<std::sync::Mutex<VecDeque<(u64, RepEnvelope)>>>,
    peer_id: u64,
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
            Self::Rep(_) => f.debug_tuple("Rep").finish_non_exhaustive(),
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

    /// Non-blocking push. Returns the message back if the yring is full.
    /// Channel variant always succeeds (awaits space).
    pub(crate) async fn try_send(&mut self, m: Message) -> Option<Message> {
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
            Self::Rep(_) => unreachable!("REP uses the blocking direct path"),
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
            Self::Rep(_) => unreachable!("plain send on REP sink"),
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
        self.send_plain(m).await
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
    /// Recv frames whose payload exceeds this threshold via a single
    /// `read_exact` into a pre-sized buffer, bypassing the fixed
    /// `read_buf` -> `Connection` buffering path. `0` disables.
    pub large_message_threshold: usize,
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
        } = self;
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
        let mut handshake_deadline: Option<Instant> =
            config.handshake_timeout.map(|d| last_input + d);
        let hb_interval = config.heartbeat_interval;
        let hb_timeout = config
            .heartbeat_timeout
            .or(config.heartbeat_interval)
            .unwrap_or(Duration::MAX);
        let hb_ttl_deciseconds = config
            .heartbeat_ttl
            .and_then(|d| u16::try_from(d.as_millis() / 100).ok())
            .unwrap_or(0);
        let hb_sleep = tokio::time::sleep(hb_interval.unwrap_or(Duration::MAX));
        tokio::pin!(hb_sleep);
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
            let hb_enabled = hb_interval.is_some();

            // Latency-routed REQ/REP sends are encoded into the wire slot by
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
                () = &mut hb_sleep, if hb_enabled => {
                    if hb_ping_sent && last_input.elapsed() > hb_timeout {
                        return Err(Error::Timeout);
                    }
                    let ping = Command::Ping {
                        ttl_deciseconds: hb_ttl_deciseconds,
                        context: Bytes::new(),
                    };
                    let _ = connection.send_command(&ping);
                    hb_ping_sent = true;
                    hb_sleep.as_mut().reset(
                        tokio::time::Instant::now() + hb_interval.unwrap(),
                    );
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

async fn drain_decoded_messages(
    connection: &mut Connection,
    decoder: &mut Option<MessageDecoder>,
    receive_profile: ReceiveProfile,
    recv_direct: &mut Option<RecvSink>,
    peer_out: &mpsc::Sender<(u64, PeerEvent)>,
    peer_id: u64,
) -> Result<DriverStep> {
    let recv_batch_start = Instant::now();
    let mut recv_budget = None;
    let mut recv_batch_time = None;
    while let Some(m) = connection.poll_message() {
        let m = match decoder.as_mut() {
            Some(dec) => match dec.decode(m)? {
                Some(plain) => plain,
                None => continue,
            },
            None => m,
        };
        let msg_bytes = m.byte_len();
        let budget = recv_budget.get_or_insert_with(|| {
            recv_batch_time = receive_profile.time(msg_bytes);
            receive_profile.budget(msg_bytes)
        });
        if !route_message(m, recv_direct, peer_out, peer_id).await {
            return Ok(DriverStep::Close);
        }
        let budget_remains = budget.account(msg_bytes);
        let time_check = budget.msgs().is_multiple_of(32);
        if !budget_remains
            || (time_check
                && recv_batch_time.is_some_and(|limit| recv_batch_start.elapsed() >= limit))
        {
            return Ok(DriverStep::Yield);
        }
    }
    Ok(DriverStep::Continue)
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

// -- Recv buffer pool --------------------------------------------------------
//
// Large messages (above the arena threshold) need their own allocation.
// Without pooling, each message triggers mmap + page faults for fresh
// zeroed pages. The pool recycles buffers: pages stay warm, no syscall
// per message after warmup.

#[derive(Debug)]
struct RecvBufPool(std::sync::Mutex<Vec<Vec<u8>>>);

impl RecvBufPool {
    fn new() -> Arc<Self> {
        Arc::new(Self(std::sync::Mutex::new(Vec::new())))
    }

    fn take(&self, capacity: usize) -> Vec<u8> {
        let mut pool = self.0.lock().expect("recv buf pool");
        if let Some(mut buf) = pool.pop() {
            if buf.capacity() < capacity {
                buf.reserve(capacity - buf.capacity());
            }
            buf
        } else {
            Vec::with_capacity(capacity)
        }
    }

    fn give(&self, buf: Vec<u8>) {
        self.0.lock().expect("recv buf pool").push(buf);
    }
}

struct PooledRecvBuf {
    buf: Vec<u8>,
    pool: Arc<RecvBufPool>,
}

impl AsRef<[u8]> for PooledRecvBuf {
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl Drop for PooledRecvBuf {
    fn drop(&mut self) {
        let buf = std::mem::take(&mut self.buf);
        self.pool.give(buf);
    }
}

/// Read large frames directly into pooled buffers (bypasses the fixed
/// `read_buf` -> `Connection` buffering path). The pool recycles allocations
/// so pages stay warm across messages.
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
        let mut buf = recv_pool.take(plen);
        buf.resize(plen, 0);
        buf[..prefix.len()].copy_from_slice(prefix.as_slice());
        if prefix.len() < plen {
            reader.read_exact(&mut buf[prefix.len()..plen]).await?;
        }
        *last_input = Instant::now();
        let payload = Bytes::from_owner(PooledRecvBuf {
            buf,
            pool: Arc::clone(recv_pool),
        });
        connection.supply_payload(payload)?;
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
    #[cfg(feature = "lz4")]
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
    #[cfg(feature = "lz4")]
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
) -> bool {
    match recv_direct {
        Some(sink) => sink.send(m).await,
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
    use tokio::io::DuplexStream;
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
}
