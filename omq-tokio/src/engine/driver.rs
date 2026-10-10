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
use omq_proto::copy_stats::{self, Site};
use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::message::Message;
use omq_proto::proto::transform::{MessageDecoder, MessageEncoder, TransformedOut};
use omq_proto::proto::{Command, Connection, Event};
use omq_proto::{DisconnectReason, MessageRateLimit, WorkloadProfile};

use super::actor_output::{DataSender, PeerOutput};
use super::compression_pool::CompressionPool;
use super::peer_completion::CompletionProgress;
use super::peer_events::PeerEventDispatch;
use super::rate_limit::{SharedIpRateLimiter, TokenBucket};
use super::send_pipe::{SendPipeConsumer, SendPipeProducerHandle};
use super::transmit_slot::PeerTransmitSlot;
use crate::socket::dispatch::{AnyReadHalf, AnyStream, AnyWriteHalf};
use omq_proto::flow::{DrainBudget, max_batch_bytes};
use omq_proto::frame_buffer::FrameBuffer;

const RECV_SMALL_MSG: usize = 1024;
const RECV_MEDIUM_MSG: usize = 4096;
const RECV_SMALL_BYTES: usize = 64 * 1024;
const RECV_MEDIUM_BYTES: usize = 1024 * 1024;
const RECV_LARGE_BYTES: usize = 1024 * 1024;
const RECV_MEDIUM_TIME: Duration = Duration::from_micros(200);
const RECV_LARGE_TIME: Duration = Duration::from_micros(200);
const OUTBOUND_BATCH_TIME: Duration = Duration::from_millis(1);
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
    type Writer: DriverWrite;

    fn split(self, fast_write: bool) -> (Self::Reader, Self::Writer);
}

/// Write half of a [`DriverStream`].
pub trait DriverWrite: AsyncWrite + Send + Unpin + 'static {
    /// The owned-chunk path of a writer that keeps wire chunks until the
    /// peer acknowledges them (QUIC). Other writers copy from `IoSlice`s.
    fn chunk_writer(&mut self) -> Option<&mut dyn ChunkWrite> {
        None
    }
}

/// A writer that takes wire chunks owned instead of copying them.
pub trait ChunkWrite: Send {
    /// Write a prefix of `bufs`, emptying accepted chunks and trimming a
    /// partly accepted one in place. Returns the bytes accepted. Pending
    /// leaves `bufs` unchanged.
    fn poll_write_chunks(
        &mut self,
        cx: &mut std::task::Context<'_>,
        bufs: &mut [Bytes],
    ) -> std::task::Poll<io::Result<usize>>;
}

impl DriverStream for AnyStream {
    type Reader = AnyReadHalf;
    type Writer = AnyWriteHalf;

    fn split(self, fast_write: bool) -> (Self::Reader, Self::Writer) {
        AnyStream::split(self, fast_write)
    }
}

impl DriverWrite for AnyWriteHalf {
    fn chunk_writer(&mut self) -> Option<&mut dyn ChunkWrite> {
        match self {
            #[cfg(feature = "quic")]
            Self::Quic(writer) => Some(writer),
            _ => None,
        }
    }
}

#[cfg(feature = "quic")]
impl ChunkWrite for crate::transport::quic::QuicSendHalf {
    fn poll_write_chunks(
        &mut self,
        cx: &mut std::task::Context<'_>,
        bufs: &mut [Bytes],
    ) -> std::task::Poll<io::Result<usize>> {
        crate::transport::quic::QuicSendHalf::poll_write_chunks(self, cx, bufs)
    }
}

impl DriverWrite for tokio::net::tcp::OwnedWriteHalf {}

impl<T: AsyncRead + AsyncWrite + Send + 'static> DriverWrite for tokio::io::WriteHalf<T> {}

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

pub use super::recv_sink::{
    AuthenticatedRecvItem, AuthenticatedRecvSink, RecvSink, RecvSinkConfig, RepRecvSink,
    ServerRecvSink, YringSink,
};

/// Batch-encode messages into `FrameBuffer`. Two modes:
///
/// **Direct** (no encoder or offloading disabled): encode each message
/// into `FrameBuffer` inline.
///
/// **Pipelined** (encoder present, offloading enabled): each message
/// enters `FuturesOrdered` as either `spawn_blocking` (large) or
/// `ready()` (small). The driver drains those futures from its main select
/// loop so compression cannot hide control work.
///
/// Does not flush to the writer.
#[expect(clippy::too_many_arguments)]
fn batch_encode(
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
    let started = Instant::now();
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
        if count.is_multiple_of(32) && started.elapsed() >= OUTBOUND_BATCH_TIME {
            break;
        }
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
    /// Install a socket-selected receive sink before activating application
    /// traffic. Used for PEER receive queues after handshake.
    ActivateWithRecvSink(RecvSink),
    /// Queue a ZMTP command for send (SUBSCRIBE, CANCEL, JOIN, LEAVE, ...).
    SendCommand(Command),
    /// Finish accepted output before shutting down the transport. The deadline
    /// belongs to the socket close operation and must not restart per stage.
    DrainAndClose { deadline: Option<Instant> },
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

/// Wire chunks removed from a [`FrameBuffer`] but not yet accepted by the
/// transport. Keeping them in driver state makes the write future safe to
/// cancel when control work wins the outer `select!`.
#[derive(Debug, Default)]
struct PendingWrite {
    chunks: Vec<Bytes>,
    first: usize,
    offset: usize,
    remaining: usize,
    arena: Vec<u8>,
    arena_offset: usize,
}

/// Owned chunks at least this long reach the writer uncopied.
const OWNED_COALESCE_BELOW: usize = 16 * 1024;
/// Upper bound for one merged run of smaller chunks.
const OWNED_COALESCE_TARGET: usize = 64 * 1024;

/// Merges runs of small owned chunks into one buffer each, in order.
///
/// Quinn's send buffer keeps one segment per written chunk until the peer
/// acknowledges it, and finds the data for each STREAM frame by walking
/// those segments linearly. Many small chunks in flight make packet
/// assembly quadratic, so small frames and shared fan-out payloads are
/// copied into larger runs. A run of one chunk is kept as is.
fn coalesce_small_chunks(chunks: &mut Vec<Bytes>) {
    let mut write = 0;
    let mut read = 0;
    while read < chunks.len() {
        if chunks[read].len() >= OWNED_COALESCE_BELOW {
            chunks.swap(write, read);
            write += 1;
            read += 1;
            continue;
        }
        let start = read;
        let mut run_len = 0;
        while read < chunks.len()
            && chunks[read].len() < OWNED_COALESCE_BELOW
            && (read == start || run_len + chunks[read].len() <= OWNED_COALESCE_TARGET)
        {
            run_len += chunks[read].len();
            read += 1;
        }
        let merged = if read - start == 1 {
            std::mem::take(&mut chunks[start])
        } else {
            copy_stats::record(Site::Coalesce, run_len);
            let mut merged = bytes::BytesMut::with_capacity(run_len);
            for chunk in &chunks[start..read] {
                merged.extend_from_slice(chunk);
            }
            merged.freeze()
        };
        chunks[write] = merged;
        write += 1;
    }
    chunks.truncate(write);
}

#[derive(Debug, Clone, Copy)]
struct GracefulClose {
    deadline: Option<Instant>,
    ws_close_started: bool,
    discard_receive: bool,
}

impl PendingWrite {
    fn is_empty(&self) -> bool {
        self.first == self.chunks.len() && self.arena_offset == self.arena.len()
    }

    fn stage(&mut self, eq: &mut FrameBuffer) {
        debug_assert!(self.is_empty());
        debug_assert!(!eq.has_arena_only());
        self.chunks.clear();
        self.first = 0;
        self.offset = 0;
        eq.drain(&mut self.chunks, 1024);
        self.remaining = self.chunks.iter().map(Bytes::len).sum();
    }

    /// Stage one slot batch. With `owned`, arena bytes move into the chunks
    /// without a copy and small chunks are merged for the owned-chunk writer.
    fn stage_slot(&mut self, slot: &PeerTransmitSlot, owned: bool) -> bool {
        debug_assert!(self.is_empty());
        self.arena.clear();
        self.arena_offset = 0;
        if let Some(outcome) = slot.try_drain_arena_only(&mut self.arena) {
            self.remaining = self.arena.len();
            return outcome.space_available;
        }

        self.chunks.clear();
        self.first = 0;
        self.offset = 0;
        let outcome = if owned {
            let outcome = slot.drain_owned(&mut self.chunks, 1024);
            coalesce_small_chunks(&mut self.chunks);
            outcome
        } else {
            slot.drain(&mut self.chunks, 1024)
        };
        self.remaining = self.chunks.iter().map(Bytes::len).sum();
        if !slot.is_empty() {
            slot.data_signal.reschedule();
        }
        outcome.space_available
    }

    /// Stage for a writer that takes owned chunks. Arena bytes move into
    /// the chunks without a copy.
    fn stage_owned(&mut self, eq: &mut FrameBuffer) {
        debug_assert!(self.is_empty());
        self.chunks.clear();
        self.first = 0;
        self.offset = 0;
        eq.drain_owned(&mut self.chunks, 1024);
        coalesce_small_chunks(&mut self.chunks);
        self.remaining = self.chunks.iter().map(Bytes::len).sum();
    }

    fn has_chunks(&self) -> bool {
        self.arena_offset == self.arena.len() && self.first < self.chunks.len()
    }

    /// Unwritten chunks for an owned-chunk writer, which trims them in place.
    fn chunks_mut(&mut self) -> &mut [Bytes] {
        debug_assert_eq!(self.offset, 0);
        &mut self.chunks[self.first..]
    }

    /// Account an owned-chunk write. The writer emptied accepted chunks and
    /// trimmed a partly accepted one.
    fn advance_owned(&mut self, written: usize) {
        debug_assert!(written <= self.remaining);
        self.remaining -= written;
        while self.first < self.chunks.len() && self.chunks[self.first].is_empty() {
            self.first += 1;
        }
        if self.is_empty() {
            self.chunks.clear();
            self.first = 0;
            self.remaining = 0;
        }
    }

    fn io_slices(&self) -> SmallVec<[io::IoSlice<'_>; 64]> {
        if self.arena_offset < self.arena.len() {
            return smallvec::smallvec![io::IoSlice::new(&self.arena[self.arena_offset..])];
        }
        self.chunks
            .iter()
            .skip(self.first)
            .enumerate()
            .map(|(index, chunk)| {
                if index == 0 {
                    io::IoSlice::new(&chunk[self.offset..])
                } else {
                    io::IoSlice::new(chunk)
                }
            })
            .collect()
    }

    fn advance(&mut self, mut written: usize) {
        debug_assert!(written <= self.remaining);
        self.remaining -= written;
        if self.arena_offset < self.arena.len() {
            self.arena_offset += written;
            if self.arena_offset == self.arena.len() {
                self.arena.clear();
                self.arena_offset = 0;
            }
            return;
        }
        while written > 0 {
            let available = self.chunks[self.first].len() - self.offset;
            if written < available {
                self.offset += written;
                return;
            }
            written -= available;
            self.first += 1;
            self.offset = 0;
        }
        if self.is_empty() {
            self.chunks.clear();
            self.first = 0;
            self.remaining = 0;
        }
    }
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

    fn batch_encode(
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

    /// Drain the selected result plus any immediately-ready followers. This
    /// preserves gather-write batching for small compressed messages without
    /// awaiting compression outside the driver's main `select!`.
    fn drain_ready_offload_batch(
        &mut self,
        first: (Option<MessageEncoder>, Result<TransformedOut>),
        connection: &Connection,
        eq: &mut FrameBuffer,
    ) -> Result<usize> {
        use futures::{FutureExt as _, StreamExt as _};

        let started = Instant::now();
        let mut count = 0;
        let mut next = Some(first);
        while let Some((pool_enc, frames)) = next {
            self.drain_offload_result(pool_enc, frames, connection, eq)?;
            count += 1;
            if count >= OUTBOUND_BATCH_MAX_MSGS
                || (count.is_multiple_of(32) && started.elapsed() >= OUTBOUND_BATCH_TIME)
            {
                break;
            }
            next = self.offload_pipeline.next().now_or_never().flatten();
        }
        Ok(count)
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
    decoded_payload_pool: Option<omq_proto::PayloadPool>,
    recv_ip_rate_limiter: Option<(Arc<SharedIpRateLimiter>, IpAddr)>,
    socket_close_state: Option<Arc<crate::socket::recv::SharedRecvPipe>>,
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
        Self::with_output_config(
            stream,
            connection,
            inbox.into(),
            peer_out.into(),
            peer_id,
            cancel,
            config,
        )
    }

    pub(crate) fn with_actor_config(
        stream: T,
        connection: Connection,
        inbox: impl Into<super::control_inbox::Receiver>,
        peer_out: DataSender,
        peer_id: u64,
        cancel: CancellationToken,
        config: PeerDriverConfig,
    ) -> Self {
        Self::with_output_config(
            stream,
            connection,
            inbox.into(),
            PeerOutput::actor(peer_out),
            peer_id,
            cancel,
            config,
        )
    }

    fn with_output_config(
        stream: T,
        connection: Connection,
        inbox: super::control_inbox::Receiver,
        peer_out: PeerOutput,
        peer_id: u64,
        cancel: CancellationToken,
        config: PeerDriverConfig,
    ) -> Self {
        Self {
            stream,
            connection,
            inbox,
            data_inbox: None,
            peer_out,
            peer_id,
            cancel,
            config,
            completion: CompletionProgress::default(),
            peer_control: None,
            notify_xpub: false,
            setup_deadline: None,
            setup_cancel: None,
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
            decoded_payload_pool: None,
            recv_ip_rate_limiter: None,
            socket_close_state: None,
        }
    }

    /// Reserve lifecycle publication independently of the actor's data mailbox.
    pub(crate) fn with_completion(mut self, completion: CompletionProgress) -> Self {
        self.completion = completion;
        self
    }

    pub(crate) fn with_actor_control(
        mut self,
        control: mpsc::Sender<(u64, PeerEvent)>,
        notify_xpub: bool,
    ) -> Self {
        self.notify_xpub = notify_xpub;
        self.peer_control = Some(control);
        self
    }

    /// Carry a transport's original deadline and pre-ready cancellation into ZMTP.
    pub(crate) fn with_setup_deadline(
        mut self,
        deadline: Option<Instant>,
        cancel: Option<CancellationToken>,
    ) -> Self {
        self.setup_deadline = deadline;
        self.setup_cancel = cancel;
        self
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

    /// Select output storage before decompressing a message transform.
    #[must_use]
    pub(crate) fn with_decoded_payload_pool(
        mut self,
        pool: Option<omq_proto::PayloadPool>,
    ) -> Self {
        self.decoded_payload_pool = pool;
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

    /// Install the fallback data-plane inbox. Kept separate from control so a
    /// full or stalled outbound path cannot hide lifecycle commands.
    #[must_use]
    pub fn with_data_inbox(mut self, rx: mpsc::Receiver<PeerDriverData>) -> Self {
        self.data_inbox = Some(super::data_inbox::Receiver::Legacy(rx));
        self
    }

    pub(crate) fn with_actor_data_inbox(mut self, rx: super::data_inbox::Receiver) -> Self {
        self.data_inbox = Some(rx);
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

    pub(crate) fn with_socket_close_state(
        mut self,
        state: Arc<crate::socket::recv::SharedRecvPipe>,
    ) -> Self {
        self.socket_close_state = Some(state);
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
    /// Socket-owned drivers publish into a pre-reserved completion slot.
    /// Standalone drivers send a final `PeerEvent::Closed` on their supplied
    /// channel. Both preserve the earlier admitted event prefix.
    pub async fn run(mut self) -> Result<()> {
        let peer_out = self.peer_out.legacy_sender();
        let peer_id = self.peer_id;
        let mut completion = std::mem::take(&mut self.completion);
        let result = self.run_inner_body(&mut completion).await;
        let reason = result
            .as_ref()
            .err()
            .map_or(DisconnectReason::PeerClosed, |error| {
                if let Error::HandshakeRefused(refusal) = error {
                    DisconnectReason::HandshakeRefused(refusal.clone())
                } else {
                    DisconnectReason::Error(close_error_reason(error))
                }
            });
        if completion.complete(reason).is_err() {
            let error = result.as_ref().err().map(close_error_reason);
            let _ = peer_out
                .expect("standalone event output")
                .send((peer_id, PeerEvent::Closed { error }))
                .await;
        }
        result
    }

    #[expect(clippy::too_many_lines)]
    async fn run_inner_body(self, completion: &mut CompletionProgress) -> Result<()> {
        let Self {
            stream,
            mut connection,
            mut inbox,
            mut data_inbox,
            mut peer_out,
            peer_control,
            notify_xpub,
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
            decoded_payload_pool,
            recv_ip_rate_limiter,
            setup_deadline,
            setup_cancel,
            socket_close_state,
            completion: _,
        } = self;
        if setup_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(Error::HandshakeFailed("transport setup timeout".into()));
        }
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
        let mut pending_write = PendingWrite::default();
        let owned_chunks = writer.chunk_writer().is_some();
        let mut deferred_data = None;
        let mut pipe_batch: Vec<Message> = Vec::new();
        let _write_lifetime = DirectWriteLifetime(
            transmit_slot
                .as_ref()
                .filter(|slot| slot.direct_writer().is_some())
                .cloned(),
        );
        let mut last_input = Instant::now();
        let mut handshake_deadline: Option<Instant> = setup_deadline.or_else(|| {
            config
                .handshake_timeout
                .and_then(|d| last_input.checked_add(d))
        });
        let hb_timeout = config
            .heartbeat_timeout
            .or(config.heartbeat_interval)
            .unwrap_or(Duration::MAX);
        let hb_ttl_deciseconds = config
            .heartbeat_ttl
            .and_then(|d| u16::try_from(d.as_millis() / 100).ok())
            .unwrap_or(0);
        let mut hb_probe = HeartbeatProbe::default();
        let mut was_recv_blocked = false;
        let mut graceful_close = None;
        let mut discard_receive = false;
        let mut discard_pending = false;
        let mut pending_receive = None;
        let mut pending_large = None;
        let mut pending_input_error = None;

        let mut peer_events = PeerEventDispatch::for_xpub(notify_xpub);
        let legacy_events = peer_out.legacy_sender();
        let event_out = peer_control
            .as_ref()
            .or(legacy_events.as_ref())
            .expect("codec control output");
        let control_credit = event_out.reserve();
        tokio::pin!(control_credit);
        let mut activation_budget = DrainBudget::new(64, 64 * 1024);
        loop {
            if !peer_events.drive(
                &mut connection,
                event_out,
                peer_id,
                recv_direct.as_mut(),
                completion,
                &mut peer_out,
                pending_receive.is_none(),
            ) {
                return Ok(());
            }
            if handshake_deadline.is_some()
                && connection.is_ready()
                && peer_events.handshake_admitted()
                && pending_input_error.is_none()
            {
                handshake_deadline = None;
            }

            // Authentication failures may have queued a mechanism ERROR.
            // Admit older events and flush that prefix through the select,
            // retaining setup cancellation and the original deadline.
            if pending_input_error.is_some()
                && !peer_events.blocked()
                && !connection.has_pending_transmit()
                && eq.is_empty()
            {
                return Err(pending_input_error.take().unwrap());
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

                cmd = inbox.recv() => {
                    let before = connection.pending_transmit_size();
                    match handle_pre_activation_inbox_command(
                        cmd,
                        &mut connection,
                        &mut recv_direct,
                        pending_input_error.is_some(),
                    )? {
                        PreActivationStep::Continue => {}
                        PreActivationStep::Activate => break,
                        PreActivationStep::Close => return Ok(()),
                    }
                    if !activation_budget.account(connection.pending_transmit_size().saturating_sub(before)) {
                        inbox.release_consumed();
                        tokio::task::yield_now().await;
                        activation_budget.reset();
                    }
                }

                permit = &mut control_credit, if peer_events.control_ready(&mut peer_out, pending_receive.is_none()) => {
                    match permit {
                        Ok(permit) => {
                            if !peer_events.send_reserved(permit, peer_id, completion, &mut peer_out) { return Ok(()); }
                        }
                        Err(_) => return Ok(()),
                    }
                    control_credit.set(event_out.reserve());
                }

                result = peer_out.ready(), if peer_events.notification_pending() && !peer_out.has_capacity() => {
                    if result.is_err() { return Ok(()); }
                }

                () = async { setup_cancel.as_ref().unwrap().cancelled().await; }, if setup_cancel.is_some() => {
                    return Ok(());
                }

                () = sleep_until_opt(handshake_deadline), if handshake_deadline.is_some() => {
                    return Err(pending_input_error.take().unwrap_or_else(||
                        Error::HandshakeFailed("handshake timeout".into())));
                }

                res = reader.read_buf(&mut read_buf), if pending_input_error.is_none() && !peer_events.blocked() && !connection.is_ready() && !connection.has_pending_input() => {
                    let n = res?;
                    if n == 0 {
                        mark_peer_dead(transmit_slot.as_deref());
                        cancel.cancel();
                        inbox.close();
                        return Ok(());
                    }
                    let input = read_stream_input(
                        n,
                        &mut connection,
                        &mut read_buf,
                        &mut read_buf_target,
                        &mut read_buf_full_reads,
                        &config,
                        &mut last_input,
                        &recv_pool,
                        &mut pending_large,
                    );
                    pending_input_error = input.err();
                }

                res = async {
                    flush_frame_buffer(&mut writer, &mut eq, &mut drain_buf).await?;
                    flush_once(&mut writer, &mut connection).await
                }, if want_write => {
                    if res.is_err() && let Some(error) = pending_input_error.take() {
                        return Err(error);
                    }
                    res?;
                }

                () = tokio::task::yield_now(), if !peer_events.has_pending() && (peer_events.needs_drain() || (pending_input_error.is_none() && connection.has_pending_input())) => {
                    if !peer_events.needs_drain() {
                        pending_input_error = connection.resume_input().err();
                    }
                }

            }
        }

        enable_transmit_slot_after_handshake(transmit_slot.as_deref(), &connection);
        let hb_interval = config
            .heartbeat_interval
            .filter(|_| connection.peer_minor() >= 1);
        let mut hb_deadline = hb_interval.and_then(|d| Instant::now().checked_add(d));
        let authenticated_sender = recv_direct
            .as_ref()
            .and_then(RecvSink::authenticated_sender);
        let authenticated_credit = super::reserve_authenticated(authenticated_sender.as_ref());
        tokio::pin!(authenticated_credit);
        loop {
            if pending_large
                .as_ref()
                .is_some_and(PendingLargeRead::complete)
            {
                pending_large
                    .take()
                    .unwrap()
                    .finish(&mut connection, &recv_pool)?;
                pending_large = PendingLargeRead::begin(&mut connection, &config, &recv_pool)?;
            }
            if !claim_direct_writer(transmit_slot.as_deref()) {
                return Ok(());
            }
            // A direct short write is older than all subsequently admitted
            // inbox/pipe messages and protocol commands.
            if pending_write.is_empty()
                && transmit_slot
                    .as_ref()
                    .is_some_and(|slot| slot.direct_writer().is_some() && !slot.is_empty())
            {
                stage_transmit_slot(
                    transmit_slot.as_ref().unwrap(),
                    &mut pending_write,
                    owned_chunks,
                );
            }
            let mut control_budget = DrainBudget::new(64, 64 * 1024);
            let mut control_exhausted = false;
            loop {
                match inbox.try_recv() {
                    Ok(cmd) => {
                        let before = connection.pending_transmit_size();
                        if handle_inbox_command(
                            Some(cmd),
                            &mut connection,
                            &mut graceful_close,
                            pending_input_error.is_some(),
                        )? == DriverStep::Close
                        {
                            return Ok(());
                        }
                        if !control_budget
                            .account(connection.pending_transmit_size().saturating_sub(before))
                        {
                            control_exhausted = true;
                            break;
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => return Ok(()),
                }
            }

            inbox.release_consumed();
            if control_exhausted {
                tokio::task::yield_now().await;
            }

            if pending_input_error.is_none() && connection.is_closed() && graceful_close.is_none() {
                // Peer-initiated WS CLOSE is a disconnect: stop accepting data
                // and finish the already-staged wire prefix plus the reply.
                graceful_close = Some(GracefulClose {
                    deadline: Instant::now().checked_add(Duration::from_secs(10)),
                    ws_close_started: true,
                    discard_receive: false,
                });
            }

            if let Some(close) = graceful_close {
                // Local close retires receives; a peer CLOSE preserves the
                // decoded message prefix, including blocked receive admission.
                discard_receive |= close.discard_receive;
                if let Some(data) = &mut data_inbox {
                    data.close();
                }
            }
            let data_plane_open = pending_input_error.is_none()
                && !graceful_close.is_some_and(|close| close.ws_close_started);
            if graceful_close
                .as_ref()
                .and_then(|close| close.deadline)
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return Ok(());
            }

            publish_direct_idle(
                transmit_slot.as_deref(),
                outbound_work_idle(&pending_write, &eq, &connection, &outbound)
                    && pipe_batch.is_empty()
                    && deferred_data.is_none()
                    && pending_input_error.is_none(),
                send_pipe_rx.as_ref(),
                data_inbox.as_ref(),
                &inbox,
            );
            // Protocol events on a separate mailbox remain reachable while
            // data waits. Standalone combined mailboxes retain their prefix.
            if (peer_control.is_some() || pending_receive.is_none())
                && !peer_events.drive(
                    &mut connection,
                    event_out,
                    peer_id,
                    recv_direct.as_mut(),
                    completion,
                    &mut peer_out,
                    pending_receive.is_none(),
                )
            {
                return Ok(());
            }
            if pending_input_error.is_some() && !peer_events.blocked() {
                return Err(pending_input_error.take().unwrap());
            }
            peer_out.set_control_prefix(peer_events.control_prefix());
            if peer_events.blocked() {
                // Preserve the codec-event prefix before decoded messages.
            } else if discard_receive {
                if pending_receive.take().is_some() {
                    authenticated_credit
                        .set(super::reserve_authenticated(authenticated_sender.as_ref()));
                }
                discard_pending = discard_received_messages(&mut connection);
            } else {
                match drain_decoded_messages(
                    &mut connection,
                    &mut decoder,
                    receive_profile,
                    MessageDelivery {
                        sink: &mut recv_direct,
                        peer_out: &mut peer_out,
                        peer_id,
                        completion,
                        pending: &mut pending_receive,
                        reserved_admission: authenticated_sender.is_some(),
                    },
                    ReceiveRateLimiters {
                        connection: &mut recv_rate_limiter,
                        ip: recv_ip_rate_limiter.as_ref(),
                    },
                    decoded_payload_pool.as_ref(),
                )? {
                    DriverStep::Continue => {}
                    DriverStep::Yield => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    DriverStep::Close => {
                        if socket_close_state
                            .as_ref()
                            .is_some_and(|state| state.is_closed())
                        {
                            // Closing the user receive pipe must not discard accepted
                            // transmit work before the drain command reaches us.
                            discard_receive = true;
                        } else {
                            return Ok(());
                        }
                    }
                }
            }

            if !claim_direct_writer(transmit_slot.as_deref()) {
                return Ok(());
            }
            if pending_write.is_empty()
                && transmit_slot
                    .as_ref()
                    .is_some_and(|slot| slot.direct_writer().is_some() && !slot.is_empty())
            {
                stage_transmit_slot(
                    transmit_slot.as_ref().unwrap(),
                    &mut pending_write,
                    owned_chunks,
                );
            }

            if data_plane_open
                && deferred_data.is_some()
                && pipe_batch.is_empty()
                && send_pipe_rx.as_ref().is_none_or(SendPipeConsumer::is_empty)
                && outbound_work_idle(&pending_write, &eq, &connection, &outbound)
            {
                let data = deferred_data.take().unwrap();
                deferred_data = handle_data_inbox(
                    data,
                    data_inbox.as_mut().expect("deferred data requires inbox"),
                    &mut outbound,
                    &mut connection,
                    &mut eq,
                )?;
                if let Some(slot) = &transmit_slot {
                    slot.space_available.notify_changed();
                }
            }

            if data_plane_open
                && !pipe_batch.is_empty()
                && outbound_work_idle(&pending_write, &eq, &connection, &outbound)
            {
                drain_send_pipe_batch(&mut pipe_batch, &mut outbound, &mut connection, &mut eq)?;
            }

            // Steady-state round-robin traffic normally leaves more work in
            // the existing yring send pipe. Drain it immediately after the
            // control check, then let the main select perform the write. The
            // readiness arm remains for an empty-to-nonempty race.
            if data_plane_open
                && pipe_batch.is_empty()
                && outbound_work_idle(&pending_write, &eq, &connection, &outbound)
                && send_pipe_rx
                    .as_ref()
                    .is_some_and(SendPipeConsumer::needs_drain)
            {
                match handle_send_pipe_ready(
                    &mut send_pipe_rx,
                    &mut pipe_batch,
                    &mut outbound,
                    &mut connection,
                    &mut eq,
                )? {
                    DriverStep::Continue => {}
                    DriverStep::Yield => tokio::task::yield_now().await,
                    DriverStep::Close => return Ok(()),
                }
            }

            // Latency-routed sends are encoded into the wire slot by
            // the caller. Stage already-queued work before polling the reader,
            // preserving the latency fast path without awaiting I/O here.
            if data_plane_open
                && latency_profile
                && outbound_work_idle(&pending_write, &eq, &connection, &outbound)
                && transmit_slot.as_ref().is_some_and(|slot| !slot.is_empty())
            {
                stage_transmit_slot(
                    transmit_slot.as_ref().unwrap(),
                    &mut pending_write,
                    owned_chunks,
                );
            }

            let shutdown_ready = graceful_close.is_some()
                && inbox.is_empty()
                && (discard_receive
                    || (pending_receive.is_none()
                        && !recv_direct.as_ref().is_some_and(RecvSink::peer_blocked)
                        && !peer_events.blocked()))
                && pending_write.is_empty()
                && eq.is_empty()
                && !connection.has_pending_transmit()
                && (!data_plane_open
                    || (!outbound.has_pending_offload()
                        && pipe_batch.is_empty()
                        && deferred_data.is_none()
                        && send_pipe_rx.as_ref().is_none_or(SendPipeConsumer::is_empty)
                        && data_inbox
                            .as_ref()
                            .is_none_or(super::data_inbox::Receiver::is_empty)
                        && transmit_slot.as_ref().is_none_or(|slot| slot.is_empty())));
            #[cfg(feature = "ws")]
            let shutdown_ready = if shutdown_ready && connection.is_ws() {
                let close = graceful_close.as_mut().unwrap();
                if close.ws_close_started {
                    connection.is_closed()
                } else {
                    connection.send_ws_close(1000);
                    close.ws_close_started = true;
                    false
                }
            } else {
                shutdown_ready
            };

            let want_write =
                !pending_write.is_empty() || connection.has_pending_transmit() || !eq.is_empty();
            let can_accept_data =
                data_plane_open && outbound_work_idle(&pending_write, &eq, &connection, &outbound);
            let message_blocked = !discard_receive
                && (pending_receive.is_some()
                    || recv_direct.as_ref().is_some_and(RecvSink::peer_blocked));
            let recv_blocked = message_blocked || peer_events.blocked();
            // A PONG can be behind application bytes we intentionally stopped
            // reading. Local backpressure is not evidence of a dead peer.
            if recv_blocked || was_recv_blocked {
                last_input = Instant::now();
            }
            was_recv_blocked = recv_blocked;
            let peer_ttl_deadline = connection
                .peer_heartbeat_ttl()
                .filter(|_| !recv_blocked && graceful_close.is_none())
                .and_then(|ttl| last_input.checked_add(ttl));

            publish_direct_idle(
                transmit_slot.as_deref(),
                outbound_work_idle(&pending_write, &eq, &connection, &outbound)
                    && pipe_batch.is_empty()
                    && deferred_data.is_none()
                    && pending_input_error.is_none(),
                send_pipe_rx.as_ref(),
                data_inbox.as_ref(),
                &inbox,
            );
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    if let Some(ref slot) = transmit_slot {
                        slot.mark_dead();
                    }
                    return Ok(());
                }

                () = sleep_until_opt(graceful_close.and_then(|close| close.deadline)),
                    if graceful_close.is_some_and(|close| close.deadline.is_some()) => return Ok(()),

                cmd = inbox.recv(), if !control_exhausted => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    if handle_inbox_command(cmd, &mut connection, &mut graceful_close, pending_input_error.is_some())? == DriverStep::Close {
                        return Ok(());
                    }
                },

                permit = &mut control_credit, if peer_events.control_ready(&mut peer_out, pending_receive.is_none()) => {
                    match permit {
                        Ok(permit) => {
                            if !peer_events.send_reserved(permit, peer_id, completion, &mut peer_out) { return Ok(()); }
                        }
                        Err(_) => return Ok(()),
                    }
                    control_credit.set(event_out.reserve());
                }

                result = peer_out.ready(),
                    if (pending_receive.is_some() && recv_direct.is_none())
                        || (pending_receive.is_none() && peer_events.notification_pending() && !peer_out.has_capacity()) => {
                    if result.is_err() { return Ok(()); }
                    peer_out.set_control_prefix(peer_events.control_prefix());
                    if recv_direct.is_none() && let Some(message) = pending_receive.take() {
                        match peer_out.try_send(peer_id, message, false) {
                            Ok(()) => completion.note_event(),
                            Err(super::SendPipeError::Full(message)) => pending_receive = Some(message),
                            Err(super::SendPipeError::Closed(_)) => return Ok(()),
                            #[cfg(feature = "dart")]
                            Err(crate::engine::SendPipeError::Invalid(_)) => unreachable!("receive output has no transport send validator"),
                        }
                    }
                }

                permit = &mut authenticated_credit,
                    if pending_receive.is_some() && authenticated_sender.is_some() => {
                    match permit {
                        Ok(permit) => {
                            let message = pending_receive.take().unwrap();
                            let _ = recv_direct.as_mut().unwrap().send_reserved(message, permit);
                        }
                        Err(_) => {
                            if socket_close_state.as_ref().is_some_and(|state| state.is_closed()) {
                                discard_receive = true;
                            } else {
                                return Ok(());
                            }
                        }
                    }
                    authenticated_credit.set(super::reserve_authenticated(authenticated_sender.as_ref()));
                }

                res = async {
                    if shutdown_ready {
                        writer.shutdown().await
                    } else {
                        write_driver_progress(
                            &mut writer,
                            &mut eq,
                            &mut pending_write,
                            &mut connection,
                            &mut hb_probe,
                        ).await
                    }
                }, if want_write || shutdown_ready => {
                    res?;
                    if shutdown_ready {
                        return Ok(());
                    }
                }

                // Latency-routed sends are written by the socket handle into
                // the slot. Poll this wakeup before the reader: otherwise a
                // reply can cause an unnecessary zero-time reactor poll
                // before the next request is written.
                () = async {
                    transmit_slot.as_ref().unwrap().data_signal.ready().await;
                }, if latency_profile && transmit_slot.as_ref().is_some_and(|s| {
                    s.handshake_done.load(Ordering::Acquire)
                }) && can_accept_data => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    stage_transmit_slot(
                        transmit_slot.as_ref().unwrap(), &mut pending_write, owned_chunks,
                    );
                }

                () = async {
                    recv_direct.as_mut().unwrap().receive_space_ready().await;
                }, if message_blocked && recv_direct.is_some() && authenticated_sender.is_none() => {}

                () = tokio::task::yield_now(), if !message_blocked && !peer_events.has_pending() && (peer_events.needs_drain() || (pending_input_error.is_none() && (connection.has_pending_input() || discard_pending))) => {
                    if !peer_events.needs_drain() && connection.has_pending_input() && !discard_pending {
                        pending_input_error = connection.resume_input().err();
                    }
                }

                res = async {
                    if let Some(large) = &mut pending_large {
                        large.read(&mut reader).await
                    } else {
                        reader.read_buf(&mut read_buf).await
                    }
                }, if pending_input_error.is_none() && !recv_blocked && !connection.has_pending_input() && !discard_pending && !connection.is_closed() => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    let n = res?;
                    if n == 0 {
                        mark_peer_dead(transmit_slot.as_deref());
                        cancel.cancel();
                        inbox.close();
                        return Ok(());
                    }
                    hb_probe.received_input();
                    if pending_large.is_some() {
                        last_input = Instant::now();
                    } else {
                        pending_input_error = read_stream_input(
                            n,
                            &mut connection,
                            &mut read_buf,
                            &mut read_buf_target,
                            &mut read_buf_full_reads,
                            &config,
                            &mut last_input,
                            &recv_pool,
                            &mut pending_large,
                        ).err();
                    }
                }

                // Drain completed offloaded compression in wire order. The
                // resulting frames are written by the write arm above.
                Some(first) = outbound.next_offload(), if data_plane_open && outbound.has_pending_offload() => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    outbound.drain_ready_offload_batch(first, &connection, &mut eq)?;
                }

                data = async {
                    data_inbox.as_mut().unwrap().recv().await
                }, if can_accept_data && data_inbox.is_some() && deferred_data.is_none() => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    match data {
                        Some(data) => {
                            deferred_data = Some(data);
                            if let Some(slot) = &transmit_slot { slot.space_available.notify_changed(); }
                        }
                        None => data_inbox = None,
                    }
                },

                // Wire-slot arm: the socket handle encoded ZMTP frames
                // into the per-peer PeerTransmitSlot. Stage one bounded batch;
                // the main write arm performs the I/O.
                () = async {
                    transmit_slot.as_ref().unwrap().data_signal.ready().await;
                }, if !latency_profile && transmit_slot.as_ref().is_some_and(|s| {
                    s.handshake_done.load(Ordering::Acquire)
                }) && can_accept_data => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    stage_transmit_slot(
                        transmit_slot.as_ref().unwrap(), &mut pending_write, owned_chunks,
                    );
                },

                // Per-peer send pipe: active round-robin pushes raw
                // messages to this driver, which encodes and writes locally.
                () = async {
                    send_pipe_rx.as_ref().unwrap().ready().await;
                }, if send_pipe_rx.is_some() && pipe_batch.is_empty() && can_accept_data => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                },

                () = sleep_until_opt(peer_ttl_deadline), if peer_ttl_deadline.is_some() && pending_input_error.is_none() => {
                    return Err(Error::Timeout);
                }

                // Heartbeat tick: enabled only post-handshake when
                // `heartbeat_interval` is set. Uses a persistent pinned
                // sleep so the safety-net timeout doesn't reset it.
                //
                // Only time a probe after its complete wire prefix reaches
                // the writer. Locally queued probes cannot prove silence.
                // Any actual peer input acknowledges the outstanding probe.
                () = sleep_until_opt(hb_deadline), if hb_deadline.is_some() && graceful_close.is_none() && pending_input_error.is_none() => {
                    if !claim_direct_writer(transmit_slot.as_deref()) { return Ok(()); }
                    if !recv_blocked && hb_probe.timed_out(last_input, hb_timeout) {
                        return Err(Error::Timeout);
                    }
                    if hb_probe.can_queue() {
                        let ping = Command::Ping {
                            ttl_deciseconds: hb_ttl_deciseconds,
                            context: Bytes::new(),
                        };
                        connection.send_command(&ping)?;
                        hb_probe.queued_through = Some(connection.pending_transmit_size());
                    }
                    hb_deadline = hb_interval.and_then(|d| Instant::now().checked_add(d));
                }

                () = tokio::task::yield_now(), if control_exhausted => {}

            }
        }
    }
}

/// Abort/error cleanup must retire a direct route even when `run()` is dropped.
struct DirectWriteLifetime(Option<Arc<PeerTransmitSlot>>);

impl Drop for DirectWriteLifetime {
    fn drop(&mut self) {
        if let Some(slot) = &self.0 {
            slot.mark_dead();
        }
    }
}

fn claim_direct_writer(slot: Option<&PeerTransmitSlot>) -> bool {
    slot.and_then(PeerTransmitSlot::direct_writer)
        .is_none_or(|writer| writer.claim_driver())
}

fn publish_direct_idle(
    slot: Option<&PeerTransmitSlot>,
    local_empty: bool,
    pipe: Option<&SendPipeConsumer>,
    data: Option<&super::data_inbox::Receiver>,
    control: &super::control_inbox::Receiver,
) {
    let Some(slot) = slot else { return };
    let Some(writer) = slot.direct_writer() else {
        return;
    };
    if !local_empty || !slot.handshake_done.load(Ordering::Acquire) {
        return;
    }
    writer.publish_idle(|| {
        slot.is_empty()
            && pipe.is_none_or(SendPipeConsumer::is_empty)
            && data.is_none_or(super::data_inbox::Receiver::is_empty)
            && control.is_empty()
    });
}

fn close_error_reason(err: &Error) -> String {
    match err {
        Error::HandshakeFailed(reason) => reason.clone(),
        Error::HandshakeRefused(refusal) => refusal.to_string(),
        other => other.to_string(),
    }
}

struct ReceiveRateLimiters<'a> {
    connection: &'a mut Option<TokenBucket>,
    ip: Option<&'a (Arc<SharedIpRateLimiter>, IpAddr)>,
}

struct MessageDelivery<'a> {
    sink: &'a mut Option<RecvSink>,
    peer_out: &'a mut PeerOutput,
    peer_id: u64,
    completion: &'a mut CompletionProgress,
    pending: &'a mut Option<Message>,
    reserved_admission: bool,
}

impl MessageDelivery<'_> {
    fn blocked(&self) -> bool {
        self.pending.is_some() || self.sink.as_ref().is_some_and(RecvSink::peer_blocked)
    }

    fn retry_pending(&mut self) -> bool {
        if self
            .sink
            .as_mut()
            .is_some_and(|sink| !sink.retry_peer_pending())
        {
            return false;
        }
        if self.pending.is_some() && (self.sink.is_none() || self.reserved_admission) {
            return true;
        }
        if let Some(message) = self.pending.take() {
            return self.send(message, false, &mut false);
        }
        true
    }

    #[inline]
    fn send(&mut self, message: Message, defer: bool, pending_flush: &mut bool) -> bool {
        if let Some(RecvSink::Fanin(sink)) = self.sink {
            return if defer {
                *pending_flush = true;
                sink.push_deferred(message)
            } else {
                sink.push(message)
            };
        }
        let result = if let Some(sink) = self.sink {
            sink.try_send_with_flush_mode(message, defer, pending_flush)
        } else {
            match self.peer_out.try_send(self.peer_id, message, false) {
                Ok(()) => {
                    self.completion.note_event();
                    Ok(())
                }
                Err(super::SendPipeError::Full(message)) => Err(TrySendError::Full(message)),
                Err(super::SendPipeError::Closed(_)) => Err(TrySendError::Closed),
                #[cfg(feature = "dart")]
                Err(crate::engine::SendPipeError::Invalid(_)) => {
                    unreachable!("receive output has no transport send validator")
                }
            }
        };
        match result {
            Ok(()) => true,
            Err(TrySendError::Full(message)) => {
                *self.pending = Some(message);
                true
            }
            Err(_) => false,
        }
    }

    fn flush(&mut self, pending: &mut bool) {
        if let Some(sink) = self.sink {
            sink.flush_deferred(pending);
        }
    }
}

fn drain_decoded_messages(
    connection: &mut Connection,
    decoder: &mut Option<MessageDecoder>,
    receive_profile: ReceiveProfile,
    mut delivery: MessageDelivery<'_>,
    rate_limiters: ReceiveRateLimiters<'_>,
    decoded_payload_pool: Option<&omq_proto::PayloadPool>,
) -> Result<DriverStep> {
    let ReceiveRateLimiters {
        connection: rate_connection,
        ip: rate_ip,
    } = rate_limiters;
    if !delivery.retry_pending() {
        return Ok(DriverStep::Close);
    }
    if delivery.blocked() {
        return Ok(DriverStep::Continue);
    }
    let recv_batch_start = Instant::now();
    let mut recv_budget = None;
    let mut recv_batch_time = None;
    let defer_yring_flush = decoder.is_none()
        && matches!(receive_profile, ReceiveProfile::Throughput)
        && delivery.sink.as_ref().is_some_and(RecvSink::is_yring);
    let mut pending_yring_flush = false;
    while let Some(m) = connection.poll_message() {
        let m = match decoder.as_mut() {
            Some(dec) => match dec.decode_with_payload_pool(m, decoded_payload_pool)? {
                Some(plain) => plain,
                None => continue,
            },
            None => m,
        };
        let rate_limited = rate_connection
            .as_mut()
            .is_some_and(|limiter| !limiter.allow(recv_batch_start))
            || rate_ip.is_some_and(|(limiter, ip)| !limiter.allow(*ip, recv_batch_start));
        if rate_limited {
            delivery.flush(&mut pending_yring_flush);
            return Err(Error::ReceiveRateLimitExceeded);
        }
        let msg_bytes = m.byte_len();
        let budget = recv_budget.get_or_insert_with(|| {
            recv_batch_time = receive_profile.time(msg_bytes);
            receive_profile.budget(msg_bytes)
        });
        if !delivery.send(m, defer_yring_flush, &mut pending_yring_flush) {
            delivery.flush(&mut pending_yring_flush);
            return Ok(DriverStep::Close);
        }
        let budget_remains = budget.account(msg_bytes);
        if delivery.blocked() {
            delivery.flush(&mut pending_yring_flush);
            return Ok(DriverStep::Continue);
        }
        let time_check = budget.msgs().is_multiple_of(32);
        if !budget_remains
            || (time_check
                && recv_batch_time.is_some_and(|limit| recv_batch_start.elapsed() >= limit))
        {
            delivery.flush(&mut pending_yring_flush);
            return Ok(DriverStep::Yield);
        }
    }
    delivery.flush(&mut pending_yring_flush);
    Ok(DriverStep::Continue)
}

/// Closed sockets no longer deliver receives, but must keep parsing control
/// traffic while accepted output drains. Bound even the discard work.
fn discard_received_messages(connection: &mut Connection) -> bool {
    let mut budget = DrainBudget::new(256, 1024 * 1024);
    while let Some(message) = connection.poll_message() {
        if !budget.account(message.byte_len()) {
            return true;
        }
    }
    false
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
fn read_stream_input(
    n: usize,
    connection: &mut Connection,
    read_buf: &mut BytesMut,
    read_buf_target: &mut usize,
    read_buf_full_reads: &mut usize,
    config: &PeerDriverConfig,
    last_input: &mut Instant,
    recv_pool: &Arc<RecvBufPool>,
    pending_large: &mut Option<PendingLargeRead>,
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
    connection.handle_input(chunk)?;
    *pending_large = PendingLargeRead::begin(connection, config, recv_pool)?;
    // Parsing (and copying any large-frame prefix into its pooled payload)
    // can release the split chunk. Reserve afterward so BytesMut can reclaim
    // that allocation instead of replacing it while the chunk still owns it.
    // Retain the existing adaptive refill policy: the target is a growth hint,
    // not a minimum spare-capacity promise after every partial read. Requiring
    // the full target here replaces buffers still shared by decoded metadata.
    read_buf.reserve(read_buf_target.saturating_sub(read_buf.capacity()));
    Ok(())
}

fn handle_pre_activation_inbox_command(
    cmd: Option<PeerDriverCommand>,
    connection: &mut Connection,
    recv_direct: &mut Option<RecvSink>,
    input_failed: bool,
) -> Result<PreActivationStep> {
    match cmd {
        Some(PeerDriverCommand::ActivateDataPlane) => Ok(if input_failed {
            PreActivationStep::Continue
        } else {
            PreActivationStep::Activate
        }),
        Some(PeerDriverCommand::ActivateWithRecvSink(_)) if input_failed => {
            Ok(PreActivationStep::Continue)
        }
        Some(PeerDriverCommand::ActivateWithRecvSink(sink)) => {
            *recv_direct = Some(sink);
            Ok(PreActivationStep::Activate)
        }
        Some(PeerDriverCommand::SendCommand(c)) => {
            if !input_failed {
                connection.send_command(&c)?;
            }
            Ok(PreActivationStep::Continue)
        }
        Some(PeerDriverCommand::Close | PeerDriverCommand::DrainAndClose { .. }) | None => {
            Ok(PreActivationStep::Close)
        }
    }
}

fn handle_inbox_command(
    cmd: Option<PeerDriverCommand>,
    connection: &mut Connection,
    graceful_close: &mut Option<GracefulClose>,
    input_failed: bool,
) -> Result<DriverStep> {
    match cmd {
        Some(PeerDriverCommand::ActivateDataPlane) => Ok(DriverStep::Continue),
        Some(PeerDriverCommand::ActivateWithRecvSink(_)) if input_failed => {
            Ok(DriverStep::Continue)
        }
        Some(PeerDriverCommand::ActivateWithRecvSink(_)) => Err(Error::Protocol(
            "receive sink cannot change after activation".into(),
        )),
        Some(PeerDriverCommand::SendCommand(c)) => {
            if !input_failed && !graceful_close.is_some_and(|close| close.ws_close_started) {
                connection.send_command(&c)?;
            }
            Ok(DriverStep::Continue)
        }
        Some(PeerDriverCommand::DrainAndClose { deadline }) => {
            match graceful_close {
                Some(close) => {
                    close.discard_receive = true;
                    close.deadline = match (close.deadline, deadline) {
                        (Some(current), Some(requested)) => Some(current.min(requested)),
                        (current, requested) => current.or(requested),
                    };
                }
                None => {
                    *graceful_close = Some(GracefulClose {
                        deadline,
                        ws_close_started: false,
                        discard_receive: true,
                    });
                }
            }
            Ok(DriverStep::Continue)
        }
        Some(PeerDriverCommand::Close) | None => Ok(DriverStep::Close),
    }
}

fn handle_data_inbox(
    first: PeerDriverData,
    data_inbox: &mut super::data_inbox::Receiver,
    outbound: &mut OutboundState,
    connection: &mut Connection,
    eq: &mut FrameBuffer,
) -> Result<Option<PeerDriverData>> {
    let mut deferred = None;
    match first {
        PeerDriverData::SendMessage(first) => {
            outbound.batch_encode(
                &first,
                || match data_inbox.try_recv() {
                    Ok(PeerDriverData::SendMessage(message)) => Some(message),
                    Ok(data @ PeerDriverData::SendEncoded(_)) => {
                        deferred = Some(data);
                        None
                    }
                    Err(_) => None,
                },
                OUTBOUND_BATCH_MAX_MSGS,
                connection,
                eq,
            )?;
        }
        PeerDriverData::SendEncoded(chunks) => eq.push_shared_chunks(&chunks),
    }
    data_inbox.release_consumed();
    Ok(deferred)
}

fn handle_send_pipe_ready(
    send_pipe_rx: &mut Option<SendPipeConsumer>,
    pipe_batch: &mut Vec<Message>,
    outbound: &mut OutboundState,
    connection: &mut Connection,
    eq: &mut FrameBuffer,
) -> Result<DriverStep> {
    let rx = send_pipe_rx.as_mut().expect("send pipe select guard");
    if outbound.encoder.is_none() {
        // Untransformed messages need no staging vector or reverse pass.
        // Encode in FIFO order while retaining the same drain/time budgets.
        let started = Instant::now();
        let mut count = 0usize;
        let mut encoded = Ok(());
        if let Some((drained, _)) =
            rx.drain_queue_with(OUTBOUND_BATCH_MAX_MSGS, max_batch_bytes(), |message| {
                encoded = encode_msg(&message, &mut outbound.encoder, connection, eq, None);
                count += 1;
                encoded.is_ok()
                    && (!count.is_multiple_of(32) || started.elapsed() < OUTBOUND_BATCH_TIME)
            })
        {
            encoded?;
            return Ok(if drained != 0 {
                DriverStep::Continue
            } else if rx.is_disconnected() {
                DriverStep::Close
            } else {
                DriverStep::Yield
            });
        }
    }
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
    pipe_batch.reverse();
    drain_send_pipe_batch(pipe_batch, outbound, connection, eq)?;
    // Producer EOF does not mean the bytes just encoded reached the writer.
    // A subsequent idle drain can close after this final batch is flushed.
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

/// Move one bounded transmit-slot batch into persistent driver-owned state.
/// No I/O happens here: the main `select!` owns every potentially blocking
/// write.
fn stage_transmit_slot(slot: &PeerTransmitSlot, pending: &mut PendingWrite, owned: bool) {
    if pending.stage_slot(slot, owned) {
        slot.space_available.notify_changed();
    }
}

fn outbound_work_idle(
    pending: &PendingWrite,
    eq: &FrameBuffer,
    connection: &Connection,
    outbound: &OutboundState,
) -> bool {
    pending.is_empty()
        && eq.is_empty()
        && !connection.has_pending_transmit()
        && !outbound.has_pending_offload()
}

/// A queued heartbeat owns one prefix of the codec transmit queue. Writes
/// from arenas/slots do not advance it. Writer admission does not acknowledge
/// remote delivery, especially through ordered TCP/WS or TLS buffering.
#[derive(Debug, Default)]
struct HeartbeatProbe {
    queued_through: Option<usize>,
    sent_at: Option<Instant>,
}

impl HeartbeatProbe {
    fn can_queue(&self) -> bool {
        // Keep periodic traffic when our receive queue is blocked: the peer
        // may need that traffic for its own heartbeat/advertised TTL. Never
        // accumulate multiple probes behind a locally stalled writer.
        self.queued_through.is_none()
    }

    fn advance(&mut self, written: usize) {
        let Some(remaining) = self.queued_through else {
            return;
        };
        if written < remaining {
            self.queued_through = Some(remaining - written);
        } else {
            self.queued_through = None;
            // A later keepalive must not postpone an unanswered probe's
            // original silence deadline.
            self.sent_at.get_or_insert_with(Instant::now);
        }
    }

    fn received_input(&mut self) {
        // Earlier peer traffic cannot acknowledge a probe still queued here.
        self.sent_at = None;
    }

    fn timed_out(&self, last_input: Instant, timeout: Duration) -> bool {
        self.sent_at
            .is_some_and(|sent_at| sent_at.max(last_input).elapsed() > timeout)
    }
}

/// Make bounded wire progress. This future is polled directly by the main
/// `select!`; if control wins, all destructively drained chunks remain owned
/// by `pending` and the write can safely resume later.
async fn write_driver_progress<W: DriverWrite>(
    writer: &mut W,
    eq: &mut FrameBuffer,
    pending: &mut PendingWrite,
    connection: &mut Connection,
    heartbeat: &mut HeartbeatProbe,
) -> io::Result<()> {
    let started = Instant::now();
    let mut budget = DrainBudget::WIRE_DRAIN;
    let owned = writer.chunk_writer().is_some();
    loop {
        let written = if owned && pending.has_chunks() {
            let owned_writer = writer.chunk_writer().expect("owned-chunk writer");
            let chunks = pending.chunks_mut();
            let written =
                std::future::poll_fn(|cx| owned_writer.poll_write_chunks(cx, chunks)).await?;
            if written == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
            }
            pending.advance_owned(written);
            written
        } else if !pending.is_empty() {
            let iovecs = pending.io_slices();
            let written = writer.write_vectored(&iovecs).await?;
            drop(iovecs);
            if written == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
            }
            pending.advance(written);
            written
        } else if owned && !eq.is_empty() {
            pending.stage_owned(eq);
            continue;
        } else if eq.has_arena_only() {
            let written = {
                let data = eq.arena_bytes();
                writer.write_vectored(&[io::IoSlice::new(data)]).await?
            };
            if written == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
            }
            eq.advance_arena(written);
            written
        } else if !eq.is_empty() {
            pending.stage(eq);
            continue;
        } else if connection.has_pending_transmit() {
            let written = flush_once(writer, connection).await?;
            // Record successful writes before any further await. The main
            // select may cancel this future on its next pending write.
            heartbeat.advance(written);
            written
        } else {
            return Ok(());
        };

        let budget_remains = budget.account(written);
        let work_remains =
            !pending.is_empty() || !eq.is_empty() || connection.has_pending_transmit();
        if !budget_remains || !work_remains || started.elapsed() >= OUTBOUND_BATCH_TIME {
            return Ok(());
        }
    }
}

fn drain_send_pipe_batch(
    batch: &mut Vec<Message>,
    outbound: &mut OutboundState,
    connection: &mut Connection,
    eq: &mut FrameBuffer,
) -> Result<()> {
    if let Some(first) = batch.pop() {
        outbound.batch_encode(
            &first,
            || batch.pop(),
            OUTBOUND_BATCH_MAX_MSGS,
            connection,
            eq,
        )?;
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
            buf.clear();
            if buf.capacity() < capacity {
                buf.reserve(capacity);
            }
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

/// Own a claimed payload across cancel-safe, bounded reads in the main select.
#[derive(Debug)]
struct PendingLargeRead {
    buf: ReceiveBody,
    target: usize,
}

#[derive(Debug)]
enum ReceiveBody {
    Stream { buf: BytesMut, pooled: bool },
    Payload(omq_proto::PayloadBuffer),
}

impl PendingLargeRead {
    fn begin(
        connection: &mut Connection,
        config: &PeerDriverConfig,
        pool: &Arc<RecvBufPool>,
    ) -> Result<Option<Self>> {
        #[cfg(feature = "ws")]
        let skip_large = connection.is_ws();
        #[cfg(not(feature = "ws"))]
        let skip_large = false;
        if matches!(config.large_message_threshold, 0 | usize::MAX)
            || connection.has_frame_transform()
            || skip_large
        {
            return Ok(None);
        }
        let Some(info) = connection.peek_next_frame_payload_size()? else {
            return Ok(None);
        };
        // Complete frames retain their byte views, including frames left by
        // a bounded codec turn. No pool checkout should make a second copy.
        if info.buffered_payload_prefix == info.payload_len {
            return Ok(None);
        }
        let payload_buffer = if info.flags.command {
            None
        } else {
            connection.try_recv_payload_buffer(info.payload_len)
        };
        if payload_buffer.is_none() && info.payload_len < config.large_message_threshold {
            return Ok(None);
        }
        if let Some(mut buffer) = payload_buffer {
            let Some((target, filled)) =
                connection.begin_supplied_payload_into(buffer.writable())?
            else {
                return Ok(None);
            };
            copy_stats::record(Site::RecvLargePrefix, filled);
            buffer.set_len(filled).expect("selected slot fits body");
            return Ok(Some(Self {
                buf: ReceiveBody::Payload(buffer),
                target,
            }));
        }
        let Some((target, prefix)) = connection.begin_supplied_payload_with_prefix() else {
            return Ok(None);
        };
        // Release the shared input prefix before the caller refills read_buf.
        copy_stats::record(Site::RecvLargePrefix, prefix.len());
        let pooled = target <= RECV_POOL_MAX_BUFFER_BYTES;
        let mut buf = if pooled {
            pool.take(target)
        } else {
            BytesMut::with_capacity(target)
        };
        buf.extend_from_slice(prefix.as_slice());
        Ok(Some(Self {
            buf: ReceiveBody::Stream { buf, pooled },
            target,
        }))
    }

    fn filled(&self) -> usize {
        match &self.buf {
            ReceiveBody::Stream { buf, .. } => buf.len(),
            ReceiveBody::Payload(buf) => buf.len(),
        }
    }

    fn complete(&self) -> bool {
        self.filled() == self.target
    }

    async fn read<R: AsyncRead + Unpin>(&mut self, reader: &mut R) -> io::Result<usize> {
        let filled = self.filled();
        let remaining = (self.target - filled).min(64 * 1024);
        let n = match &mut self.buf {
            ReceiveBody::Stream { buf, .. } => {
                let mut limited = buf.limit(remaining);
                reader.read_buf(&mut limited).await?
            }
            ReceiveBody::Payload(buf) => {
                let n = reader
                    .read(&mut buf.writable()[filled..filled + remaining])
                    .await?;
                buf.set_len(filled + n).expect("read stays within body");
                n
            }
        };
        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        Ok(n)
    }

    fn finish(self, connection: &mut Connection, pool: &Arc<RecvBufPool>) -> Result<()> {
        debug_assert!(self.complete());
        let payload = match self.buf {
            ReceiveBody::Payload(buf) => buf.into_payload(),
            ReceiveBody::Stream { buf, pooled } => {
                let capacity = buf.capacity();
                let bytes = if pooled { pool.wrap(buf) } else { buf.freeze() };
                omq_proto::Payload::from_bytes_with_retained_size(bytes, capacity)
            }
        };
        connection.supply_payload_frame(payload)
    }
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

#[cfg(test)]
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
async fn flush_once<W>(writer: &mut W, connection: &mut Connection) -> io::Result<usize>
where
    W: AsyncWrite + Unpin,
{
    let chunks = connection.transmit_chunks_capped(128);
    if chunks.is_empty() {
        return Ok(0);
    }
    let n = writer.write_vectored(&chunks).await?;
    drop(chunks);
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
    }
    connection.advance_transmit(n);
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::super::signal::StateSignal;
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
    struct StalledShutdownStream {
        stream: DuplexStream,
        shutdown_seen: Arc<std::sync::atomic::AtomicBool>,
    }

    impl DriverStream for StalledShutdownStream {
        type Reader = tokio::io::ReadHalf<DuplexStream>;
        type Writer = StalledShutdownWriter;

        fn split(self, _fast_write: bool) -> (Self::Reader, Self::Writer) {
            let (reader, writer) = tokio::io::split(self.stream);
            (
                reader,
                StalledShutdownWriter {
                    writer,
                    shutdown_seen: self.shutdown_seen,
                },
            )
        }
    }

    #[derive(Debug)]
    struct StalledShutdownWriter {
        writer: tokio::io::WriteHalf<DuplexStream>,
        shutdown_seen: Arc<std::sync::atomic::AtomicBool>,
    }

    impl AsyncWrite for StalledShutdownWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.writer).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.writer).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.shutdown_seen.store(true, Ordering::Release);
            Poll::Pending
        }
    }

    #[derive(Debug)]
    struct ChoppyDuplex {
        inner: DuplexStream,
        read_cap: usize,
        write_cap: usize,
    }

    #[cfg(feature = "plain")]
    #[derive(Debug)]
    struct GreetingOnlyStream {
        stream: DuplexStream,
        stalled: Arc<StateSignal>,
    }

    #[cfg(feature = "plain")]
    impl DriverStream for GreetingOnlyStream {
        type Reader = tokio::io::ReadHalf<DuplexStream>;
        type Writer = GreetingOnlyWriter;

        fn split(self, _fast_write: bool) -> (Self::Reader, Self::Writer) {
            let (reader, writer) = tokio::io::split(self.stream);
            (
                reader,
                GreetingOnlyWriter {
                    writer,
                    remaining: 64,
                    stalled: self.stalled,
                },
            )
        }
    }

    #[cfg(feature = "plain")]
    #[derive(Debug)]
    struct GreetingOnlyWriter {
        writer: tokio::io::WriteHalf<DuplexStream>,
        remaining: usize,
        stalled: Arc<StateSignal>,
    }

    #[cfg(feature = "plain")]
    impl AsyncWrite for GreetingOnlyWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.remaining == 0 {
                self.stalled.notify_changed();
                return Poll::Pending;
            }
            let length = self.remaining.min(buf.len());
            match Pin::new(&mut self.writer).poll_write(cx, &buf[..length]) {
                Poll::Ready(Ok(written)) => {
                    self.remaining -= written;
                    Poll::Ready(Ok(written))
                }
                other => other,
            }
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.writer).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.writer).poll_shutdown(cx)
        }
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

    impl DriverWrite for StalledShutdownWriter {}
    #[cfg(feature = "plain")]
    impl DriverWrite for GreetingOnlyWriter {}
    impl<W: AsyncWrite + Send + Unpin + 'static> DriverWrite for ChoppyWriter<W> {}

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

        assert!(matches!(sink.producer.push(Message::single("a")), Ok(())));
        sink.flush_and_signal();
        assert!(matches!(sink.producer.push(Message::single("b")), Ok(())));
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
    fn recv_buf_pool_grows_before_returning_a_larger_lease() {
        let pool = RecvBufPool::new();
        pool.give(BytesMut::with_capacity(128 * 1024));
        let buffer = pool.take(192 * 1024);
        assert!(buffer.is_empty());
        assert!(buffer.capacity() >= 192 * 1024);
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
    async fn heartbeat_queued_behind_stalled_data_does_not_kill_the_peer() {
        queued_heartbeat_with_stalled_output(
            ConnectionConfig::new(Role::Server, SocketType::Pair),
            ConnectionConfig::new(Role::Client, SocketType::Pair),
        )
        .await;
    }

    #[cfg(feature = "ws")]
    #[tokio::test]
    async fn ws_heartbeat_queued_behind_stalled_data_does_not_kill_the_peer() {
        use omq_proto::proto::connection::WsRole;
        queued_heartbeat_with_stalled_output(
            ConnectionConfig::new(Role::Server, SocketType::Pair).ws_role(WsRole::Server),
            ConnectionConfig::new(Role::Client, SocketType::Pair).ws_role(WsRole::Client),
        )
        .await;
    }

    async fn queued_heartbeat_with_stalled_output(
        server_config: ConnectionConfig,
        client_config: ConnectionConfig,
    ) {
        let (server_stream, client_stream) = tokio::io::duplex(1024);
        let (producer, _consumer) = yring::spsc(1);
        let (server_control, server_inbox) = mpsc::channel(8);
        let (client_control, client_inbox) = mpsc::channel(8);
        let (client_data, client_data_inbox) = mpsc::channel(64);
        let (server_events, mut server_event_inbox) = mpsc::channel(8);
        let (client_events, mut client_event_inbox) = mpsc::channel(8);
        let server_cancel = CancellationToken::new();
        let client_cancel = CancellationToken::new();
        let server = ConnectionDriver::new(
            server_stream,
            Connection::new(server_config),
            server_inbox,
            server_events,
            1,
            server_cancel.clone(),
        )
        .with_recv_sink(RecvSink::Yring(YringSink {
            producer,
            signal: Box::new(|| {}),
            space: Arc::new(StateSignal::new()),
        }));
        let client = ConnectionDriver::with_config(
            client_stream,
            Connection::new(client_config),
            client_inbox,
            client_events,
            2,
            client_cancel.clone(),
            PeerDriverConfig {
                heartbeat_interval: Some(Duration::from_millis(50)),
                heartbeat_timeout: Some(Duration::from_millis(150)),
                ..PeerDriverConfig::default()
            },
        )
        .with_data_inbox(client_data_inbox);
        let server_task = tokio::spawn(server.run());
        let client_task = tokio::spawn(client.run());
        assert!(matches!(
            server_event_inbox.recv().await.unwrap().1,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
        assert!(matches!(
            client_event_inbox.recv().await.unwrap().1,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
        server_control
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        client_control
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        for _ in 0..32 {
            client_data
                .send(PeerDriverData::SendMessage(Message::single(Bytes::from(
                    vec![7; 1024],
                ))))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(
            !client_control.is_closed(),
            "a locally queued PING cannot establish peer silence"
        );
        client_control.send(PeerDriverCommand::Close).await.unwrap();
        tokio::time::timeout(Duration::from_millis(500), client_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server_cancel.cancel();
        tokio::time::timeout(Duration::from_millis(500), server_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn selected_write_cancellation_preserves_external_chunks() {
        const MSG_SIZE: usize = 1024 * 1024;
        let payload = patterned_payload(MSG_SIZE, 7);
        let mut expected = Vec::with_capacity(MSG_SIZE + 9);
        push_expected_single_frame(&mut expected, &payload);

        let mut eq = FrameBuffer::one_shot();
        eq.frame(&Message::single(Bytes::from(payload)));
        let mut pending = PendingWrite::default();
        let mut connection = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
        let _ = drain_transmit(&mut connection);
        let mut heartbeat = HeartbeatProbe::default();
        let (stream, mut peer) = tokio::io::duplex(64);
        let (reader_half, mut writer) = tokio::io::split(stream);
        drop(reader_half);

        tokio::select! {
            biased;
            result = write_driver_progress(
                &mut writer,
                &mut eq,
                &mut pending,
                &mut connection,
                &mut heartbeat,
            ) => panic!("stalled write unexpectedly completed: {result:?}"),
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        assert!(
            !pending.is_empty(),
            "cancelled write dropped pending chunks"
        );

        let reader = tokio::spawn(async move {
            let mut actual = Vec::new();
            peer.read_to_end(&mut actual).await.unwrap();
            actual
        });
        while !pending.is_empty() || !eq.is_empty() {
            write_driver_progress(
                &mut writer,
                &mut eq,
                &mut pending,
                &mut connection,
                &mut heartbeat,
            )
            .await
            .unwrap();
        }
        drop(writer);
        assert_eq!(reader.await.unwrap(), expected);
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

        assert_eq!(chunks, [] as [Bytes; 0]);
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
    async fn read_stream_reuses_prefix_buffer_after_large_frame_consumes_it() {
        let (mut push, mut pull) = ready_push_pull_connections();
        let payload = vec![0x5a; 1024 * 1024];
        push.send_message(&Message::single(Bytes::from(payload.clone())))
            .unwrap();
        let wire = Bytes::from(drain_transmit(&mut push));
        let mut read_buf = BytesMut::with_capacity(READ_BUF_MAX);
        read_buf.extend_from_slice(&wire[..READ_BUF_MAX]);
        let original = read_buf.as_ptr();
        let mut target = READ_BUF_MAX;
        let mut full_reads = 0;
        let mut reader = ScriptedReader::new(wire.slice(READ_BUF_MAX..), [4093, 65537, 8191]);
        let config = PeerDriverConfig {
            large_message_threshold: 128 * 1024,
            ..PeerDriverConfig::default()
        };
        let mut last_input = Instant::now();
        let pool = RecvBufPool::new();
        let mut pending_large = None;
        read_stream_input(
            READ_BUF_MAX,
            &mut pull,
            &mut read_buf,
            &mut target,
            &mut full_reads,
            &config,
            &mut last_input,
            &pool,
            &mut pending_large,
        )
        .unwrap();
        assert!(read_buf.is_empty());
        assert!(read_buf.capacity() >= target);
        assert_eq!(
            read_buf.as_ptr(),
            original,
            "released prefix allocation should be reused"
        );
        let mut large = pending_large.take().unwrap();
        while !large.complete() {
            large.read(&mut reader).await.unwrap();
        }
        large.finish(&mut pull, &pool).unwrap();
        let message = pull.poll_message().expect("complete large frame");
        assert_eq!(message.part_bytes(0).unwrap().as_ref(), payload);
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

    #[test]
    fn full_rep_sink_returns_original_message_without_internal_route() {
        let (producer, mut consumer) = yring::spsc(1);
        let mut sink = RecvSink::rep(
            RecvSink::Yring(YringSink {
                producer,
                signal: Box::new(|| {}),
                space: Arc::new(StateSignal::new()),
            }),
            7,
        );
        sink.try_deliver(Message::multipart(["", "first"])).unwrap();
        let original = Message::multipart(["", "second"]).with_routing_id(99);
        let Err(omq_proto::TrySendError::Full(returned)) = sink.try_deliver(original.clone())
        else {
            panic!("full sink must return the request");
        };
        assert_eq!(returned, original);
        assert_eq!(returned.routing_id(), Some(99));
        let received = consumer.prefetch_and_pop().unwrap();
        assert_eq!(received.routing_id(), Some(8));
        assert_eq!(received.get(1), Some(b"first".as_slice()));
    }

    #[test]
    fn receive_config_recycles_only_its_owner_and_retains_pending_queues() {
        let (producer, mut initial_consumer) = yring::spsc(4);
        let config = RecvSinkConfig::new(
            RecvSink::Yring(YringSink {
                producer,
                signal: Box::new(|| {}),
                space: Arc::new(StateSignal::new()),
            }),
            Arc::new(|| {}),
            Arc::new(StateSignal::new()),
            4,
        );
        let mut first = config.take_sink_for_peer(0).unwrap();
        config.peer_disconnected(1);
        assert!(config.take_sink_for_peer(2).is_none());
        first.try_deliver(Message::single("first")).unwrap();
        assert_eq!(
            initial_consumer.prefetch_and_pop(),
            Some(Message::single("first"))
        );
        drop(first);
        config.peer_disconnected(0);
        let mut second = config.take_sink_for_peer(2).unwrap();
        second.try_deliver(Message::single("second")).unwrap();
        drop(second);
        config.peer_disconnected(2);
        assert!(
            config.take_sink_for_peer(3).is_none(),
            "pending ring must survive churn"
        );
        let mut pending = config.try_take_pending_consumer().unwrap();
        assert_eq!(pending.prefetch_and_pop(), Some(Message::single("second")));
        let mut third = config.take_sink_for_peer(3).unwrap();
        third.try_deliver(Message::single("third")).unwrap();
        let mut newest = config.try_take_pending_consumer().unwrap();
        assert_eq!(newest.prefetch_and_pop(), Some(Message::single("third")));
    }

    #[test]
    fn yring_sink_deferred_send_signals_once_per_flush() {
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
        // Wake hints stay conservative until the consumer acknowledges their
        // activation with an empty check. Prime that registration first.
        sink.try_send_deferred(Message::single("warmup"), &mut pending)
            .unwrap();
        sink.flush_pending(&mut pending);
        assert_eq!(consumer.prefetch_and_pop(), Some(Message::single("warmup")));
        assert_eq!(consumer.prefetch_and_pop(), None);
        signals.store(0, Ordering::Relaxed);

        sink.try_send_deferred(Message::single("a"), &mut pending)
            .unwrap();
        sink.try_send_deferred(Message::single("b"), &mut pending)
            .unwrap();
        assert_eq!(signals.load(Ordering::Relaxed), 0);
        assert_eq!(consumer.prefetch(), 0);

        sink.flush_pending(&mut pending);
        assert_eq!(signals.load(Ordering::Relaxed), 1);
        assert_eq!(consumer.prefetch(), 2);
        sink.try_send_deferred(Message::single("c"), &mut pending)
            .unwrap();
        sink.flush_pending(&mut pending);
        assert_eq!(
            signals.load(Ordering::Relaxed),
            1,
            "nonempty queue needs no extra wake"
        );
        for expected in ["a", "b", "c"] {
            assert_eq!(consumer.prefetch_and_pop(), Some(Message::single(expected)));
        }
        assert_eq!(consumer.prefetch_and_pop(), None);
        sink.try_send_deferred(Message::single("d"), &mut pending)
            .unwrap();
        sink.flush_pending(&mut pending);
        assert_eq!(
            signals.load(Ordering::Relaxed),
            2,
            "registered empty consumer must wake"
        );
        sink.flush_and_signal();
        assert_eq!(
            signals.load(Ordering::Relaxed),
            2,
            "nothing flushed needs no wake"
        );
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
        let (s_data_tx, s_data_rx) = mpsc::channel(16);
        let (c_data_tx, c_data_rx) = mpsc::channel(16);
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
        )
        .with_data_inbox(s_data_rx);
        let c_driver = ConnectionDriver::new(
            client_stream,
            client_connection,
            c_inbox_rx,
            c_evt_tx,
            0,
            c_cancel.clone(),
        )
        .with_data_inbox(c_data_rx);

        tokio::spawn(Box::pin(s_driver.run()));
        tokio::spawn(Box::pin(c_driver.run()));

        (
            PeerDriverHandle {
                inbox: c_inbox_tx,
                data_inbox: c_data_tx,
                cancel: c_cancel,
                transmit_slot: None,
                direct_tcp_writer: None,
                send_pipe: None,
                inproc: None,
            },
            EventAdapter { rx: c_evt_rx },
            PeerDriverHandle {
                inbox: s_inbox_tx,
                data_inbox: s_data_tx,
                cancel: s_cancel,
                transmit_slot: None,
                direct_tcp_writer: None,
                send_pipe: None,
                inproc: None,
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
            .data_inbox
            .send(PeerDriverData::SendMessage(Message::single("hello")))
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
    async fn reserved_completion_keeps_driver_join_independent_of_full_mailbox() {
        for cancel in [false, true] {
            let (stream, mut remote) = tokio::io::duplex(1024);
            let (_, connection) = ready_push_pull_connections();
            let (commands, inbox) = mpsc::channel(1);
            let (events, mut event_inbox) = mpsc::channel(1);
            events
                .try_send((
                    99,
                    PeerEvent::Event(Event::Message(Message::single("older"))),
                ))
                .unwrap();
            let cancellation = CancellationToken::new();
            if cancel {
                cancellation.cancel();
            } else {
                commands.try_send(PeerDriverCommand::Close).unwrap();
            }
            let driver = ConnectionDriver::new(stream, connection, inbox, events, 1, cancellation);
            let (completion, finished) = CompletionProgress::reserve(1);
            let driver = driver.with_completion(completion);
            let mut task = tokio::spawn(driver.run());
            tokio::time::timeout(Duration::from_millis(500), commands.closed())
                .await
                .expect("driver did not tear down its command inbox");
            assert_eq!(remote.read(&mut [0; 1]).await.unwrap(), 0);
            let joined = tokio::time::timeout(Duration::from_millis(100), &mut task).await;
            if joined.is_err() {
                task.abort();
            }
            joined
                .expect("final closure publication trapped the driver task")
                .unwrap()
                .unwrap();
            let finished = finished.await.unwrap();
            assert_eq!(finished.peer_id, 1);
            assert_eq!(finished.admitted_events, 0);
            assert_eq!(finished.reason, omq_proto::DisconnectReason::PeerClosed);
            assert_eq!(event_inbox.len(), 1);
            assert_eq!(event_inbox.recv().await.unwrap().0, 99);
        }
    }

    #[tokio::test]
    async fn aborted_driver_publishes_reserved_completion_after_transport_drop() {
        let (stream, mut remote) = tokio::io::duplex(1024);
        let (_, connection) = ready_push_pull_connections();
        let (_commands, inbox) = mpsc::channel(1);
        let (events, mut event_inbox) = mpsc::channel(1);
        let (completion, finished) = CompletionProgress::reserve(7);
        let driver = ConnectionDriver::new(
            stream,
            connection,
            inbox,
            events,
            7,
            CancellationToken::new(),
        )
        .with_completion(completion);
        let task = tokio::spawn(driver.run());
        assert!(matches!(
            event_inbox.recv().await.unwrap().1,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
        task.abort();
        let finished = tokio::time::timeout(Duration::from_millis(500), finished)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(finished.peer_id, 7);
        assert_eq!(finished.admitted_events, 1);
        assert_eq!(
            finished.reason,
            DisconnectReason::Error("connection driver stopped before completion".into())
        );
        assert_eq!(remote.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(event_inbox.recv().await.is_none());
    }

    #[tokio::test]
    async fn full_peer_event_mailbox_keeps_reverse_control_and_close_reachable() {
        for mode in ["drain", "close", "cancel"] {
            for reserved in [false, true] {
                full_peer_event_mailbox_controls(mode, reserved).await;
            }
        }
    }

    #[expect(clippy::too_many_lines)]
    async fn full_peer_event_mailbox_controls(mode: &str, reserved: bool) {
        let (server_stream, client_stream) = tokio::io::duplex(4096);
        let (server_control, server_inbox) = mpsc::channel(8);
        let (client_control, client_inbox) = mpsc::channel(8);
        let (client_data, client_data_inbox) = mpsc::channel(1);
        let (server_events, mut server_event_inbox) = mpsc::channel(1);
        let (client_events, mut client_event_inbox) = mpsc::channel(8);
        let server_cancel = CancellationToken::new();
        let client_cancel = CancellationToken::new();
        let server = ConnectionDriver::new(
            server_stream,
            Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pair)),
            server_inbox,
            server_events,
            1,
            server_cancel.clone(),
        );
        let (completion, finished) = CompletionProgress::reserve(1);
        let mut finished = reserved.then_some(finished);
        let server = if reserved {
            server.with_completion(completion)
        } else {
            drop(completion);
            server
        };
        let client = ConnectionDriver::new(
            client_stream,
            Connection::new(ConnectionConfig::new(Role::Client, SocketType::Pair)),
            client_inbox,
            client_events,
            2,
            client_cancel.clone(),
        )
        .with_data_inbox(client_data_inbox);
        let mut server_task = tokio::spawn(server.run());
        let client_task = tokio::spawn(client.run());
        assert!(matches!(
            server_event_inbox.recv().await.unwrap().1,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
        assert!(matches!(
            client_event_inbox.recv().await.unwrap().1,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
        server_control
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        client_control
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        for sequence in 0..3 {
            client_control
                .send(PeerDriverCommand::SendCommand(Command::Unknown {
                    name: format!("IN{}", char::from(b'A' + sequence)).into(),
                    body: Bytes::from_static(b"blocked"),
                }))
                .await
                .unwrap();
        }
        client_data
            .send(PeerDriverData::SendMessage(Message::single("after events")))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(server_event_inbox.len(), 1);
        server_control
            .send(PeerDriverCommand::SendCommand(Command::Unknown {
                name: "CONTROL".into(),
                body: Bytes::from_static(b"reverse"),
            }))
            .await
            .unwrap();
        let (_, event) =
            tokio::time::timeout(Duration::from_millis(500), client_event_inbox.recv())
                .await
                .expect("event admission trapped reverse control")
                .unwrap();
        assert!(matches!(event,
            PeerEvent::Event(Event::Command(Command::Unknown { name, body }))
                if name == "CONTROL" && body == b"reverse"[..]));
        if mode == "drain" {
            for sequence in 0..3 {
                let (_, event) =
                    tokio::time::timeout(Duration::from_millis(500), server_event_inbox.recv())
                        .await
                        .unwrap()
                        .unwrap();
                let expected = format!("IN{}", char::from(b'A' + sequence));
                assert!(matches!(event,
                    PeerEvent::Event(Event::Command(Command::Unknown { name, body }))
                        if name == expected.as_bytes() && body == b"blocked"[..]));
            }
            let (_, event) =
                tokio::time::timeout(Duration::from_millis(500), server_event_inbox.recv())
                    .await
                    .unwrap()
                    .unwrap();
            assert!(matches!(event, PeerEvent::Event(Event::Message(message))
                if message.part_slice(0) == Some(b"after events".as_slice())));
        }
        if mode == "cancel" {
            server_cancel.cancel();
        } else {
            server_control.send(PeerDriverCommand::Close).await.unwrap();
        }
        tokio::time::timeout(Duration::from_millis(500), server_control.closed())
            .await
            .expect("event admission trapped close/cancellation");
        let mut admitted = if mode == "drain" { 5 } else { 1 };
        if reserved {
            tokio::time::timeout(Duration::from_millis(500), &mut server_task)
                .await
                .expect("reserved completion waited for mailbox credit")
                .unwrap()
                .unwrap();
        }
        // The standalone public API keeps Closed on its supplied mailbox.
        // Socket-owned drivers join before that older admitted prefix drains.
        tokio::time::timeout(Duration::from_millis(500), async {
            while let Some((_, event)) = server_event_inbox.recv().await {
                if matches!(event, PeerEvent::Closed { .. }) {
                    assert!(!reserved);
                    break;
                }
                admitted += 1;
            }
        })
        .await
        .unwrap();
        if let Some(finished) = finished.take() {
            let result = finished.await.unwrap();
            assert_eq!(result.admitted_events, admitted);
            assert_eq!(result.reason, omq_proto::DisconnectReason::PeerClosed);
        } else {
            tokio::time::timeout(Duration::from_millis(500), server_task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        client_cancel.cancel();
        tokio::time::timeout(Duration::from_millis(500), client_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn protocol_error_event_cleanup_keeps_close_and_cancel_reachable() {
        for mode in ["close", "cancel", "linger", "drain"] {
            full_error_event_mailbox_controls(mode).await;
        }
    }

    #[expect(clippy::too_many_lines)]
    async fn full_error_event_mailbox_controls(mode: &str) {
        let (server_stream, client_stream) = tokio::io::duplex(4096);
        let (server_control, server_inbox) = mpsc::channel(8);
        let (client_control, client_inbox) = mpsc::channel(8);
        let (_client_data, client_data_inbox) = mpsc::channel(1);
        let (server_events, mut server_event_inbox) = mpsc::channel(1);
        let (client_events, mut client_event_inbox) = mpsc::channel(8);
        let server_cancel = CancellationToken::new();
        let client_cancel = CancellationToken::new();
        let server = ConnectionDriver::new(
            server_stream,
            Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pair)),
            server_inbox,
            server_events,
            1,
            server_cancel.clone(),
        );
        let client = ConnectionDriver::new(
            client_stream,
            Connection::new(ConnectionConfig::new(Role::Client, SocketType::Pair)),
            client_inbox,
            client_events,
            2,
            client_cancel.clone(),
        )
        .with_data_inbox(client_data_inbox);
        let server_task = tokio::spawn(server.run());
        let client_task = tokio::spawn(client.run());
        assert!(matches!(
            server_event_inbox.recv().await.unwrap().1,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
        assert!(matches!(
            client_event_inbox.recv().await.unwrap().1,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
        server_control
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        client_control
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        for sequence in 0..3 {
            client_control
                .send(PeerDriverCommand::SendCommand(Command::Unknown {
                    name: format!("IN{}", char::from(b'A' + sequence)).into(),
                    body: Bytes::from_static(b"blocked"),
                }))
                .await
                .unwrap();
        }
        client_control
            .send(PeerDriverCommand::SendCommand(Command::Error {
                reason: "post-handshake failure".into(),
            }))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(server_event_inbox.len(), 1);
        if mode == "drain" {
            for sequence in 0..3 {
                let (_, event) =
                    tokio::time::timeout(Duration::from_millis(500), server_event_inbox.recv())
                        .await
                        .unwrap()
                        .unwrap();
                let expected = format!("IN{}", char::from(b'A' + sequence));
                assert!(matches!(event,
                    PeerEvent::Event(Event::Command(Command::Unknown { name, body }))
                        if name == expected.as_bytes() && body == b"blocked"[..]));
            }
        }
        if mode == "cancel" {
            server_cancel.cancel();
        } else if mode == "linger" {
            server_control
                .send(PeerDriverCommand::DrainAndClose {
                    deadline: Instant::now().checked_add(Duration::from_millis(50)),
                })
                .await
                .unwrap();
        } else if mode != "drain" {
            server_control.send(PeerDriverCommand::Close).await.unwrap();
        }
        tokio::time::timeout(Duration::from_millis(500), server_control.closed())
            .await
            .expect("event admission trapped close/cancellation");
        // Final Closed publication deliberately waits for mailbox space.
        // Retire the older admitted prefix before expecting the run to join.
        tokio::time::timeout(Duration::from_millis(500), async {
            while let Some((_, event)) = server_event_inbox.recv().await {
                if let PeerEvent::Closed { error } = event {
                    assert_eq!(error.is_some(), mode == "drain");
                    break;
                }
            }
        })
        .await
        .unwrap();
        let result = tokio::time::timeout(Duration::from_millis(500), server_task)
            .await
            .unwrap()
            .unwrap();
        if mode == "drain" {
            assert!(matches!(result, Err(Error::Protocol(_))));
        } else {
            result.unwrap();
        }
        client_cancel.cancel();
        tokio::time::timeout(Duration::from_millis(500), client_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn full_handshake_event_mailbox_preserves_writes_and_original_deadline() {
        let (server_stream, client_stream) = tokio::io::duplex(4096);
        let (server_control, server_inbox) = mpsc::channel(8);
        let (_client_control, client_inbox) = mpsc::channel(8);
        let (server_events, mut server_event_inbox) = mpsc::channel(1);
        let (client_events, mut client_event_inbox) = mpsc::channel(8);
        server_events
            .send((
                9,
                PeerEvent::Event(Event::Command(Command::Unknown {
                    name: "occupied".into(),
                    body: Bytes::new(),
                })),
            ))
            .await
            .unwrap();
        let client_cancel = CancellationToken::new();
        let server = ConnectionDriver::new(
            server_stream,
            Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pair)),
            server_inbox,
            server_events,
            1,
            CancellationToken::new(),
        )
        .with_setup_deadline(Some(Instant::now() + Duration::from_millis(150)), None);
        let client = ConnectionDriver::new(
            client_stream,
            Connection::new(ConnectionConfig::new(Role::Client, SocketType::Pair)),
            client_inbox,
            client_events,
            2,
            client_cancel.clone(),
        );
        let server_task = tokio::spawn(server.run());
        let client_task = tokio::spawn(client.run());
        let (_, event) =
            tokio::time::timeout(Duration::from_millis(500), client_event_inbox.recv())
                .await
                .expect("full handshake mailbox trapped READY write")
                .unwrap();
        assert!(matches!(
            event,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
        tokio::time::timeout(Duration::from_millis(500), server_control.closed())
            .await
            .expect("handshake event admission lost the setup deadline");
        let (_, occupied) = server_event_inbox.recv().await.unwrap();
        assert!(matches!(occupied,
            PeerEvent::Event(Event::Command(Command::Unknown { name, .. }))
                if name == "occupied"));
        let (_, closed) =
            tokio::time::timeout(Duration::from_millis(500), server_event_inbox.recv())
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(closed, PeerEvent::Closed { error: Some(error) }
            if error.contains("handshake timeout")));
        let result = tokio::time::timeout(Duration::from_millis(500), server_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(Error::HandshakeFailed(_))));
        client_cancel.cancel();
        tokio::time::timeout(Duration::from_millis(500), client_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn full_receive_sinks_keep_control_and_reverse_writes_reachable() {
        for kind in [
            "channel",
            "yring",
            "authenticated",
            "server",
            "rep",
            "actor",
        ] {
            full_receive_sink_controls(kind).await;
        }
    }

    #[expect(clippy::too_many_lines)]
    async fn full_receive_sink_controls(kind: &str) {
        let (server_stream, client_stream) = tokio::io::duplex(4096);
        let (pipe, mut pipe_receiver, _, _) =
            crate::socket::recv::recv_pipe(1, crate::socket::recv::BlockingRecvWaker::new());
        let (producer, _yring_receiver) = yring::spsc(1);
        let (authenticated, _authenticated_receiver) = RecvSink::authenticated(1, Arc::new(|| {}));
        let sink = match kind {
            "channel" => Some(RecvSink::Channel(pipe.clone())),
            "yring" => Some(RecvSink::Yring(YringSink {
                producer,
                signal: Box::new(|| {}),
                space: Arc::new(StateSignal::new()),
            })),
            "authenticated" => Some(authenticated),
            "server" => Some(RecvSink::server(RecvSink::Channel(pipe.clone()), 7)),
            "rep" => Some(RecvSink::rep(RecvSink::Channel(pipe.clone()), 1)),
            "actor" => None,
            _ => unreachable!(),
        };
        let (server_control, server_inbox) = mpsc::channel(8);
        let (client_control, client_inbox) = mpsc::channel(8);
        let (client_data, client_data_inbox) = mpsc::channel(64);
        let (server_events, mut server_event_inbox) = mpsc::channel(1);
        let (client_events, mut client_event_inbox) = mpsc::channel(8);
        let server_cancel = CancellationToken::new();
        let client_cancel = CancellationToken::new();
        let mut server = ConnectionDriver::new(
            server_stream,
            Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pair)),
            server_inbox,
            server_events,
            1,
            server_cancel.clone(),
        );
        if let Some(sink) = sink {
            server = server.with_recv_sink(sink);
        }
        let client = ConnectionDriver::new(
            client_stream,
            Connection::new(ConnectionConfig::new(Role::Client, SocketType::Pair)),
            client_inbox,
            client_events,
            2,
            client_cancel.clone(),
        )
        .with_data_inbox(client_data_inbox);
        let server_task = tokio::spawn(server.run());
        let client_task = tokio::spawn(client.run());
        assert!(matches!(
            server_event_inbox.recv().await.unwrap().1,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
        assert!(matches!(
            client_event_inbox.recv().await.unwrap().1,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
        server_control
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        client_control
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        for sequence in 0u8..32 {
            let body = Bytes::from(vec![sequence; 128]);
            let message = if kind == "rep" {
                Message::multipart([Bytes::new(), body])
            } else {
                Message::single(body)
            };
            client_data
                .send(PeerDriverData::SendMessage(message))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        server_control
            .send(PeerDriverCommand::SendCommand(Command::Unknown {
                name: "CONTROL".into(),
                body: Bytes::from_static(b"reverse"),
            }))
            .await
            .unwrap();
        let (_, event) =
            tokio::time::timeout(Duration::from_millis(500), client_event_inbox.recv())
                .await
                .unwrap_or_else(|_| panic!("{kind}: receive backpressure trapped reverse control"))
                .unwrap();
        assert!(
            matches!(event, PeerEvent::Event(Event::Command(Command::Unknown { name, body }))
            if name == "CONTROL" && body == b"reverse"[..])
        );
        if kind == "rep" {
            let request = pipe_receiver
                .prefetch_and_pop()
                .expect("one admitted request");
            assert_eq!(request.routing_id(), Some(2));
            assert_eq!(request.get(0), Some(b"".as_slice()));
            assert_eq!(request.get(1), Some([0_u8; 128].as_slice()));
            assert!(pipe_receiver.prefetch_and_pop().is_none());
        }
        server_control.send(PeerDriverCommand::Close).await.unwrap();
        tokio::time::timeout(Duration::from_millis(500), server_control.closed())
            .await
            .unwrap_or_else(|_| panic!("{kind}: receive backpressure trapped Close"));
        // Final Closed publication still needs a free event slot for the
        // legacy combined mailbox. Test command service separately from that.
        let _ = server_event_inbox.try_recv();
        tokio::time::timeout(Duration::from_millis(500), server_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        client_cancel.cancel();
        tokio::time::timeout(Duration::from_millis(500), client_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn partial_large_read_keeps_reverse_control_and_close_reachable() {
        partial_large_read_controls(false).await;
    }

    #[tokio::test]
    async fn partial_large_read_keeps_cancellation_reachable() {
        partial_large_read_controls(true).await;
    }

    async fn partial_large_read_controls(cancel_only: bool) {
        let (server_stream, client_stream) = tokio::io::duplex(4096);
        let (server_control, server_inbox) = mpsc::channel(8);
        let (client_control, client_inbox) = mpsc::channel(8);
        let (client_data, client_data_inbox) = mpsc::channel(8);
        let (server_events, mut server_event_inbox) = mpsc::channel(8);
        let (client_events, mut client_event_inbox) = mpsc::channel(8);
        let server_cancel = CancellationToken::new();
        let client_cancel = CancellationToken::new();
        let server = ConnectionDriver::with_config(
            server_stream,
            Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pair)),
            server_inbox,
            server_events,
            1,
            server_cancel.clone(),
            PeerDriverConfig {
                large_message_threshold: 128 * 1024,
                ..PeerDriverConfig::default()
            },
        );
        let client = ConnectionDriver::new(
            client_stream,
            Connection::new(ConnectionConfig::new(Role::Client, SocketType::Pair)),
            client_inbox,
            client_events,
            2,
            client_cancel.clone(),
        )
        .with_data_inbox(client_data_inbox);
        let server_task = tokio::spawn(server.run());
        let client_task = tokio::spawn(client.run());
        assert!(matches!(
            server_event_inbox.recv().await.unwrap().1,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
        assert!(matches!(
            client_event_inbox.recv().await.unwrap().1,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
        server_control
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        client_control
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        let mut prefix = vec![0x02];
        prefix.extend_from_slice(&(1024_u64 * 1024).to_be_bytes());
        prefix.extend_from_slice(&[7; 128]);
        client_data
            .send(PeerDriverData::SendEncoded(Arc::new(smallvec::smallvec![
                Bytes::from(prefix)
            ])))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        if cancel_only {
            server_cancel.cancel();
        } else {
            server_control
                .send(PeerDriverCommand::SendCommand(Command::Unknown {
                    name: "CONTROL".into(),
                    body: Bytes::from_static(b"partial"),
                }))
                .await
                .unwrap();
            let (_, event) =
                tokio::time::timeout(Duration::from_millis(500), client_event_inbox.recv())
                    .await
                    .expect("partial large read trapped reverse control")
                    .unwrap();
            assert!(matches!(event,
                PeerEvent::Event(Event::Command(Command::Unknown { name, body }))
                if name == "CONTROL" && body == b"partial"[..]
            ));
            server_control
                .send(PeerDriverCommand::DrainAndClose {
                    deadline: Instant::now().checked_add(Duration::from_millis(50)),
                })
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_millis(500), server_task)
            .await
            .expect("partial large read trapped cancellation/linger")
            .unwrap()
            .unwrap();
        client_cancel.cancel();
        tokio::time::timeout(Duration::from_millis(500), client_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
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

        let server_task = tokio::spawn(Box::pin(server.run()));
        let client_task = tokio::spawn(Box::pin(client.run()));

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
                    #[cfg(feature = "dart")]
                    Err(crate::engine::SendPipeError::Invalid(_)) => {
                        unreachable!("receive output has no transport send validator")
                    }
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
    async fn graceful_close_deadline_interrupts_stalled_transport_shutdown() {
        let (push, _pull) = ready_push_pull_connections();
        let (stream, _remote) = tokio::io::duplex(64);
        let shutdown_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (commands, inbox) = mpsc::channel(4);
        let (events, _event_inbox) = mpsc::channel(4);
        let driver = ConnectionDriver::new(
            StalledShutdownStream {
                stream,
                shutdown_seen: shutdown_seen.clone(),
            },
            push,
            inbox,
            events,
            1,
            CancellationToken::new(),
        );
        commands
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        let deadline = Instant::now() + Duration::from_millis(100);
        commands
            .send(PeerDriverCommand::DrainAndClose {
                deadline: Some(deadline),
            })
            .await
            .unwrap();
        let task = tokio::spawn(driver.run());
        tokio::time::timeout_at((deadline + Duration::from_millis(75)).into(), task)
            .await
            .expect("transport shutdown hid the linger deadline")
            .unwrap()
            .unwrap();
        assert!(shutdown_seen.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn producer_eof_preserves_the_final_send_pipe_batch() {
        let (push, mut pull) = ready_push_pull_connections();
        let (stream, mut remote) = tokio::io::duplex(64);
        let (commands, inbox) = mpsc::channel(4);
        let (events, _event_inbox) = mpsc::channel(4);
        let (mut producer, consumer) = crate::engine::send_pipe(4);
        producer.try_send(Message::single(vec![5; 4096])).unwrap();
        drop(producer);
        let driver =
            ConnectionDriver::new(stream, push, inbox, events, 1, CancellationToken::new())
                .with_send_pipe(consumer);
        commands
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        let task = tokio::spawn(driver.run());
        let mut buffer = [0; 1024];
        loop {
            let count = tokio::time::timeout(Duration::from_secs(1), remote.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            if count == 0 {
                break;
            }
            pull.handle_input(Bytes::copy_from_slice(&buffer[..count]))
                .unwrap();
        }
        assert_eq!(
            pull.poll_message().unwrap().part_slice(0).unwrap(),
            &[5; 4096]
        );
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn close_interrupts_stalled_outbound_write() {
        let (server_stream, client_stream) = tokio::io::duplex(64);
        let server_connection =
            Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let client_connection = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push)
                .identity(Bytes::from_static(b"c")),
        );

        let (server_inbox_tx, server_inbox_rx) = mpsc::channel(16);
        let (client_inbox_tx, client_inbox_rx) = mpsc::channel(16);
        let (_server_data_tx, server_data_rx) = mpsc::channel(16);
        let (client_data_tx, client_data_rx) = mpsc::channel(16);
        let (server_events_tx, mut server_events_rx) = mpsc::channel(1);
        let (client_events_tx, mut client_events_rx) = mpsc::channel(1);
        let server_cancel = CancellationToken::new();
        let client_cancel = CancellationToken::new();

        let server = ConnectionDriver::new(
            server_stream,
            server_connection,
            server_inbox_rx,
            server_events_tx,
            0,
            server_cancel.clone(),
        )
        .with_data_inbox(server_data_rx);
        let client = ConnectionDriver::new(
            client_stream,
            client_connection,
            client_inbox_rx,
            client_events_tx,
            1,
            client_cancel.clone(),
        )
        .with_data_inbox(client_data_rx);
        let server_task = tokio::spawn(Box::pin(server.run()));
        let mut client_task = tokio::spawn(Box::pin(client.run()));

        assert!(matches!(
            client_events_rx.recv().await,
            Some((_, PeerEvent::Event(Event::HandshakeSucceeded { .. })))
        ));
        assert!(matches!(
            server_events_rx.recv().await,
            Some((_, PeerEvent::Event(Event::HandshakeSucceeded { .. })))
        ));
        client_inbox_tx
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        server_inbox_tx
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();

        let payload = Bytes::from(vec![0xA5; 1024 * 1024]);
        for _ in 0..3 {
            client_data_tx
                .send(PeerDriverData::SendMessage(Message::single(
                    payload.clone(),
                )))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!client_task.is_finished(), "writer did not reach the stall");
        client_inbox_tx
            .send(PeerDriverCommand::Close)
            .await
            .unwrap();

        let stopped = tokio::time::timeout(Duration::from_millis(250), &mut client_task).await;
        if stopped.is_err() {
            client_cancel.cancel();
        }
        server_cancel.cancel();
        assert!(stopped.is_ok(), "Close was trapped behind a stalled write");

        // No actor drains server events here. Release a queued message so the
        // driver can report Closed even when that one-slot channel is full.
        drop(server_events_rx);
        let _ = tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server teardown stalled")
            .expect("server driver panicked");
    }

    #[cfg(feature = "ws")]
    #[tokio::test]
    async fn peer_ws_close_preserves_coalesced_messages_through_backpressure() {
        use omq_proto::proto::connection::WsRole;

        for direct in [false, true] {
            let mut push = Connection::new(
                ConnectionConfig::new(Role::Server, SocketType::Push).ws_role(WsRole::Server),
            );
            let mut pull = Connection::new(
                ConnectionConfig::new(Role::Client, SocketType::Pull).ws_role(WsRole::Client),
            );
            for _ in 0..10 {
                let push_out = drain_transmit(&mut push);
                let pull_out = drain_transmit(&mut pull);
                if !push_out.is_empty() {
                    pull.handle_input(Bytes::from(push_out)).unwrap();
                }
                if !pull_out.is_empty() {
                    push.handle_input(Bytes::from(pull_out)).unwrap();
                }
            }
            assert!(push.is_ready() && pull.is_ready());
            while pull.poll_event().is_some() {}
            let messages = [
                Message::single("before close"),
                Message::single("last message"),
            ];
            for message in &messages {
                push.send_message(message).unwrap();
            }
            push.send_ws_close(1000);
            let wire = drain_transmit(&mut push);
            let (stream, mut remote) = tokio::io::duplex(4096);
            // Supply both messages and CLOSE in one read, independent of TCP timing.
            remote.write_all(&wire).await.unwrap();
            let (commands, inbox) = mpsc::channel(4);
            let (events, mut event_inbox) = mpsc::channel(1);
            let (producer, mut consumer) = yring::spsc(1);
            let space = Arc::new(StateSignal::new());
            let mut driver =
                ConnectionDriver::new(stream, pull, inbox, events, 1, CancellationToken::new());
            if direct {
                driver = driver.with_recv_sink(RecvSink::Yring(YringSink {
                    producer,
                    signal: Box::new(|| {}),
                    space: space.clone(),
                }));
            }
            commands
                .send(PeerDriverCommand::ActivateDataPlane)
                .await
                .unwrap();
            let task = tokio::spawn(driver.run());
            tokio::task::yield_now().await;
            for expected in messages {
                let received = tokio::time::timeout(Duration::from_secs(1), async {
                    if direct {
                        loop {
                            if let Some(message) = consumer.prefetch_and_pop() {
                                consumer.release();
                                space.notify_changed();
                                break message;
                            }
                            tokio::task::yield_now().await;
                        }
                    } else {
                        let (_, event) = event_inbox.recv().await.unwrap();
                        let PeerEvent::Event(Event::Message(message)) = event else {
                            panic!("peer closed before delivering {expected:?}: {event:?}");
                        };
                        message
                    }
                })
                .await
                .expect("peer CLOSE discarded a preceding message");
                assert_eq!(received, expected);
            }
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .expect("peer CLOSE did not finish after receive admission")
                .unwrap()
                .unwrap();
        }
    }

    #[cfg(feature = "ws")]
    #[tokio::test]
    async fn graceful_ws_close_drains_data_waits_for_reply_and_keeps_original_deadline() {
        use omq_proto::proto::connection::WsRole;

        for reply in [false, true] {
            let mut push = Connection::new(
                ConnectionConfig::new(Role::Server, SocketType::Push).ws_role(WsRole::Server),
            );
            let mut pull = Connection::new(
                ConnectionConfig::new(Role::Client, SocketType::Pull).ws_role(WsRole::Client),
            );
            for _ in 0..10 {
                let push_out = drain_transmit(&mut push);
                let pull_out = drain_transmit(&mut pull);
                if !push_out.is_empty() {
                    pull.handle_input(Bytes::from(push_out)).unwrap();
                }
                if !pull_out.is_empty() {
                    push.handle_input(Bytes::from(pull_out)).unwrap();
                }
            }
            assert!(push.is_ready() && pull.is_ready());
            while push.poll_event().is_some() {}
            let (stream, mut remote) = tokio::io::duplex(64);
            let (commands, inbox) = mpsc::channel(4);
            let (data, data_inbox) = mpsc::channel(4);
            let (events, _event_inbox) = mpsc::channel(4);
            let driver =
                ConnectionDriver::new(stream, push, inbox, events, 1, CancellationToken::new())
                    .with_data_inbox(data_inbox);
            data.send(PeerDriverData::SendMessage(Message::single(vec![9; 4096])))
                .await
                .unwrap();
            commands
                .send(PeerDriverCommand::ActivateDataPlane)
                .await
                .unwrap();
            let deadline = Instant::now() + Duration::from_millis(250);
            commands
                .send(PeerDriverCommand::DrainAndClose {
                    deadline: Some(deadline),
                })
                .await
                .unwrap();
            let task = tokio::spawn(driver.run());
            tokio::time::sleep(Duration::from_millis(75)).await;
            assert!(!task.is_finished(), "staged data was dropped during drain");
            let mut buffer = [0; 1024];
            while !pull.is_closed() {
                let count = tokio::time::timeout_at(deadline.into(), remote.read(&mut buffer))
                    .await
                    .unwrap()
                    .unwrap();
                assert_ne!(count, 0, "driver closed without the WS CLOSE frame");
                pull.handle_input(Bytes::copy_from_slice(&buffer[..count]))
                    .unwrap();
            }
            assert_eq!(
                pull.poll_message().unwrap().part_slice(0).unwrap(),
                &[9; 4096]
            );
            assert!(
                !task.is_finished(),
                "driver did not wait for the peer CLOSE"
            );
            if reply {
                remote.write_all(&drain_transmit(&mut pull)).await.unwrap();
            }
            tokio::time::timeout_at((deadline + Duration::from_millis(75)).into(), task)
                .await
                .expect("close restarted its deadline after draining data")
                .unwrap()
                .unwrap();
        }
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
        tokio::spawn(Box::pin(s.run()));
        tokio::spawn(Box::pin(c.run()));

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

    #[cfg(feature = "plain")]
    #[tokio::test]
    async fn stalled_authentication_error_keeps_cancel_and_original_deadline_reachable() {
        for cancel in [true, false] {
            let (server_stream, client_stream) = tokio::io::duplex(4096);
            let stalled = Arc::new(StateSignal::new());
            let server_connection = Connection::new(
                ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(
                    omq_proto::MechanismSetup::PlainServer {
                        authenticator: omq_proto::Authenticator::new(|_| false),
                    },
                ),
            );
            let client_connection = Connection::new(
                ConnectionConfig::new(Role::Client, SocketType::Push).mechanism(
                    omq_proto::MechanismSetup::PlainClient {
                        username: "alice".into(),
                        password: "wrong".into(),
                    },
                ),
            );
            let (server_inbox, server_commands) = mpsc::channel(16);
            let (_client_inbox, client_commands) = mpsc::channel(16);
            let (server_events, mut server_events_rx) = mpsc::channel(16);
            let (client_events, _client_events_rx) = mpsc::channel(16);
            let server_cancel = CancellationToken::new();
            let client_cancel = CancellationToken::new();
            let mut server = ConnectionDriver::new(
                GreetingOnlyStream {
                    stream: server_stream,
                    stalled: Arc::clone(&stalled),
                },
                server_connection,
                server_commands,
                server_events,
                0,
                server_cancel.clone(),
            );
            server.config.handshake_timeout = Some(if cancel {
                Duration::from_secs(5)
            } else {
                Duration::from_millis(300)
            });
            let client = ConnectionDriver::new(
                client_stream,
                client_connection,
                client_commands,
                client_events,
                1,
                client_cancel.clone(),
            );
            let server_task = tokio::spawn(server.run());
            let client_task = tokio::spawn(client.run());
            tokio::time::timeout(Duration::from_secs(1), stalled.changed_after(0))
                .await
                .expect("authentication ERROR write never stalled");
            if cancel {
                server_cancel.cancel();
            }
            tokio::time::timeout(Duration::from_secs(1), server_inbox.closed())
                .await
                .expect("authentication ERROR write trapped teardown");
            let result = server_task.await.unwrap();
            if cancel {
                assert!(result.is_ok());
            } else {
                assert!(matches!(result, Err(Error::HandshakeFailed(reason))
                    if reason == "PLAIN authentication failed with status 400"));
            }
            let (_, event) = server_events_rx.recv().await.unwrap();
            assert!(matches!(event, PeerEvent::Closed { .. }));
            client_cancel.cancel();
            let _ = client_task.await.unwrap();
        }
    }

    #[cfg(feature = "plain")]
    #[tokio::test]
    async fn authentication_failure_flushes_mechanism_error_before_close() {
        let (server_stream, client_stream) = tokio::io::duplex(64 * 1024);
        let server_connection = Connection::new(
            ConnectionConfig::new(Role::Server, SocketType::Pull).mechanism(
                omq_proto::MechanismSetup::PlainServer {
                    authenticator: omq_proto::Authenticator::new(|_| false),
                },
            ),
        );
        let client_connection = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push).mechanism(
                omq_proto::MechanismSetup::PlainClient {
                    username: "alice".into(),
                    password: "wrong".into(),
                },
            ),
        );
        let (server_inbox_tx, server_inbox_rx) = mpsc::channel(16);
        let (client_inbox_tx, client_inbox_rx) = mpsc::channel(16);
        let (server_events_tx, _server_events_rx) = mpsc::channel(16);
        let (client_events_tx, mut client_events_rx) = mpsc::channel(16);

        let server = ConnectionDriver::new(
            server_stream,
            server_connection,
            server_inbox_rx,
            server_events_tx,
            0,
            CancellationToken::new(),
        );
        let client = ConnectionDriver::new(
            client_stream,
            client_connection,
            client_inbox_rx,
            client_events_tx,
            1,
            CancellationToken::new(),
        );
        let server_task = tokio::spawn(Box::pin(server.run()));
        let client_task = tokio::spawn(Box::pin(client.run()));
        let _inboxes = (server_inbox_tx, client_inbox_tx);

        let reason = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some((_, PeerEvent::Closed { error: Some(error) })) =
                    client_events_rx.recv().await
                {
                    break error;
                }
            }
        })
        .await
        .expect("client did not receive the authentication failure");
        assert_eq!(reason, "PLAIN peer sent ERROR: 400");

        assert!(server_task.await.unwrap().is_err());
        assert!(client_task.await.unwrap().is_err());
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
        tokio::spawn(Box::pin(s_driver.run()));

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

    #[tokio::test]
    async fn full_peer_queue_bounds_pending_delivery_and_leaves_decoder_remainder_in_place() {
        let (mut sender, mut connection) = ready_push_pull_connections();
        for _ in 0..100 {
            sender
                .send_message(&Message::single(Bytes::from(vec![0; 512])))
                .unwrap();
        }
        let wire = drain_transmit(&mut sender);
        assert!(wire.len() < READ_BUF_MAX);
        connection.handle_input(Bytes::from(wire)).unwrap();
        let handles = crate::socket::recv::SpscHandles::new(
            crate::socket::recv::BlockingRecvWaker::new(),
            false,
        );
        let (mut routes, mut receiver) =
            crate::socket::peer_recv::PeerRecvRoutes::new(16, &handles, None);
        let sink = routes
            .register(Bytes::from_static(b"peer"), CancellationToken::new())
            .unwrap();
        let mut sink = Some(RecvSink::Peer(sink));
        let (events, _rx) = mpsc::channel(1);
        let mut events = PeerOutput::from(events);
        let mut pending = None;
        for _ in 0..8 {
            assert_eq!(
                drain_decoded_messages(
                    &mut connection,
                    &mut None,
                    ReceiveProfile::Throughput,
                    MessageDelivery {
                        sink: &mut sink,
                        peer_out: &mut events,
                        peer_id: 0,
                        completion: &mut CompletionProgress::default(),
                        pending: &mut pending,
                        reserved_admission: false,
                    },
                    ReceiveRateLimiters {
                        connection: &mut None,
                        ip: None
                    },
                    None,
                )
                .unwrap(),
                DriverStep::Continue
            );
            assert!(sink.as_ref().unwrap().peer_blocked());
        }
        let mut queued = Vec::new();
        assert_eq!(receiver.try_recv_many_into(100, &mut queued).unwrap(), 16);
        // Only one additional message left the decoder for pending admission.
        // Repeated driver turns under backpressure consume no further input.
        let mut remaining = 0;
        let mut bytes = 0;
        while let Some(message) = connection.poll_message() {
            remaining += 1;
            bytes += message.max_message_size_len();
            assert!(remaining <= 100 && bytes < READ_BUF_MAX);
        }
        assert_eq!(remaining, 83);
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
        while let Some(mut large) = PendingLargeRead::begin(connection, config, &recv_pool)? {
            while !large.complete() {
                large.read(reader).await?;
                *last_input = Instant::now();
            }
            large.finish(connection, &recv_pool)?;
        }
        Ok(())
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
        consumer: &mut yring::Consumer<Message>,
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
                let data = item.part_bytes(0).unwrap();
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

    /// Owned-chunk writer with Quinn's `write_chunks` semantics: it empties
    /// accepted chunks and trims a partly accepted one. `caps` scripts each
    /// call: `Some(n)` accepts up to `n` bytes, `None` stays pending without
    /// a wake until the test removes it, like a flow-control stall.
    #[derive(Debug, Default)]
    struct OwnedChunkWriter {
        caps: VecDeque<Option<usize>>,
        default_cap: usize,
        out: Vec<u8>,
        chunk_starts: Vec<usize>,
    }

    impl OwnedChunkWriter {
        fn new(caps: impl IntoIterator<Item = Option<usize>>, default_cap: usize) -> Self {
            Self {
                caps: caps.into_iter().collect(),
                default_cap,
                ..Self::default()
            }
        }
    }

    impl ChunkWrite for OwnedChunkWriter {
        fn poll_write_chunks(
            &mut self,
            _cx: &mut Context<'_>,
            bufs: &mut [Bytes],
        ) -> Poll<io::Result<usize>> {
            if self.caps.front() == Some(&None) {
                return Poll::Pending;
            }
            let mut left = self.caps.pop_front().flatten().unwrap_or(self.default_cap);
            let mut written = 0;
            for buf in bufs.iter_mut() {
                if left == 0 {
                    break;
                }
                let take = buf.len().min(left);
                let chunk = if take == buf.len() {
                    std::mem::take(buf)
                } else {
                    buf.split_to(take)
                };
                self.chunk_starts.push(chunk.as_ptr() as usize);
                self.out.extend_from_slice(&chunk);
                written += take;
                left -= take;
            }
            Poll::Ready(Ok(written))
        }
    }

    impl DriverWrite for OwnedChunkWriter {
        fn chunk_writer(&mut self) -> Option<&mut dyn ChunkWrite> {
            Some(self)
        }
    }

    impl AsyncWrite for OwnedChunkWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.out.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Small (arena) and large (external) frames in one buffer, plus the
    /// expected wire bytes.
    fn mixed_frames(eq: &mut FrameBuffer) -> (Vec<u8>, Vec<usize>) {
        let mut expected = Vec::new();
        let mut large_starts = Vec::new();
        for (seq, len) in [16, 300, 70_000, 64, 1_000, 200_000, 32]
            .into_iter()
            .enumerate()
        {
            let payload = Bytes::from(patterned_payload(len, seq as u64));
            push_expected_single_frame(&mut expected, &payload);
            if len >= omq_proto::frame_buffer::ARENA_THRESHOLD {
                large_starts.push(payload.as_ptr() as usize);
            }
            eq.frame(&Message::single(payload));
        }
        (expected, large_starts)
    }

    async fn write_owned_until_idle(
        writer: &mut OwnedChunkWriter,
        eq: &mut FrameBuffer,
        pending: &mut PendingWrite,
        connection: &mut Connection,
    ) {
        let mut heartbeat = HeartbeatProbe::default();
        while !pending.is_empty() || !eq.is_empty() {
            write_driver_progress(writer, eq, pending, connection, &mut heartbeat)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn owned_chunk_writer_receives_payloads_without_copies() {
        let mut eq = FrameBuffer::new();
        let (expected, large_starts) = mixed_frames(&mut eq);
        let mut connection = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
        let _ = drain_transmit(&mut connection);
        let mut pending = PendingWrite::default();
        let mut writer = OwnedChunkWriter::new([], usize::MAX);

        write_owned_until_idle(&mut writer, &mut eq, &mut pending, &mut connection).await;

        assert_eq!(writer.out, expected);
        for start in large_starts {
            assert!(
                writer.chunk_starts.contains(&start),
                "large payload reached the writer as a copy"
            );
        }
    }

    #[test]
    fn coalesce_small_chunks_merges_runs_and_keeps_large_chunks() {
        let large = Bytes::from(patterned_payload(OWNED_COALESCE_BELOW, 1));
        let small: Vec<Bytes> = (0..40)
            .map(|seq| Bytes::from(patterned_payload(4_096, seq)))
            .collect();
        let lone = Bytes::from_static(b"lone");
        let mut chunks = vec![Bytes::from_static(b"hdr"), large.clone(), lone.clone()];
        chunks.push(large.clone());
        chunks.extend(small.iter().cloned());
        let expected: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();

        coalesce_small_chunks(&mut chunks);

        let out: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
        assert_eq!(out, expected);
        assert_eq!(chunks[1].as_ptr(), large.as_ptr(), "large chunk was copied");
        assert_eq!(
            chunks[2].as_ptr(),
            lone.as_ptr(),
            "single small chunk was copied"
        );
        assert_eq!(chunks[3].as_ptr(), large.as_ptr(), "large chunk was copied");
        // 40 x 4 KiB merge into runs of at most 64 KiB.
        assert_eq!(chunks.len(), 4 + 3);
        assert!(chunks[4..].iter().all(|c| c.len() <= OWNED_COALESCE_TARGET));
    }

    #[tokio::test]
    async fn owned_chunk_writer_receives_few_chunks_for_mid_size_frames() {
        let mut eq = FrameBuffer::new();
        let mut expected = Vec::new();
        for seq in 0..64 {
            let payload = Bytes::from(patterned_payload(4_096, seq));
            push_expected_single_frame(&mut expected, &payload);
            eq.frame(&Message::single(payload));
        }
        let mut connection = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
        let _ = drain_transmit(&mut connection);
        let mut pending = PendingWrite::default();
        let mut writer = OwnedChunkWriter::new([], usize::MAX);

        write_owned_until_idle(&mut writer, &mut eq, &mut pending, &mut connection).await;

        assert_eq!(writer.out, expected);
        // 64 headers and 64 payloads, about 257 KiB, in runs of 64 KiB.
        assert!(
            writer.chunk_starts.len() <= 8,
            "{} chunks reached the writer",
            writer.chunk_starts.len()
        );
    }

    #[test]
    fn owned_slot_staging_merges_shared_fan_out_chunks() {
        let slot = PeerTransmitSlot::new(
            1,
            false,
            None,
            None,
            omq_proto::frame_buffer::ARENA_THRESHOLD,
            omq_proto::frame_buffer::ARENA_INITIAL_CAP,
            crate::engine::transmit_slot::TRANSMIT_SLOT_CAP_DEFAULT,
            crate::engine::transmit_slot::TRANSMIT_SLOT_MSG_CAP_DEFAULT,
            crate::engine::framing::WireFraming::Zmtp,
        );
        slot.handshake_done.store(true, Ordering::Release);
        // Shared fan-out payloads stay separate chunks in the slot.
        let payload = Bytes::from(patterned_payload(256, 7));
        let mut expected = Vec::new();
        for _ in 0..200 {
            let header = Bytes::from_static(&[0, 0x80]);
            expected.extend_from_slice(&header);
            expected.extend_from_slice(&payload);
            assert_eq!(
                slot.try_push_encoded(&[header, payload.clone()]),
                crate::engine::transmit_slot::TryFrameResult::Ok
            );
        }

        let mut pending = PendingWrite::default();
        pending.stage_slot(&slot, true);

        let staged: Vec<u8> = pending
            .chunks
            .iter()
            .flat_map(|c| c.iter().copied())
            .collect();
        assert_eq!(staged, expected);
        assert_eq!(pending.remaining, expected.len());
        assert!(
            pending.chunks.len() <= 2,
            "{} chunks staged",
            pending.chunks.len()
        );
    }

    #[tokio::test]
    async fn owned_chunk_writer_preserves_bytes_under_partial_writes() {
        for cap in [1, 7, 9, 64, 4_095, 65_537] {
            let mut eq = FrameBuffer::new();
            let (expected, _) = mixed_frames(&mut eq);
            let mut connection =
                Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
            let _ = drain_transmit(&mut connection);
            let mut pending = PendingWrite::default();
            let mut writer = OwnedChunkWriter::new([], cap);

            write_owned_until_idle(&mut writer, &mut eq, &mut pending, &mut connection).await;

            assert_eq!(writer.out, expected, "cap {cap}");
        }
    }

    #[tokio::test]
    async fn owned_chunk_write_cancellation_keeps_staged_chunks() {
        let mut eq = FrameBuffer::new();
        let (expected, _) = mixed_frames(&mut eq);
        let mut connection = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
        let _ = drain_transmit(&mut connection);
        let mut pending = PendingWrite::default();
        let mut heartbeat = HeartbeatProbe::default();
        let mut writer = OwnedChunkWriter::new([Some(1_234), None], 50_000);

        // The batch time limit may end a call before the scripted stall.
        loop {
            tokio::select! {
                biased;
                result = write_driver_progress(
                    &mut writer,
                    &mut eq,
                    &mut pending,
                    &mut connection,
                    &mut heartbeat,
                ) => result.unwrap(),
                () = tokio::time::sleep(Duration::from_millis(10)) => break,
            }
        }
        assert!(!pending.is_empty(), "cancelled write dropped staged chunks");
        assert_eq!(writer.out.len(), 1_234);

        writer.caps.pop_front();

        write_owned_until_idle(&mut writer, &mut eq, &mut pending, &mut connection).await;
        assert_eq!(writer.out, expected);
    }
}
