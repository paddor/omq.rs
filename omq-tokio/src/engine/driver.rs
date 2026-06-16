//! Per-connection driver: one tokio task per live peer connection.

use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, split};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use futures::stream::FuturesOrdered;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::transform::{MessageDecoder, MessageEncoder, TransformedOut};
use omq_proto::proto::{Command, Connection, Event};

use super::compression_pool::CompressionPool;
use super::wire_slot::PeerWireSlot;
use crate::routing::drop_queue::QueueReceiver;
use omq_proto::encoded_queue::EncodedQueue;

/// Where the driver routes decoded inbound messages.
///
/// `Channel`: the existing path via `async_channel` (pure-Rust callers).
/// `Yring`: direct push to a lock-free SPSC ring + external signal,
/// used by omq-libzmq to eliminate the recv-pump relay task.
pub enum RecvSink {
    Channel(async_channel::Sender<Message>),
    Yring(YringSink),
}

/// Yring-based recv sink. Pushes decoded messages directly into a
/// lock-free SPSC ring and signals the consumer via a callback on
/// empty-to-non-empty transitions.
pub struct YringSink {
    pub producer: yring::Producer<Message>,
    pub signal: Box<dyn Fn() + Send + Sync>,
    pub space: Arc<tokio::sync::Notify>,
}

/// Shared config for creating and recycling [`RecvSink::Yring`] instances.
/// The actor refills `slot` with a fresh yring pair on peer disconnect;
/// the external consumer picks up the new consumer from
/// `pending_consumer`.
pub struct RecvSinkConfig {
    pub slot: std::sync::Mutex<Option<RecvSink>>,
    pub pending_consumer: std::sync::Mutex<Option<yring::Consumer<Message>>>,
    signal: Arc<dyn Fn() + Send + Sync>,
    space: Arc<tokio::sync::Notify>,
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
        space: Arc<tokio::sync::Notify>,
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
    pub fn refill(&self) {
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
}

impl std::fmt::Debug for RecvSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Channel(tx) => f.debug_tuple("Channel").field(tx).finish(),
            Self::Yring(y) => f
                .debug_struct("Yring")
                .field("producer", &y.producer)
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

impl RecvSink {
    async fn send(&mut self, m: Message) -> bool {
        match self {
            Self::Channel(tx) => tx.send(m).await.is_ok(),
            Self::Yring(sink) => {
                let mut msg = m;
                loop {
                    match sink.producer.push(msg) {
                        Ok(()) => {
                            if let yring::FlushResult::Flushed {
                                was_empty: true, ..
                            } = sink.producer.flush_and_check()
                            {
                                (sink.signal)();
                            }
                            return true;
                        }
                        Err(returned) => {
                            msg = returned;
                            let notified = sink.space.notified();
                            tokio::pin!(notified);
                            notified.as_mut().enable();
                            match sink.producer.push(msg) {
                                Ok(()) => {
                                    if let yring::FlushResult::Flushed {
                                        was_empty: true, ..
                                    } = sink.producer.flush_and_check()
                                    {
                                        (sink.signal)();
                                    }
                                    return true;
                                }
                                Err(returned2) => {
                                    msg = returned2;
                                    tokio::select! {
                                        biased;
                                        () = notified => {}
                                        () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Batch-encode messages into `EncodedQueue`. Two modes:
///
/// **Direct** (no encoder or offloading disabled): encode each message
/// into `EncodedQueue` inline.
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
    codec: &mut Connection,
    eq: &mut EncodedQueue,
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
        encode_msg(first, encoder, codec, eq, passthrough)?;
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
                    encode_msg(&next, encoder, codec, eq, passthrough)?;
                }
                count += 1;
            }
            None => break,
        }
    }
    if use_pipeline {
        drain_pipeline(pipeline, pool, eq).await?;
    }
    Ok(count)
}

const READ_BUF_SIZE: usize = 128 * 1024;

use crate::routing::SHARED_MAX_BATCH_MSGS;

/// Max bytes one shared-queue batch encodes before flushing.
/// Override at runtime via `OMQ_BATCH_BYTES`.
fn max_batch_bytes() -> usize {
    use std::sync::OnceLock;
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("OMQ_BATCH_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024 * 1024)
    })
}

/// Driver-level timing configuration: handshake deadline, heartbeat
/// cadence, idle-close timeout.
#[derive(Debug, Clone, Copy, Default)]
pub struct DriverConfig {
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
    /// `read_buf` → codec copy path. `0` disables.
    pub large_message_threshold: usize,
}

/// Commands accepted by a running [`ConnectionDriver`].
#[derive(Debug)]
pub enum DriverCommand {
    /// Queue an application message for send.
    SendMessage(Message),
    /// Pre-encoded wire bytes. Pushed directly into the transmit buffer,
    /// skipping per-message encoding. Used by PUB fan-out when all peers
    /// share the same wire format (NULL mechanism, no per-peer crypto).
    SendEncoded(std::sync::Arc<smallvec::SmallVec<[bytes::Bytes; 4]>>),
    /// Queue a ZMTP command for send (SUBSCRIBE, CANCEL, JOIN, LEAVE, ...).
    SendCommand(Command),
    /// Initiate clean shutdown.
    Close,
}

/// Handle returned to callers after spawning a driver. `inbox` delivers
/// commands into the driver; `cancel` requests early teardown.
#[derive(Debug, Clone)]
pub struct DriverHandle {
    pub inbox: mpsc::Sender<DriverCommand>,
    pub cancel: CancellationToken,
    pub(crate) wire_slot: Option<Arc<PeerWireSlot>>,
}

/// What a [`ConnectionDriver`] writes to its shared peer-event
/// channel: either a parsed ZMTP `Event` or a final `Closed` signal
/// emitted just before the driver task exits. Replaces the old
/// per-connection shim task that wrapped Events into the
/// `SocketDriver`'s `InternalEvent::PeerEvent` / `PeerClosed`.
#[derive(Debug)]
pub enum PeerOut {
    Event(Event),
    Closed,
}

/// A single-connection driver: reads bytes from the stream, feeds the codec,
/// forwards events out, accepts commands in, writes codec-produced bytes out.
#[derive(Debug)]
pub struct ConnectionDriver<T>
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    stream: T,
    codec: Connection,
    inbox: mpsc::Receiver<DriverCommand>,
    /// Shared multi-producer channel feeding the `SocketDriver`'s
    /// per-peer event loop. Each entry is tagged with the `peer_id`
    /// this driver was assigned; the receiver dispatches on that.
    peer_out: mpsc::Sender<(u64, PeerOut)>,
    peer_id: u64,
    cancel: CancellationToken,
    config: DriverConfig,
    /// Send-side message encoder (`lz4+tcp://`).
    encoder: Option<MessageEncoder>,
    /// Receive-side message decoder. Symmetric to `encoder`.
    decoder: Option<MessageDecoder>,
    /// Shared round-robin send queue. When set, the driver reads outbound
    /// messages directly from this queue (bypassing the pump task
    /// hop through `inbox`). `None` for non-round-robin socket types.
    shared_msg_rx: Option<QueueReceiver>,
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
    /// this slot's `EncodedQueue`, and the driver flushes them to the
    /// wire. Replaces the `DirectIo` pattern where the handle locked the
    /// writer directly.
    wire_slot: Option<Arc<PeerWireSlot>>,
    arena_threshold: usize,
}

impl<T> ConnectionDriver<T>
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    pub fn new(
        stream: T,
        codec: Connection,
        inbox: mpsc::Receiver<DriverCommand>,
        peer_out: mpsc::Sender<(u64, PeerOut)>,
        peer_id: u64,
        cancel: CancellationToken,
    ) -> Self {
        Self::with_config(
            stream,
            codec,
            inbox,
            peer_out,
            peer_id,
            cancel,
            DriverConfig::default(),
        )
    }

    pub fn with_config(
        stream: T,
        codec: Connection,
        inbox: mpsc::Receiver<DriverCommand>,
        peer_out: mpsc::Sender<(u64, PeerOut)>,
        peer_id: u64,
        cancel: CancellationToken,
        config: DriverConfig,
    ) -> Self {
        Self {
            stream,
            codec,
            inbox,
            peer_out,
            peer_id,
            cancel,
            config,
            encoder: None,
            decoder: None,
            shared_msg_rx: None,
            recv_direct: None,
            compression_pool: None,
            offload_threshold: 0,
            wire_slot: None,
            arena_threshold: omq_proto::encoded_queue::ARENA_THRESHOLD,
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

    /// Provide the shared round-robin send queue. The driver polls this
    /// directly after handshake, eliminating the pump-task intermediary.
    #[must_use]
    pub(crate) fn with_shared_rx(mut self, rx: QueueReceiver) -> Self {
        self.shared_msg_rx = Some(rx);
        self
    }

    /// Install a direct recv channel. When set, inbound `Event::Message`
    /// frames are pushed straight into the user-facing recv channel, bypassing
    /// the `SocketDriver` actor's event loop. Only valid for socket types
    /// whose recv path is a plain fair-queue delivery with no per-type
    /// post-processing.
    #[must_use]
    pub fn with_recv_direct(mut self, tx: async_channel::Sender<Message>) -> Self {
        self.recv_direct = Some(RecvSink::Channel(tx));
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
    /// via the `data_ready` select arm.
    #[must_use]
    pub(crate) fn with_wire_slot(mut self, slot: Arc<PeerWireSlot>) -> Self {
        self.wire_slot = Some(slot);
        self
    }

    #[must_use]
    pub(crate) fn with_arena_threshold(mut self, threshold: usize) -> Self {
        self.arena_threshold = threshold;
        self
    }

    /// Run the driver to completion. Returns:
    /// - `Ok(())` on clean shutdown (peer EOF, canceled, `Close` command,
    ///   inbox dropped).
    /// - `Err(_)` on protocol violations, I/O errors, or codec errors.
    ///
    /// In every exit path (success or error) the driver sends one final
    /// `PeerOut::Closed` on the shared peer-event channel so the
    /// `SocketDriver` can clean up its peer entry. The previous shim task
    /// that did this wrapping is gone - we save one task spawn and one
    /// per-message channel hop on every connection.
    pub async fn run(self) -> Result<()> {
        let peer_out = self.peer_out.clone();
        let peer_id = self.peer_id;
        let result = self.run_inner_body().await;
        let _ = peer_out.send((peer_id, PeerOut::Closed)).await;
        result
    }

    #[expect(clippy::too_many_lines)]
    async fn run_inner_body(self) -> Result<()> {
        let Self {
            stream,
            mut codec,
            mut inbox,
            peer_out,
            peer_id,
            cancel,
            config,
            mut encoder,
            mut decoder,
            shared_msg_rx,
            mut recv_direct,
            compression_pool,
            offload_threshold,
            wire_slot,
            arena_threshold,
        } = self;
        let passthrough = encoder.as_ref().and_then(MessageEncoder::passthrough_info);
        let mut offload_pipeline: OffloadPipeline = FuturesOrdered::new();
        let (mut reader, mut writer) = split(stream);
        let mut read_buf = BytesMut::with_capacity(READ_BUF_SIZE);
        let mut eq = EncodedQueue::with_arena_threshold(arena_threshold);
        let mut drain_buf: Vec<Bytes> = Vec::with_capacity(64);
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
        loop {
            if handshake_deadline.is_some() && codec.is_ready() {
                handshake_deadline = None;
            }

            while let Some(ev) = codec.poll_event() {
                if peer_out.send((peer_id, PeerOut::Event(ev))).await.is_err() {
                    return Ok(());
                }
            }
            while let Some(m) = codec.poll_message() {
                let m = match decoder.as_mut() {
                    Some(dec) => match dec.decode(m)? {
                        Some(plain) => plain,
                        None => continue,
                    },
                    None => m,
                };
                if !route_message(m, &mut recv_direct, &peer_out, peer_id).await {
                    return Ok(());
                }
            }

            // Set handshake_done on the encode slot once the handshake
            // completes and there's no frame transform (CURVE/BLAKE3ZMQ).
            // The slot stays disabled for crypto connections.
            if let Some(ref slot) = wire_slot
                && codec.is_ready()
                && !slot.handshake_done.load(Ordering::Relaxed)
                && !codec.has_frame_transform()
            {
                slot.handshake_done.store(true, Ordering::Release);
            }

            let want_write = codec.has_pending_transmit() || !eq.is_empty();
            let hb_enabled = hb_interval.is_some() && codec.is_ready();

            if let Some(ref slot) = wire_slot {
                slot.driver_in_select.store(true, Ordering::Release);
            }

            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    if let Some(ref slot) = wire_slot {
                        slot.mark_dead();
                    }
                    drain_on_cancel(
                        &mut inbox, shared_msg_rx.as_ref(),
                        wire_slot.as_ref(),
                        &mut encoder, &mut codec, &mut eq,
                        &mut drain_buf, &mut writer, passthrough.as_ref(),
                    ).await;
                    return Ok(());
                }

                // Handshake deadline; disabled once handshake completes.
                () = sleep_until_opt(handshake_deadline), if handshake_deadline.is_some() => {
                    return Err(Error::HandshakeFailed("handshake timeout".into()));
                }

                res = reader.read_buf(&mut read_buf) => {
                    last_input = Instant::now();
                    let n = res?;
                    if n == 0 {
                        if let Some(ref slot) = wire_slot {
                            slot.mark_dead();
                        }
                        cancel.cancel();
                        inbox.close();
                        return Ok(());
                    }
                    let chunk = if decoder.is_some() {
                        let b = Bytes::copy_from_slice(&read_buf);
                        read_buf.clear();
                        b
                    } else {
                        let chunk = read_buf.split().freeze();
                        if read_buf.capacity() < READ_BUF_SIZE {
                            read_buf.reserve(READ_BUF_SIZE);
                        }
                        chunk
                    };
                    if let Err(e) = codec.handle_input(chunk) {
                        while let Some(ev) = codec.poll_event() {
                            let _ = peer_out
                                .send((peer_id, PeerOut::Event(ev)))
                                .await;
                        }
                        return Err(e);
                    }
                    handle_large_messages(
                        &mut codec, &mut reader, &config, &mut last_input,
                    ).await?;
                }

                // Drain completed offloaded compressions and flush.
                Some((pool_enc, frames)) = async {
                    use futures::StreamExt;
                    offload_pipeline.next().await
                }, if !offload_pipeline.is_empty() => {
                    drain_offload_result(pool_enc, frames, compression_pool.as_ref(), &mut eq)?;
                    flush_all(&mut writer, &mut eq, &mut drain_buf, &mut codec).await?;
                }

                res = async {
                    flush_encoded_queue(&mut writer, &mut eq, &mut drain_buf).await?;
                    flush_once(&mut writer, &mut codec).await
                }, if want_write => {
                    res?;
                }

                cmd = inbox.recv() => match cmd {
                    Some(DriverCommand::SendMessage(first)) => {
                        let mut closing = false;
                        let mut deferred: SmallVec<[DriverCommand; 4]> =
                            SmallVec::new();
                        let _ = batch_encode(
                            &first,
                            || match inbox.try_recv() {
                                Ok(DriverCommand::SendMessage(m)) => Some(m),
                                Ok(cmd) => { deferred.push(cmd); None }
                                Err(_) => None,
                            },
                            SHARED_MAX_BATCH_MSGS,
                            &mut encoder, &mut codec, &mut eq,
                            passthrough.as_ref(), compression_pool.as_ref(),
                            offload_threshold, &mut offload_pipeline,
                        ).await?;
                        for cmd in deferred {
                            match cmd {
                                DriverCommand::SendEncoded(chunks) => {
                                    eq.push_shared_chunks(&chunks);
                                }
                                DriverCommand::SendCommand(c) => {
                                    codec.send_command(&c)?;
                                }
                                DriverCommand::Close => closing = true,
                                DriverCommand::SendMessage(_) => unreachable!(),
                            }
                        }
                        flush_all(
                            &mut writer, &mut eq, &mut drain_buf, &mut codec,
                        ).await?;
                        if closing {
                            drain_writes(&mut writer, &mut codec).await.ok();
                            return Ok(());
                        }
                    }
                    Some(DriverCommand::SendEncoded(chunks)) => {
                        eq.push_shared_chunks(&chunks);
                        flush_encoded_queue(&mut writer, &mut eq, &mut drain_buf).await?;
                    }
                    Some(DriverCommand::SendCommand(c)) => codec.send_command(&c)?,
                    Some(DriverCommand::Close) | None => {
                        drain_writes(&mut writer, &mut codec).await.ok();
                        return Ok(());
                    }
                },

                // Wire-slot arm: the socket handle encoded ZMTP frames
                // into the per-peer PeerWireSlot. Drain and write
                // directly, bypassing the local EncodedQueue.
                () = async {
                    wire_slot.as_ref().unwrap().data_ready.notified().await;
                }, if wire_slot.as_ref().is_some_and(|s| {
                    s.handshake_done.load(Ordering::Acquire)
                }) => {
                    drain_wire_slot(
                        wire_slot.as_ref().unwrap(), &mut drain_buf, &mut writer,
                    ).await?;
                },

                // Shared-queue arm: batch-encodes up to
                // SHARED_MAX_BATCH_MSGS messages per wakeup then flushes
                // them all in one or a few write_vectored calls.
                msg = async {
                    if let Some(ref rx) = shared_msg_rx {
                        rx.recv().await
                    } else {
                        std::future::pending().await
                    }
                }, if codec.is_ready() => {
                    match msg {
                        None => {
                            drain_writes(&mut writer, &mut codec).await.ok();
                            return Ok(());
                        }
                        Some(first) => {
                            let batch_limit = shared_msg_rx
                                .as_ref()
                                .map_or(SHARED_MAX_BATCH_MSGS, QueueReceiver::batch_limit);
                            let count = batch_encode(
                                &first,
                                || shared_msg_rx.as_ref().and_then(
                                    QueueReceiver::try_pop,
                                ),
                                batch_limit,
                                &mut encoder, &mut codec, &mut eq,
                                passthrough.as_ref(), compression_pool.as_ref(),
                                offload_threshold, &mut offload_pipeline,
                            ).await?;
                            flush_all(
                                &mut writer, &mut eq, &mut drain_buf, &mut codec,
                            ).await?;
                            if let Some(ref rx) = shared_msg_rx {
                                rx.release_permits(count);
                            }
                        }
                    }
                },

                // Heartbeat tick: enabled only post-handshake when
                // `heartbeat_interval` is set. Uses a persistent pinned
                // sleep so the safety-net timeout doesn't reset it.
                () = &mut hb_sleep, if hb_enabled => {
                    if last_input.elapsed() > hb_timeout {
                        return Err(Error::Timeout);
                    }
                    let ping = Command::Ping {
                        ttl_deciseconds: hb_ttl_deciseconds,
                        context: Bytes::new(),
                    };
                    let _ = codec.send_command(&ping);
                    hb_sleep.as_mut().reset(
                        tokio::time::Instant::now() + hb_interval.unwrap(),
                    );
                }

                // TODO: remove after extended soak testing confirms no stalls.
                // All wakeup paths (data_ready Notify, inbox mpsc, heartbeat
                // timer) are believed correct. Disabled to eliminate 6400
                // spurious polls/sec at 64 peers.
                // () = tokio::time::sleep(Duration::from_millis(10)) => {}
            }

            if let Some(ref slot) = wire_slot {
                slot.driver_in_select.store(false, Ordering::Relaxed);
            }
        }
    }
}

/// Sleep until an `Option<Instant>`. Returns immediately if `None`, which
/// paired with a select `if` guard means this branch won't fire.
async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(t) => tokio::time::sleep_until(t.into()).await,
        None => std::future::pending::<()>().await,
    }
}

#[expect(clippy::too_many_arguments)]
async fn drain_on_cancel<W: AsyncWrite + Unpin>(
    inbox: &mut mpsc::Receiver<DriverCommand>,
    shared_msg_rx: Option<&QueueReceiver>,
    wire_slot: Option<&Arc<PeerWireSlot>>,
    encoder: &mut Option<MessageEncoder>,
    codec: &mut Connection,
    eq: &mut EncodedQueue,
    drain_buf: &mut Vec<Bytes>,
    writer: &mut W,
    passthrough: Option<&(Bytes, usize)>,
) {
    if let Some(slot) = wire_slot {
        drain_buf.clear();
        slot.drain_into_vec(drain_buf, 1024);
        eq.push_shared_chunks(drain_buf);
        drain_buf.clear();
    }
    while let Ok(cmd) = inbox.try_recv() {
        match cmd {
            DriverCommand::SendMessage(msg) => {
                encode_msg(&msg, encoder, codec, eq, passthrough).ok();
            }
            DriverCommand::SendEncoded(chunks) => {
                eq.push_shared_chunks(&chunks);
            }
            DriverCommand::SendCommand(c) => {
                let _ = codec.send_command(&c);
            }
            DriverCommand::Close => break,
        }
    }
    if let Some(rx) = shared_msg_rx {
        let mut drained = 0usize;
        while let Some(msg) = rx.try_pop() {
            encode_msg(&msg, encoder, codec, eq, passthrough).ok();
            drained += 1;
        }
        rx.release_permits(drained);
    }
    let _ = flush_encoded_queue(writer, eq, drain_buf).await;
    drain_writes(writer, codec).await.ok();
}

/// Flush `EncodedQueue` to the writer, then drain any pending codec
/// transmits (command frames queued during encoding).
async fn flush_all<W: AsyncWrite + Unpin>(
    writer: &mut W,
    eq: &mut EncodedQueue,
    drain_buf: &mut Vec<Bytes>,
    codec: &mut Connection,
) -> io::Result<()> {
    flush_encoded_queue(writer, eq, drain_buf).await?;
    while codec.has_pending_transmit() {
        flush_once(writer, codec).await?;
    }
    Ok(())
}

/// Drain the per-peer [`PeerWireSlot`] and write directly to the wire.
async fn drain_wire_slot<W: AsyncWrite + Unpin>(
    slot: &PeerWireSlot,
    drain_buf: &mut Vec<Bytes>,
    writer: &mut W,
) -> io::Result<()> {
    let mut batch_bytes = 0usize;
    loop {
        drain_buf.clear();
        slot.drain_into_vec(drain_buf, 1024);
        if drain_buf.is_empty() {
            break;
        }
        batch_bytes += drain_buf.iter().map(Bytes::len).sum::<usize>();
        write_chunks(writer, drain_buf).await?;
        slot.space_available.notify_one();
        if batch_bytes >= max_batch_bytes() {
            break;
        }
    }
    Ok(())
}

/// After reading bytes from the wire, check for large frames whose
/// payload exceeds the threshold and read them directly into a
/// pre-sized buffer (bypasses the fixed `read_buf` -> codec copy path).
async fn handle_large_messages<R: AsyncRead + Unpin>(
    codec: &mut Connection,
    reader: &mut R,
    config: &DriverConfig,
    last_input: &mut Instant,
) -> Result<()> {
    #[cfg(feature = "ws")]
    let skip_large = codec.is_ws();
    #[cfg(not(feature = "ws"))]
    let skip_large = false;
    if config.large_message_threshold == 0 || codec.has_frame_transform() || skip_large {
        return Ok(());
    }
    while let Some(info) = codec.peek_next_frame_payload_size()? {
        if info.payload_len < config.large_message_threshold {
            break;
        }
        let Some((plen, prefix)) = codec.begin_supplied_payload_with_prefix() else {
            break;
        };
        let mut buf = BytesMut::zeroed(plen);
        buf[..prefix.len()].copy_from_slice(prefix.as_slice());
        if prefix.len() < plen {
            reader.read_exact(&mut buf[prefix.len()..]).await?;
        }
        *last_input = Instant::now();
        codec.supply_payload(buf.freeze())?;
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

/// Drain all completed futures from the pipeline into `EncodedQueue`.
async fn drain_pipeline(
    pipeline: &mut OffloadPipeline,
    pool: Option<&Arc<CompressionPool>>,
    eq: &mut EncodedQueue,
) -> Result<()> {
    use futures::StreamExt;
    while let Some((pool_enc, frames)) = pipeline.next().await {
        drain_offload_result(pool_enc, frames, pool, eq)?;
    }
    Ok(())
}

fn drain_offload_result(
    pool_enc: Option<MessageEncoder>,
    frames: Result<TransformedOut>,
    pool: Option<&Arc<CompressionPool>>,
    eq: &mut EncodedQueue,
) -> Result<()> {
    if let (Some(enc), Some(pool)) = (pool_enc, pool) {
        pool.put(enc);
    }
    for wire in frames? {
        eq.encode_auto(&wire);
    }
    Ok(())
}

/// Encode one message into `EncodedQueue`. When a compression encoder
/// is active, the message is transformed first; the resulting wire
/// message(s) are then framed into EQ. When no encoder is present the
/// message is framed directly. Sub-threshold messages on compression
/// transports take a sentinel-prefix fast path that avoids the encoder
/// entirely.
///
/// The only path that still goes through `codec.send_message` is when a
/// frame-level transform (CURVE/BLAKE3ZMQ) is active, since those
/// encrypt at the ZMTP frame layer and need the codec's internal state.
fn encode_msg(
    msg: &Message,
    encoder: &mut Option<MessageEncoder>,
    codec: &mut Connection,
    eq: &mut EncodedQueue,
    passthrough: Option<&(Bytes, usize)>,
) -> Result<()> {
    #[cfg(feature = "ws")]
    if codec.is_ws() && !codec.has_frame_transform() {
        let masked = matches!(
            codec.ws_role(),
            Some(omq_proto::proto::connection::WsRole::Client)
        );
        eq.encode_ws(msg, masked);
        return Ok(());
    }
    if codec.has_frame_transform() {
        if let Some(enc) = encoder.as_mut() {
            for wire in enc.encode(msg)? {
                codec.send_message(&wire)?;
            }
        } else {
            codec.send_message(msg)?;
        }
        return Ok(());
    }
    if let Some((sentinel, threshold)) = passthrough
        && msg.iter().all(|b| b.len() < *threshold)
    {
        eq.encode_prefixed_auto(sentinel, msg);
    } else if let Some(enc) = encoder.as_mut() {
        for wire in enc.encode(msg)? {
            eq.encode_auto(&wire);
        }
    } else {
        eq.encode_auto(msg);
    }
    Ok(())
}

/// Route a decoded message to `recv_direct` or through the actor.
/// Returns `true` if sent, `false` if the receiving channel closed.
async fn route_message(
    m: Message,
    recv_direct: &mut Option<RecvSink>,
    peer_out: &mpsc::Sender<(u64, PeerOut)>,
    peer_id: u64,
) -> bool {
    match recv_direct {
        Some(sink) => sink.send(m).await,
        None => peer_out
            .send((peer_id, PeerOut::Event(Event::Message(m))))
            .await
            .is_ok(),
    }
}

/// Flush the `EncodedQueue` to the writer. Drains chunks into a
/// reusable `Vec<Bytes>`, builds `IoSlice` refs, and does one
/// `write_vectored`. On partial write, unwritten chunks are restored
/// to the queue front.
pub(crate) async fn flush_encoded_queue<W>(
    writer: &mut W,
    eq: &mut EncodedQueue,
    drain_buf: &mut Vec<Bytes>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    loop {
        drain_buf.clear();
        eq.drain_into_vec(drain_buf, 1024);
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
async fn flush_once<W>(writer: &mut W, codec: &mut Connection) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let chunks = codec.transmit_chunks_capped(128);
    if chunks.is_empty() {
        return Ok(());
    }
    let n = writer.write_vectored(&chunks).await?;
    drop(chunks);
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
    }
    codec.advance_transmit(n);
    Ok(())
}

/// Best-effort flush of remaining outbound bytes on shutdown.
async fn drain_writes<W>(writer: &mut W, codec: &mut Connection) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    while codec.has_pending_transmit() {
        flush_once(writer, codec).await?;
    }
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use omq_proto::proto::connection::{ConnectionConfig, Role};
    use omq_proto::proto::{Event, SocketType};

    /// Adapter: pull `(u64, PeerOut::Event)` off the shared peer-out
    /// channel and yield bare `Event` values, matching the older
    /// per-side events channel shape the tests were written
    /// against. `PeerOut::Closed` ends the stream (returns None).
    pub(super) struct EventAdapter {
        rx: mpsc::Receiver<(u64, PeerOut)>,
    }

    impl EventAdapter {
        pub(super) async fn recv(&mut self) -> Option<Event> {
            match self.rx.recv().await? {
                (_, PeerOut::Event(e)) => Some(e),
                (_, PeerOut::Closed) => None,
            }
        }
    }

    /// Spin up two drivers connected via an in-memory duplex pair,
    /// return handles + event rxes. The connection driver is generic
    /// over T: AsyncRead+AsyncWrite, so a `tokio::io::duplex` pair
    /// is the simplest way to test it without involving the inproc
    /// transport (which since the inproc fast-path landed bypasses
    /// the codec entirely).
    #[expect(clippy::unused_async)]
    async fn inproc_pair(_name: &str) -> (DriverHandle, EventAdapter, DriverHandle, EventAdapter) {
        let (server_stream, client_stream) = tokio::io::duplex(64 * 1024);

        let server_codec = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let client_codec = Connection::new(
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
            server_codec,
            s_inbox_rx,
            s_evt_tx,
            0,
            s_cancel.clone(),
        );
        let c_driver = ConnectionDriver::new(
            client_stream,
            client_codec,
            c_inbox_rx,
            c_evt_tx,
            0,
            c_cancel.clone(),
        );

        tokio::spawn(async move { s_driver.run().await });
        tokio::spawn(async move { c_driver.run().await });

        (
            DriverHandle {
                inbox: c_inbox_tx,
                cancel: c_cancel,
                wire_slot: None,
            },
            EventAdapter { rx: c_evt_rx },
            DriverHandle {
                inbox: s_inbox_tx,
                cancel: s_cancel,
                wire_slot: None,
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
        let (client, mut client_events, _server, mut server_events) = inproc_pair("drv-msg").await;
        client_events.recv().await.unwrap();
        server_events.recv().await.unwrap();

        client
            .inbox
            .send(DriverCommand::SendMessage(Message::single("hello")))
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
        let res = client.inbox.send(DriverCommand::Close).await;
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

        let server_codec = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let client_codec = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));

        let (c_inbox_tx, c_inbox_rx) = mpsc::channel(16);
        let (s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
        let (c_evt_tx, c_evt_rx) = mpsc::channel(16);
        let (s_evt_tx, s_evt_rx) = mpsc::channel(16);
        let mut c_evt_rx = EventAdapter { rx: c_evt_rx };
        let mut s_evt_rx = EventAdapter { rx: s_evt_rx };

        let s = ConnectionDriver::new(
            server_stream,
            server_codec,
            s_inbox_rx,
            s_evt_tx,
            0,
            CancellationToken::new(),
        );
        let c = ConnectionDriver::new(
            client_stream,
            client_codec,
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
        let server_codec = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let (_s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
        let (s_evt_tx, mut s_evt_rx) = mpsc::channel::<(u64, PeerOut)>(16);
        let s_driver = ConnectionDriver::new(
            server_stream,
            server_codec,
            s_inbox_rx,
            s_evt_tx,
            0,
            CancellationToken::new(),
        );
        tokio::spawn(async move { s_driver.run().await });

        // Manual client: use a codec to generate correct wire bytes.
        let mut client_codec = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push)
                .identity(Bytes::from_static(b"x")),
        );

        // Write client greeting.
        let greeting = drain_transmit(&mut client_codec);
        client_stream.write_all(&greeting).await.unwrap();

        // Read server greeting + READY from the duplex and feed to
        // client codec until it reaches Ready state.
        let mut buf = vec![0u8; 4096];
        while !client_codec.is_ready() {
            let n = client_stream.read(&mut buf).await.unwrap();
            assert!(n > 0, "server closed before handshake");
            client_codec
                .handle_input(Bytes::copy_from_slice(&buf[..n]))
                .unwrap();
        }

        // Client codec has produced READY. Also encode ERROR.
        let ready_bytes = drain_transmit(&mut client_codec);
        client_codec
            .send_command(&Command::Error {
                reason: "boom".into(),
            })
            .unwrap();
        let error_bytes = drain_transmit(&mut client_codec);

        // Write READY + ERROR in a single write so the server driver
        // reads them in one handle_input call.
        let mut combined = Vec::with_capacity(ready_bytes.len() + error_bytes.len());
        combined.extend_from_slice(&ready_bytes);
        combined.extend_from_slice(&error_bytes);
        client_stream.write_all(&combined).await.unwrap();

        // Collect all events from the server driver.
        let mut events = Vec::new();
        while let Some((_, out)) = s_evt_rx.recv().await {
            let is_closed = matches!(out, PeerOut::Closed);
            events.push(out);
            if is_closed {
                break;
            }
        }

        assert!(
            events
                .iter()
                .any(|e| matches!(e, PeerOut::Event(Event::HandshakeSucceeded { .. }))),
            "HandshakeSucceeded must not be lost when coalesced with \
             a post-handshake protocol error; got: {events:?}",
        );
    }

    fn drain_transmit(codec: &mut Connection) -> Vec<u8> {
        let mut out = Vec::new();
        while codec.has_pending_transmit() {
            let len_before = out.len();
            for chunk in codec.transmit_chunks_capped(128) {
                out.extend_from_slice(&chunk);
            }
            codec.advance_transmit(out.len() - len_before);
        }
        out
    }
}
