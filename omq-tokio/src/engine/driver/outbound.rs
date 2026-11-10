//! Natural batching, ordered encoding, and compression offloads.

use super::shm;
use super::{BATCH_TIME_CHECK_MSGS, OUTBOUND_BATCH_TIME};
use crate::engine::compression_pool::{CompressionPool, EncoderLoan};
use crate::routing::OUTBOUND_BATCH_MAX_MSGS;
use bytes::Bytes;
use futures::stream::FuturesOrdered;
#[cfg(any(feature = "lz4", feature = "zstd"))]
use omq_proto::error::Error;
use omq_proto::{
    Message,
    error::Result,
    flow::max_batch_bytes,
    frame_buffer::FrameBuffer,
    proto::{
        Connection,
        transform::{ADAPT_WINDOW, MessageEncoder, OutputWindow, TransformedOut},
    },
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub(super) struct OutboundState {
    pub(super) encoder: Option<MessageEncoder>,
    pub(super) passthrough: Option<(Bytes, usize)>,
    pub(super) compression_pool: Option<Arc<CompressionPool>>,
    pub(super) offload_threshold: usize,
    pub(super) offload_pipeline: OffloadPipeline,
    level_window: Option<LevelWindow>,
    shm: Option<shm::Outbound>,
}

/// Sender conditions over one compression-level adaptation window.
#[derive(Debug)]
struct LevelWindow {
    started: Instant,
    batches: u32,
    backlogged: u32,
    overflowed: u32,
    input_full: u32,
    due: bool,
}

impl LevelWindow {
    fn new(started: Instant) -> Self {
        Self {
            started,
            batches: 0,
            backlogged: 0,
            overflowed: 0,
            input_full: 0,
            due: false,
        }
    }
}

impl std::fmt::Debug for OutboundState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutboundState")
            .field("has_encoder", &self.encoder.is_some())
            .field("has_passthrough", &self.passthrough.is_some())
            .field("has_compression_pool", &self.compression_pool.is_some())
            .field("offload_threshold", &self.offload_threshold)
            .field("offload_pipeline_len", &self.offload_pipeline.len())
            .field("level_window", &self.level_window)
            .field("shm", &self.shm.is_some())
            .field(
                "shm_pending",
                &self.shm.as_ref().map_or(0, shm::Outbound::pending_len),
            )
            .finish()
    }
}

impl OutboundState {
    pub(super) fn new(
        encoder: Option<MessageEncoder>,
        compression_pool: Option<Arc<CompressionPool>>,
        offload_threshold: usize,
        shm_state: Option<crate::engine::shm_state::ShmState>,
    ) -> Self {
        let passthrough = encoder.as_ref().and_then(MessageEncoder::passthrough_info);
        let level_window = encoder
            .as_ref()
            .is_some_and(MessageEncoder::is_adaptive)
            .then(|| LevelWindow::new(Instant::now()));

        Self {
            encoder,
            passthrough,
            compression_pool,
            offload_threshold,
            offload_pipeline: FuturesOrdered::new(),
            level_window,
            shm: shm_state.map(shm::Outbound::new),
        }
    }

    /// Encode a batch of at most `OUTBOUND_BATCH_MAX_MSGS` messages directly,
    /// or queue ordered compression offloads. Returns whether the batch
    /// stopped at its message, byte, or time limit before input ran out.
    pub(super) fn batch_encode(
        &mut self,
        first: &Message,
        mut try_recv: impl FnMut() -> Option<Message>,
        connection: &mut Connection,
        eq: &mut FrameBuffer,
    ) -> Result<bool> {
        let max_msgs = OUTBOUND_BATCH_MAX_MSGS;
        if let Some(shm) = &mut self.shm {
            return shm
                .stage(first, try_recv, max_msgs, eq)
                .map(|count| count >= max_msgs);
        }

        #[cfg(any(feature = "lz4", feature = "zstd"))]
        if self
            .encoder
            .as_ref()
            .is_some_and(|encoder| encoder.arena_capacity() > 0)
        {
            return self.batch_encode_arena(first, try_recv, connection, eq);
        }

        let started = Instant::now();
        // With offload on, small messages also enter the ordered pipeline as
        // ready results, so they cannot overtake a large offload on the wire.
        let use_pipeline = self.offload_enabled();
        let mut held = None;

        let mut encode = |message: &Message, held: &mut Option<Message>| -> Result<()> {
            if use_pipeline {
                self.pipeline_message(message.clone(), held);
                Ok(())
            } else {
                self.encode(message, connection, eq)
            }
        };

        encode(first, &mut held)?;
        let mut count = 1usize;
        let mut bytes = first.byte_len();
        let mut limited = true;

        while count < max_msgs && bytes < max_batch_bytes() {
            if count.is_multiple_of(BATCH_TIME_CHECK_MSGS)
                && started.elapsed() >= OUTBOUND_BATCH_TIME
            {
                break;
            }

            let Some(next) = try_recv() else {
                limited = false;
                break;
            };

            bytes += next.byte_len();
            encode(&next, &mut held)?;
            count += 1;
        }

        self.encode_held(held, connection, eq)?;
        Ok(limited)
    }

    /// Pack small messages into shared compression arenas. A message that
    /// does not fit any arena closes the current one and is encoded alone.
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn batch_encode_arena(
        &mut self,
        first: &Message,
        mut try_recv: impl FnMut() -> Option<Message>,
        connection: &mut Connection,
        eq: &mut FrameBuffer,
    ) -> Result<bool> {
        use omq_proto::proto::transform::arena::{
            MAX_ARENA_MESSAGES, MAX_ARENA_PARTS, arena_message_size,
        };

        let started = Instant::now();
        let capacity = self.encoder.as_ref().unwrap().arena_capacity();

        let mut messages = smallvec::SmallVec::<[Message; 32]>::new();
        let mut arena_bytes = 0;
        let mut parts = 0;
        let mut count = 0;
        let mut bytes = 0;
        let mut next = Some(first.clone());
        let mut held = None;
        let mut limited = false;

        while let Some(message) = next {
            let size = arena_message_size(&message).filter(|size| *size <= capacity);
            if size.is_none()
                || arena_bytes + size.unwrap_or(0) > capacity
                || messages.len() == MAX_ARENA_MESSAGES
                || parts + message.len() > MAX_ARENA_PARTS
            {
                if !messages.is_empty() {
                    self.offload_held(&mut held);
                }

                self.flush_arena(&messages, connection, eq)?;
                messages.clear();
                arena_bytes = 0;
                parts = 0;
            }

            count += 1;
            bytes += message.byte_len();

            if let Some(size) = size {
                arena_bytes += size;
                parts += message.len();
                messages.push(message);
            } else {
                self.encode_outside_arena(message, &mut held, connection, eq)?;
            }

            if count >= OUTBOUND_BATCH_MAX_MSGS
                || bytes >= max_batch_bytes()
                || (count.is_multiple_of(BATCH_TIME_CHECK_MSGS)
                    && started.elapsed() >= OUTBOUND_BATCH_TIME)
            {
                limited = true;
                break;
            }

            next = try_recv();
        }

        if !messages.is_empty() {
            self.offload_held(&mut held);
        }

        self.flush_arena(&messages, connection, eq)?;
        self.encode_held(held, connection, eq)?;
        Ok(limited)
    }

    /// Preserve ordered large-message offloads around small arenas.
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn encode_outside_arena(
        &mut self,
        message: Message,
        held: &mut Option<Message>,
        connection: &Connection,
        eq: &mut FrameBuffer,
    ) -> Result<()> {
        if self.offload_enabled() {
            self.pipeline_message(message, held);
            return Ok(());
        }

        let wire = self.encoder.as_mut().unwrap().encode(&message)?;
        self.queue_arena_wire(wire, connection, eq)
    }

    /// Queue a message in wire order behind earlier offloads. Work now
    /// follows the held message, so it goes to the pool. A large message
    /// is held until the batch shows whether work follows it too.
    fn pipeline_message(&mut self, message: Message, held: &mut Option<Message>) {
        self.offload_held(held);
        if message.byte_len() >= self.offload_threshold {
            *held = Some(message);
        } else {
            self.submit(&message);
        }
    }

    fn offload_held(&mut self, held: &mut Option<Message>) {
        if let Some(message) = held.take() {
            self.submit(&message);
        }
    }

    /// Whether ordered compression offloads are on.
    fn offload_enabled(&self) -> bool {
        self.offload_threshold > 0
            && self.compression_pool.is_some()
            && self
                .encoder
                .as_ref()
                .is_some_and(MessageEncoder::can_offload)
    }

    /// Submit one message to the offload pipeline. Messages of at least
    /// `offload_threshold` bytes go to `spawn_blocking` with a pool encoder;
    /// small messages and pool-exhausted fallbacks encode on the driver.
    fn submit(&mut self, msg: &Message) {
        let encoder = self.encoder.as_mut().unwrap();
        #[cfg(any(feature = "lz4", feature = "zstd"))]
        if msg.byte_len() >= self.offload_threshold
            && let Some(mut loan) = self.compression_pool.as_ref().unwrap().try_take(encoder)
        {
            let msg = msg.clone();
            let handle = tokio::task::spawn_blocking(move || {
                let result = loan.encoder.encode(&msg);
                (Some(loan), result)
            });

            self.offload_pipeline.push_back(Box::pin(async move {
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
        self.offload_pipeline
            .push_back(Box::pin(futures::future::ready((None, result))));
    }

    /// Encode one message into `FrameBuffer`. When a compression encoder
    /// is active, the message is transformed first; the resulting wire
    /// message(s) are then framed into EQ. When no encoder is present the
    /// message is framed directly. Sub-threshold messages on compression
    /// transports take a sentinel-prefix fast path that avoids the encoder
    /// entirely.
    ///
    /// The only path that still goes through `connection.send_message` is
    /// when a frame-level transform (CURVE) is active, since those encrypt
    /// at the ZMTP frame layer and need the connection's internal state.
    pub(super) fn encode(
        &mut self,
        msg: &Message,
        connection: &mut Connection,
        eq: &mut FrameBuffer,
    ) -> Result<()> {
        #[cfg(feature = "ws")]
        if connection.is_ws() && !connection.has_frame_transform() {
            let role = ws_send_role(connection);
            if let Some(enc) = self.encoder.as_mut() {
                for wire in enc.encode(msg)? {
                    eq.frame_ws(&wire, role);
                }
            } else {
                eq.frame_ws(msg, role);
            }

            return Ok(());
        }

        if connection.has_frame_transform() {
            if let Some(enc) = self.encoder.as_mut() {
                for wire in enc.encode(msg)? {
                    connection.send_message(&wire)?;
                }
            } else {
                connection.send_message(msg)?;
            }

            return Ok(());
        }

        if let Some((sentinel, threshold)) = &self.passthrough
            && msg.iter().all(|b| b.len() < *threshold)
        {
            eq.frame_prefixed(sentinel, msg);
        } else if let Some(enc) = self.encoder.as_mut() {
            for wire in enc.encode(msg)? {
                eq.frame(&wire);
            }
        } else {
            eq.frame(msg);
        }

        Ok(())
    }

    /// Compress the batch's last large message on this thread. Offloading
    /// it would only make the driver wait for the pool.
    fn encode_held(
        &mut self,
        held: Option<Message>,
        connection: &Connection,
        eq: &mut FrameBuffer,
    ) -> Result<()> {
        let Some(message) = held else {
            return Ok(());
        };

        let wire = self.encoder.as_mut().unwrap().encode(&message)?;
        self.queue_arena_wire(wire, connection, eq)
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn flush_arena(
        &mut self,
        messages: &[Message],
        connection: &mut Connection,
        eq: &mut FrameBuffer,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        if messages.len() == 1 && self.offload_pipeline.is_empty() {
            return self.encode(&messages[0], connection, eq);
        }

        let encoder = self.encoder.as_mut().unwrap();
        let wire = if messages.len() == 1 {
            encoder.encode(&messages[0])?
        } else {
            let references: smallvec::SmallVec<[&Message; 32]> = messages.iter().collect();
            encoder.encode_arena(&references)?
        };

        self.queue_arena_wire(wire, connection, eq)
    }

    fn queue_arena_wire(
        &mut self,
        wire: TransformedOut,
        connection: &Connection,
        eq: &mut FrameBuffer,
    ) -> Result<()> {
        if self.offload_pipeline.is_empty() {
            drain_offload_result(None, Ok(wire), connection, eq)
        } else {
            self.offload_pipeline
                .push_back(Box::pin(std::future::ready((None, Ok(wire)))));
            Ok(())
        }
    }

    pub(super) fn has_pending_shm(&self) -> bool {
        self.shm.as_ref().is_some_and(shm::Outbound::has_pending)
    }

    pub(super) fn uses_shm(&self) -> bool {
        self.shm.is_some()
    }

    pub(super) fn drain_shm(&mut self, eq: &mut FrameBuffer) -> Result<()> {
        if let Some(shm) = &mut self.shm {
            shm.drain(eq)?;
        }

        Ok(())
    }

    /// Whether the encoder adapts its compression level.
    pub(super) fn adapts_level(&self) -> bool {
        self.level_window.is_some()
    }

    /// Count one encoded batch toward the level adaptation window: whether
    /// input was queued after it, whether it stopped at a batch limit, and
    /// whether it freed space in a full input queue.
    pub(super) fn note_batch(&mut self, backlogged: bool, limited: bool, input_full: bool) {
        let Some(window) = &mut self.level_window else {
            return;
        };

        window.batches += 1;
        window.backlogged += u32::from(backlogged);
        window.overflowed += u32::from(backlogged && limited);
        window.input_full += u32::from(input_full);
        window.due = window.started.elapsed() >= ADAPT_WINDOW
            && self
                .encoder
                .as_ref()
                .is_some_and(MessageEncoder::adaptation_ready);
    }

    pub(super) fn level_window_due(&self) -> bool {
        self.level_window.as_ref().is_some_and(|window| window.due)
    }

    /// Close the adaptation window with the time the writer stalled in it.
    pub(super) fn adapt_level(&mut self, now: Instant, blocked: Duration) {
        let (Some(window), Some(encoder)) = (&mut self.level_window, &mut self.encoder) else {
            return;
        };

        encoder.adapt_level(OutputWindow {
            elapsed: now.saturating_duration_since(window.started),
            blocked,
            batches: window.batches,
            backlogged: window.backlogged,
            overflowed: window.overflowed,
            input_full: window.input_full,
        });

        *window = LevelWindow::new(now);
    }

    pub(super) fn has_pending_offload(&self) -> bool {
        !self.offload_pipeline.is_empty()
    }

    pub(super) async fn next_offload(
        &mut self,
    ) -> Option<(Option<EncoderLoan>, Result<TransformedOut>)> {
        use futures::StreamExt;
        self.offload_pipeline.next().await
    }

    /// Drain the selected result plus any immediately-ready followers. This
    /// preserves gather-write batching for small compressed messages without
    /// awaiting compression outside the driver's main `select!`.
    pub(super) fn drain_ready_offload_batch(
        &mut self,
        first: (Option<EncoderLoan>, Result<TransformedOut>),
        connection: &Connection,
        eq: &mut FrameBuffer,
    ) -> Result<usize> {
        use futures::{FutureExt as _, StreamExt as _};

        let started = Instant::now();
        let mut count = 0;
        let mut next = Some(first);

        while let Some((mut loan, frames)) = next {
            if let (Some(loan), Some(encoder)) = (loan.as_mut(), self.encoder.as_mut()) {
                encoder.absorb_offload_stats(&mut loan.encoder);
            }

            drain_offload_result(loan, frames, connection, eq)?;
            count += 1;

            if count >= OUTBOUND_BATCH_MAX_MSGS
                || (count.is_multiple_of(BATCH_TIME_CHECK_MSGS)
                    && started.elapsed() >= OUTBOUND_BATCH_TIME)
            {
                break;
            }

            next = self.offload_pipeline.next().now_or_never().flatten();
        }

        Ok(count)
    }
}

pub(super) type OffloadPipeline = FuturesOrdered<
    std::pin::Pin<
        Box<dyn std::future::Future<Output = (Option<EncoderLoan>, Result<TransformedOut>)> + Send>,
    >,
>;

#[allow(unused_variables)]
pub(super) fn drain_offload_result(
    loan: Option<EncoderLoan>,
    frames: Result<TransformedOut>,
    connection: &Connection,
    eq: &mut FrameBuffer,
) -> Result<()> {
    if let Some(loan) = loan {
        loan.restore();
    }

    #[cfg(feature = "ws")]
    let ws = connection.is_ws().then(|| ws_send_role(connection));

    for wire in frames? {
        #[cfg(feature = "ws")]
        if let Some(role) = ws {
            eq.frame_ws(&wire, role);
            continue;
        }

        eq.frame(&wire);
    }

    Ok(())
}

/// Role for outgoing ZWS frames; WebSocket clients mask them.
#[cfg(feature = "ws")]
fn ws_send_role(connection: &Connection) -> omq_proto::proto::connection::WsRole {
    connection
        .ws_role()
        .unwrap_or(omq_proto::proto::connection::WsRole::Server)
}

#[cfg(all(test, feature = "zstd"))]
mod tests;
