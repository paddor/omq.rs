//! Natural batching, ordered encoding, and compression offloads.

#[cfg(target_os = "linux")]
use super::shm;
use super::{BATCH_TIME_CHECK_MSGS, OUTBOUND_BATCH_TIME};
use crate::engine::compression_pool::CompressionPool;
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
        transform::{MessageEncoder, TransformedOut},
    },
};
use std::{sync::Arc, time::Instant};

pub(super) struct OutboundState {
    pub(super) encoder: Option<MessageEncoder>,
    pub(super) passthrough: Option<(Bytes, usize)>,
    pub(super) compression_pool: Option<Arc<CompressionPool>>,
    pub(super) offload_threshold: usize,
    pub(super) offload_pipeline: OffloadPipeline,
    #[cfg(target_os = "linux")]
    shm: Option<shm::Outbound>,
}

impl std::fmt::Debug for OutboundState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("OutboundState");
        debug
            .field("has_encoder", &self.encoder.is_some())
            .field("has_passthrough", &self.passthrough.is_some())
            .field("has_compression_pool", &self.compression_pool.is_some())
            .field("offload_threshold", &self.offload_threshold)
            .field("offload_pipeline_len", &self.offload_pipeline.len());
        #[cfg(target_os = "linux")]
        debug.field("shm", &self.shm.is_some()).field(
            "shm_pending",
            &self.shm.as_ref().map_or(0, shm::Outbound::pending_len),
        );
        debug.finish()
    }
}

impl OutboundState {
    pub(super) fn new(
        encoder: Option<MessageEncoder>,
        compression_pool: Option<Arc<CompressionPool>>,
        offload_threshold: usize,
        #[cfg(target_os = "linux")] shm_state: Option<crate::transport::shm::ShmState>,
    ) -> Self {
        let passthrough = encoder.as_ref().and_then(MessageEncoder::passthrough_info);
        Self {
            encoder,
            passthrough,
            compression_pool,
            offload_threshold,
            offload_pipeline: FuturesOrdered::new(),
            #[cfg(target_os = "linux")]
            shm: shm_state.map(shm::Outbound::new),
        }
    }

    /// Encode a bounded batch directly, or queue ordered compression offloads.
    pub(super) fn batch_encode(
        &mut self,
        first: &Message,
        mut try_recv: impl FnMut() -> Option<Message>,
        max_msgs: usize,
        connection: &mut Connection,
        eq: &mut FrameBuffer,
    ) -> Result<usize> {
        #[cfg(target_os = "linux")]
        if let Some(shm) = &mut self.shm {
            return shm.stage(first, try_recv, max_msgs, eq);
        }
        let started = Instant::now();
        let use_pipeline = self.offload_threshold > 0
            && self
                .encoder
                .as_ref()
                .is_some_and(MessageEncoder::can_offload)
            && self.compression_pool.is_some();
        let mut encode = |message: &Message| -> Result<()> {
            if use_pipeline {
                submit_to_pipeline(
                    message,
                    self.encoder.as_mut().unwrap(),
                    self.compression_pool.as_ref().unwrap(),
                    self.offload_threshold,
                    &mut self.offload_pipeline,
                );
                Ok(())
            } else {
                encode_msg(
                    message,
                    &mut self.encoder,
                    connection,
                    eq,
                    self.passthrough.as_ref(),
                )
            }
        };
        encode(first)?;
        let mut count = 1usize;
        let mut bytes = first.byte_len();
        while count < max_msgs && bytes < max_batch_bytes() {
            if count.is_multiple_of(BATCH_TIME_CHECK_MSGS)
                && started.elapsed() >= OUTBOUND_BATCH_TIME
            {
                break;
            }
            match try_recv() {
                Some(next) => {
                    bytes += next.byte_len();
                    encode(&next)?;
                    count += 1;
                }
                None => break,
            }
        }
        Ok(count)
    }

    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    pub(super) fn has_pending_shm(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            self.shm.as_ref().is_some_and(shm::Outbound::has_pending)
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    pub(super) fn uses_shm(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            self.shm.is_some()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    #[cfg_attr(
        not(target_os = "linux"),
        expect(clippy::unused_self, clippy::unnecessary_wraps)
    )]
    pub(super) fn drain_shm(&mut self, eq: &mut FrameBuffer) -> Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(shm) = &mut self.shm {
            shm.drain(eq)?;
        }
        #[cfg(not(target_os = "linux"))]
        let _ = eq;
        Ok(())
    }

    pub(super) fn has_pending_offload(&self) -> bool {
        !self.offload_pipeline.is_empty()
    }

    pub(super) async fn next_offload(
        &mut self,
    ) -> Option<(Option<MessageEncoder>, Result<TransformedOut>)> {
        use futures::StreamExt;
        self.offload_pipeline.next().await
    }

    pub(super) fn drain_offload_result(
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
    pub(super) fn drain_ready_offload_batch(
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
pub(super) fn submit_to_pipeline(
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
pub(super) fn drain_offload_result(
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
pub(super) fn encode_msg(
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
