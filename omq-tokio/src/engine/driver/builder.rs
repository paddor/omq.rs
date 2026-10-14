//! Connection configuration before the driver is spawned.

use super::{
    ConnectionDriver, DriverStream, PeerDriverCommand, PeerDriverConfig, PeerDriverData, PeerEvent,
    ReceiveProfile, RecvSink,
};
use crate::engine::{
    actor_output::{DataSender, PeerOutput},
    compression_pool::CompressionPool,
    peer_completion::CompletionProgress,
    rate_limit::SharedIpRateLimiter,
    send_pipe::SendPipeConsumer,
    transmit_slot::PeerTransmitSlot,
};
use omq_proto::proto::{
    Connection,
    transform::{MessageDecoder, MessageEncoder},
};
use std::{io, net::IpAddr, sync::Arc, time::Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

impl<T> ConnectionDriver<T>
where
    T: DriverStream,
{
    /// Create a standalone driver with default configuration.
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

    /// Create a standalone driver with explicit limits and receive configuration.
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
        inbox: impl Into<crate::engine::control_inbox::Receiver>,
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
        inbox: crate::engine::control_inbox::Receiver,
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
            recv_parts: None,
            compression_pool: None,
            offload_threshold: 0,
            transmit_slot: None,
            send_pipe_rx: None,
            arena_threshold: omq_proto::frame_buffer::ARENA_THRESHOLD,
            arena_cap: omq_proto::frame_buffer::ARENA_INITIAL_CAP,
            receive_profile: ReceiveProfile::Throughput,
            latency_polling: false,
            decoded_payload_pool: None,
            recv_ip_rate_limiter: None,
            socket_close_state: None,
            #[cfg(target_os = "linux")]
            shm: None,
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

    #[cfg(target_os = "linux")]
    pub(crate) fn with_shm(mut self, state: Option<crate::transport::shm::ShmState>) -> Self {
        self.shm = state;
        self
    }

    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    pub(crate) fn uses_shm(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            self.shm.is_some()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    pub(crate) fn with_recv_parts(mut self, count: usize) -> Self {
        self.recv_parts = Some(count);
        self
    }

    /// Install a per-peer send pipe. The public socket handle pushes raw
    /// messages into the sender; this driver drains and encodes locally.
    #[must_use]
    pub(crate) fn with_send_pipe(mut self, rx: SendPipeConsumer) -> Self {
        self.send_pipe_rx = Some(rx);
        self
    }

    pub(crate) fn with_latency_polling(mut self, enabled: bool) -> Self {
        self.latency_polling = enabled;
        self
    }

    /// Install the fallback data-plane inbox. Kept separate from control so a
    /// full or stalled outbound path cannot hide lifecycle commands.
    #[must_use]
    pub fn with_data_inbox(mut self, rx: mpsc::Receiver<PeerDriverData>) -> Self {
        self.data_inbox = Some(crate::engine::data_inbox::Receiver::Legacy(rx));
        self
    }

    pub(crate) fn with_actor_data_inbox(mut self, rx: crate::engine::data_inbox::Receiver) -> Self {
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
}
