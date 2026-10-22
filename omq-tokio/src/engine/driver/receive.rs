//! Decoded delivery, receive budgets, and rate limits.

use super::{BATCH_TIME_CHECK_MSGS, DriverStep, RecvSink};
use crate::engine::{
    actor_output::PeerOutput,
    peer_completion::CompletionProgress,
    rate_limit::{SharedIpRateLimiter, TokenBucket},
};
use omq_proto::{
    Message, WorkloadProfile,
    error::{Error, Result, TrySendError},
    flow::DrainBudget,
    proto::{Connection, transform::MessageDecoder},
};
use std::{
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};

pub(super) const RECV_SMALL_MSG: usize = 1024;
pub(super) const RECV_MEDIUM_MSG: usize = 4096;
pub(super) const RECV_SMALL_BYTES: usize = 64 * 1024;
pub(super) const RECV_MEDIUM_BYTES: usize = 1024 * 1024;
pub(super) const RECV_LARGE_BYTES: usize = 1024 * 1024;
pub(super) const RECV_MEDIUM_TIME: Duration = Duration::from_micros(200);
pub(super) const RECV_LARGE_TIME: Duration = Duration::from_micros(200);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReceiveProfile {
    Latency,
    LatencyReq,
    Throughput,
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

    pub(super) fn budget(self, msg_bytes: usize) -> DrainBudget {
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

    pub(super) fn time(self, msg_bytes: usize) -> Option<Duration> {
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

pub(super) struct ReceiveRateLimiters<'a> {
    pub(super) connection: &'a mut Option<TokenBucket>,
    pub(super) ip: Option<&'a (Arc<SharedIpRateLimiter>, IpAddr)>,
}

pub(super) struct MessageDelivery<'a> {
    pub(super) sink: &'a mut Option<RecvSink>,
    pub(super) peer_out: &'a mut PeerOutput,
    pub(super) peer_id: u64,
    pub(super) completion: &'a mut CompletionProgress,
    pub(super) pending: &'a mut Option<Message>,
    pub(super) reserved_admission: bool,
    pub(super) expected_parts: Option<usize>,
}

impl MessageDelivery<'_> {
    pub(super) fn blocked(&self) -> bool {
        self.pending.is_some() || self.sink.as_ref().is_some_and(RecvSink::peer_blocked)
    }

    pub(super) fn retry_pending(&mut self) -> bool {
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
    pub(super) fn send(&mut self, message: Message, defer: bool, pending_flush: &mut bool) -> bool {
        if self
            .expected_parts
            .is_some_and(|parts| message.len() != parts)
        {
            return true;
        }
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
                Err(crate::engine::SendPipeError::Full(message)) => {
                    Err(TrySendError::Full(message))
                }
                Err(crate::engine::SendPipeError::Closed(_)) => Err(TrySendError::Closed),
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

    pub(super) fn flush(&mut self, pending: &mut bool) {
        if let Some(sink) = self.sink {
            sink.flush_deferred(pending);
        }
    }
}

impl ReceiveRateLimiters<'_> {
    #[cfg(target_os = "linux")]
    fn is_unlimited(&self) -> bool {
        self.connection.is_none() && self.ip.is_none()
    }

    /// One clock read per batch: every message in it is charged at `now`.
    #[inline]
    fn exceeded(&mut self, now: Instant) -> bool {
        self.connection
            .as_mut()
            .is_some_and(|limiter| !limiter.allow(now))
            || self
                .ip
                .is_some_and(|(limiter, ip)| !limiter.allow(*ip, now))
    }
}

/// Where decoded messages come from: the ZMTP codec, an optional message
/// transform with its output pool, and SHM payload state.
pub(super) struct DecodeInput<'a> {
    pub(super) connection: &'a mut Connection,
    pub(super) decoder: &'a mut Option<MessageDecoder>,
    pub(super) payload_pool: Option<&'a omq_proto::PayloadPool>,
    #[cfg(target_os = "linux")]
    pub(super) shm: Option<&'a crate::transport::shm::ShmState>,
}

impl DecodeInput<'_> {
    /// SHM delivery moves message headers after carrier input is budgeted,
    /// so untransformed SHM receives are charged one header each.
    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    fn charges_headers(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            self.shm.is_some() && self.decoder.is_none()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }
}

/// Budget and deferred-flush state of one `drain_decoded_messages` call.
struct ReceiveBatch {
    start: Instant,
    profile: ReceiveProfile,
    /// Sized by the first message of the batch.
    budget: Option<DrainBudget>,
    time_limit: Option<Duration>,
    charges_headers: bool,
    defer_flush: bool,
    pending_flush: bool,
}

impl ReceiveBatch {
    fn new(
        input: &DecodeInput<'_>,
        profile: ReceiveProfile,
        delivery: &MessageDelivery<'_>,
    ) -> Self {
        let defer_codec = match input.decoder {
            None => true,
            #[cfg(feature = "lz4")]
            Some(MessageDecoder::Lz4(_)) => true,
            #[cfg(feature = "zstd")]
            Some(MessageDecoder::Zstd(_)) => true,
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            Some(_) => false,
        };

        // Throughput yring sinks publish once per batch instead of per message.
        let defer_flush = defer_codec
            && matches!(profile, ReceiveProfile::Throughput)
            && delivery.sink.as_ref().is_some_and(RecvSink::is_yring);

        Self {
            start: Instant::now(),
            profile,
            budget: None,
            time_limit: None,
            charges_headers: input.charges_headers(),
            defer_flush,
            pending_flush: false,
        }
    }

    #[inline]
    fn cost(&self, message: &Message) -> usize {
        if self.charges_headers {
            std::mem::size_of::<Message>()
        } else {
            message.byte_len()
        }
    }

    /// Deliver one message and charge it. `Some` ends the batch.
    #[inline]
    fn deliver(
        &mut self,
        message: Message,
        msg_bytes: usize,
        delivery: &mut MessageDelivery<'_>,
    ) -> Option<DriverStep> {
        let profile = self.profile;
        let budget = self.budget.get_or_insert_with(|| {
            self.time_limit = profile.time(msg_bytes);
            profile.budget(msg_bytes)
        });
        if !delivery.send(message, self.defer_flush, &mut self.pending_flush) {
            return Some(DriverStep::Close);
        }

        let budget_remains = budget.account(msg_bytes);
        if delivery.blocked() {
            return Some(DriverStep::Continue);
        }

        // Read the clock only every BATCH_TIME_CHECK_MSGS messages.
        let time_check = budget.msgs().is_multiple_of(BATCH_TIME_CHECK_MSGS);
        if !budget_remains
            || (time_check
                && self
                    .time_limit
                    .is_some_and(|limit| self.start.elapsed() >= limit))
        {
            return Some(DriverStep::Yield);
        }

        None
    }
}

pub(super) fn drain_decoded_messages(
    input: DecodeInput<'_>,
    receive_profile: ReceiveProfile,
    mut delivery: MessageDelivery<'_>,
    mut rate_limiters: ReceiveRateLimiters<'_>,
) -> Result<DriverStep> {
    if !delivery.retry_pending() {
        return Ok(DriverStep::Close);
    }
    if delivery.blocked() {
        return Ok(DriverStep::Continue);
    }

    // Untransformed, unlimited SHM input hands whole batches to a batch sink.
    #[cfg(target_os = "linux")]
    if input.decoder.is_none()
        && rate_limiters.is_unlimited()
        && let Some(shm) = input.shm
        && let Some(sink) = delivery.sink.as_mut().and_then(RecvSink::batch_sink)
    {
        if !sink.enable_batches() {
            return Ok(DriverStep::Close);
        }
        return drain_shm_batches(input.connection, shm, receive_profile, delivery);
    }

    let mut batch = ReceiveBatch::new(&input, receive_profile, &delivery);
    let DecodeInput {
        connection,
        decoder,
        payload_pool,
        #[cfg(target_os = "linux")]
        shm,
    } = input;
    #[cfg(target_os = "linux")]
    let mut shm_guard = shm.map(|state| state.lock().expect("SHM connection"));

    loop {
        let m = match poll_decoded_message(
            connection,
            decoder,
            payload_pool,
            #[cfg(target_os = "linux")]
            shm_guard.as_deref_mut(),
        ) {
            Ok(DecodedReceive::Message(message)) => message,
            Ok(DecodedReceive::Setup) => continue,
            Ok(DecodedReceive::Empty) => break,
            Err(error) => {
                delivery.flush(&mut batch.pending_flush);
                return Err(error);
            }
        };

        if rate_limiters.exceeded(batch.start) {
            delivery.flush(&mut batch.pending_flush);
            return Err(Error::ReceiveRateLimitExceeded);
        }

        let msg_bytes = batch.cost(&m);
        if let Some(step) = batch.deliver(m, msg_bytes, &mut delivery) {
            delivery.flush(&mut batch.pending_flush);
            return Ok(step);
        }
    }

    delivery.flush(&mut batch.pending_flush);
    Ok(DriverStep::Continue)
}

enum DecodedReceive {
    Message(Message),
    Setup,
    Empty,
}

fn poll_decoded_message(
    connection: &mut Connection,
    decoder: &mut Option<MessageDecoder>,
    pool: Option<&omq_proto::PayloadPool>,
    #[cfg(target_os = "linux")] shm: Option<&mut omq_shm::Connection>,
) -> Result<DecodedReceive> {
    if let Some(message) = decoder.as_mut().and_then(MessageDecoder::poll_message) {
        return Ok(DecodedReceive::Message(message));
    }
    let Some(wire) = poll_received_locked(
        connection,
        #[cfg(target_os = "linux")]
        shm,
    )?
    else {
        return Ok(DecodedReceive::Empty);
    };
    let message = match decoder {
        Some(decoder) => decoder.decode_with_payload_pool(wire, pool)?,
        None => Some(wire),
    };
    Ok(message.map_or(DecodedReceive::Setup, DecodedReceive::Message))
}

#[cfg(target_os = "linux")]
pub(super) fn drain_shm_batches(
    connection: &mut Connection,
    shm: &crate::transport::shm::ShmState,
    profile: ReceiveProfile,
    mut delivery: MessageDelivery<'_>,
) -> Result<DriverStep> {
    let started = Instant::now();
    let header_bytes = std::mem::size_of::<Message>();
    let mut budget = profile.budget(header_bytes);
    let deadline = profile.time(header_bytes);
    let mut state = shm.lock().expect("SHM connection");
    let mut pending_flush = false;
    let defer = matches!(profile, ReceiveProfile::Throughput);
    loop {
        let received = if let Some(message) = state.poll_received() {
            omq_shm::Received::Message(message)
        } else if let Some(record) = connection.poll_message() {
            state.decode_delivery(record)?
        } else {
            delivery.flush(&mut pending_flush);
            return Ok(DriverStep::Continue);
        };
        let count = match received {
            omq_shm::Received::Message(message) => {
                if !delivery.send(message, defer, &mut pending_flush) {
                    delivery.flush(&mut pending_flush);
                    return Ok(DriverStep::Close);
                }
                1
            }
            omq_shm::Received::Batch(batch) => {
                let count = batch.len();
                if delivery
                    .expected_parts
                    .is_none_or(|parts| parts == batch.parts())
                    && !delivery
                        .sink
                        .as_mut()
                        .expect("batch sink")
                        .push_batch(batch)
                {
                    delivery.flush(&mut pending_flush);
                    return Ok(DriverStep::Close);
                }
                count
            }
        };
        let remaining = budget.account_many(count, count * header_bytes);
        if delivery.blocked() {
            delivery.flush(&mut pending_flush);
            return Ok(DriverStep::Continue);
        }
        if !remaining || deadline.is_some_and(|time| started.elapsed() >= time) {
            delivery.flush(&mut pending_flush);
            return Ok(DriverStep::Yield);
        }
    }
}

#[cfg_attr(not(target_os = "linux"), expect(clippy::unnecessary_wraps))]
pub(super) fn poll_received_locked(
    connection: &mut Connection,
    #[cfg(target_os = "linux")] shm: Option<&mut omq_shm::Connection>,
) -> Result<Option<Message>> {
    #[cfg(target_os = "linux")]
    if let Some(state) = shm {
        if let Some(message) = state.poll_received() {
            return Ok(Some(message));
        }
        return connection
            .poll_message()
            .map(|message| state.decode_received(message))
            .transpose();
    }
    Ok(connection.poll_message())
}

pub(super) fn poll_received_message(
    connection: &mut Connection,
    #[cfg(target_os = "linux")] shm: Option<&crate::transport::shm::ShmState>,
) -> Result<Option<Message>> {
    #[cfg(target_os = "linux")]
    let mut guard = shm.map(|state| state.lock().expect("SHM connection"));
    poll_received_locked(
        connection,
        #[cfg(target_os = "linux")]
        guard.as_deref_mut(),
    )
}

/// Closed sockets no longer deliver receives, but must keep parsing control
/// traffic while accepted output drains. Bound even the discard work.
pub(super) fn discard_received_messages(
    connection: &mut Connection,
    #[cfg(target_os = "linux")] shm: Option<&crate::transport::shm::ShmState>,
) -> Result<bool> {
    let mut budget = DrainBudget::new(256, 1024 * 1024);
    while let Some(message) = poll_received_message(
        connection,
        #[cfg(target_os = "linux")]
        shm,
    )? {
        let bytes = message.byte_len();
        if !budget.account(bytes) {
            return Ok(true);
        }
    }
    Ok(false)
}
