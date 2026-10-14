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

pub(super) fn drain_decoded_messages(
    connection: &mut Connection,
    decoder: &mut Option<MessageDecoder>,
    receive_profile: ReceiveProfile,
    mut delivery: MessageDelivery<'_>,
    rate_limiters: ReceiveRateLimiters<'_>,
    decoded_payload_pool: Option<&omq_proto::PayloadPool>,
    #[cfg(target_os = "linux")] shm: Option<&crate::transport::shm::ShmState>,
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
    #[cfg(target_os = "linux")]
    if decoder.is_none()
        && rate_connection.is_none()
        && rate_ip.is_none()
        && let Some(shm) = shm
        && let Some(sink) = delivery.sink.as_mut().and_then(RecvSink::batch_sink)
    {
        if !sink.enable_batches() {
            return Ok(DriverStep::Close);
        }
        return drain_shm_batches(connection, shm, receive_profile, delivery);
    }
    let recv_batch_start = Instant::now();
    let mut recv_budget = None;
    let mut recv_batch_time = None;
    let defer_yring_flush = decoder.is_none()
        && matches!(receive_profile, ReceiveProfile::Throughput)
        && delivery.sink.as_ref().is_some_and(RecvSink::is_yring);
    let mut pending_yring_flush = false;
    #[cfg(target_os = "linux")]
    let mut shm_guard = shm.map(|state| state.lock().expect("SHM connection"));
    while let Some(m) = poll_received_locked(
        connection,
        #[cfg(target_os = "linux")]
        shm_guard.as_deref_mut(),
    )? {
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
        #[cfg(target_os = "linux")]
        let msg_bytes = if shm.is_some() && decoder.is_none() {
            // SHM delivery moves message headers after carrier input is budgeted.
            std::mem::size_of::<Message>()
        } else {
            msg_bytes
        };
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
        let time_check = budget.msgs().is_multiple_of(BATCH_TIME_CHECK_MSGS);
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

#[cfg_attr(not(target_os = "linux"), expect(clippy::unnecessary_wraps))]
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
