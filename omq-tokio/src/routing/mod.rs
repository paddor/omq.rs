//! Routing strategies: per-socket-type send and recv policy.
//!
//! Each socket type pairs a [`SendStrategy`] (what happens when the user
//! calls `send`?) with a [`RecvStrategy`] (where do incoming messages go?).
//!
//! | Type        | Send                                  | Recv                  |
//! |-------------|---------------------------------------|-----------------------|
//! | PUSH/PAIR   | `RoundRobin` (work-stealing)            | `FairQueue`             |
//! | PULL        | None                                  | `FairQueue`             |
//! | DEALER      | `RoundRobin`                            | `FairQueue`             |
//! | PUB         | `FanOut` (subscription-filtered)        | None                  |
//! | SUB/XSUB    | None (sends SUBSCRIBE/CANCEL only)    | `FairQueue`             |
//! | ROUTER      | Identity (peer lookup by first frame) | `IdentityRecv` (prefix) |
//!
//! Conflate mode applies to `FanOut` only (PUB/XPUB/RADIO).
//! REP envelope save/restore lives at the socket-type wiring level.

pub(crate) mod exclusive;
pub(crate) mod fair_queue;
pub(crate) mod fan_out;
pub(crate) mod identity;
pub(crate) mod latency;
pub(crate) mod peer_outbound;
pub(crate) mod round_robin;
// subscription matcher lives in omq-proto now.
pub(crate) use omq_proto::subscription;

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use smallvec::SmallVec;

use crate::engine::{PeerDriverHandle, SendPipeConsumer};
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_proto::routing::{FanOutKind, RecvCategory, SendCategory, recv_category, send_category};
use tokio::sync::oneshot;

/// Max messages one outbound drain batch encodes before flushing.
pub(crate) const OUTBOUND_BATCH_MAX_MSGS: usize = 512;

pub(crate) use exclusive::{ExclusiveSend, Submitter as ExclusiveSubmitter};
pub(crate) use fair_queue::FairQueueRecv;
pub(crate) use fan_out::{FanOutMode, FanOutSend, Submitter as FanOutSubmitter};
pub(crate) use identity::{IdentityRecv, IdentitySend, Submitter as IdentitySubmitter};
pub(crate) use latency::{LatencySend, Submitter as LatencySubmitter};
pub(crate) use round_robin::{RoundRobinSend, Submitter as RoundRobinSubmitter};

pub(crate) type RepEnvelope = SmallVec<[Bytes; 2]>;

pub(crate) fn split_rep_request(msg: &Message) -> Option<(RepEnvelope, Message)> {
    let mut envelope = RepEnvelope::new();
    let mut body = Vec::new();
    let mut seen_delimiter = false;

    for part in msg {
        if seen_delimiter {
            body.push(part);
        } else if part.is_empty() {
            seen_delimiter = true;
        } else {
            envelope.push(part);
        }
    }

    seen_delimiter.then(|| (envelope, Message::multipart(body)))
}

pub(crate) fn rep_reply_with_envelope(envelope: &RepEnvelope, body: &Message) -> Message {
    let mut parts = Vec::with_capacity(envelope.len() + 1 + body.len());
    parts.extend(envelope.iter().cloned());
    parts.push(Bytes::new());
    parts.extend(body.iter());
    Message::multipart(parts)
}

/// Send-side policy.
#[derive(Debug)]
pub(crate) enum SendStrategy {
    None,
    RoundRobin(RoundRobinSend),
    Latency(LatencySend),
    Exclusive(ExclusiveSend),
    FanOut(FanOutSend),
    Identity(IdentitySend),
}

#[derive(Debug, Clone)]
pub(crate) enum SendSubmitter {
    None,
    RoundRobin(RoundRobinSubmitter),
    Latency(LatencySubmitter),
    Exclusive(ExclusiveSubmitter),
    FanOut(FanOutSubmitter),
    Identity(IdentitySubmitter),
}

impl SendSubmitter {
    pub(crate) fn shutdown(&self) {
        match self {
            Self::None => {}
            Self::RoundRobin(s) => s.shutdown(),
            Self::Latency(s) => s.shutdown(),
            Self::Exclusive(s) => s.shutdown(),
            Self::FanOut(s) => s.shutdown(),
            Self::Identity(s) => s.shutdown(),
        }
    }

    pub(crate) async fn send(&self, msg: Message) -> Result<()> {
        match self {
            Self::None => Err(Error::Protocol("socket type does not support send".into())),
            Self::RoundRobin(s) => s.send(msg).await,
            Self::Latency(s) => s.send(msg).await,
            Self::Exclusive(s) => s.send(msg).await,
            Self::FanOut(s) => s.send(msg).await,
            Self::Identity(s) => s.send(msg).await,
        }
    }

    pub(crate) async fn send_rep_to_peer(
        &self,
        peer_id: u64,
        envelope: &RepEnvelope,
        msg: Message,
    ) -> Result<()> {
        match self {
            Self::Identity(s) => s.send_rep(peer_id, envelope, msg).await,
            _ => Err(Error::Protocol("REP latency route unavailable".into())),
        }
    }

    pub(crate) fn send_rep_try_to_peer(
        &self,
        peer_id: u64,
        envelope: &RepEnvelope,
        msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        match self {
            Self::Identity(s) => s.try_send_rep(peer_id, envelope, msg),
            _ => Err(omq_proto::error::TrySendError::Error(Error::Protocol(
                "REP latency route unavailable".into(),
            ))),
        }
    }

    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        match self {
            Self::None => Err(omq_proto::error::TrySendError::Error(Error::Protocol(
                "socket type does not support send".into(),
            ))),
            Self::RoundRobin(s) => s.try_send(msg),
            Self::Latency(s) => s.try_send(msg),
            Self::Exclusive(s) => s.try_send(msg),
            Self::FanOut(s) => s.try_send(msg),
            Self::Identity(s) => s.try_send(msg),
        }
    }

    pub(crate) fn try_send_many(
        &self,
        messages: &mut VecDeque<Message>,
        max: usize,
    ) -> core::result::Result<usize, omq_proto::error::TrySendError> {
        if let Self::RoundRobin(s) = self {
            s.try_send_many(messages, max)
        } else {
            let mut sent = 0usize;
            while sent < max {
                let Some(msg) = messages.pop_front() else {
                    break;
                };
                match self.try_send(msg) {
                    Ok(()) => sent += 1,
                    Err(omq_proto::error::TrySendError::Full(returned)) => {
                        messages.push_front(returned);
                        if sent > 0 {
                            return Ok(sent);
                        }
                        let msg = messages.pop_front().expect("returned message present");
                        return Err(omq_proto::error::TrySendError::Full(msg));
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(sent)
        }
    }

    pub(crate) async fn wait_send_progress(&self, msg: &Message) {
        match self {
            Self::None | Self::FanOut(_) => tokio::task::yield_now().await,
            Self::RoundRobin(s) => s.wait_send_progress().await,
            Self::Latency(s) => s.wait_send_progress().await,
            Self::Exclusive(s) => s.wait_send_progress().await,
            Self::Identity(s) => s.wait_send_progress(msg).await,
        }
    }
}

impl SendStrategy {
    pub(crate) fn for_socket_type(
        t: SocketType,
        options: &Options,
        io_pool: &crate::context::IoPoolHandle,
    ) -> Self {
        match send_category(t) {
            SendCategory::None => Self::None,
            SendCategory::FanOut(FanOutKind::SubscriptionPrefix) => Self::FanOut(FanOutSend::new(
                t,
                options,
                FanOutMode::SubscriptionPrefix,
                io_pool,
            )),
            SendCategory::FanOut(FanOutKind::Group) => {
                Self::FanOut(FanOutSend::new(t, options, FanOutMode::Group, io_pool))
            }
            SendCategory::IdentityRouted => Self::Identity(IdentitySend::new(t, options)),
            SendCategory::RoundRobin if uses_latency_round_robin(t, options) => {
                Self::Latency(LatencySend::new(options))
            }
            SendCategory::Exclusive if uses_latency_exclusive(t, options) => {
                Self::Latency(LatencySend::new(options))
            }
            SendCategory::RoundRobin
                if t == SocketType::Rep
                    && !options.mechanism.has_frame_transform()
                    && options.workload_profile != Some(omq_proto::WorkloadProfile::Throughput) =>
            {
                Self::Identity(IdentitySend::new(t, options))
            }
            SendCategory::RoundRobin => Self::RoundRobin(RoundRobinSend::new(options)),
            SendCategory::Exclusive => Self::Exclusive(ExclusiveSend::new()),
        }
    }

    pub(crate) fn submitter(&self) -> SendSubmitter {
        match self {
            Self::None => SendSubmitter::None,
            Self::RoundRobin(s) => SendSubmitter::RoundRobin(s.submitter()),
            Self::Latency(s) => SendSubmitter::Latency(s.submitter()),
            Self::Exclusive(s) => SendSubmitter::Exclusive(s.submitter()),
            Self::FanOut(s) => SendSubmitter::FanOut(s.submitter()),
            Self::Identity(s) => SendSubmitter::Identity(s.submitter()),
        }
    }

    pub(crate) fn connection_added(
        &mut self,
        peer_id: u64,
        route_id: u64,
        handle: PeerDriverHandle,
        peer_identity: Bytes,
        is_inproc: bool,
        io_thread: usize,
    ) {
        match self {
            Self::None => {}
            Self::RoundRobin(s) => s.connection_added(route_id, &handle, is_inproc),
            Self::Latency(s) => s.connection_added(route_id, &handle),
            Self::Exclusive(s) => s.connection_added(peer_id, handle),
            Self::FanOut(s) => s.connection_added(peer_id, handle, io_thread),
            Self::Identity(s) => s.connection_added(peer_id, handle, peer_identity, is_inproc),
        }
    }

    /// FanOut-only: register a peer that matches every group / every
    /// subscription. UDP RADIO uses this since DISH never sends JOIN
    /// over the wire. No-op for non-FanOut strategies.
    pub(crate) fn connection_added_any_groups(
        &mut self,
        peer_id: u64,
        handle: PeerDriverHandle,
        io_thread: usize,
    ) {
        if let Self::FanOut(s) = self {
            s.connection_added_any_groups(peer_id, handle, io_thread);
        }
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64, route_id: u64) {
        match self {
            Self::None => {}
            Self::RoundRobin(s) => s.connection_removed(route_id),
            Self::Latency(s) => s.connection_removed(route_id),
            Self::Exclusive(s) => s.connection_removed(peer_id),
            Self::FanOut(s) => s.connection_removed(peer_id),
            Self::Identity(s) => s.connection_removed(peer_id),
        }
    }

    pub(crate) fn connect_pipe_removed(&mut self, route_id: u64) {
        match self {
            Self::RoundRobin(s) => s.connection_removed(route_id),
            Self::Latency(s) => s.connection_removed(route_id),
            Self::None | Self::Exclusive(_) | Self::FanOut(_) | Self::Identity(_) => {}
        }
    }

    pub(crate) fn peer_for_identity(&self, identity: &Bytes) -> Option<u64> {
        match self {
            Self::Identity(s) => s.peer_for_identity(identity),
            _ => None,
        }
    }

    /// Record a SUBSCRIBE from a peer. No-op except for `FanOut`.
    pub(crate) fn peer_subscribe(
        &self,
        peer_id: u64,
        prefix: Bytes,
    ) -> Option<oneshot::Receiver<()>> {
        if let Self::FanOut(s) = self {
            return s.peer_subscribe(peer_id, prefix);
        }
        None
    }

    /// Record a CANCEL from a peer. No-op except for `FanOut`.
    pub(crate) fn peer_cancel(&self, peer_id: u64, prefix: &[u8]) {
        if let Self::FanOut(s) = self {
            s.peer_cancel(peer_id, prefix);
        }
    }

    /// Record a JOIN from a peer (RADIO/DISH).
    pub(crate) fn peer_join(&self, peer_id: u64, group: &[u8]) {
        if let Self::FanOut(s) = self {
            s.peer_join(peer_id, group);
        }
    }

    /// Record a LEAVE from a peer (RADIO/DISH).
    pub(crate) fn peer_leave(&self, peer_id: u64, group: &[u8]) {
        if let Self::FanOut(s) = self {
            s.peer_leave(peer_id, group);
        }
    }

    /// Allocate a connect-side pre-ready pipe. The producer is immediately
    /// eligible for routing; the dialer keeps the consumer until a connection
    /// completes the handshake.
    pub(crate) fn make_connect_pipe(&mut self, route_id: u64) -> Option<SendPipeConsumer> {
        match self {
            Self::RoundRobin(s) => Some(s.make_connect_pipe(route_id)),
            Self::Latency(s) => Some(s.make_connect_pipe(route_id)),
            _ => None,
        }
    }

    pub(crate) fn needs_peer_send_pipe(&self) -> bool {
        match self {
            Self::RoundRobin(_) | Self::Exclusive(_) => true,
            Self::Identity(s) => s.needs_peer_send_pipe(),
            Self::None | Self::Latency(_) | Self::FanOut(_) => false,
        }
    }

    pub(crate) fn needs_transmit_slot(&self) -> bool {
        match self {
            Self::Latency(_) | Self::FanOut(_) => true,
            Self::Identity(s) => s.needs_transmit_slot(),
            Self::None | Self::RoundRobin(_) | Self::Exclusive(_) => false,
        }
    }

    pub(crate) fn shutdown(&self) {
        match self {
            Self::None => {}
            Self::RoundRobin(s) => s.shutdown(),
            Self::Latency(s) => s.shutdown(),
            Self::Exclusive(s) => s.shutdown(),
            Self::FanOut(s) => s.shutdown(),
            Self::Identity(s) => s.shutdown(),
        }
    }

    pub(crate) fn is_drained(&self) -> bool {
        match self {
            Self::None => true,
            Self::RoundRobin(s) => s.is_drained(),
            Self::Latency(s) => s.is_drained(),
            Self::Exclusive(s) => s.is_drained(),
            Self::FanOut(s) => s.is_drained(),
            Self::Identity(s) => s.is_drained(),
        }
    }
}

fn uses_latency_round_robin(t: SocketType, options: &Options) -> bool {
    if options.mechanism.has_frame_transform() {
        return false;
    }
    match t {
        // REQ keeps its historical latency default. Other round-robin
        // socket types opt in explicitly so existing throughput-oriented
        // applications retain their queues.
        SocketType::Req => options.workload_profile != Some(omq_proto::WorkloadProfile::Throughput),
        SocketType::Dealer | SocketType::Client => {
            options.workload_profile == Some(omq_proto::WorkloadProfile::Latency)
        }
        _ => false,
    }
}

fn uses_latency_exclusive(t: SocketType, options: &Options) -> bool {
    !options.mechanism.has_frame_transform()
        && matches!(t, SocketType::Pair)
        && options.workload_profile == Some(omq_proto::WorkloadProfile::Latency)
}

/// Recv-side policy.
#[derive(Debug)]
pub(crate) enum RecvStrategy {
    None,
    FairQueue(FairQueueRecv),
    Identity(IdentityRecv),
}

impl RecvStrategy {
    pub(crate) fn for_socket_type(
        t: SocketType,
        recv_tx: Arc<crate::socket::recv::SharedRecvPipe>,
    ) -> Self {
        match recv_category(t) {
            RecvCategory::None => Self::None,
            RecvCategory::Identity => Self::Identity(IdentityRecv::new(recv_tx)),
            RecvCategory::FairQueue => Self::FairQueue(FairQueueRecv::new(recv_tx)),
        }
    }

    pub(crate) fn connection_added(&mut self, peer_id: u64, peer_identity: Bytes) {
        match self {
            Self::None => {}
            Self::FairQueue(fq) => fq.connection_added(peer_id),
            Self::Identity(ir) => ir.connection_added(peer_id, peer_identity),
        }
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        match self {
            Self::None => {}
            Self::FairQueue(fq) => fq.connection_removed(peer_id),
            Self::Identity(ir) => ir.connection_removed(peer_id),
        }
    }

    pub(crate) async fn deliver(&self, peer_id: u64, msg: Message) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::FairQueue(fq) => fq.deliver(peer_id, msg).await,
            Self::Identity(ir) => ir.deliver(peer_id, msg).await,
        }
    }

    /// Prepare a recv message for a per-socket-type post-recv transform.
    /// For `Identity`, prepends the sender's identity so the REP handler
    /// sees the full envelope; for `FairQueue`, returns the message
    /// unchanged. Used when the `type_state` needs to post-process (REQ,
    /// REP) rather than hitting the recv channel directly.
    pub(crate) fn wrap_for_transform(&self, peer_id: u64, msg: Message) -> Option<Message> {
        match self {
            Self::None => None,
            Self::FairQueue(_) => Some(msg),
            Self::Identity(ir) => Some(ir.wrap(peer_id, msg)),
        }
    }
}

/// Maximum peer count imposed by the socket type. Per RFC 31, PAIR is
/// strictly 1:1; all other types accept N peers.
pub(crate) fn max_peer_count(t: SocketType) -> Option<usize> {
    match t {
        SocketType::Pair | SocketType::Channel => Some(1),
        _ => None,
    }
}

/// Whether this socket type exposes subscribe / unsubscribe.
pub(crate) fn supports_subscribe(t: SocketType) -> bool {
    matches!(t, SocketType::Sub | SocketType::XSub)
}

/// Whether this socket type exposes join / leave (groups).
pub(crate) fn supports_groups(t: SocketType) -> bool {
    matches!(t, SocketType::Dish)
}

pub(crate) use omq_proto::routing::supports_conflate;

#[cfg(test)]
mod tests {
    use super::*;
    use omq_proto::WorkloadProfile;

    #[test]
    fn dealer_uses_latency_route_only_when_explicitly_requested() {
        let io_pool = crate::context::IoPoolHandle::none();
        let latency = Options::default().workload_profile(WorkloadProfile::Latency);
        assert!(matches!(
            SendStrategy::for_socket_type(SocketType::Dealer, &latency, &io_pool),
            SendStrategy::Latency(_)
        ));

        assert!(matches!(
            SendStrategy::for_socket_type(SocketType::Dealer, &Options::default(), &io_pool),
            SendStrategy::RoundRobin(_)
        ));
    }

    #[test]
    fn latency_profile_extends_client_and_pair() {
        let io_pool = crate::context::IoPoolHandle::none();
        let latency = Options::default().workload_profile(WorkloadProfile::Latency);

        assert!(matches!(
            SendStrategy::for_socket_type(SocketType::Client, &latency, &io_pool),
            SendStrategy::Latency(_)
        ));
        assert!(matches!(
            SendStrategy::for_socket_type(SocketType::Client, &Options::default(), &io_pool),
            SendStrategy::RoundRobin(_)
        ));
        assert!(matches!(
            SendStrategy::for_socket_type(SocketType::Pair, &latency, &io_pool),
            SendStrategy::Latency(_)
        ));
        assert!(matches!(
            SendStrategy::for_socket_type(SocketType::Pair, &Options::default(), &io_pool),
            SendStrategy::Exclusive(_)
        ));
    }

    #[test]
    fn identity_types_use_transmit_slots_for_latency_profile() {
        let io_pool = crate::context::IoPoolHandle::none();
        let latency = Options::default().workload_profile(WorkloadProfile::Latency);

        for socket_type in [SocketType::Router, SocketType::Server] {
            let latency_strategy = SendStrategy::for_socket_type(socket_type, &latency, &io_pool);
            assert!(matches!(latency_strategy, SendStrategy::Identity(_)));
            assert!(latency_strategy.needs_transmit_slot());

            let throughput_strategy =
                SendStrategy::for_socket_type(socket_type, &Options::default(), &io_pool);
            assert!(matches!(throughput_strategy, SendStrategy::Identity(_)));
            assert!(!throughput_strategy.needs_transmit_slot());
        }
    }
}
