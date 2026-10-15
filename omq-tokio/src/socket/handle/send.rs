use super::{Socket, TrySendError};
use crate::routing::RepEnvelope;
use crate::socket::actor::SocketCommand;
use bytes::Bytes;
use futures::channel::oneshot;
use omq_proto::{
    error::{Error, Result},
    message::Message,
    proto::SocketType,
};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::mpsc;

impl Socket {
    /// Send a body to a ROUTER or PEER identity without an identity part.
    pub async fn send_to(&self, identity: impl AsRef<[u8]>, body: Message) -> Result<()> {
        self.check_identity_routing()?;
        if body.is_empty() {
            return Err(Error::Protocol("send_to requires a message body".into()));
        }
        crate::socket::send_budget::cooperate().await;
        self.send_submitter.send_to(identity.as_ref(), body).await
    }

    /// Nonblocking identity send. `Full` returns the unchanged body.
    pub fn try_send_to(
        &self,
        identity: impl AsRef<[u8]>,
        body: Message,
    ) -> core::result::Result<(), TrySendError> {
        self.check_identity_routing().map_err(TrySendError::Error)?;
        if body.is_empty() {
            return Err(TrySendError::Error(Error::Protocol(
                "try_send_to requires a message body".into(),
            )));
        }
        self.send_submitter.try_send_to(identity.as_ref(), body)
    }

    fn check_identity_routing(&self) -> Result<()> {
        if matches!(
            self.inner.socket_type,
            SocketType::Router | SocketType::Peer
        ) {
            Ok(())
        } else {
            Err(Error::Protocol(
                "identity routing requires a ROUTER or PEER socket".into(),
            ))
        }
    }

    /// Send one single-frame body to a RADIO group.
    /// Accepts owning `Message` bodies, including preallocated payloads.
    pub async fn send_group(
        &self,
        group: impl Into<Bytes>,
        body: impl Into<Message>,
    ) -> Result<()> {
        self.send(self.group_message(group.into(), body.into())?)
            .await
    }

    pub(crate) fn group_message(&self, group: Bytes, body: Message) -> Result<Message> {
        if self.inner.socket_type != SocketType::Radio {
            return Err(Error::Protocol(
                "send_group is only valid on RADIO sockets".into(),
            ));
        }
        if body.len() != 1 {
            return Err(Error::Protocol(
                "send_group requires a single-frame body".into(),
            ));
        }
        Ok(Message::with_prefix(group, body))
    }

    /// Send a message.
    ///
    /// This waits until the message is accepted into OMQ's outbound routing
    /// buffers. It does not wait for bytes to reach the peer or the kernel.
    ///
    /// Native round-robin sockets (`PUSH`, `DEALER`, `REQ`, `CLIENT`,
    /// `SCATTER`) with no ready bound peer mute like libzmq: this waits until
    /// a pipe exists and has space. Connected no-peer sends queue in the
    /// endpoint's pre-ready pipe up to `Options::send_hwm`.
    pub async fn send(&self, msg: Message) -> Result<()> {
        crate::socket::send_budget::cooperate().await;
        match self.inner.socket_type {
            SocketType::Req => {
                if !self.inner.req_awaiting_reply.claim() {
                    // Yield so the actor can process a potential peer
                    // disconnect that resets the flag, then retry once.
                    tokio::task::yield_now().await;
                    if !self.inner.req_awaiting_reply.claim() {
                        return Err(Error::Protocol(
                            "REQ socket must receive a reply before sending again".into(),
                        ));
                    }
                }
                // Canceling the send must allow the next send.
                let mut claim = ReqSendClaim {
                    awaiting_reply: &self.inner.req_awaiting_reply,
                    sent: false,
                };
                let msg = Message::with_prefix(Bytes::new(), msg);
                let result = self
                    .send_submitter
                    .send_request(msg, &self.inner.req_awaiting_reply)
                    .await;
                claim.sent = result.is_ok();
                result
            }
            SocketType::Rep => {
                let request = self.inner.rep_current.lock().expect("rep identity").take();
                let Some(request) = request else {
                    return Err(rep_send_without_request());
                };
                // Canceling the send must keep the request answerable.
                let mut claim = RepSendClaim {
                    current: &self.inner.rep_current,
                    request: Some(request),
                };
                let (peer_id, envelope) = claim.request.as_ref().expect("claimed request");
                let result = self
                    .inner
                    .send_submitter
                    .send_rep_to_peer(*peer_id, envelope, msg)
                    .await;
                claim.request = None;
                result
            }
            SocketType::Server => self.send_submitter.send_server(msg).await,
            SocketType::XSub => self.send_xsub_raw_command(&msg).await,
            _ => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)?;
                self.send_submitter.send(msg).await
            }
        }
    }

    /// Send messages through normal socket routing as a sink.
    ///
    /// Polls on the application's executor, with at most one pending message,
    /// and yields cooperatively under load. Pin it before using `SinkExt`.
    /// `flush` completes the pending send; closing flushes the adapter and
    /// leaves the socket open. Dropping it discards its pending unsent message.
    /// Tries [`Self::try_send`] before waiting via [`Self::send`]; REQ/REP
    /// sends use `send` directly. Normal backpressure and routing rules apply.
    ///
    /// ```no_run
    /// # async fn example(socket: &omq_tokio::Socket) -> omq_tokio::Result<()> {
    /// use futures::{SinkExt, pin_mut, stream};
    /// use omq_tokio::Message;
    /// let mut source = stream::iter([Ok(Message::single("hello"))]);
    /// let sink = socket.sink();
    /// pin_mut!(sink);
    /// sink.send_all(&mut source).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sink(&self) -> impl futures::Sink<Message, Error = Error> + Send + '_ {
        crate::socket::send_sink::MessageSink::new(self, |message| self.send(message))
    }

    /// Non-blocking send.
    ///
    /// Routes through the `SendSubmitter` directly (no actor hop), mirroring
    /// `send()` but synchronously. Returns `Full(msg)` when native outbound
    /// buffers are at HWM so the caller can retry or fall back to async
    /// `send()`.
    ///
    /// For native round-robin sockets, a connect-side pre-ready pipe counts as
    /// an outbound buffer. `try_send()` can therefore succeed before any peer
    /// is ready. Bound no-peer sockets return `Full`.
    #[inline]
    pub fn try_send(&self, msg: Message) -> core::result::Result<(), TrySendError> {
        match self.inner.socket_type {
            SocketType::Req => {
                if !self.inner.req_awaiting_reply.claim() {
                    return Err(TrySendError::Error(Error::Protocol(
                        "REQ socket must receive a reply before sending again".into(),
                    )));
                }
                let msg = Message::with_prefix(Bytes::new(), msg);
                let result = self
                    .send_submitter
                    .try_send_request(msg, &self.inner.req_awaiting_reply);
                if result.is_err() {
                    self.inner.req_awaiting_reply.clear();
                }
                match result {
                    Err(TrySendError::Full(mut returned)) => {
                        // Full returns the original application message so
                        // retrying never prepends a second REQ delimiter.
                        let _ = returned.pop_front();
                        Err(TrySendError::Full(returned))
                    }
                    other => other,
                }
            }
            SocketType::Rep => {
                let mut current = self.inner.rep_current.lock().expect("rep identity");
                let Some((peer_id, envelope)) = current.take() else {
                    return Err(TrySendError::Error(rep_send_without_request()));
                };
                let result = self
                    .inner
                    .send_submitter
                    .send_rep_try_to_peer(peer_id, &envelope, msg);
                if matches!(&result, Err(TrySendError::Full(_))) {
                    *current = Some((peer_id, envelope));
                }
                result
            }
            SocketType::Server => self.send_submitter.try_send_server(msg),
            SocketType::XSub => self.try_send_xsub_raw_command(msg),
            _ => {
                check_pre_send_frame_count(self.inner.socket_type, &msg)
                    .map_err(TrySendError::Error)?;
                self.send_submitter.try_send(msg)
            }
        }
    }

    /// Try to send up to `max` messages from `messages` without blocking.
    ///
    /// Successfully submitted messages are removed from the front of
    /// `messages`. If the socket is full before any message is submitted,
    /// the first unsent message is returned in [`TrySendError::Full`].
    pub fn try_send_many(
        &self,
        messages: &mut VecDeque<Message>,
        max: usize,
    ) -> core::result::Result<usize, TrySendError> {
        let max = max.min(messages.len());
        if max == 0 {
            return Ok(0);
        }
        match self.inner.socket_type {
            SocketType::Push | SocketType::Scatter => {
                let max = max.min(omq_proto::flow::DrainBudget::WORKER.remaining_msgs());
                let max = if self.inner.socket_type == SocketType::Scatter {
                    messages
                        .iter()
                        .take(max)
                        .position(|message| message.len() != 1)
                        .unwrap_or(max)
                } else {
                    max
                };
                if max == 0 {
                    // Report an invalid front message through the scalar check.
                    // A valid prefix is submitted first, subject to normal HWM.
                    return messages
                        .pop_front()
                        .map_or(Ok(0), |message| self.try_send(message).map(|()| 1));
                }
                self.inner.send_submitter.try_send_many(messages, max)
            }
            _ => {
                let mut sent = 0usize;
                while sent < max {
                    let Some(msg) = messages.pop_front() else {
                        break;
                    };
                    match self.try_send(msg) {
                        Ok(()) => sent += 1,
                        Err(TrySendError::Full(returned)) => {
                            messages.push_front(returned);
                            if sent > 0 {
                                return Ok(sent);
                            }
                            let msg = messages.pop_front().expect("returned message present");
                            return Err(TrySendError::Full(msg));
                        }
                        Err(error) => return Err(error),
                    }
                }
                Ok(sent)
            }
        }
    }

    /// Whether [`Self::try_send`] would accept a message now.
    ///
    /// Matches libzmq `ZMQ_POLLOUT`. `REQ` is writable once it has received
    /// its reply, `REP` while a request awaits its reply. Round-robin and
    /// exclusive types are writable while a peer queue has space. `ROUTER`
    /// is writable unless `router_mandatory` is set and every peer is full.
    /// `SERVER`, `PEER`, `STREAM`, and lossy fan-out never block. With
    /// `xpub_nodrop`, fan-out is writable while its distribution ring has
    /// space. Receive-only types are never writable. After close, sockets
    /// report writable so the next send reports the error.
    ///
    /// Another handle can take the space before this handle sends.
    pub fn send_ready(&self) -> bool {
        match self.inner.socket_type {
            SocketType::Req => {
                !self.inner.req_awaiting_reply.is_set() && self.send_submitter.send_ready()
            }
            SocketType::Rep => {
                let peer = self
                    .inner
                    .rep_current
                    .lock()
                    .expect("rep identity")
                    .as_ref()
                    .map(|(peer_id, _)| *peer_id);
                peer.is_some_and(|peer_id| self.send_submitter.rep_send_ready(peer_id))
            }
            SocketType::XSub => self.inner.cmd_tx.capacity() > 0 || self.inner.cmd_tx.is_closed(),
            _ => self.send_submitter.send_ready(),
        }
    }

    /// Wait until [`Self::send_ready`] is true.
    ///
    /// Completes when readiness was observed; another handle can take the
    /// space before this one sends. Never completes for receive-only types,
    /// or for a `REP` socket that had no request to answer when called.
    pub async fn wait_send_ready(&self) {
        match self.inner.socket_type {
            SocketType::Req => loop {
                self.inner.req_awaiting_reply.wait_cleared().await;
                self.send_submitter.wait_send_ready().await;
                // A send from another handle may have claimed the reply.
                if self.send_ready() {
                    return;
                }
            },
            SocketType::Rep => {
                let peer = self
                    .inner
                    .rep_current
                    .lock()
                    .expect("rep identity")
                    .as_ref()
                    .map(|(peer_id, _)| *peer_id);
                match peer {
                    Some(peer_id) => self.send_submitter.wait_rep_send_ready(peer_id).await,
                    None => std::future::pending().await,
                }
            }
            SocketType::XSub => drop(self.inner.cmd_tx.reserve().await),
            _ => self.send_submitter.wait_send_ready().await,
        }
    }

    #[doc(hidden)]
    pub async fn wait_send_progress_for(&self, msg: &Message) {
        if self.inner.socket_type == SocketType::XSub && xsub_raw_command(msg).is_ok() {
            let _ = self.inner.cmd_tx.reserve().await;
            return;
        }
        let routed_peer = match self.inner.socket_type {
            SocketType::Rep => self
                .inner
                .rep_current
                .lock()
                .expect("rep identity")
                .as_ref()
                .map(|(peer_id, _)| *peer_id),
            SocketType::Server => msg
                .routing_id()
                .and_then(|id| id.checked_sub(1))
                .map(u64::from),
            _ => None,
        };
        if let Some(peer_id) = routed_peer {
            self.send_submitter.wait_peer_send_progress(peer_id).await;
            return;
        }
        self.send_submitter.wait_send_progress(msg).await;
    }

    async fn send_xsub_raw_command(&self, msg: &Message) -> Result<()> {
        let (command, prefix) = xsub_raw_command(msg)?;
        match command {
            XSubRawCommand::Subscribe => self.subscribe(prefix).await,
            XSubRawCommand::Unsubscribe => self.unsubscribe(prefix).await,
        }
    }

    fn try_send_xsub_raw_command(&self, msg: Message) -> core::result::Result<(), TrySendError> {
        let (command, prefix) = xsub_raw_command(&msg).map_err(TrySendError::Error)?;
        let (ack, _rx) = oneshot::channel();
        let command = match command {
            XSubRawCommand::Subscribe => SocketCommand::Subscribe { prefix, ack },
            XSubRawCommand::Unsubscribe => SocketCommand::Unsubscribe { prefix, ack },
        };
        match self.inner.cmd_tx.try_send(command) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(TrySendError::Full(msg)),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(TrySendError::Closed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum XSubRawCommand {
    Subscribe,
    Unsubscribe,
}

fn xsub_raw_command(msg: &Message) -> Result<(XSubRawCommand, Bytes)> {
    if msg.len() != 1 {
        return Err(Error::Protocol(
            "XSUB raw command must be a single frame".into(),
        ));
    }
    let part = msg.part_bytes(0).unwrap_or_default();
    let Some((&tag, prefix)) = part.split_first() else {
        return Err(Error::Protocol("XSUB raw command cannot be empty".into()));
    };
    let command = match tag {
        0x01 => XSubRawCommand::Subscribe,
        0x00 => XSubRawCommand::Unsubscribe,
        _ => {
            return Err(Error::Protocol(
                "XSUB raw command must start with 0x01 or 0x00".into(),
            ));
        }
    };
    Ok((command, Bytes::copy_from_slice(prefix)))
}

/// REQ alternation and the connection selected for the outstanding request.
/// A pending connect route resolves to one connection when its handshake is
/// admitted. Replies retain that connection ID through binding receive queues.
#[derive(Debug, Default)]
pub(crate) struct ReqAwaitingReply {
    awaiting: AtomicBool,
    waiters: AtomicUsize,
    cleared: crate::engine::signal::StateSignal,
    routes: Mutex<ReqRoutes>,
}

#[derive(Debug, Default)]
struct ReqRoutes {
    connected: HashMap<u64, u64>,
    selected: Option<ReqReplySource>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReqReplySource {
    Pending(u64),
    Connected(u64),
}

impl ReqAwaitingReply {
    fn is_set(&self) -> bool {
        self.awaiting.load(Ordering::Acquire)
    }

    fn claim(&self) -> bool {
        let mut routes = self.routes.lock().expect("REQ routes");
        let claimed = self
            .awaiting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if claimed {
            routes.selected = None;
        }
        claimed
    }

    pub(crate) fn select_route(
        &self,
        send: impl FnOnce() -> core::result::Result<u64, TrySendError>,
    ) -> core::result::Result<(), TrySendError> {
        // Keep receive admission behind route publication, even when an
        // inproc peer replies on another application thread immediately.
        let mut routes = self.routes.lock().expect("REQ routes");
        let route_id = send()?;
        routes.selected = Some(
            routes
                .connected
                .get(&route_id)
                .map_or(ReqReplySource::Pending(route_id), |peer_id| {
                    ReqReplySource::Connected(*peer_id)
                }),
        );
        Ok(())
    }

    pub(crate) fn peer_connected(&self, route_id: u64, peer_id: u64) {
        let mut routes = self.routes.lock().expect("REQ routes");
        routes.connected.insert(route_id, peer_id);
        if routes.selected == Some(ReqReplySource::Pending(route_id)) {
            routes.selected = Some(ReqReplySource::Connected(peer_id));
        }
    }

    pub(crate) fn peer_disconnected(&self, route_id: u64, peer_id: u64) {
        let mut routes = self.routes.lock().expect("REQ routes");
        if routes.connected.get(&route_id) == Some(&peer_id) {
            routes.connected.remove(&route_id);
        }
        if matches!(routes.selected, Some(ReqReplySource::Connected(id)) if id == peer_id)
            || routes.selected == Some(ReqReplySource::Pending(route_id))
        {
            // A queued reply remains receivable after disconnect. A new
            // request replaces this source and rejects the old queued reply.
            self.clear_awaiting();
        }
    }

    pub(crate) fn accept_reply(&self, peer_id: u64) -> bool {
        let mut routes = self.routes.lock().expect("REQ routes");
        if routes.selected != Some(ReqReplySource::Connected(peer_id)) {
            return false;
        }
        routes.selected = None;
        self.clear_awaiting();
        true
    }

    pub(crate) fn clear(&self) {
        let mut routes = self.routes.lock().expect("REQ routes");
        routes.selected = None;
        self.clear_awaiting();
    }

    fn clear_awaiting(&self) {
        // Pairs with `wait_cleared`: either it sees the flag clear or this
        // sees its waiter count.
        self.awaiting.store(false, Ordering::SeqCst);
        if self.waiters.load(Ordering::SeqCst) > 0 {
            self.cleared.notify_changed();
        }
    }

    async fn wait_cleared(&self) {
        struct Waiting<'a>(&'a AtomicUsize);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        self.waiters.fetch_add(1, Ordering::SeqCst);
        let _waiting = Waiting(&self.waiters);
        loop {
            let seen = self.cleared.generation();
            if !self.awaiting.load(Ordering::SeqCst) {
                return;
            }
            self.cleared.changed_after(seen).await;
        }
    }
}

/// Clears the REQ alternation claim unless the request was sent.
struct ReqSendClaim<'a> {
    awaiting_reply: &'a ReqAwaitingReply,
    sent: bool,
}

impl Drop for ReqSendClaim<'_> {
    fn drop(&mut self) {
        if !self.sent {
            self.awaiting_reply.clear();
        }
    }
}

/// Restores an unanswered REP request when its send is canceled.
struct RepSendClaim<'a> {
    current: &'a Mutex<Option<(u64, RepEnvelope)>>,
    request: Option<(u64, RepEnvelope)>,
}

impl Drop for RepSendClaim<'_> {
    fn drop(&mut self) {
        if let Some(request) = self.request.take() {
            self.current
                .lock()
                .expect("rep identity")
                .get_or_insert(request);
        }
    }
}

fn rep_send_without_request() -> Error {
    Error::Protocol("REP socket must receive a request before replying".into())
}

/// Validate frame count for socket types that enforce a fixed count but whose
/// `TypeState::pre_send` has no mutable side effects. This mirrors the check
/// inside `TypeState::pre_send` for the relevant types so the actor-bypass
/// send path still surfaces the same protocol errors.
#[inline]
fn check_pre_send_frame_count(t: SocketType, msg: &Message) -> Result<()> {
    match t {
        SocketType::Client | SocketType::Scatter | SocketType::Gather | SocketType::Channel
            if msg.len() != 1 =>
        {
            Err(Error::Protocol(format!(
                "{t:?} socket requires single-part messages (got {})",
                msg.len()
            )))
        }
        _ => Ok(()),
    }
}
