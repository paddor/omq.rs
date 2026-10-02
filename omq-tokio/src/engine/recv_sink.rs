//! Receive queue admission and bounded pending delivery for connection drivers.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use omq_proto::error::TrySendError;
use omq_proto::message::Message;
use tokio::sync::mpsc;

use super::signal::StateSignal;
use crate::routing::RepEnvelope;

pub(crate) async fn reserve_authenticated(
    sender: Option<&mpsc::Sender<AuthenticatedRecvItem>>,
) -> core::result::Result<mpsc::Permit<'_, AuthenticatedRecvItem>, mpsc::error::SendError<()>> {
    match sender {
        Some(sender) => sender.reserve().await,
        None => std::future::pending().await,
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
    Fanin(crate::socket::fanin::Sink),
    Authenticated(AuthenticatedRecvSink),
    Conflate(Arc<crate::socket::recv::ConflateRecvSlot>),
    Rep(RepRecvSink),
    Server(ServerRecvSink),
    Peer(crate::socket::peer_recv::PeerRecvSink),
}

/// REP's latency receive path: perform identity/envelope handling in the
/// connection driver, before the message reaches the socket actor.
#[derive(Debug)]
pub struct RepRecvSink {
    sink: Box<RecvSink>,
    pending: std::sync::Arc<std::sync::Mutex<VecDeque<(u64, RepEnvelope)>>>,
    peer_id: u64,
}

impl RepRecvSink {
    fn try_send(
        &mut self,
        message: Message,
        pending_flush: &mut bool,
    ) -> core::result::Result<(), TrySendError> {
        let Some((envelope, body)) = crate::routing::split_rep_request(&message) else {
            return Ok(());
        };
        // Publish envelope and body as one admission. The user can drain
        // the body immediately, but its envelope lock waits until here.
        let mut pending = self.pending.lock().expect("rep pending");
        pending.push_back((self.peer_id, envelope));
        match self.sink.try_send_unwrapped(body, false, pending_flush) {
            Ok(()) => Ok(()),
            Err(error) => {
                pending.pop_back();
                Err(match error {
                    TrySendError::Full(_) => TrySendError::Full(message),
                    other => other,
                })
            }
        }
    }
}

/// SERVER's direct receive path: attach the connection's opaque routing ID
/// without constructing an identity frame or multipart message.
#[derive(Debug)]
pub struct ServerRecvSink {
    sink: Box<RecvSink>,
    routing_id: u32,
}

/// Yring-based recv sink. Pushes decoded messages directly into a
/// lock-free SPSC ring and signals the consumer via a callback on
/// empty-to-non-empty transitions.
#[allow(private_interfaces)]
pub struct YringSink {
    pub producer: yring::Producer<Message>,
    pub signal: Box<dyn Fn() + Send + Sync>,
    pub space: Arc<StateSignal>,
}

/// Message plus authenticated peer properties for compatibility layers that
/// expose ZAP metadata on each received message.
#[derive(Debug)]
pub struct AuthenticatedRecvItem {
    message: Message,
    peer_properties: Arc<omq_proto::proto::command::PeerProperties>,
}

impl AuthenticatedRecvItem {
    pub fn into_parts(self) -> (Message, Arc<omq_proto::proto::command::PeerProperties>) {
        (self.message, self.peer_properties)
    }
}

/// Shared MPSC receive sink used only by authenticated libzmq sockets.
#[derive(Clone)]
pub struct AuthenticatedRecvSink {
    sender: mpsc::Sender<AuthenticatedRecvItem>,
    signal: Arc<dyn Fn() + Send + Sync>,
    peer_properties: Option<Arc<omq_proto::proto::command::PeerProperties>>,
}

impl std::fmt::Debug for AuthenticatedRecvSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthenticatedRecvSink")
            .field("peer_properties", &self.peer_properties)
            .finish_non_exhaustive()
    }
}

/// Shared config for creating and recycling [`RecvSink::Yring`] instances.
/// The actor refills `slot` with a fresh yring pair on peer disconnect;
/// the external consumer picks up the new consumer from
/// `pending_consumer`.
pub struct RecvSinkConfig {
    slot: std::sync::Mutex<Option<RecvSink>>,
    pending_consumer: std::sync::Mutex<Option<yring::Consumer<Message>>>,
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
        let mut slot = self.slot.lock().unwrap();
        if let Some(RecvSink::Authenticated(sink)) = slot.as_ref() {
            return Some(RecvSink::Authenticated(sink.clone()));
        }
        slot.take()
    }

    pub(crate) fn authenticated_sink(&self) -> Option<RecvSink> {
        let slot = self.slot.lock().unwrap();
        let RecvSink::Authenticated(sink) = slot.as_ref()? else {
            return None;
        };
        Some(RecvSink::Authenticated(sink.clone()))
    }

    #[allow(private_interfaces)]
    pub fn try_take_pending_consumer(&self) -> Option<yring::Consumer<Message>> {
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
            Self::Authenticated(_) => f.debug_tuple("Authenticated").finish_non_exhaustive(),
            Self::Conflate(_) => f.debug_tuple("Conflate").finish_non_exhaustive(),
            Self::Rep(_) => f.debug_tuple("Rep").finish_non_exhaustive(),
            Self::Fanin(_) => f.debug_tuple("Fanin").finish_non_exhaustive(),
            Self::Peer(_) => f.debug_tuple("Peer").finish_non_exhaustive(),
            Self::Server(server) => f
                .debug_struct("Server")
                .field("routing_id", &server.routing_id)
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

impl YringSink {
    #[inline]
    pub(super) fn flush_and_signal(&mut self) {
        self.producer.flush();
        (self.signal)();
    }

    #[inline]
    pub(super) fn flush_pending(&mut self, pending: &mut bool) {
        if *pending {
            self.flush_and_signal();
            *pending = false;
        }
    }

    pub(super) fn try_send_deferred(
        &mut self,
        m: Message,
        pending: &mut bool,
    ) -> core::result::Result<(), TrySendError> {
        match self.producer.push(m) {
            Ok(()) => {
                *pending = true;
                Ok(())
            }
            Err(message) => {
                self.flush_pending(pending);
                if self.producer.is_consumer_dropped() {
                    Err(TrySendError::Closed)
                } else {
                    Err(TrySendError::Full(message))
                }
            }
        }
    }
}

impl RecvSink {
    /// Create a shared authenticated receive sink. Every connection gets a
    /// clone of the sender, so messages from all peers retain their own
    /// handshake properties in one application-facing queue.
    pub fn authenticated(
        cap: usize,
        signal: Arc<dyn Fn() + Send + Sync>,
    ) -> (Self, mpsc::Receiver<AuthenticatedRecvItem>) {
        let (sender, receiver) = mpsc::channel(cap);
        (
            Self::Authenticated(AuthenticatedRecvSink {
                sender,
                signal,
                peer_properties: None,
            }),
            receiver,
        )
    }

    pub(super) fn set_peer_properties(
        &mut self,
        peer_properties: Arc<omq_proto::proto::command::PeerProperties>,
    ) {
        match self {
            Self::Authenticated(sink) => sink.peer_properties = Some(peer_properties),
            Self::Rep(rep) => rep.sink.set_peer_properties(peer_properties),
            Self::Server(server) => server.sink.set_peer_properties(peer_properties),
            Self::Channel(_)
            | Self::Yring(_)
            | Self::Conflate(_)
            | Self::Peer(_)
            | Self::Fanin(_) => {}
        }
    }

    pub(crate) fn try_send_authenticated(
        &self,
        message: Message,
        peer_properties: Arc<omq_proto::proto::command::PeerProperties>,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        let Self::Authenticated(sink) = self else {
            unreachable!("actor authentication sink is unwrapped");
        };
        match sink.sender.try_send(AuthenticatedRecvItem {
            message,
            peer_properties,
        }) {
            Ok(()) => {
                (sink.signal)();
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(item)) => {
                Err(omq_proto::error::TrySendError::Full(item.message))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(omq_proto::error::TrySendError::Closed)
            }
        }
    }

    pub(crate) fn send_authenticated_reserved(
        &self,
        message: Message,
        peer_properties: Arc<omq_proto::proto::command::PeerProperties>,
        permit: mpsc::Permit<'_, AuthenticatedRecvItem>,
    ) {
        let Self::Authenticated(sink) = self else {
            unreachable!("actor authentication sink is unwrapped");
        };
        permit.send(AuthenticatedRecvItem {
            message,
            peer_properties,
        });
        (sink.signal)();
    }

    pub(crate) fn authenticated_sender(&self) -> Option<mpsc::Sender<AuthenticatedRecvItem>> {
        match self {
            Self::Authenticated(sink) => Some(sink.sender.clone()),
            Self::Rep(rep) => rep.sink.authenticated_sender(),
            Self::Server(server) => server.sink.authenticated_sender(),
            _ => None,
        }
    }

    pub(super) fn send_reserved(
        &mut self,
        message: Message,
        permit: mpsc::Permit<'_, AuthenticatedRecvItem>,
    ) -> bool {
        match self {
            Self::Rep(rep) => {
                let Some((envelope, body)) = crate::routing::split_rep_request(&message) else {
                    return true;
                };
                let mut pending = rep.pending.lock().expect("rep pending");
                pending.push_back((rep.peer_id, envelope));
                rep.sink.send_reserved(body, permit)
            }
            Self::Server(server) => server
                .sink
                .send_reserved(message.with_routing_id(server.routing_id), permit),
            Self::Authenticated(sink) => {
                let properties = sink
                    .peer_properties
                    .clone()
                    .expect("authenticated sink activated after handshake");
                self.send_authenticated_reserved(message, properties, permit);
                true
            }
            _ => unreachable!("authenticated receive reservation"),
        }
    }

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

    pub(crate) fn server(sink: RecvSink, routing_id: u32) -> Self {
        Self::Server(ServerRecvSink {
            sink: Box::new(sink),
            routing_id,
        })
    }

    pub(super) fn is_yring(&self) -> bool {
        match self {
            Self::Yring(_) | Self::Peer(_) | Self::Fanin(_) => true,
            Self::Server(server) => server.sink.is_yring(),
            _ => false,
        }
    }

    /// Non-blocking push. Returns the message back if the yring is full.
    /// Channel variant always succeeds (awaits space).
    pub(crate) async fn try_send(&mut self, m: Message) -> Option<Message> {
        if let Self::Server(server) = self {
            let routed = m.with_routing_id(server.routing_id);
            let _ = server.sink.send_plain(routed).await;
            return None;
        }
        self.try_send_plain(m).await
    }

    async fn try_send_plain(&mut self, m: Message) -> Option<Message> {
        match self {
            Self::Fanin(sink) => {
                let _ = sink.push(m);
                None
            }
            Self::Peer(_) => unreachable!("PEER receives never fall back through the actor"),
            Self::Channel(pipe) => {
                let _ = pipe.send(m).await;
                None
            }
            Self::Yring(sink) => match sink.producer.push(m) {
                Ok(()) => {
                    sink.flush_and_signal();
                    None
                }
                Err(returned) => Some(returned),
            },
            Self::Authenticated(sink) => {
                let peer_properties = sink
                    .peer_properties
                    .clone()
                    .expect("authenticated sink activated after handshake");
                match sink.sender.try_send(AuthenticatedRecvItem {
                    message: m,
                    peer_properties,
                }) {
                    Ok(()) => {
                        (sink.signal)();
                        None
                    }
                    Err(
                        mpsc::error::TrySendError::Full(item)
                        | mpsc::error::TrySendError::Closed(item),
                    ) => Some(item.message),
                }
            }
            Self::Conflate(slot) => {
                let _ = slot.send_latest(m);
                None
            }
            Self::Rep(_) => unreachable!("REP uses the blocking direct path"),
            Self::Server(_) => unreachable!("nested SERVER sink"),
        }
    }

    async fn send_plain(&mut self, m: Message) -> bool {
        match self {
            Self::Fanin(sink) => sink.push(m),
            Self::Peer(sink) => {
                let alive = sink.push(m);
                sink.flush();
                alive
            }
            Self::Channel(pipe) => pipe.send(m).await.is_ok(),
            Self::Yring(sink) => {
                let mut msg = m;
                loop {
                    if let Err(returned) = sink.producer.push(msg) {
                        msg = returned;
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
                    if let Err(returned) = sink.producer.push(msg) {
                        msg = returned;
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
            Self::Authenticated(sink) => {
                let peer_properties = sink
                    .peer_properties
                    .clone()
                    .expect("authenticated sink activated after handshake");
                if sink
                    .sender
                    .send(AuthenticatedRecvItem {
                        message: m,
                        peer_properties,
                    })
                    .await
                    .is_err()
                {
                    return false;
                }
                (sink.signal)();
                true
            }
            Self::Conflate(slot) => slot.send_latest(m),
            Self::Rep(_) | Self::Server(_) => unreachable!("wrapped sink uses routed send"),
        }
    }

    pub(crate) async fn send(&mut self, m: Message) -> bool {
        let mut message = m;
        loop {
            match self.try_send_with_flush_mode(message, false, &mut false) {
                Ok(()) => return true,
                Err(TrySendError::Full(returned)) => message = returned,
                Err(_) => return false,
            }
            if let Some(sender) = self.authenticated_sender() {
                return match sender.reserve().await {
                    Ok(permit) => self.send_reserved(message, permit),
                    Err(_) => false,
                };
            }
            self.receive_space_ready().await;
        }
    }

    #[inline]
    pub(super) fn try_send_with_flush_mode(
        &mut self,
        m: Message,
        defer_yring_flush: bool,
        pending_yring_flush: &mut bool,
    ) -> core::result::Result<(), TrySendError> {
        if let Self::Rep(rep) = self {
            return rep.try_send(m, pending_yring_flush);
        }
        if let Self::Server(server) = self {
            return server.sink.try_send_unwrapped(
                m.with_routing_id(server.routing_id),
                defer_yring_flush,
                pending_yring_flush,
            );
        }
        self.try_send_unwrapped(m, defer_yring_flush, pending_yring_flush)
    }

    #[inline]
    fn try_send_unwrapped(
        &mut self,
        message: Message,
        defer: bool,
        pending: &mut bool,
    ) -> core::result::Result<(), TrySendError> {
        let alive = match self {
            Self::Fanin(sink) => {
                if defer {
                    *pending = true;
                    sink.push_deferred(message)
                } else {
                    sink.push(message)
                }
            }
            Self::Peer(sink) => {
                let alive = sink.push(message);
                if !defer {
                    sink.flush();
                }
                alive
            }
            Self::Channel(pipe) => return pipe.try_send(message),
            Self::Yring(sink) => {
                sink.try_send_deferred(message, pending)?;
                if !defer {
                    sink.flush_pending(pending);
                }
                return Ok(());
            }
            Self::Authenticated(sink) => {
                let properties = sink
                    .peer_properties
                    .clone()
                    .expect("authenticated sink activated after handshake");
                return self.try_send_authenticated(message, properties);
            }
            Self::Conflate(slot) => slot.send_latest(message),
            Self::Rep(_) | Self::Server(_) => unreachable!("unwrapped receive sink"),
        };
        if alive {
            Ok(())
        } else {
            Err(TrySendError::Closed)
        }
    }

    /// Whether a foreign thread can deliver into this sink. The other
    /// sinks need the owning driver for admission or peer metadata.
    pub(crate) fn supports_direct(&self) -> bool {
        match self {
            Self::Channel(_) | Self::Yring(_) | Self::Fanin(_) | Self::Conflate(_) => true,
            Self::Rep(rep) => rep.sink.supports_direct(),
            Self::Server(server) => server.sink.supports_direct(),
            Self::Authenticated(_) | Self::Peer(_) => false,
        }
    }

    /// Deliver one message without waiting. A full queue returns the
    /// message unchanged; the sink never retains it.
    pub(crate) fn try_deliver(
        &mut self,
        message: Message,
    ) -> core::result::Result<(), TrySendError> {
        match self.try_send_with_flush_mode(message, false, &mut false) {
            Ok(()) => {}
            Err(TrySendError::Full(mut message)) => {
                if matches!(self, Self::Server(_)) {
                    let _ = message.take_routing_id();
                }
                return Err(TrySendError::Full(message));
            }
            Err(error) => return Err(error),
        }
        if let Self::Fanin(sink) = self
            && let Some(message) = sink.take_pending()
        {
            // Register the space waker before reporting the full queue.
            let _ = sink.is_full();
            return Err(TrySendError::Full(message));
        }
        Ok(())
    }

    fn direct_inner(&mut self) -> &mut Self {
        let mut unwrapped = self;
        loop {
            unwrapped = match unwrapped {
                Self::Rep(rep) => rep.sink.as_mut(),
                Self::Server(server) => server.sink.as_mut(),
                _ => return unwrapped,
            };
        }
    }

    /// Signal that changes when the queue behind this sink frees space.
    /// `None` for sinks that are never full.
    pub(crate) fn direct_space(&mut self) -> Option<Arc<StateSignal>> {
        match self.direct_inner() {
            Self::Channel(pipe) => Some(pipe.space_signal()),
            Self::Yring(sink) => Some(sink.space.clone()),
            Self::Fanin(sink) => {
                let _ = sink.is_full();
                Some(sink.space())
            }
            _ => None,
        }
    }

    pub(crate) fn direct_has_space(&mut self) -> bool {
        match self.direct_inner() {
            Self::Channel(pipe) => pipe.has_space(),
            Self::Yring(sink) => sink.producer.is_consumer_dropped() || !sink.producer.is_full(),
            Self::Fanin(sink) => !sink.is_full(),
            _ => true,
        }
    }

    pub(super) async fn receive_space_ready(&mut self) {
        let mut unwrapped = self;
        loop {
            unwrapped = match unwrapped {
                Self::Rep(rep) => rep.sink.as_mut(),
                Self::Server(server) => server.sink.as_mut(),
                _ => break,
            };
        }
        match unwrapped {
            Self::Channel(pipe) => pipe.space_ready().await,
            Self::Yring(sink) => {
                // A raw yring consumer drop has no StateSignal callback.
                // Preserve the old drop check without polling the data path.
                tokio::select! {
                    () = sink.space.wait_until(|| {
                        sink.producer.is_consumer_dropped() || !sink.producer.is_full()
                    }) => {},
                    () = tokio::time::sleep(Duration::from_millis(10)) => {},
                }
            }
            Self::Authenticated(_) => unreachable!("authenticated admission retains its permit"),
            Self::Fanin(sink) => sink.ready().await,
            Self::Peer(sink) => sink.ready().await,
            Self::Conflate(_) => {}
            Self::Rep(_) | Self::Server(_) => unreachable!("unwrapped receive sink"),
        }
    }

    pub(super) fn flush_deferred(&mut self, pending_yring_flush: &mut bool) {
        if let Self::Fanin(sink) = self {
            if *pending_yring_flush {
                sink.flush();
                *pending_yring_flush = false;
            }
        } else if let Self::Peer(sink) = self {
            sink.flush();
        } else if let Self::Yring(sink) = self {
            sink.flush_pending(pending_yring_flush);
        } else if let Self::Server(server) = self
            && let Self::Yring(sink) = server.sink.as_mut()
        {
            sink.flush_pending(pending_yring_flush);
        }
    }

    pub(crate) fn peer_blocked(&self) -> bool {
        match self {
            Self::Peer(sink) => sink.blocked(),
            Self::Fanin(sink) => sink.blocked(),
            _ => false,
        }
    }

    pub(crate) fn retry_peer_pending(&mut self) -> bool {
        match self {
            Self::Peer(sink) => sink.retry_pending(),
            Self::Fanin(sink) => sink.retry_pending(),
            _ => true,
        }
    }

    pub(crate) async fn peer_space_ready(&mut self) {
        if let Self::Fanin(sink) = self {
            sink.ready().await;
        }
        if let Self::Peer(sink) = self {
            sink.ready().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn concurrent_authenticated_waiters_keep_their_reserved_slots() {
        let (mut first, mut receive) = RecvSink::authenticated(1, Arc::new(|| {}));
        let properties = Arc::new(omq_proto::proto::command::PeerProperties::default());
        first.set_peer_properties(properties);
        let RecvSink::Authenticated(authenticated) = &first else {
            unreachable!()
        };
        let mut second = RecvSink::Authenticated(authenticated.clone());
        assert!(first.send(Message::single("initial")).await);
        let first = tokio::spawn(async move { first.send(Message::single("first")).await });
        tokio::task::yield_now().await;
        let second = tokio::spawn(async move { second.send(Message::single("second")).await });
        tokio::task::yield_now().await;
        let _ = receive.recv().await.unwrap();
        for expected in ["first", "second"] {
            let item = tokio::time::timeout(Duration::from_millis(100), receive.recv())
                .await
                .expect("reserved receive slots must deliver instead of passing to another waiter")
                .unwrap();
            assert_eq!(item.message.part_slice(0).unwrap(), expected.as_bytes());
        }
        assert!(first.await.unwrap());
        assert!(second.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn raw_yring_consumer_drop_releases_a_parked_sender_without_a_space_callback() {
        let (producer, consumer) = yring::spsc(1);
        let mut sink = RecvSink::Yring(YringSink {
            producer,
            signal: Box::new(|| {}),
            space: Arc::new(StateSignal::new()),
        });
        assert!(sink.send(Message::single("accepted")).await);
        let blocked = tokio::spawn(async move { sink.send(Message::single("blocked")).await });
        tokio::task::yield_now().await;
        drop(consumer);
        assert!(
            !tokio::time::timeout(Duration::from_millis(50), blocked)
                .await
                .unwrap()
                .unwrap()
        );
    }
}
