//! Receive queue admission and bounded pending delivery for connection drivers.
//!
//! A full queue retains one decoded delivery without repeating metadata/rate
//! admission. MPSC reservations stay pinned across select turns and use their
//! actual permit. REP admits body and saved envelope together. Raw yring
//! consumers have a 10 ms fallback check for consumer drop without a space signal.

use std::sync::Arc;
use std::time::Duration;

use omq_proto::error::TrySendError;
use omq_proto::message::Message;
use tokio::sync::mpsc;

use super::signal::StateSignal;

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

/// Keep the peer route attached to the complete request until application
/// receive. No envelope side queue may depend on receive-source ordering.
#[derive(Debug)]
pub struct RepRecvSink {
    sink: Box<RecvSink>,
    routing_id: u32,
}

impl RepRecvSink {
    fn try_send(
        &mut self,
        message: Message,
        pending_flush: &mut bool,
    ) -> core::result::Result<(), TrySendError> {
        let original_routing_id = message.routing_id();
        let result = self.sink.try_send_unwrapped(
            message.with_routing_id(self.routing_id),
            false,
            pending_flush,
        );
        match result {
            Err(TrySendError::Full(mut returned)) => {
                let _ = returned.take_routing_id();
                if let Some(routing_id) = original_routing_id {
                    returned = returned.with_routing_id(routing_id);
                }
                Err(TrySendError::Full(returned))
            }
            other => other,
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
/// Only the peer owning the direct sink may trigger its replacement.
/// Unadopted replacement consumers retain queued messages across churn.
pub struct RecvSinkConfig {
    slot: std::sync::Mutex<SinkSlot>,
    pending_consumer: std::sync::Mutex<Option<yring::Consumer<Message>>>,
    signal: Arc<dyn Fn() + Send + Sync>,
    space: Arc<StateSignal>,
    cap: usize,
}

#[derive(Debug)]
struct SinkSlot {
    sink: Option<RecvSink>,
    owner_peer: Option<u64>,
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
            slot: std::sync::Mutex::new(SinkSlot {
                sink: Some(initial_sink),
                owner_peer: None,
            }),
            pending_consumer: std::sync::Mutex::new(None),
            signal,
            space,
            cap,
        }
    }

    /// Refill an unowned sink if its previous replacement was adopted.
    pub fn refill_sink(&self) {
        let mut slot = self.slot.lock().unwrap();
        if slot.owner_peer.is_none() {
            self.refill_unowned(&mut slot);
        }
    }

    fn refill_unowned(&self, slot: &mut SinkSlot) {
        if slot.sink.is_some() {
            return;
        }
        let mut pending = self.pending_consumer.lock().unwrap();
        if pending.is_some() {
            // Never discard a replacement ring before application adoption.
            // New peers can use the bounded fallback receive path meanwhile.
            return;
        }
        let (prod, cons) = yring::spsc(self.cap);
        let signal = self.signal.clone();
        slot.sink = Some(RecvSink::Yring(YringSink {
            producer: prod,
            signal: Box::new(move || signal()),
            space: self.space.clone(),
        }));
        *pending = Some(cons);
    }

    pub fn take_sink(&self) -> Option<RecvSink> {
        self.take_for_owner(None)
    }

    pub(crate) fn take_sink_for_peer(&self, peer_id: u64) -> Option<RecvSink> {
        self.take_for_owner(Some(peer_id))
    }

    fn take_for_owner(&self, peer_id: Option<u64>) -> Option<RecvSink> {
        let mut slot = self.slot.lock().unwrap();
        if let Some(RecvSink::Authenticated(sink)) = slot.sink.as_ref() {
            return Some(RecvSink::Authenticated(sink.clone()));
        }
        if slot.owner_peer.is_none() {
            self.refill_unowned(&mut slot);
        }
        let sink = slot.sink.take()?;
        slot.owner_peer = peer_id;
        Some(sink)
    }

    pub(crate) fn peer_disconnected(&self, peer_id: u64) {
        let mut slot = self.slot.lock().unwrap();
        if slot.owner_peer != Some(peer_id) {
            return;
        }
        slot.owner_peer = None;
        self.refill_unowned(&mut slot);
    }

    pub(crate) fn authenticated_sink(&self) -> Option<RecvSink> {
        let slot = self.slot.lock().unwrap();
        let RecvSink::Authenticated(sink) = slot.sink.as_ref()? else {
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
        if let yring::FlushResult::Flushed {
            was_empty: true, ..
        } = self.producer.flush_and_check()
        {
            (self.signal)();
        }
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
            Self::Rep(rep) => rep
                .sink
                .send_reserved(message.with_routing_id(rep.routing_id), permit),
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

    pub(crate) fn rep(sink: RecvSink, peer_id: u64) -> Self {
        Self::Rep(RepRecvSink {
            sink: Box::new(sink),
            routing_id: u32::try_from(peer_id + 1).expect("REP peer ID checked"),
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

    #[cfg(test)]
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

    /// Deliver without waiting. PEER retains one blocked delivery for its
    /// budget-aware waiter; other full queues return the original message.
    pub(crate) fn try_deliver(
        &mut self,
        message: Message,
    ) -> core::result::Result<(), TrySendError> {
        self.try_deliver_inner(message, false, &mut false)
    }

    #[cfg(feature = "dart")]
    /// Defer notification and return rejected ownership to the reliable
    /// datagram session, which retains it until application space returns.
    pub(crate) fn try_deliver_datagram(
        &mut self,
        message: Message,
        pending: &mut bool,
    ) -> core::result::Result<(), TrySendError> {
        self.try_deliver_inner(message, true, pending)
    }

    fn try_deliver_inner(
        &mut self,
        message: Message,
        defer: bool,
        pending: &mut bool,
    ) -> core::result::Result<(), TrySendError> {
        let routing_id = message.routing_id();
        let wrapped = matches!(self, Self::Server(_));
        match self.try_send_with_flush_mode(message, defer, pending) {
            Ok(()) => {}
            Err(TrySendError::Full(mut message)) => {
                if wrapped {
                    let _ = message.take_routing_id();
                    if let Some(id) = routing_id {
                        message = message.with_routing_id(id);
                    }
                }
                return Err(TrySendError::Full(message));
            }
            Err(error) => return Err(error),
        }
        let unwrapped = self.direct_inner();
        let returned = if let Self::Fanin(sink) = unwrapped {
            let message = sink.take_pending();
            // Register the producer's space wake before the endpoint parks.
            let full = if message.is_some() || !defer {
                sink.is_full()
            } else {
                false
            };
            if message.is_some() && !full {
                sink.space().notify_changed();
            }
            message
        } else {
            #[cfg(feature = "dart")]
            if defer && let Self::Peer(sink) = unwrapped {
                sink.take_pending()
            } else {
                None
            }
            #[cfg(not(feature = "dart"))]
            None
        };
        if let Some(mut message) = returned {
            if wrapped {
                let _ = message.take_routing_id();
                if let Some(id) = routing_id {
                    message = message.with_routing_id(id);
                }
            }
            return Err(TrySendError::Full(message));
        }
        Ok(())
    }

    #[cfg(feature = "dart")]
    pub(crate) fn flush_delivery(&mut self, pending: &mut bool) {
        self.flush_deferred(pending);
    }

    #[cfg(feature = "dart")]
    pub(crate) fn dart_forward_to(&mut self, signal: &Arc<super::signal::DataSignal>) {
        if let Self::Peer(sink) = self.direct_inner() {
            sink.dart_forward_to(signal);
        } else if let Some(space) = self.direct_space() {
            space.dart_forward_to(signal);
        }
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

    pub(crate) async fn receive_space_ready(&mut self) {
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
        } else if let Self::Server(server) = self {
            server.sink.flush_deferred(pending_yring_flush);
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
