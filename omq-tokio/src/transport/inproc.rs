//! In-process transport.
//!
//! `inproc://name` endpoints are resolved via the owning context's
//! registry. Unlike TCP/IPC, **inproc skips the ZMTP codec
//! entirely** - both ends are in the same process, so we exchange
//! parsed `Message` values directly. The peer's socket type and
//! identity are exchanged during connect, not over the wire, so the
//! synthesized handshake completes immediately.
//!
//! Direct message paths use one `yring` per direction. The sending socket
//! pushes from the calling thread and the receiving socket drains it
//! from its own `recv`, so no task runs in between. The ring holds the
//! sender's `Options::send_hwm` plus the receiver's `Options::recv_hwm`
//! messages. Separate Coordinated fanring lanes carry commands (SUBSCRIBE,
//! JOIN, ...) and messages for socket types that still route through their
//! peer task. Registry requests retain their multi-producer Tokio queue.

use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex as ParkingMutex;

use omq_proto::Message;

use crate::engine::send_pipe::SendPreparation;
use crate::engine::signal::StateSignal;
use crate::engine::{RecvSink, SendPipeConsumer, SendPipeError};

mod registry;
mod relay;

pub use registry::{DEFAULT_INPROC_HWM, InprocConn, InprocListener, InprocRegistry};
pub(crate) use registry::{bind, connect, standalone_registry};
#[cfg(test)]
pub(crate) use relay::relay_channel;
pub use relay::{RelayReceiver, RelaySender};

/// What one socket tells its inproc peers at connect time.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecvConfig {
    /// The peer's send path may push into this socket's receive ring
    /// from its own threads.
    pub direct: bool,
    /// This socket's send HWM. The peer adds it to its receive HWM to
    /// size the ring for this socket's sends.
    pub send_hwm: usize,
}

/// One connection's entry into the receiving socket's queue.
///
/// The receiving socket opens the port with the sink for this connection.
/// The peer's send path then delivers from the calling thread, with no
/// task on either side in between.
pub(crate) struct InprocPort {
    state: ParkingMutex<PortState>,
    /// Notified when the port opens or closes.
    changed: Arc<StateSignal>,
}

enum PortState {
    Pending,
    Open(Box<OpenPort>),
    Closed,
}

/// Receive-side state installed when the receiving peer becomes ready.
pub(crate) struct OpenPort {
    pub(crate) sink: RecvSink,
    /// ROUTER receive: identity frame prepended to every message.
    pub(crate) identity: Option<omq_proto::Identity>,
    pub(crate) max_message_size: Option<usize>,
    /// Cancels the receiving peer after a protocol violation.
    pub(crate) cancel: tokio_util::sync::CancellationToken,
}

impl std::fmt::Debug for InprocPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match &*self.state.lock() {
            PortState::Pending => "pending",
            PortState::Open(_) => "open",
            PortState::Closed => "closed",
        };
        f.debug_struct("InprocPort")
            .field("state", &state)
            .finish_non_exhaustive()
    }
}

impl InprocPort {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: ParkingMutex::new(PortState::Pending),
            changed: Arc::new(StateSignal::new()),
        })
    }

    /// Start accepting messages. A port that already closed stays closed.
    pub(crate) fn open(&self, open: OpenPort) {
        let mut state = self.state.lock();
        if matches!(*state, PortState::Pending) {
            *state = PortState::Open(Box::new(open));
        }
        drop(state);
        self.changed.notify_changed();
    }

    pub(crate) fn close(&self) {
        Self::close_locked(&mut self.state.lock(), &self.changed);
    }

    fn close_locked(state: &mut PortState, changed: &StateSignal) {
        if let PortState::Open(mut open) = std::mem::replace(state, PortState::Closed) {
            open.sink.flush_delivery(&mut true);
            if let Some(space) = open.sink.direct_space() {
                space.notify_changed();
            }
        }
        changed.notify_changed();
    }

    pub(crate) fn is_closed(&self) -> bool {
        matches!(*self.state.lock(), PortState::Closed)
    }

    /// Deliver one message into the receiving socket's queue.
    pub(crate) fn try_send(&self, msg: Message) -> std::result::Result<(), SendPipeError> {
        self.try_send_inner::<false>(msg, &mut false)
    }

    fn try_send_inner<const DEFERRED: bool>(
        &self,
        msg: Message,
        pending: &mut bool,
    ) -> std::result::Result<(), SendPipeError> {
        let mut state = self.state.lock();
        let open = match &mut *state {
            PortState::Pending => return Err(SendPipeError::Full(msg)),
            PortState::Closed => return Err(SendPipeError::Closed(msg)),
            PortState::Open(open) => open,
        };
        if open
            .max_message_size
            .is_some_and(|max| msg.max_message_size_len() > max)
        {
            // The receiver drops the connection and the message with it.
            open.cancel.cancel();
            Self::close_locked(&mut state, &self.changed);
            return Ok(());
        }
        let prefixed = open.identity.is_some();
        let msg = match &open.identity {
            Some(identity) => Message::with_identity(identity.clone(), msg),
            None => msg,
        };
        let delivered = if DEFERRED {
            open.sink.try_deliver_deferred(msg, pending)
        } else {
            open.sink.try_deliver(msg)
        };
        match delivered {
            Ok(()) => Ok(()),
            Err(omq_proto::error::TrySendError::Full(mut msg)) => {
                if prefixed {
                    msg.pop_front_payload();
                }
                Err(SendPipeError::Full(msg))
            }
            Err(_) => {
                // The receiving socket closed its queue. The message is
                // lost like any other message queued behind the closure.
                Self::close_locked(&mut state, &self.changed);
                Ok(())
            }
        }
    }

    fn flush_deferred(&self, pending: &mut bool) {
        if !*pending {
            return;
        }
        if let PortState::Open(open) = &mut *self.state.lock() {
            open.sink.flush_delivery(pending);
        }
        *pending = false;
    }

    /// Whether a send can make progress now. A closed port reports `true`
    /// so a waiting sender retries and observes the closure.
    pub(crate) fn has_space(&self) -> bool {
        self.admission() != Admission::Full
    }

    /// What a send would run into right now, in one lock.
    pub(crate) fn admission(&self) -> Admission {
        match &mut *self.state.lock() {
            PortState::Pending => Admission::Full,
            PortState::Open(open) => {
                if open.sink.direct_has_space() {
                    Admission::Ready
                } else {
                    Admission::Full
                }
            }
            PortState::Closed => Admission::Closed,
        }
    }

    /// Signal that changes when a retry can make progress.
    pub(crate) fn space(&self) -> Arc<StateSignal> {
        match &mut *self.state.lock() {
            PortState::Open(open) => open
                .sink
                .direct_space()
                .unwrap_or_else(|| self.changed.clone()),
            _ => self.changed.clone(),
        }
    }
}

/// Outcome a send would have right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Admission {
    Ready,
    Full,
    Closed,
}

/// Send half of a direct inproc route.
///
/// Every send pushes into the connection's ring, which the peer socket
/// drains on its own threads. Messages left in a connect-side pipe from
/// before the peer was ready are moved into the ring first, in order.
#[derive(Debug, Clone)]
pub(crate) struct InprocSender {
    inner: Arc<SenderInner>,
}

#[derive(Debug)]
struct SenderInner {
    port: Arc<InprocPort>,
    backlog: ParkingMutex<Backlog>,
    close_progress: OnceLock<Arc<StateSignal>>,
    /// True until the connect-side pipe is empty. Written under the
    /// backlog lock; the send fast path reads it without the lock.
    backlog_pending: std::sync::atomic::AtomicBool,
}

#[derive(Debug)]
struct Backlog {
    pre_ready: Option<SendPipeConsumer>,
    /// Message taken from `pre_ready` that the ring did not accept yet.
    stalled: Option<Message>,
    batch: Vec<Message>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flush {
    Done,
    Stalled,
    Closed,
}

impl InprocSender {
    pub(crate) fn watch_close(&self, progress: &Arc<StateSignal>) {
        let existing = self.inner.close_progress.get_or_init(|| progress.clone());
        debug_assert!(Arc::ptr_eq(existing, progress));
    }

    fn notify_close_progress(&self) {
        if let Some(progress) = self.inner.close_progress.get() {
            progress.notify_changed();
        }
    }

    pub(crate) fn new(port: Arc<InprocPort>, pre_ready: Option<SendPipeConsumer>) -> Self {
        Self {
            inner: Arc::new(SenderInner {
                port,
                close_progress: OnceLock::new(),
                backlog_pending: std::sync::atomic::AtomicBool::new(pre_ready.is_some()),
                backlog: ParkingMutex::new(Backlog {
                    pre_ready,
                    stalled: None,
                    batch: Vec::new(),
                }),
            }),
        }
    }

    pub(crate) fn try_send_prepared(
        &self,
        msg: Message,
        preparation: SendPreparation,
    ) -> std::result::Result<(), SendPipeError> {
        let mut msg = self.after_backlog(msg)?;
        if !matches!(preparation, SendPreparation::StripIdentity) {
            return self.inner.port.try_send(msg);
        }
        let Some(identity) = msg.pop_front_payload() else {
            return self.inner.port.try_send(msg);
        };
        // The caller gets the routed message back when it was not admitted.
        let restore = |msg| Message::with_prefix(identity.as_bytes(), msg);
        self.inner.port.try_send(msg).map_err(|error| match error {
            SendPipeError::Full(msg) => SendPipeError::Full(restore(msg)),
            SendPipeError::Closed(msg) => SendPipeError::Closed(restore(msg)),
            #[cfg(feature = "dart")]
            SendPipeError::Invalid(error) => SendPipeError::Invalid(error),
        })
    }

    pub(crate) fn try_send_deferred(
        &self,
        msg: Message,
        pending: &mut bool,
    ) -> std::result::Result<(), SendPipeError> {
        let msg = self.after_backlog(msg)?;
        self.inner.port.try_send_inner::<true>(msg, pending)
    }

    pub(crate) fn flush_deferred(&self, pending: &mut bool) {
        self.inner.port.flush_deferred(pending);
    }

    #[inline]
    fn after_backlog(&self, msg: Message) -> std::result::Result<Message, SendPipeError> {
        if self.has_backlog() {
            match self.flush_backlog() {
                Flush::Done => {}
                Flush::Stalled => return Err(SendPipeError::Full(msg)),
                Flush::Closed => return Err(SendPipeError::Closed(msg)),
            }
        }
        Ok(msg)
    }

    fn has_backlog(&self) -> bool {
        self.inner.backlog_pending.load(Ordering::Acquire)
    }

    /// Move the connect-side backlog into the ring. The owning send
    /// strategy stops writing to that pipe before this sender is used,
    /// so an empty pipe is final.
    fn flush_backlog(&self) -> Flush {
        let mut backlog = self.inner.backlog.lock();
        loop {
            if let Some(msg) = backlog.stalled.take() {
                match self.inner.port.try_send(msg) {
                    #[cfg(feature = "dart")]
                    Err(SendPipeError::Invalid(_)) => unreachable!("inproc has no DART validator"),
                    Ok(()) => {}
                    Err(SendPipeError::Full(msg)) => {
                        backlog.stalled = Some(msg);
                        return Flush::Stalled;
                    }
                    Err(SendPipeError::Closed(_)) => {
                        backlog.pre_ready = None;
                        self.inner.backlog_pending.store(false, Ordering::Release);
                        self.notify_close_progress();
                        return Flush::Closed;
                    }
                }
            }
            let Backlog {
                pre_ready, batch, ..
            } = &mut *backlog;
            let drained = pre_ready
                .as_mut()
                .map_or(0, |consumer| consumer.drain_into(batch, 1, usize::MAX));
            if drained == 0 {
                backlog.pre_ready = None;
                self.inner.backlog_pending.store(false, Ordering::Release);
                self.notify_close_progress();
                // Admission counted the backlog as full. Senders waiting on
                // this route's space must see that it cleared.
                self.inner.port.space().notify_changed();
                return Flush::Done;
            }
            backlog.stalled = backlog.batch.pop();
        }
    }

    /// Deliver the connect-side backlog, waiting for the ring to open.
    pub(crate) async fn deliver_backlog(&self) {
        while self.has_backlog() {
            let space = self.inner.port.space();
            let seen = space.generation();
            if self.flush_backlog() != Flush::Stalled {
                return;
            }
            space.changed_after(seen).await;
        }
    }

    pub(crate) fn is_alive(&self) -> bool {
        !self.inner.port.is_closed()
    }

    /// No accepted message is waiting on the sending side.
    pub(crate) fn is_empty(&self) -> bool {
        !self.has_backlog()
    }

    /// Whether a send can make progress now.
    pub(crate) fn has_space(&self) -> bool {
        self.inner.port.has_space()
    }

    /// What a send would run into right now. A pending backlog counts
    /// as full: it goes first.
    pub(crate) fn admission(&self) -> Admission {
        if self.has_backlog() {
            return Admission::Full;
        }
        self.inner.port.admission()
    }

    pub(crate) fn space(&self) -> Arc<StateSignal> {
        self.inner.port.space()
    }

    /// Wait until a send can make progress or the route closed.
    pub(crate) async fn wait_space(&self) {
        loop {
            let space = self.space();
            let seen = space.generation();
            if self.has_space() {
                return;
            }
            space.changed_after(seen).await;
        }
    }
}

#[cfg(test)]
mod batch_tests;
#[cfg(test)]
mod registry_tests;
