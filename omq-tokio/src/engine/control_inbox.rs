//! Actor-owned protocol lane. Bounded lifecycle slots keep activation and
//! shutdown reachable even when forwarding protocol commands is blocked.
//! Protocol commands never make the actor wait: commands that do not fit the
//! ring queue in order behind it, so one stalled driver cannot stall the
//! socket. Their volume follows subscriptions and joins, not data.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::mpsc;

use super::{PeerDriverCommand, signal::StateSignal, single_inbox};

#[derive(Debug, Default)]
struct Pending {
    activation: Option<PeerDriverCommand>,
    close: Option<PeerDriverCommand>,
    /// Protocol commands after a full ring, oldest first.
    overflow: VecDeque<PeerDriverCommand>,
    closed: bool,
}

impl Pending {
    fn occupied(&self) -> bool {
        self.activation.is_some() || self.close.is_some() || !self.overflow.is_empty()
    }
}

#[derive(Debug)]
pub(crate) struct Lifecycle {
    pending: Mutex<Pending>,
    /// Whether `pending` holds a command. Written under the lock, read
    /// without it so an idle driver turn skips the lock.
    occupied: AtomicBool,
    changed: StateSignal,
    receive: futures::task::AtomicWaker,
    #[cfg(feature = "dart")]
    endpoint: std::sync::OnceLock<Arc<super::signal::DataSignal>>,
}

impl Lifecycle {
    fn lock(&self) -> PendingGuard<'_> {
        PendingGuard {
            pending: self.pending.lock().expect("control lifecycle poisoned"),
            occupied: &self.occupied,
        }
    }

    /// Whether a lifecycle or overflow command may be pending. Acquire pairs
    /// with the guard's Release store, so a woken receiver sees the flag.
    fn may_be_occupied(&self) -> bool {
        self.occupied.load(Ordering::Acquire)
    }
}

/// Lock on `Pending` that republishes `occupied` before it unlocks.
struct PendingGuard<'a> {
    pending: MutexGuard<'a, Pending>,
    occupied: &'a AtomicBool,
}

impl std::ops::Deref for PendingGuard<'_> {
    type Target = Pending;

    fn deref(&self) -> &Pending {
        &self.pending
    }
}

impl std::ops::DerefMut for PendingGuard<'_> {
    fn deref_mut(&mut self) -> &mut Pending {
        &mut self.pending
    }
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.occupied
            .store(self.pending.occupied(), Ordering::Release);
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Sender {
    Owned {
        commands: single_inbox::Sender<PeerDriverCommand>,
        lifecycle: Arc<Lifecycle>,
    },
    Legacy(mpsc::Sender<PeerDriverCommand>),
}

#[derive(Debug)]
pub(crate) enum Receiver {
    Owned {
        commands: Box<single_inbox::Receiver<PeerDriverCommand>>,
        lifecycle: Arc<Lifecycle>,
    },
    Legacy(mpsc::Receiver<PeerDriverCommand>),
}

pub(crate) fn channel(capacity: usize) -> (Sender, Receiver) {
    let (tx, rx) = single_inbox::channel(capacity);
    let lifecycle = Arc::new(Lifecycle {
        pending: Mutex::new(Pending::default()),
        occupied: AtomicBool::new(false),
        changed: StateSignal::new(),
        receive: futures::task::AtomicWaker::new(),
        #[cfg(feature = "dart")]
        endpoint: std::sync::OnceLock::new(),
    });

    (
        Sender::Owned {
            commands: tx,
            lifecycle: lifecycle.clone(),
        },
        Receiver::Owned {
            commands: Box::new(rx),
            lifecycle,
        },
    )
}

impl Sender {
    #[expect(
        clippy::result_large_err,
        reason = "return original command without an error allocation"
    )]
    pub(crate) fn try_send(
        &self,
        command: PeerDriverCommand,
    ) -> Result<(), mpsc::error::TrySendError<PeerDriverCommand>> {
        let Self::Owned {
            commands,
            lifecycle,
        } = self
        else {
            let Self::Legacy(sender) = self else {
                unreachable!()
            };

            return sender.try_send(command);
        };

        let mut pending = lifecycle.lock();
        if pending.closed {
            return Err(mpsc::error::TrySendError::Closed(command));
        }

        let old = match command {
            PeerDriverCommand::SendCommand(_) => {
                // Serialize with lifecycle admission; one native producer.
                // The ring only takes commands while no older one overflowed.
                if pending.overflow.is_empty() {
                    match commands.try_send(command) {
                        Err(mpsc::error::TrySendError::Full(returned)) => {
                            pending.overflow.push_back(returned);
                        }
                        result => return result,
                    }
                } else {
                    pending.overflow.push_back(command);
                }

                drop(pending);
                lifecycle.receive.wake();

                #[cfg(feature = "dart")]
                if let Some(signal) = lifecycle.endpoint.get() {
                    signal.mark();
                }

                return Ok(());
            }
            PeerDriverCommand::ActivateDataPlane | PeerDriverCommand::ActivateWithRecvSink(_) => {
                pending.activation.replace(command)
            }
            PeerDriverCommand::Close | PeerDriverCommand::DrainAndClose { .. } => {
                pending.closed = true;
                pending.close.replace(command)
            }
        };

        drop(pending);
        drop(old);
        lifecycle.changed.notify_changed();
        lifecycle.receive.wake();

        #[cfg(feature = "dart")]
        if let Some(signal) = lifecycle.endpoint.get() {
            signal.mark();
        }

        Ok(())
    }

    #[allow(
        clippy::result_large_err,
        reason = "return the original command without an error allocation"
    )]
    pub(crate) async fn send(
        &self,
        mut command: PeerDriverCommand,
    ) -> Result<(), mpsc::error::SendError<PeerDriverCommand>> {
        let Self::Owned {
            commands,
            lifecycle,
        } = self
        else {
            let Self::Legacy(sender) = self else {
                unreachable!()
            };

            return sender.send(command).await;
        };

        loop {
            let seen = lifecycle.changed.generation();
            match self.try_send(command) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Closed(returned)) => {
                    return Err(mpsc::error::SendError(returned));
                }
                Err(mpsc::error::TrySendError::Full(returned)) => command = returned,
            }

            tokio::select! {
                _ = commands.ready() => {},
                () = lifecycle.changed.changed_after(seen) => {},
            }
        }
    }

    /// Whether protocol commands wait behind a full ring.
    pub(crate) fn protocol_backlogged(&self) -> bool {
        let Self::Owned { lifecycle, .. } = self else {
            return false;
        };

        !lifecycle.lock().overflow.is_empty()
    }

    /// Wait until no protocol command waits behind a full ring, or the lane
    /// closed. A caller feels the backpressure that the actor must not.
    pub(crate) async fn protocol_drained(&self) {
        let Self::Owned { lifecycle, .. } = self else {
            return;
        };

        lifecycle
            .changed
            .wait_until(|| {
                let pending = lifecycle.lock();
                pending.closed || pending.overflow.is_empty()
            })
            .await;
    }
}

impl Receiver {
    #[cfg(feature = "dart")]
    pub(crate) fn dart_forward_to(&self, signal: Arc<super::signal::DataSignal>) {
        if let Self::Owned { lifecycle, .. } = self {
            let _ = lifecycle.endpoint.set(signal);
        }
    }

    pub(crate) async fn recv_lifecycle(&mut self) -> Option<PeerDriverCommand> {
        let Self::Owned { lifecycle, .. } = self else {
            return futures::future::pending().await;
        };

        futures::future::poll_fn(|cx| {
            lifecycle.receive.register(cx.waker());
            Self::take_lifecycle(lifecycle).map_or(std::task::Poll::Pending, |command| {
                std::task::Poll::Ready(Some(command))
            })
        })
        .await
    }

    fn take_lifecycle(lifecycle: &Lifecycle) -> Option<PeerDriverCommand> {
        if !lifecycle.may_be_occupied() {
            return None;
        }

        let mut pending = lifecycle.lock();
        if matches!(pending.close, Some(PeerDriverCommand::Close)) {
            return pending.close.take();
        }

        pending.activation.take().or_else(|| pending.close.take())
    }

    /// Overflow holds only commands newer than every ring entry, so it is
    /// read after the ring is empty.
    fn take_overflow(
        commands: &mut single_inbox::Receiver<PeerDriverCommand>,
        lifecycle: &Lifecycle,
    ) -> Option<PeerDriverCommand> {
        if !lifecycle.may_be_occupied() {
            return None;
        }

        let mut pending = lifecycle.lock();
        // The sender fills the ring and the overflow under this lock. It
        // may have done both since the caller saw the ring empty, so look
        // at the ring once more: its commands are the older ones.
        if let Ok(command) = commands.try_recv() {
            return Some(command);
        }
        let command = pending.overflow.pop_front();
        drop(pending);
        if command.is_some() {
            // Wakes callers waiting in `Sender::protocol_drained`.
            lifecycle.changed.notify_changed();
        }

        command
    }

    pub(crate) fn try_recv(&mut self) -> Result<PeerDriverCommand, mpsc::error::TryRecvError> {
        match self {
            Self::Legacy(receiver) => receiver.try_recv(),
            Self::Owned {
                commands,
                lifecycle,
            } => {
                if let Some(command) = Self::take_lifecycle(lifecycle) {
                    return Ok(command);
                }

                commands
                    .try_recv()
                    .or_else(|error| Self::take_overflow(commands, lifecycle).ok_or(error))
            }
        }
    }

    pub(crate) async fn recv(&mut self) -> Option<PeerDriverCommand> {
        futures::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// A pending poll leaves every source registered with `cx`.
    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<PeerDriverCommand>> {
        match self {
            Self::Legacy(receiver) => receiver.poll_recv(cx),
            Self::Owned {
                commands,
                lifecycle,
            } => {
                lifecycle.receive.register(cx.waker());
                if let Some(command) = Self::take_lifecycle(lifecycle) {
                    return std::task::Poll::Ready(Some(command));
                }

                match commands.poll_recv(cx) {
                    std::task::Poll::Ready(Some(command)) => std::task::Poll::Ready(Some(command)),
                    closed_or_pending => Self::take_overflow(commands, lifecycle)
                        .map_or(closed_or_pending, |command| {
                            std::task::Poll::Ready(Some(command))
                        }),
                }
            }
        }
    }

    pub(crate) fn close(&mut self) {
        match self {
            Self::Legacy(receiver) => receiver.close(),
            Self::Owned {
                commands,
                lifecycle,
            } => {
                lifecycle.lock().closed = true;
                commands.close();
                lifecycle.changed.notify_changed();
                lifecycle.receive.wake();
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Legacy(receiver) => receiver.is_empty(),
            Self::Owned {
                commands,
                lifecycle,
            } => commands.is_empty() && !lifecycle.may_be_occupied(),
        }
    }

    pub(crate) fn release_consumed(&mut self) {
        if let Self::Owned { commands, .. } = self {
            commands.release_consumed();
        }
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.close();
        if let Self::Owned { lifecycle, .. } = self {
            let mut pending = lifecycle.lock();
            let activation = pending.activation.take();
            let close = pending.close.take();
            let overflow = std::mem::take(&mut pending.overflow);
            drop(pending);

            drop(activation);
            drop(close);
            drop(overflow);
        }
    }
}

impl From<mpsc::Sender<PeerDriverCommand>> for Sender {
    fn from(sender: mpsc::Sender<PeerDriverCommand>) -> Self {
        Self::Legacy(sender)
    }
}

impl From<mpsc::Receiver<PeerDriverCommand>> for Receiver {
    fn from(receiver: mpsc::Receiver<PeerDriverCommand>) -> Self {
        Self::Legacy(receiver)
    }
}

#[cfg(test)]
mod tests;
