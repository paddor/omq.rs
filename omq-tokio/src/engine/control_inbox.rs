//! Actor-owned protocol lane. Bounded lifecycle slots keep activation and
//! shutdown reachable even when forwarding protocol commands is blocked.

use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

use super::{PeerDriverCommand, signal::StateSignal, single_inbox};

#[derive(Debug, Default)]
struct Pending {
    activation: Option<PeerDriverCommand>,
    close: Option<PeerDriverCommand>,
    closed: bool,
}

#[derive(Debug)]
pub(crate) struct Lifecycle {
    pending: Mutex<Pending>,
    changed: StateSignal,
    receive: futures::task::AtomicWaker,
    #[cfg(feature = "dart")]
    endpoint: std::sync::OnceLock<Arc<super::signal::DataSignal>>,
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
        let mut pending = lifecycle
            .pending
            .lock()
            .expect("control lifecycle poisoned");
        if pending.closed {
            return Err(mpsc::error::TrySendError::Closed(command));
        }
        let old = match command {
            PeerDriverCommand::SendCommand(_) => {
                // Serialize with lifecycle admission; one native producer.
                return commands.try_send(command);
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
        let mut pending = lifecycle
            .pending
            .lock()
            .expect("control lifecycle poisoned");
        if matches!(pending.close, Some(PeerDriverCommand::Close)) {
            return pending.close.take();
        }
        pending.activation.take().or_else(|| pending.close.take())
    }

    pub(crate) fn try_recv(&mut self) -> Result<PeerDriverCommand, mpsc::error::TryRecvError> {
        match self {
            Self::Legacy(receiver) => receiver.try_recv(),
            Self::Owned {
                commands,
                lifecycle,
            } => Self::take_lifecycle(lifecycle).map_or_else(|| commands.try_recv(), Ok),
        }
    }

    pub(crate) async fn recv(&mut self) -> Option<PeerDriverCommand> {
        futures::future::poll_fn(|cx| match self {
            Self::Legacy(receiver) => receiver.poll_recv(cx),
            Self::Owned {
                commands,
                lifecycle,
            } => {
                lifecycle.receive.register(cx.waker());
                if let Some(command) = Self::take_lifecycle(lifecycle) {
                    return std::task::Poll::Ready(Some(command));
                }
                commands.poll_recv(cx)
            }
        })
        .await
    }

    pub(crate) fn close(&mut self) {
        match self {
            Self::Legacy(receiver) => receiver.close(),
            Self::Owned {
                commands,
                lifecycle,
            } => {
                lifecycle
                    .pending
                    .lock()
                    .expect("control lifecycle poisoned")
                    .closed = true;
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
            } => {
                let pending = lifecycle
                    .pending
                    .lock()
                    .expect("control lifecycle poisoned");
                commands.is_empty() && pending.activation.is_none() && pending.close.is_none()
            }
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
            let mut pending = lifecycle
                .pending
                .lock()
                .expect("control lifecycle poisoned");
            let activation = pending.activation.take();
            let close = pending.close.take();
            drop(pending);
            drop(activation);
            drop(close);
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
mod tests {
    use super::*;
    use omq_proto::proto::Command;
    use std::time::{Duration, Instant};

    fn command(value: u8) -> PeerDriverCommand {
        PeerDriverCommand::SendCommand(Command::Subscribe(bytes::Bytes::from(vec![value])))
    }

    #[tokio::test]
    async fn activation_and_close_bypass_full_protocol_lane() {
        let (sender, mut receiver) = channel(1);
        sender.try_send(command(1)).unwrap();
        assert!(matches!(
            sender.try_send(command(2)),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        sender
            .try_send(PeerDriverCommand::ActivateDataPlane)
            .unwrap();
        assert!(matches!(
            receiver.recv_lifecycle().await,
            Some(PeerDriverCommand::ActivateDataPlane)
        ));
        let copy = sender.clone();
        let waiting = tokio::spawn(async move { copy.send(command(3)).await });
        tokio::task::yield_now().await;
        sender.try_send(PeerDriverCommand::Close).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiting)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(matches!(
            receiver.recv_lifecycle().await,
            Some(PeerDriverCommand::Close)
        ));
    }

    #[tokio::test]
    async fn graceful_close_retains_accepted_protocol_fifo() {
        let (sender, mut receiver) = channel(2);
        sender.try_send(command(1)).unwrap();
        sender.try_send(command(2)).unwrap();
        sender
            .try_send(PeerDriverCommand::DrainAndClose {
                deadline: Some(Instant::now() + Duration::from_secs(1)),
            })
            .unwrap();
        assert!(matches!(
            receiver.recv().await,
            Some(PeerDriverCommand::DrainAndClose { .. })
        ));
        assert!(!receiver.is_empty());
        for expected in [1, 2] {
            let Some(PeerDriverCommand::SendCommand(Command::Subscribe(prefix))) =
                receiver.recv().await
            else {
                panic!("protocol command");
            };
            assert_eq!(prefix.as_ref(), &[expected]);
        }
        assert!(receiver.is_empty());
    }
}
