//! One bounded application-data lane per socket-owned peer driver.
//!
//! Codec control uses its own mailbox. XPUB notifications belong to the
//! application-data lane after their subscription state has been applied.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use fanring::teardown::Coordinated;
use omq_proto::Message;
use omq_proto::proto::Event;
use tokio::sync::mpsc;

use super::peer_completion::CompletionProgress;
use super::{PeerEvent, SendPipeError};

#[derive(Debug)]
pub(crate) struct ActorData {
    pub(crate) peer_id: u64,
    pub(crate) message: Message,
    pub(crate) notification: bool,
    pub(crate) control_prefix: u64,
}

pub(crate) type DataSender = fanring::mpsc::Sender<ActorData, Coordinated>;
pub(crate) type DataReceiver = fanring::mpsc::Receiver<ActorData, Coordinated>;
type LegacySender = mpsc::Sender<(u64, PeerEvent)>;
type Credit = mpsc::OwnedPermit<(u64, PeerEvent)>;

type CreditFuture =
    Pin<Box<dyn Future<Output = Result<Credit, mpsc::error::SendError<()>>> + Send>>;

pub(crate) enum PeerOutput {
    Actor {
        sender: DataSender,
        control_prefix: u64,
    },
    Legacy {
        sender: LegacySender,
        credit: Option<Credit>,
        wait: Option<CreditFuture>,
    },
}

impl std::fmt::Debug for PeerOutput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Actor { sender, .. } => formatter.debug_tuple("Actor").field(sender).finish(),
            Self::Legacy { sender, .. } => formatter.debug_tuple("Legacy").field(sender).finish(),
        }
    }
}

impl From<LegacySender> for PeerOutput {
    fn from(sender: LegacySender) -> Self {
        Self::Legacy {
            sender,
            credit: None,
            wait: None,
        }
    }
}

impl PeerOutput {
    pub(crate) fn legacy_sender(&self) -> Option<LegacySender> {
        match self {
            Self::Actor { .. } => None,
            Self::Legacy { sender, .. } => Some(sender.clone()),
        }
    }

    pub(crate) fn try_send(
        &mut self,
        peer_id: u64,
        message: Message,
        notification: bool,
    ) -> Result<(), SendPipeError> {
        match self {
            Self::Actor {
                sender,
                control_prefix,
            } => sender
                .try_send(ActorData {
                    peer_id,
                    message,
                    notification,
                    control_prefix: *control_prefix,
                })
                .map_err(|error| match error {
                    fanring::mpsc::TrySendError::Full(data) => SendPipeError::Full(data.message),
                    fanring::mpsc::TrySendError::Disconnected(data) => {
                        SendPipeError::Closed(data.message)
                    }
                }),
            Self::Legacy { sender, credit, .. } => {
                debug_assert!(
                    !notification,
                    "standalone drivers retain combined codec events"
                );
                let output = (peer_id, PeerEvent::Event(Event::Message(message)));

                let result = if let Some(permit) = credit.take() {
                    permit.send(output);
                    Ok(())
                } else {
                    sender.try_send(output)
                };

                result.map_err(|error| match error {
                    mpsc::error::TrySendError::Full((
                        _,
                        PeerEvent::Event(Event::Message(message)),
                    )) => SendPipeError::Full(message),
                    mpsc::error::TrySendError::Closed((
                        _,
                        PeerEvent::Event(Event::Message(message)),
                    )) => SendPipeError::Closed(message),
                    _ => unreachable!("application-data output"),
                })
            }
        }
    }

    pub(crate) fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), ()>> {
        match self {
            Self::Actor { sender, .. } => sender
                .poll_ready(context)
                .map(|result| result.map_err(|_| ())),
            Self::Legacy {
                sender,
                credit,
                wait,
            } => {
                if credit.is_some() {
                    return Poll::Ready(Ok(()));
                }

                let future = wait.get_or_insert_with(|| Box::pin(sender.clone().reserve_owned()));
                match future.as_mut().poll(context) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(result) => {
                        *wait = None;
                        Poll::Ready(result.map(|permit| *credit = Some(permit)).map_err(|_| ()))
                    }
                }
            }
        }
    }

    pub(crate) async fn ready(&mut self) -> Result<(), ()> {
        std::future::poll_fn(|context| self.poll_ready(context)).await
    }

    /// The driver is this lane's only producer. Capacity observed here cannot
    /// be consumed by a different caller before the immediately following push.
    /// Waits still use `poll_ready` to register the producer waker.
    pub(crate) fn has_capacity(&mut self) -> bool {
        match self {
            Self::Actor { sender, .. } => !sender.is_disconnected() && !sender.is_full(),
            Self::Legacy { .. } => true,
        }
    }

    pub(crate) fn actor(sender: DataSender) -> Self {
        Self::Actor {
            sender,
            control_prefix: 0,
        }
    }

    pub(crate) fn set_control_prefix(&mut self, prefix: u64) {
        if let Self::Actor { control_prefix, .. } = self {
            *control_prefix = prefix;
        }
    }
}

/// One peer driver's actor-bound output, with the peer id it stamps and the
/// completion that counts every admitted event.
pub(super) struct PeerOutlet<'c> {
    pub(super) output: PeerOutput,
    peer_id: u64,
    completion: &'c mut CompletionProgress,
}

impl<'c> PeerOutlet<'c> {
    pub(super) fn new(
        output: PeerOutput,
        peer_id: u64,
        completion: &'c mut CompletionProgress,
    ) -> Self {
        Self {
            output,
            peer_id,
            completion,
        }
    }

    /// Borrow by value, so receive paths keep one pointer hop per field.
    #[inline]
    pub(super) fn view(&mut self) -> OutletRef<'_> {
        OutletRef {
            output: &mut self.output,
            peer_id: self.peer_id,
            completion: self.completion,
        }
    }
}

/// A borrowed [`PeerOutlet`].
pub(super) struct OutletRef<'a> {
    pub(super) output: &'a mut PeerOutput,
    peer_id: u64,
    completion: &'a mut CompletionProgress,
}

#[cfg(test)]
impl<'a> OutletRef<'a> {
    pub(super) fn new(
        output: &'a mut PeerOutput,
        peer_id: u64,
        completion: &'a mut CompletionProgress,
    ) -> Self {
        Self {
            output,
            peer_id,
            completion,
        }
    }
}

impl OutletRef<'_> {
    /// Admit a message or XPUB notification; counts it once admitted.
    #[inline]
    pub(super) fn try_send(
        &mut self,
        message: Message,
        notification: bool,
    ) -> Result<(), SendPipeError> {
        self.output.try_send(self.peer_id, message, notification)?;
        self.completion.note_event();
        Ok(())
    }

    /// Admit a codec event into its reserved mailbox slot.
    pub(super) fn send_event(&mut self, permit: mpsc::Permit<'_, (u64, PeerEvent)>, event: Event) {
        permit.send((self.peer_id, PeerEvent::Event(event)));
        self.completion.note_event();
    }

    /// Admit a codec event if the mailbox has room; counts it once admitted.
    pub(super) fn try_send_event(
        &mut self,
        mailbox: &mpsc::Sender<(u64, PeerEvent)>,
        event: Event,
    ) -> Result<(), mpsc::error::TrySendError<(u64, PeerEvent)>> {
        mailbox.try_send((self.peer_id, PeerEvent::Event(event)))?;
        self.completion.note_event();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
