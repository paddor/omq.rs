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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;

    #[tokio::test]
    async fn a_full_driver_lane_cannot_take_another_drivers_capacity_or_starve_it() {
        let (registrar, mut receive): (DataSender, DataReceiver) =
            fanring::mpsc::channel_with_policy(256);
        let mut busy = PeerOutput::actor(registrar.try_register().unwrap());
        let mut cold = PeerOutput::actor(registrar.try_register().unwrap());
        for index in 0..256_u32 {
            busy.try_send(1, Message::single(index.to_le_bytes().to_vec()), false)
                .unwrap();
        }
        assert!(matches!(
            busy.try_send(1, Message::single("full"), false),
            Err(SendPipeError::Full(_))
        ));
        cold.try_send(2, Message::single("cold"), false).unwrap();
        let mut next = 0_u32;
        loop {
            let data = receive.recv_async().await.unwrap();
            if data.peer_id == 2 {
                break;
            }
            assert_eq!(
                data.message.part_slice(0),
                Some(next.to_le_bytes().as_slice())
            );
            busy.try_send(
                1,
                Message::single((256 + next).to_le_bytes().to_vec()),
                false,
            )
            .unwrap();
            next += 1;
            assert!(
                next < 256,
                "continuous producer starved an already ready driver lane"
            );
        }
    }

    #[tokio::test]
    async fn cancelled_capacity_wait_keeps_release_and_teardown_wakes() {
        let (sender, mut receive): (DataSender, DataReceiver) =
            fanring::mpsc::channel_with_policy(1);
        let mut output = PeerOutput::actor(sender);
        output.try_send(1, Message::single("first"), false).unwrap();
        assert!(output.ready().now_or_never().is_none());
        receive.recv_async().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), output.ready())
            .await
            .unwrap()
            .unwrap();
        output.try_send(1, Message::single("last"), false).unwrap();
        assert!(output.ready().now_or_never().is_none());
        drop(receive);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), output.ready())
                .await
                .unwrap()
                .is_err()
        );
        assert!(matches!(
            output.try_send(1, Message::single("closed"), false),
            Err(SendPipeError::Closed(_))
        ));
    }
}
