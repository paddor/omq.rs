//! Relay lanes for inproc commands and for messages that route through a
//! peer task.

use tokio::sync::mpsc;

use omq_proto::Message;
use omq_proto::inproc::InboundFrame;
use omq_proto::proto::Command;

use crate::engine::single_inbox;

/// One item on a command lane: a ZMTP command, or the settle mark that
/// follows the commands a caller waits on.
#[derive(Debug)]
pub(crate) enum RelayControl {
    Command(Command),
    Settle(u64),
}

/// Separate bounded command and message lanes for one inproc direction.
/// Copies share the same physical producers.
#[derive(Debug, Clone)]
pub struct RelaySender {
    pub(crate) control: single_inbox::Sender<RelayControl>,
    pub(crate) data: single_inbox::Sender<Message>,
}

/// Receive half of an inproc relay. Commands remain reachable when data fills.
#[derive(Debug)]
pub struct RelayReceiver {
    pub(crate) control: Box<single_inbox::Receiver<RelayControl>>,
    pub(crate) data: Box<single_inbox::Receiver<Message>>,
}

pub(crate) fn relay_channel(capacity: usize) -> (RelaySender, RelayReceiver) {
    let (control_tx, control_rx) = single_inbox::channel(capacity);
    let (data_tx, data_rx) = single_inbox::channel(capacity);

    (
        RelaySender {
            control: control_tx,
            data: data_tx,
        },
        RelayReceiver {
            control: Box::new(control_rx),
            data: Box::new(data_rx),
        },
    )
}

/// The command of a frame the lane returned.
fn returned_command(control: RelayControl) -> InboundFrame {
    match control {
        RelayControl::Command(command) => InboundFrame::Command(Box::new(command)),
        RelayControl::Settle(_) => unreachable!("frames carry no settle marks"),
    }
}

impl RelaySender {
    /// Enqueue a parsed frame into its bounded lane.
    ///
    /// # Errors
    /// Returns the frame when the partner has closed its receive half.
    pub async fn send(
        &self,
        frame: InboundFrame,
    ) -> core::result::Result<(), mpsc::error::SendError<InboundFrame>> {
        match frame {
            InboundFrame::Message(message) => self
                .data
                .send(message)
                .await
                .map_err(|error| mpsc::error::SendError(InboundFrame::Message(error.0))),
            InboundFrame::Command(command) => self
                .control
                .send(RelayControl::Command(*command))
                .await
                .map_err(|error| mpsc::error::SendError(returned_command(error.0))),
        }
    }

    /// Enqueue without waiting, preserving the frame on full or closed lanes.
    ///
    /// # Errors
    /// Returns the frame when its lane is full or the partner has closed.
    pub fn try_send(
        &self,
        frame: InboundFrame,
    ) -> core::result::Result<(), mpsc::error::TrySendError<InboundFrame>> {
        use mpsc::error::TrySendError::{Closed, Full};
        match frame {
            InboundFrame::Message(message) => {
                self.data.try_send(message).map_err(|error| match error {
                    Full(message) => Full(InboundFrame::Message(message)),
                    Closed(message) => Closed(InboundFrame::Message(message)),
                })
            }
            InboundFrame::Command(command) => self
                .control
                .try_send(RelayControl::Command(*command))
                .map_err(|error| match error {
                    Full(control) => Full(returned_command(control)),
                    Closed(control) => Closed(returned_command(control)),
                }),
        }
    }
}

impl RelayReceiver {
    /// Receive a parsed frame, preferring commands when both lanes are ready.
    /// Settle marks are skipped.
    pub async fn recv(&mut self) -> Option<InboundFrame> {
        loop {
            let control = tokio::select! {
                biased;
                control = self.control.recv() => match control {
                    Some(control) => control,
                    None => return self.data.recv().await.map(InboundFrame::Message),
                },
                message = self.data.recv() => match message {
                    Some(message) => return Some(InboundFrame::Message(message)),
                    None => self.control.recv().await?,
                },
            };

            if let RelayControl::Command(command) = control {
                return Some(InboundFrame::Command(Box::new(command)));
            }
        }
    }
}
