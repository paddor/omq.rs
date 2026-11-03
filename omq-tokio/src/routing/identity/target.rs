//! Per-peer send targets of identity routing.

use std::sync::Arc;

use crate::engine::send_pipe::SendPreparation;
use crate::engine::signal::StateSignal;
use crate::engine::{PeerDriverData, SendPipeError, SendPipeProducer};
use crate::routing::peer_outbound::PeerOutbound;
use omq_proto::message::Message;

/// Per-peer send target. Prefers `SendPipe` (zero-copy yring) when available;
/// falls back to the driver inbox for peers without a pipe (STREAM raw TCP).
#[derive(Debug)]
pub(super) enum PeerTarget {
    Pipe(SendPipeProducer),
    RepInproc(SendPipeProducer),
    Direct(PeerOutbound),
    Inbox(crate::engine::data_inbox::Sender),
}

impl PeerTarget {
    pub(super) fn is_alive(&self) -> bool {
        match self {
            Self::Pipe(pipe) | Self::RepInproc(pipe) => pipe.is_alive(),
            Self::Direct(target) => target.is_alive(),
            Self::Inbox(inbox) => !inbox.is_closed(),
        }
    }

    pub(super) fn watch_close(&self, progress: &Arc<StateSignal>) {
        match self {
            Self::Pipe(pipe) | Self::RepInproc(pipe) => pipe.watch_close(progress),
            Self::Direct(target) => target.watch_close(progress),
            Self::Inbox(inbox) => inbox.watch_close(progress),
        }
    }
    pub(super) fn outbound(
        &self,
        lanes: &crate::engine::data_inbox::SenderLanes,
    ) -> Option<PeerOutbound> {
        match self {
            Self::Direct(target) => Some(target.bind(lanes)),
            Self::Inbox(sender) => Some(PeerOutbound::Inbox(lanes.bind(sender))),
            Self::Pipe(_) | Self::RepInproc(_) => None,
        }
    }

    pub(super) fn try_send(
        &mut self,
        msg: Message,
        lanes: &crate::engine::data_inbox::SenderLanes,
    ) -> core::result::Result<(), SendPipeError> {
        self.try_send_prepared(msg, SendPreparation::Plain, lanes)
    }

    pub(super) fn try_send_prepared(
        &mut self,
        msg: Message,
        preparation: SendPreparation,
        lanes: &crate::engine::data_inbox::SenderLanes,
    ) -> core::result::Result<(), SendPipeError> {
        #[cfg(feature = "dart")]
        if let Self::Inbox(sender) = self {
            sender
                .validate_dart(&msg, matches!(preparation, SendPreparation::StripIdentity))
                .map_err(SendPipeError::Invalid)?;
        }
        match self {
            Self::Pipe(p) | Self::RepInproc(p) => p.try_send_prepared(msg, preparation),
            Self::Direct(target) => target.bind(lanes).try_send_prepared(msg, preparation),
            Self::Inbox(tx) => match lanes.bind(tx).try_reserve() {
                Ok(permit) => {
                    permit.send(PeerDriverData::SendMessage(preparation.prepare(msg)));
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {
                    Err(SendPipeError::Full(msg))
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
                    Err(SendPipeError::Closed(msg))
                }
            },
        }
    }

    pub(super) fn can_accept(&mut self, lanes: &crate::engine::data_inbox::SenderLanes) -> bool {
        match self {
            Self::Pipe(p) | Self::RepInproc(p) => p.can_accept(),
            Self::Direct(target) => target.bind(lanes).can_accept(),
            Self::Inbox(tx) => {
                let tx = lanes.bind(tx);
                !tx.is_closed() && tx.send_ready()
            }
        }
    }

    pub(super) fn space_available(&self) -> Option<Arc<StateSignal>> {
        match self {
            Self::Pipe(p) | Self::RepInproc(p) => Some(p.space_available()),
            Self::Direct(target) => target.space_available(),
            Self::Inbox(tx) => tx.space(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Pipe(p) | Self::RepInproc(p) => p.is_empty(),
            Self::Direct(target) => target.is_empty(),
            Self::Inbox(tx) => tx.is_empty(),
        }
    }
}
