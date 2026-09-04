use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::engine::signal::StateSignal;
use crate::engine::{PeerDriverHandle, SendPipeError, SendPipeProducer};
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    pipe: Arc<Mutex<Option<SendPipeProducer>>>,
    peer_ready: Arc<StateSignal>,
    closed: Arc<AtomicBool>,
}

impl Submitter {
    pub(crate) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        *self.pipe.lock().expect("exclusive pipe") = None;
        self.peer_ready.notify_changed();
    }

    pub(crate) async fn send(&self, mut msg: Message) -> Result<()> {
        loop {
            match self.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(omq_proto::error::TrySendError::Full(returned)) => msg = returned,
                Err(omq_proto::error::TrySendError::Error(e)) => return Err(e),
                Err(omq_proto::error::TrySendError::Closed) => return Err(Error::Closed),
            }

            let space = {
                let guard = self.pipe.lock().expect("exclusive pipe");
                guard.as_ref().map(SendPipeProducer::space_available)
            };

            if let Some(space) = space {
                let seen = space.generation();
                let notified = space.changed_after(seen);
                tokio::pin!(notified);
                match self.try_send(msg) {
                    Ok(()) => return Ok(()),
                    Err(omq_proto::error::TrySendError::Full(returned)) => msg = returned,
                    Err(omq_proto::error::TrySendError::Error(e)) => return Err(e),
                    Err(omq_proto::error::TrySendError::Closed) => return Err(Error::Closed),
                }
                notified.await;
                continue;
            }

            let seen = self.peer_ready.generation();
            let notified = self.peer_ready.changed_after(seen);
            tokio::pin!(notified);
            match self.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(omq_proto::error::TrySendError::Full(returned)) => msg = returned,
                Err(omq_proto::error::TrySendError::Error(e)) => return Err(e),
                Err(omq_proto::error::TrySendError::Closed) => return Err(Error::Closed),
            }
            notified.await;
        }
    }

    pub(crate) async fn wait_send_progress(&self) {
        let space = {
            let guard = self.pipe.lock().expect("exclusive pipe");
            guard.as_ref().map(SendPipeProducer::space_available)
        };

        if let Some(space) = space {
            let seen = space.generation();
            space.changed_after(seen).await;
        } else {
            let seen = self.peer_ready.generation();
            self.peer_ready.changed_after(seen).await;
        }
    }

    pub(crate) fn try_send(
        &self,
        msg: Message,
    ) -> core::result::Result<(), omq_proto::error::TrySendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(omq_proto::error::TrySendError::Closed);
        }
        let mut guard = self.pipe.lock().expect("exclusive pipe");
        match guard.as_mut() {
            Some(producer) => match producer.try_send(msg) {
                Ok(()) => Ok(()),
                Err(SendPipeError::Full(m)) => Err(omq_proto::error::TrySendError::Full(m)),
                Err(SendPipeError::Closed(m)) => {
                    *guard = None;
                    self.peer_ready.notify_changed();
                    Err(omq_proto::error::TrySendError::Full(m))
                }
            },
            None => Err(omq_proto::error::TrySendError::Full(msg)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ExclusiveSend {
    pipe: Arc<Mutex<Option<SendPipeProducer>>>,
    peer_ready: Arc<StateSignal>,
    closed: Arc<AtomicBool>,
}

impl ExclusiveSend {
    pub(crate) fn new() -> Self {
        Self {
            pipe: Arc::new(Mutex::new(None)),
            peer_ready: Arc::new(StateSignal::new()),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn submitter(&self) -> Submitter {
        Submitter {
            pipe: self.pipe.clone(),
            peer_ready: self.peer_ready.clone(),
            closed: self.closed.clone(),
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn connection_added(&mut self, _peer_id: u64, handle: PeerDriverHandle) {
        let send_pipe = handle
            .send_pipe
            .as_ref()
            .and_then(|pipe| pipe.lock().expect("exclusive send pipe").take());
        *self.pipe.lock().expect("exclusive pipe") = send_pipe;
        self.peer_ready.notify_changed();
    }

    pub(crate) fn connection_removed(&mut self, _peer_id: u64) {
        *self.pipe.lock().expect("exclusive pipe") = None;
        self.peer_ready.notify_changed();
    }

    pub(crate) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        *self.pipe.lock().expect("exclusive pipe") = None;
        self.peer_ready.notify_changed();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let guard = self.pipe.lock().expect("exclusive pipe");
        guard.as_ref().is_none_or(SendPipeProducer::is_empty)
    }
}

#[cfg(test)]
mod tests {
    use super::ExclusiveSend;
    use crate::engine::{PeerDriverHandle, send_pipe};
    use omq_proto::error::TrySendError;
    use omq_proto::message::Message;

    #[test]
    fn closed_peer_pipe_is_treated_as_unavailable() {
        let mut send = ExclusiveSend::new();
        let submitter = send.submitter();
        let (tx, rx) = send_pipe(1);
        drop(rx);

        send.connection_added(
            1,
            PeerDriverHandle {
                inbox: tokio::sync::mpsc::channel(1).0,
                data_inbox: tokio::sync::mpsc::channel(1).0,
                cancel: tokio_util::sync::CancellationToken::new(),
                transmit_slot: None,
                direct_tcp_writer: None,
                send_pipe: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(tx)))),
            },
        );

        let msg = Message::single("retry");
        match submitter.try_send(msg) {
            Err(TrySendError::Full(returned)) => {
                assert_eq!(returned, Message::single("retry"));
            }
            other => panic!("expected retryable full, got {other:?}"),
        }

        let retry = Message::single("retry");
        match submitter.try_send(retry) {
            Err(TrySendError::Full(returned)) => {
                assert_eq!(returned, Message::single("retry"));
            }
            other => panic!("expected no-peer full, got {other:?}"),
        }
    }
}
