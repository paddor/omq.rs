use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::engine::signal::StateSignal;
use crate::engine::{ActorPeerDriverHandle, SendPipeError, SendPipeProducer};
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

            self.wait_send_ready().await;
        }
    }

    /// Whether `try_send` would find the peer pipe with space. A closed
    /// socket is ready.
    pub(crate) fn send_ready(&self) -> bool {
        self.closed.load(Ordering::Acquire)
            || self
                .pipe
                .lock()
                .expect("exclusive pipe")
                .as_mut()
                .is_some_and(SendPipeProducer::can_accept)
    }

    pub(crate) async fn wait_send_ready(&self) {
        loop {
            let changed = {
                let guard = self.pipe.lock().expect("exclusive pipe");
                crate::engine::signal::any_changed(
                    std::iter::once(self.peer_ready.clone())
                        .chain(guard.iter().flat_map(SendPipeProducer::readiness_signals)),
                )
            };

            if self.send_ready() {
                return;
            }

            changed.wait().await;
        }
    }

    /// Wait until a send may succeed. A replaced pipe or a new peer counts,
    /// so this watches both and rechecks after registering.
    pub(crate) async fn wait_send_progress(&self) {
        self.wait_send_ready().await;
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
                #[cfg(feature = "dart")]
                Err(SendPipeError::Invalid(error)) => Err(omq_proto::TrySendError::Error(error)),
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
    peer_id: Option<u64>,
    pipe: Arc<Mutex<Option<SendPipeProducer>>>,
    peer_ready: Arc<StateSignal>,
    closed: Arc<AtomicBool>,
}

impl ExclusiveSend {
    pub(crate) fn new() -> Self {
        Self {
            peer_id: None,
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
    pub(crate) fn connection_added(&mut self, peer_id: u64, handle: ActorPeerDriverHandle) {
        let send_pipe = handle
            .routes
            .send_pipe
            .as_ref()
            .and_then(|pipe| pipe.lock().expect("exclusive send pipe").take());
        *self.pipe.lock().expect("exclusive pipe") = send_pipe;
        self.peer_id = Some(peer_id);
        self.peer_ready.notify_changed();
    }

    pub(crate) fn connection_removed(&mut self, peer_id: u64) {
        // A rejected handshake can close while the admitted peer stays live.
        if self.peer_id != Some(peer_id) {
            return;
        }

        self.peer_id = None;
        *self.pipe.lock().expect("exclusive pipe") = None;
        self.peer_ready.notify_changed();
    }

    pub(crate) fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        *self.pipe.lock().expect("exclusive pipe") = None;
        self.peer_ready.notify_changed();
    }

    pub(crate) fn stop_admission(&self) {
        self.closed.store(true, Ordering::Release);
        if let Some(pipe) = self.pipe.lock().expect("exclusive pipe").as_ref() {
            pipe.space_available().notify_changed();
        }

        self.peer_ready.notify_changed();
    }

    pub(crate) fn is_drained(&self) -> bool {
        let guard = self.pipe.lock().expect("exclusive pipe");
        guard.as_ref().is_none_or(SendPipeProducer::is_empty)
    }

    pub(crate) fn watch_close(&self, progress: &Arc<StateSignal>) {
        if let Some(pipe) = self.pipe.lock().expect("exclusive pipe").as_ref() {
            pipe.watch_close(progress);
        }
    }
}

#[cfg(test)]
mod tests;
