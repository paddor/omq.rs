//! One reserved lifecycle result per materialized peer driver.
//!
//! Publication cannot wait on the actor's data mailbox. The result carries
//! the number of events actually admitted there, so the actor can retain
//! peer state until that ordered prefix has been handled.

use tokio::sync::oneshot;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamDisconnect {
    #[default]
    None,
    Pending,
    Queued,
}

#[derive(Debug)]
pub(crate) struct PeerCompletion {
    pub(crate) peer_id: u64,
    pub(crate) admitted_events: u64,
    pub(crate) error: Option<String>,
    pub(crate) stream_disconnect: StreamDisconnect,
}

#[derive(Debug, Default)]
pub(crate) struct CompletionProgress {
    sender: Option<oneshot::Sender<PeerCompletion>>,
    peer_id: u64,
    admitted_events: u64,
    stream_disconnect: StreamDisconnect,
}

impl CompletionProgress {
    pub(crate) fn reserve(peer_id: u64) -> (Self, oneshot::Receiver<PeerCompletion>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                sender: Some(sender),
                peer_id,
                admitted_events: 0,
                stream_disconnect: StreamDisconnect::None,
            },
            receiver,
        )
    }

    pub(crate) fn with_stream_disconnect(mut self) -> Self {
        self.stream_disconnect = StreamDisconnect::Pending;
        self
    }

    /// Called only after an event owns its actual mailbox slot. This counter
    /// belongs to the driver task; ordinary message admission needs no atomic.
    pub(crate) fn note_event(&mut self) {
        self.admitted_events = self.admitted_events.wrapping_add(1);
    }

    /// Return the error for standalone drivers using the legacy mailbox.
    pub(crate) fn complete(&mut self, error: Option<String>) -> Result<(), Option<String>> {
        let Some(sender) = self.sender.take() else {
            return Err(error);
        };
        let _ = sender.send(PeerCompletion {
            peer_id: self.peer_id,
            admitted_events: self.admitted_events,
            error,
            stream_disconnect: self.stream_disconnect,
        });
        Ok(())
    }
}

impl Drop for CompletionProgress {
    fn drop(&mut self) {
        // Covers task abortion, panic unwinding, and failed reactor migration.
        // The reservation exists before any of those can happen.
        if self.sender.is_some() {
            let _ = self.complete(Some("connection driver stopped before completion".into()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn abandoned_driver_retains_its_admitted_prefix() {
        let (mut progress, receiver) = CompletionProgress::reserve(7);
        progress.note_event();
        progress.note_event();
        drop(progress);
        let result = receiver.await.unwrap();
        assert_eq!(result.peer_id, 7);
        assert_eq!(result.admitted_events, 2);
        assert_eq!(
            result.error.as_deref(),
            Some("connection driver stopped before completion")
        );
    }
}
