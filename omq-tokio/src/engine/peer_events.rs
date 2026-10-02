//! Bounded codec-event admission into the socket actor mailbox.

use std::mem::size_of;
use std::time::{Duration, Instant};

use omq_proto::flow::DrainBudget;
use omq_proto::proto::command::PeerProperties;
use omq_proto::proto::{Command, Connection, Event};
use tokio::sync::mpsc;

use super::driver::PeerEvent;
use super::peer_completion::CompletionProgress;
use super::recv_sink::RecvSink;

/// Retain one popped event until its actual mailbox permit is available.
/// Existing codec events precede decoded-message admission. Pausing input
/// preserves that order while local commands and writer progress stay active.
#[derive(Debug, Default)]
pub(super) struct PeerEventDispatch {
    pending: Option<Event>,
    needs_drain: bool,
    handshake_admitted: bool,
}

impl PeerEventDispatch {
    pub(super) fn blocked(&self) -> bool {
        self.pending.is_some() || self.needs_drain
    }

    pub(super) fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    pub(super) fn needs_drain(&self) -> bool {
        self.needs_drain
    }

    pub(super) fn handshake_admitted(&self) -> bool {
        self.handshake_admitted
    }

    pub(super) fn send_reserved(
        &mut self,
        permit: mpsc::Permit<'_, (u64, PeerEvent)>,
        peer_id: u64,
        completion: &mut CompletionProgress,
    ) {
        let event = self.pending.take().expect("pending event admission guard");
        self.note_admission(&event);
        permit.send((peer_id, PeerEvent::Event(event)));
        completion.note_event();
    }

    fn note_admission(&mut self, event: &Event) {
        if matches!(event, Event::HandshakeSucceeded { .. }) {
            self.handshake_admitted = true;
        }
    }

    /// Empty checks avoid clocks or reservations on the ordinary message path.
    /// Event service is bounded by count, logical bytes, and elapsed time.
    pub(super) fn drive(
        &mut self,
        connection: &mut Connection,
        peer_out: &mpsc::Sender<(u64, PeerEvent)>,
        peer_id: u64,
        mut recv_direct: Option<&mut RecvSink>,
        completion: &mut CompletionProgress,
    ) -> bool {
        if self.pending.is_some() {
            return true;
        }
        self.needs_drain = false;
        let Some(mut event) = connection.poll_event() else {
            return true;
        };
        let mut budget = DrainBudget::new(64, 64 * 1024);
        let started = Instant::now();
        loop {
            if let Event::HandshakeSucceeded {
                ref peer_properties,
                ..
            } = event
                && let Some(sink) = recv_direct.as_deref_mut()
            {
                sink.set_peer_properties(peer_properties.clone());
            }
            let bytes = event_work_bytes(&event);
            let handshake = matches!(event, Event::HandshakeSucceeded { .. });
            match peer_out.try_send((peer_id, PeerEvent::Event(event))) {
                Ok(()) => {
                    self.handshake_admitted |= handshake;
                    completion.note_event();
                }
                Err(mpsc::error::TrySendError::Full((_, PeerEvent::Event(event)))) => {
                    self.pending = Some(event);
                    return true;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return false,
                Err(mpsc::error::TrySendError::Full(_)) => unreachable!("codec event admission"),
            }
            if !budget.account(bytes) || started.elapsed() >= Duration::from_millis(1) {
                self.needs_drain = true;
                return true;
            }
            let Some(next) = connection.poll_event() else {
                return true;
            };
            event = next;
        }
    }
}

// Logical service costs, not backing-allocation accounting. A single event's
// existing metadata parsing/copy cost still needs its own input limits.
fn event_work_bytes(event: &Event) -> usize {
    let body = match event {
        Event::HandshakeSucceeded {
            peer_properties, ..
        } => properties_work_bytes(peer_properties),
        Event::Message(message) => message.byte_len(),
        Event::Command(command) => match command {
            Command::Ready(properties) => properties_work_bytes(properties),
            Command::Subscribe(bytes)
            | Command::Cancel(bytes)
            | Command::Join(bytes)
            | Command::Leave(bytes) => bytes.len(),
            Command::Ping { context, .. } | Command::Pong { context } => context.len(),
            Command::Error { reason } => reason.len(),
            Command::Unknown { name, body } => name.len().saturating_add(body.len()),
            _ => usize::MAX,
        },
    };
    size_of::<Event>().saturating_add(body)
}

fn properties_work_bytes(properties: &PeerProperties) -> usize {
    properties.other.iter().fold(
        properties.identity.as_ref().map_or(0, bytes::Bytes::len),
        |total, (name, value)| {
            total
                .saturating_add(size_of::<(String, bytes::Bytes)>())
                .saturating_add(name.len())
                .saturating_add(value.len())
        },
    )
}
