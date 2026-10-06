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
    admitted: u64,
    notify_xpub: bool,
    notification: Option<omq_proto::Message>,
}

impl PeerEventDispatch {
    pub(super) fn blocked(&self) -> bool {
        self.pending.is_some() || self.needs_drain || self.notification.is_some()
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

    pub(super) fn for_xpub(notify_xpub: bool) -> Self {
        Self {
            notify_xpub,
            ..Self::default()
        }
    }

    pub(super) fn control_prefix(&self) -> u64 {
        self.admitted
    }

    pub(super) fn notification_pending(&self) -> bool {
        self.notification.is_some()
    }

    pub(super) fn control_ready(
        &self,
        output: &mut super::actor_output::PeerOutput,
        data_clear: bool,
    ) -> bool {
        self.has_pending()
            && (!self.notification_pending() || (data_clear && output.has_capacity()))
    }

    pub(super) fn send_notification(
        &mut self,
        output: &mut super::actor_output::PeerOutput,
        peer_id: u64,
        completion: &mut CompletionProgress,
    ) -> bool {
        if self.notification.is_none() {
            return true;
        }
        output.set_control_prefix(self.admitted);
        match output.try_send(peer_id, self.notification.take().unwrap(), true) {
            Ok(()) => {
                completion.note_event();
                true
            }
            Err(super::SendPipeError::Full(_)) => {
                unreachable!("single producer retained notification capacity")
            }
            Err(super::SendPipeError::Closed(_)) => false,
        }
    }

    pub(super) fn send_reserved(
        &mut self,
        permit: mpsc::Permit<'_, (u64, PeerEvent)>,
        peer_id: u64,
        completion: &mut CompletionProgress,
        output: &mut super::actor_output::PeerOutput,
    ) -> bool {
        let event = self.pending.take().expect("pending event admission guard");
        self.note_admission(&event);
        permit.send((peer_id, PeerEvent::Event(event)));
        completion.note_event();
        self.send_notification(output, peer_id, completion)
    }

    fn note_admission(&mut self, event: &Event) {
        self.admitted = self.admitted.wrapping_add(1);
        if matches!(event, Event::HandshakeSucceeded { .. }) {
            self.handshake_admitted = true;
        }
    }

    /// Empty checks avoid clocks or reservations on the ordinary message path.
    /// Event service is bounded by count, logical bytes, and elapsed time.
    #[expect(clippy::too_many_arguments)]
    pub(super) fn drive(
        &mut self,
        connection: &mut Connection,
        peer_out: &mpsc::Sender<(u64, PeerEvent)>,
        peer_id: u64,
        mut recv_direct: Option<&mut RecvSink>,
        completion: &mut CompletionProgress,
        output: &mut super::actor_output::PeerOutput,
        data_clear: bool,
    ) -> bool {
        if self.pending.is_some() || self.notification.is_some() {
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
            if self.notify_xpub {
                self.notification = xpub_notification(&event);
            }
            if self.notification.is_some() && (!data_clear || !output.has_capacity()) {
                self.pending = Some(event);
                return true;
            }
            let handshake = matches!(event, Event::HandshakeSucceeded { .. });
            match peer_out.try_send((peer_id, PeerEvent::Event(event))) {
                Ok(()) => {
                    self.admitted = self.admitted.wrapping_add(1);
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
            if !self.send_notification(output, peer_id, completion) {
                return false;
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

pub(crate) fn xpub_notification(event: &Event) -> Option<omq_proto::Message> {
    let (tag, prefix) = match event {
        Event::Command(Command::Subscribe(prefix)) => (0x01, prefix),
        Event::Command(Command::Cancel(prefix)) => (0x00, prefix),
        _ => return None,
    };
    let mut bytes = bytes::BytesMut::with_capacity(1 + prefix.len());
    bytes.extend_from_slice(&[tag]);
    bytes.extend_from_slice(prefix);
    Some(omq_proto::Message::single(bytes.freeze()))
}

// Logical service costs, not backing-allocation accounting. A single event's
// existing metadata parsing/copy cost still needs its own input limits.
pub(crate) fn event_work_bytes(event: &Event) -> usize {
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
