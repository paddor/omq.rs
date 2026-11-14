//! Bounded codec-event admission into the socket actor mailbox.

use std::mem::size_of;
use std::sync::Arc;
use std::time::{Duration, Instant};

use omq_proto::flow::DrainBudget;
use omq_proto::proto::command::PeerProperties;
use omq_proto::proto::{Command, Connection, Event};
use omq_proto::subscription::SubscriptionAdmission;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use super::actor_output::OutletRef;
use super::driver::PeerEvent;
use super::recv_sink::RecvSink;
use super::subscription_ledger::SubscriptionLedger;
use crate::socket::monitor::{MonitorEvent, MonitorPublisher};

/// Retain one popped event until its actual mailbox permit is available.
/// Existing codec events precede decoded-message admission. Pausing input
/// preserves that order while local commands and writer progress stay active.
#[derive(Debug, Default)]
pub(super) struct PeerEventDispatch {
    pending: Option<Event>,
    needs_drain: bool,
    handshake_admitted: bool,
    admitted: u64,
    notification: Option<omq_proto::Message>,
    subscriptions: SubscriptionGate,
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

    pub(super) fn new(subscriptions: SubscriptionGate) -> Self {
        Self {
            subscriptions,
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

    pub(super) fn send_notification(&mut self, outlet: &mut OutletRef<'_>) -> bool {
        if self.notification.is_none() {
            return true;
        }
        outlet.output.set_control_prefix(self.admitted);
        match outlet.try_send(self.notification.take().unwrap(), true) {
            Ok(()) => true,
            Err(super::SendPipeError::Full(_)) => {
                unreachable!("single producer retained notification capacity")
            }
            Err(super::SendPipeError::Closed(_)) => false,
            #[cfg(feature = "dart")]
            Err(super::SendPipeError::Invalid(_)) => {
                unreachable!("receive output has no transport send validator")
            }
        }
    }

    pub(super) fn send_reserved(
        &mut self,
        permit: mpsc::Permit<'_, (u64, PeerEvent)>,
        mut outlet: OutletRef<'_>,
    ) -> bool {
        let event = self.pending.take().expect("pending event admission guard");
        self.note_admission(is_handshake(&event));
        outlet.send_event(permit, event);
        self.send_notification(&mut outlet)
    }

    fn note_admission(&mut self, handshake: bool) {
        self.admitted = self.admitted.wrapping_add(1);
        self.handshake_admitted |= handshake;
    }

    /// Empty checks avoid clocks or reservations on the ordinary message path.
    /// Event service is bounded by count, logical bytes, and elapsed time.
    /// Returns `false` once the actor mailbox is closed.
    pub(super) fn drive(
        &mut self,
        connection: &mut Connection,
        route: EventRoute<'_>,
        mut recv_direct: Option<&mut RecvSink>,
        data_clear: bool,
    ) -> bool {
        // A retained event or notification must go first; the select arm
        // owns its permit wait.
        if self.pending.is_some() || self.notification.is_some() {
            return true;
        }
        self.needs_drain = false;

        let Some(mut event) = connection.poll_event() else {
            return true;
        };
        let EventRoute {
            mailbox,
            mut outlet,
        } = route;
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
            self.subscriptions.observe(&event);

            let bytes = event_work_bytes(&event);
            // A rejected SUBSCRIBE or JOIN still costs budget but never
            // reaches the actor or XPUB receive.
            if self.subscriptions.admits(&event) {
                self.notification = self.subscriptions.notification(&event);
                // The XPUB notification rides the data output right after its
                // event, so both wait together when that output has no room.
                if self.notification.is_some() && (!data_clear || !outlet.output.has_capacity()) {
                    self.pending = Some(event);
                    return true;
                }

                let handshake = is_handshake(&event);
                match outlet.try_send_event(mailbox, event) {
                    Ok(()) => self.note_admission(handshake),
                    Err(TrySendError::Full((_, PeerEvent::Event(event)))) => {
                        self.pending = Some(event);
                        return true;
                    }
                    Err(TrySendError::Closed(_)) => return false,
                    Err(TrySendError::Full(_)) => unreachable!("codec event admission"),
                }
                if !self.send_notification(&mut outlet) {
                    return false;
                }
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

/// Where one peer's admitted codec events go, and who accounts for them.
pub(super) struct EventRoute<'a> {
    pub(super) mailbox: &'a mpsc::Sender<(u64, PeerEvent)>,
    pub(super) outlet: OutletRef<'a>,
}

fn is_handshake(event: &Event) -> bool {
    matches!(event, Event::HandshakeSucceeded { .. })
}

/// Peer SUBSCRIBE handling for one connection: PUB/XPUB admission and XPUB
/// receive notifications. Remembers the peer's READY properties so the
/// admission callback can see who is asking.
#[derive(Debug, Default)]
pub(crate) struct SubscriptionGate {
    notify_xpub: bool,
    admission: Option<SubscriptionAdmission>,
    /// What this peer holds, when the socket limits it.
    ledger: Option<Arc<SubscriptionLedger>>,
    /// Where rejections are reported, with this connection's id.
    report: Option<(MonitorPublisher, u64)>,
    peer: Option<Arc<PeerProperties>>,
}

impl SubscriptionGate {
    pub(crate) fn new(
        notify_xpub: bool,
        admission: Option<SubscriptionAdmission>,
        ledger: Option<Arc<SubscriptionLedger>>,
        monitor: &MonitorPublisher,
        connection_id: u64,
    ) -> Self {
        let report =
            (admission.is_some() || ledger.is_some()).then(|| (monitor.clone(), connection_id));
        Self {
            notify_xpub,
            admission,
            ledger,
            report,
            peer: None,
        }
    }

    /// Keep the peer properties from the handshake for later SUBSCRIBEs.
    pub(crate) fn observe(&mut self, event: &Event) {
        if let Event::HandshakeSucceeded {
            peer_properties, ..
        } = event
            && self.admission.is_some()
        {
            self.peer = Some(peer_properties.clone());
        }
    }

    /// False for a SUBSCRIBE or JOIN that is not registered: the admission
    /// callback rejects it, or the peer holds as many as the socket allows.
    /// Monitors see `SubscribeRejected` or `JoinRejected`. CANCEL and LEAVE
    /// release what the peer held.
    pub(crate) fn admits(&self, event: &Event) -> bool {
        let Event::Command(command) = event else {
            return true;
        };
        match command {
            Command::Subscribe(prefix) => self.admits_subscribe(prefix),
            Command::Join(group) => self.admits_join(group),
            Command::Cancel(prefix) => {
                if let Some(ledger) = &self.ledger {
                    ledger.cancel(prefix);
                }
                true
            }
            Command::Leave(group) => {
                if let Some(ledger) = &self.ledger {
                    ledger.leave(group);
                }
                true
            }
            _ => true,
        }
    }

    /// The callback decides first, so a rejected prefix takes no place.
    /// Fails closed if the callback is set and no handshake was observed.
    fn admits_subscribe(&self, prefix: &bytes::Bytes) -> bool {
        let admitted = self.admission.as_ref().is_none_or(|admission| {
            self.peer
                .as_deref()
                .is_some_and(|peer| admission.admits(prefix, peer))
        }) && self
            .ledger
            .as_ref()
            .is_none_or(|ledger| ledger.subscribe(prefix));
        if !admitted && let Some((monitor, connection_id)) = &self.report {
            monitor.publish(MonitorEvent::SubscribeRejected {
                connection_id: *connection_id,
                prefix: prefix.clone(),
            });
        }
        admitted
    }

    fn admits_join(&self, group: &bytes::Bytes) -> bool {
        let admitted = self.ledger.as_ref().is_none_or(|ledger| ledger.join(group));
        if !admitted && let Some((monitor, connection_id)) = &self.report {
            monitor.publish(MonitorEvent::JoinRejected {
                connection_id: *connection_id,
                group: group.clone(),
            });
        }
        admitted
    }

    /// XPUB surfaces admitted SUBSCRIBE and CANCEL commands as messages.
    pub(crate) fn notification(&self, event: &Event) -> Option<omq_proto::Message> {
        if self.notify_xpub {
            xpub_notification(event)
        } else {
            None
        }
    }
}

fn xpub_notification(event: &Event) -> Option<omq_proto::Message> {
    let (tag, prefix) = match event {
        Event::Command(Command::Subscribe(prefix)) => (0x01, prefix),
        Event::Command(Command::Cancel(prefix)) => (0x00, prefix),
        _ => return None,
    };
    Some(xpub_message(tag, prefix))
}

/// The message XPUB surfaces for a peer that stopped subscribing to
/// `prefix`.
pub(crate) fn xpub_unsubscribe(prefix: &[u8]) -> omq_proto::Message {
    xpub_message(0x00, prefix)
}

fn xpub_message(tag: u8, prefix: &[u8]) -> omq_proto::Message {
    let mut bytes = bytes::BytesMut::with_capacity(1 + prefix.len());
    bytes.extend_from_slice(&[tag]);
    bytes.extend_from_slice(prefix);
    omq_proto::Message::single(bytes.freeze())
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
