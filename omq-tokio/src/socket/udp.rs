//! UDP RADIO/DISH integration for the socket actor.
//!
//! UDP is connectionless - no ZMTP handshake, no per-peer identity, no
//! reconnect. This module sits beside [`super::actor::SocketDriver`]'s
//! stream-oriented machinery (TCP/IPC/inproc) and runs UDP-specific
//! listener / sender tasks. Per RFC 48:
//!
//! - **DISH** binds, receives, **filters locally** by joined groups
//!   (JOIN/LEAVE never go on the wire for UDP).
//! - **RADIO** connects, sends every datagram unfiltered (the receiver
//!   does the filter).

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use omq_proto::endpoint::Endpoint;
use crate::engine::{DriverCommand, DriverHandle};
use omq_proto::message::Message;
use crate::transport::udp;

/// Joined-groups set, shared between the socket actor (mutated by
/// `apply_join`) and every UDP DISH listener task (read on each recv).
/// Lock hold time is one `HashSet::contains`; contention is negligible.
pub(crate) type JoinedGroups = Arc<Mutex<HashSet<Bytes>>>;

pub(crate) fn new_joined_groups() -> JoinedGroups {
    Arc::new(Mutex::new(HashSet::new()))
}

/// One bound UDP listener (DISH side). Cancelling the token tears down
/// the listener task, which closes the underlying socket.
pub(crate) struct UdpListenerEntry {
    pub endpoint: Endpoint,
    pub cancel: CancellationToken,
    pub _task: JoinHandle<()>,
}

/// One outbound UDP "connection" (RADIO side). Holds the synthetic
/// peer id under which the `SendStrategy` registered the sender, so
/// teardown can call `connection_removed`.
pub(crate) struct UdpDialerEntry {
    pub endpoint: Endpoint,
    pub cancel: CancellationToken,
    pub peer_id: u64,
    pub _task: JoinHandle<()>,
}

/// Spawn a DISH listener task. Reads datagrams, applies the local
/// joined-groups filter (RFC 48),
/// pushes matching `[group, body]` messages straight onto `recv_tx`.
pub(crate) fn spawn_dish_listener(
    sock: UdpSocket,
    recv_tx: async_channel::Sender<Message>,
    joined: JoinedGroups,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; udp::MAX_DATAGRAM_SIZE];
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                res = sock.recv_from(&mut buf) => match res {
                    Ok((n, _from)) => {
                        let Some((group, body)) = udp::decode_datagram(&buf[..n]) else {
                            continue;
                        };
                        // Local group filter - UDP DISH owns this; no
                        // JOIN reaches RADIO over a connectionless link.
                        let joined_now = {
                            let g = joined.lock().expect("joined_groups poisoned");
                            g.contains(&group)
                        };
                        if !joined_now {
                            continue;
                        }
                        let mut msg = Message::new();
                        msg.push_part(group);
                        msg.push_part(body);
                        if recv_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    })
}

/// Spawn a RADIO sender task. Drains [`DriverCommand`]s from `inbox_rx`
/// and emits each message as one UDP datagram. Skips messages that
/// don't match the RADIO `[group, body]` shape (any other shape is a
/// programming error; drop silently).
pub(crate) fn spawn_radio_sender(
    sock: UdpSocket,
    mut inbox_rx: mpsc::Receiver<DriverCommand>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                cmd = inbox_rx.recv() => match cmd {
                    Some(DriverCommand::SendMessage(m)) => {
                        let parts = m.parts();
                        if parts.len() != 2 {
                            continue;
                        }
                        let group = parts[0].coalesce();
                        let body = parts[1].coalesce();
                        if let Ok(dgram) = udp::encode_datagram(&group, &body) {
                            // Best-effort fire-and-forget; UDP errors
                            // (ECONNREFUSED, ENETUNREACH) are silently
                            // dropped.
                            let _ = sock.send(&dgram).await;
                        }
                    }
                    // SendCommand on UDP is meaningless (no JOIN/LEAVE
                    // exchange on the wire); discard.
                    Some(DriverCommand::SendCommand(_)) => {},
                    Some(DriverCommand::Close) | None => break,
                }
            }
        }
    })
}

/// Build a [`DriverHandle`] for a synthetic UDP RADIO peer. The handle's
/// inbox feeds the sender task; its cancellation token tears the task
/// down on `disconnect` / socket close.
pub(crate) fn fake_handle(
    inbox: mpsc::Sender<DriverCommand>,
    cancel: CancellationToken,
) -> DriverHandle {
    DriverHandle { inbox, cancel }
}
