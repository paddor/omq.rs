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

use std::sync::{Arc, Mutex};

use rustc_hash::FxHashSet;

use bytes::Bytes;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::engine::{PeerDriverCommand, PeerDriverData, PeerDriverHandle};
use crate::transport::udp;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;

/// Joined-groups set, shared between the socket actor (mutated by
/// `apply_join`) and every UDP DISH listener task (read on each recv).
/// Lock hold time is one `HashSet::contains`; contention is negligible.
pub(crate) type JoinedGroups = Arc<Mutex<FxHashSet<Bytes>>>;

pub(crate) fn new_joined_groups() -> JoinedGroups {
    Arc::new(Mutex::new(FxHashSet::default()))
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
    recv_tx: std::sync::Arc<crate::socket::recv::SharedRecvPipe>,
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
                        let joined_now = {
                            let g = joined.lock().expect("joined_groups poisoned");
                            g.contains(&group)
                        };
                        if !joined_now {
                            continue;
                        }
                        let msg = Message::multipart([group, body]);
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

/// Spawn a RADIO sender task. Drains [`PeerDriverCommand`]s from `inbox_rx`
/// and emits each message as one UDP datagram. Skips messages that
/// don't match the RADIO `[group, body]` shape (any other shape is a
/// programming error; drop silently).
pub(crate) fn spawn_radio_sender(
    sock: UdpSocket,
    mut inbox_rx: mpsc::Receiver<PeerDriverCommand>,
    mut data_inbox_rx: mpsc::Receiver<PeerDriverData>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut pending: Option<Bytes> = None;
        let mut control_open = true;
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                cmd = inbox_rx.recv(), if control_open => match cmd {
                    Some(PeerDriverCommand::ActivateDataPlane | PeerDriverCommand::SendCommand(_)) => {}
                    Some(PeerDriverCommand::Close) => break,
                    None => control_open = false,
                },
                result = async {
                    sock.send(pending.as_ref().unwrap()).await
                }, if pending.is_some() => {
                    // Best-effort fire-and-forget; UDP errors
                    // (ECONNREFUSED, ENETUNREACH) are silently dropped.
                    let _ = result;
                    pending = None;
                }
                data = data_inbox_rx.recv(), if pending.is_none() => match data {
                    Some(PeerDriverData::SendMessage(m)) => {
                        if m.len() != 2 {
                            continue;
                        }
                        let group = m.part_bytes(0).unwrap_or_default();
                        let body = m.part_bytes(1).unwrap_or_default();
                        if let Ok(dgram) = udp::encode_datagram(&group, &body) {
                            pending = Some(dgram);
                        }
                    }
                    Some(PeerDriverData::SendEncoded(_)) => {}
                    None => break,
                },
            }
        }
    })
}

/// Build a [`PeerDriverHandle`] for a synthetic UDP RADIO peer. The handle's
/// inbox feeds the sender task; its cancellation token tears the task
/// down on `disconnect` / socket close.
pub(crate) fn fake_handle(
    inbox: mpsc::Sender<PeerDriverCommand>,
    data_inbox: mpsc::Sender<PeerDriverData>,
    cancel: CancellationToken,
) -> PeerDriverHandle {
    PeerDriverHandle {
        inbox,
        data_inbox,
        cancel,
        transmit_slot: None,
        direct_tcp_writer: None,
        send_pipe: None,
    }
}
