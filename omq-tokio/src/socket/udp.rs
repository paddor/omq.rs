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

use std::sync::Arc;

use arc_swap::ArcSwap;
use rustc_hash::FxHashSet;

use bytes::Bytes;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::engine::{ActorPeerDriverHandle, PeerDriverCommand, PeerDriverData};
use crate::transport::udp;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;

/// Joined-groups set, shared between the socket actor (mutated by
/// `apply_join`) and UDP/DART DISH listeners. The actor publishes a new
/// immutable set before acknowledging join/leave; readers never lock it.
pub(crate) type JoinedGroups = Arc<ArcSwap<FxHashSet<Bytes>>>;

pub(crate) fn new_joined_groups() -> JoinedGroups {
    Arc::new(ArcSwap::from_pointee(FxHashSet::default()))
}

/// One bound UDP listener (DISH side). Cancelling the token tears down
/// the listener task, which closes the underlying socket.
pub(crate) struct UdpListenerEntry {
    pub endpoint: Endpoint,
    pub cancel: CancellationToken,
    pub task: JoinHandle<()>,
}

impl Drop for UdpListenerEntry {
    /// A detached listener would keep the UDP port bound after unbind or close.
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

/// One outbound UDP "connection" (RADIO side). Holds the synthetic
/// peer id under which the `SendStrategy` registered the sender, so
/// teardown can call `connection_removed`.
pub(crate) struct UdpDialerEntry {
    pub endpoint: Endpoint,
    pub cancel: CancellationToken,
    pub peer_id: u64,
    pub task: JoinHandle<()>,
}

impl Drop for UdpDialerEntry {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

/// Spawn a DISH listener task. Reads datagrams, applies the local
/// joined-groups filter (RFC 48),
/// pushes matching `[group, body]` messages straight onto `recv_tx`.
pub(crate) fn spawn_dish_listener(
    sock: UdpSocket,
    recv_tx: std::sync::Arc<crate::socket::recv::SharedRecvPipe>,
    joined: JoinedGroups,
    cancel: CancellationToken,
    pool: Option<crate::PayloadPool>,
    io_pool: &crate::context::IoPoolHandle,
    assignment: crate::context::IoThreadLease,
) -> JoinHandle<()> {
    let index = assignment.index();
    let network = io_pool.network_on(index);
    io_pool.spawn_on(index, async move {
        let _assignment = assignment;
        let Ok(sock) = sock.into_std().and_then(UdpSocket::from_std) else { return; };
        let mut ingress = network.ingress();
        let mut buf = vec![0u8; udp::MAX_DATAGRAM_SIZE];
        let mut budget = omq_proto::flow::DrainBudget::new(64, 1024 * 1024);
        loop {
            if budget.exhausted() {
                budget.reset();
                tokio::task::yield_now().await;
            }
            tokio::select! {
                () = cancel.cancelled() => break,
                res = async {
                    // Peeking preserves a complete datagram while waiting for
                    // credit. This socket has exactly one receive owner.
                    if let Some(gate) = &mut ingress {
                        let (length, _) = sock.peek_from(&mut buf).await?;
                        std::future::poll_fn(|cx| {
                            let n = std::task::ready!(gate.poll(cx, length, length));
                            match sock.try_recv_from(&mut buf) {
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    gate.finish(n, 0);
                                    match sock.poll_recv_ready(cx) {
                                        std::task::Poll::Ready(Ok(())) => cx.waker().wake_by_ref(),
                                        std::task::Poll::Ready(Err(e)) => return std::task::Poll::Ready(Err(e)),
                                        std::task::Poll::Pending => {}
                                    }
                                    std::task::Poll::Pending
                                }
                                result => {
                                    gate.finish(n, result.as_ref().map_or(0, |(n, _)| *n));
                                    std::task::Poll::Ready(result)
                                }
                            }
                        }).await
                    } else { sock.recv_from(&mut buf).await }
                } => match res {
                    Ok((n, _from)) => {
                        if let Some(gate) = &mut ingress { gate.cancel_wait(); }
                        let _ = budget.account(n);
                        let Some((group, body)) = udp::split_datagram(&buf[..n]) else {
                            continue;
                        };
                        if !joined.load().contains(group) {
                            continue;
                        }
                        let msg = if let Some(pool) = &pool {
                            let group = pool.payload(group.len(), |bytes| bytes.copy_from_slice(group));
                            let body = pool.payload(body.len(), |bytes| bytes.copy_from_slice(body));
                            let (Ok(group), Ok(body)) = (group, body) else { continue; };
                            Message::multipart_payloads([group, body])
                        } else {
                            Message::multipart([Bytes::copy_from_slice(group), Bytes::copy_from_slice(body)])
                        };
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
    mut inbox_rx: crate::engine::control_inbox::Receiver,
    mut data_inbox_rx: crate::engine::data_inbox::Receiver,
    cancel: CancellationToken,
    io_pool: &crate::context::IoPoolHandle,
    assignment: crate::context::IoThreadLease,
) -> JoinHandle<()> {
    let index = assignment.index();
    let network = io_pool.network_on(index);
    io_pool.spawn_on(index, async move {
        let _assignment = assignment;
        let Ok(sock) = sock.into_std().and_then(UdpSocket::from_std) else { return; };
        let mut egress = network.egress();
        let mut pending: Option<Bytes> = None;
        let mut control_open = true;
        let mut closing = false;
        let mut deadline: Option<std::time::Instant> = None;
        let mut budget = omq_proto::flow::DrainBudget::new(64, 64 * 1024);
        loop {
            if budget.exhausted() {
                inbox_rx.release_consumed();
                data_inbox_rx.release_consumed();
                budget.reset();
                tokio::task::yield_now().await;
            }
            let _ = budget.account(0);
            if pending.is_none() && let Some(gate) = &mut egress { gate.cancel_wait(); }
            if closing && pending.is_none() && inbox_rx.is_empty() && data_inbox_rx.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                () = async { tokio::time::sleep_until(deadline.unwrap().into()).await; },
                    if deadline.is_some() => break,
                cmd = inbox_rx.recv(), if control_open => match cmd {
                    Some(PeerDriverCommand::ActivateDataPlane | PeerDriverCommand::SendCommand(_)) => {}
                    Some(PeerDriverCommand::Close | PeerDriverCommand::ActivateWithRecvSink(_)) => break,
                    Some(PeerDriverCommand::DrainAndClose { deadline: end }) => {
                        if !closing {
                            closing = true;
                            deadline = end;
                            data_inbox_rx.close();
                        }
                    }
                    None => control_open = false,
                },
                result = async {
                    let bytes = pending.as_ref().unwrap();
                    if let Some(gate) = &mut egress {
                        std::future::poll_fn(|cx| {
                            let n = std::task::ready!(gate.poll(cx, bytes.len(), bytes.len()));
                            match sock.try_send(bytes) {
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    gate.finish(n, 0);
                                    match sock.poll_send_ready(cx) {
                                        std::task::Poll::Ready(Ok(())) => cx.waker().wake_by_ref(),
                                        std::task::Poll::Ready(Err(e)) => return std::task::Poll::Ready(Err(e)),
                                        std::task::Poll::Pending => {}
                                    }
                                    std::task::Poll::Pending
                                }
                                result => {
                                    gate.finish(n, result.as_ref().map_or(0, |n| *n));
                                    std::task::Poll::Ready(result)
                                }
                            }
                        }).await
                    } else { sock.send(bytes).await }
                }, if pending.is_some() => {
                    // Best-effort fire-and-forget; UDP errors
                    // (ECONNREFUSED, ENETUNREACH) are silently dropped.
                    let _ = result;
                    pending = None;
                }
                data = data_inbox_rx.recv(), if pending.is_none() => match data {
                    Some(PeerDriverData::SendMessage(m)) => {
                        let _ = budget.account(m.byte_len());
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

/// Build a [`ActorPeerDriverHandle`] for a synthetic UDP RADIO peer. The handle's
/// inbox feeds the sender task; its cancellation token tears the task
/// down on `disconnect` / socket close.
pub(crate) fn fake_handle(
    inbox: crate::engine::control_inbox::Sender,
    data_inbox: crate::engine::data_inbox::Sender,
    cancel: CancellationToken,
) -> ActorPeerDriverHandle {
    ActorPeerDriverHandle {
        inbox,
        data_inbox,
        cancel,
        transmit_slot: None,
        direct_tcp_writer: None,
        send_pipe: None,
        inproc: None,
    }
}
