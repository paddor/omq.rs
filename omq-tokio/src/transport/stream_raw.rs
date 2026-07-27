//! Raw TCP driver for STREAM sockets (tokio backend).
//!
//! No ZMTP greeting, no frame encoding: reads raw bytes from the TCP
//! connection and delivers them through the peer-out channel. Outbound
//! messages arrive through the inbox. An empty data frame closes the
//! connection.

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use omq_proto::message::Message;
use omq_proto::proto::Event as ZmtpEvent;

use crate::engine::{PeerDriverCommand, PeerDriverHandle, PeerEvent};
use crate::socket::dispatch::AnyStream;

pub(crate) fn spawn(
    mut stream: AnyStream,
    peer_id: u64,
    peer_out_tx: mpsc::Sender<(u64, PeerEvent)>,
    cancel: &CancellationToken,
) -> PeerDriverHandle {
    let (inbox_tx, mut inbox_rx) = mpsc::channel::<PeerDriverCommand>(64);
    let child_cancel = cancel.child_token();
    let handle_cancel = child_cancel.clone();
    tokio::spawn(async move {
        // Connect notification.
        let notif = ZmtpEvent::Message(Message::single(Bytes::new()));
        if peer_out_tx
            .send((peer_id, PeerEvent::Event(notif)))
            .await
            .is_err()
        {
            return;
        }

        let mut buf = vec![0u8; 64 * 1024];
        loop {
            tokio::select! {
                biased;
                () = child_cancel.cancelled() => break,
                n = stream.read(&mut buf) => {
                    match n {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            let data = Bytes::copy_from_slice(&buf[..n]);
                            let msg = Message::single(data);
                            let evt = PeerEvent::Event(ZmtpEvent::Message(msg));
                            if peer_out_tx.send((peer_id, evt)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                cmd = inbox_rx.recv() => {
                    match cmd {
                        Some(PeerDriverCommand::SendMessage(msg)) => {
                            let data = msg.part_bytes(0).unwrap_or_default();
                            if data.is_empty() {
                                break;
                            }
                            if stream.write_all(&data).await.is_err() {
                                break;
                            }
                        }
                        Some(PeerDriverCommand::Close) | None => break,
                        Some(
                            PeerDriverCommand::ActivateDataPlane
                            | PeerDriverCommand::SendEncoded(_)
                            | PeerDriverCommand::SendCommand(_),
                        ) => {}
                    }
                }
            }
        }

        // Disconnect notification.
        let notif = ZmtpEvent::Message(Message::single(Bytes::new()));
        let _ = peer_out_tx.send((peer_id, PeerEvent::Event(notif))).await;
        let _ = peer_out_tx
            .send((peer_id, PeerEvent::Closed { error: None }))
            .await;
    });

    PeerDriverHandle {
        inbox: inbox_tx,
        cancel: handle_cancel,
        transmit_slot: None,
        direct_tcp_writer: None,
        send_pipe: None,
    }
}
