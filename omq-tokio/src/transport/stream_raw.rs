//! Raw TCP driver for STREAM sockets (tokio backend).
//!
//! No ZMTP greeting, no frame encoding: reads raw bytes from the TCP
//! connection and delivers them through the peer-out channel. Outbound
//! messages arrive through the data inbox. An empty data frame closes the
//! connection.

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use omq_proto::message::Message;
use omq_proto::proto::Event as ZmtpEvent;

use crate::engine::{PeerDriverCommand, PeerDriverData, PeerDriverHandle, PeerEvent};
use crate::socket::dispatch::AnyStream;

pub(crate) fn spawn(
    stream: AnyStream,
    peer_id: u64,
    peer_out_tx: mpsc::Sender<(u64, PeerEvent)>,
    cancel: &CancellationToken,
) -> PeerDriverHandle {
    let (inbox_tx, mut inbox_rx) = mpsc::channel::<PeerDriverCommand>(64);
    let (data_inbox_tx, mut data_inbox_rx) = mpsc::channel::<PeerDriverData>(64);
    let child_cancel = cancel.child_token();
    let handle_cancel = child_cancel.clone();
    tokio::spawn(async move {
        let (mut reader, mut writer) = stream.split(false);
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
        let mut pending: Option<Bytes> = None;
        let mut pending_offset = 0usize;
        loop {
            tokio::select! {
                biased;
                () = child_cancel.cancelled() => break,
                cmd = inbox_rx.recv() => match cmd {
                    Some(PeerDriverCommand::ActivateDataPlane | PeerDriverCommand::SendCommand(_)) => {}
                    Some(PeerDriverCommand::Close) | None => break,
                },
                written = async {
                    let data = pending.as_ref().unwrap();
                    writer.write(&data[pending_offset..]).await
                }, if pending.is_some() => match written {
                    Ok(0) | Err(_) => break,
                    Ok(written) => {
                        pending_offset += written;
                        if pending_offset == pending.as_ref().unwrap().len() {
                            pending = None;
                            pending_offset = 0;
                        }
                    }
                },
                n = reader.read(&mut buf) => {
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
                data = data_inbox_rx.recv(), if pending.is_none() => {
                    match data {
                        Some(PeerDriverData::SendMessage(mut msg)) => {
                            let data = msg.pop_front().unwrap_or_default();
                            if data.is_empty() { break; }
                            pending = Some(data);
                        }
                        Some(PeerDriverData::SendEncoded(_)) => {}
                        None => break,
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
        data_inbox: data_inbox_tx,
        cancel: handle_cancel,
        transmit_slot: None,
        direct_tcp_writer: None,
        send_pipe: None,
    }
}
