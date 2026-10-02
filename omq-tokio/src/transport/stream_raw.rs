//! Raw TCP driver for STREAM sockets (tokio backend).
//!
//! Reads retain one pending actor event while reverse writes, local commands,
//! cancellation, and linger remain selected. The actor delivers the terminal
//! empty receive after the admitted prefix, using the reserved closure slot.

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use omq_proto::message::Message;
use omq_proto::proto::Event as ZmtpEvent;

use crate::engine::driver::DriverStream;
use crate::engine::peer_completion::CompletionProgress;
use crate::engine::{PeerDriverCommand, PeerDriverData, PeerDriverHandle, PeerEvent};

pub(crate) fn spawn<T: DriverStream + Send + 'static>(
    stream: T,
    peer_id: u64,
    peer_out: mpsc::Sender<(u64, PeerEvent)>,
    cancel: &CancellationToken,
    completion: CompletionProgress,
) -> (PeerDriverHandle, tokio::task::JoinHandle<()>) {
    let (inbox_tx, inbox_rx) = mpsc::channel(64);
    let (data_inbox_tx, data_inbox_rx) = mpsc::channel(64);
    let child_cancel = cancel.child_token();
    let handle_cancel = child_cancel.clone();
    let mut completion = completion.with_stream_disconnect();
    let task = tokio::spawn(async move {
        run_body(
            stream,
            peer_id,
            peer_out,
            child_cancel,
            inbox_rx,
            data_inbox_rx,
            &mut completion,
        )
        .await;
        let _ = completion.complete(None);
    });
    (
        PeerDriverHandle {
            inbox: inbox_tx,
            data_inbox: data_inbox_tx,
            cancel: handle_cancel,
            transmit_slot: None,
            direct_tcp_writer: None,
            send_pipe: None,
        },
        task,
    )
}

async fn run_body<T: DriverStream>(
    stream: T,
    peer_id: u64,
    peer_out: mpsc::Sender<(u64, PeerEvent)>,
    cancel: CancellationToken,
    mut inbox: mpsc::Receiver<PeerDriverCommand>,
    mut data_inbox: mpsc::Receiver<PeerDriverData>,
    completion: &mut CompletionProgress,
) {
    let (mut reader, mut writer) = stream.split(false);
    let mut buf = vec![0u8; 64 * 1024];
    let mut pending: Option<Bytes> = None;
    let mut pending_offset = 0usize;
    let mut pending_event = Some(Message::single(Bytes::new()));
    let mut closing = false;
    let mut deadline: Option<std::time::Instant> = None;
    let credit = peer_out.reserve();
    tokio::pin!(credit);
    loop {
        if let Some(message) = pending_event.take() {
            match peer_out.try_send((peer_id, PeerEvent::Event(ZmtpEvent::Message(message)))) {
                Ok(()) => completion.note_event(),
                Err(mpsc::error::TrySendError::Full((
                    _,
                    PeerEvent::Event(ZmtpEvent::Message(message)),
                ))) => {
                    pending_event = Some(message);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return,
                Err(_) => unreachable!("STREAM event admission"),
            }
        }
        if closing && pending.is_none() && data_inbox.is_empty() {
            tokio::select! {
                biased;
                () = cancel.cancelled() => {},
                () = async { tokio::time::sleep_until(deadline.unwrap().into()).await; },
                    if deadline.is_some() => {},
                _ = writer.shutdown() => {},
            }
            return;
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = async { tokio::time::sleep_until(deadline.unwrap().into()).await; },
                if deadline.is_some() => return,
            cmd = inbox.recv() => match cmd {
                Some(PeerDriverCommand::ActivateDataPlane | PeerDriverCommand::SendCommand(_)) => {}
                Some(PeerDriverCommand::Close | PeerDriverCommand::ActivateWithRecvSink(_)) | None => return,
                Some(PeerDriverCommand::DrainAndClose { deadline: end }) => {
                    if !closing {
                        closing = true;
                        deadline = end;
                        data_inbox.close();
                    }
                }
            },
            permit = &mut credit, if pending_event.is_some() => {
                let Ok(permit) = permit else { return; };
                permit.send((peer_id, PeerEvent::Event(ZmtpEvent::Message(pending_event.take().unwrap()))));
                completion.note_event();
                credit.set(peer_out.reserve());
            },
            written = async {
                writer.write(&pending.as_ref().unwrap()[pending_offset..]).await
            }, if pending.is_some() => match written {
                Ok(0) | Err(_) => return,
                Ok(written) => {
                    pending_offset += written;
                    if pending_offset == pending.as_ref().unwrap().len() {
                        pending = None;
                        pending_offset = 0;
                    }
                }
            },
            n = reader.read(&mut buf), if !closing && pending_event.is_none() => {
                match n {
                    Ok(0) | Err(_) => return,
                    Ok(n) => pending_event = Some(Message::single(Bytes::copy_from_slice(&buf[..n]))),
                }
            },
            data = data_inbox.recv(), if pending.is_none() => {
                match data {
                    Some(PeerDriverData::SendMessage(mut message)) => {
                        let data = message.pop_front().unwrap_or_default();
                        if data.is_empty() { return; }
                        pending = Some(data);
                    }
                    Some(PeerDriverData::SendEncoded(_)) => {}
                    None => return,
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn stream_close_joins_without_reading_the_connect_notification() {
        let (stream, mut remote) = tokio::io::duplex(1024);
        let (events, mut event_inbox) = mpsc::channel(1);
        let (completion, finished) = CompletionProgress::reserve(7);
        let (handle, mut task) = spawn(stream, 7, events, &CancellationToken::new(), completion);
        handle.inbox.try_send(PeerDriverCommand::Close).unwrap();
        tokio::time::timeout(Duration::from_millis(500), &mut task)
            .await
            .expect("STREAM terminal notification trapped teardown")
            .unwrap();
        let result = finished.await.unwrap();
        assert_eq!(result.admitted_events, 1);
        assert_eq!(
            result.stream_disconnect,
            crate::engine::peer_completion::StreamDisconnect::Pending
        );
        assert!(result.error.is_none());
        assert_eq!(remote.read(&mut [0; 1]).await.unwrap(), 0);
        let PeerEvent::Event(ZmtpEvent::Message(message)) = event_inbox.recv().await.unwrap().1
        else {
            panic!("missing connect notification");
        };
        assert_eq!(message.len(), 1);
        assert_eq!(message.part_slice(0), Some(b"".as_slice()));
        assert!(event_inbox.recv().await.is_none());
    }

    #[tokio::test]
    async fn full_stream_mailbox_keeps_reverse_writes_and_local_shutdown_reachable() {
        for mode in ["close", "cancel", "linger", "drain"] {
            let (stream, mut remote) = tokio::io::duplex(4096);
            let (events, mut event_inbox) = mpsc::channel(1);
            let (completion, finished) = CompletionProgress::reserve(7);
            let (handle, mut task) =
                spawn(stream, 7, events, &CancellationToken::new(), completion);
            // Leave the connect notification admitted in the one-slot mailbox.
            tokio::time::timeout(Duration::from_millis(500), async {
                while event_inbox.len() != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            remote.write_all(b"blocked input").await.unwrap();
            handle
                .data_inbox
                .send(PeerDriverData::SendMessage(Message::single("reverse")))
                .await
                .unwrap();
            let mut reverse = [0; 7];
            tokio::time::timeout(Duration::from_millis(500), remote.read_exact(&mut reverse))
                .await
                .expect("STREAM actor admission trapped reverse output")
                .unwrap();
            assert_eq!(&reverse, b"reverse");
            let mut admitted = 1;
            if mode == "drain" {
                event_inbox.recv().await.unwrap();
                let (_, event) =
                    tokio::time::timeout(Duration::from_millis(500), event_inbox.recv())
                        .await
                        .unwrap()
                        .unwrap();
                assert!(
                    matches!(event, PeerEvent::Event(ZmtpEvent::Message(message))
                    if message.part_slice(0) == Some(b"blocked input".as_slice()))
                );
                admitted += 1;
            }
            match mode {
                "cancel" => handle.cancel.cancel(),
                "linger" => {
                    // Fill the transport's outbound side while its reader stops.
                    handle
                        .data_inbox
                        .send(PeerDriverData::SendMessage(Message::single(vec![0; 65536])))
                        .await
                        .unwrap();
                    handle
                        .inbox
                        .send(PeerDriverCommand::DrainAndClose {
                            deadline: Some(Instant::now() + Duration::from_millis(30)),
                        })
                        .await
                        .unwrap();
                }
                _ => handle.inbox.send(PeerDriverCommand::Close).await.unwrap(),
            }
            tokio::time::timeout(Duration::from_millis(500), &mut task)
                .await
                .expect("STREAM full mailbox trapped shutdown")
                .unwrap();
            let result = finished.await.unwrap();
            assert_eq!(result.admitted_events, admitted);
            assert_eq!(
                result.stream_disconnect,
                crate::engine::peer_completion::StreamDisconnect::Pending
            );
        }
    }
}
