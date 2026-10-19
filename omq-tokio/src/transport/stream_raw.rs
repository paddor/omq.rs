//! Raw TCP driver for STREAM sockets (tokio backend).
//!
//! Reads retain one pending actor event while reverse writes, local commands,
//! cancellation, and linger remain selected. The actor delivers the terminal
//! empty receive after the admitted prefix, using the reserved closure slot.

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
#[cfg(test)]
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::engine::actor_output::PeerOutput;
use crate::network_limit::{PacedRead, PacedWrite};
use omq_proto::flow::DrainBudget;
use omq_proto::message::Message;
#[cfg(test)]
use omq_proto::proto::Event as ZmtpEvent;

use crate::engine::driver::{DriverStream, DriverWrite};
use crate::engine::peer_completion::CompletionProgress;
use crate::engine::{
    ActorPeerDriverHandle, PeerDriverCommand, PeerDriverData, SendPipeConsumer, control_inbox,
    data_inbox,
};

/// Actor-side inputs for one STREAM peer driver.
pub(crate) struct RawPeer<'a> {
    pub(crate) peer_id: u64,
    pub(crate) peer_out: PeerOutput,
    /// Socket token; the driver runs under a child of it.
    pub(crate) cancel: &'a CancellationToken,
    pub(crate) completion: CompletionProgress,
    /// Data inbox capacity in messages.
    pub(crate) capacity: usize,
    pub(crate) pool: Option<crate::PayloadPool>,
    pub(crate) send_pipe: Option<SendPipeConsumer>,
}

pub(crate) fn spawn_on<T: DriverStream + Send + 'static>(
    stream: T,
    peer: RawPeer<'_>,
    io_pool: &crate::context::IoPoolHandle,
    assignment: crate::context::IoThreadLease,
    prepare: impl FnOnce(T) -> std::io::Result<T> + Send + 'static,
) -> (ActorPeerDriverHandle, tokio::task::JoinHandle<()>) {
    let RawPeer {
        peer_id,
        peer_out,
        cancel,
        completion,
        capacity,
        pool,
        send_pipe,
    } = peer;

    let (inbox_tx, inbox_rx) = control_inbox::channel(64);
    let (data_inbox_tx, data_inbox_rx) = data_inbox::channel(capacity);
    let child_cancel = cancel.child_token();
    let handle_cancel = child_cancel.clone();
    let mut completion = completion.with_stream_disconnect();
    let index = assignment.index();
    let network = io_pool.network_on(index);
    let link = RawLink {
        peer_id,
        peer_out,
        inbox: inbox_rx,
        data_inbox: data_inbox_rx,
        pool,
        send_pipe,
    };

    let task = io_pool.spawn_on(index, async move {
        let _assignment = assignment;
        let Ok(stream) = prepare(stream) else {
            let _ = completion.complete(omq_proto::DisconnectReason::PeerClosed);
            return;
        };
        RawDriver::new(stream, network, link)
            .run(child_cancel, &mut completion)
            .await;
        let _ = completion.complete(omq_proto::DisconnectReason::PeerClosed);
    });

    let handle = ActorPeerDriverHandle {
        inbox: inbox_tx,
        data_inbox: data_inbox_tx,
        cancel: handle_cancel,
        transmit_slot: None,
        direct_tcp_writer: None,
        send_pipe: None,
        inproc: None,
    };
    (handle, task)
}

/// Test driver for peer 7 with no payload pool or parent cancellation.
#[cfg(test)]
fn spawn<T: DriverStream + Send + 'static>(
    stream: T,
    peer_out: PeerOutput,
    completion: CompletionProgress,
    capacity: usize,
    send_pipe: Option<SendPipeConsumer>,
) -> (ActorPeerDriverHandle, tokio::task::JoinHandle<()>) {
    let io = crate::context::IoPoolHandle::none();
    let cancel = CancellationToken::new();
    let peer = RawPeer {
        peer_id: 7,
        peer_out,
        cancel: &cancel,
        completion,
        capacity,
        pool: None,
        send_pipe,
    };

    spawn_on(stream, peer, &io, io.reserve_thread(), Ok)
}

/// Actor-side endpoints of one STREAM peer.
struct RawLink {
    peer_id: u64,
    peer_out: PeerOutput,
    inbox: control_inbox::Receiver,
    data_inbox: data_inbox::Receiver,
    pool: Option<crate::PayloadPool>,
    send_pipe: Option<SendPipeConsumer>,
}

/// One STREAM connection. Fields drop in declaration order, which keeps the
/// writer closing before the reader and both before the actor endpoints.
struct RawDriver<R, W> {
    /// Inbound bytes waiting for actor mailbox space. Starts as the empty
    /// connect notification.
    pending_event: Option<Message>,
    /// Outbound bytes not yet fully written, and the written prefix length.
    pending: Option<Bytes>,
    pending_offset: usize,
    closing: bool,
    deadline: Option<std::time::Instant>,
    budget: DrainBudget,
    buf: Vec<u8>,
    writer: PacedWrite<W>,
    reader: PacedRead<R>,
    _network: crate::network_limit::Lane,
    send_pipe: Option<SendPipeConsumer>,
    pool: Option<crate::PayloadPool>,
    data_inbox: data_inbox::Receiver,
    inbox: control_inbox::Receiver,
    peer_out: PeerOutput,
    peer_id: u64,
}

impl<R, W> RawDriver<R, W> {
    fn new<T>(stream: T, network: crate::network_limit::Lane, link: RawLink) -> Self
    where
        T: DriverStream<Reader = R, Writer = W>,
    {
        let (reader, writer) = stream.split(false);
        Self {
            pending_event: Some(Message::single(Bytes::new())),
            pending: None,
            pending_offset: 0,
            closing: false,
            deadline: None,
            budget: DrainBudget::new(64, 64 * 1024),
            buf: vec![0u8; 64 * 1024],
            writer: PacedWrite::new(writer, network.egress()),
            reader: PacedRead::new(reader, network.ingress()),
            _network: network,
            send_pipe: link.send_pipe,
            pool: link.pool,
            data_inbox: link.data_inbox,
            inbox: link.inbox,
            peer_out: link.peer_out,
            peer_id: link.peer_id,
        }
    }
}

impl<R, W> RawDriver<R, W>
where
    R: AsyncRead + Unpin,
    W: DriverWrite,
{
    async fn run(mut self, cancel: CancellationToken, completion: &mut CompletionProgress) {
        loop {
            // Commands, writes, and reads share one budget per turn; yield
            // once it is spent so other tasks on this runtime run.
            if self.budget.exhausted() {
                self.inbox.release_consumed();
                self.data_inbox.release_consumed();
                self.budget.reset();
                tokio::task::yield_now().await;
            }
            let _ = self.budget.account(0);

            // Release pacing reservations for directions this turn skips.
            if self.closing || self.pending_event.is_some() {
                self.reader.cancel_wait();
            }
            if self.pending.is_none() {
                self.writer.cancel_wait();
            }

            if !self.admit_event(completion) {
                return;
            }
            if self.drained() {
                self.shutdown(&cancel).await;
                return;
            }
            if !self.serve(&cancel).await {
                return;
            }
        }
    }

    /// Offer the held inbound event to the actor. Returns `false` once the
    /// actor is gone.
    fn admit_event(&mut self, completion: &mut CompletionProgress) -> bool {
        let Some(message) = self.pending_event.take() else {
            return true;
        };

        match self.peer_out.try_send(self.peer_id, message, false) {
            Ok(()) => completion.note_event(),
            Err(crate::engine::SendPipeError::Full(message)) => {
                self.pending_event = Some(message);
            }
            Err(crate::engine::SendPipeError::Closed(_)) => return false,
            #[cfg(feature = "dart")]
            Err(crate::engine::SendPipeError::Invalid(_)) => {
                unreachable!("receive output has no transport send validator")
            }
        }
        true
    }

    /// A draining close has written every accepted output.
    fn drained(&self) -> bool {
        self.closing
            && self.pending.is_none()
            && self.inbox.is_empty()
            && self.data_inbox.is_empty()
            && self
                .send_pipe
                .as_ref()
                .is_none_or(SendPipeConsumer::is_empty)
    }

    /// Shut the write side down, bounded by cancellation and linger.
    async fn shutdown(&mut self, cancel: &CancellationToken) {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {},
            () = async { tokio::time::sleep_until(self.deadline.unwrap().into()).await; },
                if self.deadline.is_some() => {},
            _ = self.writer.shutdown() => {},
        }
    }

    /// Wait for one event and serve it. Returns `false` to stop the driver.
    async fn serve(&mut self, cancel: &CancellationToken) -> bool {
        tokio::select! {
            biased;
            () = cancel.cancelled() => false,
            () = async { tokio::time::sleep_until(self.deadline.unwrap().into()).await; },
                if self.deadline.is_some() => false,
            cmd = self.inbox.recv() => self.on_command(cmd.as_ref()),
            result = self.peer_out.ready(), if self.pending_event.is_some() => result.is_ok(),
            written = async {
                self.writer.write(&self.pending.as_ref().unwrap()[self.pending_offset..]).await
            }, if self.pending.is_some() => self.on_written(written.ok()),
            n = self.reader.read(&mut self.buf), if !self.closing && self.pending_event.is_none() => {
                self.on_read(n.ok())
            },
            data = self.data_inbox.recv(),
                if self.pending.is_none() && (!self.closing || !self.data_inbox.is_empty()) => {
                self.on_data(data)
            },
            () = async { self.send_pipe.as_ref().unwrap().ready().await },
                if self.pending.is_none() && self.send_pipe.is_some() => self.on_pipe_ready(),
        }
    }

    fn on_command(&mut self, cmd: Option<&PeerDriverCommand>) -> bool {
        match cmd {
            Some(PeerDriverCommand::ActivateDataPlane | PeerDriverCommand::SendCommand(_)) => {}
            Some(PeerDriverCommand::Close | PeerDriverCommand::ActivateWithRecvSink(_)) | None => {
                return false;
            }
            // The first drain request fixes the linger deadline.
            Some(PeerDriverCommand::DrainAndClose { deadline }) => {
                if !self.closing {
                    self.closing = true;
                    self.deadline = *deadline;
                    self.data_inbox.close();
                }
            }
        }
        true
    }

    /// A write error or zero-length write ends the connection.
    fn on_written(&mut self, written: Option<usize>) -> bool {
        let Some(written) = written.filter(|&written| written != 0) else {
            return false;
        };

        self.pending_offset += written;
        if self.pending_offset == self.pending.as_ref().unwrap().len() {
            self.pending = None;
            self.pending_offset = 0;
        }
        true
    }

    /// A read error or EOF ends the connection.
    fn on_read(&mut self, n: Option<usize>) -> bool {
        let Some(n) = n.filter(|&n| n != 0) else {
            return false;
        };

        let _ = self.budget.account(n);
        let bytes = &self.buf[..n];
        let message = if let Some(pool) = &self.pool {
            let Ok(message) = pool.message(n, |out| out.copy_from_slice(bytes)) else {
                return false;
            };
            message
        } else {
            Message::single(Bytes::copy_from_slice(bytes))
        };
        self.pending_event = Some(message);
        true
    }

    fn on_data(&mut self, data: Option<PeerDriverData>) -> bool {
        match data {
            Some(PeerDriverData::SendMessage(message)) => self.stage(message),
            Some(PeerDriverData::SendEncoded(_)) => true,
            None => false,
        }
    }

    /// Take at most one message from the connect pipe.
    fn on_pipe_ready(&mut self) -> bool {
        let pipe = self.send_pipe.as_mut().unwrap();
        let mut next = None;
        pipe.drain_queue_with(1, 64 * 1024, |message| {
            next = Some(message);
            false
        })
        .expect("STREAM uses a queue pipe");

        match next {
            Some(message) => self.stage(message),
            None => !pipe.is_disconnected(),
        }
    }

    /// STREAM sends one body frame. An empty body closes the connection.
    fn stage(&mut self, mut message: Message) -> bool {
        let _ = self.budget.account(message.byte_len());
        let data = message.pop_front().unwrap_or_default();
        if data.is_empty() {
            return false;
        }
        self.pending = Some(data);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::PeerEvent;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn stream_linger_drains_connect_pipe_before_shutdown() {
        let (stream, mut remote) = tokio::io::duplex(16);
        let (events, _event_inbox) = mpsc::channel(1);
        let (completion, _finished) = CompletionProgress::reserve(7);
        let (mut producer, consumer) = crate::engine::send_pipe(2);
        producer.try_send(Message::single("first")).unwrap();
        producer.try_send(Message::single("second")).unwrap();
        let (handle, task) = spawn(stream, events.into(), completion, 2, Some(consumer));
        // Queue shutdown before the current-thread runtime runs the driver.
        handle
            .inbox
            .try_send(PeerDriverCommand::DrainAndClose {
                deadline: Some(Instant::now() + Duration::from_secs(2)),
            })
            .unwrap();
        let mut received = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), remote.read_to_end(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received, b"firstsecond");
        task.await.unwrap();
    }

    #[tokio::test]
    async fn stream_close_joins_without_reading_the_connect_notification() {
        let (stream, mut remote) = tokio::io::duplex(1024);
        let (events, mut event_inbox) = mpsc::channel(1);
        let (completion, finished) = CompletionProgress::reserve(7);
        let (handle, mut task) = spawn(stream, events.into(), completion, 64, None);
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
        assert_eq!(result.reason, omq_proto::DisconnectReason::PeerClosed);
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
            let (mut handle, mut task) = spawn(stream, events.into(), completion, 64, None);
            handle.data_inbox =
                crate::engine::data_inbox::SenderLanes::default().bind(&handle.data_inbox);

            assert_reverse_write_with_full_mailbox(&handle, &mut remote, &event_inbox).await;
            let admitted = close_with_full_mailbox(mode, &handle, &mut event_inbox).await;

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

    /// Leave the connect notification admitted in the one-slot mailbox and
    /// input pending behind it, then check reverse output still flows.
    async fn assert_reverse_write_with_full_mailbox(
        handle: &ActorPeerDriverHandle,
        remote: &mut tokio::io::DuplexStream,
        event_inbox: &mpsc::Receiver<(u64, PeerEvent)>,
    ) {
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
    }

    /// Close the stream the way `mode` names. Returns how many events the
    /// actor admitted before close.
    async fn close_with_full_mailbox(
        mode: &str,
        handle: &ActorPeerDriverHandle,
        event_inbox: &mut mpsc::Receiver<(u64, PeerEvent)>,
    ) -> u64 {
        let mut admitted = 1;
        if mode == "drain" {
            event_inbox.recv().await.unwrap();
            let (_, event) = tokio::time::timeout(Duration::from_millis(500), event_inbox.recv())
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

        admitted
    }
}
