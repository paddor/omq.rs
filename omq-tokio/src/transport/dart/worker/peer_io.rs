//! Per-peer command, data, and receive queue handling.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use omq_proto::{Message, TrySendError};
use tokio_util::sync::CancellationToken;

use super::{BYTES, DATA_MESSAGES, DartStats, MESSAGES, PeerIo, Session};
use crate::engine::signal::DataSignal;
use crate::engine::{PeerDriverCommand, PeerDriverData, RecvSink, SendPipeConsumer};

impl PeerIo {
    pub(crate) fn new(
        commands: crate::engine::control_inbox::Receiver,
        data: crate::engine::data_inbox::Receiver,
        pipe: Option<SendPipeConsumer>,
        sink: Option<RecvSink>,
        cancel: CancellationToken,
        completion: crate::engine::peer_completion::CompletionProgress,
        window: usize,
    ) -> Self {
        Self {
            commands,
            data,
            pipe,
            sink,
            cancel,
            completion,
            active: false,
            draining: false,
            deadline: None,
            staged: Vec::with_capacity(DATA_MESSAGES),
            deferred: VecDeque::with_capacity(DATA_MESSAGES),
            admissions: VecDeque::with_capacity(window),
            pipe_sequences: VecDeque::with_capacity(window),
            pending_flush: false,
        }
    }

    pub(super) fn connect_signal(&self, signal: &Arc<DataSignal>) {
        self.commands.dart_forward_to(signal.clone());
        self.data.dart_forward_to(signal.clone());
        if let Some(pipe) = &self.pipe {
            pipe.dart_forward_to(signal.clone());
        }
    }

    pub(super) fn register_space(&mut self, signal: &Arc<DataSignal>) {
        if let Some(sink) = &mut self.sink {
            sink.dart_forward_to(signal);
        }
    }

    pub(super) fn controls(&mut self, signal: &Arc<DataSignal>, now: Instant) -> bool {
        if self.cancel.is_cancelled() || self.deadline.is_some_and(|deadline| now >= deadline) {
            return false;
        }
        for _ in 0..MESSAGES {
            let Ok(command) = self.commands.try_recv() else {
                break;
            };
            match command {
                PeerDriverCommand::Close => return false,
                PeerDriverCommand::DrainAndClose { deadline } => {
                    self.draining = true;
                    self.deadline = deadline;
                }
                PeerDriverCommand::ActivateDataPlane => self.active = true,
                PeerDriverCommand::ActivateWithRecvSink(sink) => {
                    self.sink = Some(sink);
                    self.register_space(signal);
                    self.active = true;
                }
                PeerDriverCommand::SendCommand(_) => {}
            }
        }
        self.commands.release_consumed();
        true
    }

    pub(super) fn refill(&mut self, session: &mut Session) -> usize {
        if !self.active {
            return 0;
        }
        let capacity = session.send_capacity();
        let limit = capacity.min(DATA_MESSAGES);
        if limit == 0 {
            return 0;
        }
        let (mut count, mut bytes) = self.refill_pipe(session, limit);
        while count < limit && bytes < BYTES && session.send_capacity() != 0 {
            let Some((data, admission)) = self.data.dart_try_recv() else {
                break;
            };
            if let PeerDriverData::SendMessage(message) = data {
                bytes += message.byte_len();
                let sequence = session
                    .submit(message)
                    .expect("bounded validated data lane");
                self.admissions.push_back((sequence, admission));
                count += 1;
            }
        }
        self.data.release_consumed();
        count
    }

    fn refill_pipe(&mut self, session: &mut Session, limit: usize) -> (usize, usize) {
        let Some(pipe) = &mut self.pipe else {
            return (0, 0);
        };
        if self.deferred.is_empty() {
            if let Some(progress) = pipe.drain_queue_with(limit, BYTES, |message| {
                let sequence = session
                    .submit(message)
                    .expect("bounded validated send pipe");
                self.pipe_sequences.push_back(sequence);
                session.send_capacity() != 0
            }) {
                return progress;
            }
            pipe.drain_into(&mut self.staged, limit, BYTES);
        } else {
            let mut bytes = 0;
            while self.staged.len() < limit && bytes < BYTES {
                let Some(message) = self.deferred.pop_front() else {
                    break;
                };
                bytes += message.byte_len();
                self.staged.push(message);
            }
        }
        let mut bytes = 0;
        let mut count = 0;
        let mut staged = self.staged.drain(..);
        while let Some(message) = staged.next() {
            if session.send_capacity() == 0 {
                // Restore this prefix before older deferred messages.
                for pending in staged.rev() {
                    self.deferred.push_front(pending);
                }
                self.deferred.push_front(message);
                break;
            }
            bytes += message.byte_len();
            let sequence = session
                .submit(message)
                .expect("bounded validated send pipe");
            self.pipe_sequences.push_back(sequence);
            count += 1;
        }
        (count, bytes)
    }

    pub(super) fn acknowledge(&mut self, ack: u64) {
        let mut pipes = 0;
        while self
            .pipe_sequences
            .front()
            .is_some_and(|sequence| *sequence < ack)
        {
            self.pipe_sequences.pop_front();
            pipes += 1;
        }
        if pipes != 0
            && let Some(pipe) = &self.pipe
        {
            pipe.dart_acknowledge(pipes);
        }
        crate::engine::data_inbox::Admission::release_batch(
            std::iter::from_fn(|| {
                let (sequence, _) = self.admissions.front()?;
                if *sequence >= ack {
                    return None;
                }
                self.admissions.pop_front().map(|(_, admission)| admission)
            })
            .flatten(),
        );
    }

    pub(super) fn deliver(
        &mut self,
        session: &mut Session,
        groups: Option<&crate::socket::udp::JoinedGroups>,
        pool: &super::super::pool::ReceivePool,
        stats: &mut DartStats,
    ) -> usize {
        if !self.active {
            return 0;
        }
        let Some(sink) = &mut self.sink else {
            return 0;
        };
        let mut count = 0;
        let mut bytes = 0;
        let groups = groups.map(|groups| groups.load());
        while count < DATA_MESSAGES && bytes < BYTES {
            let Some(message) = session.take_received_with(|body| body.finish(pool)) else {
                break;
            };
            bytes += message.byte_len();
            let inline_credit = message.retained_size() == Some(std::mem::size_of::<Message>());
            // Empty group is the internal marker for an intentionally filtered
            // DISH message. It owns no body slot and is released immediately.
            if groups.is_some() && message.part_slice(0).is_some_and(<[u8]>::is_empty) {
                session.release_receive(1);
                count += 1;
                continue;
            }
            if let Some(groups) = &groups
                && !groups.contains(message.part_slice(0).expect("DISH group"))
            {
                drop(message);
                count += 1;
                continue;
            }
            match sink.try_deliver_deferred(message, &mut self.pending_flush) {
                Ok(()) => {
                    if inline_credit {
                        // Inline bodies own no shared receive buffer. Once the
                        // bounded application queue owns the value, reuse this
                        // session slot independently of application clones.
                        session.release_receive(1);
                    }
                    stats.received_messages += 1;
                    count += 1;
                }
                Err(TrySendError::Full(message)) => {
                    session.restore_received(message);
                    break;
                }
                Err(_) => {
                    self.cancel.cancel();
                    break;
                }
            }
        }
        sink.flush_delivery(&mut self.pending_flush);
        count
    }
}

impl Drop for PeerIo {
    fn drop(&mut self) {
        if let Some(sink) = &mut self.sink {
            sink.flush_delivery(&mut self.pending_flush);
        }
        self.cancel.cancel();
        let _ = self
            .completion
            .complete(omq_proto::DisconnectReason::PeerClosed);
    }
}
