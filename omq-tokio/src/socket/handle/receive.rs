use super::Socket;
use omq_proto::{
    error::{Error, Result},
    flow::DrainBudget,
    message::Message,
    proto::SocketType,
};

mod blocking;

impl Socket {
    /// Receive with a receipt from any unpaused PEER, PULL, or GATHER lane
    /// (`None`), or one selected lane (`Some(lane)`). PEER returns the body
    /// without an identity prefix; its receipt exposes the logical identity.
    /// PULL and GATHER receipts have no logical identity.
    /// Keep the receipt until accepting or discarding the
    /// body, or return both with [`unshift`](Self::unshift). Other receivers
    /// cannot overtake this message on its lane while the receipt is live.
    ///
    /// Targeted waits wake only for that lane and end with `Closed` after
    /// disconnect, handover, or socket close. Cancellation consumes nothing.
    /// ROUTER supports `None` with an inert receipt. Conflate and external
    /// receive sinks do not support lane claims.
    pub async fn recv_with_receipt(
        &self,
        lane: Option<&crate::socket::ReceiveLane>,
    ) -> Result<(crate::socket::ReceiveReceipt, Message)> {
        if self.inner.socket_type == SocketType::Router && lane.is_none() {
            let mut message = self.recv().await?;
            return Ok((
                crate::socket::ReceiveReceipt::inert(message.pop_identity()),
                message,
            ));
        }
        self.inner.recv_rx.recv_with_receipt(lane).await
    }

    /// Nonblocking form of [`recv_with_receipt`](Self::recv_with_receipt).
    pub fn try_recv_with_receipt(
        &self,
        lane: Option<&crate::socket::ReceiveLane>,
    ) -> Result<(crate::socket::ReceiveReceipt, Message)> {
        if self.inner.socket_type == SocketType::Router && lane.is_none() {
            let mut message = self.try_recv()?;
            return Ok((
                crate::socket::ReceiveReceipt::inert(message.pop_identity()),
                message,
            ));
        }
        self.inner.recv_rx.try_recv_with_receipt(lane)
    }

    /// Hold a received message ahead of its lane's queued messages. Fair
    /// and ordinary receives skip the lane until a targeted receive takes
    /// the held message and its new receipt is released. No new byte permit
    /// is reserved. A stale or oversized return preserves caller ownership
    /// through [`UnshiftError`](crate::socket::UnshiftError).
    pub fn unshift(
        &self,
        receipt: crate::socket::ReceiveReceipt,
        message: Message,
    ) -> std::result::Result<(), crate::socket::UnshiftError> {
        if self.inner.socket_type == SocketType::Gather && message.len() != 1 {
            return Err(crate::socket::UnshiftError {
                error: Error::Protocol("GATHER unshift requires a single-frame message".into()),
                message,
            });
        }
        self.inner.recv_rx.unshift(receipt, message)
    }

    /// Read a complete transport item without advancing request/reply state.
    /// Compatibility receive relays must defer admission to their application.
    #[doc(hidden)]
    pub async fn recv_for_external_recv(&self) -> Result<Message> {
        self.inner.recv_rx.recv().await
    }

    /// Nonblocking transport receive for compatibility polling/staging.
    /// Admission still belongs to the later application receive.
    #[doc(hidden)]
    pub fn try_recv_for_external_recv(&self) -> Result<Message> {
        self.inner.recv_rx.try_recv()
    }

    /// Validate and admit a transport item at application receive. REQ and REP
    /// retain their source peer through every queue; REP also keeps its envelope.
    #[doc(hidden)]
    pub fn prepare_external_recv(&self, mut message: Message) -> Option<Message> {
        match self.inner.socket_type {
            SocketType::Req => {
                let peer_id = u64::from(message.take_routing_id()?.checked_sub(1)?);
                if message.len() < 2 || !message.pop_front()?.is_empty() {
                    return None;
                }
                if !self.inner.req_awaiting_reply.accept_reply(peer_id) {
                    return None;
                }
                Some(message)
            }
            SocketType::Rep => {
                let peer_id = u64::from(message.routing_id()?.checked_sub(1)?);
                let (envelope, body) = crate::routing::split_rep_request(&message)?;
                *self.inner.rep_current.lock().expect("rep current") = Some((peer_id, envelope));
                Some(body)
            }
            _ => Some(message),
        }
    }

    pub(crate) fn register_recv_waker(
        &self,
        waker: std::task::Waker,
    ) -> crate::engine::signal::WakerRegistration {
        self.inner.recv_rx.register_recv_waker(waker)
    }

    /// Receive the next message. Blocks until one is available or the socket
    /// is closed.
    pub async fn recv(&self) -> Result<Message> {
        if !matches!(self.inner.socket_type, SocketType::Req | SocketType::Rep) {
            return self.inner.recv_rx.recv().await;
        }
        let mut budget = DrainBudget::WORKER;
        loop {
            let message = self.inner.recv_rx.recv().await?;
            let byte_len = message.byte_len();
            if let Some(message) = self.prepare_external_recv(message) {
                return Ok(message);
            }
            if !budget.account(byte_len) {
                tokio::task::yield_now().await;
                budget.reset();
            }
        }
    }

    /// Stream owned messages through normal socket receive admission.
    ///
    /// Polls on the application's executor and yields cooperatively under load.
    /// Closure ends the stream; other receive errors are yielded once, then it
    /// ends. Pin the stream before calling `StreamExt::next`.
    ///
    /// Streams, `recv`, and socket clones compete for the same messages.
    /// Dropping the stream leaves unread messages available.
    /// Uses [`Self::try_recv`] for ready messages and [`Self::recv`] to wait;
    /// REQ/REP receives use `recv` directly.
    ///
    /// ```no_run
    /// # async fn example(socket: &omq_tokio::Socket) -> omq_tokio::Result<()> {
    /// use futures::{StreamExt, pin_mut};
    /// let messages = socket.message();
    /// pin_mut!(messages);
    /// while let Some(message) = messages.next().await {
    ///     let _message = message?;
    ///     // Process the owned message on the application task.
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[must_use = "message streams do nothing unless polled"]
    pub fn message(&self) -> impl futures::stream::FusedStream<Item = Result<Message>> + Send + '_ {
        crate::socket::recv_stream::MessageStream::new(self, || self.recv())
    }

    /// Receive up to `max` messages, waiting only for the first one.
    /// Subsequent messages are drained without waiting, within the receive
    /// work budget. REQ/REP return at most one message per call.
    pub async fn recv_many(&self, max: usize) -> Result<Vec<Message>> {
        let mut messages = Vec::with_capacity(max);
        self.recv_many_into(max, &mut messages).await?;
        Ok(messages)
    }

    /// Append up to `max` messages to reusable storage, waiting only for the
    /// first message. A zero limit leaves `out` unchanged. Canceling the wait
    /// consumes nothing; once the first message arrives, draining never awaits.
    pub async fn recv_many_into(&self, max: usize, out: &mut Vec<Message>) -> Result<usize> {
        if max == 0 {
            return Ok(0);
        }
        match self.try_recv_many_into(max, out) {
            Err(Error::WouldBlock) => {}
            result => return result,
        }
        let start_len = out.len();
        out.push(self.recv().await?);
        self.try_recv_many_after_first(max, start_len, out)
    }

    fn try_recv_many_after_first(
        &self,
        max: usize,
        start_len: usize,
        out: &mut Vec<Message>,
    ) -> Result<usize> {
        let appended = out.len() - start_len;
        if matches!(self.inner.socket_type, SocketType::Req | SocketType::Rep) {
            return Ok(appended);
        }
        match self.inner.recv_rx.try_recv_many_after_first(max, out) {
            Ok(n) => Ok(appended + n),
            Err(Error::WouldBlock | Error::Closed) => Ok(appended),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn try_recv_many(&self, max: usize) -> Result<Vec<Message>> {
        let mut messages = Vec::with_capacity(max);
        self.try_recv_many_into(max, &mut messages)?;
        Ok(messages)
    }

    /// Try to receive up to `max` ready messages into `out` without blocking.
    pub fn try_recv_many_into(&self, max: usize, out: &mut Vec<Message>) -> Result<usize> {
        if matches!(self.inner.socket_type, SocketType::Req | SocketType::Rep) {
            if max == 0 {
                return Ok(0);
            }
            out.push(self.try_recv()?);
            return Ok(1);
        }
        self.inner.recv_rx.try_recv_many_into(max, out)
    }

    /// Non-blocking receive. Returns `Err(Error::WouldBlock)` if no message is
    /// currently queued. Does not drive the I/O engine; messages already
    /// delivered by the background driver are visible.
    #[inline]
    pub fn try_recv(&self) -> Result<Message> {
        if !matches!(self.inner.socket_type, SocketType::Req | SocketType::Rep) {
            return self.inner.recv_rx.try_recv();
        }
        let mut budget = DrainBudget::WORKER;
        loop {
            let message = self.inner.recv_rx.try_recv()?;
            let byte_len = message.byte_len();
            if let Some(message) = self.prepare_external_recv(message) {
                return Ok(message);
            }
            if !budget.account(byte_len) {
                return Err(Error::WouldBlock);
            }
        }
    }
}
